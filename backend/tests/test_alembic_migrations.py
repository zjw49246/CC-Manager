"""Tests for Alembic migrations.

Ensures:
1. A legacy database (no alembic_version) can be migrated to head.
2. A fresh database can be created from scratch via migrations.
3. The final migrated schema matches the ORM models (no drift).
"""
import importlib.util
import io
import json
import shutil
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from alembic import command
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.script import ScriptDirectory
import pytest
from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    ForeignKeyConstraint,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    inspect,
    text,
)
from sqlalchemy.dialects import mysql, postgresql, sqlite
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.schema import CreateTable

# All ORM models must be imported so Base.metadata is complete.
from backend.database import Base
import backend.models.task  # noqa: F401
import backend.models.task_id_allocator  # noqa: F401
import backend.models.task_migration  # noqa: F401
import backend.models.instance  # noqa: F401
import backend.models.project  # noqa: F401
import backend.models.project_todo  # noqa: F401
import backend.models.log_entry  # noqa: F401
import backend.models.worktree  # noqa: F401
import backend.models.global_settings  # noqa: F401
import backend.models.secret  # noqa: F401
import backend.models.quick_phrase  # noqa: F401
import backend.models.workspace_review  # noqa: F401
import backend.models.test_harness  # noqa: F401
import backend.models.plan  # noqa: F401
import backend.models.ssh_profile  # noqa: F401
import backend.models.task_ssh_grant  # noqa: F401
import backend.models.task_ssh_effect  # noqa: F401
import backend.models.plan_agent  # noqa: F401
import backend.models.capability  # noqa: F401
import backend.models.code_review  # noqa: F401
import backend.models.delivery  # noqa: F401
import backend.models.pr_monitor  # noqa: F401
import backend.models.worker_task_termination  # noqa: F401
import backend.models.discussion  # noqa: F401
import backend.models.user_group  # noqa: F401

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
PUBLISHED_PLAN_REVISION = "b6e1f4a2c9d7"
PLAN_CLEANUP_REVISION = "f7a1c3d9e5b2"
PR_REVIEW_SNAPSHOT_REVISION = "5f7a9c2e4d61"
PUBLISHED_BRANCH_MERGE_REVISION = "7e4b9c1d2a63"
PR_REVIEW_PANEL_REVISION = "7a1d4e9c2b60"
PR_FINDING_ACTIONS_REVISION = "b7c9e2f4a610"
ATTENTION_TAG_REVISION = "2f6c8a1d4e90"
WORKSPACE_REVIEW_REVISION = "5a7d2c9e1b40"
TEST_HARNESS_REVISION = "7d2f4b9a6c10"
FIRST_CLASS_PLAN_HEAD_REVISION = "d4a7c9e2f1b6"
MAIN_PLAN_MERGE_REVISION = "e5b8d1c4a7f2"
BROWSER_PLAN_MERGE_REVISION = "9f2c6b4d8a10"
SANDBOX_LEASE_REVISION = "c8f1a2d4e6b9"
RESOLVED_TARGET_REVISION = "d9a2b4c6e8f1"
CHILD_BINDING_REVISION = "e0b3c5d7f9a1"
ARCHIVE_STATE_REVISION = "f1c4e6a8b0d2"
CHILD_LAUNCH_PROFILE_REVISION = "2a6c8e0f4b1d"
SSH_PROFILES_REVISION = "73c4a9e1b2d0"
TASK_SSH_GRANTS_REVISION = "84d5b0f2c3e1"
TASK_SSH_POLICY_REVISION = "91e6a4c8d2f0"
TASK_SSH_ROOTS_REVISION = "a6d9f2c4e8b1"
MAIN_SSH_MERGE_REVISION = "f9b2c4d6e8a1"
TASK_SCOPED_TOKEN_INCARNATION_REVISION = "b4e7c1a9d2f0"
TASK_SSH_EFFECT_REVISION = "c2f8a6d4e1b9"
DISCUSSION_LEASE_REVISION = "d1a9c4e7b260"
COMBINED_BROWSER_MAIN_REVISION = "4e8a1c6d9b20"
BROWSER_OPERATION_RECEIPT_REVISION = "6f3b9d2a7c10"
PR_REVIEW_MERGE_METHOD_REVISION = "1a7c9e4d2b60"
PR_REVIEW_BASE_REF_REVISION = "2b8d4f6a1c90"
PR_LOOP_REVISION = "a3f7c2d8e1b4"
CAPACITY_OVERRIDE_REVISION = "a4d8e2f6b1c3"
CAPACITY_PR_LOOP_MERGE_REVISION = "c5e7a9d1f3b6"
AGENT_SANDBOX_RUNTIME_OVERRIDE_REVISION = "e6a2c4f8b190"
DELIVERY_FRONTEND_REVIEW_REVISION = "a7d4e9c2f610"
DELIVERY_PREVIEW_PROFILES_REVISION = "b8e4d2f6a1c9"
REMOVE_AGENT_SANDBOX_OVERRIDE_REVISION = "a7d3f9c2e610"
TASK_EXECUTION_PRINCIPAL_REVISION = "b8e4a0d3f721"
REMOVE_TEAM_SHARE_SSH_FENCES_REVISION = "c9f5b1e7d402"
TASK_ID_NAMESPACE_REVISION = "d0a6c8e4f2b1"
UNIQUE_USER_GROUP_MEMBERSHIP_REVISION = "e1b7d9f3a5c2"
WORKER_NODE_DRAIN_REVISION = "f2c8a4e6d1b0"
TASK_MIGRATION_OPERATION_REVISION = "a3d9b5f7c1e4"
WORKER_DESTROY_LIFECYCLE_NONCE_REVISION = "b4e1c7d9f203"
WORKER_RENAME_TAG_OUTBOX_REVISION = "c4a8e2f6b190"
PR_MONITOR_TASK_TOMBSTONE_REVISION = "e2a4c6f8b1d3"
DELIVERY_PR_MONITOR_MERGE_REVISION = "f4c7a9d2e610"
INSTANCE_PROCESS_IDENTITY_REVISION = "a56a13b7f287"
PR_REVIEW_RESULT_EVIDENCE_REVISION = "b1d7e4a9c302"
PR_MONITOR_DISPLAY_TASK_REVISION = "f5d7b9e1c3a4"
UPDATE_CHANNEL_REVISION = "fa1c2e3d4b5f"
CAPABILITY_CORE_REVISION = "6a4c2e9f1b73"
CODE_REVIEW_REVISION = "8d4e1f7a9c20"
DELIVERY_LOOP_REVISION = "9e5b2a7c4d10"
AUTO_CAPABILITY_TURN_REVISION = "c3a7e9f1b2d4"
TERMINAL_ARBITRATION_REVISION = "4b8d2f6a1c90"
CAPABILITY_RESUME_OUTBOX_REVISION = "7c1e4a9d2f60"
PLAN_RUNTIME_RECEIPT_REVISION = "8d2f5b7a1c90"
WORKER_PLAN_DISPATCH_RECEIPT_REVISION = "a6e4c2d9f810"
WORKER_TASK_DELETE_RECEIPT_REVISION = "b7f3d1a8c920"
WORKER_PLAN_IMPORT_RECEIPT_REVISION = "d3c8a7f1e620"
CURRENT_HEAD_REVISION = "b9d4e6f1a2c7"
PR_MERGE_QUEUE_TRIGGER_REVISION = "a8c4e2f6b0d1"
DIRECT_PR_MERGE_REVISION = "b9d4e6f1a2c7"


def _alembic_cfg(db_path: str) -> Config:
    """Create an Alembic Config pointing at a specific database file.

    Also patches backend.config.settings.database_url so that env.py
    (which reads settings at import time) uses the test DB, not production.
    """
    db_url = f"sqlite:///{db_path}"
    cfg = Config(str(PROJECT_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(PROJECT_ROOT / "alembic"))
    cfg.set_main_option("sqlalchemy.url", db_url)
    return cfg


def _get_head_revision(cfg: Config) -> str:
    """Return the current head revision ID from migration scripts."""
    return ScriptDirectory.from_config(cfg).get_current_head()


def _run_alembic(cfg: Config, func, *args):
    """Run an Alembic command with settings.database_url patched to match cfg."""
    db_url = cfg.get_main_option("sqlalchemy.url")
    # env.py reads settings.database_url and overrides sqlalchemy.url,
    # so we must patch it to point at the test DB.
    async_url = db_url.replace("sqlite:///", "sqlite+aiosqlite:///")
    with patch("backend.config.settings.database_url", async_url):
        func(cfg, *args)


@pytest.mark.parametrize(
    ("migration_file", "kind"),
    [
        ("b8e4a0d3f721_add_task_execution_principal.py", "principal"),
        ("d0a6c8e4f2b1_add_task_id_namespace_allocator.py", "singleton"),
        ("f2c8a4e6d1b0_add_worker_node_drain_control.py", "singleton"),
        ("a3d9b5f7c1e4_add_task_migration_operations.py", "singleton"),
    ],
)
def test_postgresql_check_probe_accepts_server_rewritten_any_and_casts(
    migration_file,
    kind,
):
    """Expected CHECKs are compared after PostgreSQL canonicalizes them."""

    module = _load_revision_migration(
        migration_file,
        f"postgresql_check_probe_{kind}_{migration_file[:4]}",
    )
    if kind == "principal":
        expected = {
            **module._TASK_CHECKS,
            module._TASK_GATE: module._TASK_GATE_SQL,
        }
        actual_names = set(module._TASK_CHECKS)
    else:
        gate_sql = (
            module._GATE_SQL
            if hasattr(module, "_GATE_SQL")
            else module._DOWNGRADE_GATE_SQL
        )
        expected = {
            **module._CHECKS,
            module._DOWNGRADE_GATE: gate_sql,
        }
        actual_names = set(module._CHECKS)

    rewritten = (
        "CHECK (((execution_user_role)::text = ANY "
        "((ARRAY['member'::character varying, "
        "'admin'::character varying])::text[])))"
    )
    canonical_definitions = {
        name: rewritten if index == 0 else f"CHECK (({index} = {index}))"
        for index, name in enumerate(expected, start=1)
    }
    first_name = next(iter(expected))
    canonical_definitions[first_name] = rewritten
    actual_definitions = {
        name: canonical_definitions[name]
        for name in actual_names
    }

    bind = MagicMock()
    bind.execute.side_effect = [
        None,
        [
            (name, definition, True)
            for name, definition in actual_definitions.items()
        ],
        [
            (name, definition, True)
            for name, definition in canonical_definitions.items()
        ],
        None,
    ]
    with patch.object(module.op, "get_bind", return_value=bind):
        if kind == "principal":
            present = module._postgresql_canonical_check_state(
                module._TASK_TABLE,
                module._TASK_COLUMN_SPECS,
                module._TASK_CHECKS,
                gate_name=module._TASK_GATE,
                gate_sql=module._TASK_GATE_SQL,
            )
        else:
            present = set(module._postgresql_canonical_checks())

    assert present == actual_names


def test_enable_workflows_boolean_inversion_is_dialect_neutral(tmp_path):
    db_path = str(tmp_path / "enable-workflows-inversion.db")
    cfg = _alembic_cfg(db_path)
    _run_alembic(cfg, command.upgrade, "2d287d9865fc")
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO tasks "
            "(id, title, description, status, priority, disable_workflows) "
            "VALUES (1, 'disabled', 'd', 'pending', 0, 1), "
            "(2, 'enabled', 'd', 'pending', 0, 0)"
        ))
    engine.dispose()

    _run_alembic(cfg, command.upgrade, "0aa352edf132")
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        assert conn.execute(text(
            "SELECT id, enable_workflows FROM tasks ORDER BY id"
        )).all() == [(1, 0), (2, 1)]
    engine.dispose()

    _run_alembic(cfg, command.downgrade, "2d287d9865fc")
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        assert conn.execute(text(
            "SELECT id, disable_workflows FROM tasks ORDER BY id"
        )).all() == [(1, 1), (2, 0)]
    engine.dispose()


@pytest.mark.parametrize(
    ("migration_file", "json_column"),
    [
        ("69d5cf74de62_add_sort_order_and_tags_to_projects.py", "tags"),
        ("f3a8b2c1d9e0_add_env_files_to_projects.py", "env_files"),
    ],
)
@pytest.mark.parametrize(
    ("dialect_name", "expected_default"),
    [
        ("mysql", "(JSON_ARRAY())"),
        ("postgresql", "'[]'"),
    ],
)
def test_project_json_defaults_are_dialect_compatible(
    migration_file,
    json_column,
    dialect_name,
    expected_default,
):
    module = _load_revision_migration(
        migration_file,
        f"json_default_{json_column}_{dialect_name}",
    )
    bind = SimpleNamespace(
        dialect=SimpleNamespace(name=dialect_name, is_mariadb=False),
    )
    batch = MagicMock()
    batch_context = MagicMock()
    batch_context.__enter__.return_value = batch
    with (
        patch.object(module.op, "get_bind", return_value=bind),
        patch.object(
            module.op,
            "batch_alter_table",
            return_value=batch_context,
        ),
    ):
        module.upgrade()

    columns = [call.args[0] for call in batch.add_column.call_args_list]
    target = next(column for column in columns if column.name == json_column)
    assert str(target.server_default.arg) == expected_default


def test_project_model_json_defaults_compile_on_all_dialects():
    project_table = backend.models.project.Project.__table__
    expected_defaults = {
        "mysql": "DEFAULT (JSON_ARRAY())",
        "postgresql": "DEFAULT '[]'",
        "sqlite": "DEFAULT '[]'",
    }

    for dialect in (mysql.dialect(), postgresql.dialect(), sqlite.dialect()):
        ddl = str(CreateTable(project_table).compile(dialect=dialect))
        assert ddl.count(expected_defaults[dialect.name]) >= 2


class TestTaskIdNamespaceMigration:
    def test_postgresql_downgrade_replay_skips_lock_after_table_drop(self):
        module = _load_revision_migration(
            "d0a6c8e4f2b1_add_task_id_namespace_allocator.py",
            "task_id_postgresql_absent_table",
        )
        bind = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))
        inspector = MagicMock()
        inspector.has_table.return_value = False
        with (
            patch.object(module, "_is_offline", return_value=False),
            patch.object(module.op, "get_bind", return_value=bind),
            patch.object(module.sa, "inspect", return_value=inspector),
            patch.object(module.op, "execute") as execute,
        ):
            module._acquire_transactional_fence(downgrade=True)

        execute.assert_not_called()

    def test_upgrade_replays_create_before_seed_or_revision_stamp(
        self,
        tmp_path,
    ):
        """A committed CREATE with no singleton row must resume safely."""

        from backend.models.task_id_allocator import TaskIdAllocator

        db_path = str(tmp_path / "task-id-create-crash.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(
            cfg,
            command.upgrade,
            REMOVE_TEAM_SHARE_SSH_FENCES_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        TaskIdAllocator.__table__.create(engine)
        with engine.begin() as conn:
            # The metadata hook normally seeds test databases.  Removing that
            # row reproduces a non-transactional DDL crash after CREATE but
            # before the migration's INSERT/version stamp.
            conn.execute(text("DELETE FROM task_id_allocators"))
        engine.dispose()

        _run_alembic(cfg, command.upgrade, TASK_ID_NAMESPACE_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT id, node_role, next_worker_task_id "
                "FROM task_id_allocators"
            )).one() == (1, None, 1_000_000_000)
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == TASK_ID_NAMESPACE_REVISION
        engine.dispose()

    def test_downgrade_replays_drop_before_revision_stamp(self, tmp_path):
        """A committed DROP with the old Alembic stamp is an idempotent suffix."""

        db_path = str(tmp_path / "task-id-drop-crash.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, TASK_ID_NAMESPACE_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE task_id_allocators"))
        engine.dispose()

        _run_alembic(
            cfg,
            command.downgrade,
            REMOVE_TEAM_SHARE_SSH_FENCES_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        assert "task_id_allocators" not in inspect(engine).get_table_names()
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == REMOVE_TEAM_SHARE_SSH_FENCES_REVISION
        engine.dispose()

    def test_upgrade_seeds_singleton_and_allows_unbound_downgrade(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "task-id-namespace.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(
            cfg,
            command.upgrade,
            REMOVE_TEAM_SHARE_SSH_FENCES_REVISION,
        )
        assert "task_id_allocators" not in inspect(
            create_engine(f"sqlite:///{db_path}")
        ).get_table_names()

        _run_alembic(cfg, command.upgrade, "head")
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT id, node_role, next_worker_task_id "
                "FROM task_id_allocators"
            )).one()
        assert tuple(row) == (1, None, 1_000_000_000)

        _run_alembic(
            cfg,
            command.downgrade,
            REMOVE_TEAM_SHARE_SSH_FENCES_REVISION,
        )
        assert "task_id_allocators" not in inspect(engine).get_table_names()
        engine.dispose()


    @pytest.mark.parametrize(
        "unsafe_state",
        ["manager-role", "worker-role", "high-task"],
    )
    def test_downgrade_refuses_to_reopen_collision_risk(
        self,
        tmp_path,
        unsafe_state,
    ):
        db_path = str(tmp_path / f"task-id-{unsafe_state}.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, "head")
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            if unsafe_state in {"manager-role", "worker-role"}:
                conn.execute(text(
                    "UPDATE task_id_allocators SET node_role = :node_role "
                    "WHERE id = 1"
                ), {"node_role": unsafe_state.removesuffix("-role")})
            else:
                conn.execute(
                    backend.models.task.Task.__table__.insert().values(
                        id=1_000_000_000,
                        title="reserved high row",
                        description="d",
                        status="completed",
                    )
                )

        expected = (
            "after Task-id namespace binding"
            if unsafe_state in {"manager-role", "worker-role"}
            else "high-range Tasks"
        )
        with pytest.raises(RuntimeError, match=expected):
            _run_alembic(
                cfg,
                command.downgrade,
                REMOVE_TEAM_SHARE_SSH_FENCES_REVISION,
            )
        assert "task_id_allocators" in inspect(engine).get_table_names()
        engine.dispose()


class TestUniqueUserGroupMembershipMigration:
    @pytest.mark.parametrize("direction", ("upgrade", "downgrade"))
    def test_sqlite_offline_batch_reflection_is_refused(self, direction):
        module = _load_revision_migration(
            "e1b7d9f3a5c2_unique_user_group_membership.py",
            f"unique_membership_sqlite_offline_{direction}",
        )
        bind = MagicMock()
        bind.dialect.name = "sqlite"
        with (
            patch.object(module, "_is_offline", return_value=True),
            patch.object(module.op, "get_bind", return_value=bind),
            pytest.raises(RuntimeError, match="online SQLite batch reflection"),
        ):
            getattr(module, direction)()

    @pytest.mark.parametrize("dialect_name", ("mysql", "mariadb"))
    def test_mysql_family_uses_self_join_delete(self, dialect_name):
        module = _load_revision_migration(
            "e1b7d9f3a5c2_unique_user_group_membership.py",
            f"dedupe_{dialect_name}",
        )
        bind = SimpleNamespace(
            dialect=SimpleNamespace(name=dialect_name),
        )
        with (
            patch.object(module.op, "get_bind", return_value=bind),
            patch.object(module.op, "execute") as execute,
        ):
            module._delete_duplicate_memberships()

        sql = execute.call_args.args[0]
        assert sql.startswith("DELETE duplicate FROM user_group_members")
        assert "INNER JOIN user_group_members AS keeper" in sql

    def test_upgrade_deduplicates_rows_and_enforces_uniqueness(self, tmp_path):
        db_path = str(tmp_path / "unique-user-group-membership.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, TASK_ID_NAMESPACE_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO user_group_members (id, group_id, user_id) "
                "VALUES (10, 7, 11), (12, 7, 11), (13, 7, 12)"
            ))

        _run_alembic(cfg, command.upgrade, CURRENT_HEAD_REVISION)

        inspector = inspect(engine)
        constraints = {
            constraint["name"]: tuple(constraint["column_names"])
            for constraint in inspector.get_unique_constraints(
                "user_group_members"
            )
        }
        assert constraints["uq_user_group_member"] == (
            "group_id",
            "user_id",
        )
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT id, group_id, user_id FROM user_group_members "
                "ORDER BY id"
            )).all()
        assert [tuple(row) for row in rows] == [
            (10, 7, 11),
            (13, 7, 12),
        ]
        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(text(
                    "INSERT INTO user_group_members (group_id, user_id) "
                    "VALUES (7, 11)"
                ))
        engine.dispose()


    def test_replays_committed_upgrade_and_downgrade_before_stamp(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "unique-membership-stamp-replay.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(
            cfg,
            command.upgrade,
            UNIQUE_USER_GROUP_MEMBERSHIP_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE alembic_version SET version_num = :revision"),
                {"revision": TASK_ID_NAMESPACE_REVISION},
            )
        engine.dispose()

        _run_alembic(
            cfg,
            command.upgrade,
            UNIQUE_USER_GROUP_MEMBERSHIP_REVISION,
        )
        _run_alembic(
            cfg,
            command.downgrade,
            TASK_ID_NAMESPACE_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE alembic_version SET version_num = :revision"),
                {"revision": UNIQUE_USER_GROUP_MEMBERSHIP_REVISION},
            )
        engine.dispose()

        _run_alembic(
            cfg,
            command.downgrade,
            TASK_ID_NAMESPACE_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        assert not any(
            tuple(item.get("column_names") or ()) == ("group_id", "user_id")
            for item in inspect(engine).get_unique_constraints(
                "user_group_members"
            )
        )
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == TASK_ID_NAMESPACE_REVISION
        engine.dispose()


class TestRoleAndShareMigrationReplay:
    def test_remove_setting_upgrade_fence_accepts_only_published_sibling(self):
        module = _load_revision_migration(
            "a7d3f9c2e610_remove_agent_sandbox_unrestricted_setting.py",
            "published_sibling_writer_fence",
        )
        bind = MagicMock()
        bind.dialect.name = "sqlite"
        bind.execute.return_value.rowcount = 1
        with (
            patch.object(module, "_is_offline", return_value=False),
            patch.object(module.op, "get_bind", return_value=bind),
        ):
            module._acquire_sqlite_writer_fence(downgrade=False)

        params = bind.execute.call_args.args[1]
        assert params == {
            "expected_revisions": (
                module.down_revision,
                DELIVERY_PREVIEW_PROFILES_REVISION,
            )
        }

    def test_remove_setting_upgrade_fence_rejects_ambiguous_rows(self):
        module = _load_revision_migration(
            "a7d3f9c2e610_remove_agent_sandbox_unrestricted_setting.py",
            "ambiguous_sibling_writer_fence",
        )
        bind = MagicMock()
        bind.dialect.name = "sqlite"
        bind.execute.return_value.rowcount = 2
        with (
            patch.object(module, "_is_offline", return_value=False),
            patch.object(module.op, "get_bind", return_value=bind),
            pytest.raises(RuntimeError, match="writer fence"),
        ):
            module._acquire_sqlite_writer_fence(downgrade=False)

    @pytest.mark.parametrize("direction", ("upgrade", "downgrade"))
    def test_remove_setting_sqlite_offline_is_refused(self, direction):
        module = _load_revision_migration(
            "a7d3f9c2e610_remove_agent_sandbox_unrestricted_setting.py",
            f"sqlite_offline_{direction}",
        )
        bind = MagicMock()
        bind.dialect.name = "sqlite"
        with (
            patch.object(module, "_is_offline", return_value=True),
            patch.object(module.op, "get_bind", return_value=bind),
            pytest.raises(RuntimeError, match="online SQLite batch reflection"),
        ):
            getattr(module, direction)()

    def test_task_principal_sqlite_offline_upgrade_is_refused(self):
        module = _load_revision_migration(
            "b8e4a0d3f721_add_task_execution_principal.py",
            "sqlite_offline_upgrade",
        )
        bind = MagicMock()
        bind.dialect.name = "sqlite"
        with (
            patch.object(module, "_is_offline", return_value=True),
            patch.object(module, "_require_supported_mysql_family"),
            patch.object(module.op, "get_bind", return_value=bind),
            pytest.raises(RuntimeError, match="online SQLite batch reflection"),
        ):
            module.upgrade()

    @pytest.mark.parametrize(
        ("migration_file", "table_name"),
        [
            (
                "d0a6c8e4f2b1_add_task_id_namespace_allocator.py",
                "task_id_allocators",
            ),
            (
                "f2c8a4e6d1b0_add_worker_node_drain_control.py",
                "worker_node_controls",
            ),
        ],
    )
    @pytest.mark.parametrize("initial_state", ("canonical", "canonical_gated"))
    def test_mysql_singleton_downgrade_gate_precedes_drop_and_replays(
        self,
        migration_file,
        table_name,
        initial_state,
    ):
        module = _load_revision_migration(
            migration_file,
            f"mysql_singleton_gate_{table_name}_{initial_state}",
        )
        states = iter(
            ["canonical", "canonical_gated", "absent"]
            if initial_state == "canonical"
            else ["canonical_gated", "absent"]
        )
        events: list[str] = []
        with (
            patch.object(module, "_require_supported_mysql_family"),
            patch.object(module, "_is_offline", return_value=False),
            patch.object(module, "_acquire_transactional_fence"),
            patch.object(module, "_table_state", side_effect=lambda **_kw: next(states)),
            patch.object(module, "_seed_or_validate_singleton"),
            patch.object(module, "_assert_downgrade_safe"),
            patch.object(module, "_is_mysql_family", return_value=True),
            patch.object(
                module.op,
                "create_check_constraint",
                side_effect=lambda *_a, **_kw: events.append("gate"),
            ),
            patch.object(
                module.op,
                "drop_table",
                side_effect=lambda *_a, **_kw: events.append("drop"),
            ),
        ):
            module.downgrade()

        assert events[-1:] == ["drop"]
        if initial_state == "canonical":
            assert events == ["gate", "drop"]
        else:
            assert events == ["drop"]

    def test_remove_setting_rejects_foreign_boolean_default(self):
        module = _load_revision_migration(
            "a7d3f9c2e610_remove_agent_sandbox_unrestricted_setting.py",
            "foreign_default",
        )
        inspector = MagicMock()
        inspector.get_columns.return_value = [{
            "name": "agent_sandbox_unrestricted_enabled",
            "type": sqlite.BOOLEAN(),
            "nullable": True,
            "default": "1",
        }]
        with (
            patch.object(module.op, "get_bind", return_value=object()),
            patch.object(module.sa, "inspect", return_value=inspector),
        ):
            with pytest.raises(RuntimeError, match="foreign shape"):
                module._column_present()

    def test_remove_setting_replays_committed_ddl_before_stamp(self, tmp_path):
        db_path = str(tmp_path / "remove-setting-stamp-replay.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(
            cfg,
            command.upgrade,
            REMOVE_AGENT_SANDBOX_OVERRIDE_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE alembic_version SET version_num = :revision"),
                {"revision": DELIVERY_FRONTEND_REVIEW_REVISION},
            )
        engine.dispose()

        _run_alembic(
            cfg,
            command.upgrade,
            REMOVE_AGENT_SANDBOX_OVERRIDE_REVISION,
        )
        _run_alembic(
            cfg,
            command.downgrade,
            DELIVERY_FRONTEND_REVIEW_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE alembic_version SET version_num = :revision"),
                {"revision": REMOVE_AGENT_SANDBOX_OVERRIDE_REVISION},
            )
        engine.dispose()

        _run_alembic(
            cfg,
            command.downgrade,
            DELIVERY_FRONTEND_REVIEW_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        columns = {
            item["name"]
            for item in inspect(engine).get_columns("global_settings")
        }
        assert "agent_sandbox_unrestricted_enabled" in columns
        engine.dispose()

    def test_task_principal_replays_partial_upgrade_and_both_stamps(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "task-principal-partial-replay.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(
            cfg,
            command.upgrade,
            REMOVE_AGENT_SANDBOX_OVERRIDE_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text(
                "ALTER TABLE tasks ADD COLUMN execution_user_id INTEGER"
            ))
        engine.dispose()

        _run_alembic(cfg, command.upgrade, TASK_EXECUTION_PRINCIPAL_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        task_columns = {
            item["name"] for item in inspect(engine).get_columns("tasks")
        }
        assert {
            "execution_user_id",
            "execution_user_role",
            "execution_mode",
            "execution_principal_kind",
        } <= task_columns
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE alembic_version SET version_num = :revision"),
                {"revision": REMOVE_AGENT_SANDBOX_OVERRIDE_REVISION},
            )
        engine.dispose()

        _run_alembic(cfg, command.upgrade, TASK_EXECUTION_PRINCIPAL_REVISION)
        _run_alembic(
            cfg,
            command.downgrade,
            REMOVE_AGENT_SANDBOX_OVERRIDE_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE alembic_version SET version_num = :revision"),
                {"revision": TASK_EXECUTION_PRINCIPAL_REVISION},
            )
        engine.dispose()

        _run_alembic(
            cfg,
            command.downgrade,
            REMOVE_AGENT_SANDBOX_OVERRIDE_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        task_columns = {
            item["name"] for item in inspect(engine).get_columns("tasks")
        }
        assert "execution_principal_kind" not in task_columns
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == REMOVE_AGENT_SANDBOX_OVERRIDE_REVISION
        engine.dispose()

    @pytest.mark.parametrize("task_phase", ("new", "gated"))
    def test_mysql_task_principal_downgrade_gates_before_any_drop(
        self,
        task_phase,
    ):
        """A crash after the first gate resumes without an unguarded window."""

        module = _load_revision_migration(
            "b8e4a0d3f721_add_task_execution_principal.py",
            f"mysql_gate_order_{task_phase}",
        )
        phases = {
            module._TASK_TABLE: iter(
                (["new", "gated", "old"] if task_phase == "new" else [
                    "gated",
                    "old",
                ])
            ),
            module._OUTBOX_TABLE: iter(["new", "gated", "old"]),
        }
        events: list[tuple[str, str]] = []

        def reflected_phase(table_name, *_args, **_kwargs):
            return next(phases[table_name])

        def install_gate(table_name, *_args, **_kwargs):
            events.append(("gate", table_name))

        def drop_schema(table_name, *_args, **_kwargs):
            events.append(("drop", table_name))

        with (
            patch.object(module, "_require_supported_mysql_family"),
            patch.object(module, "_is_offline", return_value=False),
            patch.object(module, "_acquire_transactional_fence"),
            patch.object(module, "_table_phase", side_effect=reflected_phase),
            patch.object(module, "_assert_downgrade_safe"),
            patch.object(module, "_is_mysql_family", return_value=True),
            patch.object(
                module,
                "_mysql_install_gate",
                side_effect=install_gate,
            ),
            patch.object(
                module,
                "_mysql_drop_principal_schema",
                side_effect=drop_schema,
            ),
        ):
            module.downgrade()

        first_drop = next(
            index for index, event in enumerate(events) if event[0] == "drop"
        )
        assert ("gate", module._OUTBOX_TABLE) in events[:first_drop]
        if task_phase == "new":
            assert ("gate", module._TASK_TABLE) in events[:first_drop]
        else:
            assert ("gate", module._TASK_TABLE) not in events
        assert events[first_drop:] == [
            ("drop", module._OUTBOX_TABLE),
            ("drop", module._TASK_TABLE),
        ]

    def test_team_share_trigger_migration_replays_both_stamp_suffixes(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "team-share-trigger-stamp-replay.db")
        cfg = _alembic_cfg(db_path)
        trigger_names = {
            "trg_task_ssh_effect_team_task_share_insert",
            "trg_task_ssh_effect_team_task_share_update",
            "trg_task_ssh_effect_team_task_share_delete",
            "trg_task_ssh_effect_team_project_share_insert",
            "trg_task_ssh_effect_team_project_share_update",
            "trg_task_ssh_effect_team_project_share_delete",
        }

        def present_triggers() -> set[str]:
            engine = create_engine(f"sqlite:///{db_path}")
            try:
                with engine.connect() as conn:
                    return set(conn.execute(text(
                        "SELECT name FROM sqlite_master WHERE type = 'trigger'"
                    )).scalars()) & trigger_names
            finally:
                engine.dispose()

        _run_alembic(
            cfg,
            command.upgrade,
            REMOVE_TEAM_SHARE_SSH_FENCES_REVISION,
        )
        assert not present_triggers()
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE alembic_version SET version_num = :revision"),
                {"revision": TASK_EXECUTION_PRINCIPAL_REVISION},
            )
        engine.dispose()

        _run_alembic(
            cfg,
            command.upgrade,
            REMOVE_TEAM_SHARE_SSH_FENCES_REVISION,
        )
        assert not present_triggers()
        _run_alembic(
            cfg,
            command.downgrade,
            TASK_EXECUTION_PRINCIPAL_REVISION,
        )
        assert present_triggers() == trigger_names
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE alembic_version SET version_num = :revision"),
                {"revision": REMOVE_TEAM_SHARE_SSH_FENCES_REVISION},
            )
        engine.dispose()

        _run_alembic(
            cfg,
            command.downgrade,
            TASK_EXECUTION_PRINCIPAL_REVISION,
        )
        assert present_triggers() == trigger_names


class TestWorkerNodeDrainControlMigration:
    def test_metadata_create_all_seed_is_pristine_and_downgradable(
        self,
        tmp_path,
    ):
        from backend.models.worker import WorkerNodeControl

        db_path = str(tmp_path / "worker-node-metadata-seed.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(
            cfg,
            command.upgrade,
            UNIQUE_USER_GROUP_MEMBERSHIP_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        Base.metadata.create_all(
            engine,
            tables=[WorkerNodeControl.__table__],
        )
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT id, drain_claim, active_login_attempt_id, "
                "active_login_kind, drain_started_at, updated_at "
                "FROM worker_node_controls"
            )).one() == (1, None, None, None, None, None)
        engine.dispose()

        # A metadata-created canonical table can be adopted by the migration,
        # stamped, and removed again while the protocol remains unused.
        _run_alembic(cfg, command.upgrade, WORKER_NODE_DRAIN_REVISION)
        _run_alembic(
            cfg,
            command.downgrade,
            UNIQUE_USER_GROUP_MEMBERSHIP_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        assert "worker_node_controls" not in inspect(engine).get_table_names()
        engine.dispose()

    def test_upgrade_replays_create_before_seed_or_revision_stamp(
        self,
        tmp_path,
    ):
        """A CREATE/INSERT split crash must preserve the canonical table."""

        from backend.models.worker import WorkerNodeControl

        db_path = str(tmp_path / "worker-node-create-crash.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(
            cfg,
            command.upgrade,
            UNIQUE_USER_GROUP_MEMBERSHIP_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        WorkerNodeControl.__table__.create(engine)
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM worker_node_controls"))
        engine.dispose()

        _run_alembic(cfg, command.upgrade, WORKER_NODE_DRAIN_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT id, drain_claim, active_login_attempt_id, "
                "active_login_kind, drain_started_at, updated_at "
                "FROM worker_node_controls"
            )).one() == (1, None, None, None, None, None)
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == WORKER_NODE_DRAIN_REVISION
        engine.dispose()

    def test_downgrade_replays_drop_before_revision_stamp(self, tmp_path):
        db_path = str(tmp_path / "worker-node-drop-crash.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, WORKER_NODE_DRAIN_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE worker_node_controls"))
        engine.dispose()

        _run_alembic(
            cfg,
            command.downgrade,
            UNIQUE_USER_GROUP_MEMBERSHIP_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        assert "worker_node_controls" not in inspect(engine).get_table_names()
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == UNIQUE_USER_GROUP_MEMBERSHIP_REVISION
        engine.dispose()

    def test_pristine_upgrade_downgrade_upgrade(self, tmp_path):
        db_path = str(tmp_path / "worker-node-drain-control.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(
            cfg,
            command.upgrade,
            UNIQUE_USER_GROUP_MEMBERSHIP_REVISION,
        )

        _run_alembic(cfg, command.upgrade, WORKER_NODE_DRAIN_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "worker_node_controls" in inspect(engine).get_table_names()
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT id, drain_claim, active_login_attempt_id, "
                "active_login_kind FROM worker_node_controls"
            )).one() == (1, None, None, None)
        engine.dispose()

        _run_alembic(
            cfg,
            command.downgrade,
            UNIQUE_USER_GROUP_MEMBERSHIP_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        assert "worker_node_controls" not in inspect(engine).get_table_names()
        engine.dispose()

        _run_alembic(cfg, command.upgrade, WORKER_NODE_DRAIN_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT id FROM worker_node_controls"
            )).scalar_one() == 1
        engine.dispose()

    @pytest.mark.parametrize(
        ("column", "value"),
        [
            ("drain_claim", "a" * 64),
            ("active_login_attempt_id", "b" * 32),
            ("active_login_kind", "codex"),
            ("drain_started_at", "2026-08-14 00:00:00"),
            ("runtime_seal_claim", "c" * 64),
            ("runtime_sealed_at", "2026-08-14 00:00:01"),
            ("updated_at", "2026-08-14 00:00:00"),
        ],
    )
    def test_downgrade_refuses_durable_node_evidence(
        self,
        tmp_path,
        column,
        value,
    ):
        db_path = str(tmp_path / f"worker-node-drain-{column}.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, WORKER_NODE_DRAIN_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            if column in {"drain_claim", "drain_started_at"}:
                conn.execute(
                    text(
                        "UPDATE worker_node_controls "
                        "SET drain_claim = :drain_claim, "
                        "drain_started_at = :drain_started_at WHERE id = 1"
                    ),
                    {
                        "drain_claim": (
                            value if column == "drain_claim" else "a" * 64
                        ),
                        "drain_started_at": (
                            value
                            if column == "drain_started_at"
                            else "2026-08-14 00:00:00"
                        ),
                    },
                )
            elif column in {"runtime_seal_claim", "runtime_sealed_at"}:
                claim = value if column == "runtime_seal_claim" else "c" * 64
                conn.execute(
                    text(
                        "UPDATE worker_node_controls "
                        "SET drain_claim = :claim, "
                        "drain_started_at = :drain_started_at, "
                        "runtime_seal_claim = :claim, "
                        "runtime_sealed_at = :runtime_sealed_at WHERE id = 1"
                    ),
                    {
                        "claim": claim,
                        "drain_started_at": "2026-08-14 00:00:00",
                        "runtime_sealed_at": (
                            value
                            if column == "runtime_sealed_at"
                            else "2026-08-14 00:00:01"
                        ),
                    },
                )
            else:
                conn.execute(
                    text(
                        f"UPDATE worker_node_controls SET {column} = :value "
                        "WHERE id = 1"
                    ),
                    {"value": value},
                )
        engine.dispose()

        with pytest.raises(
            RuntimeError,
            match="Worker node drain/login evidence exists",
        ):
            _run_alembic(
                cfg,
                command.downgrade,
                UNIQUE_USER_GROUP_MEMBERSHIP_REVISION,
            )

        engine = create_engine(f"sqlite:///{db_path}")
        assert "worker_node_controls" in inspect(engine).get_table_names()
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == WORKER_NODE_DRAIN_REVISION
        engine.dispose()

    def test_postgresql_downgrade_takes_exclusive_control_table_lock(self):
        module = _load_revision_migration(
            "f2c8a4e6d1b0_add_worker_node_drain_control.py",
            "worker_node_postgresql_lock",
        )
        bind = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))
        inspector = MagicMock()
        inspector.has_table.return_value = True

        with (
            patch.object(module, "_is_offline", return_value=False),
            patch.object(module.op, "get_bind", return_value=bind),
            patch.object(module.sa, "inspect", return_value=inspector),
            patch.object(module.op, "execute") as execute,
        ):
            module._acquire_transactional_fence(downgrade=True)

        inspector.has_table.assert_called_once_with(module._TABLE)
        assert str(execute.call_args.args[0]) == (
            "LOCK TABLE worker_node_controls IN ACCESS EXCLUSIVE MODE"
        )

    def test_postgresql_downgrade_replay_skips_lock_after_table_drop(self):
        module = _load_revision_migration(
            "f2c8a4e6d1b0_add_worker_node_drain_control.py",
            "worker_node_postgresql_absent_table",
        )
        bind = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))
        inspector = MagicMock()
        inspector.has_table.return_value = False

        with (
            patch.object(module, "_is_offline", return_value=False),
            patch.object(module.op, "get_bind", return_value=bind),
            patch.object(module.sa, "inspect", return_value=inspector),
            patch.object(module.op, "execute") as execute,
        ):
            module._acquire_transactional_fence(downgrade=True)

        inspector.has_table.assert_called_once_with(module._TABLE)
        execute.assert_not_called()

    def test_mysql_upgrade_adopts_created_table_before_seed_or_stamp(self):
        module = _load_revision_migration(
            "f2c8a4e6d1b0_add_worker_node_drain_control.py",
            "worker_node_mysql_upgrade_replay",
        )
        with (
            patch.object(module, "_require_supported_mysql_family"),
            patch.object(module, "_is_offline", return_value=False),
            patch.object(module, "_acquire_transactional_fence"),
            patch.object(
                module,
                "_table_state",
                side_effect=("canonical", "canonical"),
            ),
            patch.object(module, "_create_table") as create_table,
            patch.object(module, "_seed_or_validate_singleton") as seed,
        ):
            module.upgrade()

        create_table.assert_not_called()
        seed.assert_called_once_with()

    @pytest.mark.parametrize("initial_state", ("canonical", "canonical_gated"))
    def test_mysql_downgrade_gate_precedes_drop_and_replays(
        self,
        initial_state,
    ):
        module = _load_revision_migration(
            "f2c8a4e6d1b0_add_worker_node_drain_control.py",
            f"worker_node_mysql_downgrade_{initial_state}",
        )
        states = iter(
            ("canonical", "canonical_gated", "absent")
            if initial_state == "canonical"
            else ("canonical_gated", "absent")
        )
        events: list[str] = []
        with (
            patch.object(module, "_require_supported_mysql_family"),
            patch.object(module, "_is_offline", return_value=False),
            patch.object(module, "_acquire_transactional_fence"),
            patch.object(
                module,
                "_table_state",
                side_effect=lambda **_kwargs: next(states),
            ),
            patch.object(
                module,
                "_assert_downgrade_safe",
                side_effect=lambda: events.append("safe"),
            ),
            patch.object(module, "_is_mysql_family", return_value=True),
            patch.object(
                module.op,
                "create_check_constraint",
                side_effect=lambda *_args, **_kwargs: events.append("gate"),
            ) as create_gate,
            patch.object(
                module.op,
                "drop_table",
                side_effect=lambda *_args, **_kwargs: events.append("drop"),
            ),
        ):
            module.downgrade()

        assert events == (
            ["safe", "gate", "safe", "drop"]
            if initial_state == "canonical"
            else ["safe", "safe", "drop"]
        )
        assert "drain_started_at IS NULL" in module._GATE_SQL
        if initial_state == "canonical":
            create_gate.assert_called_once_with(
                module._DOWNGRADE_GATE,
                module._TABLE,
                module._GATE_SQL,
            )
        else:
            create_gate.assert_not_called()

    def test_mysql_table_state_rejects_unenforced_control_check(self):
        module = _load_revision_migration(
            "f2c8a4e6d1b0_add_worker_node_drain_control.py",
            "worker_node_mysql_unenforced_check",
        )
        inspector = MagicMock()
        inspector.has_table.return_value = True
        inspector.get_columns.return_value = [
            {
                "name": name,
                "type": kind() if length is None else kind(length=length),
                "nullable": nullable,
                "default": None,
            }
            for name, (kind, length, nullable) in {
                "id": (module.sa.Integer, None, False),
                "drain_claim": (module.sa.String, 64, True),
                "runtime_seal_claim": (module.sa.String, 64, True),
                "active_login_attempt_id": (module.sa.String, 64, True),
                "active_login_kind": (module.sa.String, 32, True),
                "drain_started_at": (module.sa.DateTime, None, True),
                "runtime_sealed_at": (module.sa.DateTime, None, True),
                "updated_at": (module.sa.DateTime, None, True),
            }.items()
        ]
        inspector.get_pk_constraint.return_value = {
            "constrained_columns": ["id"]
        }
        inspector.get_unique_constraints.return_value = []
        inspector.get_foreign_keys.return_value = []
        inspector.get_check_constraints.return_value = [
            {"name": name, "sqltext": sqltext}
            for name, sqltext in module._CHECKS.items()
        ]
        engine_result = MagicMock()
        engine_result.scalars.return_value.all.return_value = ["InnoDB"]
        bind = MagicMock(
            dialect=SimpleNamespace(
                name="mysql",
                is_mariadb=False,
                server_version_info=(8, 0, 36),
            )
        )
        bind.execute.return_value = engine_result

        with (
            patch.object(module.sa, "inspect", return_value=inspector),
            patch.object(module.op, "get_bind", return_value=bind),
            patch.object(
                module,
                "_mysql_enforced_checks",
                return_value={"ck_worker_node_controls_singleton"},
            ),
            pytest.raises(RuntimeError, match="CHECK constraints are not enforced"),
        ):
            module._table_state()

    def test_mysql_enforcement_query_filters_not_enforced_checks(self):
        module = _load_revision_migration(
            "f2c8a4e6d1b0_add_worker_node_drain_control.py",
            "worker_node_mysql_enforcement_query",
        )
        bind = MagicMock(
            dialect=SimpleNamespace(name="mysql", is_mariadb=False)
        )
        bind.execute.return_value = [
            ("ck_worker_node_controls_singleton", "YES"),
            ("ck_worker_node_controls_drain_phase", "NO"),
        ]
        with patch.object(module.op, "get_bind", return_value=bind):
            assert module._mysql_enforced_checks() == {
                "ck_worker_node_controls_singleton"
            }
        statement, params = bind.execute.call_args.args
        assert "ENFORCED" in str(statement)
        assert params == {"table": module._TABLE}


class TestWorkerDestroyLifecycleNonceMigration:
    migration_file = (
        "b4e1c7d9f203_add_worker_destroy_lifecycle_nonce.py"
    )

    def test_pristine_upgrade_downgrade_upgrade(self, tmp_path):
        db_path = str(tmp_path / "worker-destroy-lifecycle-nonce.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, TASK_MIGRATION_OPERATION_REVISION)

        _run_alembic(
            cfg,
            command.upgrade,
            WORKER_DESTROY_LIFECYCLE_NONCE_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        columns = {
            item["name"]: item
            for item in inspect(engine).get_columns("workers")
        }
        nonce = columns["destroy_lifecycle_nonce"]
        assert isinstance(nonce["type"], String)
        assert nonce["type"].length == 32
        assert nonce["nullable"] is True
        receipt = columns["destroy_termination_receipt"]
        assert isinstance(receipt["type"], JSON)
        assert receipt["nullable"] is True
        engine.dispose()

        _run_alembic(
            cfg,
            command.downgrade,
            TASK_MIGRATION_OPERATION_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        worker_columns = _get_table_columns(engine, "workers")
        assert "destroy_lifecycle_nonce" not in worker_columns
        assert "destroy_termination_receipt" not in worker_columns
        engine.dispose()

        _run_alembic(
            cfg,
            command.upgrade,
            WORKER_DESTROY_LIFECYCLE_NONCE_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        worker_columns = _get_table_columns(engine, "workers")
        assert worker_columns["destroy_lifecycle_nonce"] == "VARCHAR(32)"
        assert worker_columns["destroy_termination_receipt"] == "JSON"
        engine.dispose()

    @pytest.mark.parametrize(
        ("preexisting_column", "column_type"),
        [
            ("destroy_lifecycle_nonce", "VARCHAR(32)"),
            ("destroy_termination_receipt", "JSON"),
        ],
    )
    def test_upgrade_replays_partial_add_before_revision_stamp(
        self,
        tmp_path,
        preexisting_column,
        column_type,
    ):
        db_path = str(
            tmp_path / f"worker-destroy-add-crash-{preexisting_column}.db"
        )
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, TASK_MIGRATION_OPERATION_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text(
                f"ALTER TABLE workers ADD COLUMN {preexisting_column} "
                f"{column_type}"
            ))
        engine.dispose()

        _run_alembic(
            cfg,
            command.upgrade,
            WORKER_DESTROY_LIFECYCLE_NONCE_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == WORKER_DESTROY_LIFECYCLE_NONCE_REVISION
        worker_columns = _get_table_columns(engine, "workers")
        assert worker_columns["destroy_lifecycle_nonce"] == "VARCHAR(32)"
        assert worker_columns["destroy_termination_receipt"] == "JSON"
        engine.dispose()

    def test_downgrade_replays_drop_before_revision_stamp(self, tmp_path):
        db_path = str(tmp_path / "worker-destroy-drop-crash.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(
            cfg,
            command.upgrade,
            WORKER_DESTROY_LIFECYCLE_NONCE_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text(
                "ALTER TABLE workers DROP COLUMN destroy_termination_receipt"
            ))
            conn.execute(text(
                "ALTER TABLE workers DROP COLUMN destroy_lifecycle_nonce"
            ))
        engine.dispose()

        _run_alembic(
            cfg,
            command.downgrade,
            TASK_MIGRATION_OPERATION_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        worker_columns = _get_table_columns(engine, "workers")
        assert "destroy_lifecycle_nonce" not in worker_columns
        assert "destroy_termination_receipt" not in worker_columns
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == TASK_MIGRATION_OPERATION_REVISION
        engine.dispose()

    @pytest.mark.parametrize(
        ("case", "status", "bootstrap_step", "nonce", "receipt"),
        [
            ("nonce", "ready", None, "d" * 32, None),
            (
                "termination-receipt",
                "ready",
                None,
                None,
                '{"phase":"submitted"}',
            ),
            ("destroying-status", "destroying", None, None, None),
            ("destroy-step", "ready", "destroy", None, None),
        ],
    )
    def test_downgrade_refuses_destroy_lifecycle_evidence(
        self,
        tmp_path,
        case,
        status,
        bootstrap_step,
        nonce,
        receipt,
    ):
        db_path = str(tmp_path / f"worker-destroy-evidence-{case}.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(
            cfg,
            command.upgrade,
            WORKER_DESTROY_LIFECYCLE_NONCE_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO workers "
                    "(name, status, bootstrap_step, "
                    "destroy_lifecycle_nonce, destroy_termination_receipt, "
                    "created_at, updated_at) "
                    "VALUES ('destroy evidence', :status, :bootstrap_step, "
                    ":nonce, :receipt, '2026-08-14 00:00:00', "
                    "'2026-08-14 00:00:00')"
                ),
                {
                    "status": status,
                    "bootstrap_step": bootstrap_step,
                    "nonce": nonce,
                    "receipt": receipt,
                },
            )
        engine.dispose()

        with pytest.raises(
            RuntimeError,
            match="Worker destroy lifecycle evidence exists",
        ):
            _run_alembic(
                cfg,
                command.downgrade,
                TASK_MIGRATION_OPERATION_REVISION,
            )

        engine = create_engine(f"sqlite:///{db_path}")
        assert "destroy_lifecycle_nonce" in _get_table_columns(
            engine,
            "workers",
        )
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == WORKER_DESTROY_LIFECYCLE_NONCE_REVISION
        engine.dispose()

    @pytest.mark.parametrize(
        ("column", "column_type", "reflected_type"),
        [
            ("destroy_lifecycle_nonce", "VARCHAR(16)", "VARCHAR(16)"),
            ("destroy_termination_receipt", "TEXT", "TEXT"),
        ],
    )
    def test_upgrade_rejects_foreign_existing_column_shape(
        self,
        tmp_path,
        column,
        column_type,
        reflected_type,
    ):
        db_path = str(tmp_path / f"worker-destroy-foreign-{column}.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, TASK_MIGRATION_OPERATION_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text(
                f"ALTER TABLE workers ADD COLUMN {column} {column_type}"
            ))
        engine.dispose()

        with pytest.raises(RuntimeError, match="foreign shape"):
            _run_alembic(
                cfg,
                command.upgrade,
                WORKER_DESTROY_LIFECYCLE_NONCE_REVISION,
            )
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == TASK_MIGRATION_OPERATION_REVISION
        assert _get_table_columns(engine, "workers")[column] == reflected_type
        engine.dispose()

    @pytest.mark.parametrize("json_check_present", (True, False))
    def test_mariadb_longtext_json_alias_requires_exact_validation_check(
        self,
        json_check_present,
    ):
        module = _load_revision_migration(
            self.migration_file,
            f"worker_destroy_mariadb_json_{json_check_present}",
        )
        inspector = MagicMock()
        inspector.has_table.return_value = True
        inspector.get_columns.return_value = [
            {
                "name": "destroy_lifecycle_nonce",
                "type": String(32),
                "nullable": True,
                "default": None,
            },
            {
                "name": "destroy_termination_receipt",
                "type": mysql.LONGTEXT(),
                "nullable": True,
                "default": None,
            },
        ]
        inspector.get_check_constraints.return_value = (
            [{
                "name": "destroy_termination_receipt",
                "sqltext": (
                    "json_valid(`destroy_termination_receipt`)"
                ),
            }]
            if json_check_present
            else []
        )
        inspector.get_pk_constraint.return_value = {
            "constrained_columns": ["id"]
        }
        inspector.get_indexes.return_value = []
        inspector.get_unique_constraints.return_value = []
        inspector.get_foreign_keys.return_value = []
        with (
            patch.object(module.op, "get_bind", return_value=object()),
            patch.object(module.sa, "inspect", return_value=inspector),
            patch.object(module, "_is_mariadb", return_value=True),
        ):
            if json_check_present:
                assert module._column_state() == "canonical"
            else:
                with pytest.raises(RuntimeError, match="JSON_VALID"):
                    module._column_state()

    @pytest.mark.parametrize("initial_state", ("canonical", "canonical_gated"))
    def test_mysql_downgrade_gate_precedes_drop_and_replays(
        self,
        initial_state,
    ):
        module = _load_revision_migration(
            self.migration_file,
            f"worker_destroy_mysql_gate_{initial_state}",
        )
        states = iter(
            ("canonical", "canonical_gated", "absent")
            if initial_state == "canonical"
            else ("canonical_gated", "absent")
        )
        events: list[str] = []
        with (
            patch.object(module, "_require_supported_mysql_family"),
            patch.object(module, "_is_offline", return_value=False),
            patch.object(module, "_acquire_transactional_fence"),
            patch.object(
                module,
                "_column_state",
                side_effect=lambda **_kwargs: next(states),
            ),
            patch.object(
                module,
                "_assert_downgrade_safe",
                side_effect=lambda: events.append("safe"),
            ),
            patch.object(module, "_is_mysql_family", return_value=True),
            patch.object(
                module.op,
                "create_check_constraint",
                side_effect=lambda *_args, **_kwargs: events.append("gate"),
            ) as create_gate,
            patch.object(
                module,
                "_drop_columns",
                side_effect=lambda **_kwargs: events.append("drop"),
            ),
        ):
            module.downgrade()

        expected = (
            ["safe", "gate", "safe", "drop"]
            if initial_state == "canonical"
            else ["safe", "safe", "drop"]
        )
        assert events == expected
        if initial_state == "canonical":
            create_gate.assert_called_once_with(
                module._MYSQL_DOWNGRADE_GATE,
                module._TABLE,
                module._MYSQL_DOWNGRADE_GATE_SQL,
            )
        else:
            create_gate.assert_not_called()

    @pytest.mark.parametrize(
        ("is_mariadb", "drop_gate"),
        [(False, "DROP CHECK"), (True, "DROP CONSTRAINT")],
    )
    def test_mysql_family_drops_gate_and_columns_in_one_atomic_alter(
        self,
        is_mariadb,
        drop_gate,
    ):
        module = _load_revision_migration(
            self.migration_file,
            f"worker_destroy_atomic_drop_{is_mariadb}",
        )
        with (
            patch.object(module, "_is_mysql_family", return_value=True),
            patch.object(module, "_is_mariadb", return_value=is_mariadb),
            patch.object(module.op, "execute") as execute,
        ):
            module._drop_columns(state="canonical_gated")

        execute.assert_called_once()
        assert str(execute.call_args.args[0]) == (
            f"ALTER TABLE workers {drop_gate} "
            "ck_workers_destroy_nonce_no_downgrade_use, "
            "DROP COLUMN destroy_termination_receipt, "
            "DROP COLUMN destroy_lifecycle_nonce"
        )

    def test_offline_downgrade_fails_closed(self):
        module = _load_revision_migration(
            self.migration_file,
            "worker_destroy_offline_downgrade",
        )
        with (
            patch.object(module, "_require_supported_mysql_family"),
            patch.object(module, "_is_offline", return_value=True),
            patch.object(module, "_drop_columns") as drop,
            pytest.raises(RuntimeError, match="offline downgrade is refused"),
        ):
            module.downgrade()
        drop.assert_not_called()

    @pytest.mark.parametrize(
        ("table_present", "lock_count"),
        [(True, 1), (False, 0)],
    )
    def test_postgresql_downgrade_lock_is_replay_safe(
        self,
        table_present,
        lock_count,
    ):
        module = _load_revision_migration(
            self.migration_file,
            f"worker_destroy_postgresql_lock_{table_present}",
        )
        bind = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))
        inspector = MagicMock()
        inspector.has_table.return_value = table_present
        with (
            patch.object(module, "_is_offline", return_value=False),
            patch.object(module.op, "get_bind", return_value=bind),
            patch.object(module.sa, "inspect", return_value=inspector),
            patch.object(module.op, "execute") as execute,
        ):
            module._acquire_transactional_fence(downgrade=True)

        inspector.has_table.assert_called_once_with(module._TABLE)
        assert execute.call_count == lock_count
        if table_present:
            assert str(execute.call_args.args[0]) == (
                "LOCK TABLE workers IN ACCESS EXCLUSIVE MODE"
            )


class TestWorkerRenameTagOutboxMigration:
    migration_file = "c4a8e2f6b190_add_worker_rename_tag_outbox.py"

    def test_pristine_upgrade_downgrade_upgrade(self, tmp_path):
        db_path = str(tmp_path / "worker-rename-tag-outbox.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(
            cfg,
            command.upgrade,
            WORKER_DESTROY_LIFECYCLE_NONCE_REVISION,
        )

        _run_alembic(
            cfg,
            command.upgrade,
            WORKER_RENAME_TAG_OUTBOX_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        columns = {
            item["name"]: item
            for item in inspect(engine).get_columns("workers")
        }
        generation = columns["rename_generation"]
        assert type(generation["type"]).__name__.upper() == "INTEGER"
        assert generation["nullable"] is False
        module = _load_revision_migration(
            self.migration_file,
            "worker_rename_default_shape",
        )
        assert module._default_is_zero(generation["default"])
        outbox = columns["rename_tag_outbox"]
        assert isinstance(outbox["type"], JSON)
        assert outbox["nullable"] is True
        engine.dispose()

        _run_alembic(
            cfg,
            command.downgrade,
            WORKER_DESTROY_LIFECYCLE_NONCE_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        columns = _get_table_columns(engine, "workers")
        assert "rename_generation" not in columns
        assert "rename_tag_outbox" not in columns
        engine.dispose()

        _run_alembic(
            cfg,
            command.upgrade,
            WORKER_RENAME_TAG_OUTBOX_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        columns = _get_table_columns(engine, "workers")
        assert columns["rename_generation"] == "INTEGER"
        assert columns["rename_tag_outbox"] == "JSON"
        engine.dispose()

    @pytest.mark.parametrize(
        ("preexisting_column", "column_ddl"),
        [
            (
                "rename_generation",
                "rename_generation INTEGER DEFAULT 0 NOT NULL",
            ),
            ("rename_tag_outbox", "rename_tag_outbox JSON"),
        ],
    )
    def test_upgrade_replays_partial_add_before_revision_stamp(
        self,
        tmp_path,
        preexisting_column,
        column_ddl,
    ):
        db_path = str(
            tmp_path / f"worker-rename-add-crash-{preexisting_column}.db"
        )
        cfg = _alembic_cfg(db_path)
        _run_alembic(
            cfg,
            command.upgrade,
            WORKER_DESTROY_LIFECYCLE_NONCE_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text(
                f"ALTER TABLE workers ADD COLUMN {column_ddl}"
            ))
        engine.dispose()

        _run_alembic(
            cfg,
            command.upgrade,
            WORKER_RENAME_TAG_OUTBOX_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        columns = _get_table_columns(engine, "workers")
        assert columns["rename_generation"] == "INTEGER"
        assert columns["rename_tag_outbox"] == "JSON"
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == WORKER_RENAME_TAG_OUTBOX_REVISION
        engine.dispose()

    @pytest.mark.parametrize("drop_generation", (False, True))
    def test_downgrade_replays_partial_drop_before_revision_stamp(
        self,
        tmp_path,
        drop_generation,
    ):
        db_path = str(
            tmp_path / f"worker-rename-drop-crash-{drop_generation}.db"
        )
        cfg = _alembic_cfg(db_path)
        _run_alembic(
            cfg,
            command.upgrade,
            WORKER_RENAME_TAG_OUTBOX_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text(
                "ALTER TABLE workers DROP COLUMN rename_tag_outbox"
            ))
            if drop_generation:
                conn.execute(text(
                    "ALTER TABLE workers DROP COLUMN rename_generation"
                ))
        engine.dispose()

        _run_alembic(
            cfg,
            command.downgrade,
            WORKER_DESTROY_LIFECYCLE_NONCE_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        columns = _get_table_columns(engine, "workers")
        assert "rename_generation" not in columns
        assert "rename_tag_outbox" not in columns
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == WORKER_DESTROY_LIFECYCLE_NONCE_REVISION
        engine.dispose()

    @pytest.mark.parametrize(
        ("column", "column_ddl"),
        [
            (
                "rename_generation",
                "rename_generation INTEGER DEFAULT 1 NOT NULL",
            ),
            (
                "rename_generation",
                "rename_generation INTEGER DEFAULT 0",
            ),
            ("rename_tag_outbox", "rename_tag_outbox TEXT"),
            ("rename_tag_outbox", "rename_tag_outbox JSON NOT NULL"),
        ],
    )
    def test_upgrade_rejects_foreign_existing_column_shape(
        self,
        tmp_path,
        column,
        column_ddl,
    ):
        db_path = str(
            tmp_path / f"worker-rename-foreign-{column}-{len(column_ddl)}.db"
        )
        cfg = _alembic_cfg(db_path)
        _run_alembic(
            cfg,
            command.upgrade,
            WORKER_DESTROY_LIFECYCLE_NONCE_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text(
                f"ALTER TABLE workers ADD COLUMN {column_ddl}"
            ))
        engine.dispose()

        with pytest.raises(RuntimeError, match="foreign shape"):
            _run_alembic(
                cfg,
                command.upgrade,
                WORKER_RENAME_TAG_OUTBOX_REVISION,
            )
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == WORKER_DESTROY_LIFECYCLE_NONCE_REVISION
        assert column in _get_table_columns(engine, "workers")
        engine.dispose()

    @pytest.mark.parametrize("json_check_present", (True, False))
    def test_mariadb_longtext_json_alias_requires_exact_validation_check(
        self,
        json_check_present,
    ):
        module = _load_revision_migration(
            self.migration_file,
            f"worker_rename_mariadb_json_{json_check_present}",
        )
        inspector = MagicMock()
        inspector.has_table.return_value = True
        inspector.get_columns.return_value = [
            {
                "name": "rename_generation",
                "type": Integer(),
                "nullable": False,
                "default": "0",
            },
            {
                "name": "rename_tag_outbox",
                "type": mysql.LONGTEXT(),
                "nullable": True,
                "default": None,
            },
        ]
        inspector.get_check_constraints.return_value = (
            [{
                "name": "rename_tag_outbox",
                "sqltext": "json_valid(`rename_tag_outbox`)",
            }]
            if json_check_present
            else []
        )
        inspector.get_indexes.return_value = []
        inspector.get_unique_constraints.return_value = []
        inspector.get_foreign_keys.return_value = []
        with (
            patch.object(module.op, "get_bind", return_value=object()),
            patch.object(module.sa, "inspect", return_value=inspector),
            patch.object(module, "_is_mariadb", return_value=True),
        ):
            if json_check_present:
                assert module._column_state() == "canonical"
            else:
                with pytest.raises(RuntimeError, match="JSON alias"):
                    module._column_state()

    def test_postgresql_jsonb_is_not_adopted_as_canonical_json(self):
        module = _load_revision_migration(
            self.migration_file,
            "worker_rename_postgresql_jsonb",
        )
        inspector = MagicMock()
        inspector.has_table.return_value = True
        inspector.get_columns.return_value = [
            {
                "name": "rename_generation",
                "type": Integer(),
                "nullable": False,
                "default": "0",
            },
            {
                "name": "rename_tag_outbox",
                "type": postgresql.JSONB(),
                "nullable": True,
                "default": None,
            },
        ]
        inspector.get_check_constraints.return_value = []
        inspector.get_pk_constraint.return_value = {
            "constrained_columns": ["id"]
        }
        inspector.get_indexes.return_value = []
        inspector.get_unique_constraints.return_value = []
        inspector.get_foreign_keys.return_value = []
        with (
            patch.object(module.op, "get_bind", return_value=object()),
            patch.object(module.sa, "inspect", return_value=inspector),
            patch.object(module, "_is_mariadb", return_value=False),
            pytest.raises(RuntimeError, match="rename_tag_outbox.*foreign"),
        ):
            module._column_state()

    def test_downgrade_refuses_pending_cloud_rename(self, tmp_path):
        db_path = str(tmp_path / "worker-rename-pending-outbox.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(
            cfg,
            command.upgrade,
            WORKER_RENAME_TAG_OUTBOX_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO workers "
                    "(name, status, rename_generation, rename_tag_outbox, "
                    "created_at, updated_at) VALUES "
                    "('rename pending', 'ready', 1, :outbox, "
                    "'2026-08-14 00:00:00', '2026-08-14 00:00:00')"
                ),
                {"outbox": '{"generation":1}'},
            )
        engine.dispose()

        with pytest.raises(RuntimeError, match="rename outboxes are pending"):
            _run_alembic(
                cfg,
                command.downgrade,
                WORKER_DESTROY_LIFECYCLE_NONCE_REVISION,
            )
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == WORKER_RENAME_TAG_OUTBOX_REVISION
        engine.dispose()

    def test_postgresql_offline_upgrade_emits_canonical_columns(self):
        module = _load_revision_migration(
            self.migration_file,
            "worker_rename_postgresql_offline_upgrade",
        )
        output = io.StringIO()
        context = MigrationContext.configure(
            dialect_name="postgresql",
            opts={"as_sql": True, "output_buffer": output},
        )
        with patch.object(module, "op", Operations(context)):
            module.upgrade()

        ddl = " ".join(output.getvalue().lower().split())
        assert (
            "alter table workers add column rename_generation "
            "integer default 0 not null" in ddl
        )
        assert (
            "alter table workers add column rename_tag_outbox json" in ddl
        )

    def test_mysql_offline_upgrade_fails_closed(self):
        module = _load_revision_migration(
            self.migration_file,
            "worker_rename_mysql_offline_upgrade",
        )
        output = io.StringIO()
        context = MigrationContext.configure(
            dialect_name="mysql",
            opts={"as_sql": True, "output_buffer": output},
        )
        with (
            patch.object(module, "op", Operations(context)),
            pytest.raises(RuntimeError, match="refuses MySQL/MariaDB"),
        ):
            module.upgrade()
        assert output.getvalue() == ""

    def test_offline_downgrade_fails_closed(self):
        module = _load_revision_migration(
            self.migration_file,
            "worker_rename_offline_downgrade",
        )
        output = io.StringIO()
        context = MigrationContext.configure(
            dialect_name="postgresql",
            opts={"as_sql": True, "output_buffer": output},
        )
        with (
            patch.object(module, "op", Operations(context)),
            pytest.raises(RuntimeError, match="offline downgrade is refused"),
        ):
            module.downgrade()
        assert output.getvalue() == ""

    @pytest.mark.parametrize("initial_state", ("canonical", "canonical_gated"))
    def test_mysql_downgrade_gate_precedes_atomic_drop_and_replays(
        self,
        initial_state,
    ):
        module = _load_revision_migration(
            self.migration_file,
            f"worker_rename_mysql_gate_{initial_state}",
        )
        states = iter(
            ("canonical", "canonical_gated", "absent")
            if initial_state == "canonical"
            else ("canonical_gated", "absent")
        )
        events: list[str] = []
        with (
            patch.object(module, "_require_supported_mysql_family"),
            patch.object(module, "_is_offline", return_value=False),
            patch.object(module, "_acquire_transactional_fence"),
            patch.object(
                module,
                "_column_state",
                side_effect=lambda **_kwargs: next(states),
            ),
            patch.object(
                module,
                "_pending_outbox_count",
                side_effect=lambda: (events.append("safe"), 0)[1],
            ),
            patch.object(module, "_is_mysql_family", return_value=True),
            patch.object(
                module.op,
                "create_check_constraint",
                side_effect=lambda *_args, **_kwargs: events.append("gate"),
            ) as create_gate,
            patch.object(
                module,
                "_drop_canonical_columns",
                side_effect=lambda **_kwargs: events.append("drop"),
            ),
        ):
            module.downgrade()

        assert events == (
            ["safe", "gate", "safe", "drop"]
            if initial_state == "canonical"
            else ["safe", "safe", "drop"]
        )
        if initial_state == "canonical":
            create_gate.assert_called_once_with(
                module._MYSQL_DOWNGRADE_GATE,
                module._TABLE,
                module._MYSQL_DOWNGRADE_GATE_SQL,
            )
        else:
            create_gate.assert_not_called()

    @pytest.mark.parametrize(
        ("is_mariadb", "drop_gate"),
        [(False, "DROP CHECK"), (True, "DROP CONSTRAINT")],
    )
    def test_mysql_family_drops_gate_and_columns_in_one_atomic_alter(
        self,
        is_mariadb,
        drop_gate,
    ):
        module = _load_revision_migration(
            self.migration_file,
            f"worker_rename_atomic_drop_{is_mariadb}",
        )
        with (
            patch.object(module, "_is_mysql_family", return_value=True),
            patch.object(module, "_is_mariadb", return_value=is_mariadb),
            patch.object(module.op, "execute") as execute,
        ):
            module._drop_canonical_columns(state="canonical_gated")

        execute.assert_called_once()
        assert str(execute.call_args.args[0]) == (
            f"ALTER TABLE workers {drop_gate} "
            "ck_workers_rename_tag_no_downgrade_use, "
            "DROP COLUMN rename_tag_outbox, "
            "DROP COLUMN rename_generation"
        )

    def test_mysql_column_state_rejects_unenforced_downgrade_gate(self):
        module = _load_revision_migration(
            self.migration_file,
            "worker_rename_unenforced_gate",
        )
        inspector = MagicMock()
        inspector.has_table.return_value = True
        inspector.get_columns.return_value = [
            {
                "name": "rename_generation",
                "type": Integer(),
                "nullable": False,
                "default": "0",
            },
            {
                "name": "rename_tag_outbox",
                "type": JSON(),
                "nullable": True,
                "default": None,
            },
        ]
        inspector.get_check_constraints.return_value = [{
            "name": module._MYSQL_DOWNGRADE_GATE,
            "sqltext": module._MYSQL_DOWNGRADE_GATE_SQL,
        }]
        inspector.get_pk_constraint.return_value = {
            "constrained_columns": ["id"]
        }
        inspector.get_indexes.return_value = []
        inspector.get_unique_constraints.return_value = []
        inspector.get_foreign_keys.return_value = []
        with (
            patch.object(module.op, "get_bind", return_value=object()),
            patch.object(module.sa, "inspect", return_value=inspector),
            patch.object(module, "_is_mariadb", return_value=False),
            patch.object(module, "_is_mysql_family", return_value=True),
            patch.object(module, "_mysql_enforced_checks", return_value=set()),
            pytest.raises(RuntimeError, match="not enforced"),
        ):
            module._column_state(allow_gate=True)

    @pytest.mark.parametrize(
        ("table_present", "lock_count"),
        [(True, 1), (False, 0)],
    )
    def test_postgresql_downgrade_lock_is_replay_safe(
        self,
        table_present,
        lock_count,
    ):
        module = _load_revision_migration(
            self.migration_file,
            f"worker_rename_postgresql_lock_{table_present}",
        )
        bind = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))
        inspector = MagicMock()
        inspector.has_table.return_value = table_present
        with (
            patch.object(module, "_is_offline", return_value=False),
            patch.object(module.op, "get_bind", return_value=bind),
            patch.object(module.sa, "inspect", return_value=inspector),
            patch.object(module.op, "execute") as execute,
        ):
            module._acquire_transactional_fence(downgrade=True)

        inspector.has_table.assert_called_once_with(module._TABLE)
        assert execute.call_count == lock_count
        if table_present:
            assert str(execute.call_args.args[0]) == (
                "LOCK TABLE workers IN ACCESS EXCLUSIVE MODE"
            )

    def test_sqlite_revision_writer_fence_uses_exact_direction(self):
        module = _load_revision_migration(
            self.migration_file,
            "worker_rename_sqlite_fence",
        )
        result = MagicMock(rowcount=1)
        bind = MagicMock(dialect=SimpleNamespace(name="sqlite"))
        bind.execute.return_value = result
        with (
            patch.object(module, "_is_offline", return_value=False),
            patch.object(module.op, "get_bind", return_value=bind),
        ):
            module._acquire_transactional_fence(downgrade=False)
            upgrade_call = bind.execute.call_args
            module._acquire_transactional_fence(downgrade=True)
            downgrade_call = bind.execute.call_args

        assert upgrade_call.args[1] == {
            "expected_revision": module.down_revision
        }
        assert downgrade_call.args[1] == {
            "expected_revision": module.revision
        }


class TestPRMonitorTaskTombstoneMigration:
    migration_file = "e2a4c6f8b1d3_add_pr_monitor_task_tombstones.py"

    def test_pristine_upgrade_downgrade_upgrade(self, tmp_path):
        db_path = str(tmp_path / "pr-monitor-task-tombstones.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(
            cfg,
            command.upgrade,
            WORKER_RENAME_TAG_OUTBOX_REVISION,
        )

        _run_alembic(
            cfg,
            command.upgrade,
            PR_MONITOR_TASK_TOMBSTONE_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        inspector = inspect(engine)
        columns = {
            column["name"]: column
            for column in inspector.get_columns(
                "pr_monitor_task_tombstones"
            )
        }
        assert set(columns) == {"task_id", "created_at"}
        assert columns["task_id"]["nullable"] is False
        assert columns["created_at"]["nullable"] is False
        assert inspector.get_pk_constraint(
            "pr_monitor_task_tombstones"
        )["constrained_columns"] == ["task_id"]
        assert inspector.get_foreign_keys(
            "pr_monitor_task_tombstones"
        ) == []
        engine.dispose()

        _run_alembic(
            cfg,
            command.downgrade,
            WORKER_RENAME_TAG_OUTBOX_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        assert not inspect(engine).has_table("pr_monitor_task_tombstones")
        engine.dispose()

        _run_alembic(
            cfg,
            command.upgrade,
            PR_MONITOR_TASK_TOMBSTONE_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        assert inspect(engine).has_table("pr_monitor_task_tombstones")
        engine.dispose()

    def test_upgrade_replays_create_before_revision_stamp(self, tmp_path):
        from backend.models.pr_monitor import PRMonitorTaskTombstone

        db_path = str(tmp_path / "pr-monitor-tombstone-create-crash.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(
            cfg,
            command.upgrade,
            WORKER_RENAME_TAG_OUTBOX_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        PRMonitorTaskTombstone.__table__.create(engine)
        engine.dispose()

        _run_alembic(
            cfg,
            command.upgrade,
            PR_MONITOR_TASK_TOMBSTONE_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == PR_MONITOR_TASK_TOMBSTONE_REVISION
        engine.dispose()

    def test_downgrade_replays_drop_before_revision_stamp(self, tmp_path):
        db_path = str(tmp_path / "pr-monitor-tombstone-drop-crash.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(
            cfg,
            command.upgrade,
            PR_MONITOR_TASK_TOMBSTONE_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE pr_monitor_task_tombstones"))
        engine.dispose()

        _run_alembic(
            cfg,
            command.downgrade,
            WORKER_RENAME_TAG_OUTBOX_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == WORKER_RENAME_TAG_OUTBOX_REVISION
        assert not inspect(engine).has_table("pr_monitor_task_tombstones")
        engine.dispose()

    def test_downgrade_refuses_durable_identity(self, tmp_path):
        db_path = str(tmp_path / "pr-monitor-tombstone-nonempty.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(
            cfg,
            command.upgrade,
            PR_MONITOR_TASK_TOMBSTONE_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO pr_monitor_task_tombstones (task_id) VALUES (41)"
            ))
        engine.dispose()

        with pytest.raises(RuntimeError, match="tombstones exist"):
            _run_alembic(
                cfg,
                command.downgrade,
                WORKER_RENAME_TAG_OUTBOX_REVISION,
            )
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT task_id FROM pr_monitor_task_tombstones"
            )).scalar_one() == 41
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == PR_MONITOR_TASK_TOMBSTONE_REVISION
        engine.dispose()

    def test_model_ddl_is_portable_and_deliberately_fk_free(self):
        from backend.models.pr_monitor import PRMonitorTaskTombstone

        table = PRMonitorTaskTombstone.__table__
        assert list(table.primary_key.columns.keys()) == ["task_id"]
        assert list(table.foreign_keys) == []
        for dialect in (
            sqlite.dialect(),
            postgresql.dialect(),
            mysql.dialect(),
        ):
            ddl = str(CreateTable(table).compile(dialect=dialect)).lower()
            assert "primary key (task_id)" in ddl
            assert "created_at" in ddl
            assert "current_timestamp" in ddl or "now()" in ddl
            assert "foreign key" not in ddl

    def test_postgresql_offline_upgrade_emits_canonical_table(self):
        module = _load_revision_migration(
            self.migration_file,
            "pr_monitor_tombstone_postgresql_offline_upgrade",
        )
        output = io.StringIO()
        context = MigrationContext.configure(
            dialect_name="postgresql",
            opts={"as_sql": True, "output_buffer": output},
        )
        with patch.object(module, "op", Operations(context)):
            module.upgrade()

        ddl = " ".join(output.getvalue().lower().split())
        assert "create table pr_monitor_task_tombstones" in ddl
        assert "task_id serial not null" not in ddl
        assert "primary key (task_id)" in ddl
        assert "created_at timestamp without time zone default now()" in ddl

    def test_mysql_offline_upgrade_fails_closed(self):
        module = _load_revision_migration(
            self.migration_file,
            "pr_monitor_tombstone_mysql_offline_upgrade",
        )
        output = io.StringIO()
        context = MigrationContext.configure(
            dialect_name="mysql",
            opts={"as_sql": True, "output_buffer": output},
        )
        with (
            patch.object(module, "op", Operations(context)),
            pytest.raises(RuntimeError, match="refuses MySQL/MariaDB"),
        ):
            module.upgrade()
        assert output.getvalue() == ""


def _task_migration_values(**overrides):
    values = {
        "operation_id": "a" * 32,
        "operation_sequence": 1,
        "side": "manager",
        "active_task_id": 101,
        "task_id": 101,
        "task_incarnation_id": "b" * 32,
        "retry_count": 0,
        "turn_generation": 0,
        "source_worker_id": None,
        "target_worker_id": 7,
        "source_status": "plan_review",
        "phase": "claimed",
        "instance_id": None,
        "started_at": None,
        "completed_at": None,
        "created_at": datetime(2026, 8, 14),
        "updated_at": datetime(2026, 8, 14),
    }
    values.update(overrides)
    return values


class TestTaskMigrationOperationMigration:
    migration_file = (
        "a3d9b5f7c1e4_add_task_migration_operations.py"
    )

    def test_task_migration_upgrade_creates_canonical_table_and_accepts_all_values(
        self,
        tmp_path,
    ):
        from backend.models.task_migration import (
            TASK_MIGRATION_PHASES,
            TASK_MIGRATION_SOURCE_STATUSES,
            TaskMigrationOperation,
        )

        db_path = str(tmp_path / "task-migration-operation.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, TASK_MIGRATION_OPERATION_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        inspector = inspect(engine)

        assert set(inspector.get_table_names()) >= {
            "task_migration_operations"
        }
        assert {
            column["name"] for column in inspector.get_columns(
                "task_migration_operations"
            )
        } == {
            "operation_id",
            "operation_sequence",
            "side",
            "active_task_id",
            "task_id",
            "task_incarnation_id",
            "retry_count",
            "turn_generation",
            "source_worker_id",
            "target_worker_id",
            "source_status",
            "phase",
            "instance_id",
            "started_at",
            "completed_at",
            "created_at",
            "updated_at",
        }
        assert tuple(
            inspector.get_pk_constraint(
                "task_migration_operations"
            )["constrained_columns"]
        ) == ("operation_id",)
        assert {
            item["name"]: tuple(item["column_names"])
            for item in inspector.get_unique_constraints(
                "task_migration_operations"
            )
        } == {
            "uq_task_migration_active_task": ("active_task_id",),
            "uq_task_migration_task_sequence": (
                "task_id",
                "operation_sequence",
            ),
        }
        assert inspector.get_foreign_keys("task_migration_operations") == []
        assert {
            item["name"]
            for item in inspector.get_check_constraints(
                "task_migration_operations"
            )
        } == {
            "ck_task_migration_active_slot",
            "ck_task_migration_generation",
            "ck_task_migration_identity",
            "ck_task_migration_incarnation_hex",
            "ck_task_migration_operation_id_hex",
            "ck_task_migration_phase",
            "ck_task_migration_route",
            "ck_task_migration_side_phase",
            "ck_task_migration_source_status",
        }

        routes = ((None, 7), (8, None), (8, 9))
        count = max(
            len(TASK_MIGRATION_PHASES),
            len(TASK_MIGRATION_SOURCE_STATUSES),
        )
        rows = []
        for index in range(count):
            source_worker_id, target_worker_id = routes[index % len(routes)]
            task_id = 101 + index
            phase = TASK_MIGRATION_PHASES[index % len(TASK_MIGRATION_PHASES)]
            side = "worker" if phase == "prepared" else "manager"
            if side == "worker":
                source_worker_id = None
                target_worker_id = None
            rows.append(_task_migration_values(
                operation_id=f"{index + 1:032x}",
                operation_sequence=index + 1,
                side=side,
                active_task_id=(
                    None
                    if phase in {"committed", "rolled_back"}
                    else task_id
                ),
                task_id=task_id,
                task_incarnation_id=f"{index + 11:032x}",
                source_worker_id=source_worker_id,
                target_worker_id=target_worker_id,
                source_status=TASK_MIGRATION_SOURCE_STATUSES[
                    index % len(TASK_MIGRATION_SOURCE_STATUSES)
                ],
                phase=phase,
            ))
        with engine.begin() as conn:
            conn.execute(TaskMigrationOperation.__table__.insert(), rows)
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT COUNT(*) FROM task_migration_operations"
            )).scalar_one() == count
        engine.dispose()

    def test_task_migration_unique_active_task_slot_is_enforced(self, tmp_path):
        from backend.models.task_migration import TaskMigrationOperation

        db_path = str(tmp_path / "task-migration-unique.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, TASK_MIGRATION_OPERATION_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(
                TaskMigrationOperation.__table__.insert().values(
                    **_task_migration_values()
                )
            )
        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(
                    TaskMigrationOperation.__table__.insert().values(
                        **_task_migration_values(
                            operation_id="c" * 32,
                            operation_sequence=2,
                            task_incarnation_id="d" * 32,
                        )
                    )
                )
        engine.dispose()

    def test_task_migration_unique_task_sequence_is_enforced(self, tmp_path):
        from backend.models.task_migration import TaskMigrationOperation

        db_path = str(tmp_path / "task-migration-sequence-unique.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, TASK_MIGRATION_OPERATION_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(
                TaskMigrationOperation.__table__.insert().values(
                    **_task_migration_values(
                        active_task_id=None,
                        phase="rolled_back",
                    )
                )
            )
        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(
                    TaskMigrationOperation.__table__.insert().values(
                        **_task_migration_values(
                            operation_id="c" * 32,
                            active_task_id=None,
                            phase="committed",
                        )
                    )
                )
        engine.dispose()

    @pytest.mark.parametrize(
        ("case", "overrides"),
        [
            ("short-operation", {"operation_id": "a" * 31}),
            ("uppercase-operation", {"operation_id": "A" * 32}),
            (
                "non-hex-incarnation",
                {"task_incarnation_id": "g" * 32},
            ),
            ("foreign-phase", {"phase": "settled"}),
            ("foreign-side", {"side": "relay"}),
            ("foreign-source-status", {"source_status": "executing"}),
            ("zero-operation-sequence", {"operation_sequence": 0}),
            ("negative-retry", {"retry_count": -1}),
            ("negative-generation", {"turn_generation": -1}),
            ("invalid-task", {"task_id": 0, "active_task_id": 0}),
            ("invalid-instance", {"instance_id": 0}),
            (
                "invalid-worker",
                {"source_worker_id": 0, "target_worker_id": None},
            ),
            (
                "same-worker",
                {"source_worker_id": 7, "target_worker_id": 7},
            ),
            (
                "both-local",
                {"source_worker_id": None, "target_worker_id": None},
            ),
            ("missing-active-slot", {"active_task_id": None}),
            ("wrong-active-slot", {"active_task_id": 102}),
            (
                "manager-prepared",
                {"side": "manager", "phase": "prepared"},
            ),
            (
                "worker-claimed",
                {
                    "side": "worker",
                    "phase": "claimed",
                    "source_worker_id": None,
                    "target_worker_id": None,
                },
            ),
            (
                "terminal-active-slot",
                {"phase": "committed", "active_task_id": 101},
            ),
        ],
    )
    def test_task_migration_constraints_reject_invalid_operations(
        self,
        tmp_path,
        case,
        overrides,
    ):
        from backend.models.task_migration import TaskMigrationOperation

        db_path = str(tmp_path / f"task-migration-invalid-{case}.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, TASK_MIGRATION_OPERATION_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(
                    TaskMigrationOperation.__table__.insert().values(
                        **_task_migration_values(**overrides)
                    )
                )
        engine.dispose()

    def test_task_migration_upgrade_replays_create_before_revision_stamp(
        self,
        tmp_path,
    ):
        from backend.models.task_migration import TaskMigrationOperation

        db_path = str(tmp_path / "task-migration-create-crash.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, WORKER_NODE_DRAIN_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        TaskMigrationOperation.__table__.create(engine)
        engine.dispose()

        _run_alembic(cfg, command.upgrade, TASK_MIGRATION_OPERATION_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == TASK_MIGRATION_OPERATION_REVISION
        engine.dispose()

    def test_task_migration_upgrade_rejects_foreign_existing_table(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "task-migration-foreign-table.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, WORKER_NODE_DRAIN_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text(
                "CREATE TABLE task_migration_operations "
                "(operation_id VARCHAR(32) PRIMARY KEY)"
            ))
        engine.dispose()

        with pytest.raises(RuntimeError, match="foreign column set"):
            _run_alembic(
                cfg,
                command.upgrade,
                TASK_MIGRATION_OPERATION_REVISION,
            )
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == WORKER_NODE_DRAIN_REVISION
        engine.dispose()

    def test_task_migration_downgrade_replays_drop_before_revision_stamp(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "task-migration-drop-crash.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, TASK_MIGRATION_OPERATION_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE task_migration_operations"))
        engine.dispose()

        _run_alembic(cfg, command.downgrade, WORKER_NODE_DRAIN_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "task_migration_operations" not in inspect(
            engine
        ).get_table_names()
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == WORKER_NODE_DRAIN_REVISION
        engine.dispose()

    def test_task_migration_downgrade_refuses_rows_then_replays_both_directions(
        self,
        tmp_path,
    ):
        from backend.models.task_migration import TaskMigrationOperation

        db_path = str(tmp_path / "task-migration-downgrade.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, TASK_MIGRATION_OPERATION_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(
                TaskMigrationOperation.__table__.insert().values(
                    **_task_migration_values()
                )
            )
        engine.dispose()

        with pytest.raises(
            RuntimeError,
            match="Task migration operations exist",
        ):
            _run_alembic(
                cfg,
                command.downgrade,
                WORKER_NODE_DRAIN_REVISION,
            )
        engine = create_engine(f"sqlite:///{db_path}")
        assert "task_migration_operations" in inspect(engine).get_table_names()
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == TASK_MIGRATION_OPERATION_REVISION
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM task_migration_operations"))
        engine.dispose()

        _run_alembic(cfg, command.downgrade, WORKER_NODE_DRAIN_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "task_migration_operations" not in inspect(
            engine
        ).get_table_names()
        engine.dispose()

        _run_alembic(cfg, command.upgrade, TASK_MIGRATION_OPERATION_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "task_migration_operations" in inspect(engine).get_table_names()
        engine.dispose()

    @pytest.mark.parametrize("initial_state", ("canonical", "canonical_gated"))
    def test_task_migration_mysql_downgrade_gate_precedes_drop_and_replays(
        self,
        initial_state,
    ):
        module = _load_revision_migration(
            self.migration_file,
            f"task_migration_mysql_gate_{initial_state}",
        )
        states = iter(
            ("canonical", "canonical_gated", "absent")
            if initial_state == "canonical"
            else ("canonical_gated", "absent")
        )
        events: list[str] = []
        with (
            patch.object(module, "_require_supported_mysql_family"),
            patch.object(module, "_is_offline", return_value=False),
            patch.object(module, "_acquire_transactional_fence"),
            patch.object(
                module,
                "_table_state",
                side_effect=lambda **_kwargs: next(states),
            ),
            patch.object(
                module,
                "_assert_empty",
                side_effect=lambda: events.append("empty"),
            ),
            patch.object(module, "_is_mysql_family", return_value=True),
            patch.object(
                module.op,
                "create_check_constraint",
                side_effect=lambda *_args, **_kwargs: events.append("gate"),
            ) as create_gate,
            patch.object(
                module.op,
                "drop_table",
                side_effect=lambda *_args, **_kwargs: events.append("drop"),
            ),
        ):
            module.downgrade()

        expected = (
            ["empty", "gate", "empty", "drop"]
            if initial_state == "canonical"
            else ["empty", "empty", "drop"]
        )
        assert events == expected
        if initial_state == "canonical":
            create_gate.assert_called_once_with(
                module._DOWNGRADE_GATE,
                module._TABLE,
                module._DOWNGRADE_GATE_SQL,
            )
        else:
            create_gate.assert_not_called()

    @pytest.mark.parametrize(
        ("dialect_name", "is_mariadb", "version", "required"),
        [
            ("mysql", False, (8, 0, 15), "MySQL 8.0.16"),
            ("mysql", True, (10, 6, 0), "MariaDB 10.6.1"),
        ],
    )
    def test_task_migration_mysql_unsupported_versions_fail_closed(
        self,
        dialect_name,
        is_mariadb,
        version,
        required,
    ):
        module = _load_revision_migration(
            self.migration_file,
            f"task_migration_unsupported_{required}",
        )
        dialect = SimpleNamespace(
            name=dialect_name,
            is_mariadb=is_mariadb,
            server_version_info=version,
        )
        with (
            patch.object(
                module.op,
                "get_bind",
                return_value=SimpleNamespace(dialect=dialect),
            ),
            patch.object(module, "_is_offline", return_value=False),
            pytest.raises(RuntimeError, match=required),
        ):
            module._require_supported_mysql_family()

    @pytest.mark.parametrize(
        ("dialect_name", "is_mariadb", "version"),
        [
            ("mysql", False, (8, 0, 16)),
            ("mariadb", True, (10, 6, 1)),
        ],
    )
    def test_task_migration_mysql_minimum_versions_are_supported(
        self,
        dialect_name,
        is_mariadb,
        version,
    ):
        module = _load_revision_migration(
            self.migration_file,
            f"task_migration_supported_{dialect_name}",
        )
        dialect = SimpleNamespace(
            name=dialect_name,
            is_mariadb=is_mariadb,
            server_version_info=version,
        )
        with (
            patch.object(
                module.op,
                "get_bind",
                return_value=SimpleNamespace(dialect=dialect),
            ),
            patch.object(module, "_is_offline", return_value=False),
        ):
            module._require_supported_mysql_family()

    def test_task_migration_offline_downgrades_fail_closed(self):
        module = _load_revision_migration(
            self.migration_file,
            "task_migration_offline",
        )
        dialect = SimpleNamespace(
            name="mysql",
            is_mariadb=False,
            server_version_info=(8, 0, 36),
        )
        with (
            patch.object(
                module.op,
                "get_bind",
                return_value=SimpleNamespace(dialect=dialect),
            ),
            patch.object(module, "_is_offline", return_value=True),
            pytest.raises(RuntimeError, match="refuses MySQL/MariaDB offline"),
        ):
            module._require_supported_mysql_family()

        with (
            patch.object(module, "_require_supported_mysql_family"),
            patch.object(module, "_is_offline", return_value=True),
            patch.object(module.op, "drop_table") as drop,
            pytest.raises(RuntimeError, match="offline downgrade is refused"),
        ):
            module.downgrade()
        drop.assert_not_called()

    def test_task_migration_postgresql_downgrade_takes_exclusive_lock(self):
        module = _load_revision_migration(
            self.migration_file,
            "task_migration_postgresql_lock",
        )
        bind = SimpleNamespace(
            dialect=SimpleNamespace(name="postgresql")
        )
        inspector = MagicMock()
        inspector.has_table.return_value = True
        with (
            patch.object(module, "_is_offline", return_value=False),
            patch.object(module.op, "get_bind", return_value=bind),
            patch.object(module.sa, "inspect", return_value=inspector),
            patch.object(module.op, "execute") as execute,
        ):
            module._acquire_transactional_fence(downgrade=True)

        inspector.has_table.assert_called_once_with(module._TABLE)
        assert str(execute.call_args.args[0]) == (
            "LOCK TABLE task_migration_operations IN ACCESS EXCLUSIVE MODE"
        )


def _load_terminal_arbitration_migration(module_suffix: str = "test"):
    migration_path = (
        PROJECT_ROOT
        / "alembic"
        / "versions"
        / "4b8d2f6a1c90_add_terminal_arbitration_identity.py"
    )
    spec = importlib.util.spec_from_file_location(
        f"terminal_arbitration_migration_{module_suffix}",
        migration_path,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_worker_task_delete_receipt_migration(module_suffix: str = "test"):
    migration_path = (
        PROJECT_ROOT
        / "alembic"
        / "versions"
        / "b7f3d1a8c920_add_worker_task_delete_receipts.py"
    )
    spec = importlib.util.spec_from_file_location(
        f"worker_task_delete_receipt_migration_{module_suffix}",
        migration_path,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_worker_plan_import_receipt_migration(module_suffix: str = "test"):
    migration_path = (
        PROJECT_ROOT
        / "alembic"
        / "versions"
        / "d3c8a7f1e620_add_worker_plan_import_receipts.py"
    )
    spec = importlib.util.spec_from_file_location(
        f"worker_plan_import_receipt_migration_{module_suffix}",
        migration_path,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_capability_resume_outbox_migration(module_suffix: str = "test"):
    migration_path = (
        PROJECT_ROOT
        / "alembic"
        / "versions"
        / "7c1e4a9d2f60_add_capability_resume_outbox.py"
    )
    spec = importlib.util.spec_from_file_location(
        f"capability_resume_outbox_migration_{module_suffix}",
        migration_path,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_task_incarnation_migration(module_suffix: str = "test"):
    migration_path = (
        PROJECT_ROOT
        / "alembic"
        / "versions"
        / "b4e7c1a9d2f0_backfill_task_incarnations_for_scoped_tokens.py"
    )
    spec = importlib.util.spec_from_file_location(
        f"task_incarnation_migration_{module_suffix}",
        migration_path,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_task_ssh_effect_migration(module_suffix: str = "test"):
    migration_path = (
        PROJECT_ROOT
        / "alembic"
        / "versions"
        / "c2f8a6d4e1b9_add_task_ssh_effect_receipts.py"
    )
    spec = importlib.util.spec_from_file_location(
        f"task_ssh_effect_migration_{module_suffix}",
        migration_path,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_revision_migration(filename: str, module_suffix: str = "test"):
    migration_path = PROJECT_ROOT / "alembic" / "versions" / filename
    spec = importlib.util.spec_from_file_location(
        f"revision_migration_{migration_path.stem}_{module_suffix}",
        migration_path,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_browser_operation_receipt_migration(
    module_suffix: str = "test",
):
    return _load_revision_migration(
        "6f3b9d2a7c10_add_browser_operation_receipts.py",
        module_suffix,
    )


def _load_pr_review_merge_method_migration(
    module_suffix: str = "test",
):
    return _load_revision_migration(
        "1a7c9e4d2b60_freeze_pr_review_merge_method.py",
        module_suffix,
    )


def _load_pr_review_base_ref_migration(
    module_suffix: str = "test",
):
    return _load_revision_migration(
        "2b8d4f6a1c90_freeze_pr_review_base_ref.py",
        module_suffix,
    )


def _load_pr_review_result_evidence_migration(
    module_suffix: str = "test",
):
    return _load_revision_migration(
        "b1d7e4a9c302_add_pr_review_result_evidence.py",
        module_suffix,
    )


def _mysql_terminal_state(
    *,
    canonical: str | None,
    shadow: str | None = None,
    gate: bool = False,
    columns: set[str] | None = None,
    unique: bool = False,
) -> dict[str, object]:
    return {
        "columns": set(columns or ()),
        "column_shapes": {
            "request_reason": True,
            "request_protocol_version": True,
            "request_output_hash": True,
        },
        "unique": unique,
        "unique_present": unique,
        "canonical": canonical,
        "canonical_present": canonical is not None,
        "canonical_enforced": canonical is not None,
        "shadow": shadow,
        "shadow_present": shadow is not None,
        "shadow_enforced": shadow is not None,
        "gate": gate,
        "gate_present": gate,
        "gate_enforced": gate,
    }


def _mysql_auxiliary_state(
    *,
    task_source: bool = True,
    log_columns: bool = True,
    task_gate: bool = False,
    log_gate: bool = False,
) -> dict[str, object]:
    return {
        "task_source_present": task_source,
        "task_source_shape": True,
        "task_gate": task_gate,
        "task_gate_present": task_gate,
        "task_gate_enforced": task_gate,
        "turn_scope_present": log_columns,
        "turn_scope_shape": True,
        "actual_transport_present": log_columns,
        "actual_transport_shape": True,
        "scope_check": log_columns,
        "scope_check_present": log_columns,
        "scope_check_enforced": log_columns,
        "transport_check": log_columns,
        "transport_check_present": log_columns,
        "transport_check_enforced": log_columns,
        "log_gate": log_gate,
        "log_gate_present": log_gate,
        "log_gate_enforced": log_gate,
    }


def _get_table_columns(engine, table_name: str) -> dict[str, str]:
    """Return {column_name: column_type_str} for a table."""
    insp = inspect(engine)
    if table_name not in insp.get_table_names():
        return {}
    cols = insp.get_columns(table_name)
    return {c["name"]: str(c["type"]) for c in cols}


def _get_all_tables(engine) -> set[str]:
    """Return set of all user table names (excluding alembic_version)."""
    insp = inspect(engine)
    return {t for t in insp.get_table_names() if t != "alembic_version"}


def _create_legacy_db(db_path: str):
    """Create a legacy database matching the backup structure (no alembic_version,
    no loop-task columns). This mirrors claude_manager_backup_20260307_2.db."""
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE instances (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(100) NOT NULL,
                pid INTEGER,
                status VARCHAR(20),
                current_task_id INTEGER,
                worktree_path VARCHAR(500),
                worktree_branch VARCHAR(100),
                model VARCHAR(50),
                total_tasks_completed INTEGER,
                total_cost_usd FLOAT,
                config JSON,
                started_at DATETIME,
                last_heartbeat DATETIME
            )
        """))
        conn.execute(text("""
            CREATE TABLE projects (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(100) NOT NULL UNIQUE,
                git_url VARCHAR(500),
                has_remote BOOLEAN,
                local_path VARCHAR(500),
                default_branch VARCHAR(100),
                status VARCHAR(20),
                error_message VARCHAR(1000),
                created_at DATETIME
            )
        """))
        conn.execute(text("""
            CREATE TABLE tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title VARCHAR(200) NOT NULL,
                description TEXT NOT NULL,
                status VARCHAR(20) NOT NULL,
                priority INTEGER NOT NULL,
                project_id INTEGER,
                target_repo VARCHAR(500),
                target_branch VARCHAR(100),
                result_branch VARCHAR(100),
                merge_status VARCHAR(20),
                instance_id INTEGER,
                retry_count INTEGER,
                max_retries INTEGER,
                mode VARCHAR(20),
                plan_content TEXT,
                plan_approved BOOLEAN,
                session_id VARCHAR(200),
                last_cwd VARCHAR(500),
                error_message TEXT,
                tags JSON,
                metadata JSON,
                created_at DATETIME,
                started_at DATETIME,
                completed_at DATETIME
            )
        """))
        conn.execute(text("CREATE INDEX ix_tasks_status ON tasks (status)"))
        conn.execute(text("CREATE INDEX ix_tasks_priority ON tasks (priority)"))
        conn.execute(text("CREATE INDEX ix_tasks_project_id ON tasks (project_id)"))
        conn.execute(text("""
            CREATE TABLE log_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                instance_id INTEGER NOT NULL,
                task_id INTEGER,
                event_type VARCHAR(50) NOT NULL,
                role VARCHAR(20),
                content TEXT,
                tool_name VARCHAR(100),
                tool_input TEXT,
                tool_output TEXT,
                raw_json TEXT,
                is_error BOOLEAN,
                timestamp DATETIME
            )
        """))
        conn.execute(text("CREATE INDEX ix_log_entries_instance_id ON log_entries (instance_id)"))
        conn.execute(text("CREATE INDEX ix_log_entries_task_id ON log_entries (task_id)"))
        conn.execute(text("CREATE INDEX ix_log_entries_event_type ON log_entries (event_type)"))
        conn.execute(text("""
            CREATE TABLE worktrees (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                repo_path VARCHAR(500) NOT NULL,
                worktree_path VARCHAR(500) NOT NULL UNIQUE,
                branch_name VARCHAR(100) NOT NULL,
                base_branch VARCHAR(100),
                instance_id INTEGER,
                status VARCHAR(20),
                created_at DATETIME,
                removed_at DATETIME
            )
        """))
        # Insert a sample row so we can verify data survives migration
        conn.execute(text(
            "INSERT INTO tasks (title, description, status, priority, mode, created_at) "
            "VALUES ('test task', 'test desc', 'pending', 0, 'auto', '2026-01-01 00:00:00')"
        ))
    engine.dispose()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestLegacyMigration:
    """A legacy database (pre-Alembic) can be migrated to head."""

    def test_legacy_db_upgrades_successfully(self, tmp_path):
        """init_db logic: stamp initial, then upgrade to head."""
        db_path = str(tmp_path / "legacy.db")
        _create_legacy_db(db_path)

        cfg = _alembic_cfg(db_path)

        # Simulate init_db() logic for legacy DB:
        # stamp the initial revision, then upgrade to head
        _run_alembic(cfg, command.stamp, "6b3f8a1c2d9e")
        _run_alembic(cfg, command.upgrade, "head")

        # Verify alembic_version is at head
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version_num FROM alembic_version"))
            version = result.scalar()
            assert version == _get_head_revision(cfg), f"Expected head revision, got {version}"

        # Verify new columns exist
        task_cols = _get_table_columns(engine, "tasks")
        assert "todo_file_path" in task_cols
        assert "loop_progress" in task_cols
        assert "max_iterations" in task_cols
        assert "context_window_usage" in task_cols
        assert "attention_tag" in task_cols
        assert "delivery_run_id" in task_cols
        assert "delivery_role" in task_cols

        instance_cols = _get_table_columns(engine, "instances")
        assert instance_cols["process_identity"] == "VARCHAR(128)"
        instance_checks = {
            item["name"]
            for item in inspect(engine).get_check_constraints("instances")
        }
        assert "ck_instances_process_identity_requires_pid" in instance_checks
        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "INSERT INTO instances (name, pid, process_identity) "
                        "VALUES ('invalid-identity', NULL, 'v1:123:456:"
                        "11111111-1111-4111-8111-111111111111')"
                    )
                )
        assert "turn_generation" in task_cols
        assert "capability_policy" in task_cols

        log_cols = _get_table_columns(engine, "log_entries")
        assert "loop_iteration" in log_cols
        assert "task_retry_count" in log_cols
        assert "turn_scope" in log_cols
        assert "actual_transport" in log_cols
        assert "task_turn_generation" in log_cols
        assert "native_turn_id" in log_cols

        plan_step_cols = _get_table_columns(engine, "plan_agent_steps")
        assert "last_delta_at" in plan_step_cols
        assert "streamed_output_chars" in plan_step_cols
        assert "last_event_type" in plan_step_cols
        assert "capability_execution_id" in _get_table_columns(
            engine, "plan_agent_runs"
        )

        worktree_cols = _get_table_columns(engine, "worktrees")
        assert "delivery_run_id" in worktree_cols
        assert "cleanup_status" in worktree_cols
        assert "delivery_runs" in _get_all_tables(engine)

        project_cols = _get_table_columns(engine, "projects")
        assert "sort_order" in project_cols
        assert "tags" in project_cols

        pr_review_cols = _get_table_columns(engine, "pr_reviews")
        assert "base_ref" in pr_review_cols
        assert "base_sha" in pr_review_cols
        assert "head_sha" in pr_review_cols
        assert "delivery_id" in pr_review_cols
        assert "merge_method" in pr_review_cols

        # Verify existing data survived
        with engine.connect() as conn:
            result = conn.execute(text("SELECT title FROM tasks WHERE id = 1"))
            assert result.scalar() == "test task"

        engine.dispose()

    def test_legacy_db_data_preserved(self, tmp_path):
        """Migration preserves all existing data including nullable new columns."""
        db_path = str(tmp_path / "legacy_data.db")
        _create_legacy_db(db_path)

        # Insert more data
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO log_entries (instance_id, task_id, event_type, content, timestamp) "
                "VALUES (1, 1, 'message', 'hello', '2026-01-01 00:00:00')"
            ))
        engine.dispose()

        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.stamp, "6b3f8a1c2d9e")
        _run_alembic(cfg, command.upgrade, "head")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            # New nullable columns default to NULL for existing rows
            row = conn.execute(text("SELECT todo_file_path, loop_progress FROM tasks WHERE id = 1")).fetchone()
            assert row[0] is None
            assert row[1] is None

            # max_iterations has server_default=50, so existing rows get 50
            row = conn.execute(text("SELECT max_iterations FROM tasks WHERE id = 1")).fetchone()
            assert row[0] == 50

            row = conn.execute(text("SELECT loop_iteration FROM log_entries WHERE id = 1")).fetchone()
            assert row[0] is None

        engine.dispose()


class TestLegacyDefaultAdminMigration:
    def test_known_seeded_account_is_disabled_and_password_rotated(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "legacy-admin.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, "d8f0a1b2c3d4")

        engine = create_engine(f"sqlite:///{db_path}")
        import bcrypt

        old_hash = bcrypt.hashpw(
            b"admin123456",
            bcrypt.gensalt(),
        ).decode()
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO users "
                    "(email, name, password_hash, role, avatar_url, "
                    "is_active, feishu_open_id, feishu_name, created_at) "
                    "VALUES "
                    "(:email, 'Admin', :password_hash, 'super_admin', "
                    "'', TRUE, '', '', CURRENT_TIMESTAMP)"
                ),
                {
                    "email": "admin@apexin.ai",
                    "password_hash": old_hash,
                },
            )
        engine.dispose()

        _run_alembic(cfg, command.upgrade, "head")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT password_hash, is_active FROM users "
                    "WHERE email = :email"
                ),
                {"email": "admin@apexin.ai"},
            ).one()
        engine.dispose()

        assert row.password_hash != old_hash
        assert bool(row.is_active) is False

    def test_changed_legacy_admin_password_is_preserved(self, tmp_path):
        import bcrypt

        db_path = str(tmp_path / "changed-legacy-admin.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, "d8f0a1b2c3d4")

        changed_hash = bcrypt.hashpw(
            b"a-deployment-owned-password",
            bcrypt.gensalt(),
        ).decode()
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO users "
                    "(email, name, password_hash, role, avatar_url, "
                    "is_active, feishu_open_id, feishu_name, created_at) "
                    "VALUES "
                    "(:email, 'Admin', :password_hash, 'super_admin', "
                    "'', TRUE, '', '', CURRENT_TIMESTAMP)"
                ),
                {
                    "email": "admin@apexin.ai",
                    "password_hash": changed_hash,
                },
            )
        engine.dispose()

        _run_alembic(cfg, command.upgrade, "head")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT password_hash, is_active FROM users "
                    "WHERE email = :email"
                ),
                {"email": "admin@apexin.ai"},
            ).one()
        engine.dispose()

        assert row.password_hash == changed_hash
        assert bool(row.is_active) is True


class TestCodexServiceTierMigration:
    def test_existing_tasks_are_backfilled_as_standard(self, tmp_path):
        db_path = str(tmp_path / "codex-service-tier.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, "e4c9f2a71b03")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO tasks "
                "(title, description, status, priority, target_branch, "
                "merge_status, retry_count, max_retries, mode, created_at) "
                "VALUES "
                "('existing task', 'd', 'pending', 0, 'main', 'pending', "
                "0, 2, 'auto', '2026-07-28 00:00:00')"
            ))
        engine.dispose()

        _run_alembic(cfg, command.upgrade, "head")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            tier = conn.execute(text(
                "SELECT codex_service_tier FROM tasks "
                "WHERE title = 'existing task'"
            )).scalar_one()
            assert tier == "default"

            column = inspect(conn).get_columns("tasks")
            column = next(
                item for item in column
                if item["name"] == "codex_service_tier"
            )
            assert column["nullable"] is False
        engine.dispose()


class TestDeliveryLoopMigration:
    def test_upgrade_closes_task_id_reuse_and_purges_stale_acl(self, tmp_path):
        db_path = str(tmp_path / "delivery-task-id-aba.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, CODE_REVIEW_REVISION)

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO tasks "
                "(id, title, description, status, priority, target_branch, "
                "merge_status, retry_count, max_retries, mode, shared_from_id, "
                "created_at) "
                "VALUES (10, 'current task', 'd', 'pending', 0, 'main', "
                "'pending', 0, 2, 'auto', 55, '2026-08-05 00:00:00')"
            ))
            conn.execute(text(
                "INSERT INTO task_shares "
                "(task_id, shared_to_open_id, shared_to_ccm_url, share_token, "
                "status, created_at) VALUES "
                "(10, 'stale', 'https://old.example', 'stale-token', 'active', "
                "'2026-08-04 00:00:00'), "
                "(10, 'current', 'https://new.example', 'current-token', "
                "'active', '2026-08-06 00:00:00'), "
                "(25, 'orphan', 'https://old.example', 'orphan-token', "
                "'active', '2026-08-01 00:00:00')"
            ))
            conn.execute(text(
                "INSERT INTO team_task_shares "
                "(task_id, target_type, target_id, permission, shared_by, "
                "created_at) VALUES "
                "(10, 'user', 1, 'chat', 1, '2026-08-04 00:00:00'), "
                "(30, 'user', 2, 'chat', 1, '2026-08-01 00:00:00')"
            ))
            conn.execute(text(
                "INSERT INTO shared_tasks_received "
                "(owner_ccm_url, remote_task_id, share_token, local_task_id, "
                "status, received_at) VALUES "
                "('https://owner.example', 9, 'relay-token', 40, 'active', "
                "'2026-08-01 00:00:00')"
            ))
        engine.dispose()

        _run_alembic(cfg, command.upgrade, DELIVERY_LOOP_REVISION)

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            task_share_tokens = conn.execute(text(
                "SELECT share_token FROM task_shares ORDER BY share_token"
            )).scalars().all()
            assert task_share_tokens == ["current-token"]
            assert conn.execute(text(
                "SELECT COUNT(*) FROM team_task_shares"
            )).scalar_one() == 0
            task_ddl = conn.execute(text(
                "SELECT sql FROM sqlite_master "
                "WHERE type = 'table' AND name = 'tasks'"
            )).scalar_one()
            assert "AUTOINCREMENT" in task_ddl.upper()
            shared_ddl = conn.execute(text(
                "SELECT sql FROM sqlite_master WHERE type = 'table' "
                "AND name = 'shared_tasks_received'"
            )).scalar_one()
            assert "AUTOINCREMENT" in shared_ddl.upper()
            assert conn.execute(text(
                "SELECT incarnation_id FROM tasks WHERE id = 10"
            )).scalar_one() is None
            unique_names = {
                item["name"]
                for item in inspect(conn).get_unique_constraints("tasks")
            }
            assert "uq_tasks_incarnation_id" in unique_names

            conn.execute(text(
                "INSERT INTO shared_tasks_received "
                "(owner_ccm_url, remote_task_id, share_token, status, received_at) "
                "VALUES ('https://new-owner.example', 10, 'new-relay-token', "
                "'active', '2026-08-07 00:00:00')"
            ))
            new_shared_id = conn.execute(text(
                "SELECT id FROM shared_tasks_received "
                "WHERE share_token = 'new-relay-token'"
            )).scalar_one()
            assert new_shared_id > 55

            conn.execute(text("DELETE FROM tasks WHERE id = 10"))
            conn.execute(text(
                "INSERT INTO tasks "
                "(title, description, status, priority, target_branch, "
                "merge_status, retry_count, max_retries, mode, created_at) "
                "VALUES ('new task', 'd', 'pending', 0, 'main', 'pending', "
                "0, 2, 'auto', '2026-08-07 00:00:00')"
            ))
            new_id = conn.execute(text(
                "SELECT id FROM tasks WHERE title = 'new task'"
            )).scalar_one()
            assert new_id > 40
        engine.dispose()

    def test_upgrade_from_code_review_head_preserves_existing_rows(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "delivery-existing.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, CODE_REVIEW_REVISION)

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO tasks "
                "(title, description, status, priority, target_branch, "
                "merge_status, retry_count, max_retries, mode, created_at) "
                "VALUES ('existing auto task', 'd', 'pending', 0, 'main', "
                "'pending', 0, 2, 'auto', '2026-08-05 00:00:00')"
            ))
            conn.execute(text(
                "INSERT INTO worktrees "
                "(repo_path, worktree_path, branch_name, base_branch, status, "
                "created_at) VALUES ('/repo', '/repo-wt', 'feature', 'main', "
                "'active', '2026-08-05 00:00:00')"
            ))
            conn.execute(text(
                "INSERT INTO plan_agent_runs "
                "(run_type, current_stage, generation, interaction_count, "
                "max_interactions, execution_seconds, status, round, "
                "review_exhausted, created_at, updated_at) VALUES "
                "('legacy', 'planner', 0, 0, 3, 0, 'completed', 1, 0, "
                "'2026-08-05 00:00:00', '2026-08-05 00:00:00')"
            ))
        engine.dispose()

        _run_alembic(cfg, command.upgrade, DELIVERY_LOOP_REVISION)

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            task_owner = conn.execute(text(
                "SELECT delivery_run_id, delivery_role FROM tasks "
                "WHERE title = 'existing auto task'"
            )).one()
            assert task_owner == (None, None)

            worktree_owner = conn.execute(text(
                "SELECT task_id, delivery_run_id, last_verified_head, "
                "cleanup_status FROM worktrees WHERE worktree_path = '/repo-wt'"
            )).one()
            assert worktree_owner == (None, None, None, "retained")

            capability_execution_id = conn.execute(text(
                "SELECT capability_execution_id FROM plan_agent_runs"
            )).scalar_one()
            assert capability_execution_id is None

        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(text(
                    "UPDATE tasks SET delivery_run_id = 99 "
                    "WHERE title = 'existing auto task'"
                ))

        with engine.begin() as conn:
            conn.execute(text(
                "UPDATE tasks SET mode = 'delivery_loop', delivery_run_id = 99, "
                "delivery_role = 'developer' "
                "WHERE title = 'existing auto task'"
            ))
        engine.dispose()

    def test_delivery_revision_downgrades_and_reupgrades(self, tmp_path):
        db_path = str(tmp_path / "delivery-roundtrip.db")
        cfg = _alembic_cfg(db_path)

        _run_alembic(cfg, command.upgrade, DELIVERY_LOOP_REVISION)
        _run_alembic(cfg, command.downgrade, CODE_REVIEW_REVISION)

        engine = create_engine(f"sqlite:///{db_path}")
        assert not {
            "delivery_runs",
            "delivery_cycles",
            "delivery_turns",
            "delivery_events",
            "delivery_actions",
            "delivery_transitions",
        }.intersection(_get_all_tables(engine))
        assert "delivery_run_id" not in _get_table_columns(engine, "tasks")
        assert "delivery_run_id" not in _get_table_columns(engine, "worktrees")
        assert "capability_execution_id" not in _get_table_columns(
            engine, "plan_agent_runs"
        )
        assert "incarnation_id" not in _get_table_columns(engine, "tasks")
        with engine.begin() as conn:
            task_ddl = conn.execute(text(
                "SELECT sql FROM sqlite_master "
                "WHERE type = 'table' AND name = 'tasks'"
            )).scalar_one()
            shared_ddl = conn.execute(text(
                "SELECT sql FROM sqlite_master WHERE type = 'table' "
                "AND name = 'shared_tasks_received'"
            )).scalar_one()
            assert "AUTOINCREMENT" in task_ddl.upper()
            assert "AUTOINCREMENT" in shared_ddl.upper()
            conn.execute(text(
                "INSERT INTO tasks "
                "(title, description, status, priority, target_branch, "
                "merge_status, retry_count, max_retries, mode, created_at) "
                "VALUES ('downgrade highest', 'd', 'pending', 0, 'main', "
                "'pending', 0, 2, 'auto', '2026-08-06 00:00:00')"
            ))
            old_id = conn.execute(text(
                "SELECT id FROM tasks WHERE title = 'downgrade highest'"
            )).scalar_one()
            conn.execute(text("DELETE FROM tasks WHERE id = :id"), {"id": old_id})
            conn.execute(text(
                "INSERT INTO tasks "
                "(title, description, status, priority, target_branch, "
                "merge_status, retry_count, max_retries, mode, created_at) "
                "VALUES ('downgrade next', 'd', 'pending', 0, 'main', "
                "'pending', 0, 2, 'auto', '2026-08-06 00:00:01')"
            ))
            new_id = conn.execute(text(
                "SELECT id FROM tasks WHERE title = 'downgrade next'"
            )).scalar_one()
            assert new_id > old_id
        engine.dispose()

        _run_alembic(cfg, command.upgrade, DELIVERY_LOOP_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "delivery_runs" in _get_all_tables(engine)
        assert "delivery_run_id" in _get_table_columns(engine, "tasks")
        assert "delivery_run_id" in _get_table_columns(engine, "worktrees")
        assert "capability_execution_id" in _get_table_columns(
            engine, "plan_agent_runs"
        )
        assert "incarnation_id" in _get_table_columns(engine, "tasks")
        engine.dispose()

    def test_delivery_revision_refuses_downgrade_with_run_history(self, tmp_path):
        db_path = str(tmp_path / "delivery-downgrade-history.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, DELIVERY_LOOP_REVISION)

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO delivery_runs "
                "(admission_scope, idempotency_key, request_hash, project_id, "
                "title, requirements, requirements_hash, "
                "policy_snapshot, policy_hash, base_branch, delivery_branch, "
                "created_at, updated_at) VALUES "
                "('system', 'downgrade-test', :digest, 1, "
                "'retained delivery', 'requirements', :digest, '{}', "
                ":digest, 'main', 'ccm/delivery/1-retained', "
                "'2026-08-05 00:00:00', '2026-08-05 00:00:00')"
            ), {"digest": "a" * 64})
        engine.dispose()

        with pytest.raises(RuntimeError, match="delivery_runs contains history"):
            _run_alembic(cfg, command.downgrade, CODE_REVIEW_REVISION)

        engine = create_engine(f"sqlite:///{db_path}")
        assert "delivery_runs" in _get_all_tables(engine)
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT COUNT(*) FROM delivery_runs"
            )).scalar_one() == 1
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == DELIVERY_LOOP_REVISION
        engine.dispose()

    def test_delivery_revision_refuses_residual_owner_downgrade(self, tmp_path):
        db_path = str(tmp_path / "delivery-downgrade-owners.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, DELIVERY_LOOP_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")

        residues = (
            (
                "tasks",
                "INSERT INTO tasks "
                "(title, description, status, priority, target_branch, "
                "merge_status, retry_count, max_retries, mode, "
                "delivery_run_id, delivery_role, created_at) VALUES "
                "('orphan delivery', 'd', 'cancelled', 0, 'main', "
                "'pending', 0, 2, 'delivery_loop', 91, 'developer', "
                "'2026-08-05 00:00:00')",
                "DELETE FROM tasks WHERE title = 'orphan delivery'",
            ),
            (
                "worktrees",
                "INSERT INTO worktrees "
                "(repo_path, worktree_path, branch_name, base_branch, "
                "delivery_run_id, cleanup_status, status, created_at) VALUES "
                "('/repo', '/repo/delivery-92', 'delivery-92', 'main', 92, "
                "'retained', 'active', '2026-08-05 00:00:00')",
                "DELETE FROM worktrees WHERE delivery_run_id = 92",
            ),
            (
                "plan_agent_runs",
                "INSERT INTO plan_agent_runs "
                "(run_type, current_stage, generation, interaction_count, "
                "max_interactions, execution_seconds, status, round, "
                "review_exhausted, capability_execution_id, created_at, "
                "updated_at) VALUES ('capability', 'planner', 0, 0, 3, 0, "
                "'cancelled', 1, 0, 93, '2026-08-05 00:00:00', "
                "'2026-08-05 00:00:00')",
                "DELETE FROM plan_agent_runs WHERE capability_execution_id = 93",
            ),
        )
        for table_name, insert_sql, delete_sql in residues:
            with engine.begin() as conn:
                conn.execute(text(insert_sql))
            with pytest.raises(RuntimeError, match=table_name):
                _run_alembic(cfg, command.downgrade, CODE_REVIEW_REVISION)
            with engine.begin() as conn:
                conn.execute(text(delete_sql))

        engine.dispose()
        _run_alembic(cfg, command.downgrade, CODE_REVIEW_REVISION)


class TestAutoCapabilityTurnMigration:
    def test_upgrade_downgrade_preserves_rows_and_task_identity(self, tmp_path):
        db_path = str(tmp_path / "auto-capability-turn.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, DELIVERY_LOOP_REVISION)

        digest = "a" * 64
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO tasks "
                "(title, description, status, priority, target_branch, "
                "merge_status, retry_count, max_retries, mode, created_at) "
                "VALUES ('existing exact turn', 'd', 'pending', 0, 'main', "
                "'pending', 0, 2, 'auto', '2026-08-06 00:00:00')"
            ))
            task_id = conn.execute(text(
                "SELECT id FROM tasks WHERE title = 'existing exact turn'"
            )).scalar_one()
            conn.execute(
                text(
                    "INSERT INTO log_entries "
                    "(task_id, event_type, content, is_error, timestamp) "
                    "VALUES (:task_id, 'result', 'existing output', 0, "
                    "'2026-08-06 00:00:01')"
                ),
                {"task_id": task_id},
            )
            log_id = conn.execute(text(
                "SELECT id FROM log_entries WHERE task_id = :task_id"
            ), {"task_id": task_id}).scalar_one()
            conn.execute(
                text(
                    "INSERT INTO capability_invocations "
                    "(task_id, capability_key, source, purpose, status, "
                    "state_version, idempotency_key, input_payload, input_hash, "
                    "subject_kind, subject_ref, subject_hash, executor_kind, "
                    "executor_config, executor_config_hash, policy_snapshot, "
                    "policy_hash, resume_policy, max_attempts, active_task_id, "
                    "created_at, updated_at) VALUES "
                    "(:task_id, 'plan', 'human_request', 'advisory', 'failed', "
                    "1, 'existing-exact-turn', '{}', :digest, "
                    "'task_generation', '{}', :digest, 'plan_agent', '{}', "
                    ":digest, '{}', :digest, 'attach_only', 1, NULL, "
                    "'2026-08-06 00:00:02', '2026-08-06 00:00:02')"
                ),
                {"task_id": task_id, "digest": digest},
            )
            invocation_id = conn.execute(text(
                "SELECT id FROM capability_invocations "
                "WHERE idempotency_key = 'existing-exact-turn'"
            )).scalar_one()
            conn.execute(text(
                "INSERT INTO tasks "
                "(id, title, description, status, priority, target_branch, "
                "merge_status, retry_count, max_retries, mode, created_at) "
                "VALUES (80, 'deleted high-water task', 'd', 'completed', 0, "
                "'main', 'pending', 0, 2, 'auto', '2026-08-06 00:00:03')"
            ))
            conn.execute(text("DELETE FROM tasks WHERE id = 80"))
        engine.dispose()

        _run_alembic(cfg, command.upgrade, AUTO_CAPABILITY_TURN_REVISION)

        engine = create_engine(f"sqlite:///{db_path}")
        columns = {
            table: {
                column["name"]: column
                for column in inspect(engine).get_columns(table)
            }
            for table in ("tasks", "log_entries", "capability_invocations")
        }
        assert "BIGINT" in str(columns["tasks"]["turn_generation"]["type"]).upper()
        assert columns["tasks"]["turn_generation"]["nullable"] is False
        assert columns["tasks"]["turn_generation"]["default"] is not None
        assert columns["tasks"]["capability_policy"]["nullable"] is True
        assert "BIGINT" in str(
            columns["log_entries"]["task_turn_generation"]["type"]
        ).upper()
        assert columns["log_entries"]["native_turn_id"]["type"].length == 200
        assert columns["capability_invocations"]["request_output_log_id"][
            "nullable"
        ] is True
        assert columns["capability_invocations"]["request_native_turn_id"][
            "type"
        ].length == 200
        capability_checks = {
            item["name"]
            for item in inspect(engine).get_check_constraints(
                "capability_invocations"
            )
        }
        assert "ck_cap_inv_agent_request_identity" in capability_checks
        handoff_columns = _get_table_columns(
            engine,
            "worker_turn_handoff_receipts",
        )
        assert {
            "handoff_id",
            "task_id",
            "source_log_id",
            "side",
            "worker_id",
            "retry_count",
            "from_generation",
            "status",
            "request_payload",
            "request_digest",
            "queue_payload",
            "queue_payload_digest",
            "response",
            "claimed_turn_generation",
            "terminal_pr_review_chat",
            "cancel_reason",
            "created_at",
            "updated_at",
        } == set(handoff_columns)
        handoff_checks = {
            item["name"]: item["sqltext"]
            for item in inspect(engine).get_check_constraints(
                "worker_turn_handoff_receipts"
            )
        }
        assert "CLAIMED_TURN_GENERATION IS NOT NULL" in handoff_checks[
            "ck_worker_turn_handoff_claim"
        ].upper()
        with engine.begin() as conn:
            assert conn.execute(
                text(
                    "SELECT turn_generation, capability_policy FROM tasks "
                    "WHERE id = :task_id"
                ),
                {"task_id": task_id},
            ).one() == (0, None)
            assert conn.execute(
                text(
                    "SELECT task_turn_generation, native_turn_id "
                    "FROM log_entries WHERE id = :log_id"
                ),
                {"log_id": log_id},
            ).one() == (None, None)
            assert conn.execute(
                text(
                    "SELECT request_output_log_id, request_native_turn_id "
                    "FROM capability_invocations WHERE id = :invocation_id"
                ),
                {"invocation_id": invocation_id},
            ).one() == (None, None)
            task_ddl = conn.execute(text(
                "SELECT sql FROM sqlite_master "
                "WHERE type = 'table' AND name = 'tasks'"
            )).scalar_one()
            assert "AUTOINCREMENT" in task_ddl.upper()
            conn.execute(text(
                "INSERT INTO tasks "
                "(title, description, status, priority, target_branch, "
                "merge_status, retry_count, max_retries, mode, created_at) "
                "VALUES ('post-upgrade sequence task', 'd', 'completed', 0, "
                "'main', 'pending', 0, 2, 'auto', '2026-08-06 00:00:04')"
            ))
            post_upgrade_id = conn.execute(text(
                "SELECT id FROM tasks WHERE title = 'post-upgrade sequence task'"
            )).scalar_one()
            assert post_upgrade_id > 80
            conn.execute(text(
                "DELETE FROM tasks WHERE id = :task_id"
            ), {"task_id": post_upgrade_id})
        handoff_insert = text(
            "INSERT INTO worker_turn_handoff_receipts "
            "(handoff_id, task_id, source_log_id, side, worker_id, "
            "retry_count, from_generation, status, request_payload, "
            "request_digest, queue_payload, queue_payload_digest, response, "
            "claimed_turn_generation, terminal_pr_review_chat, created_at, "
            "updated_at) VALUES (:handoff_id, :task_id, :log_id, 'worker', "
            "NULL, 0, 4, :status, '{}', :digest, '{}', :digest, '{}', "
            ":claimed_turn_generation, 0, '2026-08-06 00:00:05', "
            "'2026-08-06 00:00:05')"
        )
        for handoff_id, status, claimed_generation in (
            ("1" * 32, "claimed", None),
            ("2" * 32, "accepted", 5),
        ):
            with pytest.raises(IntegrityError):
                with engine.begin() as conn:
                    conn.execute(
                        handoff_insert,
                        {
                            "handoff_id": handoff_id,
                            "task_id": task_id,
                            "log_id": log_id,
                            "status": status,
                            "digest": digest,
                            "claimed_turn_generation": claimed_generation,
                        },
                    )
        with engine.begin() as conn:
            conn.execute(
                handoff_insert,
                {
                    "handoff_id": "3" * 32,
                    "task_id": task_id,
                    "log_id": log_id,
                    "status": "launching",
                    "digest": digest,
                    "claimed_turn_generation": 5,
                },
            )
            conn.execute(text(
                "DELETE FROM worker_turn_handoff_receipts "
                "WHERE handoff_id = :handoff_id"
            ), {"handoff_id": "3" * 32})
        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "UPDATE capability_invocations "
                        "SET source = 'agent_request', "
                        "resume_policy = 'resume_task' "
                        "WHERE id = :invocation_id"
                    ),
                    {"invocation_id": invocation_id},
                )
        engine.dispose()

        _run_alembic(cfg, command.downgrade, DELIVERY_LOOP_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "turn_generation" not in _get_table_columns(engine, "tasks")
        assert "capability_policy" not in _get_table_columns(engine, "tasks")
        assert "task_turn_generation" not in _get_table_columns(
            engine, "log_entries"
        )
        assert "request_output_log_id" not in _get_table_columns(
            engine, "capability_invocations"
        )
        assert "worker_turn_handoff_receipts" not in _get_all_tables(engine)
        with engine.begin() as conn:
            assert conn.execute(text(
                "SELECT COUNT(*) FROM tasks WHERE id = :task_id"
            ), {"task_id": task_id}).scalar_one() == 1
            assert conn.execute(text(
                "SELECT COUNT(*) FROM log_entries WHERE id = :log_id"
            ), {"log_id": log_id}).scalar_one() == 1
            task_ddl = conn.execute(text(
                "SELECT sql FROM sqlite_master "
                "WHERE type = 'table' AND name = 'tasks'"
            )).scalar_one()
            assert "AUTOINCREMENT" in task_ddl.upper()
            conn.execute(text(
                "INSERT INTO tasks "
                "(title, description, status, priority, target_branch, "
                "merge_status, retry_count, max_retries, mode, created_at) "
                "VALUES ('post-downgrade sequence task', 'd', 'completed', 0, "
                "'main', 'pending', 0, 2, 'auto', '2026-08-06 00:00:05')"
            ))
            post_downgrade_id = conn.execute(text(
                "SELECT id FROM tasks "
                "WHERE title = 'post-downgrade sequence task'"
            )).scalar_one()
            assert post_downgrade_id > post_upgrade_id
        engine.dispose()

        _run_alembic(cfg, command.upgrade, AUTO_CAPABILITY_TURN_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "turn_generation" in _get_table_columns(engine, "tasks")
        assert "request_native_turn_id" in _get_table_columns(
            engine, "capability_invocations"
        )
        engine.dispose()


class TestTerminalArbitrationMigration:
    def test_worker_termination_table_partial_index_replay(self, tmp_path):
        db_path = str(tmp_path / "termination-receipt-index-replay.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, AUTO_CAPABILITY_TURN_REVISION)
        module = _load_terminal_arbitration_migration(
            "termination_receipt_index_replay"
        )

        from alembic.migration import MigrationContext
        from alembic.operations import Operations

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as connection:
            context = MigrationContext.configure(connection=connection)
            with patch.object(module, "op", Operations(context)):
                module._create_worker_task_termination_table()
            connection.execute(
                text("DROP INDEX ix_worker_task_term_due")
            )
        engine.dispose()

        _run_alembic(cfg, command.upgrade, TERMINAL_ARBITRATION_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        indexes = {
            item["name"]: tuple(item["column_names"])
            for item in inspect(engine).get_indexes(
                "worker_task_termination_receipts"
            )
        }
        assert indexes == module._WORKER_TASK_TERMINATION_INDEXES
        with engine.connect() as connection:
            assert connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == TERMINAL_ARBITRATION_REVISION
        engine.dispose()

    def test_worker_termination_table_malformed_replay_fails_before_ddl(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "termination-receipt-malformed-replay.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, AUTO_CAPABILITY_TURN_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as connection:
            connection.execute(text(
                "CREATE TABLE worker_task_termination_receipts ("
                "operation_id VARCHAR(32) PRIMARY KEY)"
            ))
        engine.dispose()

        with pytest.raises(RuntimeError, match="partial or foreign column set"):
            _run_alembic(cfg, command.upgrade, TERMINAL_ARBITRATION_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "turn_source_log_id" not in _get_table_columns(engine, "tasks")
        with engine.connect() as connection:
            assert connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == AUTO_CAPABILITY_TURN_REVISION
        engine.dispose()

    @pytest.mark.parametrize("malformation", ("named_unique", "foreign_unique"))
    def test_worker_termination_table_unique_index_replay_fails_closed(
        self,
        tmp_path,
        malformation,
    ):
        db_path = str(tmp_path / f"termination-receipt-{malformation}.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, AUTO_CAPABILITY_TURN_REVISION)
        module = _load_terminal_arbitration_migration(
            f"termination_receipt_{malformation}"
        )

        from alembic.migration import MigrationContext
        from alembic.operations import Operations

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as connection:
            context = MigrationContext.configure(connection=connection)
            with patch.object(module, "op", Operations(context)):
                module._create_worker_task_termination_table()
            if malformation == "named_unique":
                connection.execute(text(
                    "DROP INDEX ix_worker_task_term_due"
                ))
                connection.execute(text(
                    "CREATE UNIQUE INDEX ix_worker_task_term_due ON "
                    "worker_task_termination_receipts"
                    "(side, status, next_reconcile_at)"
                ))
            else:
                connection.execute(text(
                    "CREATE UNIQUE INDEX uq_worker_task_term_foreign ON "
                    "worker_task_termination_receipts(task_id, status)"
                ))
        engine.dispose()

        expected = (
            "index ix_worker_task_term_due is malformed"
            if malformation == "named_unique"
            else "foreign UNIQUE index"
        )
        with pytest.raises(RuntimeError, match=expected):
            _run_alembic(cfg, command.upgrade, TERMINAL_ARBITRATION_REVISION)

        engine = create_engine(f"sqlite:///{db_path}")
        assert "turn_source_log_id" not in _get_table_columns(engine, "tasks")
        with engine.connect() as connection:
            assert connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == AUTO_CAPABILITY_TURN_REVISION
        engine.dispose()

    def test_downgrade_refuses_worker_termination_receipt_history(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "termination-receipt-downgrade-fence.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, TERMINAL_ARBITRATION_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as connection:
            connection.execute(text(
                "INSERT INTO tasks "
                "(title, description, status, priority, target_branch, "
                "merge_status, retry_count, max_retries, mode, created_at) "
                "VALUES ('termination downgrade fence', 'd', 'pending', 0, "
                "'main', 'pending', 0, 2, 'auto', "
                "'2026-08-06 00:00:00')"
            ))
            task_id = connection.execute(text(
                "SELECT id FROM tasks "
                "WHERE title = 'termination downgrade fence'"
            )).scalar_one()
            connection.execute(
                text(
                    "INSERT INTO worker_task_termination_receipts "
                    "(operation_id, task_id, active_task_id, side, worker_id, "
                    "operation, status, source_task_status, "
                    "source_task_retry_count, source_task_turn_generation, "
                    "request_payload, request_digest, created_at, updated_at) "
                    "VALUES (:operation_id, :task_id, :task_id, 'manager', 4, "
                    "'cancel', 'pending_remote', 'pending', 0, 0, '{}', "
                    ":digest, '2026-08-06 00:00:01', "
                    "'2026-08-06 00:00:01')"
                ),
                {
                    "operation_id": "f" * 32,
                    "task_id": task_id,
                    "digest": "f" * 64,
                },
            )
        engine.dispose()

        with pytest.raises(RuntimeError, match="receipt history"):
            _run_alembic(cfg, command.downgrade, AUTO_CAPABILITY_TURN_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "worker_task_termination_receipts" in _get_all_tables(engine)
        with engine.begin() as connection:
            assert connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == TERMINAL_ARBITRATION_REVISION
            connection.execute(
                text("DELETE FROM worker_task_termination_receipts")
            )
        engine.dispose()

        _run_alembic(cfg, command.downgrade, AUTO_CAPABILITY_TURN_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "worker_task_termination_receipts" not in _get_all_tables(engine)
        engine.dispose()

    def test_upgrade_downgrade_preserves_rows_constraints_and_task_ids(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "terminal-arbitration.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, AUTO_CAPABILITY_TURN_REVISION)

        digest = "d" * 64
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO tasks "
                "(title, description, status, priority, target_branch, "
                "merge_status, retry_count, max_retries, mode, created_at) "
                "VALUES ('terminal arbitration', 'd', 'completed', 0, "
                "'main', 'pending', 0, 2, 'auto', "
                "'2026-08-06 01:00:00')"
            ))
            task_id = conn.execute(text(
                "SELECT id FROM tasks WHERE title = 'terminal arbitration'"
            )).scalar_one()
            conn.execute(
                text(
                    "INSERT INTO log_entries "
                    "(task_id, task_retry_count, task_turn_generation, "
                    "event_type, role, content, is_error, timestamp) VALUES "
                    "(:task_id, 0, 7, 'user_message', 'user', 'source', 0, "
                    "'2026-08-06 01:00:01'), "
                    "(:task_id, 0, 7, 'result', 'assistant', 'output', 0, "
                    "'2026-08-06 01:00:02')"
                ),
                {"task_id": task_id},
            )
            log_ids = conn.execute(text(
                "SELECT id FROM log_entries WHERE task_id = :task_id "
                "ORDER BY id"
            ), {"task_id": task_id}).scalars().all()
            source_log_id, output_log_id = log_ids
            conn.execute(
                text(
                    "INSERT INTO capability_invocations "
                    "(task_id, capability_key, source, purpose, status, "
                    "state_version, idempotency_key, input_payload, input_hash, "
                    "subject_kind, subject_ref, subject_hash, executor_kind, "
                    "executor_config, executor_config_hash, policy_snapshot, "
                    "policy_hash, resume_policy, max_attempts, active_task_id, "
                    "request_task_retry_count, request_task_turn_generation, "
                    "request_source_log_id, request_output_log_id, created_at, "
                    "updated_at) VALUES "
                    "(:task_id, 'plan', 'human_request', 'advisory', 'failed', "
                    "1, 'terminal-arbitration-existing', '{}', :digest, "
                    "'task_generation', '{}', :digest, 'plan_agent', '{}', "
                    ":digest, '{}', :digest, 'attach_only', 1, NULL, 0, 7, "
                    ":source_log_id, :output_log_id, "
                    "'2026-08-06 01:00:03', '2026-08-06 01:00:03')"
                ),
                {
                    "task_id": task_id,
                    "digest": digest,
                    "source_log_id": source_log_id,
                    "output_log_id": output_log_id,
                },
            )
            invocation_id = conn.execute(text(
                "SELECT id FROM capability_invocations WHERE "
                "idempotency_key = 'terminal-arbitration-existing'"
            )).scalar_one()
            conn.execute(text(
                "INSERT INTO tasks "
                "(id, title, description, status, priority, target_branch, "
                "merge_status, retry_count, max_retries, mode, created_at) "
                "VALUES (180, 'terminal deleted high-water', 'd', 'completed', "
                "0, 'main', 'pending', 0, 2, 'auto', "
                "'2026-08-06 01:00:04')"
            ))
            conn.execute(text("DELETE FROM tasks WHERE id = 180"))
        engine.dispose()

        _run_alembic(cfg, command.upgrade, TERMINAL_ARBITRATION_REVISION)

        engine = create_engine(f"sqlite:///{db_path}")
        inspector = inspect(engine)
        task_columns = {
            item["name"]: item for item in inspector.get_columns("tasks")
        }
        log_columns = {
            item["name"]: item for item in inspector.get_columns("log_entries")
        }
        invocation_columns = {
            item["name"]: item
            for item in inspector.get_columns("capability_invocations")
        }
        assert task_columns["turn_source_log_id"]["nullable"] is True
        assert log_columns["turn_scope"]["nullable"] is True
        assert log_columns["turn_scope"]["type"].length == 16
        assert log_columns["actual_transport"]["nullable"] is True
        assert log_columns["actual_transport"]["type"].length == 24
        for column_name in (
            "request_reason",
            "request_protocol_version",
            "request_output_hash",
        ):
            assert invocation_columns[column_name]["nullable"] is True
        assert invocation_columns["request_output_hash"]["type"].length == 64

        log_checks = {
            item["name"]: item["sqltext"]
            for item in inspector.get_check_constraints("log_entries")
        }
        assert "ck_log_entries_turn_scope" in log_checks
        assert "AUTONOMOUS" in log_checks["ck_log_entries_turn_scope"].upper()
        assert "ck_log_entries_actual_transport" in log_checks
        actual_transport_check = log_checks[
            "ck_log_entries_actual_transport"
        ].upper()
        assert "TURN_SCOPE IS NOT NULL" in actual_transport_check
        assert "TURN_SCOPE = 'SOURCE'" in actual_transport_check
        assert "CODEX_APP_SERVER" in actual_transport_check
        invocation_checks = {
            item["name"]: item["sqltext"]
            for item in inspector.get_check_constraints(
                "capability_invocations"
            )
        }
        identity_check = invocation_checks[
            "ck_cap_inv_agent_request_identity"
        ].upper()
        assert "REQUEST_REASON IS NOT NULL" in identity_check
        assert "REQUEST_PROTOCOL_VERSION >= 1" in identity_check
        assert "REQUEST_OUTPUT_HASH IS NOT NULL" in identity_check
        invocation_uniques = {
            item["name"]: tuple(item["column_names"])
            for item in inspector.get_unique_constraints(
                "capability_invocations"
            )
        }
        assert invocation_uniques["uq_cap_inv_task_output_log"] == (
            "task_id",
            "request_output_log_id",
        )
        assert "worker_task_termination_receipts" in inspector.get_table_names()
        termination_columns = {
            item["name"]: item
            for item in inspector.get_columns(
                "worker_task_termination_receipts"
            )
        }
        assert termination_columns["operation_id"]["type"].length == 32
        assert termination_columns["source_task_incarnation_id"][
            "nullable"
        ] is True
        assert termination_columns["source_task_turn_generation"][
            "nullable"
        ] is False
        assert termination_columns["reconcile_count"]["nullable"] is False
        assert termination_columns["ack_intent_at"]["nullable"] is True
        termination_checks = {
            item["name"]
            for item in inspector.get_check_constraints(
                "worker_task_termination_receipts"
            )
        }
        assert termination_checks == set(
            _load_terminal_arbitration_migration(
                "roundtrip_checks"
            )._WORKER_TASK_TERMINATION_CHECKS
        )
        termination_indexes = {
            item["name"]: tuple(item["column_names"])
            for item in inspector.get_indexes(
                "worker_task_termination_receipts"
            )
        }
        assert termination_indexes == {
            "ix_worker_task_term_task_created": ("task_id", "created_at"),
            "ix_worker_task_term_due": (
                "side",
                "status",
                "next_reconcile_at",
            ),
            "ix_worker_task_term_worker_status": ("worker_id", "status"),
        }

        with engine.begin() as conn:
            assert conn.execute(
                text(
                    "SELECT turn_source_log_id FROM tasks WHERE id = :task_id"
                ),
                {"task_id": task_id},
            ).scalar_one() is None
            assert conn.execute(
                text(
                    "SELECT turn_scope, actual_transport FROM log_entries "
                    "WHERE task_id = :task_id "
                    "ORDER BY id"
                ),
                {"task_id": task_id},
            ).all() == [(None, None), (None, None)]
            assert conn.execute(
                text(
                    "SELECT request_reason, request_protocol_version, "
                    "request_output_hash FROM capability_invocations "
                    "WHERE id = :invocation_id"
                ),
                {"invocation_id": invocation_id},
            ).one() == (None, None, None)
            task_ddl = conn.execute(text(
                "SELECT sql FROM sqlite_master "
                "WHERE type = 'table' AND name = 'tasks'"
            )).scalar_one()
            assert "AUTOINCREMENT" in task_ddl.upper()
            conn.execute(
                text(
                    "UPDATE tasks SET turn_source_log_id = :source_log_id "
                    "WHERE id = :task_id"
                ),
                {"source_log_id": source_log_id, "task_id": task_id},
            )
            conn.execute(
                text(
                    "UPDATE log_entries SET turn_scope = CASE id "
                    "WHEN :source_log_id THEN 'source' ELSE 'foreground' END, "
                    "actual_transport = CASE id WHEN :source_log_id "
                    "THEN 'codex_exec' ELSE NULL END "
                    "WHERE task_id = :task_id"
                ),
                {
                    "source_log_id": source_log_id,
                    "task_id": task_id,
                },
            )
            conn.execute(text(
                "INSERT INTO tasks "
                "(title, description, status, priority, target_branch, "
                "merge_status, retry_count, max_retries, mode, created_at) "
                "VALUES ('terminal post-upgrade', 'd', 'completed', 0, "
                "'main', 'pending', 0, 2, 'auto', "
                "'2026-08-06 01:00:05')"
            ))
            post_upgrade_id = conn.execute(text(
                "SELECT id FROM tasks WHERE title = 'terminal post-upgrade'"
            )).scalar_one()
            assert post_upgrade_id > 180
            conn.execute(
                text("DELETE FROM tasks WHERE id = :task_id"),
                {"task_id": post_upgrade_id},
            )

        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "UPDATE log_entries SET turn_scope = 'background' "
                        "WHERE id = :output_log_id"
                    ),
                    {"output_log_id": output_log_id},
                )

        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "UPDATE log_entries SET turn_scope = NULL, "
                        "actual_transport = 'codex_exec' "
                        "WHERE id = :output_log_id"
                    ),
                    {"output_log_id": output_log_id},
                )

        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "UPDATE log_entries SET actual_transport = 'claude_exec' "
                        "WHERE id = :output_log_id"
                    ),
                    {"output_log_id": output_log_id},
                )

        agent_identity_update = (
            "UPDATE capability_invocations SET source = 'agent_request', "
            "resume_policy = 'resume_task', request_reason = :reason, "
            "request_protocol_version = :protocol_version, "
            "request_output_hash = :output_hash WHERE id = :invocation_id"
        )
        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(
                    text(agent_identity_update),
                    {
                        "reason": None,
                        "protocol_version": None,
                        "output_hash": None,
                        "invocation_id": invocation_id,
                    },
                )
        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(
                    text(agent_identity_update),
                    {
                        "reason": "Need terminal Plan",
                        "protocol_version": 0,
                        "output_hash": digest,
                        "invocation_id": invocation_id,
                    },
                )
        with engine.begin() as conn:
            conn.execute(
                text(agent_identity_update),
                {
                    "reason": "Need terminal Plan",
                    "protocol_version": 1,
                    "output_hash": digest,
                    "invocation_id": invocation_id,
                },
            )

        duplicate_insert = text(
            "INSERT INTO capability_invocations "
            "(task_id, capability_key, source, purpose, status, state_version, "
            "idempotency_key, input_payload, input_hash, subject_kind, "
            "subject_ref, subject_hash, executor_kind, executor_config, "
            "executor_config_hash, policy_snapshot, policy_hash, resume_policy, "
            "max_attempts, active_task_id, request_output_log_id, created_at, "
            "updated_at) VALUES (:task_id, 'code_review', 'human_request', "
            "'advisory', 'failed', 1, :idempotency_key, '{}', :digest, "
            "'task_generation', '{}', :digest, 'code_review', '{}', :digest, "
            "'{}', :digest, 'attach_only', 1, NULL, :output_log_id, "
            "'2026-08-06 01:00:06', '2026-08-06 01:00:06')"
        )
        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(
                    duplicate_insert,
                    {
                        "task_id": task_id,
                        "idempotency_key": "terminal-output-duplicate",
                        "digest": digest,
                        "output_log_id": output_log_id,
                    },
                )
        # The downgrade deliberately refuses to discard protocol audit fields
        # from a real Agent request.  Return this round-trip fixture to the
        # human-request shape; a dedicated guard test below covers refusal.
        with engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE capability_invocations SET "
                    "source = 'human_request', resume_policy = 'attach_only' "
                    "WHERE id = :invocation_id"
                ),
                {"invocation_id": invocation_id},
            )
            # A downgrade must never erase terminal-arbitration provenance.
            # This fixture already exercised the fields above; clear them
            # explicitly so the schema round-trip itself remains admissible.
            conn.execute(
                text(
                    "UPDATE tasks SET turn_source_log_id = NULL "
                    "WHERE id = :task_id"
                ),
                {"task_id": task_id},
            )
            conn.execute(
                text(
                    "UPDATE log_entries SET turn_scope = NULL, "
                    "actual_transport = NULL WHERE task_id = :task_id"
                ),
                {"task_id": task_id},
            )
        engine.dispose()

        _run_alembic(cfg, command.downgrade, AUTO_CAPABILITY_TURN_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "turn_source_log_id" not in _get_table_columns(engine, "tasks")
        assert "turn_scope" not in _get_table_columns(engine, "log_entries")
        assert "actual_transport" not in _get_table_columns(engine, "log_entries")
        assert (
            "worker_task_termination_receipts"
            not in _get_all_tables(engine)
        )
        downgraded_invocation_columns = _get_table_columns(
            engine,
            "capability_invocations",
        )
        assert "request_reason" not in downgraded_invocation_columns
        assert "request_protocol_version" not in downgraded_invocation_columns
        assert "request_output_hash" not in downgraded_invocation_columns
        downgraded_uniques = {
            item["name"]
            for item in inspect(engine).get_unique_constraints(
                "capability_invocations"
            )
        }
        assert "uq_cap_inv_task_output_log" not in downgraded_uniques
        downgraded_checks = {
            item["name"]: item["sqltext"].upper()
            for item in inspect(engine).get_check_constraints(
                "capability_invocations"
            )
        }
        downgraded_identity = downgraded_checks[
            "ck_cap_inv_agent_request_identity"
        ]
        assert "REQUEST_OUTPUT_LOG_ID IS NOT NULL" in downgraded_identity
        assert "REQUEST_REASON" not in downgraded_identity
        assert "REQUEST_PROTOCOL_VERSION" not in downgraded_identity
        assert "REQUEST_OUTPUT_HASH" not in downgraded_identity
        with engine.begin() as conn:
            assert conn.execute(
                text(
                    "SELECT source FROM capability_invocations "
                    "WHERE id = :invocation_id"
                ),
                {"invocation_id": invocation_id},
            ).scalar_one() == "human_request"
            task_ddl = conn.execute(text(
                "SELECT sql FROM sqlite_master "
                "WHERE type = 'table' AND name = 'tasks'"
            )).scalar_one()
            assert "AUTOINCREMENT" in task_ddl.upper()
            conn.execute(text(
                "INSERT INTO tasks "
                "(title, description, status, priority, target_branch, "
                "merge_status, retry_count, max_retries, mode, created_at) "
                "VALUES ('terminal post-downgrade', 'd', 'completed', 0, "
                "'main', 'pending', 0, 2, 'auto', "
                "'2026-08-06 01:00:07')"
            ))
            post_downgrade_id = conn.execute(text(
                "SELECT id FROM tasks WHERE title = 'terminal post-downgrade'"
            )).scalar_one()
            assert post_downgrade_id > post_upgrade_id
            conn.execute(
                duplicate_insert,
                {
                    "task_id": task_id,
                    "idempotency_key": "terminal-output-duplicate",
                    "digest": digest,
                    "output_log_id": output_log_id,
                },
            )
            # The old identity CHECK still accepts its complete legacy shape.
            conn.execute(
                text(
                    "UPDATE capability_invocations SET "
                    "source = 'agent_request', resume_policy = 'resume_task' "
                    "WHERE id = :invocation_id"
                ),
                {"invocation_id": invocation_id},
            )

        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "UPDATE capability_invocations "
                        "SET request_output_log_id = NULL "
                        "WHERE id = :invocation_id"
                    ),
                    {"invocation_id": invocation_id},
                )
        with engine.begin() as conn:
            conn.execute(text(
                "DELETE FROM capability_invocations WHERE "
                "idempotency_key = 'terminal-output-duplicate'"
            ))
            conn.execute(
                text(
                    "UPDATE capability_invocations SET "
                    "source = 'human_request', resume_policy = 'attach_only' "
                    "WHERE id = :invocation_id"
                ),
                {"invocation_id": invocation_id},
            )
        engine.dispose()

        _run_alembic(cfg, command.upgrade, TERMINAL_ARBITRATION_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "turn_source_log_id" in _get_table_columns(engine, "tasks")
        assert "turn_scope" in _get_table_columns(engine, "log_entries")
        assert "request_output_hash" in _get_table_columns(
            engine,
            "capability_invocations",
        )
        assert "worker_task_termination_receipts" in _get_all_tables(engine)
        engine.dispose()

    def test_preflight_refuses_unsafe_data_before_schema_changes(self, tmp_path):
        db_path = str(tmp_path / "terminal-arbitration-preflight.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, AUTO_CAPABILITY_TURN_REVISION)

        digest = "e" * 64
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO tasks "
                "(title, description, status, priority, target_branch, "
                "merge_status, retry_count, max_retries, mode, created_at) "
                "VALUES ('terminal preflight', 'd', 'completed', 0, "
                "'main', 'pending', 0, 2, 'auto', "
                "'2026-08-06 02:00:00')"
            ))
            task_id = conn.execute(text(
                "SELECT id FROM tasks WHERE title = 'terminal preflight'"
            )).scalar_one()
            conn.execute(
                text(
                    "INSERT INTO log_entries "
                    "(task_id, task_retry_count, task_turn_generation, "
                    "event_type, role, content, is_error, timestamp) VALUES "
                    "(:task_id, 0, 1, 'user_message', 'user', 'source', 0, "
                    "'2026-08-06 02:00:01'), "
                    "(:task_id, 0, 1, 'result', 'assistant', 'output', 0, "
                    "'2026-08-06 02:00:02')"
                ),
                {"task_id": task_id},
            )
            source_log_id, output_log_id = conn.execute(
                text(
                    "SELECT id FROM log_entries WHERE task_id = :task_id "
                    "ORDER BY id"
                ),
                {"task_id": task_id},
            ).scalars().all()
            invocation_insert = text(
                "INSERT INTO capability_invocations "
                "(task_id, capability_key, source, purpose, status, "
                "state_version, idempotency_key, input_payload, input_hash, "
                "subject_kind, subject_ref, subject_hash, executor_kind, "
                "executor_config, executor_config_hash, policy_snapshot, "
                "policy_hash, resume_policy, max_attempts, active_task_id, "
                "request_task_retry_count, request_task_turn_generation, "
                "request_source_log_id, request_output_log_id, created_at, "
                "updated_at) VALUES "
                "(:task_id, 'plan', :source, 'advisory', 'failed', 1, "
                ":idempotency_key, '{}', :digest, 'task_generation', '{}', "
                ":digest, 'plan_agent', '{}', :digest, '{}', :digest, "
                ":resume_policy, 1, NULL, 0, 1, :source_log_id, "
                ":output_log_id, '2026-08-06 02:00:03', "
                "'2026-08-06 02:00:03')"
            )
            conn.execute(
                invocation_insert,
                {
                    "task_id": task_id,
                    "source": "agent_request",
                    "idempotency_key": "terminal-preflight-agent",
                    "digest": digest,
                    "resume_policy": "resume_task",
                    "source_log_id": source_log_id,
                    "output_log_id": output_log_id,
                },
            )
        engine.dispose()

        with pytest.raises(RuntimeError, match="zero legacy agent_request"):
            _run_alembic(cfg, command.upgrade, TERMINAL_ARBITRATION_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "turn_source_log_id" not in _get_table_columns(engine, "tasks")
        assert "turn_scope" not in _get_table_columns(engine, "log_entries")
        with engine.begin() as conn:
            assert conn.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == AUTO_CAPABILITY_TURN_REVISION
            conn.execute(text(
                "UPDATE capability_invocations SET source = 'human_request', "
                "resume_policy = 'attach_only' WHERE "
                "idempotency_key = 'terminal-preflight-agent'"
            ))
            conn.execute(
                invocation_insert,
                {
                    "task_id": task_id,
                    "source": "human_request",
                    "idempotency_key": "terminal-preflight-duplicate",
                    "digest": digest,
                    "resume_policy": "attach_only",
                    "source_log_id": source_log_id,
                    "output_log_id": output_log_id,
                },
            )
        engine.dispose()

        with pytest.raises(RuntimeError, match="duplicate task/output-log"):
            _run_alembic(cfg, command.upgrade, TERMINAL_ARBITRATION_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "turn_source_log_id" not in _get_table_columns(engine, "tasks")
        with engine.begin() as conn:
            assert conn.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == AUTO_CAPABILITY_TURN_REVISION
            conn.execute(text(
                "DELETE FROM capability_invocations WHERE "
                "idempotency_key = 'terminal-preflight-duplicate'"
            ))
        engine.dispose()

        _run_alembic(cfg, command.upgrade, TERMINAL_ARBITRATION_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE capability_invocations SET source = 'agent_request', "
                    "resume_policy = 'resume_task', request_reason = :reason, "
                    "request_protocol_version = 1, request_output_hash = :digest "
                    "WHERE idempotency_key = 'terminal-preflight-agent'"
                ),
                {"reason": "Need a durable Plan", "digest": digest},
            )
        engine.dispose()

        with pytest.raises(RuntimeError, match="audit history would be destroyed"):
            _run_alembic(cfg, command.downgrade, AUTO_CAPABILITY_TURN_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "turn_source_log_id" in _get_table_columns(engine, "tasks")
        assert "turn_scope" in _get_table_columns(engine, "log_entries")
        with engine.connect() as conn:
            assert conn.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == TERMINAL_ARBITRATION_REVISION
        engine.dispose()

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text(
                "UPDATE capability_invocations SET source = 'human_request', "
                "resume_policy = 'attach_only' WHERE "
                "idempotency_key = 'terminal-preflight-agent'"
            ))
            conn.execute(
                text(
                    "UPDATE tasks SET turn_source_log_id = :source_log_id "
                    "WHERE id = :task_id"
                ),
                {"source_log_id": source_log_id, "task_id": task_id},
            )
        engine.dispose()

        with pytest.raises(RuntimeError, match="Task turn provenance"):
            _run_alembic(cfg, command.downgrade, AUTO_CAPABILITY_TURN_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            assert conn.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == TERMINAL_ARBITRATION_REVISION
            conn.execute(
                text(
                    "UPDATE tasks SET turn_source_log_id = NULL "
                    "WHERE id = :task_id"
                ),
                {"task_id": task_id},
            )
            conn.execute(
                text(
                    "UPDATE log_entries SET turn_scope = 'foreground' "
                    "WHERE id = :output_log_id"
                ),
                {"output_log_id": output_log_id},
            )
        engine.dispose()

        with pytest.raises(RuntimeError, match="LogEntry turn provenance"):
            _run_alembic(cfg, command.downgrade, AUTO_CAPABILITY_TURN_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE log_entries SET turn_scope = NULL "
                    "WHERE id = :output_log_id"
                ),
                {"output_log_id": output_log_id},
            )
            conn.execute(
                text(
                    "UPDATE log_entries SET turn_scope = 'source', "
                    "actual_transport = 'codex_exec' "
                    "WHERE id = :source_log_id"
                ),
                {"source_log_id": source_log_id},
            )
        engine.dispose()

        with pytest.raises(RuntimeError, match="LogEntry turn provenance"):
            _run_alembic(cfg, command.downgrade, AUTO_CAPABILITY_TURN_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            assert conn.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == TERMINAL_ARBITRATION_REVISION
            conn.execute(
                text(
                    "UPDATE log_entries SET turn_scope = NULL, "
                    "actual_transport = NULL WHERE id = :source_log_id"
                ),
                {"source_log_id": source_log_id},
            )
        engine.dispose()

        _run_alembic(cfg, command.downgrade, AUTO_CAPABILITY_TURN_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "turn_source_log_id" not in _get_table_columns(engine, "tasks")
        assert "turn_scope" not in _get_table_columns(engine, "log_entries")
        engine.dispose()

    def test_sqlite_preflight_fence_blocks_a_second_writer(self, tmp_path):
        db_path = str(tmp_path / "terminal-preflight-fence.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, AUTO_CAPABILITY_TURN_REVISION)
        module = _load_terminal_arbitration_migration("sqlite_fence")

        from alembic.migration import MigrationContext
        from alembic.operations import Operations

        engine = create_engine(
            f"sqlite:///{db_path}",
            connect_args={"timeout": 0},
        )
        first = engine.connect()
        transaction = first.begin()
        context = MigrationContext.configure(connection=first)
        try:
            with patch.object(module, "op", Operations(context)):
                module._acquire_preflight_fence(
                    expected_revision=AUTO_CAPABILITY_TURN_REVISION,
                )
                module._assert_upgrade_preconditions()
                with engine.connect() as second:
                    second.exec_driver_sql("PRAGMA busy_timeout = 0")
                    with pytest.raises(OperationalError, match="locked"):
                        second.execute(
                            text(
                                "UPDATE alembic_version "
                                "SET version_num = version_num"
                            )
                        )
        finally:
            transaction.rollback()
            first.close()

        # Prove the second statement was blocked by the held writer transaction,
        # not because the statement or database was otherwise invalid.
        with engine.begin() as connection:
            updated = connection.execute(
                text("UPDATE alembic_version SET version_num = version_num")
            )
            assert updated.rowcount == 1
        engine.dispose()

    def test_mysql_reflected_identity_checks_require_exact_boolean_shape(self):
        module = _load_terminal_arbitration_migration("mysql_check_shape")
        old = (
            "((`source` <> _utf8mb4'agent_request') or "
            "((`purpose` = _utf8mb4'advisory') and "
            "(`resume_policy` = _utf8mb4'resume_task') and "
            "(`requested_by_user_id` is null) and "
            "(`request_task_retry_count` is not null) and "
            "(`request_task_turn_generation` is not null) and "
            "(`request_source_log_id` is not null) and "
            "(`request_output_log_id` is not null)))"
        )
        new = old[:-2] + (
            " and (`request_reason` is not null)"
            " and (`request_protocol_version` is not null)"
            " and (`request_protocol_version` >= 1)"
            " and (`request_output_hash` is not null)))"
        )
        gate = "(`source` <> _latin1'agent_request')"

        assert module._identity_check_kind(old) == "old"
        assert module._identity_check_kind(new) == "new"
        assert module._is_mysql_downgrade_gate(gate)
        assert module._identity_check_kind(new.replace(">= 1", ">= 0")) is None
        assert module._identity_check_kind(f"({new}) or (1 = 1)") is None
        regrouped = new.replace(
            ") or ((`purpose`",
            ") or (`purpose`",
        ).replace("is not null)))", "is not null)) and (1 = 1)")
        assert module._identity_check_kind(regrouped) is None

    @pytest.mark.parametrize("failure_call", (0, 1, 2))
    def test_mysql_upgrade_phase_failure_keeps_a_guard_and_replays(
        self,
        failure_call,
    ):
        module = _load_terminal_arbitration_migration(
            f"mysql_upgrade_failure_{failure_call}"
        )
        model = {
            "canonical": "old",
            "shadow": None,
            "gate": False,
            "columns": set(),
            "unique": False,
        }
        control = {"failure": failure_call, "calls": 0}

        def state():
            return _mysql_terminal_state(**model)

        def alter(table, actions):
            assert table == "capability_invocations"
            if not actions:
                return
            call = control["calls"]
            control["calls"] += 1
            if control["failure"] == call:
                raise RuntimeError("injected atomic ALTER failure")
            joined = " ".join(actions)
            if "ADD COLUMN request_reason" in joined:
                model["columns"] = set(module._MYSQL_NEW_COLUMNS)
                model["unique"] = True
                model["shadow"] = "new"
            if any(
                action.startswith(
                    "ADD CONSTRAINT ck_cap_inv_agent_request_identity "
                )
                for action in actions
            ):
                model["canonical"] = "new"
            if any(
                action == (
                    "DROP CHECK "
                    f"{module._MYSQL_SHADOW_IDENTITY_CHECK}"
                )
                for action in actions
            ):
                model["shadow"] = None

        with (
            patch.object(module, "_mysql_capability_state", side_effect=state),
            patch.object(module, "_mysql_alter", side_effect=alter),
        ):
            with pytest.raises(RuntimeError, match="injected atomic ALTER"):
                module._mysql_upgrade_capability_online()
            assert model["canonical"] in {"old", "new"} or model["shadow"] == "new"

            control.update(failure=None, calls=0)
            module._mysql_upgrade_capability_online()

        assert model == {
            "canonical": "new",
            "shadow": None,
            "gate": False,
            "columns": set(module._MYSQL_NEW_COLUMNS),
            "unique": True,
        }

    @pytest.mark.parametrize("failure_call", (0, 1, 2))
    def test_mysql_downgrade_phase_failure_keeps_a_guard_and_replays(
        self,
        failure_call,
    ):
        module = _load_terminal_arbitration_migration(
            f"mysql_downgrade_failure_{failure_call}"
        )
        model = {
            "canonical": "new",
            "shadow": None,
            "gate": False,
            "columns": set(module._MYSQL_NEW_COLUMNS),
            "unique": True,
        }
        control = {"failure": failure_call, "calls": 0}

        def state():
            return _mysql_terminal_state(**model)

        def alter(table, actions):
            assert table == "capability_invocations"
            if not actions:
                return
            call = control["calls"]
            control["calls"] += 1
            if control["failure"] == call:
                raise RuntimeError("injected atomic ALTER failure")
            joined = " ".join(actions)
            if "ADD CONSTRAINT ck_cap_inv_no_agent_request_downgrade" in joined:
                model["gate"] = True
            if any(
                action.startswith(
                    "ADD CONSTRAINT ck_cap_inv_agent_request_identity "
                )
                for action in actions
            ):
                model["canonical"] = "old"
            if "DROP COLUMN request_reason" in joined:
                model["columns"] = set()
                model["unique"] = False
            if "DROP CHECK ck_cap_inv_no_agent_request_downgrade" in joined:
                model["gate"] = False

        with (
            patch.object(module, "_mysql_capability_state", side_effect=state),
            patch.object(
                module,
                "_mysql_auxiliary_state",
                return_value=_mysql_auxiliary_state(
                    task_source=False,
                    log_columns=False,
                ),
            ),
            patch.object(module, "_mysql_alter", side_effect=alter),
        ):
            with pytest.raises(RuntimeError, match="injected atomic ALTER"):
                module._mysql_downgrade_capability_online()
            assert model["canonical"] in {"old", "new"} or model["gate"]

            control.update(failure=None, calls=0)
            module._mysql_downgrade_capability_online()
            module._mysql_finish_downgrade_online()

        assert model == {
            "canonical": "old",
            "shadow": None,
            "gate": False,
            "columns": set(),
            "unique": False,
        }

    @pytest.mark.parametrize("failure_call", (0, 1, 2, 3))
    def test_mysql_auxiliary_gate_failure_is_atomic_and_replayable(
        self,
        failure_call,
    ):
        module = _load_terminal_arbitration_migration(
            f"mysql_auxiliary_failure_{failure_call}"
        )
        model = {
            "task_source": True,
            "log_columns": True,
            "task_gate": False,
            "log_gate": False,
        }
        control = {"failure": failure_call, "calls": 0}

        def state():
            return _mysql_auxiliary_state(**model)

        def alter(table, actions):
            if not actions:
                return
            call = control["calls"]
            control["calls"] += 1
            if control["failure"] == call:
                raise RuntimeError("injected atomic auxiliary ALTER failure")
            joined = " ".join(actions)
            if (
                table == "tasks"
                and f"ADD CONSTRAINT {module._MYSQL_TASK_DOWNGRADE_GATE}"
                in joined
            ):
                model["task_gate"] = True
            if (
                table == "log_entries"
                and f"ADD CONSTRAINT {module._MYSQL_LOG_DOWNGRADE_GATE}"
                in joined
            ):
                model["log_gate"] = True
            if table == "log_entries" and "DROP COLUMN actual_transport" in joined:
                assert model["log_gate"] is True
                model["log_columns"] = False
                model["log_gate"] = False
            if table == "tasks" and "DROP COLUMN turn_source_log_id" in joined:
                assert model["task_gate"] is True
                model["task_source"] = False
                model["task_gate"] = False

        def run_downgrade():
            module._mysql_install_auxiliary_downgrade_gates_online()
            module._mysql_downgrade_auxiliary_online()

        with (
            patch.object(module, "_mysql_auxiliary_state", side_effect=state),
            patch.object(module, "_mysql_alter", side_effect=alter),
        ):
            with pytest.raises(
                RuntimeError,
                match="injected atomic auxiliary ALTER failure",
            ):
                run_downgrade()

            # Every successful destructive phase retained the other table's
            # durable gate. Reset only the injected failure and replay from the
            # exact reflected state.
            control.update(failure=None, calls=0)
            run_downgrade()

        assert model == {
            "task_source": False,
            "log_columns": False,
            "task_gate": False,
            "log_gate": False,
        }

    def test_mysql_worker_termination_gate_fences_drop_and_replays(self):
        module = _load_terminal_arbitration_migration(
            "mysql_worker_termination_gate"
        )
        model = {"present": True, "gate": False}
        control = {"fail": True}

        def state(*, allow_downgrade_gate=False):
            assert allow_downgrade_gate
            return {
                "present": model["present"],
                "missing_indexes": set(),
                "downgrade_gate": model["gate"],
                "downgrade_gate_enforced": model["gate"],
            }

        def alter(table, actions):
            assert table == "worker_task_termination_receipts"
            assert "operation_id IS NULL" in " ".join(actions)
            if control["fail"]:
                raise RuntimeError("injected termination gate failure")
            model["gate"] = True

        def drop_table(table):
            assert table == "worker_task_termination_receipts"
            assert model["gate"] is True
            model["present"] = False

        fake_op = SimpleNamespace(drop_table=drop_table)
        with (
            patch.object(
                module,
                "_worker_task_termination_state",
                side_effect=state,
            ),
            patch.object(module, "_mysql_alter", side_effect=alter),
            patch.object(module, "op", fake_op),
        ):
            with pytest.raises(
                RuntimeError,
                match="injected termination gate failure",
            ):
                module._mysql_install_worker_task_termination_downgrade_gate()
            assert model == {"present": True, "gate": False}

            control["fail"] = False
            module._mysql_install_worker_task_termination_downgrade_gate()
            module._mysql_drop_worker_task_termination_table()

        assert model == {"present": False, "gate": True}

    def test_mysql_worker_termination_drop_refuses_without_fence(self):
        module = _load_terminal_arbitration_migration(
            "mysql_worker_termination_unfenced_drop"
        )
        state = {
            "present": True,
            "missing_indexes": set(),
            "downgrade_gate": False,
            "downgrade_gate_enforced": False,
        }
        with (
            patch.object(
                module,
                "_worker_task_termination_state",
                return_value=state,
            ),
            patch.object(module, "op") as fake_op,
            pytest.raises(RuntimeError, match="without its durable writer fence"),
        ):
            module._mysql_drop_worker_task_termination_table()
        fake_op.drop_table.assert_not_called()

    def test_mysql_worker_termination_missing_index_suffix_replays(self):
        module = _load_terminal_arbitration_migration(
            "mysql_worker_termination_index_replay"
        )
        due = "ix_worker_task_term_due"
        states = [
            {
                "present": True,
                "missing_indexes": {due},
                "downgrade_gate": False,
                "downgrade_gate_enforced": False,
            },
            {
                "present": True,
                "missing_indexes": set(),
                "downgrade_gate": False,
                "downgrade_gate_enforced": False,
            },
        ]
        with (
            patch.object(
                module,
                "_worker_task_termination_state",
                side_effect=states,
            ),
            patch.object(module, "_is_offline", return_value=False),
            patch.object(module, "op") as fake_op,
        ):
            module._ensure_worker_task_termination_table()
        fake_op.create_index.assert_called_once_with(
            due,
            "worker_task_termination_receipts",
            ["side", "status", "next_reconcile_at"],
            unique=False,
        )

    @pytest.mark.parametrize(
        ("reflected", "dialect_name", "expected"),
        (
            (mysql.TINYINT(display_width=1), "mysql", True),
            (mysql.BOOLEAN(), "mysql", True),
            (mysql.TINYINT(display_width=2), "mysql", False),
            (mysql.TINYINT(display_width=None), "mysql", False),
            (mysql.TINYINT(display_width=1, unsigned=True), "mysql", False),
            (mysql.TINYINT(display_width=1, zerofill=True), "mysql", False),
            (mysql.TINYINT(display_width=1), "sqlite", False),
        ),
    )
    def test_mysql_worker_termination_boolean_reflection_is_exact(
        self,
        reflected,
        dialect_name,
        expected,
    ):
        module = _load_terminal_arbitration_migration(
            f"mysql_worker_termination_boolean_{dialect_name}_{expected}"
        )
        assert module._worker_task_termination_type_matches(
            reflected,
            module.sa.Boolean,
            None,
            dialect_name=dialect_name,
        ) is expected

    def test_mysql_worker_termination_legal_check_reflection_is_accepted(self):
        module = _load_terminal_arbitration_migration(
            "mysql_worker_termination_legal_check_reflection"
        )
        actual = dict(module._WORKER_TASK_TERMINATION_CHECKS)
        actual["ck_worker_task_term_operation_id"] = (
            "((LENGTH(`operation_id`) = 32))"
        )
        actual["ck_worker_task_term_side"] = (
            "(`side` IN (_utf8mb4'manager', _utf8mb4'worker'))"
        )

        module._assert_worker_task_termination_check_semantics(
            actual,
            module._WORKER_TASK_TERMINATION_CHECKS,
        )

    def test_mysql_worker_termination_weakened_check_replay_fails_closed(self):
        module = _load_terminal_arbitration_migration(
            "mysql_worker_termination_weakened_check"
        )
        actual = dict(module._WORKER_TASK_TERMINATION_CHECKS)
        actual["ck_worker_task_term_active_slot"] = "TRUE"

        with pytest.raises(
            RuntimeError,
            match="ck_worker_task_term_active_slot is malformed",
        ):
            module._assert_worker_task_termination_check_semantics(
                actual,
                module._WORKER_TASK_TERMINATION_CHECKS,
            )

    def test_postgresql_worker_termination_legal_check_reflection_is_accepted(
        self,
    ):
        module = _load_terminal_arbitration_migration(
            "postgresql_worker_termination_legal_check_reflection"
        )
        canonical = {
            name: f"CHECK ({expression})"
            for name, expression in module._WORKER_TASK_TERMINATION_CHECKS.items()
        }
        actual = dict(canonical)
        actual["ck_worker_task_term_operation_id"] = (
            "CHECK (((LENGTH(operation_id) = 32)))"
        )

        module._assert_worker_task_termination_check_semantics(
            actual,
            canonical,
        )

    def test_postgresql_worker_termination_weakened_check_replay_fails_closed(
        self,
    ):
        module = _load_terminal_arbitration_migration(
            "postgresql_worker_termination_weakened_check"
        )
        canonical = {
            name: f"CHECK ({expression})"
            for name, expression in module._WORKER_TASK_TERMINATION_CHECKS.items()
        }
        actual = dict(canonical)
        actual["ck_worker_task_term_active_slot"] = "CHECK (TRUE)"

        with pytest.raises(
            RuntimeError,
            match="ck_worker_task_term_active_slot is malformed",
        ):
            module._assert_worker_task_termination_check_semantics(
                actual,
                canonical,
            )

    def test_mysql_capability_gate_is_not_released_before_auxiliary_settles(self):
        module = _load_terminal_arbitration_migration("mysql_gate_release_order")
        capability = _mysql_terminal_state(
            canonical="old",
            gate=True,
        )
        auxiliary = _mysql_auxiliary_state(
            task_gate=True,
            log_gate=True,
        )
        with (
            patch.object(
                module,
                "_mysql_capability_state",
                return_value=capability,
            ),
            patch.object(
                module,
                "_mysql_auxiliary_state",
                return_value=auxiliary,
            ),
            patch.object(module, "_mysql_alter") as alter,
        ):
            with pytest.raises(
                RuntimeError,
                match="before auxiliary downgrade settles",
            ):
                module._mysql_finish_downgrade_online()
            alter.assert_not_called()

    @pytest.mark.parametrize("gate", ("task", "log"))
    def test_mysql_auxiliary_gate_reflection_requires_enforcement(self, gate):
        module = _load_terminal_arbitration_migration(
            f"mysql_auxiliary_gate_enforcement_{gate}"
        )
        state = _mysql_auxiliary_state(
            task_gate=gate == "task",
            log_gate=gate == "log",
        )
        state[f"{gate}_gate_enforced"] = False
        with pytest.raises(RuntimeError, match="gate.*not enforced"):
            module._assert_mysql_auxiliary_state(state)

    def test_mysql_completed_before_stamp_skips_destructive_preflight(self):
        module = _load_terminal_arbitration_migration("mysql_stamp_replay")
        upgraded = _mysql_terminal_state(
            canonical="new",
            columns=set(module._MYSQL_NEW_COLUMNS),
            unique=True,
        )
        downgraded = _mysql_terminal_state(canonical="old")

        assert module._mysql_has_v2_identity_guard(upgraded)
        assert module._mysql_has_v2_audit_schema(upgraded)
        assert not module._mysql_has_v2_identity_guard(downgraded)
        assert not module._mysql_has_v2_audit_schema(downgraded)

        bind = MagicMock()
        duplicate_result = MagicMock()
        duplicate_result.first.return_value = None
        bind.execute.return_value = duplicate_result
        fake_op = SimpleNamespace(
            get_context=lambda: SimpleNamespace(as_sql=False),
            get_bind=lambda: bind,
        )
        with patch.object(module, "op", fake_op):
            module._assert_upgrade_preconditions(require_zero_agent=False)
            assert bind.execute.call_count == 1
            assert "GROUP BY" in str(bind.execute.call_args.args[0])
            bind.reset_mock()
            module._assert_downgrade_preconditions(
                require_zero_agent=False,
                require_zero_task_source=False,
                require_zero_log_provenance=False,
                require_zero_worker_terminations=False,
            )
            bind.execute.assert_not_called()

    @pytest.mark.parametrize(
        ("version", "is_mariadb", "engines", "error"),
        [
            ((8, 0, 15), False, (), "8.0.16"),
            ((8, 0, 36), True, (), "MariaDB"),
            (
                (8, 0, 36),
                False,
                (
                    ("capability_invocations", "InnoDB"),
                    ("log_entries", "InnoDB"),
                ),
                "InnoDB tables",
            ),
            (
                (8, 0, 36),
                False,
                (
                    ("capability_invocations", "InnoDB"),
                    ("log_entries", "MyISAM"),
                    ("tasks", "InnoDB"),
                ),
                "InnoDB tables",
            ),
        ],
    )
    def test_mysql_runtime_requirements_fail_closed(
        self,
        version,
        is_mariadb,
        engines,
        error,
    ):
        module = _load_terminal_arbitration_migration(
            f"mysql_requirement_{error}"
        )
        bind = MagicMock()
        bind.dialect = SimpleNamespace(
            name="mysql",
            is_mariadb=is_mariadb,
            server_version_info=version,
        )
        bind.execute.return_value = engines
        fake_op = SimpleNamespace(
            get_context=lambda: SimpleNamespace(as_sql=False),
            get_bind=lambda: bind,
        )
        with patch.object(module, "op", fake_op):
            with pytest.raises(RuntimeError, match=error):
                module._require_supported_mysql()

    def test_mysql_runtime_requirements_accept_supported_innodb(self):
        module = _load_terminal_arbitration_migration("mysql_requirement_ok")
        bind = MagicMock()
        bind.dialect = SimpleNamespace(
            name="mysql",
            is_mariadb=False,
            server_version_info=(8, 4, 0),
        )
        bind.execute.return_value = (
            ("capability_invocations", "InnoDB"),
            ("log_entries", "InnoDB"),
            ("tasks", "InnoDB"),
        )
        fake_op = SimpleNamespace(
            get_context=lambda: SimpleNamespace(as_sql=False),
            get_bind=lambda: bind,
        )
        with patch.object(module, "op", fake_op):
            module._require_supported_mysql()

    def test_mysql_guard_reflection_requires_enforcement_and_exact_shapes(self):
        module = _load_terminal_arbitration_migration("mysql_guard_validation")
        state = _mysql_terminal_state(canonical="new")
        state["canonical_enforced"] = False
        with pytest.raises(RuntimeError, match="no enforceable"):
            module._assert_mysql_guarded(state)

        state = _mysql_terminal_state(canonical="old", shadow="new")
        state["canonical_enforced"] = False
        with pytest.raises(RuntimeError, match="canonical.*not enforced"):
            module._assert_mysql_guarded(state)

        state = _mysql_terminal_state(canonical="old", shadow="new")
        state["shadow_enforced"] = False
        with pytest.raises(RuntimeError, match="not enforced"):
            module._assert_mysql_guarded(state)

        state = _mysql_terminal_state(canonical="old", unique=True)
        state["unique"] = False
        with pytest.raises(RuntimeError, match="unique constraint is malformed"):
            module._assert_mysql_guarded(state)

        state = _mysql_terminal_state(canonical="old")
        state["column_shapes"]["request_output_hash"] = False
        with pytest.raises(RuntimeError, match="column shape is malformed"):
            module._assert_mysql_guarded(state)

    @pytest.mark.parametrize(
        ("transport_sql", "enforced", "should_pass"),
        [
            (
                "actual_transport IS NULL OR (turn_scope IS NOT NULL AND "
                "turn_scope = 'source' AND "
                "actual_transport IN ('claude_pty', 'claude_exec', "
                "'codex_app_server', 'codex_exec'))",
                {
                    "ck_log_entries_turn_scope",
                    "ck_log_entries_actual_transport",
                },
                True,
            ),
            (
                "actual_transport IS NULL OR actual_transport IN "
                "('claude_pty', 'claude_exec', 'codex_app_server', "
                "'codex_exec')",
                {
                    "ck_log_entries_turn_scope",
                    "ck_log_entries_actual_transport",
                },
                False,
            ),
            (
                # This weaker expression admits a valid transport with NULL
                # scope because a SQL CHECK treats UNKNOWN as satisfied.
                "actual_transport IS NULL OR (turn_scope = 'source' AND "
                "actual_transport IN ('claude_pty', 'claude_exec', "
                "'codex_app_server', 'codex_exec'))",
                {
                    "ck_log_entries_turn_scope",
                    "ck_log_entries_actual_transport",
                },
                False,
            ),
            (
                "actual_transport IS NULL OR (turn_scope IS NOT NULL AND "
                "turn_scope = 'source' AND "
                "actual_transport IN ('claude_pty', 'claude_exec', "
                "'codex_app_server', 'codex_exec'))",
                {"ck_log_entries_turn_scope"},
                False,
            ),
        ],
    )
    def test_mysql_actual_transport_reflection_requires_source_scope_and_enforcement(
        self,
        transport_sql,
        enforced,
        should_pass,
    ):
        module = _load_terminal_arbitration_migration(
            f"mysql_actual_transport_{should_pass}_{len(enforced)}"
        )
        inspector = MagicMock()

        def columns(table):
            if table == "tasks":
                return [
                    {
                        "name": "turn_source_log_id",
                        "type": mysql.INTEGER(),
                        "nullable": True,
                    }
                ]
            assert table == "log_entries"
            return [
                {
                    "name": "turn_scope",
                    "type": mysql.VARCHAR(length=16),
                    "nullable": True,
                },
                {
                    "name": "actual_transport",
                    "type": mysql.VARCHAR(length=24),
                    "nullable": True,
                },
            ]

        inspector.get_columns.side_effect = columns
        inspector.get_check_constraints.return_value = [
            {
                "name": "ck_log_entries_turn_scope",
                "sqltext": module._TURN_SCOPE_CHECK,
            },
            {
                "name": "ck_log_entries_actual_transport",
                "sqltext": transport_sql,
            },
        ]
        fake_op = SimpleNamespace(get_bind=MagicMock())
        with (
            patch.object(module, "op", fake_op),
            patch.object(module.sa, "inspect", return_value=inspector),
            patch.object(
                module,
                "_mysql_enforced_checks",
                return_value=enforced,
            ),
            patch.object(module, "_mysql_alter") as alter,
        ):
            if should_pass:
                module._mysql_upgrade_auxiliary_online()
                alter.assert_called_once_with("log_entries", [])
            else:
                with pytest.raises(
                    RuntimeError,
                    match="actual-transport CHECK.*malformed or not enforced",
                ):
                    module._mysql_upgrade_auxiliary_online()


class TestCapabilityResumeOutboxDialectMigration:
    def test_postgresql_offline_sql_fences_preflight_before_ddl(self):
        module = _load_capability_resume_outbox_migration("postgresql_offline")

        from alembic.migration import MigrationContext
        from alembic.operations import Operations

        upgrade_output = io.StringIO()
        upgrade_context = MigrationContext.configure(
            dialect_name="postgresql",
            opts={"as_sql": True, "output_buffer": upgrade_output},
        )
        with patch.object(module, "op", Operations(upgrade_context)):
            module.upgrade()
        upgrade_ddl = upgrade_output.getvalue().lower()
        upgrade_lock = (
            "lock table capability_invocations in access exclusive mode"
        )
        upgrade_guard = "do $ccm_capability_resume_outbox_upgrade$"
        first_upgrade_ddl = min(
            upgrade_ddl.index("alter table"),
            upgrade_ddl.index("create table capability_resume_outbox"),
        )
        assert upgrade_ddl.index(upgrade_lock) < upgrade_ddl.index(upgrade_guard)
        assert upgrade_ddl.index(upgrade_guard) < first_upgrade_ddl
        assert "where source = 'agent_request'" in upgrade_ddl
        assert "exact identities cannot be reconstructed" in upgrade_ddl

        downgrade_output = io.StringIO()
        downgrade_context = MigrationContext.configure(
            dialect_name="postgresql",
            opts={"as_sql": True, "output_buffer": downgrade_output},
        )
        with patch.object(module, "op", Operations(downgrade_context)):
            module.downgrade()
        downgrade_ddl = downgrade_output.getvalue().lower()
        downgrade_lock = (
            "lock table capability_invocations, capability_resume_outbox "
            "in access exclusive mode"
        )
        guard = "do $ccm_capability_resume_outbox$"
        first_drop = min(
            downgrade_ddl.index("drop index"),
            downgrade_ddl.index("drop table"),
        )
        assert downgrade_ddl.index(downgrade_lock) < downgrade_ddl.index(guard)
        assert downgrade_ddl.index(guard) < first_drop
        assert "select 1 from capability_resume_outbox" in downgrade_ddl
        assert "where source = 'agent_request'" in downgrade_ddl
        assert downgrade_ddl.count("raise exception") == 2

    @pytest.mark.parametrize("direction", ("upgrade", "downgrade"))
    def test_mysql_offline_sql_is_refused_before_output(self, direction):
        module = _load_capability_resume_outbox_migration(
            f"mysql_offline_{direction}"
        )

        from alembic.migration import MigrationContext
        from alembic.operations import Operations

        output = io.StringIO()
        context = MigrationContext.configure(
            dialect_name="mysql",
            opts={"as_sql": True, "output_buffer": output},
        )
        with (
            patch.object(module, "op", Operations(context)),
            pytest.raises(RuntimeError, match="refuses MySQL offline SQL"),
        ):
            getattr(module, direction)()
        assert output.getvalue() == ""

    def test_mysql_check_reflection_accepts_charset_introducers_only(self):
        module = _load_capability_resume_outbox_migration(
            "mysql_charset_reflection"
        )
        reflected = module._NEW_AGENT_REQUEST_IDENTITY
        for literal in ("agent_request", "advisory", "resume_task"):
            reflected = reflected.replace(
                f"'{literal}'",
                f"_utf8mb4'{literal}'",
            )
        reflected = f"CHECK ((({reflected.replace('source <>', '`source` <>', 1)})))"

        assert module._check_shape(reflected) == module._check_shape(
            module._NEW_AGENT_REQUEST_IDENTITY
        )
        assert module._check_shape(
            reflected.replace(
                "request_protocol_version >= 1",
                "request_protocol_version >= 0",
            )
        ) != module._check_shape(module._NEW_AGENT_REQUEST_IDENTITY)

    def test_mysql_partial_outbox_preflight_precedes_capability_ddl(self):
        module = _load_capability_resume_outbox_migration(
            "mysql_partial_outbox_preflight"
        )
        with (
            patch.object(
                module,
                "_mysql_capability_state",
                side_effect=("old", "new"),
            ),
            patch.object(
                module,
                "_mysql_outbox_state",
                side_effect=RuntimeError("partial column set"),
            ),
            patch.object(module, "_assert_zero_agent_requests"),
            patch.object(module, "_mysql_alter_capability") as alter,
            patch.object(module, "_create_outbox_table") as create_table,
            pytest.raises(RuntimeError, match="partial column set"),
        ):
            module._upgrade_mysql()
        alter.assert_not_called()
        create_table.assert_not_called()

    def test_mysql_legacy_preflight_rejects_before_any_ddl(self):
        module = _load_capability_resume_outbox_migration(
            "mysql_legacy_preflight"
        )
        with (
            patch.object(module, "_mysql_capability_state", return_value="old"),
            patch.object(module, "_mysql_outbox_state", return_value=False),
            patch.object(
                module,
                "_assert_zero_agent_requests",
                side_effect=RuntimeError("zero agent requests required"),
            ),
            patch.object(module, "_mysql_alter_capability") as alter,
            patch.object(module, "_create_outbox_table") as create_table,
            patch.object(module, "op") as fake_op,
            pytest.raises(RuntimeError, match="zero agent requests required"),
        ):
            module._upgrade_mysql()
        alter.assert_not_called()
        create_table.assert_not_called()
        fake_op.create_index.assert_not_called()

    def test_mysql_missing_index_suffix_replays_without_recreating_table(self):
        module = _load_capability_resume_outbox_migration(
            "mysql_outbox_index_replay"
        )
        due = "ix_cap_resume_outbox_due"
        reflected_indexes = [
            {"ix_cap_resume_outbox_task_created": ("task_id", "created_at")},
            dict(module._OUTBOX_INDEXES),
        ]
        with (
            patch.object(module, "_mysql_capability_state", return_value="new"),
            patch.object(module, "_mysql_outbox_state", return_value=True),
            patch.object(
                module,
                "_mysql_outbox_indexes",
                side_effect=reflected_indexes,
            ),
            patch.object(module, "op") as fake_op,
            patch.object(module, "_mysql_alter_capability") as alter,
            patch.object(module, "_create_outbox_table") as create_table,
        ):
            module._upgrade_mysql()
        fake_op.create_index.assert_called_once_with(
            due,
            "capability_resume_outbox",
            ["status", "next_attempt_at"],
            unique=False,
        )
        alter.assert_not_called()
        create_table.assert_not_called()

    def test_mysql_completed_capability_alter_replays_missing_outbox_only(self):
        module = _load_capability_resume_outbox_migration(
            "mysql_outbox_create_replay"
        )
        reflected_indexes = [
            {},
            dict(module._OUTBOX_INDEXES),
        ]
        with (
            patch.object(module, "_mysql_capability_state", return_value="new"),
            patch.object(
                module,
                "_mysql_outbox_state",
                side_effect=(False, True),
            ) as outbox_state,
            patch.object(
                module,
                "_mysql_outbox_indexes",
                side_effect=reflected_indexes,
            ),
            patch.object(module, "op") as fake_op,
            patch.object(module, "_mysql_alter_capability") as alter,
            patch.object(module, "_create_outbox_table") as create_table,
        ):
            module._upgrade_mysql()
        create_table.assert_called_once_with()
        assert outbox_state.call_count == 2
        alter.assert_not_called()
        assert fake_op.create_index.call_count == len(module._OUTBOX_INDEXES)
        for name, columns in module._OUTBOX_INDEXES.items():
            fake_op.create_index.assert_any_call(
                name,
                "capability_resume_outbox",
                list(columns),
                unique=False,
            )

    def test_mysql_downgrade_never_drops_before_both_writer_gates(self):
        module = _load_capability_resume_outbox_migration(
            "mysql_downgrade_gate_order"
        )
        events: list[str] = []
        capability_checks: set[str] = set()
        outbox_checks: set[str] = set(module._OUTBOX_CHECKS)

        def alter(actions):
            joined = " ".join(actions)
            if joined.startswith(
                f"ADD CONSTRAINT {module._MYSQL_CAPABILITY_GATE}"
            ):
                capability_checks.add(module._MYSQL_CAPABILITY_GATE)
                events.append("capability_gate")
            else:
                capability_checks.discard(module._MYSQL_CAPABILITY_GATE)
                events.append("capability_downgrade")

        def execute(statement):
            assert module._MYSQL_OUTBOX_GATE in str(statement)
            outbox_checks.add(module._MYSQL_OUTBOX_GATE)
            events.append("outbox_gate")

        def drop_table(table_name):
            assert table_name == "capability_resume_outbox"
            assert events[:2] == ["capability_gate", "outbox_gate"]
            events.append("drop_outbox")

        fake_op = SimpleNamespace(execute=execute, drop_table=drop_table)
        with (
            patch.object(module, "_mysql_capability_state", side_effect=("new", "old")),
            patch.object(module, "_mysql_outbox_state", return_value=True),
            patch.object(
                module,
                "_mysql_capability_has_gate",
                side_effect=lambda: module._MYSQL_CAPABILITY_GATE
                in capability_checks,
            ),
            patch.object(
                module,
                "_mysql_outbox_has_gate",
                side_effect=lambda: module._MYSQL_OUTBOX_GATE in outbox_checks,
            ),
            patch.object(module, "_assert_downgrade_empty"),
            patch.object(module, "_mysql_alter_capability", side_effect=alter),
            patch.object(module, "op", fake_op),
        ):
            module._downgrade_mysql()

        assert events == [
            "capability_gate",
            "outbox_gate",
            "drop_outbox",
            "capability_downgrade",
        ]


class TestCodeReviewMigration:
    def test_refuses_review_history_or_reviewer_task_downgrade(self, tmp_path):
        db_path = str(tmp_path / "code-review-downgrade-guard.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, CODE_REVIEW_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        digest = "b" * 64
        sha = "c" * 40

        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO code_review_runs "
                "(capability_invocation_id, capability_execution_id, attempt, "
                "developer_task_id, reviewer_task_id, reviewer_task_retry_count, "
                "repo_path, base_sha, head_sha, head_tree_sha, patch_sha256, "
                "subject_ref, subject_hash, prompt_hash, created_at, updated_at) "
                "VALUES (1, 1, 1, 1, 2, 0, '/repo', :sha, :sha, :sha, "
                ":digest, '{}', :digest, :digest, "
                "'2026-08-05 00:00:00', '2026-08-05 00:00:00')"
            ), {"sha": sha, "digest": digest})
        with pytest.raises(RuntimeError, match="code_review_runs contains history"):
            _run_alembic(cfg, command.downgrade, CAPABILITY_CORE_REVISION)
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM code_review_runs"))
            conn.execute(text(
                "INSERT INTO tasks "
                "(title, description, status, priority, target_branch, "
                "merge_status, retry_count, max_retries, mode, tags, metadata, "
                "created_at) VALUES ('orphan reviewer', 'd', 'pending', 0, "
                "'main', 'pending', 0, 0, 'auto', "
                ":tags, :metadata, '2026-08-05 00:00:00')"
            ), {
                "tags": '["pre-pr-code-review"]',
                "metadata": (
                    '{"code_review_run_id":1,"capability_invocation_id":1,'
                    '"capability_execution_id":1}'
                ),
            })
        with pytest.raises(RuntimeError, match="retains reviewer ownership"):
            _run_alembic(cfg, command.downgrade, CAPABILITY_CORE_REVISION)
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM tasks WHERE title = 'orphan reviewer'"))
        engine.dispose()

        _run_alembic(cfg, command.downgrade, CAPABILITY_CORE_REVISION)


class TestPlanRuntimeReceiptMigration:
    @staticmethod
    def _assert_receipt_schema(engine) -> None:
        inspector = inspect(engine)
        assert "plan_agent_runtime_receipts" in inspector.get_table_names()
        run_columns = {
            column["name"]: column
            for column in inspector.get_columns("plan_agent_runs")
        }
        receipt_columns = {
            column["name"]: column
            for column in inspector.get_columns("plan_agent_runtime_receipts")
        }
        assert "cancellation_target_generation" in run_columns
        assert {
            "run_id",
            "step_id",
            "run_generation",
            "attempt_index",
            "runtime_token",
            "prepared_boot_id",
            "prepared_start_ticks",
            "prepared_uid",
            "process_start_ticks",
        }.issubset(receipt_columns)
        assert isinstance(
            receipt_columns["prepared_start_ticks"]["type"], BigInteger
        )
        assert isinstance(
            receipt_columns["process_start_ticks"]["type"], BigInteger
        )
        assert isinstance(
            Base.metadata.tables["plan_agent_runtime_receipts"]
            .c.process_start_ticks.type,
            BigInteger,
        )
        assert receipt_columns["runtime_token"]["type"].length == 32

    @staticmethod
    def _insert_run_and_step(
        conn,
        *,
        run_status="waiting_user",
        step_status="completed",
        worker_id: int | None = None,
    ):
        plan_id = None
        if worker_id is not None:
            plan_id = conn.execute(
                text(
                    "INSERT INTO plans "
                    "(title, initial_request, worker_id, priority, "
                    "pipeline_config, lock_version, created_at, updated_at) "
                    "VALUES ('legacy Worker mirror', 'plan', :worker_id, 0, "
                    "'{}', 0, '2026-08-08 00:00:00', "
                    "'2026-08-08 00:00:01') RETURNING id"
                ),
                {"worker_id": worker_id},
            ).scalar_one()
        run_id = conn.execute(
            text(
                "INSERT INTO plan_agent_runs "
                "(plan_id, run_type, current_stage, generation, interaction_count, "
                "max_interactions, execution_seconds, status, round, worker_id, "
                "review_exhausted, created_at, updated_at, finished_at) VALUES "
                "(:plan_id, 'initial', 'planner', 4, 0, 3, 0, :run_status, 1, "
                ":worker_id, 0, '2026-08-08 00:00:00', "
                "'2026-08-08 00:00:01', CASE WHEN :run_status IN "
                "('completed', 'failed', 'cancelled') THEN "
                "'2026-08-08 00:00:01' ELSE NULL END) RETURNING id"
            ),
            {
                "plan_id": plan_id,
                "run_status": run_status,
                "worker_id": worker_id,
            },
        ).scalar_one()
        step_id = conn.execute(
            text(
                "INSERT INTO plan_agent_steps "
                "(run_id, plan_id, worker_id, worker_step_id, generation, step_type, "
                "round, provider, status, "
                "streamed_output_chars, started_at, finished_at) VALUES "
                "(:run_id, :plan_id, :worker_id, :worker_step_id, 4, 'planner', 1, "
                "'claude', :step_status, 0, "
                "'2026-08-08 00:00:00', "
                "CASE WHEN :step_status = 'running' THEN NULL "
                "ELSE '2026-08-08 00:00:01' END) RETURNING id"
            ),
            {
                "run_id": run_id,
                "plan_id": plan_id,
                "worker_id": worker_id,
                "worker_step_id": 501 if worker_id is not None else None,
                "step_status": step_status,
            },
        ).scalar_one()
        return run_id, step_id

    def test_revision_backfills_terminal_steps_downgrades_and_reupgrades(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "plan-runtime-receipt-roundtrip.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, CAPABILITY_RESUME_OUTBOX_REVISION)

        engine = create_engine(f"sqlite:///{db_path}")
        assert "plan_agent_runtime_receipts" not in _get_all_tables(engine)
        assert "cancellation_target_generation" not in _get_table_columns(
            engine, "plan_agent_runs"
        )
        with engine.begin() as conn:
            _run_id, step_id = self._insert_run_and_step(conn)
        engine.dispose()

        _run_alembic(cfg, command.upgrade, PLAN_RUNTIME_RECEIPT_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        self._assert_receipt_schema(engine)
        large_start_ticks = 5_000_000_123
        with engine.begin() as conn:
            backfill = conn.execute(
                text(
                    "SELECT status, length(runtime_token), prepared_start_ticks, "
                    "cleaned_at FROM plan_agent_runtime_receipts WHERE step_id = :step_id"
                ),
                {"step_id": step_id},
            ).one()
            assert backfill == (
                "cleaned",
                32,
                0,
                "2026-08-08 00:00:01.000000",
            )
            with pytest.raises(IntegrityError):
                conn.execute(
                    text(
                        "INSERT INTO plan_agent_runtime_receipts "
                        "(run_id, step_id, run_generation, attempt_index, provider, "
                        "runtime_token, prepared_boot_id, prepared_start_ticks, "
                        "prepared_uid, status, created_at, updated_at) VALUES "
                        "(11, 12, 7, 1, 'claude', :token, :boot_id, :ticks, 1000, "
                        "'cleaned', '2026-08-08 00:00:00', "
                        "'2026-08-08 00:00:00')"
                    ),
                    {
                        "token": "a" * 32,
                        "boot_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                        "ticks": large_start_ticks,
                    },
                )
        engine.dispose()

        _run_alembic(cfg, command.downgrade, CAPABILITY_RESUME_OUTBOX_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "plan_agent_runtime_receipts" not in _get_all_tables(engine)
        assert "cancellation_target_generation" not in _get_table_columns(
            engine, "plan_agent_runs"
        )
        with engine.connect() as conn:
            assert conn.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == CAPABILITY_RESUME_OUTBOX_REVISION
        engine.dispose()

        _run_alembic(cfg, command.upgrade, PLAN_RUNTIME_RECEIPT_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        self._assert_receipt_schema(engine)
        with engine.connect() as conn:
            assert conn.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == PLAN_RUNTIME_RECEIPT_REVISION
            assert conn.execute(
                text("SELECT COUNT(*) FROM plan_agent_runtime_receipts")
            ).scalar_one() == 1
        engine.dispose()

    @pytest.mark.parametrize(
        "receipt_status",
        ["prepared", "admitting", "launching", "cleanup_failed", "cleaned"],
    )
    def test_downgrade_refuses_nonclean_or_malformed_receipt(
        self,
        tmp_path,
        receipt_status,
    ):
        db_path = str(tmp_path / f"plan-runtime-{receipt_status}-downgrade.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, PLAN_RUNTIME_RECEIPT_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            run_id, step_id = self._insert_run_and_step(
                conn,
                run_status="running",
                step_status="running",
            )
            conn.execute(text("PRAGMA ignore_check_constraints = ON"))
            conn.execute(
                text(
                    "INSERT INTO plan_agent_runtime_receipts "
                    "(run_id, step_id, run_generation, attempt_index, provider, "
                    "runtime_token, prepared_boot_id, prepared_start_ticks, "
                    "prepared_uid, status, process_id, process_group_id, "
                    "process_start_ticks, process_uid, boot_id, cleanup_error, "
                    "created_at, updated_at, cleaned_at) VALUES "
                    "(:run_id, :step_id, 4, 1, 'claude', :token, :boot_id, 1, 1000, "
                    ":status, :process_id, :process_group_id, :process_ticks, "
                    ":process_uid, :process_boot_id, :cleanup_error, "
                    "'2026-08-08 00:00:00', '2026-08-08 00:00:01', :cleaned_at)"
                ),
                {
                    "run_id": run_id,
                    "step_id": step_id,
                    "token": "a" * 32,
                    "boot_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                    "status": receipt_status,
                    "process_id": 42 if receipt_status == "launching" else None,
                    "process_group_id": 42 if receipt_status == "launching" else None,
                    "process_ticks": 2 if receipt_status == "launching" else None,
                    "process_uid": 1000 if receipt_status == "launching" else None,
                    "process_boot_id": (
                        "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
                        if receipt_status == "launching"
                        else None
                    ),
                    "cleanup_error": (
                        "cleanup uncertain"
                        if receipt_status == "cleanup_failed"
                        else None
                    ),
                    # The cleaned case is deliberately malformed.
                    "cleaned_at": None,
                },
            )
            conn.execute(text("PRAGMA ignore_check_constraints = OFF"))
        engine.dispose()

        with pytest.raises(
            RuntimeError,
            match="non-clean, malformed, or actively cancelling",
        ):
            _run_alembic(
                cfg,
                command.downgrade,
                CAPABILITY_RESUME_OUTBOX_REVISION,
            )

        engine = create_engine(f"sqlite:///{db_path}")
        assert "plan_agent_runtime_receipts" in _get_all_tables(engine)
        engine.dispose()

    def test_upgrade_rejects_active_legacy_step_without_identity(self, tmp_path):
        db_path = str(tmp_path / "plan-runtime-active-legacy-upgrade.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, CAPABILITY_RESUME_OUTBOX_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            self._insert_run_and_step(
                conn,
                run_status="running",
                step_status="running",
            )
        engine.dispose()

        with pytest.raises(
            RuntimeError,
            match="active or malformed legacy Plan Steps",
        ):
            _run_alembic(cfg, command.upgrade, PLAN_RUNTIME_RECEIPT_REVISION)

    def test_terminal_worker_mirror_gets_dispatch_not_manager_runtime_proof(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "plan-runtime-worker-mirror-upgrade.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, CAPABILITY_RESUME_OUTBOX_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            _run_id, step_id = self._insert_run_and_step(
                conn,
                run_status="completed",
                step_status="completed",
                worker_id=7,
            )
        engine.dispose()

        _run_alembic(cfg, command.upgrade, PLAN_RUNTIME_RECEIPT_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            assert conn.execute(
                text(
                    "SELECT COUNT(*) FROM plan_agent_runtime_receipts "
                    "WHERE step_id = :step_id"
                ),
                {"step_id": step_id},
            ).scalar_one() == 0
        engine.dispose()

        _run_alembic(
            cfg,
            command.upgrade,
            WORKER_PLAN_DISPATCH_RECEIPT_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            legacy = conn.execute(
                text(
                    "SELECT status, payload_digest, remote_status, "
                    "settlement_reason, run_generation, settled_at "
                    "FROM plan_agent_worker_dispatch_receipts"
                )
            ).one()
            assert legacy == (
                "settled",
                None,
                "completed",
                "legacy_terminal",
                4,
                "2026-08-08 00:00:01.000000",
            )
        engine.dispose()

        _run_alembic(cfg, command.downgrade, CAPABILITY_RESUME_OUTBOX_REVISION)

    def test_upgrade_rejects_active_worker_mirror_without_dispatch_identity(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "plan-runtime-active-worker-mirror.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, CAPABILITY_RESUME_OUTBOX_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            self._insert_run_and_step(
                conn,
                run_status="running",
                step_status="running",
                worker_id=7,
            )
        engine.dispose()

        with pytest.raises(
            RuntimeError,
            match="active or malformed legacy Worker Plan Runs",
        ):
            _run_alembic(cfg, command.upgrade, PLAN_RUNTIME_RECEIPT_REVISION)

    def test_downgrade_refuses_active_cancellation_generation(self, tmp_path):
        db_path = str(tmp_path / "plan-runtime-active-cancel-downgrade.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, PLAN_RUNTIME_RECEIPT_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO plan_agent_runs "
                    "(run_type, current_stage, generation, "
                    "cancellation_target_generation, interaction_count, "
                    "max_interactions, execution_seconds, status, round, "
                    "review_exhausted, created_at, updated_at) VALUES "
                    "('initial', 'planner', 5, 4, 0, 3, 0, 'cancelling', 1, 0, "
                    "'2026-08-08 00:00:00', '2026-08-08 00:00:01')"
                )
            )
        engine.dispose()

        with pytest.raises(
            RuntimeError,
            match="actively cancelling durable Plan runtime evidence",
        ):
            _run_alembic(
                cfg,
                command.downgrade,
                CAPABILITY_RESUME_OUTBOX_REVISION,
            )


class TestWorkerPlanDispatchReceiptMigration:
    @pytest.mark.parametrize(
        ("status", "payload_digest"),
        [
            ("prepared", None),
            ("remote_possible", "d" * 64),
        ],
    )
    def test_downgrade_refuses_unsettled_boundary_evidence(
        self,
        tmp_path,
        status,
        payload_digest,
    ):
        db_path = str(tmp_path / f"worker-plan-{status}-downgrade.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(
            cfg,
            command.upgrade,
            WORKER_PLAN_DISPATCH_RECEIPT_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO plan_agent_worker_dispatch_receipts "
                    "(plan_id, run_id, worker_id, run_generation, protocol, "
                    "status, payload_digest, created_at, updated_at) VALUES "
                    "(1, 2, 3, 4, 1, :status, :payload_digest, "
                    "'2026-08-08 00:00:00', '2026-08-08 00:00:00')"
                ),
                {"status": status, "payload_digest": payload_digest},
            )
        engine.dispose()

        with pytest.raises(
            RuntimeError,
            match="non-settled or malformed durable Worker Plan dispatch",
        ):
            _run_alembic(
                cfg,
                command.downgrade,
                PLAN_RUNTIME_RECEIPT_REVISION,
            )

        engine = create_engine(f"sqlite:///{db_path}")
        assert (
            "plan_agent_worker_dispatch_receipts" in _get_all_tables(engine)
        )
        with engine.connect() as conn:
            assert conn.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == WORKER_PLAN_DISPATCH_RECEIPT_REVISION
        engine.dispose()

    def test_revision_enforces_boundary_state_and_roundtrips(self, tmp_path):
        db_path = str(tmp_path / "worker-plan-dispatch-receipt.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, PLAN_RUNTIME_RECEIPT_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert (
            "plan_agent_worker_dispatch_receipts"
            not in _get_all_tables(engine)
        )
        engine.dispose()

        _run_alembic(
            cfg,
            command.upgrade,
            WORKER_PLAN_DISPATCH_RECEIPT_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        inspector = inspect(engine)
        assert "plan_agent_worker_dispatch_receipts" in inspector.get_table_names()
        columns = {
            item["name"]
            for item in inspector.get_columns(
                "plan_agent_worker_dispatch_receipts"
            )
        }
        assert {
            "plan_id",
            "run_id",
            "target_task_id",
            "worker_id",
            "run_generation",
            "protocol",
            "status",
            "payload_digest",
            "settlement_reason",
            "settled_at",
        }.issubset(columns)
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO plan_agent_worker_dispatch_receipts "
                "(plan_id, run_id, worker_id, run_generation, protocol, status, "
                "created_at, updated_at) VALUES "
                "(1, 2, 3, 4, 1, 'prepared', "
                "'2026-08-08 00:00:00', '2026-08-08 00:00:00')"
            ))
            conn.execute(text(
                "INSERT INTO plan_agent_worker_dispatch_receipts "
                "(plan_id, run_id, worker_id, run_generation, protocol, status, "
                "payload_digest, created_at, updated_at) VALUES "
                "(1, 2, 3, 5, 1, 'remote_possible', :digest, "
                "'2026-08-08 00:00:00', '2026-08-08 00:00:00')"
            ), {"digest": "a" * 64})
            conn.execute(text(
                "INSERT INTO plan_agent_worker_dispatch_receipts "
                "(plan_id, run_id, worker_id, run_generation, protocol, status, "
                "settlement_reason, created_at, updated_at, settled_at) VALUES "
                "(1, 2, 3, 6, 1, 'settled', 'not_launched', "
                "'2026-08-08 00:00:00', '2026-08-08 00:00:00', "
                "'2026-08-08 00:00:01')"
            ))
        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(text(
                    "INSERT INTO plan_agent_worker_dispatch_receipts "
                    "(plan_id, run_id, worker_id, run_generation, protocol, "
                    "status, payload_digest, created_at, updated_at) VALUES "
                    "(1, 2, 3, 7, 1, 'prepared', :digest, "
                    "'2026-08-08 00:00:00', '2026-08-08 00:00:00')"
                ), {"digest": "b" * 64})
        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(text(
                    "INSERT INTO plan_agent_worker_dispatch_receipts "
                    "(plan_id, run_id, worker_id, run_generation, protocol, "
                    "status, payload_digest, created_at, updated_at) VALUES "
                    "(1, 2, 3, 8, 1, 'remote_possible', :digest, "
                    "'2026-08-08 00:00:00', '2026-08-08 00:00:00')"
                ), {"digest": "A" * 64})
        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(text(
                    "INSERT INTO plan_agent_worker_dispatch_receipts "
                    "(plan_id, run_id, worker_id, run_generation, protocol, "
                    "status, payload_digest, remote_status, settlement_reason, "
                    "created_at, updated_at, settled_at) VALUES "
                    "(1, 2, 3, 9, 1, 'settled', :digest, 'completed', "
                    "'remote_pause', '2026-08-08 00:00:00', "
                    "'2026-08-08 00:00:00', '2026-08-08 00:00:01')"
                ), {"digest": "z" * 64})
        engine.dispose()

        with pytest.raises(
            RuntimeError,
            match="non-settled or malformed durable Worker Plan dispatch",
        ):
            _run_alembic(
                cfg,
                command.downgrade,
                PLAN_RUNTIME_RECEIPT_REVISION,
            )
        engine = create_engine(f"sqlite:///{db_path}")
        assert (
            "plan_agent_worker_dispatch_receipts" in _get_all_tables(engine)
        )
        with engine.begin() as conn:
            assert conn.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == WORKER_PLAN_DISPATCH_RECEIPT_REVISION
            conn.execute(text(
                "DELETE FROM plan_agent_worker_dispatch_receipts "
                "WHERE status != 'settled'"
            ))
        engine.dispose()

        _run_alembic(cfg, command.downgrade, PLAN_RUNTIME_RECEIPT_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert (
            "plan_agent_worker_dispatch_receipts"
            not in _get_all_tables(engine)
        )
        engine.dispose()


class TestWorkerTaskDeleteReceiptMigration:
    @staticmethod
    def _insert_task(connection, *, title: str) -> int:
        connection.execute(
            text(
                "INSERT INTO tasks "
                "(title, description, status, priority, target_branch, "
                "merge_status, retry_count, max_retries, mode, created_at) "
                "VALUES (:title, 'd', 'completed', 0, 'main', 'pending', "
                "0, 2, 'auto', '2026-08-08 00:00:00')"
            ),
            {"title": title},
        )
        return connection.execute(
            text("SELECT id FROM tasks WHERE title = :title"),
            {"title": title},
        ).scalar_one()

    @staticmethod
    def _insert_manager_receipt(
        connection,
        *,
        task_id: int,
        operation: str,
        source_task_status: str,
    ) -> None:
        connection.execute(
            text(
                "INSERT INTO worker_task_termination_receipts "
                "(operation_id, task_id, active_task_id, side, worker_id, "
                "operation, status, source_task_status, "
                "source_task_retry_count, source_task_turn_generation, "
                "request_payload, request_digest, created_at, updated_at) "
                "VALUES (:operation_id, :task_id, :task_id, 'manager', 1, "
                ":operation, 'pending_remote', :source_task_status, 0, 0, "
                "'{}', :digest, '2026-08-08 00:00:00', "
                "'2026-08-08 00:00:00')"
            ),
            {
                "operation_id": "d" * 32,
                "task_id": task_id,
                "operation": operation,
                "source_task_status": source_task_status,
                "digest": "e" * 64,
            },
        )

    def test_revision_roundtrips_and_enforces_manager_only_delete(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "worker-task-delete-receipt-roundtrip.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(
            cfg,
            command.upgrade,
            WORKER_PLAN_DISPATCH_RECEIPT_REVISION,
        )

        engine = create_engine(f"sqlite:///{db_path}")
        old_checks = {
            item["name"]: item["sqltext"]
            for item in inspect(engine).get_check_constraints(
                "worker_task_termination_receipts"
            )
        }
        assert "delete" not in old_checks["ck_worker_task_term_operation"]
        assert (
            "superseded"
            not in old_checks["ck_worker_task_term_source_status"]
        )
        assert "ck_worker_task_term_delete_manager_only" not in old_checks
        engine.dispose()

        _run_alembic(
            cfg,
            command.upgrade,
            WORKER_TASK_DELETE_RECEIPT_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        new_checks = {
            item["name"]: item["sqltext"]
            for item in inspect(engine).get_check_constraints(
                "worker_task_termination_receipts"
            )
        }
        assert "delete" in new_checks["ck_worker_task_term_operation"]
        assert (
            "superseded"
            in new_checks["ck_worker_task_term_source_status"]
        )
        assert "ck_worker_task_term_delete_manager_only" in new_checks
        with engine.begin() as connection:
            task_id = self._insert_task(
                connection,
                title="worker-side delete constraint",
            )
        with pytest.raises(IntegrityError):
            with engine.begin() as connection:
                connection.execute(
                    text(
                        "INSERT INTO worker_task_termination_receipts "
                        "(operation_id, task_id, active_task_id, side, "
                        "operation, status, source_task_status, "
                        "source_task_retry_count, "
                        "source_task_turn_generation, request_payload, "
                        "request_digest, accepted_at, created_at, updated_at) "
                        "VALUES (:operation_id, :task_id, :task_id, 'worker', "
                        "'delete', 'accepted', 'completed', 0, 0, '{}', "
                        ":digest, '2026-08-08 00:00:00', "
                        "'2026-08-08 00:00:00', '2026-08-08 00:00:00')"
                    ),
                    {
                        "operation_id": "f" * 32,
                        "task_id": task_id,
                        "digest": "a" * 64,
                    },
                )
        engine.dispose()

        _run_alembic(
            cfg,
            command.downgrade,
            WORKER_PLAN_DISPATCH_RECEIPT_REVISION,
        )
        _run_alembic(
            cfg,
            command.upgrade,
            WORKER_TASK_DELETE_RECEIPT_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as connection:
            assert connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == WORKER_TASK_DELETE_RECEIPT_REVISION
        engine.dispose()

    @pytest.mark.parametrize(
        ("operation", "source_task_status"),
        [
            ("delete", "completed"),
            ("supersede", "superseded"),
        ],
    )
    def test_downgrade_refuses_rows_old_constraints_cannot_represent(
        self,
        tmp_path,
        operation,
        source_task_status,
    ):
        db_path = str(
            tmp_path
            / f"worker-task-delete-{operation}-{source_task_status}.db"
        )
        cfg = _alembic_cfg(db_path)
        _run_alembic(
            cfg,
            command.upgrade,
            WORKER_TASK_DELETE_RECEIPT_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as connection:
            task_id = self._insert_task(
                connection,
                title=f"downgrade guard {operation} {source_task_status}",
            )
            self._insert_manager_receipt(
                connection,
                task_id=task_id,
                operation=operation,
                source_task_status=source_task_status,
            )
        engine.dispose()

        with pytest.raises(
            RuntimeError,
            match="delete operations or superseded source status",
        ):
            _run_alembic(
                cfg,
                command.downgrade,
                WORKER_PLAN_DISPATCH_RECEIPT_REVISION,
            )

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as connection:
            assert connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == WORKER_TASK_DELETE_RECEIPT_REVISION
            connection.execute(
                text("DELETE FROM worker_task_termination_receipts")
            )
        engine.dispose()

        _run_alembic(
            cfg,
            command.downgrade,
            WORKER_PLAN_DISPATCH_RECEIPT_REVISION,
        )


class TestWorkerPlanImportReceiptMigration:
    @staticmethod
    def _insert_imported_graph(
        connection,
        *,
        plan_origin="manager_v1",
        include_receipt_protocol=False,
    ):
        plan_id = connection.execute(
            text(
                "INSERT INTO plans "
                "(title, initial_request, worker_id, relay_origin, priority, "
                "pipeline_config, lock_version, created_at, updated_at) VALUES "
                "('imported', 'plan', NULL, :plan_origin, 0, '{}', 0, "
                "'2026-08-08 00:00:00', '2026-08-08 00:00:01') RETURNING id"
            ),
            {"plan_origin": plan_origin},
        ).scalar_one()
        protocol_column = (
            ", import_receipt_protocol" if include_receipt_protocol else ""
        )
        protocol_value = ", 1" if include_receipt_protocol else ""
        run_id = connection.execute(
            text(
                "INSERT INTO plan_agent_runs "
                "(plan_id, run_type, current_stage, generation, "
                "interaction_count, max_interactions, execution_seconds, "
                "status, round, worker_id, relay_origin, "
                "import_payload_digest, review_exhausted, created_at, "
                f"updated_at, finished_at{protocol_column}) VALUES "
                "(:plan_id, 'initial', 'completed', 0, 0, 3, 0, "
                "'completed', 1, NULL, 'manager_v1', :digest, 0, "
                "'2026-08-08 00:00:00', '2026-08-08 00:00:01', "
                f"'2026-08-08 00:00:01'{protocol_value}) RETURNING id"
            ),
            {"plan_id": plan_id, "digest": "a" * 64},
        ).scalar_one()
        return plan_id, run_id

    def test_backfill_constraints_and_clean_roundtrip(self, tmp_path):
        db_path = str(tmp_path / "worker-plan-import-receipt-roundtrip.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, WORKER_TASK_DELETE_RECEIPT_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            plan_id, run_id = self._insert_imported_graph(conn)
        engine.dispose()

        _run_alembic(cfg, command.upgrade, WORKER_PLAN_IMPORT_RECEIPT_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        inspector = inspect(engine)
        assert "plan_agent_worker_import_receipts" in inspector.get_table_names()
        assert {
            "run_id",
            "plan_id",
            "protocol",
            "relay_origin",
            "payload_digest",
            "outcome",
            "created_at",
        } == {
            column["name"]
            for column in inspector.get_columns(
                "plan_agent_worker_import_receipts"
            )
        }
        with engine.connect() as conn:
            assert conn.execute(
                text(
                    "SELECT run_id, plan_id, protocol, relay_origin, "
                    "payload_digest, outcome FROM "
                    "plan_agent_worker_import_receipts"
                )
            ).one() == (
                run_id,
                plan_id,
                1,
                "manager_v1",
                "a" * 64,
                "imported",
            )
            assert conn.execute(
                text(
                    "SELECT import_receipt_protocol FROM plan_agent_runs "
                    "WHERE id = :run_id"
                ),
                {"run_id": run_id},
            ).scalar_one() == 1
        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "INSERT INTO plan_agent_worker_import_receipts "
                        "(run_id, plan_id, protocol, relay_origin, "
                        "payload_digest, outcome, created_at) VALUES "
                        "(999, 999, 1, 'manager_v1', :digest, 'imported', "
                        "'2026-08-08 00:00:00')"
                    ),
                    {"digest": "A" * 64},
                )
        for bad_run_id, bad_plan_id in ((0, 999), (998, 0), (-997, 999)):
            with pytest.raises(IntegrityError):
                with engine.begin() as conn:
                    conn.execute(
                        text(
                            "INSERT INTO plan_agent_worker_import_receipts "
                            "(run_id, plan_id, protocol, relay_origin, "
                            "payload_digest, outcome, created_at) VALUES "
                            "(:run_id, :plan_id, 1, 'manager_v1', :digest, "
                            "'imported', '2026-08-08 00:00:00')"
                        ),
                        {
                            "run_id": bad_run_id,
                            "plan_id": bad_plan_id,
                            "digest": "a" * 64,
                        },
                    )
        engine.dispose()

        _run_alembic(cfg, command.downgrade, WORKER_TASK_DELETE_RECEIPT_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "plan_agent_worker_import_receipts" not in _get_all_tables(engine)
        engine.dispose()
        _run_alembic(cfg, command.upgrade, WORKER_PLAN_IMPORT_RECEIPT_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            assert conn.execute(
                text("SELECT COUNT(*) FROM plan_agent_worker_import_receipts")
            ).scalar_one() == 1
        engine.dispose()

    def test_run_gate_rejects_old_importer_without_receipt_protocol(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "worker-plan-import-old-writer-gate.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, WORKER_PLAN_IMPORT_RECEIPT_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")

        # The legacy importer still writes relay_origin/import digest but does
        # not know the receipt protocol marker.  SQL CHECK must evaluate to
        # FALSE (not UNKNOWN, which databases accept) for that exact shape.
        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                self._insert_imported_graph(conn)

        with engine.begin() as conn:
            plan_id = conn.execute(
                text(
                    "INSERT INTO plans "
                    "(title, initial_request, worker_id, relay_origin, "
                    "priority, pipeline_config, lock_version, created_at, "
                    "updated_at) VALUES ('local', 'plan', NULL, NULL, 0, "
                    "'{}', 0, '2026-08-08 00:00:00', "
                    "'2026-08-08 00:00:01') RETURNING id"
                )
            ).scalar_one()
            with pytest.raises(IntegrityError):
                conn.execute(
                    text(
                        "INSERT INTO plan_agent_runs "
                        "(plan_id, run_type, current_stage, generation, "
                        "interaction_count, max_interactions, execution_seconds, "
                        "status, round, relay_origin, import_receipt_protocol, "
                        "review_exhausted, created_at, updated_at) VALUES "
                        "(:plan_id, 'initial', 'planner', 0, 0, 3, 0, "
                        "'queued', 1, NULL, 1, 0, "
                        "'2026-08-08 00:00:00', '2026-08-08 00:00:01')"
                    ),
                    {"plan_id": plan_id},
                )
            ordinary_run_id = conn.execute(
                text(
                    "INSERT INTO plan_agent_runs "
                    "(plan_id, run_type, current_stage, generation, "
                    "interaction_count, max_interactions, execution_seconds, "
                    "status, round, relay_origin, review_exhausted, "
                    "created_at, updated_at) VALUES "
                    "(:plan_id, 'initial', 'planner', 0, 0, 3, 0, "
                    "'queued', 1, NULL, 0, '2026-08-08 00:00:00', "
                    "'2026-08-08 00:00:01') RETURNING id"
                ),
                {"plan_id": plan_id},
            ).scalar_one()
            assert conn.execute(
                text(
                    "SELECT import_receipt_protocol FROM plan_agent_runs "
                    "WHERE id = :run_id"
                ),
                {"run_id": ordinary_run_id},
            ).scalar_one_or_none() is None
        engine.dispose()

    @pytest.mark.parametrize("downgrade", (False, True))
    def test_sqlite_preflight_fence_blocks_a_second_writer(
        self,
        tmp_path,
        downgrade,
    ):
        db_path = str(
            tmp_path / f"worker-plan-import-sqlite-fence-{downgrade}.db"
        )
        cfg = _alembic_cfg(db_path)
        target = (
            WORKER_PLAN_IMPORT_RECEIPT_REVISION
            if downgrade
            else WORKER_TASK_DELETE_RECEIPT_REVISION
        )
        _run_alembic(cfg, command.upgrade, target)
        module = _load_worker_plan_import_receipt_migration(
            f"sqlite_fence_{downgrade}"
        )

        from alembic.migration import MigrationContext
        from alembic.operations import Operations

        engine = create_engine(
            f"sqlite:///{db_path}",
            connect_args={"timeout": 0},
        )
        first = engine.connect()
        transaction = first.begin()
        context = MigrationContext.configure(connection=first)
        try:
            with patch.object(module, "op", Operations(context)):
                module._acquire_transactional_fence(downgrade=downgrade)
                with engine.connect() as second:
                    second.exec_driver_sql("PRAGMA busy_timeout = 0")
                    with pytest.raises(OperationalError, match="locked"):
                        second.execute(
                            text(
                                "UPDATE alembic_version "
                                "SET version_num = version_num"
                            )
                        )
        finally:
            transaction.rollback()
            first.close()
            engine.dispose()

    @pytest.mark.parametrize("direction", ("upgrade", "downgrade"))
    def test_mysql_offline_sql_is_refused_before_output(self, direction):
        module = _load_worker_plan_import_receipt_migration(
            f"mysql_offline_{direction}"
        )

        from alembic.migration import MigrationContext
        from alembic.operations import Operations

        output = io.StringIO()
        context = MigrationContext.configure(
            dialect_name="mysql",
            opts={"as_sql": True, "output_buffer": output},
        )
        with (
            patch.object(module, "op", Operations(context)),
            pytest.raises(RuntimeError, match="refuses MySQL offline SQL"),
        ):
            getattr(module, direction)()
        assert output.getvalue() == ""

    def test_postgresql_offline_fences_before_preflight_and_ddl(self):
        module = _load_worker_plan_import_receipt_migration("postgresql_fence")

        from alembic.migration import MigrationContext
        from alembic.operations import Operations

        for direction in ("upgrade", "downgrade"):
            output = io.StringIO()
            context = MigrationContext.configure(
                dialect_name="postgresql",
                opts={"as_sql": True, "output_buffer": output},
            )
            with patch.object(module, "op", Operations(context)):
                getattr(module, direction)()
            ddl = output.getvalue().lower()
            lock = "lock table plan_agent_runs, plans"
            if direction == "downgrade":
                lock += ", plan_agent_worker_import_receipts"
            lock += " in access exclusive mode"
            guard = f"do $ccm_worker_plan_import_{direction}$"
            mutation = "alter table" if direction == "upgrade" else "drop index"
            assert ddl.index(lock) < ddl.index(guard) < ddl.index(mutation)

    def test_receipt_table_uses_innodb_on_mysql(self):
        module = _load_worker_plan_import_receipt_migration("mysql_innodb")

        from alembic.migration import MigrationContext
        from alembic.operations import Operations

        output = io.StringIO()
        context = MigrationContext.configure(
            dialect_name="mysql",
            opts={"as_sql": True, "output_buffer": output},
        )
        with patch.object(module, "op", Operations(context)):
            module._create_receipt_table()
        assert "engine=innodb" in output.getvalue().lower()

    @pytest.mark.parametrize(
        ("version", "is_mariadb", "engines", "error"),
        [
            ((8, 0, 15), False, (), "8.0.16"),
            ((8, 0, 36), True, (), "8.0.16"),
            (
                (8, 0, 36),
                False,
                (("plans", "InnoDB"), ("plan_agent_runs", "MyISAM")),
                "InnoDB tables",
            ),
        ],
    )
    def test_mysql_runtime_requirements_fail_closed(
        self,
        version,
        is_mariadb,
        engines,
        error,
    ):
        module = _load_worker_plan_import_receipt_migration(
            f"mysql_requirements_{error}_{is_mariadb}"
        )
        dialect = SimpleNamespace(
            name="mysql",
            is_mariadb=is_mariadb,
            server_version_info=version,
        )
        bind = SimpleNamespace(
            dialect=dialect,
            execute=MagicMock(return_value=engines),
        )
        fake_op = SimpleNamespace(
            get_bind=lambda: bind,
            get_context=lambda: SimpleNamespace(as_sql=False),
        )
        with (
            patch.object(module, "op", fake_op),
            pytest.raises(RuntimeError, match=error),
        ):
            module._require_supported_mysql()

    def test_mysql_runtime_requirements_accept_supported_innodb(self):
        module = _load_worker_plan_import_receipt_migration(
            "mysql_requirements_supported"
        )
        dialect = SimpleNamespace(
            name="mysql",
            is_mariadb=False,
            server_version_info=(8, 0, 36),
        )
        bind = SimpleNamespace(
            dialect=dialect,
            execute=MagicMock(
                return_value=(
                    ("plans", "InnoDB"),
                    ("plan_agent_runs", "InnoDB"),
                )
            ),
        )
        fake_op = SimpleNamespace(
            get_bind=lambda: bind,
            get_context=lambda: SimpleNamespace(as_sql=False),
        )
        with patch.object(module, "op", fake_op):
            module._require_supported_mysql()

    def test_mysql_upgrade_replays_legacy_phase_and_table_suffix(self):
        module = _load_worker_plan_import_receipt_migration(
            "mysql_upgrade_state_machine"
        )
        events: list[str] = []

        class _Result:
            def scalar_one(self):
                return 0

        bind = SimpleNamespace(
            execute=lambda statement, *args, **kwargs: (
                events.append("run_backfill") or _Result()
            )
        )

        def alter(table_name, actions):
            assert table_name == "plan_agent_runs"
            events.append(
                "phase_alter"
                if actions[0].startswith("ADD COLUMN")
                else "canonical_alter"
            )

        with (
            patch.object(
                module,
                "_mysql_run_state",
                side_effect=("legacy", "phase", "canonical"),
            ),
            patch.object(
                module,
                "_mysql_receipt_state",
                side_effect=("absent", "absent", "canonical"),
            ),
            patch.object(
                module,
                "_assert_valid_import_identities",
                side_effect=lambda: events.append("preflight"),
            ),
            patch.object(module, "_mysql_alter", side_effect=alter),
            patch.object(
                module,
                "_create_receipt_table",
                side_effect=lambda: events.append("create_receipt"),
            ),
            patch.object(
                module,
                "_backfill_receipts",
                side_effect=lambda: events.append("receipt_backfill"),
            ),
            patch.object(
                module,
                "_assert_live_runs_have_receipts",
                side_effect=lambda: events.append("validate_receipts"),
            ),
            patch.object(
                module,
                "_ensure_mysql_receipt_index",
                side_effect=lambda: events.append("ensure_index"),
            ),
            patch.object(
                module,
                "op",
                SimpleNamespace(get_bind=lambda: bind),
            ),
        ):
            module._upgrade_mysql()

        assert events == [
            "preflight",
            "phase_alter",
            "preflight",
            "run_backfill",
            "canonical_alter",
            "preflight",
            "create_receipt",
            "receipt_backfill",
            "validate_receipts",
            "ensure_index",
        ]

    def test_mysql_upgrade_replays_canonical_gate_without_realtering_run(self):
        module = _load_worker_plan_import_receipt_migration(
            "mysql_upgrade_canonical_replay"
        )
        with (
            patch.object(module, "_mysql_run_state", return_value="canonical"),
            patch.object(
                module,
                "_mysql_receipt_state",
                side_effect=("absent", "absent", "canonical"),
            ),
            patch.object(module, "_assert_valid_import_identities"),
            patch.object(module, "_mysql_alter") as alter,
            patch.object(module, "_create_receipt_table") as create_receipt,
            patch.object(module, "_backfill_receipts") as backfill,
            patch.object(module, "_assert_live_runs_have_receipts"),
            patch.object(module, "_ensure_mysql_receipt_index"),
        ):
            module._upgrade_mysql()
        alter.assert_not_called()
        create_receipt.assert_called_once_with()
        backfill.assert_called_once_with()

    def test_mysql_downgrade_installs_both_writer_gates_before_drop(self):
        module = _load_worker_plan_import_receipt_migration(
            "mysql_downgrade_gate_order"
        )
        events: list[str] = []

        class _Result:
            def scalar_one(self):
                return 0

        bind = SimpleNamespace(execute=lambda *args, **kwargs: _Result())

        def alter(table_name, actions):
            if table_name == "plan_agent_runs" and actions[0].startswith(
                "ADD CONSTRAINT"
            ):
                events.append("run_gate")
            elif table_name == "plan_agent_worker_import_receipts":
                events.append("receipt_gate")
            else:
                events.append("run_downgrade")

        fake_op = SimpleNamespace(
            get_bind=lambda: bind,
            drop_table=lambda table_name: events.append(f"drop:{table_name}"),
        )
        with (
            patch.object(
                module,
                "_mysql_run_state",
                side_effect=("canonical", "canonical_gated", "legacy"),
            ),
            patch.object(
                module,
                "_mysql_receipt_state",
                side_effect=("canonical", "canonical_gated", "absent"),
            ),
            patch.object(module, "_mysql_alter", side_effect=alter),
            patch.object(module, "op", fake_op),
        ):
            module._downgrade_mysql()
        assert events == [
            "receipt_gate",
            "run_gate",
            "drop:plan_agent_worker_import_receipts",
            "run_downgrade",
        ]

    def test_mysql_downgrade_refuses_history_before_any_ddl(self):
        module = _load_worker_plan_import_receipt_migration(
            "mysql_downgrade_history"
        )

        class _Result:
            def scalar_one(self):
                return 1

        bind = SimpleNamespace(execute=lambda *args, **kwargs: _Result())
        with (
            patch.object(module, "_mysql_run_state", return_value="canonical"),
            patch.object(module, "_mysql_receipt_state", return_value="canonical"),
            patch.object(module, "_mysql_alter") as alter,
            patch.object(
                module,
                "op",
                SimpleNamespace(get_bind=lambda: bind),
            ),
            pytest.raises(RuntimeError, match="while receipt or imported Run"),
        ):
            module._downgrade_mysql()
        alter.assert_not_called()

    def test_mysql_downgrade_replays_after_receipt_drop(self):
        module = _load_worker_plan_import_receipt_migration(
            "mysql_downgrade_after_drop"
        )
        with (
            patch.object(
                module,
                "_mysql_run_state",
                side_effect=("canonical_gated", "legacy"),
            ),
            patch.object(module, "_mysql_receipt_state", return_value="absent"),
            patch.object(module, "_mysql_alter") as alter,
        ):
            module._downgrade_mysql()
        alter.assert_called_once()
        table_name, actions = alter.call_args.args
        assert table_name == "plan_agent_runs"
        assert actions[-1] == "DROP COLUMN import_receipt_protocol"

    def test_upgrade_rejects_null_plan_relay_origin(self, tmp_path):
        db_path = str(tmp_path / "worker-plan-import-null-origin.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, WORKER_TASK_DELETE_RECEIPT_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            self._insert_imported_graph(conn, plan_origin=None)
        engine.dispose()

        with pytest.raises(RuntimeError, match="malformed immutable identity"):
            _run_alembic(
                cfg,
                command.upgrade,
                WORKER_PLAN_IMPORT_RECEIPT_REVISION,
            )

    @pytest.mark.parametrize("identity", ["run", "plan"])
    def test_upgrade_rejects_nonpositive_import_identity(
        self,
        tmp_path,
        identity,
    ):
        db_path = str(tmp_path / f"worker-plan-import-{identity}-identity.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, WORKER_TASK_DELETE_RECEIPT_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            plan_id, run_id = self._insert_imported_graph(conn)
            if identity == "run":
                conn.execute(
                    text(
                        "UPDATE plan_agent_runs SET id = :invalid_id "
                        "WHERE id = :run_id"
                    ),
                    {"invalid_id": -run_id, "run_id": run_id},
                )
            else:
                conn.execute(
                    text(
                        "UPDATE plans SET id = :invalid_id WHERE id = :plan_id"
                    ),
                    {"invalid_id": -plan_id, "plan_id": plan_id},
                )
                conn.execute(
                    text(
                        "UPDATE plan_agent_runs SET plan_id = :invalid_id "
                        "WHERE id = :run_id"
                    ),
                    {
                        "invalid_id": -plan_id,
                        "run_id": run_id,
                    },
                )
        engine.dispose()

        with pytest.raises(RuntimeError, match="malformed immutable identity"):
            _run_alembic(
                cfg,
                command.upgrade,
                WORKER_PLAN_IMPORT_RECEIPT_REVISION,
            )

    def test_downgrade_rejects_imported_run_with_null_plan_identity(self, tmp_path):
        db_path = str(tmp_path / "worker-plan-import-null-plan-downgrade.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, WORKER_PLAN_IMPORT_RECEIPT_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            plan_id, run_id = self._insert_imported_graph(
                conn,
                include_receipt_protocol=True,
            )
            conn.execute(
                text(
                    "INSERT INTO plan_agent_worker_import_receipts "
                    "(run_id, plan_id, protocol, relay_origin, "
                    "payload_digest, outcome, created_at) VALUES "
                    "(:run_id, :plan_id, 1, 'manager_v1', :digest, "
                    "'imported', '2026-08-08 00:00:00')"
                ),
                {
                    "run_id": run_id,
                    "plan_id": plan_id,
                    "digest": "a" * 64,
                },
            )
            conn.execute(
                text("UPDATE plan_agent_runs SET plan_id = NULL WHERE id = :run_id"),
                {"run_id": run_id},
            )
        engine.dispose()

        with pytest.raises(RuntimeError, match="Cannot downgrade"):
            _run_alembic(
                cfg,
                command.downgrade,
                WORKER_TASK_DELETE_RECEIPT_REVISION,
            )
        engine = create_engine(f"sqlite:///{db_path}")
        assert "plan_agent_worker_import_receipts" in _get_all_tables(engine)
        engine.dispose()

    @pytest.mark.parametrize("outcome", ["cancelled_before_import", "imported"])
    def test_downgrade_refuses_tombstone_or_historical_graph(
        self,
        tmp_path,
        outcome,
    ):
        db_path = str(tmp_path / f"worker-plan-import-{outcome}-downgrade.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, WORKER_PLAN_IMPORT_RECEIPT_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            if outcome == "imported":
                plan_id, run_id = self._insert_imported_graph(
                    conn,
                    include_receipt_protocol=True,
                )
                conn.execute(
                    text(
                        "INSERT INTO plan_agent_worker_import_receipts "
                        "(run_id, plan_id, protocol, relay_origin, "
                        "payload_digest, outcome, created_at) VALUES "
                        "(:run_id, :plan_id, 1, 'manager_v1', :digest, "
                        "'imported', '2026-08-08 00:00:00')"
                    ),
                    {
                        "run_id": run_id,
                        "plan_id": plan_id,
                        "digest": "a" * 64,
                    },
                )
                conn.execute(
                    text("DELETE FROM plan_agent_runs WHERE id = :run_id"),
                    {"run_id": run_id},
                )
                conn.execute(
                    text("DELETE FROM plans WHERE id = :plan_id"),
                    {"plan_id": plan_id},
                )
            else:
                conn.execute(
                    text(
                        "INSERT INTO plan_agent_worker_import_receipts "
                        "(run_id, plan_id, protocol, relay_origin, "
                        "payload_digest, outcome, created_at) VALUES "
                        "(777, 776, 1, 'manager_v1', :digest, "
                        "'cancelled_before_import', "
                        "'2026-08-08 00:00:00')"
                    ),
                    {"digest": "b" * 64},
                )
        engine.dispose()

        with pytest.raises(RuntimeError, match="Cannot downgrade"):
            _run_alembic(
                cfg,
                command.downgrade,
                WORKER_TASK_DELETE_RECEIPT_REVISION,
            )
        engine = create_engine(f"sqlite:///{db_path}")
        assert "plan_agent_worker_import_receipts" in _get_all_tables(engine)
        engine.dispose()


class TestFreshMigration:
    """A fresh database (no tables) can be fully created via Alembic upgrade."""

    def test_fresh_db_upgrade_from_scratch(self, tmp_path):
        """Running upgrade head on empty DB creates all tables."""
        db_path = str(tmp_path / "fresh.db")

        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, "head")

        engine = create_engine(f"sqlite:///{db_path}")
        tables = _get_all_tables(engine)
        expected_tables = {"instances", "projects", "project_todos", "tasks", "log_entries", "worktrees", "global_settings", "secrets", "tags", "discussions", "discussion_messages", "discussion_agents", "discussion_events", "quick_phrases", "sub_agent_sessions", "sub_agent_reports", "pr_reviews", "pr_reviewer_runs", "pr_findings", "pr_finding_actions", "pr_finding_rebuttals", "pr_monitor_runs", "pr_repair_wakes", "pr_merge_queue_actions", "monitored_repos", "workers", "worker_turn_handoff_receipts", "worker_task_termination_receipts", "skill_lessons", "skill_usage", "feishu_user_binding", "org_members", "org_teams", "org_team_members", "task_shares", "project_shares", "shared_tasks_received", "user_skills", "users", "user_groups", "user_group_members", "team_task_shares", "team_project_shares", "plan_agent_runs", "plan_agent_steps", "plan_agent_runtime_receipts", "plan_agent_worker_dispatch_receipts", "plan_agent_worker_import_receipts", "plans", "plan_versions", "plan_input_requests", "plan_applications", "plan_application_receipts", "plan_application_attempts", "plan_legacy_task_links", "capability_invocations", "capability_executions", "capability_resume_outbox", "code_review_runs", "code_review_results", "delivery_runs", "delivery_cycles", "delivery_turns", "delivery_events", "delivery_actions", "delivery_transitions", "workspace_review_runs", "test_harness_runs", "test_harness_attempts", "test_harness_events", "test_harness_evidence", "test_harness_findings", "test_harness_sandbox_leases", "test_harness_child_bindings"}
        expected_tables |= {
            "browser_review_operation_receipts",
            "ssh_profiles",
            "task_id_allocators",
            "task_migration_operations",
            "pr_monitor_task_tombstones",
            "task_ssh_grants",
            "task_ssh_effect_receipts",
            "worker_node_controls",
        }
        assert tables == expected_tables, f"Missing tables: {expected_tables - tables}"

        # Verify all columns from latest migration exist
        task_cols = _get_table_columns(engine, "tasks")
        assert "todo_file_path" in task_cols
        assert "loop_progress" in task_cols
        assert "max_iterations" in task_cols
        assert "context_window_usage" in task_cols
        assert "plan_target_task_id" in task_cols
        assert "plan_context_snapshot" in task_cols
        assert "plan_applied_log_id" in task_cols
        assert "attention_tag" in task_cols
        assert "delivery_run_id" in task_cols
        assert "delivery_role" in task_cols

        worktree_cols = _get_table_columns(engine, "worktrees")
        assert {
            "task_id",
            "delivery_run_id",
            "last_verified_head",
            "cleanup_status",
        }.issubset(worktree_cols)

        plan_run_cols = _get_table_columns(engine, "plan_agent_runs")
        assert "capability_execution_id" in plan_run_cols
        assert "cancellation_target_generation" in plan_run_cols

        receipt_columns = {
            item["name"]: item
            for item in inspect(engine).get_columns(
                "plan_agent_runtime_receipts"
            )
        }
        assert isinstance(
            receipt_columns["process_start_ticks"]["type"], BigInteger
        )
        assert isinstance(
            Base.metadata.tables["plan_agent_runtime_receipts"]
            .c.process_start_ticks.type,
            BigInteger,
        )

        delivery_run_cols = set(_get_table_columns(engine, "delivery_runs"))
        assert {
            "admission_scope",
            "idempotency_key",
            "request_hash",
            "developer_task_id",
            "worktree_id",
            "current_cycle_id",
            "controller_generation",
            "lease_owner",
            "lease_expires_at",
            "next_reconcile_at",
        }.issubset(delivery_run_cols)
        delivery_run_unique_columns = {
            tuple(constraint["column_names"])
            for constraint in inspect(engine).get_unique_constraints(
                "delivery_runs"
            )
        }
        assert (
            "admission_scope",
            "project_id",
            "idempotency_key",
        ) in delivery_run_unique_columns
        assert ("source_todo_id",) in delivery_run_unique_columns

        child_binding_cols = _get_table_columns(
            engine, "test_harness_child_bindings"
        )
        assert {
            "harness_run_id",
            "workspace_review_run_id",
            "owner_task_id",
            "child_task_id",
            "browser_review_job_id",
            "state",
            "launch_profile_version",
            "provider",
            "model",
            "reasoning_effort",
            "codex_service_tier",
            "task_mode",
            "launch_config_digest",
            "owner_task_incarnation_id",
            "owner_task_retry_count",
            "owner_task_turn_generation",
            "owner_task_status",
            "child_task_incarnation_id",
        }.issubset(child_binding_cols)

        operation_receipt_cols = _get_table_columns(
            engine,
            "browser_review_operation_receipts",
        )
        assert {
            "id",
            "browser_review_job_id",
            "operation_id",
            "binding_id",
            "harness_run_id",
            "workspace_review_run_id",
            "owner_task_id",
            "owner_task_incarnation_id",
            "owner_task_retry_count",
            "owner_task_turn_generation",
            "owner_task_status",
            "child_task_id",
            "child_task_incarnation_id",
            "child_task_retry_count",
            "child_task_turn_generation",
            "child_task_status",
            "action_kind",
            "request_digest",
            "execution_nonce_digest",
            "status",
            "ack_digest",
            "result_data",
            "error",
            "created_at",
            "acknowledged_at",
        }.issubset(operation_receipt_cols)
        operation_receipt_column_info = {
            item["name"]: item
            for item in inspect(engine).get_columns(
                "browser_review_operation_receipts"
            )
        }
        for generation_column in (
            "owner_task_turn_generation",
            "child_task_turn_generation",
        ):
            assert isinstance(
                operation_receipt_column_info[generation_column]["type"],
                BigInteger,
            )

        for run_table in ("test_harness_runs", "workspace_review_runs"):
            run_cols = _get_table_columns(engine, run_table)
            assert {
                "owner_task_incarnation_id",
                "owner_task_retry_count",
                "owner_task_turn_generation",
                "owner_task_status",
            }.issubset(run_cols)
        for identity_table in (
            "test_harness_runs",
            "workspace_review_runs",
            "test_harness_child_bindings",
        ):
            identity_columns = {
                item["name"]: item
                for item in inspect(engine).get_columns(identity_table)
            }
            assert isinstance(
                identity_columns["owner_task_turn_generation"]["type"],
                BigInteger,
            )

        attempt_cols = _get_table_columns(engine, "test_harness_attempts")
        assert {
            "artifact_staging_root",
            "artifact_archive_prefix",
            "archive_state",
            "archive_manifest",
            "archive_error",
            "archived_at",
        }.issubset(attempt_cols)

        log_cols = _get_table_columns(engine, "log_entries")
        assert "loop_iteration" in log_cols
        assert "task_retry_count" in log_cols

        project_cols = _get_table_columns(engine, "projects")
        assert "sort_order" in project_cols
        assert "tags" in project_cols

        pr_review_cols = _get_table_columns(engine, "pr_reviews")
        assert "base_sha" in pr_review_cols
        assert "head_sha" in pr_review_cols
        assert "delivery_id" in pr_review_cols
        unique_column_sets = {
            tuple(constraint["column_names"])
            for constraint in inspect(engine).get_unique_constraints("pr_reviews")
        }
        assert (
            "repo_id",
            "pr_number",
            "base_ref",
            "base_sha",
            "head_sha",
            "attempt",
        ) in unique_column_sets
        assert (
            "repo_id",
            "pr_number",
            "base_ref",
            "base_sha",
            "head_sha",
        ) not in unique_column_sets
        assert (
            "repo_id",
            "pr_number",
            "base_sha",
            "head_sha",
        ) not in unique_column_sets
        assert ("repo_id", "pr_number", "head_sha") not in unique_column_sets
        assert ("repo_id", "delivery_id") in unique_column_sets

        pr_finding_cols = {
            item["name"]: item
            for item in inspect(engine).get_columns("pr_findings")
        }
        assert "resolution_lease_token" in pr_finding_cols
        assert "resolution_lease_expires_at" in pr_finding_cols
        assert "fixed_resolution_actor" in pr_finding_cols
        assert "BIGINT" in str(pr_finding_cols["github_comment_id"]["type"]).upper()
        assert isinstance(
            Base.metadata.tables["pr_findings"].c.github_comment_id.type,
            BigInteger,
        )

        capability_invocation_columns = set(
            _get_table_columns(engine, "capability_invocations")
        )
        assert {
            "active_task_id",
            "idempotency_key",
            "request_task_turn_generation",
            "request_output_log_id",
            "request_reason",
            "request_protocol_version",
            "request_output_hash",
            "request_native_turn_id",
            "result_hash",
        }.issubset(capability_invocation_columns)
        capability_execution_columns = set(
            _get_table_columns(engine, "capability_executions")
        )
        assert {
            "active_invocation_id",
            "lease_token",
            "handle_generation",
            "output_hash",
        }.issubset(capability_execution_columns)
        invocation_unique_columns = {
            tuple(constraint["column_names"])
            for constraint in inspect(engine).get_unique_constraints(
                "capability_invocations"
            )
        }
        assert ("task_id", "idempotency_key") in invocation_unique_columns
        assert ("active_task_id",) in invocation_unique_columns
        execution_unique_columns = {
            tuple(constraint["column_names"])
            for constraint in inspect(engine).get_unique_constraints(
                "capability_executions"
            )
        }
        assert ("invocation_id", "attempt") in execution_unique_columns
        assert ("active_invocation_id",) in execution_unique_columns

        action_columns = {
            item["name"]
            for item in inspect(engine).get_columns("pr_finding_actions")
        }
        assert {
            "finding_id",
            "action_type",
            "status",
            "idempotency_key",
            "actor_user_id",
            "human_advice",
            "task_id",
            "expected_head_sha",
            "active_fix_finding_id",
            "patch_sha256",
            "download_receipt_hash",
            "downloaded_by_user_id",
            "downloaded_at",
            "confirmed_by_user_id",
            "confirmed_at",
            "candidate_commit_sha",
            "candidate_created_at",
            "push_attempted_at",
            "cancelled_by_user_id",
            "cancelled_at",
            "operation_token",
            "operation_expires_at",
            "result",
            "error_message",
            "created_at",
            "updated_at",
            "completed_at",
        }.issubset(action_columns)
        action_unique_constraints = {
            (constraint["name"], tuple(constraint["column_names"]))
            for constraint in inspect(engine).get_unique_constraints(
                "pr_finding_actions"
            )
        }
        assert (
            "uq_pr_finding_actions_idempotency_key",
            ("idempotency_key",),
        ) in action_unique_constraints
        assert (
            "uq_pr_finding_actions_active_fix",
            ("active_fix_finding_id",),
        ) in action_unique_constraints
        action_check_constraints = {
            constraint["name"]: constraint.get("sqltext", "")
            for constraint in inspect(engine).get_check_constraints(
                "pr_finding_actions"
            )
        }
        assert set(action_check_constraints) == {
            "ck_pr_finding_actions_active_slot",
            "ck_pr_finding_actions_status",
            "ck_pr_finding_actions_type",
        }
        active_slot_sql = " ".join(
            action_check_constraints["ck_pr_finding_actions_active_slot"]
            .lower()
            .split()
        )
        assert "active_fix_finding_id is not null" in active_slot_sql
        assert "active_fix_finding_id = finding_id" in active_slot_sql
        action_foreign_keys = {
            (
                tuple(constraint["constrained_columns"]),
                constraint["referred_table"],
                tuple(constraint["referred_columns"]),
                (constraint.get("options") or {}).get("ondelete"),
            )
            for constraint in inspect(engine).get_foreign_keys(
                "pr_finding_actions"
            )
        }
        assert (("finding_id",), "pr_findings", ("id",), "CASCADE") in (
            action_foreign_keys
        )
        assert (("task_id",), "tasks", ("id",), None) in action_foreign_keys
        action_indexes = {
            (index["name"], tuple(index["column_names"]))
            for index in inspect(engine).get_indexes("pr_finding_actions")
        }
        assert {
            ("ix_pr_finding_actions_finding_id", ("finding_id",)),
            ("ix_pr_finding_actions_status", ("status",)),
            ("ix_pr_finding_actions_actor_user_id", ("actor_user_id",)),
            ("ix_pr_finding_actions_task_id", ("task_id",)),
        }.issubset(action_indexes)
        assert (
            Base.metadata.tables["monitored_repos"]
            .c.required_checks.server_default
            is None
        )

        # Verify alembic_version at head
        with engine.connect() as conn:
            version = conn.execute(text("SELECT version_num FROM alembic_version")).scalar()
            assert version == _get_head_revision(cfg)

        engine.dispose()

    def test_instance_process_identity_revision_roundtrips(self, tmp_path):
        """The new Instance identity column owns a reversible linear step."""

        db_path = str(tmp_path / "instance-process-identity-roundtrip.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, DELIVERY_FRONTEND_REVIEW_REVISION)

        engine = create_engine(f"sqlite:///{db_path}")
        assert "process_identity" not in _get_table_columns(engine, "instances")
        engine.dispose()

        _run_alembic(cfg, command.upgrade, INSTANCE_PROCESS_IDENTITY_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert (
            _get_table_columns(engine, "instances")["process_identity"]
            == "VARCHAR(128)"
        )
        engine.dispose()

        _run_alembic(cfg, command.downgrade, DELIVERY_FRONTEND_REVIEW_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "process_identity" not in _get_table_columns(engine, "instances")
        with engine.connect() as conn:
            assert (
                conn.execute(
                    text("SELECT version_num FROM alembic_version")
                ).scalar_one()
                == DELIVERY_FRONTEND_REVIEW_REVISION
            )
        engine.dispose()

        _run_alembic(cfg, command.upgrade, INSTANCE_PROCESS_IDENTITY_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "process_identity" in _get_table_columns(engine, "instances")
        engine.dispose()

    def test_fresh_db_downgrade_and_upgrade(self, tmp_path):
        """Migrations are reversible: upgrade → downgrade → upgrade."""
        db_path = str(tmp_path / "roundtrip.db")
        cfg = _alembic_cfg(db_path)

        _run_alembic(cfg, command.upgrade, "head")
        _run_alembic(cfg, command.downgrade, "6b3f8a1c2d9e")

        engine = create_engine(f"sqlite:///{db_path}")
        task_cols = _get_table_columns(engine, "tasks")
        assert "todo_file_path" not in task_cols
        assert "loop_progress" not in task_cols
        log_cols = _get_table_columns(engine, "log_entries")
        assert "loop_iteration" not in log_cols
        engine.dispose()

        # Upgrade again
        _run_alembic(cfg, command.upgrade, "head")
        engine = create_engine(f"sqlite:///{db_path}")
        task_cols = _get_table_columns(engine, "tasks")
        assert "todo_file_path" in task_cols
        engine.dispose()

    def test_finding_actions_revision_downgrades_and_reupgrades(self, tmp_path):
        """The finding-action table is owned by the new linear head."""

        db_path = str(tmp_path / "finding-actions-roundtrip.db")
        cfg = _alembic_cfg(db_path)

        _run_alembic(cfg, command.upgrade, CURRENT_HEAD_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "pr_finding_actions" in _get_all_tables(engine)
        engine.dispose()

        _run_alembic(cfg, command.downgrade, PR_REVIEW_PANEL_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "pr_finding_actions" not in _get_all_tables(engine)
        with engine.connect() as conn:
            revisions = {
                row[0]
                for row in conn.execute(
                text("SELECT version_num FROM alembic_version")
                ).fetchall()
            }
        assert revisions == {
            PR_REVIEW_PANEL_REVISION,
            FIRST_CLASS_PLAN_HEAD_REVISION,
        }
        engine.dispose()

        _run_alembic(cfg, command.upgrade, CURRENT_HEAD_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "pr_finding_actions" in _get_all_tables(engine)
        with engine.connect() as conn:
            revision = conn.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one()
        assert revision == CURRENT_HEAD_REVISION
        engine.dispose()

    def test_archive_state_migration_preserves_legacy_pointer_and_roundtrips(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "archive-state-roundtrip.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, CHILD_BINDING_REVISION)

        run_id = "a" * 32
        attempt_id = "b" * 32
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO test_harness_runs ("
                    "id, target_kind, target_spec, test_plan, runtime_config, "
                    "request_fingerprint, root_run_id, attempt_number, status, "
                    "stage, stale, cleanup_status, event_sequence, created_at"
                    ") VALUES ("
                    ":id, 'fixed_url', '{}', '{}', '{}', :fingerprint, :id, "
                    "1, 'completed', 'completed', 0, 'completed', 0, :created_at"
                    ")"
                ),
                {
                    "id": run_id,
                    "fingerprint": "c" * 64,
                    "created_at": "2026-08-08 00:00:00",
                },
            )
            conn.execute(
                text(
                    "INSERT INTO test_harness_attempts ("
                    "id, run_id, ordinal, status, stage, provider, model, "
                    "reasoning_effort, codex_service_tier, artifact_root, "
                    "result_data, created_at"
                    ") VALUES ("
                    ":id, :run_id, 1, 'completed', 'completed', 'codex', "
                    "'gpt-5.6-sol', 'medium', 'default', :artifact_root, '{}', "
                    ":created_at"
                    ")"
                ),
                {
                    "id": attempt_id,
                    "run_id": run_id,
                    "artifact_root": "/private/tmp/legacy-browser-job",
                    "created_at": "2026-08-08 00:00:00",
                },
            )
        engine.dispose()

        _run_alembic(cfg, command.upgrade, ARCHIVE_STATE_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT artifact_staging_root, artifact_archive_prefix, "
                    "archive_state, archive_manifest, archive_error, archived_at "
                    "FROM test_harness_attempts WHERE id = :id"
                ),
                {"id": attempt_id},
            ).one()
        assert row.artifact_staging_root == "/private/tmp/legacy-browser-job"
        assert row.artifact_archive_prefix is None
        assert row.archive_state == "staging"
        manifest = row.archive_manifest
        if isinstance(manifest, str):
            manifest = json.loads(manifest)
        assert manifest == {}
        assert row.archive_error is None
        assert row.archived_at is None
        engine.dispose()

        _run_alembic(cfg, command.downgrade, CHILD_BINDING_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        attempt_cols = _get_table_columns(engine, "test_harness_attempts")
        assert "artifact_root" in attempt_cols
        assert "archive_state" not in attempt_cols
        assert "artifact_staging_root" not in attempt_cols
        engine.dispose()

    def test_child_launch_profile_backfills_only_durable_task_identities(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "child-launch-profile-backfill.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, ARCHIVE_STATE_REVISION)

        run_id = "a" * 32
        workspace_id = "b" * 32
        binding_id = "c" * 32
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO tasks ("
                    "id, title, description, status, priority, "
                    "target_branch, merge_status, retry_count, max_retries, "
                    "mode, created_at"
                    ") VALUES "
                    "(1, 'owner', 'owner', 'completed', 0, "
                    "'main', 'pending', 3, 0, 'auto', :created_at), "
                    "(2, 'child', 'child', 'cancelled', 0, "
                    "'main', 'pending', 0, 0, 'auto', :created_at)"
                ),
                {
                    "created_at": "2026-08-09 00:00:00",
                },
            )
            conn.execute(
                text(
                    "INSERT INTO test_harness_runs ("
                    "id, task_id, agent_task_id, target_kind, target_spec, "
                    "test_plan, runtime_config, request_fingerprint, "
                    "root_run_id, attempt_number, status, stage, stale, "
                    "cleanup_status, event_sequence, created_at"
                    ") VALUES ("
                    ":id, 1, 2, 'fixed_url', '{}', '{}', '{}', :fingerprint, "
                    ":id, 1, 'completed', 'completed', 0, 'completed', 0, "
                    ":created_at)"
                ),
                {
                    "id": run_id,
                    "fingerprint": "3" * 64,
                    "created_at": "2026-08-09 00:00:00",
                },
            )
            conn.execute(
                text(
                    "INSERT INTO workspace_review_runs ("
                    "id, task_id, agent_task_id, mode, profile, goal, status, "
                    "stage, workspace_path, git_head, workspace_fingerprint, "
                    "preview_config, stale, cleanup_status, created_at"
                    ") VALUES ("
                    ":id, 1, 2, 'review_only', 'standard', 'Review', "
                    "'completed', 'completed', '/tmp/workspace', :head, "
                    ":fingerprint, '{}', 0, 'completed', :created_at)"
                ),
                {
                    "id": workspace_id,
                    "head": "4" * 40,
                    "fingerprint": "5" * 64,
                    "created_at": "2026-08-09 00:00:00",
                },
            )
            conn.execute(
                text(
                    "INSERT INTO test_harness_child_bindings ("
                    "id, harness_run_id, owner_task_id, child_task_id, "
                    "browser_review_job_id, state, created_at"
                    ") VALUES ("
                    ":id, :run_id, 1, 2, 'legacy-job', 'stopped', :created_at)"
                ),
                {
                    "id": binding_id,
                    "run_id": run_id,
                    "created_at": "2026-08-09 00:00:00",
                },
            )
        engine.dispose()

        # Bring the independent main branch to its published head. Delivery
        # introduced Task incarnation/turn identity after the Browser branch
        # had already shipped, so a database at this point legitimately has
        # both f1c4... and d3c8... stamped until the new merge revision runs.
        _run_alembic(cfg, command.upgrade, WORKER_PLAN_IMPORT_RECEIPT_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE tasks SET incarnation_id = :incarnation, "
                    "turn_generation = :generation WHERE id = :task_id"
                ),
                [
                    {
                        "incarnation": "1" * 32,
                        "generation": 2**31 + 7,
                        "task_id": 1,
                    },
                    {
                        "incarnation": "2" * 32,
                        "generation": 2**31 + 5,
                        "task_id": 2,
                    },
                ],
            )
        engine.dispose()

        _run_alembic(cfg, command.upgrade, CHILD_LAUNCH_PROFILE_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            task_identities = {
                row.id: (
                    row.incarnation_id,
                    row.retry_count,
                    row.turn_generation,
                    row.status,
                )
                for row in conn.execute(
                    text(
                        "SELECT id, incarnation_id, retry_count, "
                        "turn_generation, status FROM tasks WHERE id IN (1, 2)"
                    )
                )
            }
            run = conn.execute(
                text(
                    "SELECT owner_task_incarnation_id, "
                    "owner_task_retry_count, owner_task_turn_generation, "
                    "owner_task_status FROM test_harness_runs WHERE id = :id"
                ),
                {"id": run_id},
            ).one()
            workspace = conn.execute(
                text(
                    "SELECT owner_task_incarnation_id, "
                    "owner_task_retry_count, owner_task_turn_generation, "
                    "owner_task_status FROM workspace_review_runs WHERE id = :id"
                ),
                {"id": workspace_id},
            ).one()
            binding = conn.execute(
                text(
                    "SELECT owner_task_incarnation_id, "
                    "owner_task_retry_count, owner_task_turn_generation, "
                    "owner_task_status, child_task_incarnation_id, "
                    "launch_profile_version, launch_config_digest "
                    "FROM test_harness_child_bindings WHERE id = :id"
                ),
                {"id": binding_id},
            ).one()
        owner_identity = task_identities[1]
        child_identity = task_identities[2]
        assert isinstance(owner_identity[0], str) and len(owner_identity[0]) == 32
        assert isinstance(child_identity[0], str) and len(child_identity[0]) == 32
        assert owner_identity == ("1" * 32, 3, 2**31 + 7, "completed")
        assert child_identity == ("2" * 32, 0, 2**31 + 5, "cancelled")
        assert tuple(run) == owner_identity
        assert tuple(workspace) == owner_identity
        assert tuple(binding) == (
            *owner_identity,
            child_identity[0],
            None,
            None,
        )
        engine.dispose()

        _run_alembic(cfg, command.downgrade, ARCHIVE_STATE_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "owner_task_incarnation_id" not in _get_table_columns(
            engine,
            "test_harness_runs",
        )
        assert "launch_profile_version" not in _get_table_columns(
            engine,
            "test_harness_child_bindings",
        )
        engine.dispose()

    def test_browser_operation_receipt_migration_roundtrip(self, tmp_path):
        db_path = str(tmp_path / "browser-operation-receipt-roundtrip.db")
        cfg = _alembic_cfg(db_path)

        _run_alembic(
            cfg,
            command.upgrade,
            COMBINED_BROWSER_MAIN_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        assert (
            "browser_review_operation_receipts"
            not in _get_all_tables(engine)
        )
        engine.dispose()

        _run_alembic(
            cfg,
            command.upgrade,
            BROWSER_OPERATION_RECEIPT_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        inspector = inspect(engine)
        assert set(_get_table_columns(
            engine,
            "browser_review_operation_receipts",
        )) == {
            "id",
            "browser_review_job_id",
            "operation_id",
            "binding_id",
            "harness_run_id",
            "workspace_review_run_id",
            "owner_task_id",
            "owner_task_incarnation_id",
            "owner_task_retry_count",
            "owner_task_turn_generation",
            "owner_task_status",
            "child_task_id",
            "child_task_incarnation_id",
            "child_task_retry_count",
            "child_task_turn_generation",
            "child_task_status",
            "action_kind",
            "request_digest",
            "execution_nonce_digest",
            "status",
            "ack_digest",
            "result_data",
            "error",
            "created_at",
            "acknowledged_at",
        }
        assert {
            tuple(constraint["column_names"])
            for constraint in inspector.get_unique_constraints(
                "browser_review_operation_receipts"
            )
        } == {("browser_review_job_id", "operation_id")}
        assert {
            constraint["name"]
            for constraint in inspector.get_check_constraints(
                "browser_review_operation_receipts"
            )
        } == {"ck_browser_review_operation_status"}
        assert {
            index["name"]
            for index in inspector.get_indexes(
                "browser_review_operation_receipts"
            )
        } == {
            "ix_browser_review_operation_receipts_binding_id",
            "ix_browser_review_operation_receipts_browser_review_job_id",
            "ix_browser_review_operation_receipts_child_task_id",
            "ix_browser_review_operation_receipts_harness_run_id",
            "ix_browser_review_operation_receipts_owner_task_id",
            "ix_browser_review_operation_receipts_status",
            "ix_browser_review_operation_receipts_workspace_review_run_id",
        }
        engine.dispose()

        _run_alembic(
            cfg,
            command.downgrade,
            COMBINED_BROWSER_MAIN_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        assert (
            "browser_review_operation_receipts"
            not in _get_all_tables(engine)
        )
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == COMBINED_BROWSER_MAIN_REVISION
        engine.dispose()

        _run_alembic(
            cfg,
            command.upgrade,
            BROWSER_OPERATION_RECEIPT_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        assert "browser_review_operation_receipts" in _get_all_tables(engine)
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == BROWSER_OPERATION_RECEIPT_REVISION
        engine.dispose()

    @pytest.mark.parametrize(
        "status",
        ["permitted", "completed", "uncertain"],
    )
    def test_browser_operation_receipt_downgrade_refuses_evidence(
        self,
        tmp_path,
        status,
    ):
        db_path = str(tmp_path / f"browser-operation-{status}.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(
            cfg,
            command.upgrade,
            BROWSER_OPERATION_RECEIPT_REVISION,
        )

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO browser_review_operation_receipts (
                    id, browser_review_job_id, operation_id, binding_id,
                    harness_run_id, workspace_review_run_id,
                    owner_task_id, owner_task_incarnation_id,
                    owner_task_retry_count, owner_task_turn_generation,
                    owner_task_status, child_task_id,
                    child_task_incarnation_id, child_task_retry_count,
                    child_task_turn_generation, child_task_status,
                    action_kind, request_digest, execution_nonce_digest,
                    status, ack_digest, result_data, error, created_at,
                    acknowledged_at
                ) VALUES (
                    :id, :job_id, :operation_id, :binding_id,
                    :harness_run_id, NULL,
                    1, :owner_incarnation_id,
                    0, 0, 'completed', 2,
                    :child_incarnation_id, 0,
                    0, 'in_progress',
                    'click', :request_digest, :nonce_digest,
                    :status, NULL, '{}', NULL,
                    '2026-08-09 00:00:00', NULL
                )
            """), {
                "id": "a" * 32,
                "job_id": "b" * 32,
                "operation_id": "operation-1",
                "binding_id": "c" * 32,
                "harness_run_id": "d" * 32,
                "owner_incarnation_id": "e" * 32,
                "child_incarnation_id": "f" * 32,
                "request_digest": "1" * 64,
                "nonce_digest": "2" * 64,
                "status": status,
            })
        engine.dispose()

        with pytest.raises(RuntimeError, match="permanent.*evidence"):
            _run_alembic(
                cfg,
                command.downgrade,
                COMBINED_BROWSER_MAIN_REVISION,
            )

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT status FROM browser_review_operation_receipts"
            )).scalar_one() == status
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == BROWSER_OPERATION_RECEIPT_REVISION
        engine.dispose()

    def test_browser_operation_receipt_offline_downgrade_fails_closed(
        self,
        monkeypatch,
    ):
        module = _load_browser_operation_receipt_migration(
            "offline_downgrade"
        )
        monkeypatch.setattr(module.context, "is_offline_mode", lambda: True)
        get_bind = MagicMock()
        monkeypatch.setattr(module.op, "get_bind", get_bind)

        with pytest.raises(RuntimeError, match="Offline.*downgrade.*refused"):
            module._assert_downgrade_safe()

        get_bind.assert_not_called()

    @pytest.mark.parametrize(
        "bad_table",
        [
            "test_harness_child_bindings",
            "task_ssh_effect_receipts",
            "project_shares",
            "instances",
        ],
    )
    def test_browser_operation_mysql_preflight_rejects_non_innodb(
        self,
        monkeypatch,
        bad_table,
    ):
        module = _load_browser_operation_receipt_migration(
            "mysql_preflight"
        )
        rows = [
            (table, "MyISAM" if table == bad_table else "InnoDB")
            for table in module._MYSQL_TRANSACTION_TABLES
        ]
        bind = SimpleNamespace(
            execute=MagicMock(
                return_value=SimpleNamespace(all=lambda: rows)
            ),
        )
        monkeypatch.setattr(
            module.op,
            "get_context",
            lambda: SimpleNamespace(
                dialect=SimpleNamespace(name="mysql")
            ),
        )
        monkeypatch.setattr(module.context, "is_offline_mode", lambda: False)
        monkeypatch.setattr(module.op, "get_bind", lambda: bind)

        with pytest.raises(
            RuntimeError,
            match=bad_table,
        ):
            module._require_mysql_transaction_tables()

        bind.execute.assert_called_once()

    def test_browser_operation_mysql_preflight_accepts_all_innodb(
        self,
        monkeypatch,
    ):
        module = _load_browser_operation_receipt_migration(
            "mysql_preflight_ok"
        )
        rows = [
            (table, "InnoDB")
            for table in module._MYSQL_TRANSACTION_TABLES
        ]
        bind = SimpleNamespace(
            execute=MagicMock(
                return_value=SimpleNamespace(all=lambda: rows)
            ),
        )
        monkeypatch.setattr(
            module.op,
            "get_context",
            lambda: SimpleNamespace(
                dialect=SimpleNamespace(name="mysql")
            ),
        )
        monkeypatch.setattr(module.context, "is_offline_mode", lambda: False)
        monkeypatch.setattr(module.op, "get_bind", lambda: bind)

        module._require_mysql_transaction_tables()

        bind.execute.assert_called_once()

    def test_browser_operation_mysql_offline_upgrade_fails_closed(
        self,
        monkeypatch,
    ):
        module = _load_browser_operation_receipt_migration(
            "mysql_offline_upgrade"
        )
        monkeypatch.setattr(
            module.op,
            "get_context",
            lambda: SimpleNamespace(
                dialect=SimpleNamespace(name="mysql")
            ),
        )
        monkeypatch.setattr(module.context, "is_offline_mode", lambda: True)
        get_bind = MagicMock()
        monkeypatch.setattr(module.op, "get_bind", get_bind)

        with pytest.raises(RuntimeError, match="Offline.*upgrade.*refused"):
            module._require_mysql_transaction_tables()

        get_bind.assert_not_called()


class TestAlreadyMigratedDb:
    """A database already at head is a no-op."""

    def test_upgrade_head_is_noop(self, tmp_path):
        db_path = str(tmp_path / "current.db")
        cfg = _alembic_cfg(db_path)

        _run_alembic(cfg, command.upgrade, "head")
        # Running again should not raise
        _run_alembic(cfg, command.upgrade, "head")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            version = conn.execute(text("SELECT version_num FROM alembic_version")).scalar()
            assert version == _get_head_revision(cfg)
        engine.dispose()

    def test_idempotency_migration_preserves_existing_pr_reviews(self, tmp_path):
        db_path = str(tmp_path / "existing_pr_reviews.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, "31fe767354b7")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO monitored_repos (
                    repo_full_name, enabled, auto_merge, webhook_secret,
                    provider, default_branch, allowed_authors, status,
                    created_at, updated_at
                ) VALUES (
                    'owner/repo', 1, 0, 'secret', 'codex', 'main', '[]',
                    'active', '2026-07-22 00:00:00', '2026-07-22 00:00:00'
                )
            """))
            for created_at in ("2026-07-22 00:00:00", "2026-07-22 00:01:00"):
                conn.execute(text("""
                    INSERT INTO pr_reviews (
                        repo_id, pr_number, pr_title, pr_author, pr_url,
                        status, created_at
                    ) VALUES (
                        1, 42, 'Title', 'alice',
                        'https://github.com/owner/repo/pull/42',
                        'approved', :created_at
                    )
                """), {"created_at": created_at})
        engine.dispose()

        _run_alembic(cfg, command.upgrade, "head")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT base_sha, head_sha, delivery_id "
                "FROM pr_reviews ORDER BY id"
            )).fetchall()
            assert rows == [(None, None, None), (None, None, None)]
            required_checks = conn.execute(text(
                "SELECT required_checks FROM monitored_repos WHERE id = 1"
            )).scalar_one()
            if isinstance(required_checks, str):
                required_checks = json.loads(required_checks)
            assert required_checks == []
            conn.execute(text("""
                INSERT INTO monitored_repos (
                    repo_full_name, enabled, auto_merge, webhook_secret,
                    provider, default_branch, allowed_authors, required_checks, status,
                    created_at, updated_at
                ) VALUES (
                    'owner/default-checks', 1, 0, 'secret', 'codex', 'main',
                    '[]', '[]', 'active', '2026-07-22 00:02:00',
                    '2026-07-22 00:02:00'
                )
            """))
            inserted_default = conn.execute(text(
                "SELECT required_checks FROM monitored_repos "
                "WHERE repo_full_name = 'owner/default-checks'"
            )).scalar_one()
            if isinstance(inserted_default, str):
                inserted_default = json.loads(inserted_default)
            assert inserted_default == []
        required_column = next(
            item for item in inspect(engine).get_columns("monitored_repos")
            if item["name"] == "required_checks"
        )
        assert required_column["nullable"] is False
        assert required_column["default"] is None
        engine.dispose()

    @pytest.mark.parametrize("dialect_name", ("postgresql", "mysql"))
    def test_pr_panel_migration_compiles_portable_schema(self, dialect_name):
        """The Panel schema uses portable Boolean/JSON defaults and bigint IDs."""

        migration_path = (
            PROJECT_ROOT
            / "alembic"
            / "versions"
            / "7a1d4e9c2b60_add_pr_review_panel.py"
        )
        spec = importlib.util.spec_from_file_location(
            f"pr_panel_migration_for_{dialect_name}", migration_path
        )
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        from alembic.migration import MigrationContext
        from alembic.operations import Operations

        output = io.StringIO()
        context = MigrationContext.configure(
            dialect_name=dialect_name,
            opts={"as_sql": True, "output_buffer": output},
        )
        with patch.object(module, "op", Operations(context)):
            module.upgrade()
        ddl = output.getvalue().lower()
        if dialect_name == "postgresql":
            assert "boolean default false not null" in ddl
            assert "required_checks set not null" in ddl
            assert "cast('[]' as json)" in ddl
        else:
            assert "bool not null default false" in ddl
            assert "modify required_checks json not null" in ddl
            assert "required_checks = json_array()" in ddl
        assert "boolean default 0" not in ddl
        assert all(
            "default" not in line
            for line in ddl.splitlines()
            if "required_checks" in line
        )
        assert "github_comment_id bigint" in ddl

    def test_base_sha_migration_preserves_existing_snapshot_keys(self, tmp_path):
        db_path = str(tmp_path / "existing_pr_review_snapshot.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, "c8f5d3a72b10")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO monitored_repos (
                    repo_full_name, enabled, auto_merge, webhook_secret,
                    provider, default_branch, allowed_authors, status,
                    created_at, updated_at
                ) VALUES (
                    'owner/repo', 1, 0, 'secret', 'codex', 'main', '[]',
                    'active', '2026-07-31 00:00:00', '2026-07-31 00:00:00'
                )
            """))
            conn.execute(
                text("""
                    INSERT INTO pr_reviews (
                        repo_id, pr_number, head_sha, delivery_id, pr_title,
                        pr_author, pr_url, status, created_at
                    ) VALUES (
                        1, 42, :head_sha, 'delivery-1', 'Title', 'alice',
                        'https://github.com/owner/repo/pull/42',
                        'approved', '2026-07-31 00:00:00'
                    )
                """),
                {"head_sha": "a" * 40},
            )
        engine.dispose()

        _run_alembic(cfg, command.upgrade, "head")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT base_ref, base_sha, head_sha, delivery_id, action_nonce, "
                "pending_action, pending_review_body, publishing_actor, "
                "publishing_retry_count, publishing_task_started_at, "
                "publishing_started_at FROM pr_reviews"
            )).one()
            assert row == (
                "main",
                None,
                "a" * 40,
                "delivery-1",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )

            unique_column_sets = {
                tuple(constraint["column_names"])
                for constraint in inspect(conn).get_unique_constraints("pr_reviews")
            }
            assert (
                "repo_id",
                "pr_number",
                "base_ref",
                "base_sha",
                "head_sha",
                "attempt",
            ) in unique_column_sets
            assert (
                "repo_id",
                "pr_number",
                "base_ref",
                "base_sha",
                "head_sha",
            ) not in unique_column_sets
            assert (
                "repo_id",
                "pr_number",
                "base_sha",
                "head_sha",
            ) not in unique_column_sets
            assert ("repo_id", "pr_number", "head_sha") not in unique_column_sets
        engine.dispose()

    def test_base_sha_migration_downgrade_restores_head_constraint(self, tmp_path):
        db_path = str(tmp_path / "base_sha_roundtrip.db")
        cfg = _alembic_cfg(db_path)
        # Stop immediately before the base_ref migration.  This test exercises
        # the older base_sha downgrade and intentionally needs legacy Review
        # rows; the newer migration refuses to discard frozen non-empty
        # base_ref subjects on downgrade.
        _run_alembic(cfg, command.upgrade, "1a7c9e4d2b60")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO monitored_repos (
                    repo_full_name, enabled, auto_merge, webhook_secret,
                    provider, default_branch, allowed_authors, required_checks, status,
                    created_at, updated_at
                ) VALUES (
                    'owner/rollback', 1, 0, 'secret', 'claude', 'main', '[]',
                    '[]', 'active', '2026-07-31 00:00:00',
                    '2026-07-31 00:00:00'
                )
            """))
            for base_sha in ("1" * 40, "2" * 40):
                conn.execute(
                    text("""
                        INSERT INTO pr_reviews (
                            repo_id, pr_number, base_sha, head_sha, pr_title,
                            pr_author, pr_url, status, created_at
                        ) VALUES (
                            1, 42, :base_sha, :head_sha, 'Title', 'alice',
                            'https://github.com/owner/rollback/pull/42',
                            'approved', '2026-07-31 00:00:00'
                        )
                    """),
                    {"base_sha": base_sha, "head_sha": "a" * 40},
                )
        engine.dispose()

        _run_alembic(cfg, command.downgrade, "c8f5d3a72b10")

        engine = create_engine(f"sqlite:///{db_path}")
        pr_review_cols = _get_table_columns(engine, "pr_reviews")
        assert "base_sha" not in pr_review_cols
        assert "publishing_actor" not in pr_review_cols
        log_cols = _get_table_columns(engine, "log_entries")
        assert "task_retry_count" not in log_cols
        unique_column_sets = {
            tuple(constraint["column_names"])
            for constraint in inspect(engine).get_unique_constraints("pr_reviews")
        }
        assert ("repo_id", "pr_number", "head_sha") in unique_column_sets
        assert (
            "repo_id",
            "pr_number",
            "base_sha",
            "head_sha",
        ) not in unique_column_sets
        with engine.connect() as conn:
            heads = [
                row[0]
                for row in conn.execute(
                    text("SELECT head_sha FROM pr_reviews ORDER BY id")
                ).fetchall()
            ]
            assert heads == [None, "a" * 40]
        engine.dispose()

        _run_alembic(cfg, command.upgrade, "head")

        engine = create_engine(f"sqlite:///{db_path}")
        pr_review_cols = _get_table_columns(engine, "pr_reviews")
        assert "base_ref" in pr_review_cols
        assert "base_sha" in pr_review_cols
        assert "publishing_actor" in pr_review_cols
        log_cols = _get_table_columns(engine, "log_entries")
        assert "task_retry_count" in log_cols
        unique_column_sets = {
            tuple(constraint["column_names"])
            for constraint in inspect(engine).get_unique_constraints("pr_reviews")
        }
        assert (
            "repo_id",
            "pr_number",
            "base_ref",
            "base_sha",
            "head_sha",
            "attempt",
        ) in unique_column_sets
        assert (
            "repo_id",
            "pr_number",
            "base_ref",
            "base_sha",
            "head_sha",
        ) not in unique_column_sets
        assert (
            "repo_id",
            "pr_number",
            "base_sha",
            "head_sha",
        ) not in unique_column_sets
        assert ("repo_id", "pr_number", "head_sha") not in unique_column_sets
        engine.dispose()


class TestVersionedPlanBackfill:
    def test_feature_branch_revision_chain_is_not_migrated(self, tmp_path):
        db_path = str(tmp_path / "legacy_plans.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, "d2b8f6a10c43")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            required = """
                id, title, description, status, priority, target_branch,
                merge_status, retry_count, max_retries, mode, created_at
            """
            conn.execute(text(f"""
                INSERT INTO tasks ({required}) VALUES
                (1, 'Target', 'Implement feature', 'completed', 0, 'main',
                 'pending', 0, 2, 'auto', '2026-08-01 09:00:00')
            """))
            conn.execute(text(f"""
                INSERT INTO tasks (
                    {required}, plan_target_task_id, plan_context_session_id,
                    plan_context_log_id, plan_context_snapshot,
                    plan_repo_revision, supersedes_plan_task_id, plan_content,
                    plan_approved, plan_approved_at, plan_approved_by,
                    plan_applied_at, plan_applied_to_session_id,
                    plan_applied_log_id, plan_pipeline_config, completed_at
                ) VALUES
                (2, 'Plan root', 'Design it', 'completed', 1, 'main',
                 'pending', 0, 2, 'plan', '2026-08-01 10:00:00',
                 1, 'session-1', 10, 'bounded context',
                 '{{"commit": "abc"}}', NULL, '# Version 1', 1,
                 '2026-08-01 10:30:00', 7, '2026-08-01 11:00:00',
                 'session-1', 11, '{{"planner": {{"provider": "claude"}}}}',
                 '2026-08-01 10:30:00'),
                (3, 'Plan revision', 'Add rollback', 'completed', 1, 'main',
                 'pending', 0, 2, 'plan', '2026-08-01 12:00:00',
                 1, 'session-1', 12, 'new bounded context',
                 '{{"commit": "def"}}', 2, '# Version 2', NULL,
                 NULL, NULL, NULL, NULL, NULL,
                 '{{"planner": {{"provider": "claude"}}}}',
                 '2026-08-01 12:30:00')
            """))
            conn.execute(text("""
                INSERT INTO log_entries (
                    id, task_id, event_type, content, timestamp, is_error
                ) VALUES (
                    11, 1, 'user', 'Applied Plan', '2026-08-01 11:00:00', 0
                )
            """))
            run_id = conn.execute(text("""
                INSERT INTO plan_agent_runs (
                    plan_task_id, status, round, review_exhausted,
                    created_at, updated_at
                ) VALUES (
                    3, 'completed', 1, 0,
                    '2026-08-01 12:00:00', '2026-08-01 12:30:00'
                ) RETURNING id
            """)).scalar_one()
            conn.execute(text("""
                INSERT INTO plan_agent_steps (
                    run_id, step_type, round, provider, status, started_at
                ) VALUES (
                    :run_id, 'planner', 1, 'claude', 'completed',
                    '2026-08-01 12:00:00'
                )
            """), {"run_id": run_id})
        engine.dispose()

        _run_alembic(cfg, command.upgrade, "head")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            for table in (
                "plans",
                "plan_versions",
                "plan_agent_runs",
                "plan_agent_steps",
                "plan_applications",
                "plan_legacy_task_links",
            ):
                assert conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar_one() == 0
            assert conn.execute(text(
                "SELECT COUNT(*) FROM tasks WHERE id IN (1, 2, 3)"
            )).scalar_one() == 3
        engine.dispose()

    def test_pending_failed_and_attachments_are_backfilled(self, tmp_path):
        db_path = str(tmp_path / "legacy_plan_states.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, "d2b8f6a10c43")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO tasks (
                    id, title, description, status, priority, target_branch,
                    merge_status, retry_count, max_retries, mode, metadata,
                    created_at
                ) VALUES
                (21, 'Pending legacy Plan', 'Wait to run', 'pending', 0,
                 'main', 'pending', 0, 2, 'plan', :metadata,
                 '2026-08-01 10:00:00')
            """), {"metadata": json.dumps({
                "file_paths": ["/uploads/requirements.txt"],
                "attachments": [{
                    "url": "/api/uploads/requirements.txt",
                    "name": "requirements.txt",
                    "is_image": False,
                }],
            })})
            conn.execute(text("""
                INSERT INTO tasks (
                    id, title, description, status, priority, target_branch,
                    merge_status, retry_count, max_retries, mode, metadata,
                    created_at
                ) VALUES
                (22, 'Failed legacy Plan', 'Failed before output', 'failed', 0,
                 'main', 'pending', 0, 2, 'plan', NULL,
                 '2026-08-01 11:00:00')
            """))
        engine.dispose()

        _run_alembic(cfg, command.upgrade, "head")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT l.legacy_task_id, l.plan_run_id, r.status,
                       p.active_run_id, p.initial_attachments, t.status AS task_status,
                       p.pipeline_config
                FROM plan_legacy_task_links l
                JOIN plan_agent_runs r ON r.id = l.plan_run_id
                JOIN plans p ON p.id = l.plan_id
                JOIN tasks t ON t.id = l.legacy_task_id
                ORDER BY l.legacy_task_id
            """)).mappings().all()
            assert [row["status"] for row in rows] == ["queued", "failed"]
            assert rows[0]["active_run_id"] == rows[0]["plan_run_id"]
            assert rows[1]["active_run_id"] is None
            assert [row["task_status"] for row in rows] == ["superseded", "failed"]
            attachments = rows[0]["initial_attachments"]
            if isinstance(attachments, str):
                attachments = json.loads(attachments)
            assert attachments == [{
                "url": "/api/uploads/requirements.txt",
                "name": "requirements.txt",
                "is_image": False,
                "path": "/uploads/requirements.txt",
            }]
            pipeline = rows[0]["pipeline_config"]
            if isinstance(pipeline, str):
                pipeline = json.loads(pipeline)
            assert pipeline["planner"]["primary"]["provider"] == "claude"
            assert pipeline["max_interactions"] == 3
        engine.dispose()

    def test_main_plan_task_states_preserve_review_and_execution_semantics(
        self,
        tmp_path,
    ):
        """Main approved by reusing the carrier Task; do not execute it twice."""
        db_path = str(tmp_path / "main_plan_task_states.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, "d2b8f6a10c43")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO tasks (
                    id, title, description, status, priority, target_branch,
                    merge_status, retry_count, max_retries, mode, plan_content,
                    plan_approved, plan_approved_at, created_at, completed_at
                ) VALUES
                (31, 'Needs decision', 'Review this', 'plan_review', 0,
                 'main', 'pending', 0, 2, 'plan', '# Review', NULL, NULL,
                 '2026-08-01 09:00:00', NULL),
                (32, 'Approved and queued', 'Execute this', 'pending', 0,
                 'main', 'pending', 0, 2, 'plan', '# Queued', 1, NULL,
                 '2026-08-01 10:00:00', NULL),
                (33, 'Already executed', 'Was executed', 'completed', 0,
                 'main', 'pending', 0, 2, 'plan', '# Done', 1, NULL,
                 '2026-08-01 11:00:00', '2026-08-01 12:00:00')
            """))
        engine.dispose()

        _run_alembic(cfg, command.upgrade, "head")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT l.legacy_task_id, t.status AS task_status,
                       p.active_run_id, v.review_verdict, v.human_decision,
                       a.application_type, a.execution_task_id
                FROM plan_legacy_task_links l
                JOIN tasks t ON t.id = l.legacy_task_id
                JOIN plans p ON p.id = l.plan_id
                JOIN plan_versions v ON v.id = l.plan_version_id
                LEFT JOIN plan_applications a
                  ON a.plan_version_id = l.plan_version_id
                ORDER BY l.legacy_task_id
            """)).mappings().all()

            assert dict(rows[0]) == {
                "legacy_task_id": 31,
                "task_status": "plan_review",
                "active_run_id": None,
                "review_verdict": "disabled",
                "human_decision": "pending",
                "application_type": None,
                "execution_task_id": None,
            }
            assert dict(rows[1]) == {
                "legacy_task_id": 32,
                "task_status": "pending",
                "active_run_id": None,
                "review_verdict": None,
                "human_decision": "approved",
                "application_type": "execution_task",
                "execution_task_id": 32,
            }
            assert dict(rows[2]) == {
                "legacy_task_id": 33,
                "task_status": "completed",
                "active_run_id": None,
                "review_verdict": None,
                "human_decision": "approved",
                "application_type": "execution_task",
                "execution_task_id": 33,
            }
        engine.dispose()

    def test_active_legacy_plan_process_blocks_backfill(self, tmp_path):
        db_path = str(tmp_path / "active_legacy_plan.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, "d2b8f6a10c43")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO tasks (
                    id, title, description, status, priority, target_branch,
                    merge_status, retry_count, max_retries, mode, created_at
                ) VALUES (
                    31, 'Active legacy Plan', 'Still running', 'executing', 0,
                    'main', 'pending', 0, 2, 'plan', '2026-08-01 10:00:00'
                )
            """))
            conn.execute(text("""
                INSERT INTO instances (
                    id, name, pid, status, current_task_id, provider, model,
                    total_tasks_completed, total_cost_usd
                ) VALUES (
                    41, 'legacy-owner', 12345, 'running', 31, 'claude',
                    'default', 0, 0
                )
            """))
        engine.dispose()

        with pytest.raises(
            RuntimeError,
            match="active process evidence",
        ):
            _run_alembic(cfg, command.upgrade, "head")

    def test_active_legacy_plan_task_state_blocks_without_instance(self, tmp_path):
        db_path = str(tmp_path / "active_legacy_plan_without_instance.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, "d2b8f6a10c43")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO tasks (
                    id, title, description, status, priority, target_branch,
                    merge_status, retry_count, max_retries, mode, created_at
                ) VALUES (
                    32, 'Unowned active Plan', 'State is still authoritative',
                    'in_progress', 0, 'main', 'pending', 0, 2, 'plan',
                    '2026-08-01 10:00:00'
                )
            """))
        engine.dispose()

        with pytest.raises(
            RuntimeError,
            match="active state evidence",
        ):
            _run_alembic(cfg, command.upgrade, "head")

    def test_feature_branch_application_fields_are_not_migrated(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "legacy_applied_pending_plan.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, "d2b8f6a10c43")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO tasks (
                    id, title, description, status, priority, target_branch,
                    merge_status, retry_count, max_retries, mode, plan_content,
                    plan_approved, plan_approved_at, plan_applied_at,
                    plan_execution_task_id, created_at, completed_at
                ) VALUES
                (50, 'Applied legacy Plan', 'Historical application',
                 'completed', 0, 'main', 'pending', 0, 2, 'plan', '# Applied',
                 NULL, '2026-08-01 11:00:00', '2026-08-01 11:00:00', 51,
                 '2026-08-01 09:00:00', '2026-08-01 11:00:00'),
                (51, 'Execution Task', 'Implemented the Plan', 'completed', 0,
                 'main', 'pending', 0, 2, 'auto', NULL, NULL, NULL, NULL, NULL,
                 '2026-08-01 11:00:00', '2026-08-01 12:00:00')
            """))
        engine.dispose()

        _run_alembic(cfg, command.upgrade, "head")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            assert conn.execute(text("SELECT COUNT(*) FROM plans")).scalar_one() == 0
            assert conn.execute(text(
                "SELECT COUNT(*) FROM tasks WHERE id IN (50, 51)"
            )).scalar_one() == 2
        engine.dispose()

    def test_reconcile_keeps_main_carrier_and_deletes_feature_branch_plans(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "reconcile_feature_branch_plans.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, "f5b7c9d1e3a2")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO tasks (
                    id, title, description, status, priority, target_branch,
                    merge_status, retry_count, max_retries, mode, plan_content,
                    plan_approved, created_at, completed_at
                ) VALUES
                (60, 'Main Plan Task', 'Main request', 'completed', 0, 'main',
                 'pending', 0, 2, 'plan', '# Main content', 1,
                 '2026-04-01 09:00:00', '2026-04-01 11:00:00')
            """))
            conn.execute(text("""
                INSERT INTO tasks (
                    id, title, description, status, priority, target_branch,
                    merge_status, retry_count, max_retries, mode, plan_content,
                    plan_target_task_id, plan_approved_at, created_at,
                    completed_at
                ) VALUES
                (61, 'Branch Plan Task', 'Branch request', 'superseded', 0,
                 'main', 'pending', 0, 2, 'plan', '# Branch content', 60,
                 '2026-08-01 10:30:00', '2026-08-01 10:00:00',
                 '2026-08-01 11:00:00')
            """))
            conn.execute(text("""
                INSERT INTO plans (
                    id, title, initial_request, priority, pipeline_config,
                    current_version_id, archived_at, lock_version,
                    created_at, updated_at
                ) VALUES
                (100, 'Previously migrated Main', 'Old request', 0, '{}',
                 1002, '2026-08-02 09:00:00', 3,
                 '2026-04-01 09:00:00', '2026-08-02 09:00:00'),
                (101, 'Branch standalone', 'Discard me', 0, '{}',
                 1011, NULL, 0,
                 '2026-08-01 09:00:00', '2026-08-01 11:00:00')
            """))
            conn.execute(text("""
                INSERT INTO plan_versions (
                    id, plan_id, version_number, parent_version_id, content,
                    review_exhausted, human_decision,
                    superseded_by_version_id, created_at
                ) VALUES
                (1001, 100, 1, NULL, '# Old migrated content', 0, 'pending',
                 1002, '2026-04-01 11:00:00'),
                (1002, 100, 2, 1001, '# Branch revision', 0, 'approved',
                 NULL, '2026-08-02 09:00:00'),
                (1011, 101, 1, NULL, '# Branch standalone', 0, 'pending',
                 NULL, '2026-08-01 11:00:00')
            """))
            run_id = conn.execute(text("""
                INSERT INTO plan_agent_runs (
                    plan_task_id, plan_id, status, round, review_exhausted,
                    created_at, updated_at
                ) VALUES (
                    61, 101, 'completed', 1, 0,
                    '2026-08-01 10:00:00', '2026-08-01 11:00:00'
                ) RETURNING id
            """)).scalar_one()
            step_id = conn.execute(text("""
                INSERT INTO plan_agent_steps (
                    run_id, plan_id, step_type, round, provider, status,
                    started_at
                ) VALUES (
                    :run_id, 101, 'planner', 1, 'claude', 'completed',
                    '2026-08-01 10:00:00'
                ) RETURNING id
            """), {"run_id": run_id}).scalar_one()
            conn.execute(text("""
                INSERT INTO plan_input_requests (
                    plan_id, run_id, source_step_id, requested_by, questions,
                    status, idempotency_key, created_at
                ) VALUES (
                    101, :run_id, :step_id, 'planner', '[]', 'open',
                    'branch-input', '2026-08-01 10:30:00'
                )
            """), {"run_id": run_id, "step_id": step_id})
            conn.execute(text("""
                INSERT INTO plan_applications (
                    plan_id, plan_version_id, application_type,
                    execution_task_id, created_at
                ) VALUES (
                    100, 1002, 'execution_task', 61,
                    '2026-08-02 10:00:00'
                )
            """))
            conn.execute(text("""
                INSERT INTO plan_application_receipts (
                    receipt_key, target_task_id, plan_version_ids, status,
                    created_at, updated_at
                ) VALUES (
                    'branch-receipt', 61, '[1002]', 'completed',
                    '2026-08-02 10:00:00', '2026-08-02 10:00:00'
                )
            """))
            conn.execute(text("""
                INSERT INTO plan_legacy_task_links (
                    legacy_task_id, plan_id, plan_version_id, plan_run_id,
                    created_at
                ) VALUES
                (60, 100, 1001, NULL, '2026-04-01 09:00:00'),
                (61, 101, 1011, :run_id, '2026-08-01 09:00:00')
            """), {"run_id": run_id})
        engine.dispose()

        _run_alembic(cfg, command.upgrade, "head")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            plan = conn.execute(text("""
                SELECT id, title, current_version_id, active_run_id,
                       archived_at, lock_version
                FROM plans
            """)).mappings().one()
            assert plan["id"] == 100
            assert plan["title"] == "Main Plan Task"
            assert plan["archived_at"] is None
            assert plan["lock_version"] == 0

            version = conn.execute(text("""
                SELECT id, plan_id, version_number, parent_version_id, content,
                       human_decision, superseded_by_version_id
                FROM plan_versions
            """)).mappings().one()
            assert dict(version) == {
                "id": 1001,
                "plan_id": 100,
                "version_number": 1,
                "parent_version_id": None,
                "content": "# Main content",
                "human_decision": "approved",
                "superseded_by_version_id": None,
            }
            assert plan["current_version_id"] == 1001
            assert plan["active_run_id"] is None

            application = conn.execute(text("""
                SELECT plan_id, plan_version_id, application_type,
                       execution_task_id
                FROM plan_applications
            """)).mappings().one()
            assert dict(application) == {
                "plan_id": 100,
                "plan_version_id": 1001,
                "application_type": "execution_task",
                "execution_task_id": 60,
            }
            link = conn.execute(text("""
                SELECT legacy_task_id, plan_id, plan_version_id, plan_run_id
                FROM plan_legacy_task_links
            """)).mappings().one()
            assert link["legacy_task_id"] == 60
            assert link["plan_id"] == 100
            assert link["plan_version_id"] == 1001
            assert link["plan_run_id"] is not None

            assert conn.execute(text(
                "SELECT COUNT(*) FROM plan_agent_runs"
            )).scalar_one() == 1
            for table in (
                "plan_agent_steps",
                "plan_input_requests",
                "plan_application_receipts",
            ):
                assert conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar_one() == 0
            assert conn.execute(text(
                "SELECT COUNT(*) FROM tasks WHERE id=61"
            )).scalar_one() == 1
        engine.dispose()

    def test_reconcile_blocks_while_canonical_run_waits_for_user(self, tmp_path):
        db_path = str(tmp_path / "active_canonical_plan.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, "f5b7c9d1e3a2")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO plan_agent_runs (
                    plan_task_id, status, round, review_exhausted,
                    created_at, updated_at
                ) VALUES (
                    NULL, 'waiting_user', 1, 0,
                    '2026-08-01 10:00:00', '2026-08-01 11:00:00'
                )
            """))
        engine.dispose()

        with pytest.raises(
            RuntimeError,
            match="canonical Run has active state evidence",
        ):
            _run_alembic(cfg, command.upgrade, "head")

    def test_repair_migration_only_approves_versions_with_applications(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "repair_applied_pending_plan.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, "f1a8c4d72e90")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO plans (
                    id, title, initial_request, priority, pipeline_config,
                    current_version_id, lock_version, created_at, updated_at
                ) VALUES (
                    90, 'Migrated Plan', 'Repair it', 0, '{}', 902, 0,
                    '2026-08-01 09:00:00', '2026-08-01 11:00:00'
                )
            """))
            conn.execute(text("""
                INSERT INTO plan_versions (
                    id, plan_id, version_number, content, review_exhausted,
                    human_decision, superseded_by_version_id, created_at
                ) VALUES
                (901, 90, 1, '# Superseded', 0, 'pending', 902,
                 '2026-08-01 10:00:00'),
                (902, 90, 2, '# Applied', 0, 'pending', NULL,
                 '2026-08-01 11:00:00')
            """))
            conn.execute(text("""
                INSERT INTO plan_applications (
                    plan_id, plan_version_id, application_type,
                    execution_task_id, applied_by, created_at
                ) VALUES (
                    90, 902, 'execution_task', 999, 77,
                    '2026-08-01 12:00:00'
                )
            """))
        engine.dispose()

        _run_alembic(cfg, command.upgrade, "f5b7c9d1e3a2")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT id, human_decision, decided_at, decided_by
                FROM plan_versions ORDER BY id
            """)).mappings().all()
            assert dict(rows[0]) == {
                "id": 901,
                "human_decision": "pending",
                "decided_at": None,
                "decided_by": None,
            }
            assert rows[1]["human_decision"] == "approved"
            assert rows[1]["decided_at"] is not None
            assert rows[1]["decided_by"] == 77
        engine.dispose()


class TestSchemaConsistency:
    """The schema produced by Alembic migrations matches the ORM models.

    This is the critical test: if someone adds a column to an ORM model
    but forgets to create an Alembic migration, this test will catch it.
    """

    def test_plan_application_integrity_constraint_compiles_on_all_dialects(self):
        table = backend.models.plan.PlanApplication.__table__
        for dialect in (sqlite.dialect(), postgresql.dialect(), mysql.dialect()):
            ddl = str(CreateTable(table).compile(dialect=dialect))
            assert "ck_plan_application_target" in ddl
            assert "execution_task" in ddl

    def test_capability_integrity_constraints_compile_on_all_dialects(self):
        for table_name in (
            "capability_invocations",
            "capability_executions",
        ):
            table = Base.metadata.tables[table_name]
            for dialect in (
                sqlite.dialect(),
                postgresql.dialect(),
                mysql.dialect(),
            ):
                ddl = str(CreateTable(table).compile(dialect=dialect))
                assert "active_slot" in ddl
                assert "UNIQUE" in ddl
                if table_name == "capability_invocations":
                    assert "uq_cap_inv_task_output_log" in ddl
                    assert "request_protocol_version >= 1" in ddl

    def test_terminal_turn_constraints_compile_on_all_dialects(self):
        table = Base.metadata.tables["log_entries"]
        for dialect in (
            sqlite.dialect(),
            postgresql.dialect(),
            mysql.dialect(),
        ):
            ddl = str(CreateTable(table).compile(dialect=dialect))
            assert "ck_log_entries_turn_scope" in ddl
            assert "'foreground'" in ddl
            assert "ck_log_entries_actual_transport" in ddl
            assert "actual_transport IS NULL" in ddl
            assert "turn_scope IS NOT NULL" in ddl
            assert "turn_scope = 'source'" in ddl
            assert "'codex_app_server'" in ddl

    def test_worker_plan_dispatch_digest_constraint_is_portable(self):
        table = Base.metadata.tables[
            "plan_agent_worker_dispatch_receipts"
        ]
        for dialect in (
            sqlite.dialect(),
            postgresql.dialect(),
            mysql.dialect(),
        ):
            ddl = str(CreateTable(table).compile(dialect=dialect))
            assert "ck_plan_worker_dispatch_state_shape" in ddl
            assert "length(payload_digest) = 64" in ddl
            assert ddl.count("replace(") >= 16
            assert "'f', '') = ''" in ddl

    def test_worker_plan_import_receipt_metadata_uses_innodb_on_mysql(self):
        table = Base.metadata.tables[
            "plan_agent_worker_import_receipts"
        ]
        mysql_ddl = str(CreateTable(table).compile(dialect=mysql.dialect()))
        assert "ENGINE=InnoDB" in mysql_ddl

    def test_transactional_feature_metadata_uses_innodb_on_mysql(self):
        for table_name in (
            "workspace_review_runs",
            "test_harness_runs",
            "test_harness_attempts",
            "test_harness_events",
            "test_harness_evidence",
            "test_harness_findings",
            "test_harness_sandbox_leases",
            "test_harness_child_bindings",
            "browser_review_operation_receipts",
            "ssh_profiles",
            "task_ssh_grants",
            "task_ssh_effect_receipts",
        ):
            table = Base.metadata.tables[table_name]
            mysql_ddl = str(
                CreateTable(table).compile(dialect=mysql.dialect())
            )
            assert "ENGINE=InnoDB" in mysql_ddl, table_name

    @pytest.mark.parametrize(
        ("filename", "table_names"),
        [
            (
                "5a7d2c9e1b40_add_workspace_review_runs.py",
                ("workspace_review_runs",),
            ),
            (
                "7d2f4b9a6c10_add_test_harness_records.py",
                (
                    "test_harness_runs",
                    "test_harness_attempts",
                    "test_harness_events",
                    "test_harness_evidence",
                    "test_harness_findings",
                ),
            ),
            (
                "c8f1a2d4e6b9_add_test_harness_sandbox_leases.py",
                ("test_harness_sandbox_leases",),
            ),
            (
                "e0b3c5d7f9a1_add_test_harness_child_bindings.py",
                ("test_harness_child_bindings",),
            ),
            (
                "6f3b9d2a7c10_add_browser_operation_receipts.py",
                ("browser_review_operation_receipts",),
            ),
            (
                "73c4a9e1b2d0_add_managed_ssh_profiles.py",
                ("ssh_profiles",),
            ),
            (
                "84d5b0f2c3e1_add_task_ssh_grants.py",
                ("task_ssh_grants",),
            ),
            (
                "c2f8a6d4e1b9_add_task_ssh_effect_receipts.py",
                ("task_ssh_effect_receipts",),
            ),
        ],
    )
    def test_transactional_feature_migrations_use_innodb_on_mysql(
        self,
        filename,
        table_names,
    ):
        from alembic.migration import MigrationContext
        from alembic.operations import Operations

        module = _load_revision_migration(filename, "mysql_innodb")
        output = io.StringIO()
        migration_context = MigrationContext.configure(
            dialect_name="mysql",
            opts={"as_sql": True, "output_buffer": output},
        )
        patches = {"op": Operations(migration_context)}
        if hasattr(module, "_require_mysql_transaction_tables"):
            patches["_require_mysql_transaction_tables"] = lambda: None
        with patch.multiple(module, **patches):
            module.upgrade()

        ddl = output.getvalue()
        for table_name in table_names:
            start = ddl.index(f"CREATE TABLE {table_name}")
            statement = ddl[start:ddl.index(";", start)]
            assert "ENGINE=InnoDB" in statement, table_name

    @pytest.mark.parametrize(
        ("filename", "expected_default"),
        [
            (
                "91e6a4c8d2f0_add_ssh_profile_task_policy.py",
                "task_capabilities JSON NOT NULL DEFAULT ('[]')",
            ),
            (
                "a6d9f2c4e8b1_add_ssh_profile_allowed_roots.py",
                "allowed_roots JSON NOT NULL DEFAULT ('[\"/\"]')",
            ),
            (
                "f1c4e6a8b0d2_add_test_harness_archive_state.py",
                "archive_manifest JSON NOT NULL DEFAULT ('{}')",
            ),
        ],
    )
    def test_json_defaults_compile_as_mysql_expressions(
        self,
        filename,
        expected_default,
    ):
        from alembic.migration import MigrationContext
        from alembic.operations import Operations

        module = _load_revision_migration(filename, "mysql_json_default")
        output = io.StringIO()
        migration_context = MigrationContext.configure(
            dialect_name="mysql",
            opts={"as_sql": True, "output_buffer": output},
        )
        with patch.object(module, "op", Operations(migration_context)):
            module.upgrade()

        ddl = " ".join(output.getvalue().split())
        assert expected_default in ddl

    def test_json_default_metadata_compiles_as_mysql_expressions(self):
        ssh_ddl = " ".join(str(CreateTable(
            Base.metadata.tables["ssh_profiles"]
        ).compile(dialect=mysql.dialect())).split())
        assert "task_capabilities JSON NOT NULL DEFAULT ('[]')" in ssh_ddl
        assert (
            "allowed_roots JSON NOT NULL DEFAULT ('[\"/\"]')"
            in ssh_ddl
        )

        attempt_ddl = " ".join(str(CreateTable(
            Base.metadata.tables["test_harness_attempts"]
        ).compile(dialect=mysql.dialect())).split())
        assert "archive_manifest JSON NOT NULL DEFAULT ('{}')" in attempt_ddl

    def test_worker_termination_constraints_compile_on_all_dialects(self):
        table = Base.metadata.tables["worker_task_termination_receipts"]
        for dialect in (
            sqlite.dialect(),
            postgresql.dialect(),
            mysql.dialect(),
        ):
            ddl = str(CreateTable(table).compile(dialect=dialect))
            assert "ck_worker_task_term_active_slot" in ddl
            assert "ck_worker_task_term_source_status" in ddl
            assert "ck_worker_task_term_handoff_shape" in ddl
            assert (
                "source_worker_turn_handoff_acknowledged IN (TRUE, FALSE)"
                in ddl
            )
            assert "ck_worker_task_term_counters" in ddl
            assert "ck_worker_task_term_execution_owner" in ddl
            assert "execution_token" in ddl
            assert "state_version" in ddl
            assert "next_reconcile_at IS NOT NULL" in ddl
            assert "ck_worker_task_term_ack_intent" in ddl
            assert "ack_intent_at" in ddl
            assert "reconcile_count >= 0" in ddl
            assert (
                "'accepted', 'executing', 'succeeded', 'rejected', 'conflict'"
                in ddl
            )
            assert "uq_worker_task_term_active_task" in ddl
            assert "FOREIGN KEY(task_id) REFERENCES tasks" in ddl

        mysql_ddl = str(CreateTable(table).compile(dialect=mysql.dialect()))
        assert "ENGINE=InnoDB" in mysql_ddl

    def test_worker_termination_migration_constraints_match_orm(self, tmp_path):
        db_path = str(tmp_path / "worker-termination-schema.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, "head")
        engine = create_engine(f"sqlite:///{db_path}")
        inspector = inspect(engine)
        table_name = "worker_task_termination_receipts"
        table = Base.metadata.tables[table_name]

        expected_indexes = {
            (index.name, tuple(column.name for column in index.columns))
            for index in table.indexes
        }
        actual_indexes = {
            (index["name"], tuple(index["column_names"]))
            for index in inspector.get_indexes(table_name)
        }
        assert actual_indexes == expected_indexes

        expected_uniques = {
            (
                constraint.name,
                tuple(column.name for column in constraint.columns),
            )
            for constraint in table.constraints
            if isinstance(constraint, UniqueConstraint)
        }
        actual_uniques = {
            (constraint["name"], tuple(constraint["column_names"]))
            for constraint in inspector.get_unique_constraints(table_name)
        }
        assert actual_uniques == expected_uniques

        expected_checks = {
            constraint.name
            for constraint in table.constraints
            if isinstance(constraint, CheckConstraint)
        }
        actual_checks = {
            constraint["name"]
            for constraint in inspector.get_check_constraints(table_name)
        }
        assert actual_checks == expected_checks
        module = _load_terminal_arbitration_migration(
            "worker_termination_orm_expression_match"
        )
        delete_module = _load_worker_task_delete_receipt_migration(
            "worker_termination_orm_expression_match"
        )
        orm_check_sql = {
            constraint.name: constraint.sqltext
            for constraint in table.constraints
            if isinstance(constraint, CheckConstraint)
        }
        effective_checks = dict(module._WORKER_TASK_TERMINATION_CHECKS)
        effective_checks[delete_module._OPERATION_CONSTRAINT] = (
            delete_module._NEW_OPERATIONS
        )
        effective_checks[delete_module._SOURCE_STATUS_CONSTRAINT] = (
            delete_module._NEW_SOURCE_STATUSES
        )
        effective_checks[delete_module._DELETE_SIDE_CONSTRAINT] = (
            delete_module._DELETE_MANAGER_ONLY
        )
        assert set(orm_check_sql) == set(effective_checks)
        for name, expected_sql in effective_checks.items():
            assert module._boolean_check_shape(
                orm_check_sql[name]
            ) == module._boolean_check_shape(expected_sql)

        expected_foreign_keys = {
            (
                tuple(
                    element.parent.name for element in constraint.elements
                ),
                constraint.elements[0].column.table.name,
                tuple(
                    element.column.name for element in constraint.elements
                ),
                constraint.ondelete,
            )
            for constraint in table.constraints
            if isinstance(constraint, ForeignKeyConstraint)
        }
        actual_foreign_keys = {
            (
                tuple(constraint["constrained_columns"]),
                constraint["referred_table"],
                tuple(constraint["referred_columns"]),
                (constraint.get("options") or {}).get("ondelete"),
            )
            for constraint in inspector.get_foreign_keys(table_name)
        }
        assert actual_foreign_keys == expected_foreign_keys
        engine.dispose()

    def test_code_review_integrity_constraints_compile_on_all_dialects(self):
        for table_name in ("code_review_runs", "code_review_results"):
            table = Base.metadata.tables[table_name]
            for dialect in (
                sqlite.dialect(),
                postgresql.dialect(),
                mysql.dialect(),
            ):
                ddl = str(CreateTable(table).compile(dialect=dialect))
                assert "code_review" in ddl
                assert "UNIQUE" in ddl

    @pytest.mark.parametrize("dialect_name", ("postgresql", "mysql"))
    def test_auto_capability_turn_migration_compiles_offline(self, dialect_name):
        migration_path = (
            PROJECT_ROOT
            / "alembic"
            / "versions"
            / "c3a7e9f1b2d4_add_auto_capability_turn_identity.py"
        )
        spec = importlib.util.spec_from_file_location(
            f"auto_capability_turn_migration_for_{dialect_name}",
            migration_path,
        )
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        from alembic.migration import MigrationContext
        from alembic.operations import Operations

        upgrade_output = io.StringIO()
        upgrade_context = MigrationContext.configure(
            dialect_name=dialect_name,
            opts={"as_sql": True, "output_buffer": upgrade_output},
        )
        with patch.object(module, "op", Operations(upgrade_context)):
            module.upgrade()
        upgrade_ddl = upgrade_output.getvalue().lower()
        assert "turn_generation" in upgrade_ddl
        assert "capability_policy" in upgrade_ddl
        assert "task_turn_generation" in upgrade_ddl
        assert "native_turn_id" in upgrade_ddl
        assert "request_output_log_id" in upgrade_ddl
        assert "request_native_turn_id" in upgrade_ddl

        downgrade_output = io.StringIO()
        downgrade_context = MigrationContext.configure(
            dialect_name=dialect_name,
            opts={"as_sql": True, "output_buffer": downgrade_output},
        )
        with patch.object(module, "op", Operations(downgrade_context)):
            module.downgrade()
        downgrade_ddl = downgrade_output.getvalue().lower()
        assert "drop column turn_generation" in downgrade_ddl
        assert "drop column capability_policy" in downgrade_ddl
        assert "drop column task_turn_generation" in downgrade_ddl
        assert "drop column native_turn_id" in downgrade_ddl
        assert "drop column request_output_log_id" in downgrade_ddl
        assert "drop column request_native_turn_id" in downgrade_ddl

    def test_terminal_arbitration_postgresql_migration_compiles_offline(self):
        module = _load_terminal_arbitration_migration("postgresql")

        from alembic.migration import MigrationContext
        from alembic.operations import Operations

        upgrade_output = io.StringIO()
        upgrade_context = MigrationContext.configure(
            dialect_name="postgresql",
            opts={"as_sql": True, "output_buffer": upgrade_output},
        )
        with patch.object(module, "op", Operations(upgrade_context)):
            module.upgrade()
        upgrade_ddl = upgrade_output.getvalue().lower()
        assert "turn_source_log_id" in upgrade_ddl
        assert "turn_scope" in upgrade_ddl
        assert "ck_log_entries_turn_scope" in upgrade_ddl
        assert "actual_transport" in upgrade_ddl
        assert "ck_log_entries_actual_transport" in upgrade_ddl
        assert "turn_scope is not null" in upgrade_ddl
        assert "turn_scope = 'source'" in upgrade_ddl
        assert "request_reason" in upgrade_ddl
        assert "request_protocol_version" in upgrade_ddl
        assert "request_output_hash" in upgrade_ddl
        assert "uq_cap_inv_task_output_log" in upgrade_ddl
        assert "request_protocol_version >= 1" in upgrade_ddl
        assert "create table worker_task_termination_receipts" in upgrade_ddl
        assert "ck_worker_task_term_active_slot" in upgrade_ddl
        assert "ck_worker_task_term_ack_intent" in upgrade_ddl
        assert "ack_intent_at" in upgrade_ddl
        upgrade_lock_sql = (
            "lock table tasks, log_entries, capability_invocations "
            "in access exclusive mode"
        )
        assert upgrade_ddl.index(upgrade_lock_sql) < upgrade_ddl.index(
            "alter table"
        )

        downgrade_output = io.StringIO()
        downgrade_context = MigrationContext.configure(
            dialect_name="postgresql",
            opts={"as_sql": True, "output_buffer": downgrade_output},
        )
        with patch.object(module, "op", Operations(downgrade_context)):
            module.downgrade()
        downgrade_ddl = downgrade_output.getvalue().lower()
        assert "drop column turn_source_log_id" in downgrade_ddl
        assert "drop column turn_scope" in downgrade_ddl
        assert "drop column actual_transport" in downgrade_ddl
        assert "drop column request_reason" in downgrade_ddl
        assert "drop column request_protocol_version" in downgrade_ddl
        assert "drop column request_output_hash" in downgrade_ddl
        assert "drop constraint uq_cap_inv_task_output_log" in downgrade_ddl
        assert "drop table worker_task_termination_receipts" in downgrade_ddl
        guard_index = downgrade_ddl.index("do $ccm_terminal_arbitration$")
        downgrade_lock_sql = (
            "lock table tasks, log_entries, capability_invocations, "
            "worker_task_termination_receipts in access exclusive mode"
        )
        assert downgrade_ddl.index(downgrade_lock_sql) < guard_index
        assert guard_index < downgrade_ddl.index("alter table")
        assert "where source = 'agent_request'" in downgrade_ddl
        assert "where turn_source_log_id is not null" in downgrade_ddl
        assert (
            "where turn_scope is not null or actual_transport is not null"
            in downgrade_ddl
        )
        assert "select 1 from worker_task_termination_receipts" in downgrade_ddl
        assert downgrade_ddl.count("raise exception") == 4

    @pytest.mark.parametrize("direction", ("upgrade", "downgrade"))
    def test_terminal_arbitration_mysql_offline_is_refused(self, direction):
        module = _load_terminal_arbitration_migration(
            f"mysql_offline_{direction}"
        )

        from alembic.migration import MigrationContext
        from alembic.operations import Operations

        output = io.StringIO()
        context = MigrationContext.configure(
            dialect_name="mysql",
            opts={"as_sql": True, "output_buffer": output},
        )
        with (
            patch.object(module, "op", Operations(context)),
            pytest.raises(RuntimeError, match="refuses MySQL offline SQL"),
        ):
            getattr(module, direction)()
        assert output.getvalue() == ""

    @pytest.mark.parametrize("dialect_name", ("postgresql", "mysql"))
    def test_delivery_migration_compiles_offline(self, dialect_name):
        migration_path = (
            PROJECT_ROOT
            / "alembic"
            / "versions"
            / "9e5b2a7c4d10_add_delivery_loop_state.py"
        )
        spec = importlib.util.spec_from_file_location(
            f"delivery_migration_for_{dialect_name}", migration_path
        )
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        from alembic.migration import MigrationContext
        from alembic.operations import Operations

        upgrade_output = io.StringIO()
        upgrade_context = MigrationContext.configure(
            dialect_name=dialect_name,
            opts={"as_sql": True, "output_buffer": upgrade_output},
        )
        with patch.object(module, "op", Operations(upgrade_context)):
            module.upgrade()
        upgrade_ddl = upgrade_output.getvalue().lower()
        assert "create table delivery_runs" in upgrade_ddl
        assert "create table delivery_transitions" in upgrade_ddl
        assert "ck_tasks_delivery_owner_shape" in upgrade_ddl
        assert "uq_plan_agent_runs_capability_execution" in upgrade_ddl
        assert "uq_worktrees_delivery_run" in upgrade_ddl
        assert "idempotency_key varchar(191)" in upgrade_ddl

        downgrade_output = io.StringIO()
        downgrade_context = MigrationContext.configure(
            dialect_name=dialect_name,
            opts={"as_sql": True, "output_buffer": downgrade_output},
        )
        with patch.object(module, "op", Operations(downgrade_context)):
            with pytest.raises(RuntimeError, match="Offline downgrade"):
                module.downgrade()
            # Compile the destructive statements separately only to prove the
            # dialect syntax. Production offline downgrade remains refused.
            with patch.object(
                module,
                "_assert_delivery_history_empty",
                return_value=None,
            ):
                module.downgrade()
        downgrade_ddl = downgrade_output.getvalue().lower()
        assert "drop table delivery_runs" in downgrade_ddl
        assert "drop column delivery_run_id" in downgrade_ddl
        assert "uq_plan_agent_runs_capability_execution" in downgrade_ddl

    def test_delivery_schema_constraints_and_indexes_match_orm(self, tmp_path):
        db_path = str(tmp_path / "delivery-schema.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, CURRENT_HEAD_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        inspector = inspect(engine)

        for table_name in (
            "delivery_runs",
            "delivery_cycles",
            "delivery_turns",
            "delivery_events",
            "delivery_actions",
            "delivery_transitions",
        ):
            table = Base.metadata.tables[table_name]

            expected_indexes = {
                (index.name, tuple(column.name for column in index.columns))
                for index in table.indexes
            }
            actual_indexes = {
                (index["name"], tuple(index["column_names"]))
                for index in inspector.get_indexes(table_name)
            }
            assert actual_indexes == expected_indexes

            expected_uniques = {
                (
                    constraint.name,
                    tuple(column.name for column in constraint.columns),
                )
                for constraint in table.constraints
                if isinstance(constraint, UniqueConstraint)
            }
            actual_uniques = {
                (constraint["name"], tuple(constraint["column_names"]))
                for constraint in inspector.get_unique_constraints(table_name)
            }
            assert actual_uniques == expected_uniques

            expected_checks = {
                constraint.name
                for constraint in table.constraints
                if isinstance(constraint, CheckConstraint)
            }
            actual_checks = {
                constraint["name"]
                for constraint in inspector.get_check_constraints(table_name)
            }
            assert actual_checks == expected_checks

            expected_foreign_keys = {
                (
                    tuple(element.parent.name for element in constraint.elements),
                    constraint.elements[0].column.table.name,
                    tuple(element.column.name for element in constraint.elements),
                    constraint.ondelete,
                )
                for constraint in table.constraints
                if isinstance(constraint, ForeignKeyConstraint)
            }
            actual_foreign_keys = {
                (
                    tuple(constraint["constrained_columns"]),
                    constraint["referred_table"],
                    tuple(constraint["referred_columns"]),
                    (constraint.get("options") or {}).get("ondelete"),
                )
                for constraint in inspector.get_foreign_keys(table_name)
            }
            assert actual_foreign_keys == expected_foreign_keys

        task_checks = {
            constraint["name"]
            for constraint in inspector.get_check_constraints("tasks")
        }
        assert "ck_tasks_delivery_owner_shape" in task_checks
        worktree_checks = {
            constraint["name"]
            for constraint in inspector.get_check_constraints("worktrees")
        }
        assert "ck_worktrees_cleanup_status" in worktree_checks
        worktree_uniques = {
            constraint["name"]
            for constraint in inspector.get_unique_constraints("worktrees")
        }
        assert "uq_worktrees_delivery_run" in worktree_uniques
        plan_run_uniques = {
            constraint["name"]
            for constraint in inspector.get_unique_constraints("plan_agent_runs")
        }
        assert "uq_plan_agent_runs_capability_execution" in plan_run_uniques
        delivery_action_columns = {
            column["name"]: column
            for column in inspector.get_columns("delivery_actions")
        }
        assert delivery_action_columns["idempotency_key"]["type"].length == 191
        engine.dispose()

    @pytest.mark.parametrize("dialect_name", ("postgresql", "mysql"))
    def test_capability_migration_compiles_offline(self, dialect_name):
        migration_path = (
            PROJECT_ROOT
            / "alembic"
            / "versions"
            / "6a4c2e9f1b73_add_capability_core.py"
        )
        spec = importlib.util.spec_from_file_location(
            f"capability_migration_for_{dialect_name}", migration_path
        )
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        from alembic.migration import MigrationContext
        from alembic.operations import Operations

        output = io.StringIO()
        context = MigrationContext.configure(
            dialect_name=dialect_name,
            opts={"as_sql": True, "output_buffer": output},
        )
        with patch.object(module, "op", Operations(context)):
            module.upgrade()
        ddl = output.getvalue().lower()
        assert "create table capability_invocations" in ddl
        assert "create table capability_executions" in ddl
        assert "ck_cap_inv_active_slot" in ddl
        assert "ck_cap_exec_active_slot" in ddl

    def test_migrated_schema_matches_orm(self, tmp_path):
        """Compare columns from Alembic-migrated DB vs ORM metadata.create_all."""
        # DB 1: created by Alembic migrations
        alembic_path = str(tmp_path / "alembic.db")
        cfg = _alembic_cfg(alembic_path)
        _run_alembic(cfg, command.upgrade, "head")
        alembic_engine = create_engine(f"sqlite:///{alembic_path}")

        # DB 2: created by ORM metadata.create_all
        orm_path = str(tmp_path / "orm.db")
        orm_engine = create_engine(f"sqlite:///{orm_path}")
        Base.metadata.create_all(orm_engine)

        # Compare tables
        alembic_tables = _get_all_tables(alembic_engine)
        orm_tables = _get_all_tables(orm_engine)
        assert alembic_tables == orm_tables, (
            f"Table mismatch.\n"
            f"  Only in Alembic: {alembic_tables - orm_tables}\n"
            f"  Only in ORM: {orm_tables - alembic_tables}"
        )

        # Compare columns for each table
        for table in sorted(orm_tables):
            alembic_cols = set(_get_table_columns(alembic_engine, table).keys())
            orm_cols = set(_get_table_columns(orm_engine, table).keys())
            assert alembic_cols == orm_cols, (
                f"Column mismatch in table '{table}'.\n"
                f"  Only in Alembic: {alembic_cols - orm_cols}\n"
                f"  Only in ORM (missing migration!): {orm_cols - alembic_cols}"
            )

        alembic_engine.dispose()
        orm_engine.dispose()

    def test_no_pending_autogenerate_changes(self, tmp_path):
        """Alembic autogenerate should detect no new changes.

        This verifies that the migrations fully cover the ORM models.
        If this fails, run: alembic revision --autogenerate -m 'description'
        """
        from alembic.autogenerate import compare_metadata

        db_path = str(tmp_path / "autogen.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, "head")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            from alembic.migration import MigrationContext
            mc = MigrationContext.configure(conn)
            diffs = compare_metadata(mc, Base.metadata)

            # Filter out differences that are cosmetic for SQLite:
            # - index differences (SQLite doesn't preserve index info perfectly)
            # - nullable differences (SQLite doesn't enforce NOT NULL strictly,
            #   and initial migration used nullable=True for columns with defaults)
            significant_diffs = [
                d for d in diffs
                if not (isinstance(d, tuple) and d[0] in ("add_index", "remove_index"))
                and not (isinstance(d, list) and len(d) == 1 and isinstance(d[0], tuple)
                         and d[0][0] == "modify_nullable")
            ]

            assert len(significant_diffs) == 0, (
                "Alembic autogenerate found pending changes (need a new migration!):\n"
                + "\n".join(str(d) for d in significant_diffs)
            )

        engine.dispose()


class TestPublishedMigrationHistory:
    """Published main history stays intact and Plan v2 advances linearly."""

    def _assert_revision_schema(
        self,
        engine,
        *,
        revisions,
        plan_schema_present,
        snapshot_schema_present,
    ):
        tables = _get_all_tables(engine)
        task_columns = _get_table_columns(engine, "tasks")
        log_columns = _get_table_columns(engine, "log_entries")
        review_columns = _get_table_columns(engine, "pr_reviews")

        assert ("plan_agent_runs" in tables) is plan_schema_present
        assert ("plan_agent_steps" in tables) is plan_schema_present
        assert (
            "plan_target_task_id" in task_columns
        ) is plan_schema_present
        assert (
            "task_retry_count" in log_columns
        ) is snapshot_schema_present
        assert ("base_sha" in review_columns) is snapshot_schema_present

        with engine.connect() as conn:
            current_revisions = {
                row[0]
                for row in conn.execute(
                    text("SELECT version_num FROM alembic_version")
                ).fetchall()
            }
        assert current_revisions == set(revisions)

    def test_migration_graph_merges_both_published_heads(self, tmp_path):
        cfg = _alembic_cfg(str(tmp_path / "graph.db"))
        script = ScriptDirectory.from_config(cfg)

        assert set(script.get_heads()) == {DIRECT_PR_MERGE_REVISION}
        assert (
            script.get_revision(DIRECT_PR_MERGE_REVISION).down_revision
            == PR_MERGE_QUEUE_TRIGGER_REVISION
        )
        assert set(
            script.get_revision(UPDATE_CHANNEL_REVISION).down_revision
        ) == {
            MAIN_SSH_MERGE_REVISION,
            PR_MONITOR_DISPLAY_TASK_REVISION,
        }
        assert (
            script.get_revision(PR_MONITOR_DISPLAY_TASK_REVISION).down_revision
            == PR_REVIEW_RESULT_EVIDENCE_REVISION
        )
        assert (
            script.get_revision(
                PR_REVIEW_RESULT_EVIDENCE_REVISION
            ).down_revision
            == INSTANCE_PROCESS_IDENTITY_REVISION
        )
        assert set(
            script.get_revision(
                DELIVERY_PR_MONITOR_MERGE_REVISION
            ).down_revision
        ) == {
            DELIVERY_PREVIEW_PROFILES_REVISION,
            PR_MONITOR_TASK_TOMBSTONE_REVISION,
        }
        assert (
            script.get_revision(
                DELIVERY_PREVIEW_PROFILES_REVISION
            ).down_revision
            == DELIVERY_FRONTEND_REVIEW_REVISION
        )
        assert (
            script.get_revision(
                PR_MONITOR_TASK_TOMBSTONE_REVISION
            ).down_revision
            == WORKER_RENAME_TAG_OUTBOX_REVISION
        )
        assert (
            script.get_revision(
                WORKER_RENAME_TAG_OUTBOX_REVISION
            ).down_revision
            == WORKER_DESTROY_LIFECYCLE_NONCE_REVISION
        )
        assert (
            script.get_revision(
                WORKER_DESTROY_LIFECYCLE_NONCE_REVISION
            ).down_revision
            == TASK_MIGRATION_OPERATION_REVISION
        )
        assert (
            script.get_revision(
                TASK_MIGRATION_OPERATION_REVISION
            ).down_revision
            == WORKER_NODE_DRAIN_REVISION
        )
        assert (
            script.get_revision(WORKER_NODE_DRAIN_REVISION).down_revision
            == UNIQUE_USER_GROUP_MEMBERSHIP_REVISION
        )
        assert (
            script.get_revision(
                UNIQUE_USER_GROUP_MEMBERSHIP_REVISION
            ).down_revision
            == TASK_ID_NAMESPACE_REVISION
        )
        assert (
            script.get_revision(TASK_ID_NAMESPACE_REVISION).down_revision
            == REMOVE_TEAM_SHARE_SSH_FENCES_REVISION
        )
        assert (
            script.get_revision(
                REMOVE_TEAM_SHARE_SSH_FENCES_REVISION
            ).down_revision
            == TASK_EXECUTION_PRINCIPAL_REVISION
        )
        assert (
            script.get_revision(TASK_EXECUTION_PRINCIPAL_REVISION).down_revision
            == REMOVE_AGENT_SANDBOX_OVERRIDE_REVISION
        )
        assert (
            script.get_revision(
                REMOVE_AGENT_SANDBOX_OVERRIDE_REVISION
            ).down_revision
            == DELIVERY_FRONTEND_REVIEW_REVISION
        )
        assert (
            script.get_revision(
                INSTANCE_PROCESS_IDENTITY_REVISION
            ).down_revision
            == DELIVERY_PR_MONITOR_MERGE_REVISION
        )
        assert (
            script.get_revision(DELIVERY_FRONTEND_REVIEW_REVISION).down_revision
            == AGENT_SANDBOX_RUNTIME_OVERRIDE_REVISION
        )
        assert (
            script.get_revision(
                AGENT_SANDBOX_RUNTIME_OVERRIDE_REVISION
            ).down_revision
            == CAPACITY_PR_LOOP_MERGE_REVISION
        )
        assert set(
            script.get_revision(
                CAPACITY_PR_LOOP_MERGE_REVISION
            ).down_revision
        ) == {PR_LOOP_REVISION, CAPACITY_OVERRIDE_REVISION}
        assert (
            script.get_revision(PR_LOOP_REVISION).down_revision
            == PR_REVIEW_BASE_REF_REVISION
        )
        assert (
            script.get_revision(CAPACITY_OVERRIDE_REVISION).down_revision
            == WORKER_PLAN_IMPORT_RECEIPT_REVISION
        )
        assert (
            script.get_revision(PR_REVIEW_BASE_REF_REVISION).down_revision
            == PR_REVIEW_MERGE_METHOD_REVISION
        )
        assert (
            script.get_revision(PR_REVIEW_MERGE_METHOD_REVISION).down_revision
            == BROWSER_OPERATION_RECEIPT_REVISION
        )
        assert (
            script.get_revision(BROWSER_OPERATION_RECEIPT_REVISION).down_revision
            == COMBINED_BROWSER_MAIN_REVISION
        )
        assert set(
            script.get_revision(COMBINED_BROWSER_MAIN_REVISION).down_revision
        ) == {
            CHILD_LAUNCH_PROFILE_REVISION,
            DISCUSSION_LEASE_REVISION,
        }
        assert set(
            script.get_revision(CHILD_LAUNCH_PROFILE_REVISION).down_revision
        ) == {ARCHIVE_STATE_REVISION, WORKER_PLAN_IMPORT_RECEIPT_REVISION}
        assert script.get_revision(ARCHIVE_STATE_REVISION).down_revision == CHILD_BINDING_REVISION
        assert (
            script.get_revision(CHILD_BINDING_REVISION).down_revision
            == RESOLVED_TARGET_REVISION
        )
        assert (
            script.get_revision(RESOLVED_TARGET_REVISION).down_revision
            == SANDBOX_LEASE_REVISION
        )
        assert (
            script.get_revision(SANDBOX_LEASE_REVISION).down_revision
            == BROWSER_PLAN_MERGE_REVISION
        )
        assert (
            script.get_revision(BROWSER_PLAN_MERGE_REVISION).down_revision
            == (TEST_HARNESS_REVISION, MAIN_PLAN_MERGE_REVISION)
        )
        assert (
            script.get_revision(TEST_HARNESS_REVISION).down_revision
            == WORKSPACE_REVIEW_REVISION
        )
        assert (
            script.get_revision(WORKSPACE_REVIEW_REVISION).down_revision
            == ATTENTION_TAG_REVISION
        )

        assert (
            script.get_revision(DISCUSSION_LEASE_REVISION).down_revision
            == TASK_SSH_EFFECT_REVISION
        )
        assert (
            script.get_revision(TASK_SSH_EFFECT_REVISION).down_revision
            == TASK_SCOPED_TOKEN_INCARNATION_REVISION
        )
        assert (
            script.get_revision(
                TASK_SCOPED_TOKEN_INCARNATION_REVISION
            ).down_revision
            == MAIN_SSH_MERGE_REVISION
        )
        assert set(
            script.get_revision(MAIN_SSH_MERGE_REVISION).down_revision
        ) == {
            TASK_SSH_ROOTS_REVISION,
            WORKER_PLAN_IMPORT_RECEIPT_REVISION,
        }
        assert (
            script.get_revision(TASK_SSH_ROOTS_REVISION).down_revision
            == TASK_SSH_POLICY_REVISION
        )
        assert (
            script.get_revision(TASK_SSH_POLICY_REVISION).down_revision
            == TASK_SSH_GRANTS_REVISION
        )
        assert (
            script.get_revision(TASK_SSH_GRANTS_REVISION).down_revision
            == SSH_PROFILES_REVISION
        )
        assert (
            script.get_revision(SSH_PROFILES_REVISION).down_revision
            == MAIN_PLAN_MERGE_REVISION
        )
        assert (
            script.get_revision(WORKER_PLAN_IMPORT_RECEIPT_REVISION).down_revision
            == WORKER_TASK_DELETE_RECEIPT_REVISION
        )
        assert (
            script.get_revision(WORKER_TASK_DELETE_RECEIPT_REVISION).down_revision
            == WORKER_PLAN_DISPATCH_RECEIPT_REVISION
        )
        assert (
            script.get_revision(WORKER_PLAN_DISPATCH_RECEIPT_REVISION).down_revision
            == PLAN_RUNTIME_RECEIPT_REVISION
        )
        assert (
            script.get_revision(PLAN_RUNTIME_RECEIPT_REVISION).down_revision
            == CAPABILITY_RESUME_OUTBOX_REVISION
        )
        assert (
            script.get_revision(CAPABILITY_RESUME_OUTBOX_REVISION).down_revision
            == TERMINAL_ARBITRATION_REVISION
        )
        assert (
            script.get_revision(TERMINAL_ARBITRATION_REVISION).down_revision
            == AUTO_CAPABILITY_TURN_REVISION
        )
        assert (
            script.get_revision(AUTO_CAPABILITY_TURN_REVISION).down_revision
            == DELIVERY_LOOP_REVISION
        )
        assert (
            script.get_revision(DELIVERY_LOOP_REVISION).down_revision
            == CODE_REVIEW_REVISION
        )
        assert (
            script.get_revision(CODE_REVIEW_REVISION).down_revision
            == CAPABILITY_CORE_REVISION
        )
        assert (
            script.get_revision(CAPABILITY_CORE_REVISION).down_revision
            == MAIN_PLAN_MERGE_REVISION
        )
        assert (
            script.get_revision(MAIN_PLAN_MERGE_REVISION).down_revision
            == (FIRST_CLASS_PLAN_HEAD_REVISION, ATTENTION_TAG_REVISION)
        )
        assert "3f2a9c8e7b10" not in {
            revision.revision for revision in script.walk_revisions()
        }
        assert (
            script.get_revision(PR_FINDING_ACTIONS_REVISION).down_revision
            == PR_REVIEW_PANEL_REVISION
        )
        assert (
            script.get_revision(PR_REVIEW_PANEL_REVISION).down_revision
            == PUBLISHED_BRANCH_MERGE_REVISION
        )

    def test_delivery_preview_profiles_revision_roundtrip(self, tmp_path):
        db_path = str(tmp_path / "delivery-preview-profiles.db")
        cfg = _alembic_cfg(db_path)

        _run_alembic(cfg, command.upgrade, DELIVERY_FRONTEND_REVIEW_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "frontend_review_profile_ids" not in _get_table_columns(
            engine, "delivery_cycles"
        )
        engine.dispose()

        _run_alembic(cfg, command.upgrade, DELIVERY_PREVIEW_PROFILES_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert {
            "frontend_review_config_snapshot",
            "frontend_review_profile_ids",
            "frontend_review_profile_index",
            "frontend_review_results",
        }.issubset(_get_table_columns(engine, "delivery_cycles"))
        engine.dispose()

        _run_alembic(cfg, command.downgrade, DELIVERY_FRONTEND_REVIEW_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "frontend_review_profile_ids" not in _get_table_columns(
            engine, "delivery_cycles"
        )
        engine.dispose()

    @pytest.mark.parametrize(
        "parent_revision",
        [CHILD_LAUNCH_PROFILE_REVISION, DISCUSSION_LEASE_REVISION],
    )
    def test_each_combined_parent_upgrades_to_browser_operation_head(
        self,
        tmp_path,
        parent_revision,
    ):
        db_path = str(tmp_path / f"combined-parent-{parent_revision}.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, parent_revision)

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalars().all() == [parent_revision]
        engine.dispose()

        _run_alembic(cfg, command.upgrade, CURRENT_HEAD_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "browser_review_operation_receipts" in _get_all_tables(engine)
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == CURRENT_HEAD_REVISION
        engine.dispose()

    def test_combined_browser_main_merge_downgrades_and_reupgrades(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "combined-browser-main-roundtrip.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, COMBINED_BROWSER_MAIN_REVISION)
        _run_alembic(
            cfg,
            command.downgrade,
            CHILD_LAUNCH_PROFILE_REVISION,
        )

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            assert set(conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalars()) == {
                CHILD_LAUNCH_PROFILE_REVISION,
                DISCUSSION_LEASE_REVISION,
            }
        engine.dispose()

        _run_alembic(cfg, command.upgrade, COMBINED_BROWSER_MAIN_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == COMBINED_BROWSER_MAIN_REVISION
        engine.dispose()

    def test_deployed_main_plan_head_upgrades_to_capability_head(self, tmp_path):
        db_path = str(tmp_path / "main-plan-to-capability.db")
        cfg = _alembic_cfg(db_path)

        _run_alembic(cfg, command.upgrade, MAIN_PLAN_MERGE_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "plans" in _get_all_tables(engine)
        assert "capability_invocations" not in _get_all_tables(engine)
        engine.dispose()

        _run_alembic(cfg, command.upgrade, CURRENT_HEAD_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        tables = _get_all_tables(engine)
        assert "plans" in tables
        assert "capability_invocations" in tables
        assert "capability_resume_outbox" in tables
        with engine.connect() as conn:
            assert conn.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == CURRENT_HEAD_REVISION
        engine.dispose()

    def test_deployed_main_head_upgrades_to_combined_plan_head(self, tmp_path):
        db_path = str(tmp_path / "main-to-combined.db")
        cfg = _alembic_cfg(db_path)

        _run_alembic(cfg, command.upgrade, ATTENTION_TAG_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "plans" not in _get_all_tables(engine)
        assert "attention_tag" in _get_table_columns(engine, "tasks")
        engine.dispose()

        _run_alembic(cfg, command.upgrade, CURRENT_HEAD_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "plans" in _get_all_tables(engine)
        task_columns = _get_table_columns(engine, "tasks")
        assert "attention_tag" in task_columns
        assert "plan_target_task_id" in task_columns
        with engine.connect() as conn:
            assert conn.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == CURRENT_HEAD_REVISION
        engine.dispose()

    def test_deployed_browser_harness_head_upgrades_to_combined_plan_head(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "browser-to-combined.db")
        cfg = _alembic_cfg(db_path)

        # This is the exact state deployed by the browser feature branch before
        # it was rebased onto the first-class Plan migration history.
        _run_alembic(cfg, command.upgrade, TEST_HARNESS_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        tables = _get_all_tables(engine)
        assert "workspace_review_runs" in tables
        assert "test_harness_runs" in tables
        assert "plans" not in tables
        assert "plan_pipeline_config" not in _get_table_columns(
            engine,
            "global_settings",
        )
        engine.dispose()

        _run_alembic(cfg, command.upgrade, CURRENT_HEAD_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        tables = _get_all_tables(engine)
        assert "workspace_review_runs" in tables
        assert "test_harness_runs" in tables
        assert "plans" in tables
        assert "plan_pipeline_config" in _get_table_columns(
            engine,
            "global_settings",
        )
        with engine.connect() as conn:
            assert conn.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == CURRENT_HEAD_REVISION
        engine.dispose()

    def test_existing_external_key_profiles_stay_files_only_on_upgrade(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "ssh-policy.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, TASK_SSH_GRANTS_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO ssh_profiles (
                    name, host, port, username, key_path,
                    public_key_fingerprint, host_key_type, host_key_value,
                    host_key_fingerprint, revision, enabled,
                    created_at, updated_at
                ) VALUES (
                    'existing', 'ssh.example.internal', 22, 'deploy', '/tmp/key',
                    'SHA256:client', 'ssh-ed25519', 'ssh-ed25519 AAAA',
                    'SHA256:host', 1, 1,
                    '2026-08-07 00:00:00', '2026-08-07 00:00:00'
                )
            """))
        engine.dispose()

        _run_alembic(cfg, command.upgrade, "head")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            row = conn.execute(text("""
                SELECT task_access_enabled, task_capabilities, allowed_roots
                FROM ssh_profiles WHERE name = 'existing'
            """)).one()
        assert bool(row[0]) is False
        assert json.loads(row[1]) == []
        assert json.loads(row[2]) == ["/"]
        engine.dispose()

    def test_scoped_token_migration_backfills_legacy_task_incarnation(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "task-token-incarnation.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, MAIN_SSH_MERGE_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO tasks "
                "(id, title, description, status, priority, target_branch, "
                "merge_status, retry_count, max_retries, mode, incarnation_id, "
                "created_at) VALUES "
                "(73, 'legacy token task', 'd', 'pending', 0, 'main', "
                "'pending', 0, 2, 'auto', NULL, '2026-08-08 00:00:00')"
            ))
        engine.dispose()

        _run_alembic(cfg, command.upgrade, CURRENT_HEAD_REVISION)

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            incarnation = conn.execute(text(
                "SELECT incarnation_id FROM tasks WHERE id = 73"
            )).scalar_one()
            assert len(incarnation) == 32
            assert set(incarnation) <= set("0123456789abcdef")
        engine.dispose()

    @pytest.mark.parametrize(
        ("dialect_name", "expected_fragment", "forbidden_fragment"),
        [
            ("sqlite", "lower(hex(randomblob(16)))", "uuid()"),
            ("postgresql", "clock_timestamp()::text", "uuid()"),
            (
                "mysql",
                "lower(replace(uuid(), '-', ''))",
                "random_bytes",
            ),
            (
                "mariadb",
                "lower(replace(uuid(), '-', ''))",
                "random_bytes",
            ),
        ],
    )
    def test_task_incarnation_backfill_uses_portable_dialect_expression(
        self,
        monkeypatch,
        dialect_name,
        expected_fragment,
        forbidden_fragment,
    ):
        module = _load_task_incarnation_migration(
            f"dialect_{dialect_name}"
        )
        statements = []
        monkeypatch.setattr(
            module.op,
            "get_bind",
            lambda: SimpleNamespace(
                dialect=SimpleNamespace(name=dialect_name)
            ),
        )
        monkeypatch.setattr(module.op, "execute", statements.append)

        module.upgrade()

        assert len(statements) == 1
        rendered = str(statements[0]).lower()
        assert expected_fragment in rendered
        assert forbidden_fragment not in rendered

    def test_task_ssh_effect_receipt_migration_roundtrip(self, tmp_path):
        db_path = str(tmp_path / "task-ssh-effect-receipt.db")
        cfg = _alembic_cfg(db_path)

        _run_alembic(
            cfg,
            command.upgrade,
            TASK_SCOPED_TOKEN_INCARNATION_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        assert "task_ssh_effect_receipts" not in _get_all_tables(engine)
        engine.dispose()

        _run_alembic(cfg, command.upgrade, TASK_SSH_EFFECT_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        inspector = inspect(engine)
        assert "task_ssh_effect_receipts" in _get_all_tables(engine)
        assert inspector.get_foreign_keys("task_ssh_effect_receipts") == []
        assert {
            "effect_id",
            "task_incarnation_id",
            "task_retry_count",
            "task_turn_generation",
            "profile_revision",
            "operation",
            "request_digest",
            "status",
            "result_payload",
            "result_compacted",
        }.issubset(_get_table_columns(
            engine,
            "task_ssh_effect_receipts",
        ))
        assert {
            "ix_task_ssh_effect_unknown_digest",
            "ix_task_ssh_effect_generation_count",
        }.issubset({
            index["name"]
            for index in inspector.get_indexes("task_ssh_effect_receipts")
        })
        with engine.connect() as conn:
            trigger_names = set(conn.execute(text(
                "SELECT name FROM sqlite_master WHERE type = 'trigger' "
                "AND name LIKE 'trg_task_ssh_effect_%'"
            )).scalars())
        from backend.models.task_ssh_effect import (
            SQLITE_TASK_SSH_EFFECT_TRIGGER_NAMES,
        )

        # This assertion targets the historical c2f8 revision.  The current
        # ORM trigger list intentionally omits the Team Share fences removed
        # later by c9f5, so include those historical names explicitly instead
        # of comparing an old schema to today's runtime constants.
        historical_team_share_triggers = {
            "trg_task_ssh_effect_team_task_share_insert",
            "trg_task_ssh_effect_team_task_share_update",
            "trg_task_ssh_effect_team_task_share_delete",
            "trg_task_ssh_effect_team_project_share_insert",
            "trg_task_ssh_effect_team_project_share_update",
            "trg_task_ssh_effect_team_project_share_delete",
        }
        assert trigger_names == (
            set(SQLITE_TASK_SSH_EFFECT_TRIGGER_NAMES)
            | historical_team_share_triggers
        )
        engine.dispose()

        _run_alembic(
            cfg,
            command.downgrade,
            TASK_SCOPED_TOKEN_INCARNATION_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        assert "task_ssh_effect_receipts" not in _get_all_tables(engine)
        engine.dispose()

        _run_alembic(cfg, command.upgrade, TASK_SSH_EFFECT_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "task_ssh_effect_receipts" in _get_all_tables(engine)
        engine.dispose()

    @pytest.mark.parametrize(
        ("status", "result_payload", "result_digest", "outcome_code"),
        [
            ("running", None, None, None),
            ("completed", '{"ok":true}', "a" * 64, "success"),
            (
                "ambiguous",
                None,
                None,
                "remote_outcome_unknown",
            ),
            (
                "aborted",
                None,
                None,
                "cancelled_before_execution",
            ),
        ],
    )
    def test_task_ssh_effect_downgrade_refuses_permanent_evidence(
        self,
        tmp_path,
        status,
        result_payload,
        result_digest,
        outcome_code,
    ):
        db_path = str(tmp_path / f"task-ssh-effect-{status}.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, TASK_SSH_EFFECT_REVISION)
        completed_at = (
            None if status == "running" else "2026-08-09 00:00:01"
        )

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO task_ssh_effect_receipts (
                    effect_id, task_id, task_incarnation_id,
                    task_retry_count, task_turn_generation, task_status,
                    profile_id, profile_revision, operation, request_digest,
                    status, result_payload, result_digest, result_compacted,
                    outcome_code, created_at, updated_at, completed_at
                ) VALUES (
                    :effect_id, 91, :incarnation_id,
                    0, 0, 'pending',
                    73, 1, 'execute', :request_digest,
                    :status, :result_payload, :result_digest, false,
                    :outcome_code, '2026-08-09 00:00:00',
                    '2026-08-09 00:00:01', :completed_at
                )
            """), {
                "effect_id": f"{len(status):032x}",
                "incarnation_id": "b" * 32,
                "request_digest": "c" * 64,
                "status": status,
                "result_payload": result_payload,
                "result_digest": result_digest,
                "outcome_code": outcome_code,
                "completed_at": completed_at,
            })
        engine.dispose()

        with pytest.raises(RuntimeError, match="permanent.*evidence"):
            _run_alembic(
                cfg,
                command.downgrade,
                TASK_SCOPED_TOKEN_INCARNATION_REVISION,
            )

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT status FROM task_ssh_effect_receipts"
            )).scalar_one() == status
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == TASK_SSH_EFFECT_REVISION
        engine.dispose()

    def test_task_ssh_effect_offline_downgrade_fails_closed(
        self,
        monkeypatch,
    ):
        module = _load_task_ssh_effect_migration("offline_downgrade")
        monkeypatch.setattr(module.context, "is_offline_mode", lambda: True)
        get_bind = MagicMock()
        monkeypatch.setattr(module.op, "get_bind", get_bind)

        with pytest.raises(RuntimeError, match="Offline.*refused"):
            module._assert_downgrade_safe()

        get_bind.assert_not_called()

    @pytest.mark.parametrize(
        "dialect_name",
        ["sqlite", "postgresql", "mysql", "mariadb"],
    )
    def test_task_ssh_effect_downgrade_gate_uses_supported_bind(
        self,
        monkeypatch,
        dialect_name,
    ):
        module = _load_task_ssh_effect_migration(
            f"downgrade_bind_{dialect_name}"
        )
        result = MagicMock()
        result.scalar_one.return_value = 0
        bind = SimpleNamespace(
            dialect=SimpleNamespace(name=dialect_name),
            execute=MagicMock(return_value=result),
        )
        monkeypatch.setattr(module.context, "is_offline_mode", lambda: False)
        monkeypatch.setattr(module.op, "get_bind", lambda: bind)

        module._assert_downgrade_safe()

        statement = bind.execute.call_args.args[0]
        assert str(statement).strip().lower() == (
            "select count(*) from task_ssh_effect_receipts"
        )
        result.scalar_one.assert_called_once_with()

    @pytest.mark.parametrize(
        ("start_revision", "plan_schema_present", "snapshot_schema_present"),
        [
            (PUBLISHED_PLAN_REVISION, True, False),
            (PLAN_CLEANUP_REVISION, False, False),
            (PR_REVIEW_SNAPSHOT_REVISION, False, True),
        ],
    )
    def test_each_published_branch_upgrades_to_merge_head(
        self,
        tmp_path,
        start_revision,
        plan_schema_present,
        snapshot_schema_present,
    ):
        db_path = str(tmp_path / f"published-{start_revision}.db")
        cfg = _alembic_cfg(db_path)

        # Each revision was a deployable branch head before the histories met.
        _run_alembic(cfg, command.upgrade, start_revision)
        engine = create_engine(f"sqlite:///{db_path}")
        self._assert_revision_schema(
            engine,
            revisions={start_revision},
            plan_schema_present=plan_schema_present,
            snapshot_schema_present=snapshot_schema_present,
        )
        engine.dispose()

        # The no-op merge applies the missing sibling branch and converges all
        # deployed states on one schema/head.
        _run_alembic(cfg, command.upgrade, PUBLISHED_BRANCH_MERGE_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        self._assert_revision_schema(
            engine,
            revisions={PUBLISHED_BRANCH_MERGE_REVISION},
            plan_schema_present=False,
            snapshot_schema_present=True,
        )
        engine.dispose()

    def test_merge_revision_downgrades_and_reupgrades(self, tmp_path):
        db_path = str(tmp_path / "merge-roundtrip.db")
        cfg = _alembic_cfg(db_path)

        _run_alembic(cfg, command.upgrade, PUBLISHED_BRANCH_MERGE_REVISION)
        # Relative ``-1`` is ambiguous at a mergepoint, so select either
        # published parent explicitly; Alembic retains the sibling head.
        _run_alembic(cfg, command.downgrade, PLAN_CLEANUP_REVISION)

        engine = create_engine(f"sqlite:///{db_path}")
        self._assert_revision_schema(
            engine,
            revisions={
                PLAN_CLEANUP_REVISION,
                PR_REVIEW_SNAPSHOT_REVISION,
            },
            plan_schema_present=False,
            snapshot_schema_present=True,
        )
        engine.dispose()

        _run_alembic(cfg, command.upgrade, PUBLISHED_BRANCH_MERGE_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        self._assert_revision_schema(
            engine,
            revisions={PUBLISHED_BRANCH_MERGE_REVISION},
            plan_schema_present=False,
            snapshot_schema_present=True,
        )
        engine.dispose()

    def test_reverted_plan_cleanup_downgrades_and_reupgrades(self, tmp_path):
        db_path = str(tmp_path / "plan-cleanup-roundtrip.db")
        cfg = _alembic_cfg(db_path)

        _run_alembic(cfg, command.upgrade, PUBLISHED_BRANCH_MERGE_REVISION)
        _run_alembic(cfg, command.downgrade, PUBLISHED_PLAN_REVISION)

        engine = create_engine(f"sqlite:///{db_path}")
        self._assert_revision_schema(
            engine,
            revisions={
                PUBLISHED_PLAN_REVISION,
                PR_REVIEW_SNAPSHOT_REVISION,
            },
            plan_schema_present=True,
            snapshot_schema_present=True,
        )
        engine.dispose()

        _run_alembic(cfg, command.upgrade, PUBLISHED_BRANCH_MERGE_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        self._assert_revision_schema(
            engine,
            revisions={PUBLISHED_BRANCH_MERGE_REVISION},
            plan_schema_present=False,
            snapshot_schema_present=True,
        )
        engine.dispose()


    def test_pr_review_merge_method_backfills_only_legacy_merge_outbox(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "pr-review-merge-method-backfill.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(
            cfg,
            command.upgrade,
            BROWSER_OPERATION_RECEIPT_REVISION,
        )

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO monitored_repos (
                    id, repo_full_name, webhook_secret,
                    required_checks, created_at, updated_at
                ) VALUES (
                    931, 'owner/legacy-merge-outbox', 'secret',
                    '[]', '2026-08-10 00:00:00',
                    '2026-08-10 00:00:00'
                )
            """))
            conn.execute(text("""
                INSERT INTO pr_reviews (
                    id, repo_id, pr_number, pr_title, pr_author, pr_url,
                    status, pending_action, created_at
                ) VALUES
                    (932, 931, 1, 'legacy merge', 'ccm-bot',
                     'https://github.com/owner/legacy-merge-outbox/pull/1',
                     'publishing', 'approved_merged',
                     '2026-08-10 00:00:00'),
                    (933, 931, 2, 'ready only', 'ccm-bot',
                     'https://github.com/owner/legacy-merge-outbox/pull/2',
                     'publishing', 'lgtm_comment',
                     '2026-08-10 00:00:00'),
                    (934, 931, 3, 'blocking review', 'ccm-bot',
                     'https://github.com/owner/legacy-merge-outbox/pull/3',
                     'publishing', 'review_comments',
                     '2026-08-10 00:00:00'),
                    (935, 931, 4, 'already terminal', 'ccm-bot',
                     'https://github.com/owner/legacy-merge-outbox/pull/4',
                     'merged', 'approved_merged',
                     '2026-08-10 00:00:00'),
                    (936, 931, 5, 'not yet armed', 'ccm-bot',
                     'https://github.com/owner/legacy-merge-outbox/pull/5',
                     'reviewing', 'approved_merged',
                     '2026-08-10 00:00:00')
            """))
        engine.dispose()

        _run_alembic(cfg, command.upgrade, PR_REVIEW_MERGE_METHOD_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            methods = dict(conn.execute(text("""
                SELECT id, merge_method
                FROM pr_reviews
                WHERE id BETWEEN 932 AND 936
                ORDER BY id
            """)).all())
        assert methods == {
            932: "merge",
            933: None,
            934: None,
            935: None,
            936: None,
        }
        engine.dispose()


    def test_pr_review_merge_method_migration_roundtrip(self, tmp_path):
        db_path = str(tmp_path / "pr-review-merge-method-roundtrip.db")
        cfg = _alembic_cfg(db_path)

        _run_alembic(
            cfg,
            command.upgrade,
            BROWSER_OPERATION_RECEIPT_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        assert "merge_method" not in _get_table_columns(engine, "pr_reviews")
        engine.dispose()

        _run_alembic(cfg, command.upgrade, PR_REVIEW_MERGE_METHOD_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "merge_method" in _get_table_columns(engine, "pr_reviews")
        engine.dispose()

        _run_alembic(
            cfg,
            command.downgrade,
            BROWSER_OPERATION_RECEIPT_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        assert "merge_method" not in _get_table_columns(engine, "pr_reviews")
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == BROWSER_OPERATION_RECEIPT_REVISION
        engine.dispose()

        _run_alembic(cfg, command.upgrade, PR_REVIEW_MERGE_METHOD_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "merge_method" in _get_table_columns(engine, "pr_reviews")
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == PR_REVIEW_MERGE_METHOD_REVISION
        engine.dispose()

    def test_pr_review_merge_method_downgrade_refuses_durable_evidence(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "pr-review-merge-method-refusal.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, PR_REVIEW_MERGE_METHOD_REVISION)

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO monitored_repos (
                    id, repo_full_name, webhook_secret,
                    required_checks, created_at, updated_at
                ) VALUES (
                    941, 'owner/merge-evidence', 'secret',
                    '[]', '2026-08-10 00:00:00',
                    '2026-08-10 00:00:00'
                )
            """))
            conn.execute(text("""
                INSERT INTO pr_reviews (
                    id, repo_id, pr_number, pr_title, pr_author, pr_url,
                    status, merge_method, created_at
                ) VALUES (
                    942, 941, 7, 'merged evidence', 'ccm-bot',
                    'https://github.com/owner/merge-evidence/pull/7',
                    'merged', 'squash', '2026-08-10 00:00:00'
                )
            """))
        engine.dispose()

        with pytest.raises(RuntimeError, match="durable merge evidence exists"):
            _run_alembic(
                cfg,
                command.downgrade,
                BROWSER_OPERATION_RECEIPT_REVISION,
            )

        engine = create_engine(f"sqlite:///{db_path}")
        assert "merge_method" in _get_table_columns(engine, "pr_reviews")
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT merge_method FROM pr_reviews WHERE id = 942"
            )).scalar_one() == "squash"
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == PR_REVIEW_MERGE_METHOD_REVISION
        engine.dispose()

    def test_pr_review_merge_method_upgrade_replays_partial_add_column(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "pr-review-merge-method-replay.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(
            cfg,
            command.upgrade,
            BROWSER_OPERATION_RECEIPT_REVISION,
        )

        # Simulate non-transactional DDL committing before Alembic writes the
        # new revision stamp, followed by a process crash.
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text(
                "ALTER TABLE pr_reviews ADD COLUMN "
                "merge_method VARCHAR(16) NULL"
            ))
            conn.execute(text("""
                INSERT INTO monitored_repos (
                    id, repo_full_name, webhook_secret,
                    required_checks, created_at, updated_at
                ) VALUES (
                    951, 'owner/partial-add', 'secret',
                    '[]', '2026-08-10 00:00:00',
                    '2026-08-10 00:00:00'
                )
            """))
            conn.execute(text("""
                INSERT INTO pr_reviews (
                    id, repo_id, pr_number, pr_title, pr_author, pr_url,
                    status, pending_action, created_at
                ) VALUES (
                    952, 951, 8, 'partial add', 'ccm-bot',
                    'https://github.com/owner/partial-add/pull/8',
                    'publishing', 'approved_merged',
                    '2026-08-10 00:00:00'
                )
            """))
        engine.dispose()

        _run_alembic(cfg, command.upgrade, PR_REVIEW_MERGE_METHOD_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT merge_method FROM pr_reviews WHERE id = 952"
            )).scalar_one() == "merge"
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == PR_REVIEW_MERGE_METHOD_REVISION
        engine.dispose()

    def test_pr_review_merge_method_upgrade_rejects_partial_wrong_shape(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "pr-review-merge-method-wrong-shape.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(
            cfg,
            command.upgrade,
            BROWSER_OPERATION_RECEIPT_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text(
                "ALTER TABLE pr_reviews ADD COLUMN "
                "merge_method VARCHAR(15) NULL"
            ))
        engine.dispose()

        with pytest.raises(RuntimeError, match="nullable VARCHAR\\(16\\)"):
            _run_alembic(
                cfg,
                command.upgrade,
                PR_REVIEW_MERGE_METHOD_REVISION,
            )

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == BROWSER_OPERATION_RECEIPT_REVISION
        assert _get_table_columns(engine, "pr_reviews")["merge_method"] == (
            "VARCHAR(15)"
        )
        engine.dispose()

    def test_pr_review_merge_method_mysql_add_crash_replays_once(self):
        module = _load_pr_review_merge_method_migration("mysql_add_replay")
        column_state = {"value": None}
        exact_column = {
            "name": "merge_method",
            "type": mysql.VARCHAR(length=16),
            "nullable": True,
        }

        def add_column():
            column_state["value"] = exact_column

        class SimulatedCrash(RuntimeError):
            pass

        with (
            patch.object(module, "_dialect_name", return_value="mysql"),
            patch.object(module, "_is_offline", return_value=False),
            patch.object(
                module,
                "_reflected_merge_method_column",
                side_effect=lambda: column_state["value"],
            ),
            patch.object(
                module,
                "_add_merge_method_column",
                side_effect=add_column,
            ) as add,
            patch.object(module, "_assert_existing_merge_values"),
            patch.object(
                module,
                "_backfill_legacy_merge_outboxes",
                side_effect=(SimulatedCrash("after DDL"), None),
            ) as backfill,
        ):
            with pytest.raises(SimulatedCrash, match="after DDL"):
                module.upgrade()
            module.upgrade()

        assert add.call_count == 1
        assert backfill.call_count == 2

    @pytest.mark.parametrize(
        "column",
        [
            {
                "name": "merge_method",
                "type": postgresql.VARCHAR(length=15),
                "nullable": True,
            },
            {
                "name": "merge_method",
                "type": mysql.VARCHAR(length=16),
                "nullable": False,
            },
            {
                "name": "merge_method",
                "type": Integer(),
                "nullable": True,
            },
            {
                "name": "merge_method",
                "type": postgresql.CHAR(length=16),
                "nullable": True,
            },
            {
                "name": "merge_method",
                "type": postgresql.VARCHAR(length=16),
                "nullable": True,
                "default": "'merge'::character varying",
            },
        ],
    )
    def test_pr_review_merge_method_replay_rejects_wrong_column_shape(
        self,
        column,
    ):
        module = _load_pr_review_merge_method_migration("wrong_shape")
        with pytest.raises(RuntimeError, match="nullable VARCHAR\\(16\\)"):
            module._assert_merge_method_column_shape(column)

    @pytest.mark.parametrize("dialect", ["sqlite", "mysql", "mariadb"])
    def test_pr_review_merge_method_offline_nontransactional_upgrade_refused(
        self,
        dialect,
    ):
        module = _load_pr_review_merge_method_migration(
            f"offline_{dialect}"
        )
        with (
            patch.object(module, "_dialect_name", return_value=dialect),
            patch.object(module, "_is_offline", return_value=True),
            patch.object(module, "_add_merge_method_column") as add,
            pytest.raises(RuntimeError, match="Offline PR merge-method"),
        ):
            module._ensure_merge_method_column()
        add.assert_not_called()

    def test_pr_review_merge_method_postgresql_downgrade_fences_first(self):
        module = _load_pr_review_merge_method_migration("postgresql_fence")
        events = []
        column = {
            "name": "merge_method",
            "type": postgresql.VARCHAR(length=16),
            "nullable": True,
        }

        def record_lock(statement):
            events.append(("lock", str(statement)))

        def reflect_column():
            events.append(("reflect", None))
            return column

        def count_evidence():
            events.append(("count", None))
            return 0

        def drop_column():
            events.append(("drop", None))

        with (
            patch.object(module, "_is_offline", return_value=False),
            patch.object(
                module,
                "_dialect_name",
                return_value="postgresql",
            ),
            patch.object(module.op, "execute", side_effect=record_lock),
            patch.object(
                module,
                "_reflected_merge_method_column",
                side_effect=reflect_column,
            ),
            patch.object(
                module,
                "_count_frozen_merge_evidence",
                side_effect=count_evidence,
            ),
            patch.object(
                module,
                "_drop_merge_method_column",
                side_effect=drop_column,
            ),
        ):
            module.downgrade()

        assert [name for name, _ in events] == [
            "lock",
            "reflect",
            "count",
            "drop",
        ]
        assert events[0][1] == (
            "LOCK TABLE pr_reviews IN ACCESS EXCLUSIVE MODE"
        )

    @pytest.mark.parametrize("dialect", ["mysql", "mariadb"])
    def test_pr_review_merge_method_mysql_downgrade_fails_closed(
        self,
        dialect,
    ):
        module = _load_pr_review_merge_method_migration(
            f"{dialect}_downgrade"
        )
        with (
            patch.object(module, "_is_offline", return_value=False),
            patch.object(module, "_dialect_name", return_value=dialect),
            patch.object(
                module,
                "_reflected_merge_method_column",
            ) as reflect,
            pytest.raises(RuntimeError, match="refused for MySQL/MariaDB"),
        ):
            module.downgrade()
        reflect.assert_not_called()

    def test_pr_review_merge_method_sqlite_downgrade_replays_partial_drop(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "pr-review-merge-method-drop-replay.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, PR_REVIEW_MERGE_METHOD_REVISION)

        # Simulate batch DDL completing before Alembic can stamp the parent.
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text(
                "ALTER TABLE pr_reviews DROP COLUMN merge_method"
            ))
        engine.dispose()

        _run_alembic(
            cfg,
            command.downgrade,
            BROWSER_OPERATION_RECEIPT_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        assert "merge_method" not in _get_table_columns(
            engine,
            "pr_reviews",
        )
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == BROWSER_OPERATION_RECEIPT_REVISION
        engine.dispose()


    def test_pr_review_base_ref_backfills_frozen_monitor_branch(self, tmp_path):
        db_path = str(tmp_path / "pr-review-base-ref-backfill.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, PR_REVIEW_MERGE_METHOD_REVISION)

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO monitored_repos (
                    id, repo_full_name, webhook_secret, default_branch,
                    required_checks, created_at, updated_at
                ) VALUES (
                    961, 'owner/base-ref-backfill', 'secret', 'release/2026',
                    '[]', '2026-08-10 00:00:00', '2026-08-10 00:00:00'
                )
            """))
            conn.execute(text("""
                INSERT INTO pr_reviews (
                    id, repo_id, pr_number, pr_title, pr_author, pr_url,
                    status, created_at
                ) VALUES
                    (962, 961, 1, 'open review', 'alice',
                     'https://github.com/owner/base-ref-backfill/pull/1',
                     'reviewing', '2026-08-10 00:00:00'),
                    (963, 961, 2, 'terminal review', 'alice',
                     'https://github.com/owner/base-ref-backfill/pull/2',
                     'approved', '2026-08-10 00:00:00')
            """))
        engine.dispose()

        _run_alembic(cfg, command.upgrade, PR_REVIEW_BASE_REF_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        base_ref_column = inspect(engine).get_columns("pr_reviews")
        base_ref_column = next(
            item for item in base_ref_column if item["name"] == "base_ref"
        )
        assert base_ref_column["nullable"] is False
        subject_constraints = {
            item["name"]: tuple(item["column_names"])
            for item in inspect(engine).get_unique_constraints("pr_reviews")
        }
        assert "uq_pr_reviews_repo_pr_base_head" not in subject_constraints
        assert subject_constraints[
            "uq_pr_reviews_repo_pr_base_ref_base_head"
        ] == (
            "repo_id", "pr_number", "base_ref", "base_sha", "head_sha",
        )
        queue_constraints = {
            item["name"]: tuple(item["column_names"])
            for item in inspect(engine).get_unique_constraints(
                "pr_merge_queue_actions"
            )
        }
        assert "uq_pr_merge_queue_actions_run_head" not in queue_constraints
        assert queue_constraints[
            "uq_pr_merge_queue_actions_run_review"
        ] == ("monitor_run_id", "review_id")
        with engine.connect() as conn:
            assert dict(conn.execute(text(
                "SELECT id, base_ref FROM pr_reviews ORDER BY id"
            )).all()) == {962: "release/2026", 963: "release/2026"}
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == PR_REVIEW_BASE_REF_REVISION
        engine.dispose()

        with pytest.raises(RuntimeError, match="review subjects still exist"):
            _run_alembic(
                cfg,
                command.downgrade,
                PR_REVIEW_MERGE_METHOD_REVISION,
            )


    def test_pr_review_base_ref_migration_empty_roundtrip(self, tmp_path):
        db_path = str(tmp_path / "pr-review-base-ref-roundtrip.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, PR_REVIEW_MERGE_METHOD_REVISION)

        _run_alembic(cfg, command.upgrade, PR_REVIEW_BASE_REF_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "base_ref" in _get_table_columns(engine, "pr_reviews")
        engine.dispose()

        _run_alembic(
            cfg,
            command.downgrade,
            PR_REVIEW_MERGE_METHOD_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        assert "base_ref" not in _get_table_columns(engine, "pr_reviews")
        engine.dispose()

        _run_alembic(cfg, command.upgrade, PR_REVIEW_BASE_REF_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "base_ref" in _get_table_columns(engine, "pr_reviews")
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == PR_REVIEW_BASE_REF_REVISION
        engine.dispose()


    def test_pr_review_base_ref_is_part_of_unique_subject(self, tmp_path):
        db_path = str(tmp_path / "pr-review-base-ref-unique.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, PR_REVIEW_BASE_REF_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO monitored_repos (
                    id, repo_full_name, webhook_secret, default_branch,
                    required_checks, created_at, updated_at
                ) VALUES (
                    964, 'owner/base-ref-unique', 'secret', 'main', '[]',
                    '2026-08-10 00:00:00', '2026-08-10 00:00:00'
                )
            """))
            for review_id, base_ref in ((965, "main"), (966, "release")):
                conn.execute(text("""
                    INSERT INTO pr_reviews (
                        id, repo_id, pr_number, base_ref, base_sha, head_sha,
                        pr_title, pr_author, pr_url, status, created_at
                    ) VALUES (
                        :id, 964, 7, :base_ref, :base_sha, :head_sha,
                        'same commits', 'alice',
                        'https://github.com/owner/base-ref-unique/pull/7',
                        'approved', '2026-08-10 00:00:00'
                    )
                """), {
                    "id": review_id,
                    "base_ref": base_ref,
                    "base_sha": "a" * 40,
                    "head_sha": "b" * 40,
                })
        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO pr_reviews (
                        id, repo_id, pr_number, base_ref, base_sha, head_sha,
                        pr_title, pr_author, pr_url, status, created_at
                    ) VALUES (
                        967, 964, 7, 'main', :base_sha, :head_sha,
                        'duplicate subject', 'alice',
                        'https://github.com/owner/base-ref-unique/pull/7',
                        'approved', '2026-08-10 00:00:00'
                    )
                """), {"base_sha": "a" * 40, "head_sha": "b" * 40})
        engine.dispose()


    def test_pr_review_base_ref_upgrade_replays_partial_add_column(self, tmp_path):
        db_path = str(tmp_path / "pr-review-base-ref-replay.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, PR_REVIEW_MERGE_METHOD_REVISION)

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text(
                "ALTER TABLE pr_reviews ADD COLUMN base_ref VARCHAR(200) NULL"
            ))
            conn.execute(text("""
                INSERT INTO monitored_repos (
                    id, repo_full_name, webhook_secret, default_branch,
                    required_checks, created_at, updated_at
                ) VALUES (
                    971, 'owner/base-ref-replay', 'secret', 'stable', '[]',
                    '2026-08-10 00:00:00', '2026-08-10 00:00:00'
                )
            """))
            conn.execute(text("""
                INSERT INTO pr_reviews (
                    id, repo_id, pr_number, pr_title, pr_author, pr_url,
                    status, created_at
                ) VALUES (
                    972, 971, 1, 'partial add', 'alice',
                    'https://github.com/owner/base-ref-replay/pull/1',
                    'reviewing', '2026-08-10 00:00:00'
                )
            """))
        engine.dispose()

        _run_alembic(cfg, command.upgrade, PR_REVIEW_BASE_REF_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT base_ref FROM pr_reviews WHERE id = 972"
            )).scalar_one() == "stable"
        engine.dispose()


    def test_pr_review_base_ref_upgrade_rejects_invalid_backfill(self, tmp_path):
        db_path = str(tmp_path / "pr-review-base-ref-invalid.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, PR_REVIEW_MERGE_METHOD_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO monitored_repos (
                    id, repo_full_name, webhook_secret, default_branch,
                    required_checks, created_at, updated_at
                ) VALUES (
                    981, 'owner/base-ref-invalid', 'secret', '', '[]',
                    '2026-08-10 00:00:00', '2026-08-10 00:00:00'
                )
            """))
            conn.execute(text("""
                INSERT INTO pr_reviews (
                    id, repo_id, pr_number, pr_title, pr_author, pr_url,
                    status, created_at
                ) VALUES (
                    982, 981, 1, 'invalid base', 'alice',
                    'https://github.com/owner/base-ref-invalid/pull/1',
                    'reviewing', '2026-08-10 00:00:00'
                )
            """))
        engine.dispose()

        with pytest.raises(RuntimeError, match="valid target branch"):
            _run_alembic(cfg, command.upgrade, PR_REVIEW_BASE_REF_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "base_ref" in _get_table_columns(engine, "pr_reviews")
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == PR_REVIEW_MERGE_METHOD_REVISION
        engine.dispose()


    def test_pr_review_base_ref_mysql_add_crash_replays_once(self):
        module = _load_pr_review_base_ref_migration("mysql_add_replay")
        column_state = {"value": None}
        exact_column = {
            "name": "base_ref",
            "type": mysql.VARCHAR(length=200),
            "nullable": True,
        }

        def add_column():
            column_state["value"] = exact_column

        class SimulatedCrash(RuntimeError):
            pass

        with (
            patch.object(module, "_dialect_name", return_value="mysql"),
            patch.object(module, "_is_offline", return_value=False),
            patch.object(
                module,
                "_reflected_base_ref_column",
                side_effect=lambda: column_state["value"],
            ),
            patch.object(
                module,
                "_add_base_ref_column",
                side_effect=add_column,
            ) as add,
            patch.object(
                module,
                "_backfill_existing_reviews",
                side_effect=(SimulatedCrash("after DDL"), None),
            ) as backfill,
            patch.object(module, "_assert_backfilled_values"),
            patch.object(module, "_make_base_ref_non_nullable"),
            patch.object(module, "_ensure_frozen_subject_constraint"),
            patch.object(module, "_ensure_frozen_queue_constraint"),
        ):
            with pytest.raises(SimulatedCrash, match="after DDL"):
                module.upgrade()
            module.upgrade()

        assert add.call_count == 1
        assert backfill.call_count == 2


    @pytest.mark.parametrize(
        ("constraint_kind", "crash_phase"),
        [
            ("subject", "create"),
            ("subject", "drop"),
            ("queue", "create"),
            ("queue", "drop"),
        ],
    )
    def test_pr_review_base_ref_constraint_crash_replays_once(
        self,
        constraint_kind,
        crash_phase,
    ):
        module = _load_pr_review_base_ref_migration(
            f"{constraint_kind}_{crash_phase}_replay"
        )
        state = {"old": True, "new": False}
        crashed = {"value": False}

        class SimulatedCrash(RuntimeError):
            pass

        def reflect():
            return state["old"], state["new"]

        def mutate(phase, *, old, new):
            state.update(old=old, new=new)
            if phase == crash_phase and not crashed["value"]:
                crashed["value"] = True
                raise SimulatedCrash(f"after {phase} DDL")

        if constraint_kind == "subject":
            ensure_name = "_ensure_frozen_subject_constraint"
            reflect_name = "_reflected_subject_constraints"
            create_name = "_create_subject_constraint"
            drop_name = "_drop_subject_constraint"
        else:
            ensure_name = "_ensure_frozen_queue_constraint"
            reflect_name = "_reflected_queue_constraints"
            create_name = "_create_queue_constraint"
            drop_name = "_drop_queue_constraint"

        with (
            patch.object(module, "_is_offline", return_value=False),
            patch.object(module, reflect_name, side_effect=reflect),
            patch.object(
                module,
                create_name,
                side_effect=lambda *_: mutate("create", old=True, new=True),
            ) as create,
            patch.object(
                module,
                drop_name,
                side_effect=lambda *_: mutate("drop", old=False, new=True),
            ) as drop,
        ):
            with pytest.raises(SimulatedCrash, match=f"after {crash_phase}"):
                getattr(module, ensure_name)()
            getattr(module, ensure_name)()

        assert state == {"old": False, "new": True}
        assert create.call_count == 1
        assert drop.call_count == 1


    @pytest.mark.parametrize(
        "column",
        [
            {
                "name": "base_ref",
                "type": postgresql.VARCHAR(length=199),
                "nullable": True,
            },
            {
                "name": "base_ref",
                "type": Integer(),
                "nullable": True,
            },
            {
                "name": "base_ref",
                "type": postgresql.VARCHAR(length=200),
                "nullable": "yes",
            },
            {
                "name": "base_ref",
                "type": postgresql.VARCHAR(length=200),
                "nullable": True,
                "default": "'main'::character varying",
            },
        ],
    )
    def test_pr_review_base_ref_replay_rejects_wrong_column_shape(
        self,
        column,
    ):
        module = _load_pr_review_base_ref_migration("wrong_shape")
        with pytest.raises(RuntimeError, match="VARCHAR\\(200\\)"):
            module._assert_base_ref_column_shape(column)


    @pytest.mark.parametrize("dialect", ["sqlite", "mysql", "mariadb"])
    def test_pr_review_base_ref_offline_nontransactional_upgrade_refused(
        self,
        dialect,
    ):
        module = _load_pr_review_base_ref_migration(f"offline_{dialect}")
        with (
            patch.object(module, "_dialect_name", return_value=dialect),
            patch.object(module, "_is_offline", return_value=True),
            patch.object(module, "_add_base_ref_column") as add,
            pytest.raises(RuntimeError, match="Offline PR base-ref"),
        ):
            module._ensure_base_ref_column()
        add.assert_not_called()


    def test_pr_review_base_ref_postgresql_offline_upgrade_is_transactional(
        self,
    ):
        module = _load_pr_review_base_ref_migration("postgresql_offline")
        output = io.StringIO()
        migration_context = MigrationContext.configure(
            dialect_name="postgresql",
            opts={"as_sql": True, "output_buffer": output},
        )
        with (
            patch.object(module, "op", Operations(migration_context)),
            patch.object(module, "_is_offline", return_value=True),
        ):
            module.upgrade()

        ddl = output.getvalue().lower()
        add_column = ddl.index(
            "alter table pr_reviews add column base_ref varchar(200)"
        )
        backfill = ddl.index("update pr_reviews")
        assertion = ddl.index("do $ccm$")
        non_null = ddl.index(
            "alter table pr_reviews alter column base_ref set not null"
        )
        new_subject = ddl.index(
            "add constraint uq_pr_reviews_repo_pr_base_ref_base_head"
        )
        old_subject = ddl.index(
            "drop constraint uq_pr_reviews_repo_pr_base_head"
        )
        new_queue = ddl.index(
            "add constraint uq_pr_merge_queue_actions_run_review"
        )
        old_queue = ddl.index(
            "drop constraint uq_pr_merge_queue_actions_run_head"
        )
        assert (
            add_column
            < backfill
            < assertion
            < non_null
            < new_subject
            < old_subject
            < new_queue
            < old_queue
        )
        assert "where base_ref is null" in ddl
        assert "raise exception" in ddl


    def test_pr_review_base_ref_postgresql_offline_downgrade_is_refused(
        self,
    ):
        module = _load_pr_review_base_ref_migration(
            "postgresql_offline_downgrade"
        )
        output = io.StringIO()
        migration_context = MigrationContext.configure(
            dialect_name="postgresql",
            opts={"as_sql": True, "output_buffer": output},
        )
        with (
            patch.object(module, "op", Operations(migration_context)),
            patch.object(module, "_is_offline", return_value=True),
            pytest.raises(RuntimeError, match="Offline downgrade is refused"),
        ):
            module.downgrade()
        assert output.getvalue() == ""


    def test_pr_review_base_ref_postgresql_downgrade_fences_first(self):
        module = _load_pr_review_base_ref_migration("postgresql_fence")
        events = []
        column = {
            "name": "base_ref",
            "type": postgresql.VARCHAR(length=200),
            "nullable": False,
        }

        with (
            patch.object(module, "_is_offline", return_value=False),
            patch.object(module, "_dialect_name", return_value="postgresql"),
            patch.object(
                module.op,
                "execute",
                side_effect=lambda statement: events.append(("lock", str(statement))),
            ),
            patch.object(
                module,
                "_reflected_base_ref_column",
                side_effect=lambda: (events.append(("reflect", None)), column)[1],
            ),
            patch.object(
                module,
                "_count_reviews",
                side_effect=lambda: (events.append(("count", None)), 0)[1],
            ),
            patch.object(
                module,
                "_ensure_legacy_subject_constraint",
                side_effect=lambda: events.append(("constraint", None)),
            ),
            patch.object(
                module,
                "_ensure_legacy_queue_constraint",
                side_effect=lambda: events.append(("queue", None)),
            ),
            patch.object(
                module,
                "_drop_base_ref_column",
                side_effect=lambda: events.append(("drop", None)),
            ),
        ):
            module.downgrade()

        assert [name for name, _ in events] == [
            "lock", "reflect", "count", "queue", "constraint", "drop",
        ]


    @pytest.mark.parametrize("dialect", ["mysql", "mariadb"])
    def test_pr_review_base_ref_mysql_downgrade_fails_closed(
        self,
        dialect,
    ):
        module = _load_pr_review_base_ref_migration(f"{dialect}_downgrade")
        with (
            patch.object(module, "_is_offline", return_value=False),
            patch.object(module, "_dialect_name", return_value=dialect),
            patch.object(module, "_reflected_base_ref_column") as reflect,
            pytest.raises(RuntimeError, match="refused for MySQL/MariaDB"),
        ):
            module.downgrade()
        reflect.assert_not_called()


    def test_pr_review_base_ref_sqlite_downgrade_replays_partial_drop(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "pr-review-base-ref-drop-replay.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, PR_REVIEW_BASE_REF_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        module = _load_pr_review_base_ref_migration("sqlite_drop_replay")
        with engine.begin() as conn:
            migration_context = MigrationContext.configure(conn)
            with (
                patch.object(module, "op", Operations(migration_context)),
                patch.object(module, "_is_offline", return_value=False),
            ):
                module.downgrade()
        engine.dispose()

        _run_alembic(
            cfg,
            command.downgrade,
            PR_REVIEW_MERGE_METHOD_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        assert "base_ref" not in _get_table_columns(engine, "pr_reviews")
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == PR_REVIEW_MERGE_METHOD_REVISION
        engine.dispose()


    @pytest.mark.parametrize(
        ("subject_state", "queue_state"),
        [
            ((False, False), (True, False)),
            ((True, True), (True, False)),
            ((True, False), (False, False)),
            ((True, False), (True, True)),
        ],
    )
    def test_pr_review_base_ref_sqlite_missing_column_replay_requires_exact_constraints(
        self,
        subject_state,
        queue_state,
    ):
        module = _load_pr_review_base_ref_migration(
            "sqlite_missing_column_wrong_constraints"
        )
        with (
            patch.object(module, "_is_offline", return_value=False),
            patch.object(
                module,
                "_acquire_downgrade_writer_fence",
                return_value="sqlite",
            ),
            patch.object(
                module,
                "_reflected_base_ref_column",
                return_value=None,
            ),
            patch.object(
                module,
                "_reflected_subject_constraints",
                return_value=subject_state,
            ),
            patch.object(
                module,
                "_reflected_queue_constraints",
                return_value=queue_state,
            ),
            patch.object(module, "_count_reviews") as count,
            patch.object(module, "_drop_base_ref_column") as drop,
            pytest.raises(RuntimeError, match="exact legacy constraints"),
        ):
            module.downgrade()
        count.assert_not_called()
        drop.assert_not_called()


    def test_pr_review_base_ref_sqlite_missing_column_replay_rejects_new_reviews(
        self,
    ):
        module = _load_pr_review_base_ref_migration(
            "sqlite_missing_column_with_reviews"
        )
        with (
            patch.object(
                module,
                "_acquire_downgrade_writer_fence",
                return_value="sqlite",
            ),
            patch.object(
                module,
                "_reflected_base_ref_column",
                return_value=None,
            ),
            patch.object(
                module,
                "_reflected_subject_constraints",
                return_value=(True, False),
            ),
            patch.object(
                module,
                "_reflected_queue_constraints",
                return_value=(True, False),
            ),
            patch.object(module, "_count_reviews", return_value=1),
            patch.object(module, "_drop_base_ref_column") as drop,
            pytest.raises(RuntimeError, match="review subjects still exist"),
        ):
            module.downgrade()
        drop.assert_not_called()


    def test_discussion_provider_lease_migration_roundtrip(self, tmp_path):
        db_path = str(tmp_path / "discussion-provider-lease.db")
        cfg = _alembic_cfg(db_path)

        _run_alembic(cfg, command.upgrade, TASK_SSH_EFFECT_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        before = inspect(engine)
        assert "ix_discussions_project_status_id" not in {
            index["name"] for index in before.get_indexes("discussions")
        }
        assert "ck_discussions_status" not in {
            constraint["name"]
            for constraint in before.get_check_constraints("discussions")
        }
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO discussions (
                    id, title, project_id, max_agents,
                    facilitator_model, agent_model, status, created_at
                ) VALUES
                    (901, 'active lease', 71, 3, 'facilitator', 'agent',
                     'active', '2026-08-09 00:00:00'),
                    (902, 'closing lease', 71, 3, 'facilitator', 'agent',
                     'closing', '2026-08-09 00:00:01'),
                    (903, 'closed lease', 71, 3, 'facilitator', 'agent',
                     'closed', '2026-08-09 00:00:02')
            """))
        engine.dispose()

        _run_alembic(cfg, command.upgrade, DISCUSSION_LEASE_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        inspector = inspect(engine)
        assert "ix_discussions_project_status_id" in {
            index["name"] for index in inspector.get_indexes("discussions")
        }
        assert "ck_discussions_status" in {
            constraint["name"]
            for constraint in inspector.get_check_constraints("discussions")
        }
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT status FROM discussions ORDER BY id"
            )).scalars().all() == ["active", "closing", "closed"]
        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO discussions (
                        title, max_agents, facilitator_model,
                        agent_model, status, created_at
                    ) VALUES (
                        'invalid lease', 3, 'facilitator',
                        'agent', 'paused', '2026-08-09 00:00:03'
                    )
                """))
        engine.dispose()

        _run_alembic(cfg, command.downgrade, TASK_SSH_EFFECT_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        inspector = inspect(engine)
        assert "ix_discussions_project_status_id" not in {
            index["name"] for index in inspector.get_indexes("discussions")
        }
        assert "ck_discussions_status" not in {
            constraint["name"]
            for constraint in inspector.get_check_constraints("discussions")
        }
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT status FROM discussions ORDER BY id"
            )).scalars().all() == ["active", "closing", "closed"]
        engine.dispose()

        _run_alembic(cfg, command.upgrade, DISCUSSION_LEASE_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == DISCUSSION_LEASE_REVISION
        engine.dispose()

    def test_discussion_provider_lease_migration_refuses_unknown_status(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "discussion-provider-lease-unknown.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, TASK_SSH_EFFECT_REVISION)

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO discussions (
                    id, title, max_agents, facilitator_model,
                    agent_model, status, created_at
                ) VALUES (
                    904, 'unknown lease', 3, 'facilitator',
                    'agent', 'paused', '2026-08-09 00:00:00'
                )
            """))
        engine.dispose()

        with pytest.raises(RuntimeError, match="unsupported historical status"):
            _run_alembic(cfg, command.upgrade, DISCUSSION_LEASE_REVISION)

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT status FROM discussions WHERE id = 904"
            )).scalar_one() == "paused"
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == TASK_SSH_EFFECT_REVISION
        engine.dispose()


def test_task_execution_principal_upgrade_downgrade_upgrade(tmp_path):
    db_path = str(tmp_path / "task-execution-principal.db")
    cfg = _alembic_cfg(db_path)
    _run_alembic(
        cfg,
        command.upgrade,
        AGENT_SANDBOX_RUNTIME_OVERRIDE_REVISION,
    )
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        trigger_sql = dict(conn.execute(text(
            "SELECT name, sql FROM sqlite_master "
            "WHERE type = 'trigger' ORDER BY name"
        )).tuples().all())
    assert {
        "trg_task_ssh_effect_task_update",
        "trg_task_ssh_effect_task_delete",
        "trg_task_ssh_effect_project_share_insert",
        "trg_task_ssh_effect_project_share_update",
        "trg_task_ssh_effect_project_share_delete",
        "trg_task_ssh_effect_team_project_share_insert",
        "trg_task_ssh_effect_team_project_share_update",
        "trg_task_ssh_effect_team_project_share_delete",
    } <= set(trigger_sql)
    engine.dispose()

    _run_alembic(cfg, command.upgrade, TASK_EXECUTION_PRINCIPAL_REVISION)
    engine = create_engine(f"sqlite:///{db_path}")
    task_columns = _get_table_columns(engine, "tasks")
    resume_columns = _get_table_columns(engine, "capability_resume_outbox")
    assert {
        "execution_user_id",
        "execution_user_role",
        "execution_mode",
        "execution_principal_kind",
    } <= set(task_columns)
    assert {
        "request_execution_user_id",
        "request_execution_user_role",
        "request_execution_mode",
        "request_execution_principal_kind",
    } <= set(resume_columns)
    assert task_columns["execution_principal_kind"] == "VARCHAR(32)"
    assert (
        resume_columns["request_execution_principal_kind"] == "VARCHAR(32)"
    )
    with engine.connect() as conn:
        conn.execute(text(
            "INSERT INTO tasks "
            "(title, description, status, priority, target_branch, "
            "merge_status, retry_count, max_retries, mode, created_at, "
            "execution_user_id, "
            "execution_user_role, execution_mode, "
            "execution_principal_kind) VALUES "
            "('delegated admin', 'run', 'pending', 0, 'main', 'pending', "
            "0, 2, 'auto', '2026-08-14 00:00:00', "
            "41, 'admin', 'unrestricted', "
            "'delegated_user')"
        ))
        conn.execute(text(
            "INSERT INTO tasks "
            "(title, description, status, priority, target_branch, "
            "merge_status, retry_count, max_retries, mode, created_at, "
            "execution_user_id, "
            "execution_user_role, execution_mode, "
            "execution_principal_kind) VALUES "
            "('delegated member', 'run', 'pending', 0, 'main', 'pending', "
            "0, 2, 'auto', '2026-08-14 00:00:00', "
            "42, 'member', 'sandbox', "
            "'delegated_user')"
        ))
        conn.execute(text(
            "INSERT INTO tasks "
            "(title, description, status, priority, target_branch, "
            "merge_status, retry_count, max_retries, mode, created_at, "
            "execution_user_id, "
            "execution_user_role, execution_mode, "
            "execution_principal_kind) VALUES "
            "('delegated token', 'run', 'pending', 0, 'main', 'pending', "
            "0, 2, 'auto', '2026-08-14 00:00:00', "
            "NULL, 'super_admin', "
            "'unrestricted', 'delegated_deployment_token')"
        ))
        assert dict(conn.execute(text(
            "SELECT name, sql FROM sqlite_master "
            "WHERE type = 'trigger' ORDER BY name"
        )).tuples().all()) == trigger_sql
    engine.dispose()

    _run_alembic(
        cfg,
        command.downgrade,
        AGENT_SANDBOX_RUNTIME_OVERRIDE_REVISION,
    )
    engine = create_engine(f"sqlite:///{db_path}")
    assert "execution_mode" not in _get_table_columns(engine, "tasks")
    assert "request_execution_mode" not in _get_table_columns(
        engine,
        "capability_resume_outbox",
    )
    with engine.connect() as conn:
        assert dict(conn.execute(text(
            "SELECT name, sql FROM sqlite_master "
            "WHERE type = 'trigger' ORDER BY name"
        )).tuples().all()) == trigger_sql
    engine.dispose()

    _run_alembic(cfg, command.upgrade, TASK_EXECUTION_PRINCIPAL_REVISION)
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        assert conn.execute(text(
            "SELECT version_num FROM alembic_version"
        )).scalar_one() == TASK_EXECUTION_PRINCIPAL_REVISION
    engine.dispose()


def test_task_execution_principal_downgrade_rejects_committed_user_evidence(
    tmp_path,
):
    db_path = str(tmp_path / "task-execution-principal-downgrade-gate.db")
    cfg = _alembic_cfg(db_path)
    _run_alembic(cfg, command.upgrade, TASK_EXECUTION_PRINCIPAL_REVISION)

    engine = create_engine(f"sqlite:///{db_path}")
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO tasks "
            "(title, description, status, priority, target_branch, "
            "merge_status, retry_count, max_retries, mode, created_at, "
            "execution_user_id, execution_user_role, execution_mode, "
            "execution_principal_kind) VALUES "
            "('durable admin authority', 'must survive', 'pending', 0, "
            "'main', 'pending', 0, 2, 'auto', "
            "'2026-08-14 00:00:00', 41, 'admin', 'unrestricted', 'user')"
        ))
    engine.dispose()

    with pytest.raises(
        RuntimeError,
        match="Task execution principal evidence exists",
    ):
        _run_alembic(
            cfg,
            command.downgrade,
            REMOVE_AGENT_SANDBOX_OVERRIDE_REVISION,
        )

    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        assert conn.execute(text(
            "SELECT version_num FROM alembic_version"
        )).scalar_one() == TASK_EXECUTION_PRINCIPAL_REVISION
        assert conn.execute(text(
            "SELECT execution_mode FROM tasks "
            "WHERE title = 'durable admin authority'"
        )).scalar_one() == "unrestricted"
    assert "execution_mode" in _get_table_columns(engine, "tasks")
    engine.dispose()


def test_task_execution_principal_downgrade_rejects_committed_resume_evidence(
    tmp_path,
):
    db_path = str(tmp_path / "task-principal-resume-downgrade-gate.db")
    cfg = _alembic_cfg(db_path)
    _run_alembic(cfg, command.upgrade, TASK_EXECUTION_PRINCIPAL_REVISION)
    digest = "d" * 64
    incarnation = "e" * 32

    engine = create_engine(f"sqlite:///{db_path}")
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO tasks "
            "(title, description, status, priority, target_branch, "
            "merge_status, retry_count, max_retries, mode, turn_generation, "
            "incarnation_id, created_at) VALUES "
            "('resume principal evidence', 'd', 'waiting_capability', 0, "
            "'main', 'pending', 0, 2, 'auto', 3, :incarnation, "
            "'2026-08-14 00:00:00')"
        ), {"incarnation": incarnation})
        task_id = conn.execute(text(
            "SELECT id FROM tasks WHERE title = 'resume principal evidence'"
        )).scalar_one()
        conn.execute(text(
            "INSERT INTO capability_invocations "
            "(task_id, capability_key, source, purpose, status, state_version, "
            "idempotency_key, input_payload, input_hash, subject_kind, "
            "subject_ref, subject_hash, executor_kind, executor_config, "
            "executor_config_hash, policy_snapshot, policy_hash, "
            "resume_policy, max_attempts, active_task_id, created_at, "
            "updated_at) VALUES "
            "(:task_id, 'plan', 'human_request', 'advisory', 'failed', 1, "
            "'principal-resume-downgrade', '{}', :digest, "
            "'task_generation', '{}', :digest, 'plan_agent', '{}', :digest, "
            "'{}', :digest, 'attach_only', 1, NULL, "
            "'2026-08-14 00:00:01', '2026-08-14 00:00:01')"
        ), {"task_id": task_id, "digest": digest})
        invocation_id = conn.execute(text(
            "SELECT id FROM capability_invocations "
            "WHERE idempotency_key = 'principal-resume-downgrade'"
        )).scalar_one()
        conn.execute(text(
            "INSERT INTO capability_resume_outbox "
            "(task_id, invocation_id, active_task_id, active_invocation_id, "
            "status, state_version, request_task_incarnation_id, "
            "request_task_retry_count, from_turn_generation, "
            "request_source_log_id, request_output_log_id, "
            "request_terminal_log_id, attempt_count, "
            "request_execution_user_id, request_execution_user_role, "
            "request_execution_mode, request_execution_principal_kind, "
            "created_at, updated_at) VALUES "
            "(:task_id, :invocation_id, :task_id, :invocation_id, 'pending', "
            "1, :incarnation, 0, 3, 11, 12, 13, 0, 55, 'admin', "
            "'unrestricted', 'user', '2026-08-14 00:00:02', "
            "'2026-08-14 00:00:02')"
        ), {
            "task_id": task_id,
            "invocation_id": invocation_id,
            "incarnation": incarnation,
        })
    engine.dispose()

    with pytest.raises(
        RuntimeError,
        match="Capability resume principal evidence exists",
    ):
        _run_alembic(
            cfg,
            command.downgrade,
            REMOVE_AGENT_SANDBOX_OVERRIDE_REVISION,
        )

    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        assert conn.execute(text(
            "SELECT version_num FROM alembic_version"
        )).scalar_one() == TASK_EXECUTION_PRINCIPAL_REVISION
        assert conn.execute(text(
            "SELECT request_execution_mode FROM capability_resume_outbox"
        )).scalar_one() == "unrestricted"
    engine.dispose()


def test_remove_team_share_ssh_fences_upgrade_downgrade_upgrade(tmp_path):
    db_path = str(tmp_path / "remove-team-share-ssh-fences.db")
    cfg = _alembic_cfg(db_path)
    team_triggers = {
        "trg_task_ssh_effect_team_task_share_insert",
        "trg_task_ssh_effect_team_task_share_update",
        "trg_task_ssh_effect_team_task_share_delete",
        "trg_task_ssh_effect_team_project_share_insert",
        "trg_task_ssh_effect_team_project_share_update",
        "trg_task_ssh_effect_team_project_share_delete",
    }
    federation_triggers = {
        "trg_task_ssh_effect_task_share_insert",
        "trg_task_ssh_effect_task_share_update",
        "trg_task_ssh_effect_task_share_delete",
        "trg_task_ssh_effect_project_share_insert",
        "trg_task_ssh_effect_project_share_update",
        "trg_task_ssh_effect_project_share_delete",
    }

    def trigger_names() -> set[str]:
        engine = create_engine(f"sqlite:///{db_path}")
        try:
            with engine.connect() as conn:
                return set(conn.execute(text(
                    "SELECT name FROM sqlite_master "
                    "WHERE type = 'trigger' "
                    "AND name LIKE 'trg_task_ssh_effect_%'"
                )).scalars())
        finally:
            engine.dispose()

    _run_alembic(cfg, command.upgrade, TASK_EXECUTION_PRINCIPAL_REVISION)
    assert team_triggers | federation_triggers <= trigger_names()

    _run_alembic(
        cfg,
        command.upgrade,
        REMOVE_TEAM_SHARE_SSH_FENCES_REVISION,
    )
    upgraded = trigger_names()
    assert upgraded.isdisjoint(team_triggers)
    assert federation_triggers <= upgraded

    _run_alembic(cfg, command.downgrade, TASK_EXECUTION_PRINCIPAL_REVISION)
    downgraded = trigger_names()
    assert team_triggers | federation_triggers <= downgraded

    _run_alembic(
        cfg,
        command.upgrade,
        REMOVE_TEAM_SHARE_SSH_FENCES_REVISION,
    )
    assert trigger_names().isdisjoint(team_triggers)
    engine = create_engine(f"sqlite:///{db_path}")
    try:
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == REMOVE_TEAM_SHARE_SSH_FENCES_REVISION
    finally:
        engine.dispose()


_PR_RESULT_REVIEW_COLUMNS = (
    "attempt",
    "rerun_of_review_id",
    "rerun_idempotency_key",
    "code_verdict",
    "code_verdict_task_id",
    "code_verdict_retry_count",
    "code_verdict_task_started_at",
    "code_verdict_recorded_at",
    "publication_state",
    "publication_error",
    "failure_stage",
    "error_category",
    "error_measured",
    "error_limit",
    "error_unit",
    "published_actor",
    "published_at",
    "github_review_id",
    "github_review_url",
    "github_review_state",
)
_PR_RESULT_RUN_COLUMNS = (
    "terminal_intent_status",
    "terminal_intent_base_ref",
    "terminal_intent_head_sha",
    "terminal_intent_delivery_id",
    "terminal_intent_observed_at",
    "terminal_intent_checked_at",
    "legacy_terminal_recovery_pending",
)
_PR_RESULT_DOWNGRADE_CRASH_TARGETS = (
    ("create_unique_constraint", "uq_pr_reviews_repo_pr_base_ref_base_head"),
    ("drop_constraint", "uq_pr_reviews_repo_pr_base_ref_base_head_attempt"),
    ("drop_constraint", "fk_pr_reviews_rerun_of_review_id_pr_reviews"),
    ("drop_index", "ix_pr_reviews_rerun_of_review_id"),
    ("drop_constraint", "uq_pr_reviews_rerun_idempotency"),
    ("drop_constraint", "ck_pr_reviews_input_error_evidence"),
    ("drop_constraint", "ck_pr_reviews_rerun_shape"),
    ("drop_constraint", "ck_pr_reviews_attempt"),
    *[("drop_column", name) for name in reversed(_PR_RESULT_REVIEW_COLUMNS)],
    *[("drop_column", name) for name in reversed(_PR_RESULT_RUN_COLUMNS)],
)


@pytest.fixture(scope="module")
def _pr_result_head_sqlite_db(tmp_path_factory):
    """Build one immutable head database copied by every crash-point test."""

    directory = tmp_path_factory.mktemp("pr-result-head")
    db_path = str(directory / "head.db")
    cfg = _alembic_cfg(db_path)
    _run_alembic(cfg, command.upgrade, PR_REVIEW_RESULT_EVIDENCE_REVISION)
    engine = create_engine(f"sqlite:///{db_path}")
    try:
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == PR_REVIEW_RESULT_EVIDENCE_REVISION
    finally:
        engine.dispose()
    return db_path


def _seed_pr_result_subject(conn, *, suffix: int = 1) -> tuple[int, int, int]:
    task_id = 8700 + suffix
    repo_id = 8800 + suffix
    review_id = 8900 + suffix
    run_id = 9000 + suffix
    base_sha = f"{suffix % 10}" * 40
    head_sha = f"{(suffix + 1) % 10}" * 40
    conn.execute(text("""
        INSERT INTO tasks (
            id, title, description, status, priority, target_branch,
            merge_status, retry_count, max_retries, mode, created_at
        ) VALUES (
            :task_id, 'PR result source', 'review', 'completed', 0, 'main',
            'pending', 0, 2, 'auto', '2026-08-16 00:00:00'
        )
    """), {"task_id": task_id})
    conn.execute(text("""
        INSERT INTO monitored_repos (
            id, repo_full_name, webhook_secret, required_checks,
            created_at, updated_at
        ) VALUES (
            :repo_id, :repo_name, 'secret', '[]',
            '2026-08-16 00:00:00', '2026-08-16 00:00:00'
        )
    """), {
        "repo_id": repo_id,
        "repo_name": f"owner/pr-result-{suffix}",
    })
    conn.execute(text("""
        INSERT INTO pr_reviews (
            id, repo_id, pr_number, base_ref, base_sha, head_sha,
            pr_title, pr_author, pr_url, status, created_at
        ) VALUES (
            :review_id, :repo_id, :pr_number, 'main', :base_sha, :head_sha,
            'PR result evidence', 'alice', :pr_url, 'approved',
            '2026-08-16 00:00:01'
        )
    """), {
        "review_id": review_id,
        "repo_id": repo_id,
        "pr_number": suffix,
        "base_sha": base_sha,
        "head_sha": head_sha,
        "pr_url": f"https://github.com/owner/pr-result-{suffix}/pull/{suffix}",
    })
    conn.execute(text("""
        INSERT INTO pr_monitor_runs (
            id, repo_id, pr_number, status, current_base_sha,
            current_head_sha, current_review_id, created_at, updated_at
        ) VALUES (
            :run_id, :repo_id, :pr_number, 'reviewing', :base_sha,
            :head_sha, :review_id,
            '2026-08-16 00:00:02', '2026-08-16 00:00:02'
        )
    """), {
        "run_id": run_id,
        "repo_id": repo_id,
        "pr_number": suffix,
        "base_sha": base_sha,
        "head_sha": head_sha,
        "review_id": review_id,
    })
    conn.execute(text(
        "UPDATE pr_reviews SET monitor_run_id = :run_id WHERE id = :review_id"
    ), {"run_id": run_id, "review_id": review_id})
    return repo_id, review_id, run_id


class _PRResultSimulatedCrash(RuntimeError):
    pass


class TestPRReviewResultEvidenceMigration:
    def test_input_error_check_matches_orm_and_preserves_literal_case(self):
        module = _load_pr_review_result_evidence_migration(
            "input_error_model_check"
        )
        constraints = {
            constraint.name: constraint
            for constraint in backend.models.pr_monitor.PRReview.__table__.constraints
            if isinstance(constraint, CheckConstraint)
        }
        model_check = constraints[module._INPUT_ERROR_CHECK]

        assert module._check_shape(model_check.sqltext) == module._check_shape(
            module._INPUT_ERROR_CHECK_SQL
        )
        assert module._check_shape(
            constraints[module._ATTEMPT_CHECK].sqltext
        ) == module._check_shape(module._ATTEMPT_CHECK_SQL)
        assert module._check_shape(
            constraints[module._RERUN_SHAPE_CHECK].sqltext
        ) == module._check_shape(module._RERUN_SHAPE_CHECK_SQL)
        assert module._check_shape(
            module._INPUT_ERROR_CHECK_SQL.replace(
                "'UTF-8 bytes'",
                "'utf-8 bytes'",
            )
        ) != module._check_shape(module._INPUT_ERROR_CHECK_SQL)
        normalized = module._normalized_check_sql(module._INPUT_ERROR_CHECK_SQL)
        assert "replace(error_category, 'unsupported_input_size', '')" in normalized
        assert "replace(error_unit, 'characters', '')" in normalized
        assert "replace(error_unit, 'UTF-8 bytes', '')" in normalized
        assert module._normalized_check_sql(
            module._INPUT_ERROR_CHECK_SQL.replace(
                "'unsupported_input_size'",
                "_utf8mb4'unsupported_input_size'",
            )
        ) == normalized

    @pytest.mark.parametrize(
        "drifted_sql",
        (
            "error_category IS NULL",
            None,
            "__canonical_with_wrong_category__",
            "__canonical_with_wrong_limit__",
            "__canonical_with_wrong_unit__",
        ),
    )
    def test_input_error_check_reflection_is_fail_closed(
        self,
        drifted_sql,
    ):
        module = _load_pr_review_result_evidence_migration(
            "input_error_check_drift"
        )
        replacements = {
            "__canonical_with_wrong_category__": (
                module._INPUT_ERROR_CHECK_SQL.replace(
                    "unsupported_input_size",
                    "Unsupported_input_size",
                )
            ),
            "__canonical_with_wrong_limit__": (
                module._INPUT_ERROR_CHECK_SQL.replace(
                    "9007199254740991",
                    "9007199254740992",
                )
            ),
            "__canonical_with_wrong_unit__": (
                module._INPUT_ERROR_CHECK_SQL.replace(
                    "'UTF-8 bytes'",
                    "'UTF-8 Bytes'",
                )
            ),
        }
        drifted_sql = replacements.get(drifted_sql, drifted_sql)
        bind = SimpleNamespace(dialect=SimpleNamespace(name="sqlite"))
        inspector = MagicMock()
        inspector.get_check_constraints.return_value = [{
            "name": module._INPUT_ERROR_CHECK,
            "sqltext": drifted_sql,
        }]

        with (
            patch.object(module.op, "get_bind", return_value=bind),
            patch.object(module.sa, "inspect", return_value=inspector),
            pytest.raises(RuntimeError, match="incompatible input evidence"),
        ):
            module._input_error_check_present()

    def test_mysql_input_error_check_must_be_enforced(self):
        module = _load_pr_review_result_evidence_migration(
            "input_error_check_mysql_enforcement"
        )
        bind = MagicMock()
        bind.dialect = SimpleNamespace(name="mysql", is_mariadb=False)
        bind.execute.return_value = [
            (module._INPUT_ERROR_CHECK, "NO"),
        ]
        inspector = MagicMock()
        inspector.get_check_constraints.return_value = [{
            "name": module._INPUT_ERROR_CHECK,
            "sqltext": module._INPUT_ERROR_CHECK_SQL,
        }]

        with (
            patch.object(module.op, "get_bind", return_value=bind),
            patch.object(module.sa, "inspect", return_value=inspector),
            pytest.raises(RuntimeError, match="is not enforced"),
        ):
            module._input_error_check_present()

    @pytest.mark.parametrize(
        ("dialect_name", "is_mariadb", "version", "accepted"),
        (
            ("mysql", False, (8, 0, 15), False),
            ("mysql", False, (8, 0, 16), True),
            ("mariadb", True, (10, 6, 0), False),
            ("mariadb", True, (10, 6, 1), True),
        ),
    )
    def test_mysql_family_check_enforcement_version_gate(
        self,
        dialect_name,
        is_mariadb,
        version,
        accepted,
    ):
        module = _load_pr_review_result_evidence_migration(
            f"input_error_version_{dialect_name}_{version[-1]}"
        )
        bind = MagicMock()
        bind.dialect = SimpleNamespace(
            name=dialect_name,
            is_mariadb=is_mariadb,
            server_version_info=version,
        )
        bind.execute.return_value = [
            ("pr_reviews", "InnoDB"),
            ("pr_monitor_runs", "InnoDB"),
        ]
        with (
            patch.object(module.op, "get_bind", return_value=bind),
            patch.object(
                module,
                "context",
                SimpleNamespace(is_offline_mode=lambda: False),
            ),
        ):
            if accepted:
                module._require_supported_mysql_family()
            else:
                with pytest.raises(RuntimeError, match="requires"):
                    module._require_supported_mysql_family()

    @pytest.mark.parametrize(
        "engines",
        (
            (("pr_reviews", "MyISAM"), ("pr_monitor_runs", "InnoDB")),
            (("pr_reviews", "InnoDB"),),
            (("pr_reviews", None), ("pr_monitor_runs", "InnoDB")),
        ),
    )
    def test_mysql_family_requires_both_target_tables_to_be_innodb(
        self,
        engines,
    ):
        module = _load_pr_review_result_evidence_migration("mysql_engines")
        bind = MagicMock()
        bind.dialect = SimpleNamespace(
            name="mysql",
            is_mariadb=False,
            server_version_info=(8, 0, 36),
        )
        bind.execute.return_value = engines
        with (
            patch.object(module.op, "get_bind", return_value=bind),
            patch.object(
                module,
                "context",
                SimpleNamespace(is_offline_mode=lambda: False),
            ),
            pytest.raises(RuntimeError, match="InnoDB"),
        ):
            module._require_supported_mysql_family()

    def test_constraint_creation_rechecks_the_rerun_foreign_key(self):
        module = _load_pr_review_result_evidence_migration(
            "rerun_fk_post_create_reflection"
        )
        batch_alter = MagicMock()
        batch = batch_alter.return_value.__enter__.return_value

        with (
            patch.object(
                module,
                "context",
                SimpleNamespace(is_offline_mode=lambda: False),
            ),
            patch.object(module, "_check_present", return_value=True),
            patch.object(
                module,
                "_unique_constraints",
                return_value={
                    module._OLD_SUBJECT: module._OLD_SUBJECT_COLUMNS,
                },
            ),
            patch.object(module, "_ensure_unique"),
            patch.object(
                module,
                "_owned_rerun_foreign_keys",
                side_effect=([], []),
            ),
            patch.object(module.op, "batch_alter_table", batch_alter),
            pytest.raises(
                RuntimeError,
                match="did not install the canonical rerun foreign key",
            ),
        ):
            module._ensure_constraints()

        batch.create_foreign_key.assert_called_once_with(
            module._RERUN_FK,
            module._TABLE,
            ["rerun_of_review_id"],
            ["id"],
            ondelete="SET NULL",
        )

    def test_postgresql_check_uses_server_canonical_definition(self):
        module = _load_pr_review_result_evidence_migration(
            "input_error_postgresql_check"
        )
        bind = MagicMock()
        bind.dialect = SimpleNamespace(name="postgresql")
        canonical = (
            "CHECK (((error_category)::text = "
            "'unsupported_input_size'::text))"
        )
        with (
            patch.object(module.op, "get_bind", return_value=bind),
            patch.object(
                module,
                "_postgresql_check_definitions",
                side_effect=(
                    {module._INPUT_ERROR_CHECK: canonical},
                    {module._INPUT_ERROR_CHECK: canonical},
                ),
            ),
        ):
            assert module._input_error_check_present() is True
        assert bind.execute.call_count == 2

    def test_legacy_nonce_backfill_is_mysql_collation_independent(self):
        module = _load_pr_review_result_evidence_migration(
            "mysql_nonce_collation"
        )
        expression = module._lowercase_hex_sql_remainder(
            module.sa.column("action_nonce", module.sa.String())
        )
        compiled = str(module.sa.select(expression).compile(
            dialect=mysql.dialect(),
            compile_kwargs={"literal_binds": True},
        )).lower()

        assert "lower(" not in compiled
        assert compiled.count("replace(") == 16
        assert "'a'" in compiled
        assert "'f'" in compiled

    def test_sqlite_fresh_upgrade_downgrade_reupgrade(self, tmp_path):
        db_path = str(tmp_path / "pr-result-roundtrip.db")
        cfg = _alembic_cfg(db_path)
        module = _load_pr_review_result_evidence_migration(
            "sqlite_roundtrip_check"
        )

        _run_alembic(cfg, command.upgrade, PR_REVIEW_RESULT_EVIDENCE_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        inspector = inspect(engine)
        assert set(_PR_RESULT_REVIEW_COLUMNS) <= {
            item["name"] for item in inspector.get_columns("pr_reviews")
        }
        assert set(_PR_RESULT_RUN_COLUMNS) <= {
            item["name"] for item in inspector.get_columns("pr_monitor_runs")
        }
        assert {
            item["name"]: tuple(item["column_names"])
            for item in inspector.get_unique_constraints("pr_reviews")
        }["uq_pr_reviews_repo_pr_base_ref_base_head_attempt"] == (
            "repo_id", "pr_number", "base_ref", "base_sha", "head_sha",
            "attempt",
        )
        checks = {
            item["name"]: item["sqltext"]
            for item in inspector.get_check_constraints("pr_reviews")
        }
        for name, sql in module._CHECK_SPECS.items():
            assert module._check_shape(checks[name]) == module._check_shape(sql)
        engine.dispose()

        _run_alembic(cfg, command.downgrade, INSTANCE_PROCESS_IDENTITY_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        inspector = inspect(engine)
        assert set(_PR_RESULT_REVIEW_COLUMNS).isdisjoint(
            item["name"] for item in inspector.get_columns("pr_reviews")
        )
        assert set(_PR_RESULT_RUN_COLUMNS).isdisjoint(
            item["name"] for item in inspector.get_columns("pr_monitor_runs")
        )
        assert set(module._CHECK_SPECS).isdisjoint({
            item["name"]
            for item in inspector.get_check_constraints("pr_reviews")
        })
        assert {
            item["name"]: tuple(item["column_names"])
            for item in inspector.get_unique_constraints("pr_reviews")
        }["uq_pr_reviews_repo_pr_base_ref_base_head"] == (
            "repo_id", "pr_number", "base_ref", "base_sha", "head_sha",
        )
        engine.dispose()

        _run_alembic(cfg, command.upgrade, PR_REVIEW_RESULT_EVIDENCE_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == PR_REVIEW_RESULT_EVIDENCE_REVISION
        engine.dispose()

    def test_postgresql_downgrade_takes_access_exclusive_writer_locks(self):
        module = _load_pr_review_result_evidence_migration(
            "postgresql_downgrade_writer_fence"
        )
        execute = MagicMock()
        with (
            patch.object(
                module,
                "context",
                SimpleNamespace(is_offline_mode=lambda: False),
            ),
            patch.object(module, "_dialect", return_value="postgresql"),
            patch.object(module.op, "execute", execute),
        ):
            assert module._acquire_downgrade_writer_fence() == "postgresql"

        execute.assert_called_once()
        lock_sql = " ".join(str(execute.call_args.args[0]).lower().split())
        assert lock_sql == (
            "lock table pr_reviews, pr_monitor_runs "
            "in access exclusive mode"
        )

    def test_sqlite_downgrade_preserves_legacy_duplicate_null_subjects(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "pr-result-null-subject-downgrade.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, INSTANCE_PROCESS_IDENTITY_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO monitored_repos (
                    id, repo_full_name, webhook_secret, required_checks,
                    created_at, updated_at
                ) VALUES (
                    9600, 'owner/legacy-null-subject', 'secret', '[]',
                    '2026-08-16 00:00:00', '2026-08-16 00:00:00'
                )
            """))
            for review_id in (9601, 9602):
                conn.execute(text("""
                    INSERT INTO pr_reviews (
                        id, repo_id, pr_number, base_ref, base_sha, head_sha,
                        pr_title, pr_author, pr_url, status, created_at
                    ) VALUES (
                        :review_id, 9600, 4, 'main', NULL, NULL,
                        'legacy null subject', 'alice',
                        'https://github.com/owner/legacy-null-subject/pull/4',
                        'pending', '2026-08-16 00:00:01'
                    )
                """), {"review_id": review_id})
        engine.dispose()

        _run_alembic(cfg, command.upgrade, PR_REVIEW_RESULT_EVIDENCE_REVISION)
        _run_alembic(cfg, command.downgrade, INSTANCE_PROCESS_IDENTITY_REVISION)

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT id FROM pr_reviews WHERE repo_id = 9600 ORDER BY id"
            )).scalars().all() == [9601, 9602]
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == INSTANCE_PROCESS_IDENTITY_REVISION
        engine.dispose()

    @pytest.mark.parametrize("rowcount", (0, -1, 2))
    def test_sqlite_downgrade_writer_fence_requires_exact_revision_row(
        self,
        rowcount,
    ):
        module = _load_pr_review_result_evidence_migration(
            f"sqlite_downgrade_writer_fence_{rowcount}"
        )
        bind = MagicMock()
        bind.execute.return_value.rowcount = rowcount
        with (
            patch.object(
                module,
                "context",
                SimpleNamespace(is_offline_mode=lambda: False),
            ),
            patch.object(module, "_dialect", return_value="sqlite"),
            patch.object(module.op, "get_bind", return_value=bind),
            pytest.raises(RuntimeError, match="SQLite revision writer fence"),
        ):
            module._acquire_downgrade_writer_fence()

        statement, params = bind.execute.call_args.args
        assert " ".join(str(statement).lower().split()) == (
            "update alembic_version set version_num = version_num "
            "where version_num = :expected_revision"
        )
        assert params == {"expected_revision": module.revision}

    def test_sqlite_downgrade_writer_fence_accepts_exact_revision_row(self):
        module = _load_pr_review_result_evidence_migration(
            "sqlite_downgrade_writer_fence_success"
        )
        bind = MagicMock()
        bind.execute.return_value.rowcount = 1
        with (
            patch.object(
                module,
                "context",
                SimpleNamespace(is_offline_mode=lambda: False),
            ),
            patch.object(module, "_dialect", return_value="sqlite"),
            patch.object(module.op, "get_bind", return_value=bind),
        ):
            assert module._acquire_downgrade_writer_fence() == "sqlite"

    @pytest.mark.parametrize("dialect_name", ("mysql", "mariadb"))
    def test_mysql_family_downgrade_refuses_before_destructive_ddl(
        self,
        dialect_name,
    ):
        module = _load_pr_review_result_evidence_migration(
            f"{dialect_name}_downgrade_writer_fence"
        )
        batch_alter = MagicMock()
        with (
            patch.object(
                module,
                "context",
                SimpleNamespace(is_offline_mode=lambda: False),
            ),
            patch.object(module, "_require_supported_mysql_family"),
            patch.object(module, "_dialect", return_value=dialect_name),
            patch.object(module.op, "batch_alter_table", batch_alter),
            pytest.raises(RuntimeError, match="releases writer locks"),
        ):
            module.downgrade()
        batch_alter.assert_not_called()

    def test_downgrade_discards_only_reconstructible_legacy_projections(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "legacy-projection-downgrade.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, INSTANCE_PROCESS_IDENTITY_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        legacy_rows = (
            (
                9401,
                401,
                "approved",
                "lgtm_comment",
                None,
                "legacy approval summary",
            ),
            (
                9402,
                402,
                "commented",
                "review_comments",
                None,
                "legacy changes summary",
            ),
            (
                9403,
                403,
                "error",
                "error",
                "approved_merged",
                "PR was merged before publication",
            ),
        )
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO monitored_repos (
                    id, repo_full_name, webhook_secret, required_checks,
                    created_at, updated_at
                ) VALUES (
                    9400, 'owner/legacy-projection', 'secret', '[]',
                    '2026-08-16 00:00:00', '2026-08-16 00:00:00'
                )
            """))
            for (
                review_id,
                pr_number,
                status,
                action_taken,
                pending_action,
                summary,
            ) in legacy_rows:
                conn.execute(text("""
                    INSERT INTO pr_reviews (
                        id, repo_id, pr_number, base_ref, base_sha, head_sha,
                        pr_title, pr_author, pr_url, status, action_taken,
                        pending_action, review_summary, created_at
                    ) VALUES (
                        :review_id, 9400, :pr_number, 'main', :base_sha,
                        :head_sha, 'legacy projection', 'alice', :pr_url,
                        :status, :action_taken, :pending_action, :summary,
                        '2026-08-16 00:00:01'
                    )
                """), {
                    "review_id": review_id,
                    "pr_number": pr_number,
                    "base_sha": f"{pr_number % 10}" * 40,
                    "head_sha": f"{(pr_number + 1) % 10}" * 40,
                    "pr_url": (
                        "https://github.com/owner/legacy-projection/pull/"
                        f"{pr_number}"
                    ),
                    "status": status,
                    "action_taken": action_taken,
                    "pending_action": pending_action,
                    "summary": summary,
                })
            conn.execute(text("""
                INSERT INTO pr_monitor_runs (
                    id, repo_id, pr_number, status, current_base_sha,
                    current_head_sha, current_review_id, created_at, updated_at
                ) VALUES (
                    9503, 9400, 403, 'reviewing', :base_sha, :head_sha, 9403,
                    '2026-08-16 00:00:02', '2026-08-16 00:00:02'
                )
            """), {
                "base_sha": "3" * 40,
                "head_sha": "4" * 40,
            })
            conn.execute(text(
                "UPDATE pr_reviews SET monitor_run_id = 9503 WHERE id = 9403"
            ))
        engine.dispose()

        _run_alembic(cfg, command.upgrade, PR_REVIEW_RESULT_EVIDENCE_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            projected = {
                row.id: (
                    row.code_verdict,
                    row.publication_state,
                    row.failure_stage,
                )
                for row in conn.execute(text(
                    "SELECT id, code_verdict, publication_state, failure_stage "
                    "FROM pr_reviews WHERE id BETWEEN 9401 AND 9403 "
                    "ORDER BY id"
                ))
            }
            marker = conn.execute(text(
                "SELECT legacy_terminal_recovery_pending "
                "FROM pr_monitor_runs WHERE id = 9503"
            )).scalar_one()
        assert projected == {
            9401: ("pass", "reconciling", "recovery"),
            9402: ("changes_required", "reconciling", "recovery"),
            9403: (None, "not_applicable", "lifecycle"),
        }
        assert marker == 1
        engine.dispose()

        _run_alembic(
            cfg,
            command.downgrade,
            INSTANCE_PROCESS_IDENTITY_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        inspector = inspect(engine)
        assert set(_PR_RESULT_REVIEW_COLUMNS).isdisjoint(
            item["name"] for item in inspector.get_columns("pr_reviews")
        )
        assert set(_PR_RESULT_RUN_COLUMNS).isdisjoint(
            item["name"] for item in inspector.get_columns("pr_monitor_runs")
        )
        with engine.connect() as conn:
            restored = conn.execute(text(
                "SELECT id, status, action_taken, pending_action, review_summary "
                "FROM pr_reviews WHERE id BETWEEN 9401 AND 9403 ORDER BY id"
            )).all()
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == INSTANCE_PROCESS_IDENTITY_REVISION
        assert restored == [
            (row[0], row[2], row[3], row[4], row[5]) for row in legacy_rows
        ]
        engine.dispose()

    def test_sqlite_input_error_check_enforces_canonical_quartet(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "pr-result-input-error-check.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, PR_REVIEW_RESULT_EVIDENCE_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            _, review_id, _ = _seed_pr_result_subject(conn, suffix=41)

        update = text(
            "UPDATE pr_reviews SET error_category = :category, "
            "error_measured = :measured, error_limit = :limit, "
            "error_unit = :unit WHERE id = :review_id"
        )
        canonical = {
            "category": "unsupported_input_size",
            "measured": 2,
            "limit": 1,
            "unit": "characters",
            "review_id": review_id,
        }
        with engine.begin() as conn:
            conn.execute(update, canonical)
            conn.execute(update, {
                **canonical,
                "measured": 9_007_199_254_740_991,
                "limit": 9_007_199_254_740_990,
                "unit": "UTF-8 bytes",
            })
            conn.execute(update, {
                "category": None,
                "measured": None,
                "limit": None,
                "unit": None,
                "review_id": review_id,
            })

        invalid = (
            {**canonical, "category": None},
            {**canonical, "category": "Unsupported_input_size"},
            {**canonical, "category": "unsupported_input_size "},
            {**canonical, "measured": None},
            {**canonical, "limit": None},
            {**canonical, "unit": None},
            {**canonical, "limit": 0},
            {**canonical, "limit": -1},
            {**canonical, "measured": 1},
            {
                **canonical,
                "measured": 9_007_199_254_740_992,
                "limit": 9_007_199_254_740_991,
            },
            {**canonical, "unit": "utf-8 bytes"},
            {**canonical, "unit": "characters "},
            {
                **canonical,
                "measured": None,
                "limit": None,
                "unit": None,
            },
        )
        for values in invalid:
            with engine.connect() as conn:
                transaction = conn.begin()
                with pytest.raises(IntegrityError):
                    conn.execute(update, values)
                transaction.rollback()
        engine.dispose()

    def test_sqlite_rerun_attempt_and_shape_checks_are_enforced(self, tmp_path):
        db_path = str(tmp_path / "pr-result-rerun-checks.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, PR_REVIEW_RESULT_EVIDENCE_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            _, parent_id, _ = _seed_pr_result_subject(conn, suffix=42)
            _, child_id, _ = _seed_pr_result_subject(conn, suffix=43)

        invalid_updates = (
            ("attempt = 0", {}),
            (
                "attempt = 2, rerun_of_review_id = :parent_id, "
                "rerun_idempotency_key = NULL",
                {"parent_id": parent_id},
            ),
            (
                "attempt = 2, rerun_of_review_id = :child_id, "
                "rerun_idempotency_key = 'self-rerun'",
                {"child_id": child_id},
            ),
            (
                "attempt = 1, rerun_of_review_id = :parent_id, "
                "rerun_idempotency_key = 'first-attempt-parent'",
                {"parent_id": parent_id},
            ),
            (
                "attempt = 2, rerun_of_review_id = NULL, "
                "rerun_idempotency_key = NULL",
                {},
            ),
        )
        for assignment, params in invalid_updates:
            with engine.connect() as conn:
                transaction = conn.begin()
                with pytest.raises(IntegrityError):
                    conn.execute(
                        text(
                            f"UPDATE pr_reviews SET {assignment} "
                            "WHERE id = :child_id"
                        ),
                        {"child_id": child_id, **params},
                    )
                transaction.rollback()

        with engine.connect() as conn:
            conn.exec_driver_sql("PRAGMA foreign_keys = ON")
            conn.commit()
            transaction = conn.begin()
            # A first review admitted by ready_for_review may carry a durable
            # idempotency key without having a parent review.
            conn.execute(
                text(
                    "UPDATE pr_reviews SET attempt = 1, "
                    "rerun_of_review_id = NULL, "
                    "rerun_idempotency_key = 'first-attempt-key' "
                    "WHERE id = :child_id"
                ),
                {"child_id": child_id},
            )
            conn.execute(
                text(
                    "UPDATE pr_reviews SET attempt = 2, "
                    "rerun_of_review_id = :parent_id, "
                    "rerun_idempotency_key = 'valid-rerun' WHERE id = :child_id"
                ),
                {"parent_id": parent_id, "child_id": child_id},
            )
            conn.execute(
                text("DELETE FROM pr_reviews WHERE id = :parent_id"),
                {"parent_id": parent_id},
            )
            rerun = conn.execute(text(
                "SELECT attempt, rerun_of_review_id, rerun_idempotency_key "
                "FROM pr_reviews WHERE id = :child_id"
            ), {"child_id": child_id}).one()
            assert rerun == (2, None, "valid-rerun")
            transaction.commit()
        engine.dispose()

    @pytest.mark.parametrize(
        ("table_name", "column_name"),
        [
            *[("pr_reviews", name) for name in _PR_RESULT_REVIEW_COLUMNS],
            *[("pr_monitor_runs", name) for name in _PR_RESULT_RUN_COLUMNS],
        ],
    )
    def test_upgrade_replays_every_committed_column_add(
        self,
        table_name,
        column_name,
    ):
        module = _load_pr_review_result_evidence_migration(
            f"add_{table_name}_{column_name}"
        )
        state = {module._TABLE: {}, module._RUN_TABLE: {}}
        calls: dict[tuple[str, str], int] = {}
        crashed = False

        @contextmanager
        def batch_alter_table(selected_table, **_kwargs):
            class Batch:
                def add_column(self, column):
                    nonlocal crashed
                    key = (selected_table, column.name)
                    calls[key] = calls.get(key, 0) + 1
                    state[selected_table][column.name] = {"name": column.name}
                    if key == (table_name, column_name) and not crashed:
                        crashed = True
                        raise _PRResultSimulatedCrash(column_name)

            yield Batch()

        def reflected(selected_table=module._TABLE):
            return dict(state[selected_table])

        with (
            patch.object(module, "_dialect", return_value="sqlite"),
            patch.object(
                module,
                "context",
                SimpleNamespace(is_offline_mode=lambda: False),
            ),
            patch.object(module, "_reflected_columns", side_effect=reflected),
            patch.object(module, "_assert_column_shape"),
            patch.object(
                module.op,
                "batch_alter_table",
                side_effect=batch_alter_table,
            ),
        ):
            with pytest.raises(_PRResultSimulatedCrash, match=column_name):
                module._ensure_columns()
            module._ensure_columns()

        assert crashed
        assert set(state[module._TABLE]) == set(module._column_specs())
        assert set(state[module._RUN_TABLE]) == set(module._run_column_specs())
        assert set(calls) == {
            *[(module._TABLE, name) for name in module._column_specs()],
            *[(module._RUN_TABLE, name) for name in module._run_column_specs()],
        }
        assert all(count == 1 for count in calls.values())

    @pytest.mark.parametrize(
        "target",
        (
            ("create_check_constraint", "ck_pr_reviews_attempt"),
            ("create_check_constraint", "ck_pr_reviews_rerun_shape"),
            ("create_check_constraint", "ck_pr_reviews_input_error_evidence"),
            ("create_unique_constraint", "uq_pr_reviews_repo_pr_base_ref_base_head_attempt"),
            ("create_unique_constraint", "uq_pr_reviews_rerun_idempotency"),
            ("create_foreign_key", "fk_pr_reviews_rerun_of_review_id_pr_reviews"),
            ("create_index", "ix_pr_reviews_rerun_of_review_id"),
            ("drop_constraint", "uq_pr_reviews_repo_pr_base_ref_base_head"),
        ),
        ids=lambda item: f"{item[0]}-{item[1]}",
    )
    def test_upgrade_replays_every_committed_constraint_step(self, target):
        module = _load_pr_review_result_evidence_migration(
            f"constraint_{target[0]}_{target[1]}"
        )
        uniques = {module._OLD_SUBJECT: module._OLD_SUBJECT_COLUMNS}
        checks: dict[str, str] = {}
        foreign_keys: list[dict] = []
        indexes: dict[str, tuple[tuple[str, ...], bool, bool]] = {}
        calls: dict[tuple[str, str], int] = {}
        crashed = False

        def mutate(method, name, operation):
            nonlocal crashed
            key = (method, name)
            calls[key] = calls.get(key, 0) + 1
            operation()
            if key == target and not crashed:
                crashed = True
                raise _PRResultSimulatedCrash(name)

        @contextmanager
        def batch_alter_table(_table, **_kwargs):
            class Batch:
                def create_check_constraint(self, name, expression):
                    mutate(
                        "create_check_constraint",
                        name,
                        lambda: checks.__setitem__(name, expression),
                    )

                def create_unique_constraint(self, name, columns):
                    mutate(
                        "create_unique_constraint",
                        name,
                        lambda: uniques.__setitem__(name, tuple(columns)),
                    )

                def drop_constraint(self, name, *, type_):
                    assert type_ == "unique"
                    mutate(
                        "drop_constraint",
                        name,
                        lambda: uniques.pop(name, None),
                    )

                def create_foreign_key(
                    self, name, referred_table, constrained, referred, **options
                ):
                    def create():
                        foreign_keys[:] = [{
                            "name": name,
                            "constrained_columns": list(constrained),
                            "referred_schema": None,
                            "referred_table": referred_table,
                            "referred_columns": list(referred),
                            "options": options,
                            "dialect_options": {},
                        }]

                    mutate("create_foreign_key", name, create)

                def create_index(self, name, columns, *, unique):
                    mutate(
                        "create_index",
                        name,
                        lambda: indexes.__setitem__(
                            name, (tuple(columns), unique, False)
                        ),
                    )

            yield Batch()

        inspector = MagicMock()
        inspector.get_foreign_keys.side_effect = lambda _table: list(
            foreign_keys
        )
        with (
            patch.object(
                module,
                "context",
                SimpleNamespace(is_offline_mode=lambda: False),
            ),
            patch.object(
                module,
                "_unique_constraints",
                side_effect=lambda: dict(uniques),
            ),
            patch.object(
                module,
                "_check_present",
                side_effect=lambda name, _sql: name in checks,
            ),
            patch.object(module, "_indexes", side_effect=lambda: dict(indexes)),
            patch.object(module.sa, "inspect", return_value=inspector),
            patch.object(module.op, "get_bind", return_value=object()),
            patch.object(
                module.op,
                "batch_alter_table",
                side_effect=batch_alter_table,
            ),
        ):
            with pytest.raises(_PRResultSimulatedCrash, match=target[1]):
                module._ensure_constraints()
            module._ensure_constraints()

        assert crashed
        assert uniques == {
            module._NEW_SUBJECT: module._NEW_SUBJECT_COLUMNS,
            module._RERUN_UNIQUE: module._RERUN_UNIQUE_COLUMNS,
        }
        assert set(checks) == set(module._CHECK_SPECS)
        for name, sql in module._CHECK_SPECS.items():
            assert module._check_shape(checks[name]) == module._check_shape(sql)
        assert len(foreign_keys) == 1
        assert module._rerun_foreign_key_matches(foreign_keys[0])
        assert indexes == {
            module._RERUN_INDEX: (("rerun_of_review_id",), False, False)
        }
        assert calls[target] == 1

    @pytest.mark.parametrize(
        "target",
        _PR_RESULT_DOWNGRADE_CRASH_TARGETS,
        ids=lambda item: f"{item[0]}-{item[1]}",
    )
    def test_sqlite_downgrade_replays_every_committed_ddl_step(
        self,
        tmp_path,
        _pr_result_head_sqlite_db,
        target,
    ):
        db_path = str(tmp_path / f"downgrade-{target[0]}-{target[1]}.db")
        shutil.copyfile(_pr_result_head_sqlite_db, db_path)
        module = _load_pr_review_result_evidence_migration(
            f"downgrade_{target[0]}_{target[1]}"
        )
        engine = create_engine(f"sqlite:///{db_path}")
        crashed = False

        with engine.begin() as conn:
            operations = Operations(MigrationContext.configure(conn))
            original_batch = operations.batch_alter_table

            @contextmanager
            def crash_batch(*args, **kwargs):
                triggered = False
                with original_batch(*args, **kwargs) as batch:
                    class BatchProxy:
                        def __getattr__(self, name):
                            attribute = getattr(batch, name)
                            if name not in {
                                "create_unique_constraint",
                                "drop_constraint",
                                "drop_index",
                                "drop_column",
                            }:
                                return attribute

                            def invoke(*method_args, **method_kwargs):
                                nonlocal triggered
                                result = attribute(*method_args, **method_kwargs)
                                object_name = method_args[0]
                                if (name, object_name) == target:
                                    triggered = True
                                return result

                            return invoke

                    yield BatchProxy()
                if triggered:
                    raise _PRResultSimulatedCrash(target[1])

            try:
                with (
                    patch.object(operations, "batch_alter_table", crash_batch),
                    patch.object(module, "op", operations),
                    patch.object(
                        module,
                        "context",
                        SimpleNamespace(is_offline_mode=lambda: False),
                    ),
                ):
                    module.downgrade()
            except _PRResultSimulatedCrash:
                crashed = True
        engine.dispose()
        assert crashed, f"downgrade never reached {target!r}"

        cfg = _alembic_cfg(db_path)
        _run_alembic(
            cfg,
            command.downgrade,
            INSTANCE_PROCESS_IDENTITY_REVISION,
        )
        engine = create_engine(f"sqlite:///{db_path}")
        inspector = inspect(engine)
        assert set(_PR_RESULT_REVIEW_COLUMNS).isdisjoint(
            item["name"] for item in inspector.get_columns("pr_reviews")
        )
        assert set(_PR_RESULT_RUN_COLUMNS).isdisjoint(
            item["name"] for item in inspector.get_columns("pr_monitor_runs")
        )
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == INSTANCE_PROCESS_IDENTITY_REVISION
        engine.dispose()

    @pytest.mark.parametrize(
        (
            "case_name",
            "spec_group",
            "column_name",
            "dialect_name",
            "actual_type",
            "nullable",
            "default",
            "matches",
        ),
        (
            (
                "mysql-boolean-quoted-zero",
                "run",
                "legacy_terminal_recovery_pending",
                "mysql",
                mysql.TINYINT(display_width=1),
                False,
                "'0'",
                True,
            ),
            (
                "mariadb-boolean-quoted-zero",
                "run",
                "legacy_terminal_recovery_pending",
                "mariadb",
                mysql.TINYINT(display_width=1),
                False,
                "'0'",
                True,
            ),
            (
                "mysql-boolean-quoted-false-is-text",
                "run",
                "legacy_terminal_recovery_pending",
                "mysql",
                mysql.TINYINT(display_width=1),
                False,
                "'false'",
                False,
            ),
            (
                "sqlite-boolean-quoted-false-is-text",
                "run",
                "legacy_terminal_recovery_pending",
                "sqlite",
                Boolean(),
                False,
                "'false'",
                False,
            ),
            (
                "postgresql-boolean-cast",
                "run",
                "legacy_terminal_recovery_pending",
                "postgresql",
                postgresql.BOOLEAN(),
                False,
                "'false'::boolean",
                True,
            ),
            (
                "integer-is-not-bigint",
                "review",
                "attempt",
                "sqlite",
                BigInteger(),
                False,
                "1",
                False,
            ),
            (
                "integer-default-drift",
                "review",
                "attempt",
                "sqlite",
                Integer(),
                False,
                "2",
                False,
            ),
            (
                "code-verdict-varchar-length-drift",
                "review",
                "code_verdict",
                "postgresql",
                postgresql.VARCHAR(length=29),
                True,
                None,
                False,
            ),
            (
                "code-verdict-task-id-is-not-bigint",
                "review",
                "code_verdict_task_id",
                "sqlite",
                BigInteger(),
                True,
                None,
                False,
            ),
            (
                "code-verdict-retry-is-not-bigint",
                "review",
                "code_verdict_retry_count",
                "sqlite",
                BigInteger(),
                True,
                None,
                False,
            ),
            (
                "code-verdict-task-time-timezone-drift",
                "review",
                "code_verdict_task_started_at",
                "postgresql",
                postgresql.TIMESTAMP(timezone=True),
                True,
                None,
                False,
            ),
            (
                "code-verdict-recorded-time-timezone-drift",
                "review",
                "code_verdict_recorded_at",
                "postgresql",
                postgresql.TIMESTAMP(timezone=True),
                True,
                None,
                False,
            ),
            (
                "varchar-length-drift",
                "review",
                "publication_state",
                "postgresql",
                postgresql.VARCHAR(length=29),
                False,
                "'not_started'::character varying",
                False,
            ),
            (
                "text-is-not-varchar",
                "review",
                "publication_error",
                "sqlite",
                String(length=2000),
                True,
                None,
                False,
            ),
            (
                "timestamp-timezone-drift",
                "review",
                "published_at",
                "postgresql",
                postgresql.TIMESTAMP(timezone=True),
                True,
                None,
                False,
            ),
            (
                "mysql-unsigned-bigint-drift",
                "review",
                "github_review_id",
                "mysql",
                mysql.BIGINT(unsigned=True),
                True,
                None,
                False,
            ),
        ),
        ids=lambda *values: values[0] if len(values) == 1 else None,
    )
    def test_column_shape_is_dialect_aware_and_strict(
        self,
        case_name,
        spec_group,
        column_name,
        dialect_name,
        actual_type,
        nullable,
        default,
        matches,
    ):
        del case_name
        module = _load_pr_review_result_evidence_migration(
            f"column_shape_{column_name}_{dialect_name}"
        )
        specs = (
            module._column_specs()
            if spec_group == "review"
            else module._run_column_specs()
        )
        reflected = {
            "name": column_name,
            "type": actual_type,
            "nullable": nullable,
            "default": default,
        }
        with patch.object(module, "_dialect", return_value=dialect_name):
            if matches:
                module._assert_column_shape(reflected, specs[column_name])
            else:
                with pytest.raises(RuntimeError, match="incompatible column"):
                    module._assert_column_shape(reflected, specs[column_name])

    @pytest.mark.parametrize(
        "drift",
        ("name", "schema", "onupdate", "deferrable", "ondelete"),
    )
    def test_rerun_foreign_key_rejects_every_semantic_drift(self, drift):
        module = _load_pr_review_result_evidence_migration(f"fk_{drift}")
        foreign_key = {
            "name": module._RERUN_FK,
            "constrained_columns": ["rerun_of_review_id"],
            "referred_schema": None,
            "referred_table": module._TABLE,
            "referred_columns": ["id"],
            "options": {"ondelete": "SET NULL"},
            "dialect_options": {},
        }
        assert module._rerun_foreign_key_matches(foreign_key)
        if drift == "name":
            foreign_key["name"] = "fk_unowned"
        elif drift == "schema":
            foreign_key["referred_schema"] = "public"
        elif drift == "onupdate":
            foreign_key["options"]["onupdate"] = "CASCADE"
        elif drift == "deferrable":
            foreign_key["options"]["deferrable"] = True
        else:
            foreign_key["options"]["ondelete"] = "CASCADE"
        assert not module._rerun_foreign_key_matches(foreign_key)

    @pytest.mark.parametrize(
        "extra",
        (
            {"dialect_options": {"sqlite_where": text("0")}},
            {"include_columns": ["attempt"]},
            {"column_sorting": {"rerun_of_review_id": ("desc",)}},
            {"expressions": ["lower(rerun_of_review_id)"]},
        ),
    )
    def test_rerun_index_signature_rejects_partial_or_extended_indexes(
        self,
        extra,
    ):
        module = _load_pr_review_result_evidence_migration("index_extra")
        index = {
            "name": module._RERUN_INDEX,
            "column_names": ["rerun_of_review_id"],
            "unique": False,
            **extra,
        }
        assert module._index_signature(index) == (
            ("rerun_of_review_id",),
            False,
            True,
        )
        assert module._index_signature({
            "name": module._RERUN_INDEX,
            "column_names": ["rerun_of_review_id"],
            "unique": False,
            "dialect_options": {
                "postgresql_include": [],
                "postgresql_nulls_not_distinct": False,
            },
        }) == (("rerun_of_review_id",), False, False)

    def test_upgrade_preflights_all_existing_columns_before_any_ddl(self):
        module = _load_pr_review_result_evidence_migration(
            "upgrade_column_preflight"
        )
        review_columns = {
            name: {"name": name}
            for name in module._column_specs()
            if name != "attempt"
        }
        run_columns = {
            name: {"name": name} for name in module._run_column_specs()
        }

        def reflected(table=module._TABLE):
            return review_columns if table == module._TABLE else run_columns

        def validate(_reflected, expected):
            if expected.name == "legacy_terminal_recovery_pending":
                raise RuntimeError("late existing upgrade column drift")

        batch = MagicMock()
        backfill = MagicMock()
        with (
            patch.object(
                module,
                "context",
                SimpleNamespace(is_offline_mode=lambda: False),
            ),
            patch.object(module, "_dialect", return_value="sqlite"),
            patch.object(module, "_require_supported_mysql_family"),
            patch.object(module, "_reflected_columns", side_effect=reflected),
            patch.object(module, "_assert_column_shape", side_effect=validate),
            patch.object(module, "_check_present", return_value=True),
            patch.object(module, "_backfill", backfill),
            patch.object(module.op, "batch_alter_table", batch),
            pytest.raises(
                RuntimeError,
                match="late existing upgrade column drift",
            ),
        ):
            module.upgrade()
        batch.assert_not_called()
        backfill.assert_not_called()

    @pytest.mark.parametrize(
        "drift",
        ("rerun_unique", "foreign_key", "index"),
    )
    def test_upgrade_constraint_drift_fails_before_backfill_or_ddl(self, drift):
        module = _load_pr_review_result_evidence_migration(
            f"upgrade_preflight_{drift}"
        )
        review_columns = {
            name: {"name": name} for name in module._column_specs()
        }
        run_columns = {
            name: {"name": name} for name in module._run_column_specs()
        }
        uniques = {module._OLD_SUBJECT: module._OLD_SUBJECT_COLUMNS}
        foreign_key = {
            "name": module._RERUN_FK,
            "constrained_columns": ["rerun_of_review_id"],
            "referred_schema": None,
            "referred_table": module._TABLE,
            "referred_columns": ["id"],
            "options": {"ondelete": "SET NULL"},
            "dialect_options": {},
        }
        indexes = {
            module._RERUN_INDEX: (("rerun_of_review_id",), False, False)
        }
        if drift == "rerun_unique":
            uniques[module._RERUN_UNIQUE] = ("rerun_of_review_id",)
        elif drift == "foreign_key":
            foreign_key["options"]["onupdate"] = "CASCADE"
        else:
            indexes[module._RERUN_INDEX] = (
                ("rerun_of_review_id",), False, True,
            )

        def reflected(table=module._TABLE):
            return review_columns if table == module._TABLE else run_columns

        inspector = MagicMock()
        inspector.get_foreign_keys.return_value = [foreign_key]
        bind = MagicMock()
        batch = MagicMock()
        backfill = MagicMock()
        with (
            patch.object(
                module,
                "context",
                SimpleNamespace(is_offline_mode=lambda: False),
            ),
            patch.object(module, "_dialect", return_value="sqlite"),
            patch.object(module, "_reflected_columns", side_effect=reflected),
            patch.object(module, "_assert_column_shape"),
            patch.object(module, "_check_present", return_value=True),
            patch.object(
                module,
                "_unique_constraints",
                side_effect=lambda: dict(uniques),
            ),
            patch.object(
                module,
                "_indexes",
                side_effect=lambda: dict(indexes),
            ),
            patch.object(module.sa, "inspect", return_value=inspector),
            patch.object(module.op, "get_bind", return_value=bind),
            patch.object(module.op, "batch_alter_table", batch),
            patch.object(module, "_backfill", backfill),
            pytest.raises(RuntimeError),
        ):
            module.upgrade()
        batch.assert_not_called()
        backfill.assert_not_called()

    def test_downgrade_preflights_all_columns_before_any_ddl(self):
        module = _load_pr_review_result_evidence_migration(
            "downgrade_column_preflight"
        )
        review_columns = {
            name: {"name": name} for name in module._column_specs()
        }
        run_columns = {
            name: {"name": name} for name in module._run_column_specs()
        }

        def reflected(table=module._TABLE):
            return review_columns if table == module._TABLE else run_columns

        def validate(_reflected, expected):
            if expected.name == "legacy_terminal_recovery_pending":
                raise RuntimeError("late owned column drift")

        batch = MagicMock()
        execute = MagicMock()
        foreign_key = {
            "name": module._RERUN_FK,
            "constrained_columns": ["rerun_of_review_id"],
            "referred_schema": None,
            "referred_table": module._TABLE,
            "referred_columns": ["id"],
            "options": {"ondelete": "SET NULL"},
            "dialect_options": {},
        }
        with (
            patch.object(
                module,
                "context",
                SimpleNamespace(is_offline_mode=lambda: False),
            ),
            patch.object(module, "_reflected_columns", side_effect=reflected),
            patch.object(module, "_assert_column_shape", side_effect=validate),
            patch.object(module, "_require_supported_mysql_family"),
            patch.object(module, "_acquire_downgrade_writer_fence"),
            patch.object(
                module,
                "_check_present",
                return_value=True,
            ),
            patch.object(
                module,
                "_unique_constraints",
                return_value={
                    module._NEW_SUBJECT: module._NEW_SUBJECT_COLUMNS,
                    module._RERUN_UNIQUE: module._RERUN_UNIQUE_COLUMNS,
                },
            ),
            patch.object(
                module,
                "_owned_rerun_foreign_keys",
                return_value=[foreign_key],
            ),
            patch.object(
                module,
                "_indexes",
                return_value={
                    module._RERUN_INDEX: (
                        ("rerun_of_review_id",),
                        False,
                        False,
                    )
                },
            ),
            patch.object(module.op, "batch_alter_table", batch),
            patch.object(module.op, "execute", execute),
            pytest.raises(RuntimeError, match="late owned column drift"),
        ):
            module.downgrade()
        batch.assert_not_called()
        execute.assert_not_called()

    @pytest.mark.parametrize("drift", ("unique", "foreign_key", "index"))
    def test_downgrade_constraint_drift_fails_before_any_ddl(self, drift):
        module = _load_pr_review_result_evidence_migration(
            f"downgrade_preflight_{drift}"
        )
        review_columns = {
            name: {"name": name} for name in module._column_specs()
        }
        run_columns = {
            name: {"name": name} for name in module._run_column_specs()
        }
        uniques = {
            module._NEW_SUBJECT: module._NEW_SUBJECT_COLUMNS,
            module._RERUN_UNIQUE: module._RERUN_UNIQUE_COLUMNS,
        }
        foreign_key = {
            "name": module._RERUN_FK,
            "constrained_columns": ["rerun_of_review_id"],
            "referred_schema": None,
            "referred_table": module._TABLE,
            "referred_columns": ["id"],
            "options": {"ondelete": "SET NULL"},
            "dialect_options": {},
        }
        indexes = {
            module._RERUN_INDEX: (("rerun_of_review_id",), False, False)
        }
        if drift == "unique":
            uniques[module._NEW_SUBJECT] = ("repo_id", "attempt")
        elif drift == "foreign_key":
            foreign_key["options"]["onupdate"] = "CASCADE"
        else:
            indexes[module._RERUN_INDEX] = (
                ("rerun_of_review_id",), False, True,
            )

        def reflected(table=module._TABLE):
            return review_columns if table == module._TABLE else run_columns

        inspector = MagicMock()
        inspector.get_foreign_keys.return_value = [foreign_key]
        bind = MagicMock()
        bind.execute.return_value.first.return_value = None
        batch = MagicMock()
        with (
            patch.object(
                module,
                "context",
                SimpleNamespace(is_offline_mode=lambda: False),
            ),
            patch.object(module, "_reflected_columns", side_effect=reflected),
            patch.object(module, "_assert_column_shape"),
            patch.object(module, "_require_supported_mysql_family"),
            patch.object(module, "_acquire_downgrade_writer_fence"),
            patch.object(
                module,
                "_check_present",
                return_value=True,
            ),
            patch.object(
                module,
                "_unique_constraints",
                side_effect=lambda: dict(uniques),
            ),
            patch.object(
                module,
                "_indexes",
                side_effect=lambda: dict(indexes),
            ),
            patch.object(module.sa, "inspect", return_value=inspector),
            patch.object(module.op, "get_bind", return_value=bind),
            patch.object(module.op, "batch_alter_table", batch),
            pytest.raises(RuntimeError),
        ):
            module.downgrade()
        batch.assert_not_called()

    def test_review_113_legacy_terminal_backfill_is_strict(self, tmp_path):
        db_path = str(tmp_path / "review-113-backfill.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, INSTANCE_PROCESS_IDENTITY_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        repo_id = 9113
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO monitored_repos (
                    id, repo_full_name, webhook_secret, required_checks,
                    created_at, updated_at
                ) VALUES (
                    :repo_id, 'owner/review-113', 'secret', '[]',
                    '2026-08-15 00:00:00', '2026-08-15 00:00:00'
                )
            """), {"repo_id": repo_id})
            conn.execute(text("""
                INSERT INTO tasks (
                    id, title, description, status, priority, target_branch,
                    merge_status, retry_count, max_retries, mode, created_at
                ) VALUES (
                    3113, 'review 113', 'review', 'completed', 0, 'main',
                    'pending', 0, 2, 'auto', '2026-08-15 00:00:00'
                )
            """))
            reviews = (
                (113, 1, 1, "error", "error", "lgtm_comment", "PR was merged before publication"),
                (114, 2, 2, "error", "lgtm_comment", "review_comments", "PR was closed before publication"),
                (115, 3, 3, "error", "error", "lgtm_comment", "GitHub transport timed out"),
                (116, 4, 4, "error", "error", "approved_merged", "PR snapshot changed before publication"),
                (117, 4, 6, "approved", "lgtm_comment", None, "ordinary current review"),
                (118, 5, 5, "error", "error", None, "PR became draft before publication"),
            )
            for (
                review_id,
                pr_number,
                sha_seed,
                status,
                action,
                pending,
                summary,
            ) in reviews:
                conn.execute(text("""
                    INSERT INTO pr_reviews (
                        id, repo_id, pr_number, base_ref, base_sha, head_sha,
                        pr_title, pr_author, pr_url, status, action_taken,
                        pending_action, review_summary, created_at
                    ) VALUES (
                        :id, :repo_id, :pr_number, 'main', :base_sha, :head_sha,
                        'legacy review', 'alice', :url, :status, :action,
                        :pending, :summary, '2026-08-15 00:00:01'
                    )
                """), {
                    "id": review_id,
                    "repo_id": repo_id,
                    "pr_number": pr_number,
                    "base_sha": f"{sha_seed}" * 40,
                    "head_sha": f"{(sha_seed + 1) % 10}" * 40,
                    "url": f"https://github.com/owner/review-113/pull/{pr_number}",
                    "status": status,
                    "action": action,
                    "pending": pending,
                    "summary": summary,
                })
            conn.execute(text("""
                UPDATE pr_reviews
                SET task_id = 3113,
                    action_nonce = :nonce,
                    pending_review_body = 'legacy pass body',
                    publishing_actor = 'ccm-bot',
                    publishing_retry_count = 0,
                    publishing_task_started_at = '2026-08-15 00:00:03',
                    publishing_started_at = '2026-08-15 00:00:04'
                WHERE id = 113
            """), {"nonce": "a" * 48})
            for run_id, pr_number, current_review_id, sha_seed in (
                (213, 1, 113, 1),
                (214, 2, 114, 2),
                (215, 3, 115, 3),
                (216, 4, 117, 6),
                (218, 5, 118, 5),
            ):
                conn.execute(text("""
                    INSERT INTO pr_monitor_runs (
                        id, repo_id, pr_number, status, current_base_sha,
                        current_head_sha, current_review_id,
                        created_at, updated_at
                    ) VALUES (
                        :id, :repo_id, :pr_number, 'reviewing', :base_sha,
                        :head_sha, :review_id,
                        '2026-08-15 00:00:02', '2026-08-15 00:00:02'
                    )
                """), {
                    "id": run_id,
                    "repo_id": repo_id,
                    "pr_number": pr_number,
                    "base_sha": f"{sha_seed}" * 40,
                    "head_sha": f"{(sha_seed + 1) % 10}" * 40,
                    "review_id": current_review_id,
                })
        engine.dispose()

        _run_alembic(cfg, command.upgrade, PR_REVIEW_RESULT_EVIDENCE_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            markers = dict(conn.execute(text(
                "SELECT id, legacy_terminal_recovery_pending "
                "FROM pr_monitor_runs ORDER BY id"
            )).all())
            states = {
                row.id: (
                    row.code_verdict,
                    row.publication_state,
                    row.failure_stage,
                )
                for row in conn.execute(text(
                    "SELECT id, code_verdict, publication_state, "
                    "failure_stage "
                    "FROM pr_reviews ORDER BY id"
                ))
            }
        assert markers == {213: 1, 214: 0, 215: 0, 216: 0, 218: 0}
        assert states[113] == ("pass", "not_applicable", "lifecycle")
        assert states[114] == (None, "not_applicable", "lifecycle")
        assert states[115] == (None, "failed", "publication")
        assert states[116] == (None, "not_applicable", "lifecycle")
        assert states[117] == ("pass", "reconciling", "recovery")
        assert states[118] == (None, "not_started", None)
        engine.dispose()

    def test_legacy_code_verdict_backfill_requires_provable_terminal_shape(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "legacy-code-verdict-backfill.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, INSTANCE_PROCESS_IDENTITY_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        full_nonce = "a" * 48
        started_at = "2026-08-15 00:00:03"

        def legacy_row(
            review_id,
            *,
            status,
            action_taken=None,
            pending_action=None,
            expected=None,
            **overrides,
        ):
            values = {
                "id": review_id,
                "pr_number": review_id,
                "base_sha": f"{review_id:040x}"[-40:],
                "head_sha": f"{review_id + 1:040x}"[-40:],
                "url": (
                    "https://github.com/owner/verdict-backfill/pull/"
                    f"{review_id}"
                ),
                "status": status,
                "action_taken": action_taken,
                "pending_action": pending_action,
                "action_nonce": None,
                "task_id": None,
                "pending_body": None,
                "publishing_actor": None,
                "retry_count": None,
                "publishing_task_started_at": None,
                "publishing_started_at": None,
                "expected": expected,
            }
            values.update(overrides)
            return values

        complete_outbox = {
            "action_nonce": full_nonce,
            "task_id": 3301,
            "pending_body": "review body",
            "publishing_actor": "ccm-bot",
            "retry_count": 0,
            "publishing_task_started_at": started_at,
            "publishing_started_at": started_at,
        }
        rows = (
            # Only coherent, already-terminal status/action pairs are proof.
            legacy_row(
                301,
                status="approved",
                action_taken="lgtm_comment",
                expected="pass",
            ),
            legacy_row(
                302,
                status="merged",
                action_taken="approved_merged",
                expected="pass",
            ),
            legacy_row(
                303,
                status="commented",
                action_taken="review_comments",
                expected="changes_required",
            ),
            legacy_row(
                304,
                status="approved",
                action_taken="review_comments",
            ),
            legacy_row(
                305,
                status="merged",
                action_taken="lgtm_comment",
            ),
            legacy_row(
                306,
                status="commented",
                action_taken="lgtm_comment",
            ),
            legacy_row(307, status="approved"),
            # A complete armed Single outbox freezes the verdict before the
            # external GitHub mutation, including publishing/error recovery.
            legacy_row(
                308,
                status="publishing",
                pending_action="lgtm_comment",
                expected="pass",
                **complete_outbox,
            ),
            legacy_row(
                309,
                status="error",
                action_taken="error",
                pending_action="approved_merged",
                expected="pass",
                **(complete_outbox | {"retry_count": 2}),
            ),
            legacy_row(
                310,
                status="error",
                action_taken="error",
                pending_action="review_comments",
                expected="changes_required",
                **(complete_outbox | {"retry_count": 1}),
            ),
            # Near misses must stay unknown; migration may not infer a verdict
            # from a stray pending action or malformed outbox generation.
            legacy_row(
                311,
                status="publishing",
                pending_action="lgtm_comment",
                **(complete_outbox | {"action_nonce": "A" * 48}),
            ),
            legacy_row(
                312,
                status="publishing",
                pending_action="lgtm_comment",
                **(complete_outbox | {"action_nonce": "a" * 47 + "g"}),
            ),
            legacy_row(
                313,
                status="error",
                action_taken="error",
                pending_action="review_comments",
                **(complete_outbox | {"pending_body": None}),
            ),
            legacy_row(
                314,
                status="error",
                action_taken="error",
                pending_action="review_comments",
                **(complete_outbox | {"publishing_actor": None}),
            ),
            legacy_row(
                315,
                status="error",
                action_taken="error",
                pending_action="review_comments",
                **(
                    complete_outbox
                    | {"publishing_task_started_at": None}
                ),
            ),
            legacy_row(
                316,
                status="error",
                action_taken="error",
                pending_action="review_comments",
                **(complete_outbox | {"publishing_started_at": None}),
            ),
            legacy_row(
                317,
                status="error",
                action_taken="error",
                pending_action="review_comments",
                **(complete_outbox | {"retry_count": -1}),
            ),
            legacy_row(
                318,
                status="error",
                action_taken="error",
                pending_action="review_comments",
                **(complete_outbox | {"task_id": None}),
            ),
            legacy_row(
                319,
                status="pending",
                pending_action="lgtm_comment",
                **complete_outbox,
            ),
            # Panel evidence remains in PRReviewerRun and must not be guessed
            # from the legacy aggregate status/action pair.
            legacy_row(
                320,
                status="approved",
                action_taken="lgtm_comment",
            ),
        )
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO monitored_repos (
                    id, repo_full_name, webhook_secret, required_checks,
                    created_at, updated_at
                ) VALUES (
                    3300, 'owner/verdict-backfill', 'secret', '[]',
                    '2026-08-15 00:00:00', '2026-08-15 00:00:00'
                )
            """))
            conn.execute(text("""
                INSERT INTO tasks (
                    id, title, description, status, priority, target_branch,
                    merge_status, retry_count, max_retries, mode, created_at
                ) VALUES (
                    3301, 'legacy reviewer', 'review', 'completed', 0, 'main',
                    'pending', 0, 2, 'auto', '2026-08-15 00:00:01'
                )
            """))
            for row in rows:
                conn.execute(text("""
                    INSERT INTO pr_reviews (
                        id, repo_id, pr_number, base_ref, base_sha, head_sha,
                        pr_title, pr_author, pr_url, task_id, status,
                        action_taken, action_nonce, pending_action,
                        pending_review_body, publishing_actor,
                        publishing_retry_count, publishing_task_started_at,
                        publishing_started_at, created_at
                    ) VALUES (
                        :id, 3300, :pr_number, 'main', :base_sha, :head_sha,
                        'legacy verdict', 'alice', :url, :task_id, :status,
                        :action_taken, :action_nonce, :pending_action,
                        :pending_body, :publishing_actor, :retry_count,
                        :publishing_task_started_at,
                        :publishing_started_at,
                        '2026-08-15 00:00:02'
                    )
                """), row)
            conn.execute(text("""
                INSERT INTO pr_reviewer_runs (
                    pr_review_id, role, provider, status, verdict,
                    prompt_policy_hash, guide_pack_hash, created_at
                ) VALUES (
                    320, 'principal', 'codex', 'completed', 'pass',
                    :policy_hash, :guide_hash, '2026-08-15 00:00:03'
                )
            """), {
                "policy_hash": "b" * 64,
                "guide_hash": "c" * 64,
            })
        engine.dispose()

        _run_alembic(cfg, command.upgrade, PR_REVIEW_RESULT_EVIDENCE_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            actual = {
                item.id: (
                    item.code_verdict,
                    item.code_verdict_task_id,
                    item.code_verdict_retry_count,
                    item.code_verdict_task_started_at,
                    item.code_verdict_recorded_at,
                )
                for item in conn.execute(text(
                    "SELECT id, code_verdict, code_verdict_task_id, "
                    "code_verdict_retry_count, "
                    "code_verdict_task_started_at, "
                    "code_verdict_recorded_at "
                    "FROM pr_reviews ORDER BY id"
                ))
            }
        assert actual == {
            row["id"]: (row["expected"], None, None, None, None)
            for row in rows
        }
        engine.dispose()

    def test_postgresql_offline_upgrade_emits_complete_transactional_sql(self):
        module = _load_pr_review_result_evidence_migration("postgresql_offline")
        output = io.StringIO()
        migration_context = MigrationContext.configure(
            dialect_name="postgresql",
            opts={"as_sql": True, "output_buffer": output},
        )
        with (
            patch.object(module, "op", Operations(migration_context)),
            patch.object(
                module,
                "context",
                SimpleNamespace(is_offline_mode=lambda: True),
            ),
        ):
            module.upgrade()
        ddl = output.getvalue().lower()
        assert "add column attempt integer default '1' not null" in ddl
        assert "add column code_verdict varchar(30)" in ddl
        assert "add column code_verdict_task_id integer" in ddl
        assert "add column code_verdict_retry_count integer" in ddl
        assert "add column code_verdict_task_started_at timestamp" in ddl
        assert "add column code_verdict_recorded_at timestamp" in ddl
        assert "add column legacy_terminal_recovery_pending boolean" in ddl
        assert "ck_pr_reviews_input_error_evidence" in ddl
        assert "error_category = 'unsupported_input_size'" in ddl
        assert "error_measured <= 9007199254740991" in ddl
        assert "error_unit in ('characters', 'utf-8 bytes')" in ddl
        assert "update pr_reviews" in ddl
        new_subject = ddl.index(
            "add constraint uq_pr_reviews_repo_pr_base_ref_base_head_attempt"
        )
        old_subject = ddl.index(
            "drop constraint uq_pr_reviews_repo_pr_base_ref_base_head"
        )
        assert new_subject < old_subject
        assert "fk_pr_reviews_rerun_of_review_id_pr_reviews" in ddl
        assert "ix_pr_reviews_rerun_of_review_id" in ddl

    @pytest.mark.parametrize("dialect_name", ("sqlite", "mysql", "mariadb"))
    def test_nontransactional_offline_upgrade_is_refused_before_ddl(
        self,
        dialect_name,
    ):
        module = _load_pr_review_result_evidence_migration(
            f"offline_{dialect_name}"
        )
        batch = MagicMock()
        with (
            patch.object(module, "_dialect", return_value=dialect_name),
            patch.object(
                module,
                "context",
                SimpleNamespace(is_offline_mode=lambda: True),
            ),
            patch.object(module.op, "batch_alter_table", batch),
            pytest.raises(RuntimeError, match="interrupted DDL"),
        ):
            module._ensure_columns()
        batch.assert_not_called()

    @pytest.mark.parametrize(
        ("evidence_sql", "error_pattern"),
        (
            (
                "UPDATE pr_reviews SET attempt = 2, "
                "rerun_idempotency_key = 'manual-rerun' WHERE id = 8901",
                "rerun or publication evidence",
            ),
            (
                "UPDATE pr_reviews SET code_verdict = 'pass', "
                "code_verdict_task_id = 8701, "
                "code_verdict_retry_count = 0, "
                "code_verdict_task_started_at = '2026-08-16 00:00:03', "
                "code_verdict_recorded_at = '2026-08-16 00:00:04' "
                "WHERE id = 8901",
                "rerun or publication evidence",
            ),
            (
                "UPDATE pr_reviews SET code_verdict = 'pass', "
                "action_taken = 'review_comments' WHERE id = 8901",
                "rerun or publication evidence",
            ),
            (
                "UPDATE pr_reviews SET code_verdict_task_id = 8701 "
                "WHERE id = 8901",
                "rerun or publication evidence",
            ),
            (
                "UPDATE pr_reviews SET code_verdict_retry_count = 0 "
                "WHERE id = 8901",
                "rerun or publication evidence",
            ),
            (
                "UPDATE pr_reviews SET code_verdict_task_started_at = "
                "'2026-08-16 00:00:03' WHERE id = 8901",
                "rerun or publication evidence",
            ),
            (
                "UPDATE pr_reviews SET code_verdict_recorded_at = "
                "'2026-08-16 00:00:04' WHERE id = 8901",
                "rerun or publication evidence",
            ),
            (
                "UPDATE pr_reviews SET publication_state = 'published', "
                "published_actor = 'ccm-bot', github_review_id = 77 "
                "WHERE id = 8901",
                "rerun or publication evidence",
            ),
            (
                "UPDATE pr_reviews SET "
                "error_category = 'unsupported_input_size', "
                "error_measured = 120001, error_limit = 120000, "
                "error_unit = 'characters' WHERE id = 8901",
                "rerun or publication evidence",
            ),
            (
                "UPDATE pr_monitor_runs SET terminal_intent_status = 'merged' "
                "WHERE id = 9001",
                "terminal lifecycle evidence",
            ),
            (
                "UPDATE pr_monitor_runs SET terminal_intent_checked_at = "
                "'2026-08-16 00:00:05' WHERE id = 9001",
                "terminal lifecycle evidence",
            ),
            (
                "UPDATE pr_monitor_runs SET "
                "legacy_terminal_recovery_pending = 1 WHERE id = 9001",
                "terminal lifecycle evidence",
            ),
        ),
        ids=(
            "rerun",
            "native-verdict-provenance",
            "unproven-verdict",
            "code-verdict-task-id",
            "code-verdict-retry",
            "code-verdict-task-started",
            "code-verdict-recorded",
            "publication",
            "input-rejection",
            "terminal-intent",
            "checked-recovery",
            "legacy-marker",
        ),
    )
    def test_downgrade_refuses_every_kind_of_durable_evidence(
        self,
        tmp_path,
        _pr_result_head_sqlite_db,
        evidence_sql,
        error_pattern,
    ):
        db_path = str(tmp_path / f"evidence-{error_pattern.split()[0]}.db")
        shutil.copyfile(_pr_result_head_sqlite_db, db_path)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            _seed_pr_result_subject(conn)
            conn.execute(text(evidence_sql))
        engine.dispose()

        cfg = _alembic_cfg(db_path)
        with pytest.raises(RuntimeError, match=error_pattern):
            _run_alembic(
                cfg,
                command.downgrade,
                INSTANCE_PROCESS_IDENTITY_REVISION,
            )
        engine = create_engine(f"sqlite:///{db_path}")
        inspector = inspect(engine)
        assert set(_PR_RESULT_REVIEW_COLUMNS) <= {
            item["name"] for item in inspector.get_columns("pr_reviews")
        }
        assert set(_PR_RESULT_RUN_COLUMNS) <= {
            item["name"] for item in inspector.get_columns("pr_monitor_runs")
        }
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == PR_REVIEW_RESULT_EVIDENCE_REVISION
        engine.dispose()


class TestInitDbLogic:
    """Test the init_db() branching logic from database.py."""

    def test_init_db_fresh_database(self, tmp_path):
        """Fresh DB (no tables): upgrade head creates everything."""
        db_path = str(tmp_path / "fresh_init.db")

        engine = create_engine(f"sqlite:///{db_path}")
        insp = inspect(engine)
        tables = insp.get_table_names()
        has_tables = "tasks" in tables
        has_alembic = "alembic_version" in tables
        engine.dispose()

        assert not has_tables
        assert not has_alembic

        cfg = _alembic_cfg(db_path)
        # Same logic as init_db: else branch (fresh install)
        _run_alembic(cfg, command.upgrade, "head")

        engine = create_engine(f"sqlite:///{db_path}")
        assert "tasks" in _get_all_tables(engine)
        task_cols = _get_table_columns(engine, "tasks")
        assert "todo_file_path" in task_cols
        engine.dispose()

    def test_init_db_legacy_database(self, tmp_path):
        """Legacy DB (has tables, no alembic_version): stamp initial + upgrade."""
        db_path = str(tmp_path / "legacy_init.db")
        _create_legacy_db(db_path)

        engine = create_engine(f"sqlite:///{db_path}")
        insp = inspect(engine)
        has_tasks = "tasks" in insp.get_table_names()
        has_alembic = "alembic_version" in insp.get_table_names()
        engine.dispose()

        assert has_tasks
        assert not has_alembic

        cfg = _alembic_cfg(db_path)
        # Same logic as init_db: stamp initial, then upgrade
        _run_alembic(cfg, command.stamp, "6b3f8a1c2d9e")
        _run_alembic(cfg, command.upgrade, "head")

        engine = create_engine(f"sqlite:///{db_path}")
        task_cols = _get_table_columns(engine, "tasks")
        assert "todo_file_path" in task_cols
        assert "loop_progress" in task_cols
        engine.dispose()

    def test_init_db_already_tracked(self, tmp_path):
        """Already tracked DB: upgrade head is no-op."""
        db_path = str(tmp_path / "tracked_init.db")
        cfg = _alembic_cfg(db_path)

        # First run creates everything
        _run_alembic(cfg, command.upgrade, "head")

        engine = create_engine(f"sqlite:///{db_path}")
        insp = inspect(engine)
        has_tasks = "tasks" in insp.get_table_names()
        has_alembic = "alembic_version" in insp.get_table_names()
        engine.dispose()

        assert has_tasks
        assert has_alembic

        # Second run is no-op
        _run_alembic(cfg, command.upgrade, "head")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            version = conn.execute(text("SELECT version_num FROM alembic_version")).scalar()
            assert version == _get_head_revision(cfg)
        engine.dispose()
