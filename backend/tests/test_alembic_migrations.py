"""Tests for Alembic migrations.

Ensures:
1. A legacy database (no alembic_version) can be migrated to head.
2. A fresh database can be created from scratch via migrations.
3. The final migrated schema matches the ORM models (no drift).
"""
import importlib.util
import io
import json
from pathlib import Path
from unittest.mock import patch

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
import pytest
from sqlalchemy import BigInteger, create_engine, inspect, text
from sqlalchemy.dialects import mysql, postgresql, sqlite
from sqlalchemy.schema import CreateTable

# All ORM models must be imported so Base.metadata is complete.
from backend.database import Base
import backend.models.task  # noqa: F401
import backend.models.instance  # noqa: F401
import backend.models.project  # noqa: F401
import backend.models.project_todo  # noqa: F401
import backend.models.log_entry  # noqa: F401
import backend.models.worktree  # noqa: F401
import backend.models.global_settings  # noqa: F401
import backend.models.secret  # noqa: F401
import backend.models.quick_phrase  # noqa: F401
import backend.models.plan  # noqa: F401
import backend.models.ssh_profile  # noqa: F401
import backend.models.task_ssh_grant  # noqa: F401

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
PUBLISHED_PLAN_REVISION = "b6e1f4a2c9d7"
PLAN_CLEANUP_REVISION = "f7a1c3d9e5b2"
PR_REVIEW_SNAPSHOT_REVISION = "5f7a9c2e4d61"
PUBLISHED_BRANCH_MERGE_REVISION = "7e4b9c1d2a63"
PR_REVIEW_PANEL_REVISION = "7a1d4e9c2b60"
PR_FINDING_ACTIONS_REVISION = "b7c9e2f4a610"
ATTENTION_TAG_REVISION = "2f6c8a1d4e90"
FIRST_CLASS_PLAN_HEAD_REVISION = "d4a7c9e2f1b6"
PLAN_MAIN_MERGE_REVISION = "e5b8d1c4a7f2"
SSH_PROFILES_REVISION = "73c4a9e1b2d0"
TASK_SSH_GRANTS_REVISION = "84d5b0f2c3e1"
TASK_SSH_POLICY_REVISION = "91e6a4c8d2f0"
CURRENT_HEAD_REVISION = "a6d9f2c4e8b1"


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

        log_cols = _get_table_columns(engine, "log_entries")
        assert "loop_iteration" in log_cols
        assert "task_retry_count" in log_cols

        plan_step_cols = _get_table_columns(engine, "plan_agent_steps")
        assert "last_delta_at" in plan_step_cols
        assert "streamed_output_chars" in plan_step_cols
        assert "last_event_type" in plan_step_cols

        project_cols = _get_table_columns(engine, "projects")
        assert "sort_order" in project_cols
        assert "tags" in project_cols

        pr_review_cols = _get_table_columns(engine, "pr_reviews")
        assert "base_sha" in pr_review_cols
        assert "head_sha" in pr_review_cols
        assert "delivery_id" in pr_review_cols

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


class TestFreshMigration:
    """A fresh database (no tables) can be fully created via Alembic upgrade."""

    def test_fresh_db_upgrade_from_scratch(self, tmp_path):
        """Running upgrade head on empty DB creates all tables."""
        db_path = str(tmp_path / "fresh.db")

        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, "head")

        engine = create_engine(f"sqlite:///{db_path}")
        tables = _get_all_tables(engine)
        expected_tables = {"instances", "projects", "project_todos", "tasks", "log_entries", "worktrees", "global_settings", "secrets", "tags", "discussions", "discussion_messages", "discussion_agents", "discussion_events", "quick_phrases", "sub_agent_sessions", "sub_agent_reports", "pr_reviews", "pr_reviewer_runs", "pr_findings", "pr_finding_actions", "pr_finding_rebuttals", "pr_monitor_runs", "pr_repair_wakes", "pr_merge_queue_actions", "monitored_repos", "workers", "ssh_profiles", "task_ssh_grants", "skill_lessons", "skill_usage", "feishu_user_binding", "org_members", "org_teams", "org_team_members", "task_shares", "project_shares", "shared_tasks_received", "user_skills", "users", "user_groups", "user_group_members", "team_task_shares", "team_project_shares", "plan_agent_runs", "plan_agent_steps", "plans", "plan_versions", "plan_input_requests", "plan_applications", "plan_application_receipts", "plan_application_attempts", "plan_legacy_task_links"}
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
            "base_sha",
            "head_sha",
        ) in unique_column_sets
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
                "SELECT base_sha, head_sha, delivery_id, action_nonce, "
                "pending_action, pending_review_body, publishing_actor, "
                "publishing_retry_count, publishing_task_started_at, "
                "publishing_started_at FROM pr_reviews"
            )).one()
            assert row == (
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
                "base_sha",
                "head_sha",
            ) in unique_column_sets
            assert ("repo_id", "pr_number", "head_sha") not in unique_column_sets
        engine.dispose()

    def test_base_sha_migration_downgrade_restores_head_constraint(self, tmp_path):
        db_path = str(tmp_path / "base_sha_roundtrip.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, "head")

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
            "base_sha",
            "head_sha",
        ) in unique_column_sets
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
    """Published sibling histories converge without rewriting either branch."""

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

    def test_migration_graph_has_one_head_after_ssh_allowed_roots(self, tmp_path):
        cfg = _alembic_cfg(str(tmp_path / "graph.db"))
        script = ScriptDirectory.from_config(cfg)

        assert script.get_heads() == [CURRENT_HEAD_REVISION]
        assert script.get_current_head() == CURRENT_HEAD_REVISION
        assert (
            script.get_revision(CURRENT_HEAD_REVISION).down_revision
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
            == PLAN_MAIN_MERGE_REVISION
        )
        assert (
            script.get_revision(PLAN_MAIN_MERGE_REVISION).down_revision
            == (FIRST_CLASS_PLAN_HEAD_REVISION, ATTENTION_TAG_REVISION)
        )
        assert (
            script.get_revision(PR_FINDING_ACTIONS_REVISION).down_revision
            == PR_REVIEW_PANEL_REVISION
        )
        assert (
            script.get_revision(PR_REVIEW_PANEL_REVISION).down_revision
            == PUBLISHED_BRANCH_MERGE_REVISION
        )

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

    def test_existing_managed_profiles_keep_task_access_on_upgrade(self, tmp_path):
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
        assert bool(row[0]) is True
        assert json.loads(row[1]) == ["exec", "read", "write"]
        assert json.loads(row[2]) == ["/"]
        engine.dispose()

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
