from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool, inspect
from alembic import context

config = context.config

if config.config_file_name is not None:
    # Alembic can run in-process (startup migrations and migration tests).
    # The logging.config default disables every already-created application
    # logger that is not named in alembic.ini.  That silently turns off CCM
    # service diagnostics for the rest of the process, including deliberately
    # redacted SSH audit messages.
    fileConfig(config.config_file_name, disable_existing_loggers=False)

# Import all models so Alembic can detect the full target schema.
# New models must be imported here for autogenerate to work.
from backend.models.instance import Instance   # noqa: F401
from backend.models.project import Project     # noqa: F401
from backend.models.project_todo import ProjectTodo  # noqa: F401
from backend.models.task import Task           # noqa: F401
from backend.models.task_id_allocator import TaskIdAllocator  # noqa: F401
from backend.models.task_migration import TaskMigrationOperation  # noqa: F401
from backend.models.log_entry import LogEntry  # noqa: F401
from backend.models.worktree import Worktree   # noqa: F401
from backend.models.secret import Secret               # noqa: F401
from backend.models.tag import Tag                     # noqa: F401
from backend.models.global_settings import GlobalSettings  # noqa: F401
from backend.models.discussion import Discussion, DiscussionMessage, DiscussionAgent, DiscussionEvent  # noqa: F401
from backend.models.quick_phrase import QuickPhrase  # noqa: F401
from backend.models.monitor_session import MonitorSession, MonitorCheck  # noqa: F401
from backend.models.pr_monitor import (  # noqa: F401
    MonitoredRepo,
    PRFinding,
    PRReview,
    PRReviewerRun,
    PRMonitorRun,
    PRMonitorTaskTombstone,
)
from backend.models.worker import Worker  # noqa: F401
from backend.models.ssh_profile import SSHProfile  # noqa: F401
from backend.models.task_ssh_grant import TaskSSHGrant  # noqa: F401
from backend.models.task_ssh_effect import TaskSSHEffectReceipt  # noqa: F401
from backend.models.worker_turn_handoff import WorkerTurnHandoffReceipt  # noqa: F401
from backend.models.worker_task_termination import WorkerTaskTerminationReceipt  # noqa: F401
from backend.models.skill_lesson import SkillLesson, SkillUsage  # noqa: F401
from backend.models.user_skill import UserSkill  # noqa: F401
from backend.models.feishu_binding import FeishuUserBinding  # noqa: F401
from backend.models.org import OrgMember, OrgTeam, OrgTeamMember  # noqa: F401
from backend.models.task_share import TaskShare, ProjectShare, SharedTaskReceived  # noqa: F401
from backend.models.user import User  # noqa: F401
from backend.models.team_share import TeamProjectShare, TeamTaskShare  # noqa: F401
from backend.models.user_group import UserGroup, UserGroupMember  # noqa: F401
from backend.models.workspace_review import WorkspaceReviewRun  # noqa: F401
from backend.models.test_harness import (  # noqa: F401
    TestHarnessAttempt,
    TestHarnessChildBinding,
    TestHarnessEvent,
    TestHarnessEvidence,
    TestHarnessFinding,
    TestHarnessRun,
    TestHarnessSandboxLease,
)
from backend.models.plan_agent import PlanAgentRun, PlanAgentStep  # noqa: F401
from backend.models.plan import Plan, PlanVersion, PlanInputRequest, PlanApplication, PlanLegacyTaskLink  # noqa: F401
from backend.models.capability import (  # noqa: F401
    CapabilityExecution,
    CapabilityInvocation,
    CapabilityResumeOutbox,
)
from backend.models.code_review import CodeReviewRun, CodeReviewResult  # noqa: F401
from backend.models.delivery import (  # noqa: F401
    DeliveryAction,
    DeliveryCycle,
    DeliveryEvent,
    DeliveryRun,
    DeliveryTransition,
    DeliveryTurn,
)
from backend.database import Base, _async_url_to_sync, _is_sqlite

target_metadata = Base.metadata

# Override sqlalchemy.url from app settings.
# Convert async URL to sync equivalent for Alembic.
from backend.config import settings
sync_url = _async_url_to_sync(settings.database_url)
config.set_main_option('sqlalchemy.url', sync_url)


_use_batch = _is_sqlite(settings.database_url)


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        render_as_batch=_use_batch,  # required for SQLite ALTER TABLE support
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            render_as_batch=_use_batch,  # required for SQLite ALTER TABLE support
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
