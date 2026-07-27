"""Shared fixtures for backend tests."""
import atexit
import asyncio
import os
import shutil
import tempfile
from pathlib import Path

# This must run before the first ``backend.*`` import.  API fixtures override
# FastAPI's ``get_db`` dependency, but process-wide services imported from
# ``backend.main`` retain ``backend.database.async_session``.  Without this
# bootstrap, an incompletely mocked test can write Instance/Task lifecycle
# state into the developer's real ``claude_manager.db``.
TEST_DB_URL = "sqlite+aiosqlite:///:memory:"
_GLOBAL_TEST_DB_DIR = Path(tempfile.mkdtemp(prefix="ccm-pytest-global-"))
atexit.register(shutil.rmtree, _GLOBAL_TEST_DB_DIR, ignore_errors=True)
_GLOBAL_TEST_PROJECT_DIR = _GLOBAL_TEST_DB_DIR / "project"
_GLOBAL_TEST_PROJECT_DIR.mkdir(mode=0o700)
os.environ.update({
    "DATABASE_URL": (
        f"sqlite+aiosqlite:///{_GLOBAL_TEST_DB_DIR / 'global.db'}"
    ),
    # Global services created by ``backend.main`` must never inspect real
    # account journals, credentials, cloud workers, backups, or this checkout.
    "CCM_TESTING": "1",
    "CCM_TEST_PROJECT_DIR": str(_GLOBAL_TEST_PROJECT_DIR),
    "CODEX_POOL_CONFIG_PATH": str(
        _GLOBAL_TEST_DB_DIR / "codex-pool" / "accounts.json"
    ),
    "POOL_CONFIG_PATH": str(
        _GLOBAL_TEST_DB_DIR / "claude-pool" / "accounts.json"
    ),
    "CLOUDROUTER_ACCOUNTS_DIR": str(
        _GLOBAL_TEST_DB_DIR / "cloudrouter-accounts"
    ),
    "WORKSPACE_DIR": str(_GLOBAL_TEST_DB_DIR / "workspace"),
    "WORKER_ENABLED": "false",
    "POOL_ENABLED": "false",
    "CODEX_POOL_ENABLED": "false",
    # Rollout-enabled paths are exercised explicitly.  Keep unrelated tests
    # isolated from task-scoped MCP subprocess construction.
    "CODEX_MAIN_MCP_ENABLED": "false",
    "BACKUP_ENABLED": "false",
    # Unit tests exercise the cleaner with isolated roots explicitly. Importing
    # backend.main must never start a watchdog against the host's real /tmp.
    "TMP_CLEANUP_ENABLED": "false",
    "AUTO_START_DISPATCHER": "false",
    "AUTO_PUSH_TO_ORIGIN": "false",
    # Preserve the product default for constructor/wiring tests. Dispatcher is
    # disabled, so no PTY process can start merely by importing backend.main.
    "USE_PTY_MODE": "true",
    "SERVICE_NAME": "ccm-pytest.invalid",
    "PORT": "0",
    # Host shell/.env settings must not make baseline tests nondeterministic.
    "DEFAULT_PROVIDER": "codex",
    "DEFAULT_CODEX_MODEL": "gpt-5.6-sol",
})

import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

from backend.database import Base

# Import all models so Base.metadata knows about them for create_all
import backend.models.task  # noqa: F401
import backend.models.instance  # noqa: F401
import backend.models.project  # noqa: F401
import backend.models.project_todo  # noqa: F401
import backend.models.log_entry  # noqa: F401
import backend.models.worktree  # noqa: F401
import backend.models.global_settings  # noqa: F401
import backend.models.tag  # noqa: F401
import backend.models.discussion  # noqa: F401
import backend.models.monitor_session  # noqa: F401
import backend.models.pr_monitor  # noqa: F401
import backend.models.worker  # noqa: F401

@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture
async def db_engine():
    engine = create_async_engine(TEST_DB_URL, echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest_asyncio.fixture
async def db_session(db_engine):
    session_factory = async_sessionmaker(db_engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        yield session


@pytest_asyncio.fixture
async def db_factory(db_engine):
    """Returns a session factory (contextmanager), matching the pattern used by services."""
    factory = async_sessionmaker(db_engine, class_=AsyncSession, expire_on_commit=False)
    return factory


# === Shared API test fixtures ===


@pytest_asyncio.fixture
async def app(db_engine):
    """Create a test FastAPI app with in-memory DB and auth disabled.

    Yields (real_app, session_factory) tuple.
    """
    session_factory = async_sessionmaker(db_engine, class_=AsyncSession, expire_on_commit=False)

    from backend.main import app as real_app
    from backend.database import get_db

    async def override_get_db():
        async with session_factory() as session:
            yield session

    real_app.dependency_overrides[get_db] = override_get_db

    from backend.config import settings
    original_token = settings.auth_token
    settings.auth_token = ""

    yield real_app, session_factory

    real_app.dependency_overrides.clear()
    settings.auth_token = original_token


@pytest_asyncio.fixture
async def client(app):
    real_app, _ = app
    transport = ASGITransport(app=real_app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest_asyncio.fixture
async def session_factory(app):
    _, factory = app
    return factory
