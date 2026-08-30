"""Transactional creation and transition tests for Delivery Runs."""

import asyncio

import pytest
from sqlalchemy import event, func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from backend.config import settings
from backend.database import Base
from backend.models.delivery import DeliveryCycle, DeliveryRun, DeliveryTransition
from backend.models.pr_monitor import MonitoredRepo
from backend.models.project import Project
from backend.models.project_todo import ProjectTodo
from backend.models.task import Task
from backend.services.delivery_reducer import DeliveryReducerEvent
from backend.services.delivery_service import (
    DeliveryCreateSpec,
    DeliveryUnavailableError,
    DeliveryUnsupportedScopeError,
    DeliveryValidationError,
    apply_run_event,
    complete_cycle,
    create_delivery_run,
    lock_current_cycle,
    lock_run,
    start_next_cycle,
    value_hash,
)


async def _scope(db_session, *, worker_id=None, auto_merge=False):
    project = Project(
        name=f"delivery-project-{worker_id}-{auto_merge}",
        worker_id=worker_id,
        local_path="/srv/repos/example",
        git_url="git@github.com:acme/example.git",
        has_remote=True,
        default_branch="main",
        status="ready",
    )
    db_session.add(project)
    await db_session.flush()
    repo = MonitoredRepo(
        repo_full_name="acme/example",
        project_id=project.id,
        worker_id=worker_id,
        webhook_secret="secret",
        enabled=True,
        auto_merge=auto_merge,
        review_mode="panel",
        wait_for_ci=True,
        required_checks=[{
            "kind": "check_run",
            "name": "test",
            "app_slug": "github-actions",
        }],
        merge_queue_mode="manual",
        default_branch="main",
    )
    db_session.add(repo)
    await db_session.commit()
    return project, repo


@pytest.mark.asyncio
async def test_delivery_admission_locks_repo_before_project(
    db_session,
    db_engine,
):
    """Keep Delivery and PR Monitor topology writers in one lock order."""

    project, repo = await _scope(db_session)
    statements: list[str] = []

    def record_statement(
        _connection,
        _cursor,
        statement,
        _parameters,
        _context,
        _executemany,
    ):
        statements.append(" ".join(statement.lower().split()))

    event.listen(
        db_engine.sync_engine,
        "before_cursor_execute",
        record_statement,
    )
    try:
        await create_delivery_run(
            db_session,
            DeliveryCreateSpec(
                idempotency_key="topology-lock-order",
                project_id=project.id,
                monitored_repo_id=repo.id,
                title="Preserve topology lock order",
                requirements="Lock the PR Monitor before its Project.",
                provider="codex",
            ),
        )
    finally:
        event.remove(
            db_engine.sync_engine,
            "before_cursor_execute",
            record_statement,
        )

    repo_update = next(
        index
        for index, statement in enumerate(statements)
        if statement.startswith("update monitored_repos ")
    )
    project_update = next(
        index
        for index, statement in enumerate(statements)
        if statement.startswith("update projects ")
    )
    assert repo_update < project_update


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("git_url", "repo_full_name"),
    (
        ("git@gitlab.com:acme/example.git", "acme/example"),
        ("git@github.com:acme/other.git", "acme/example"),
        (None, "acme/example"),
    ),
)
async def test_admission_rejects_nonmatching_github_remote_before_side_effects(
    db_session,
    git_url,
    repo_full_name,
):
    project, repo = await _scope(db_session)
    project.git_url = git_url
    repo.repo_full_name = repo_full_name
    await db_session.commit()

    with pytest.raises(
        DeliveryValidationError,
        match="GitHub remote must exactly match",
    ):
        await create_delivery_run(
            db_session,
            DeliveryCreateSpec(
                idempotency_key=f"bad-remote-{repo.id}",
                project_id=project.id,
                monitored_repo_id=repo.id,
                title="Must reject early",
                requirements="Do not spend a planning or coding turn.",
                provider="codex",
            ),
        )
    await db_session.rollback()
    assert await db_session.scalar(select(func.count(DeliveryRun.id))) == 0
    assert await db_session.scalar(select(func.count(Task.id))) == 0


@pytest.mark.asyncio
async def test_create_delivery_run_is_atomic_and_developer_task_rests(db_session):
    project, repo = await _scope(db_session)

    run = await create_delivery_run(
        db_session,
        DeliveryCreateSpec(
            idempotency_key="service-create-atomic",
            project_id=project.id,
            monitored_repo_id=repo.id,
            title="Fix timeout race",
            requirements="Fix the timeout race and add regression coverage.",
            provider="codex",
            model="gpt-5.6-sol",
            effort_level="high",
            max_cycles=7,
        ),
    )

    task = await db_session.get(Task, run.developer_task_id)
    cycle = await db_session.get(DeliveryCycle, run.current_cycle_id)
    transitions = list(
        (
            await db_session.execute(
                select(DeliveryTransition).where(
                    DeliveryTransition.run_id == run.id
                )
            )
        ).scalars()
    )
    assert run.delivery_branch == f"ccm/delivery/{run.id}-fix-timeout-race"
    assert run.phase == "planning"
    assert run.activity == "ready"
    assert run.cycle_count == 1
    assert run.max_cycles == 7
    assert task is not None
    assert task.mode == "delivery_loop"
    assert task.status == "delivery_waiting"
    assert task.delivery_run_id == run.id
    assert task.delivery_role == "developer"
    assert task.result_branch == run.delivery_branch
    assert task.provider == "codex"
    assert task.execution_user_id is None
    assert task.execution_user_role == "member"
    assert task.execution_mode == "sandbox"
    assert task.execution_principal_kind == "system"
    assert run.policy_snapshot["provider"] == "codex"
    assert run.policy_snapshot["model"] == "gpt-5.6-sol"
    assert run.policy_snapshot["effort_level"] == "high"
    assert cycle is not None
    assert cycle.active_run_id == run.id
    assert cycle.trigger_kind == "initial_request"
    assert len(transitions) == 1
    assert transitions[0].state_version == 1


@pytest.mark.asyncio
async def test_create_delivery_run_links_source_todo_in_same_transaction(db_session):
    project, repo = await _scope(db_session)
    todo = ProjectTodo(
        project_id=project.id,
        title="Fix timeout race",
        prompt="Fix it and add tests.",
        status="open",
    )
    db_session.add(todo)
    await db_session.commit()

    run = await create_delivery_run(
        db_session,
        DeliveryCreateSpec(
            idempotency_key="service-source-todo",
            project_id=project.id,
            monitored_repo_id=repo.id,
            source_todo_id=todo.id,
            title=todo.title,
            requirements=todo.prompt,
        ),
    )

    await db_session.refresh(todo)
    assert todo.status == "done"
    assert todo.created_task_id == run.developer_task_id


@pytest.mark.asyncio
async def test_runtime_defaults_are_frozen_into_policy_and_task(
    db_session,
    monkeypatch,
):
    project, repo = await _scope(db_session)
    monkeypatch.setattr(settings, "default_codex_model", "gpt-5.6-terra")
    monkeypatch.setattr(settings, "default_effort", "xhigh")

    run = await create_delivery_run(
        db_session,
        DeliveryCreateSpec(
            idempotency_key="service-frozen-defaults",
            project_id=project.id,
            monitored_repo_id=repo.id,
            title="Freeze runtime",
            requirements="Persist the resolved runtime tuple.",
        ),
    )

    task = await db_session.get(Task, run.developer_task_id)
    assert task is not None
    assert run.policy_snapshot["provider"] == "codex"
    assert run.policy_snapshot["model"] == "gpt-5.6-terra"
    assert run.policy_snapshot["effort_level"] == "xhigh"
    assert (task.provider, task.model, task.effort_level) == (
        "codex",
        "gpt-5.6-terra",
        "xhigh",
    )


@pytest.mark.asyncio
async def test_claude_runtime_defaults_are_frozen_into_policy_and_task(
    db_session,
    monkeypatch,
):
    project, repo = await _scope(db_session)
    monkeypatch.setattr(settings, "provider_options", "claude")
    monkeypatch.setattr(settings, "default_model", "claude-opus-4-6")
    monkeypatch.setattr(settings, "default_effort", "medium")

    run = await create_delivery_run(
        db_session,
        DeliveryCreateSpec(
            idempotency_key="service-claude-frozen",
            project_id=project.id,
            monitored_repo_id=repo.id,
            title="Run with Claude",
            requirements="Use the available Claude Code provider safely.",
            provider="claude",
        ),
    )

    task = await db_session.get(Task, run.developer_task_id)
    assert task is not None
    assert run.policy_snapshot["provider"] == "claude"
    assert run.policy_snapshot["model"] == "claude-opus-4-6"
    assert run.policy_snapshot["codex_service_tier"] == "default"
    assert run.policy_snapshot["effort_level"] == "medium"
    assert (
        task.provider,
        task.model,
        task.codex_service_tier,
        task.effort_level,
    ) == ("claude", "claude-opus-4-6", "default", "medium")


@pytest.mark.asyncio
async def test_service_rejects_codex_fast_for_claude_provider(db_session):
    project, repo = await _scope(db_session)

    with pytest.raises(DeliveryValidationError, match="only available"):
        await create_delivery_run(
            db_session,
            DeliveryCreateSpec(
                idempotency_key="service-claude-fast-rejected",
                project_id=project.id,
                monitored_repo_id=repo.id,
                title="Reject Fast on Claude",
                requirements="Provider tiers must remain exact.",
                provider="claude",
                codex_service_tier="priority",
            ),
        )
    await db_session.rollback()


@pytest.mark.asyncio
async def test_admission_rejects_disabled_developer_provider_before_side_effects(
    db_session,
    monkeypatch,
):
    project, repo = await _scope(db_session)
    repo.provider = "claude"
    await db_session.commit()
    monkeypatch.setattr(settings, "provider_options", "claude")

    with pytest.raises(
        DeliveryValidationError,
        match="Delivery provider 'codex' is not enabled",
    ):
        await create_delivery_run(
            db_session,
            DeliveryCreateSpec(
                idempotency_key="service-disabled-developer-provider",
                project_id=project.id,
                monitored_repo_id=repo.id,
                title="Reject disabled developer",
                requirements="Do not start a disabled provider.",
                provider="codex",
            ),
        )
    await db_session.rollback()
    assert await db_session.scalar(select(func.count(DeliveryRun.id))) == 0
    assert await db_session.scalar(select(func.count(Task.id))) == 0


@pytest.mark.asyncio
async def test_admission_rejects_disabled_monitor_provider_before_side_effects(
    db_session,
    monkeypatch,
):
    project, repo = await _scope(db_session)
    repo.provider = "codex"
    await db_session.commit()
    monkeypatch.setattr(settings, "provider_options", "claude")

    with pytest.raises(
        DeliveryValidationError,
        match="PR Monitor provider 'codex' is not enabled",
    ):
        await create_delivery_run(
            db_session,
            DeliveryCreateSpec(
                idempotency_key="service-disabled-monitor-provider",
                project_id=project.id,
                monitored_repo_id=repo.id,
                title="Reject disabled reviewer",
                requirements="Do not defer this failure until panel review.",
                provider="claude",
            ),
        )
    await db_session.rollback()
    assert await db_session.scalar(select(func.count(DeliveryRun.id))) == 0
    assert await db_session.scalar(select(func.count(Task.id))) == 0


@pytest.mark.asyncio
async def test_idempotency_scope_is_per_principal_and_project(db_session):
    project, repo = await _scope(db_session)
    common = {
        "idempotency_key": "same-caller-key",
        "project_id": project.id,
        "monitored_repo_id": repo.id,
        "title": "Scoped admission",
        "requirements": "The principal namespace is part of uniqueness.",
    }

    first = await create_delivery_run(
        db_session,
        DeliveryCreateSpec(**common, created_by=11),
    )
    replay = await create_delivery_run(
        db_session,
        DeliveryCreateSpec(**common, created_by=11),
    )
    second_user = await create_delivery_run(
        db_session,
        DeliveryCreateSpec(**common, created_by=12),
    )

    assert replay.id == first.id
    assert second_user.id != first.id
    assert first.admission_scope == "user:11"
    assert second_user.admission_scope == "user:12"
    assert await db_session.scalar(select(func.count(DeliveryRun.id))) == 2


@pytest.mark.asyncio
async def test_idempotent_replay_accepts_pre_frontend_policy_hash(db_session):
    project, repo = await _scope(db_session)
    spec = DeliveryCreateSpec(
        idempotency_key="legacy-delivery-replay",
        project_id=project.id,
        monitored_repo_id=repo.id,
        title="Legacy admission",
        requirements="Return the already-frozen Run after an upgrade.",
        created_by=15,
    )
    first = await create_delivery_run(db_session, spec)

    legacy_request = {
        "schema_version": 1,
        "project_id": project.id,
        "monitored_repo_id": repo.id,
        "title": spec.title,
        "requirements": spec.requirements,
        "source_todo_id": None,
        "base_branch": None,
        "provider": "codex",
        "model": None,
        "codex_service_tier": "default",
        "effort_level": None,
        "timeout_hours": None,
        "max_cycles": 10,
        "max_no_progress": 3,
    }
    legacy_policy = dict(first.policy_snapshot)
    legacy_policy["schema_version"] = 1
    legacy_policy.pop("frontend_review", None)
    legacy_policy.pop("strict_branch_protection", None)
    first.request_hash = value_hash(legacy_request)
    first.policy_snapshot = legacy_policy
    first.policy_hash = value_hash(legacy_policy)
    await db_session.commit()

    replay = await create_delivery_run(db_session, spec)

    assert replay.id == first.id
    assert await db_session.scalar(select(func.count(DeliveryRun.id))) == 1


@pytest.mark.asyncio
async def test_concurrent_same_admission_returns_one_run(tmp_path):
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'delivery-idempotency.db'}",
        connect_args={"timeout": 30},
    )
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        async with factory() as seed:
            project, repo = await _scope(seed)

        spec = DeliveryCreateSpec(
            idempotency_key="concurrent-service-admission",
            project_id=project.id,
            monitored_repo_id=repo.id,
            title="Concurrent admission",
            requirements="Both callers must observe one durable Run.",
        )

        async def admit() -> int:
            async with factory() as session:
                return (await create_delivery_run(session, spec)).id

        first_id, second_id = await asyncio.gather(admit(), admit())

        assert first_id == second_id
        async with factory() as check:
            assert await check.scalar(select(func.count(DeliveryRun.id))) == 1
            assert await check.scalar(select(func.count(Task.id))) == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_create_delivery_run_rejects_remote_worker_scope(db_session):
    project, repo = await _scope(db_session, worker_id=9)

    with pytest.raises(DeliveryUnsupportedScopeError, match="local projects"):
        await create_delivery_run(
            db_session,
            DeliveryCreateSpec(
                idempotency_key="service-remote-scope",
                project_id=project.id,
                monitored_repo_id=repo.id,
                title="Remote",
                requirements="Do not silently downgrade to a normal Task.",
            ),
        )

    assert list((await db_session.execute(select(Task))).scalars()) == []


@pytest.mark.asyncio
async def test_create_delivery_run_freezes_automatic_merge_terminal(db_session):
    project, repo = await _scope(db_session, auto_merge=True)

    run = await create_delivery_run(
        db_session,
        DeliveryCreateSpec(
            idempotency_key="service-auto-merge",
            project_id=project.id,
            monitored_repo_id=repo.id,
            title="Merge after the Gate",
            requirements="Merge only after the exact PR Monitor Gate passes.",
        ),
    )

    assert run.policy_snapshot["auto_merge"] is True
    assert run.policy_snapshot["terminal"] == "merged"


@pytest.mark.asyncio
async def test_explicit_run_merge_choice_overrides_repo_default(db_session):
    project, repo = await _scope(db_session, auto_merge=True)

    run = await create_delivery_run(
        db_session,
        DeliveryCreateSpec(
            idempotency_key="service-explicit-manual-merge",
            project_id=project.id,
            monitored_repo_id=repo.id,
            title="Keep the PR open",
            requirements="Finish all gates but leave merge to a human.",
            auto_merge=False,
        ),
    )

    assert run.policy_snapshot["auto_merge"] is False
    assert run.policy_snapshot["terminal"] == "ready_to_merge"


@pytest.mark.asyncio
async def test_panel_only_monitor_is_valid_for_manual_delivery(db_session):
    project, repo = await _scope(db_session, auto_merge=False)
    repo.wait_for_ci = False
    repo.required_checks = []
    await db_session.commit()

    run = await create_delivery_run(
        db_session,
        DeliveryCreateSpec(
            idempotency_key="service-panel-only",
            project_id=project.id,
            monitored_repo_id=repo.id,
            title="Panel without invented CI",
            requirements="Create a PR and require the independent Panel.",
            auto_merge=False,
        ),
    )

    assert run.policy_snapshot["pr_monitor"]["wait_for_ci"] is False
    assert run.policy_snapshot["pr_monitor"]["required_checks"] == []
    assert run.policy_snapshot["terminal"] == "ready_to_merge"


@pytest.mark.asyncio
async def test_required_frontend_review_fails_closed_without_auth_token(
    db_session,
    monkeypatch,
):
    project, repo = await _scope(db_session)
    monkeypatch.setattr(settings, "auth_token", "")

    with pytest.raises(
        DeliveryUnavailableError,
        match="configured AUTH_TOKEN",
    ):
        await create_delivery_run(
            db_session,
            DeliveryCreateSpec(
                idempotency_key="service-required-frontend-no-auth",
                project_id=project.id,
                monitored_repo_id=repo.id,
                title="Require Browser review",
                requirements="Do not publish without Browser evidence.",
                frontend_review="required",
            ),
        )
    await db_session.rollback()
    assert await db_session.scalar(select(func.count(DeliveryRun.id))) == 0
    assert await db_session.scalar(select(func.count(Task.id))) == 0


@pytest.mark.asyncio
async def test_required_frontend_review_freezes_valid_preview_policy(
    db_session,
    monkeypatch,
    tmp_path,
):
    project, repo = await _scope(db_session)
    workspace = tmp_path / "repo"
    workspace.mkdir()
    (workspace / ".git").mkdir()
    project.local_path = str(workspace)
    project.preview_config = {
        "version": 1,
        "name": "Static preview",
        "setup": [],
        "processes": [
            {
                "name": "web",
                "command": [
                    "{python}",
                    "-m",
                    "http.server",
                    "{preview_port}",
                ],
                "cwd": ".",
            }
        ],
        "url": "http://127.0.0.1:{preview_port}/",
        "health_url": "http://127.0.0.1:{preview_port}/",
    }
    await db_session.commit()
    monkeypatch.setattr(settings, "auth_token", "test-token")

    run = await create_delivery_run(
        db_session,
        DeliveryCreateSpec(
            idempotency_key="service-required-frontend-valid",
            project_id=project.id,
            monitored_repo_id=repo.id,
            title="Require Browser review",
            requirements="Publish only with complete Browser evidence.",
            frontend_review="required",
        ),
    )

    assert run.policy_snapshot["frontend_review"] == {
        "mode": "required",
        "profile": "standard",
        "allow_actions": True,
    }


@pytest.mark.asyncio
async def test_auto_merge_admission_rejects_legacy_status_ci_policy(db_session):
    project, repo = await _scope(db_session, auto_merge=True)
    repo.required_checks = [{
        "kind": "status",
        "name": "tests",
        "app_slug": "ci-bot",
    }]
    await db_session.commit()

    with pytest.raises(
        DeliveryValidationError,
        match="app-bound check_run required CI policies",
    ):
        await create_delivery_run(
            db_session,
            DeliveryCreateSpec(
                idempotency_key="service-status-auto-merge",
                project_id=project.id,
                monitored_repo_id=repo.id,
                title="Reject late auto-merge failure",
                requirements="Reject before any agent work is admitted.",
            ),
        )

    await db_session.rollback()
    assert await db_session.scalar(select(func.count(DeliveryRun.id))) == 0
    assert await db_session.scalar(select(func.count(Task.id))) == 0


@pytest.mark.asyncio
async def test_create_delivery_run_freezes_manual_ready_terminal(db_session):
    project, repo = await _scope(db_session, auto_merge=False)

    run = await create_delivery_run(
        db_session,
        DeliveryCreateSpec(
            idempotency_key="service-manual-merge",
            project_id=project.id,
            monitored_repo_id=repo.id,
            title="Wait for merge",
            requirements="Stop after the exact PR Monitor Gate passes.",
        ),
    )

    assert run.policy_snapshot["auto_merge"] is False
    assert run.policy_snapshot["terminal"] == "ready_to_merge"


@pytest.mark.asyncio
async def test_transition_and_new_cycle_share_one_transaction(db_session):
    project, repo = await _scope(db_session)
    created = await create_delivery_run(
        db_session,
        DeliveryCreateSpec(
            idempotency_key="service-transition-cycle",
            project_id=project.id,
            monitored_repo_id=repo.id,
            title="Loop",
            requirements="Loop after blocking review evidence.",
        ),
    )

    run = await lock_run(db_session, created.id)
    cycle = await lock_current_cycle(db_session, run)
    await apply_run_event(
        db_session,
        run=run,
        event=DeliveryReducerEvent("plan_requested"),
        actor_kind="controller",
    )
    await apply_run_event(
        db_session,
        run=run,
        event=DeliveryReducerEvent("plan_ready"),
        actor_kind="capability",
    )
    await apply_run_event(
        db_session,
        run=run,
        event=DeliveryReducerEvent("code_started"),
        actor_kind="controller",
    )
    await apply_run_event(
        db_session,
        run=run,
        event=DeliveryReducerEvent("code_completed"),
        actor_kind="developer",
    )
    await apply_run_event(
        db_session,
        run=run,
        event=DeliveryReducerEvent("review_requested"),
        actor_kind="controller",
    )
    await apply_run_event(
        db_session,
        run=run,
        event=DeliveryReducerEvent("review_changes_requested"),
        actor_kind="capability",
    )
    complete_cycle(cycle)
    second = await start_next_cycle(
        db_session,
        run=run,
        trigger_kind="pre_review_changes_requested",
        trigger_payload={"review_result_id": 42},
    )
    await db_session.commit()

    refreshed = await db_session.get(type(run), run.id, populate_existing=True)
    first = await db_session.get(DeliveryCycle, cycle.id, populate_existing=True)
    assert refreshed.phase == "planning"
    assert refreshed.activity == "ready"
    assert refreshed.current_cycle_id == second.id
    assert refreshed.cycle_count == 2
    assert first.status == "completed"
    assert first.active_run_id is None
    assert second.active_run_id == run.id
    assert second.trigger_hash
    versions = list(
        (
            await db_session.execute(
                select(DeliveryTransition.state_version)
                .where(DeliveryTransition.run_id == run.id)
                .order_by(DeliveryTransition.state_version)
            )
        ).scalars()
    )
    assert versions == list(range(1, 8))
