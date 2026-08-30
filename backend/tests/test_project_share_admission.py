"""Local Team ACLs stay distinct from legacy cross-CCM sharing."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import func, select

from backend.config import settings
from backend.models.discussion import Discussion
from backend.models.instance import Instance
from backend.models.plan import Plan
from backend.models.plan_agent import (
    PlanAgentRun,
    PlanAgentStep,
)
from backend.models.project import Project
from backend.models.sub_agent import SubAgentSession
from backend.models.task import Task
from backend.models.task_share import ProjectShare
from backend.models.team_share import TeamProjectShare
from backend.models.user import User
from backend.services import task_sharing
from backend.services.container_manager import is_shared_project
from backend.services.instance_manager import (
    InstanceManager,
    SharedProjectAgentLaunchDisabledError,
)
from backend.services.project_share_admission import ProjectShareAdmissionError
from backend.services.plan_runtime_receipt import new_prepared_runtime_receipt


async def _seed_project_task(
    session_factory,
    *,
    status: str = "pending",
    claimed: bool = False,
):
    async with session_factory() as db:
        project = Project(name=f"share-fence-{status}-{claimed}", status="ready")
        instance = Instance(name=f"share-slot-{status}-{claimed}", status="idle")
        target = User(
            email=f"share-target-{status}-{claimed}@example.test",
            name="Share Target",
            password_hash="unused",
            is_active=True,
        )
        db.add_all([project, instance, target])
        await db.flush()
        task = Task(
            title="project share admission",
            status=status,
            project_id=project.id,
            instance_id=instance.id if claimed else None,
            provider="claude",
            incarnation_id="a" * 32,
        )
        db.add(task)
        await db.flush()
        if claimed:
            instance.status = "running"
            instance.current_task_id = task.id
            instance.pid = 991_001
        await db.commit()
        return project.id, task.id, instance.id, target.id


@pytest.mark.asyncio
async def test_team_project_share_does_not_interrupt_active_local_agent(
    client,
    session_factory,
):
    project_id, _task_id, _instance_id, target_id = await _seed_project_task(
        session_factory,
        status="executing",
        claimed=True,
    )

    response = await client.post(
        f"/api/team/projects/{project_id}/share",
        json={"target_type": "user", "target_id": target_id},
    )

    assert response.status_code == 200
    async with session_factory() as db:
        assert await db.scalar(
            select(func.count())
            .select_from(TeamProjectShare)
            .where(TeamProjectShare.project_id == project_id)
        ) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "expected_status"),
    [
        (ProjectShareAdmissionError("share writer fence busy"), 409),
        (ValueError("project deleted during share"), 404),
    ],
)
async def test_team_project_share_maps_authority_fence_failures(
    client,
    session_factory,
    failure,
    expected_status,
):
    project_id, _task_id, _instance_id, target_id = await _seed_project_task(
        session_factory,
    )

    with patch(
        "backend.api.team_sharing.lock_project_share_authority",
        new=AsyncMock(side_effect=failure),
    ):
        response = await client.post(
            f"/api/team/projects/{project_id}/share",
            json={"target_type": "user", "target_id": target_id},
        )

    assert response.status_code == expected_status
    assert "NameError" not in response.text


@pytest.mark.asyncio
@pytest.mark.parametrize("discussion_status", ["active", "closing"])
async def test_team_project_share_does_not_interrupt_discussion_lease(
    client,
    session_factory,
    discussion_status,
):
    project_id, _task_id, _instance_id, target_id = await _seed_project_task(
        session_factory,
    )
    async with session_factory() as db:
        db.add(Discussion(
            title=f"share-blocking-{discussion_status}",
            project_id=project_id,
            status=discussion_status,
        ))
        await db.commit()

    response = await client.post(
        f"/api/team/projects/{project_id}/share",
        json={"target_type": "user", "target_id": target_id},
    )

    assert response.status_code == 200
    async with session_factory() as db:
        assert await db.scalar(
            select(func.count())
            .select_from(TeamProjectShare)
            .where(TeamProjectShare.project_id == project_id)
        ) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("session_status", "runtime_fields"),
    [
        ("sleeping", {}),
        (
            "failed",
            {
                "codex_thread_id": "retained-project-thread",
                "codex_home": "/private/codex-home",
                "codex_cleanup_pending": True,
            },
        ),
    ],
)
async def test_team_project_share_does_not_interrupt_auxiliary_provider_lease(
    client,
    session_factory,
    session_status,
    runtime_fields,
):
    project_id, task_id, _instance_id, target_id = await _seed_project_task(
        session_factory,
    )
    async with session_factory() as db:
        db.add(SubAgentSession(
            task_id=task_id,
            agent_type="monitor",
            source="ccm",
            description="share-blocking monitor",
            provider="codex" if runtime_fields else "claude",
            status=session_status,
            **runtime_fields,
        ))
        await db.commit()

    response = await client.post(
        f"/api/team/projects/{project_id}/share",
        json={"target_type": "user", "target_id": target_id},
    )

    assert response.status_code == 200


async def _seed_first_class_plan(
    session_factory,
    *,
    with_target_task: bool = False,
    task_project_id_override: int | None | object = ...,
    plan_project_id_override: int | None | object = ...,
):
    async with session_factory() as db:
        project = Project(name="plan-share-fence", status="ready")
        instance = Instance(name="plan-share-slot", status="idle")
        target = User(
            email="plan-share-target@example.test",
            name="Plan Share Target",
            password_hash="unused",
            is_active=True,
        )
        db.add_all([project, instance, target])
        await db.flush()
        task = None
        if with_target_task:
            task_project_id = (
                project.id
                if task_project_id_override is ...
                else task_project_id_override
            )
            task = Task(
                title="Plan target",
                status="pending",
                project_id=task_project_id,
                provider="claude",
                incarnation_id="f" * 32,
            )
            db.add(task)
            await db.flush()
        plan_project_id = (
            project.id
            if plan_project_id_override is ...
            else plan_project_id_override
        )
        plan = Plan(
            title="Project share Plan",
            initial_request="Plan safely",
            target_task_id=task.id if task is not None else None,
            project_id=plan_project_id,
            pipeline_config={},
        )
        db.add(plan)
        await db.flush()
        run = PlanAgentRun(
            plan_id=plan.id,
            run_type="standalone",
            status="queued",
            generation=0,
        )
        db.add(run)
        await db.flush()
        plan.active_run_id = run.id
        await db.commit()
        return {
            "project_id": project.id,
            "task_id": task.id if task is not None else None,
            "target_id": target.id,
            "instance_id": instance.id,
            "plan_id": plan.id,
            "run_id": run.id,
        }


@pytest.mark.asyncio
async def test_plan_claim_does_not_veto_team_project_share(
    client,
    session_factory,
):
    seeded = await _seed_first_class_plan(session_factory)
    from backend.services.dispatcher import GlobalDispatcher

    dispatcher = GlobalDispatcher.__new__(GlobalDispatcher)
    dispatcher.db_factory = session_factory
    dispatcher._request_plan_runtime_recovery = MagicMock()
    async with session_factory() as db:
        claim = await dispatcher._claim_plan_run(
            db,
            instance_id=seeded["instance_id"],
        )

    assert claim == (seeded["run_id"], 1)
    response = await client.post(
        f"/api/team/projects/{seeded['project_id']}/share",
        json={"target_type": "user", "target_id": seeded["target_id"]},
    )
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_existing_team_share_allows_plan_claim(
    session_factory,
):
    seeded = await _seed_first_class_plan(session_factory)
    async with session_factory() as db:
        db.add(TeamProjectShare(
            project_id=seeded["project_id"],
            target_type="user",
            target_id=seeded["target_id"],
            shared_by=seeded["target_id"],
        ))
        await db.commit()

    from backend.services.dispatcher import GlobalDispatcher

    dispatcher = GlobalDispatcher.__new__(GlobalDispatcher)
    dispatcher.db_factory = session_factory
    dispatcher._request_plan_runtime_recovery = MagicMock()
    async with session_factory() as db:
        assert await dispatcher._claim_plan_run(
            db,
            instance_id=seeded["instance_id"],
        ) == (seeded["run_id"], 1)

    async with session_factory() as db:
        run = await db.get(PlanAgentRun, seeded["run_id"])
        plan = await db.get(Plan, seeded["plan_id"])
        instance = await db.get(Instance, seeded["instance_id"])
    assert run.status == "running"
    assert run.instance_id == seeded["instance_id"]
    assert plan.active_run_id == seeded["run_id"]
    assert instance.status == "running"
    assert instance.current_plan_run_id == seeded["run_id"]


@pytest.mark.asyncio
async def test_plan_claim_cleanup_settles_before_delivering_cancellation(
    session_factory,
):
    seeded = await _seed_first_class_plan(session_factory)
    from backend.services.dispatcher import GlobalDispatcher

    dispatcher = GlobalDispatcher.__new__(GlobalDispatcher)
    dispatcher.db_factory = session_factory
    dispatcher._request_plan_runtime_recovery = MagicMock()
    original_cleanup = dispatcher._cleanup_plan_run_owner
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()

    async def delayed_cleanup(**kwargs):
        cleanup_started.set()
        await release_cleanup.wait()
        return await original_cleanup(**kwargs)

    dispatcher._cleanup_plan_run_owner = delayed_cleanup
    admission = AsyncMock(
        side_effect=ProjectShareAdmissionError("forced share race")
    )
    async with session_factory() as db:
        with patch(
            "backend.services.project_share_admission."
            "require_unshared_project_plan_claim",
            admission,
        ):
            claim = asyncio.create_task(
                dispatcher._claim_plan_run(
                    db,
                    instance_id=seeded["instance_id"],
                )
            )
            await asyncio.wait_for(cleanup_started.wait(), timeout=5)
            claim.cancel()
            await asyncio.sleep(0)
            assert not claim.done()
            release_cleanup.set()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(claim, timeout=5)

    async with session_factory() as db:
        run = await db.get(PlanAgentRun, seeded["run_id"])
        instance = await db.get(Instance, seeded["instance_id"])
    assert run.status == "failed"
    assert run.instance_id is None
    assert instance.status == "idle"
    assert instance.current_plan_run_id is None


@pytest.mark.asyncio
async def test_terminal_plan_receipt_does_not_veto_team_project_share(
    client,
    session_factory,
):
    seeded = await _seed_first_class_plan(session_factory)
    async with session_factory() as db:
        run = await db.get(PlanAgentRun, seeded["run_id"])
        plan = await db.get(Plan, seeded["plan_id"])
        run.status = "failed"
        run.generation = 1
        plan.active_run_id = None
        step = PlanAgentStep(
            run_id=run.id,
            plan_id=plan.id,
            generation=1,
            step_type="planner",
            provider="claude",
            status="failed",
        )
        db.add(step)
        await db.flush()
        db.add(new_prepared_runtime_receipt(step, attempt_index=1))
        await db.commit()

    response = await client.post(
        f"/api/team/projects/{seeded['project_id']}/share",
        json={"target_type": "user", "target_id": seeded["target_id"]},
    )
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_plan_target_project_drift_does_not_veto_team_share(
    client,
    session_factory,
):
    seeded = await _seed_first_class_plan(
        session_factory,
        with_target_task=True,
        plan_project_id_override=None,
    )

    response = await client.post(
        f"/api/team/projects/{seeded['project_id']}/share",
        json={"target_type": "user", "target_id": seeded["target_id"]},
    )
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_plan_target_project_drift_is_rejected_at_claim_gate(
    session_factory,
):
    seeded = await _seed_first_class_plan(
        session_factory,
        with_target_task=True,
        plan_project_id_override=None,
    )
    from backend.services.dispatcher import GlobalDispatcher

    dispatcher = GlobalDispatcher.__new__(GlobalDispatcher)
    dispatcher.db_factory = session_factory
    dispatcher._request_plan_runtime_recovery = MagicMock()
    async with session_factory() as db:
        assert await dispatcher._claim_plan_run(
            db,
            instance_id=seeded["instance_id"],
        ) is None

    async with session_factory() as db:
        run = await db.get(PlanAgentRun, seeded["run_id"])
        plan = await db.get(Plan, seeded["plan_id"])
        instance = await db.get(Instance, seeded["instance_id"])
    assert run.status == "failed"
    assert "target Task changed Project" in run.error
    assert run.instance_id is None
    assert plan.active_run_id is None
    assert instance.status == "idle"
    assert instance.current_plan_run_id is None


@pytest.mark.asyncio
async def test_projectless_plan_target_claim_is_admitted(session_factory):
    seeded = await _seed_first_class_plan(
        session_factory,
        with_target_task=True,
        task_project_id_override=None,
        plan_project_id_override=None,
    )
    from backend.services.dispatcher import GlobalDispatcher

    dispatcher = GlobalDispatcher.__new__(GlobalDispatcher)
    dispatcher.db_factory = session_factory
    dispatcher._request_plan_runtime_recovery = MagicMock()
    async with session_factory() as db:
        claim = await dispatcher._claim_plan_run(
            db,
            instance_id=seeded["instance_id"],
        )

    assert claim == (seeded["run_id"], 1)
    async with session_factory() as db:
        run = await db.get(PlanAgentRun, seeded["run_id"])
        plan = await db.get(Plan, seeded["plan_id"])
        target = await db.get(Task, seeded["task_id"])
    assert run.status == "running"
    assert plan.project_id is None
    assert target.project_id is None


@pytest.mark.asyncio
async def test_legacy_feishu_share_service_rejects_active_local_agent(
    db_factory,
):
    async with db_factory() as db:
        project = Project(name="legacy-feishu-active-agent", status="ready")
        instance = Instance(name="legacy-feishu-active-slot", status="running")
        db.add_all([project, instance])
        await db.flush()
        task = Task(
            title="legacy Feishu service admission",
            status="executing",
            project_id=project.id,
            instance_id=instance.id,
            provider="claude",
            incarnation_id="e" * 32,
        )
        db.add(task)
        await db.flush()
        instance.current_task_id = task.id
        await db.commit()
        project_id = project.id

    async with db_factory() as db:
        with pytest.raises(ProjectShareAdmissionError, match="local Agent"):
            await task_sharing.share_project(
                db,
                project_id,
                [],
                instance_manager=InstanceManager(db_factory, MagicMock()),
            )
        assert await db.scalar(
            select(func.count())
            .select_from(ProjectShare)
            .where(
                ProjectShare.project_id == project_id,
                ProjectShare.status == "active",
            )
        ) == 0


@pytest.mark.asyncio
async def test_existing_cross_family_share_keeps_recipient_addition_idempotent(
    client,
    session_factory,
):
    project_id, _task_id, _instance_id, target_id = await _seed_project_task(
        session_factory,
        status="executing",
        claimed=True,
    )
    async with session_factory() as db:
        db.add(ProjectShare(
            project_id=project_id,
            shared_to_open_id="ou-existing",
            shared_to_ccm_url="https://existing.example.test",
            status="active",
        ))
        await db.commit()

    response = await client.post(
        f"/api/team/projects/{project_id}/share",
        json={"target_type": "user", "target_id": target_id},
    )

    assert response.status_code == 200
    async with session_factory() as db:
        assert await db.scalar(
            select(func.count())
            .select_from(TeamProjectShare)
            .where(TeamProjectShare.project_id == project_id)
        ) == 1


@pytest.mark.asyncio
async def test_launch_reservation_racing_first_share_is_rejected(db_factory):
    async with db_factory() as db:
        project = Project(name="reservation-share-race", status="ready")
        instance = Instance(name="reservation-share-race-slot", status="idle")
        db.add_all([project, instance])
        await db.flush()
        task = Task(
            title="launch reservation race",
            status="pending",
            project_id=project.id,
            provider="claude",
            incarnation_id="b" * 32,
        )
        db.add(task)
        await db.commit()
        project_id = project.id
        task_id = task.id
        instance_id = instance.id

    manager = InstanceManager(db_factory, MagicMock())
    reservation_ready = asyncio.Event()
    release_launch = asyncio.Event()

    async def hold_launch(**_kwargs):
        reservation_ready.set()
        await release_launch.wait()
        return 991_002

    manager._launch_locked = hold_launch
    launch = asyncio.create_task(
        manager.launch(
            instance_id,
            "hold before provider effect",
            task_id=task_id,
            task_turn_generation=0,
            provider="claude",
        )
    )
    await asyncio.wait_for(reservation_ready.wait(), timeout=5)
    try:
        async with db_factory() as db:
            with pytest.raises(
                ProjectShareAdmissionError,
                match="launch is in progress",
            ):
                await task_sharing.share_project(
                    db,
                    project_id,
                    [],
                    instance_manager=manager,
                )
            await db.rollback()
    finally:
        release_launch.set()
        assert await asyncio.wait_for(launch, timeout=5) == 991_002


@pytest.mark.asyncio
async def test_shared_project_fact_source_includes_only_active_feishu_share(
    db_factory,
):
    async with db_factory() as db:
        active_project = Project(name="active-feishu-share", status="ready")
        revoked_project = Project(name="revoked-feishu-share", status="ready")
        db.add_all([active_project, revoked_project])
        await db.flush()
        db.add_all([
            ProjectShare(
                project_id=active_project.id,
                shared_to_open_id="ou-active",
                shared_to_ccm_url="https://active.example.test",
                status="active",
            ),
            ProjectShare(
                project_id=revoked_project.id,
                shared_to_open_id="ou-revoked",
                shared_to_ccm_url="https://revoked.example.test",
                status="revoked",
            ),
        ])
        await db.commit()
        active_project_id = active_project.id
        revoked_project_id = revoked_project.id

    assert await is_shared_project(active_project_id, db_factory) is True
    assert await is_shared_project(revoked_project_id, db_factory) is False


async def _seed_launch_task_with_optional_feishu_share(
    db_factory,
    *,
    shared: bool,
):
    async with db_factory() as db:
        project = Project(
            name=f"feishu-launch-gate-{shared}",
            status="ready",
        )
        instance = Instance(name=f"feishu-launch-slot-{shared}", status="idle")
        db.add_all([project, instance])
        await db.flush()
        task = Task(
            title="Feishu launch gate",
            status="executing",
            project_id=project.id,
            instance_id=instance.id,
            provider="claude",
            incarnation_id=("c" if shared else "d") * 32,
        )
        db.add(task)
        await db.flush()
        instance.current_task_id = task.id
        if shared:
            db.add(ProjectShare(
                project_id=project.id,
                shared_to_open_id="ou-launch-gate",
                shared_to_ccm_url="https://recipient.example.test",
                status="active",
            ))
        await db.commit()
        return project.id, instance.id, task.id


@pytest.mark.asyncio
async def test_active_feishu_share_vetoes_initial_agent_launch(
    db_factory,
    monkeypatch,
    tmp_path,
):
    _project_id, instance_id, task_id = (
        await _seed_launch_task_with_optional_feishu_share(
            db_factory,
            shared=True,
        )
    )
    monkeypatch.setattr(settings, "use_pty_mode", False)
    manager = InstanceManager(db_factory, MagicMock())
    manager._build_command = MagicMock()
    manager._spawn_managed_direct_process = AsyncMock()

    with pytest.raises(
        SharedProjectAgentLaunchDisabledError,
        match="is shared",
    ):
        await manager.launch(
            instance_id,
            "must not cross initial gate",
            task_id=task_id,
            cwd=str(tmp_path),
            provider="claude",
        )

    manager._build_command.assert_not_called()
    manager._spawn_managed_direct_process.assert_not_awaited()


@pytest.mark.asyncio
async def test_feishu_share_appearing_before_final_callback_vetoes_launch(
    db_factory,
    monkeypatch,
    tmp_path,
):
    project_id, instance_id, task_id = (
        await _seed_launch_task_with_optional_feishu_share(
            db_factory,
            shared=False,
        )
    )
    monkeypatch.setattr(settings, "use_pty_mode", False)
    monkeypatch.setattr(
        "backend.services.task_agent_isolation."
        "validate_claude_task_isolation_settings",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "backend.services.task_ssh_access.task_ssh_protected_paths",
        AsyncMock(return_value=()),
    )
    monkeypatch.setattr(
        "backend.services.task_ssh_access._protected_path_variants",
        lambda *_args, **_kwargs: (),
    )

    manager = InstanceManager(db_factory, MagicMock())
    manager._build_command = MagicMock(return_value=["agent"])
    manager._spawn_managed_direct_process = AsyncMock()
    manager._persist_actual_turn_transport = AsyncMock()
    real_shared_check = is_shared_project
    checks = 0

    async def publish_feishu_share_after_initial_check(checked_id, factory):
        nonlocal checks
        checks += 1
        shared = await real_shared_check(checked_id, factory)
        if checks == 1:
            assert shared is False
            async with factory() as db:
                db.add(ProjectShare(
                    project_id=project_id,
                    shared_to_open_id="ou-final-gate",
                    shared_to_ccm_url="https://recipient.example.test",
                    status="active",
                ))
                await db.commit()
        return shared

    with (
        patch(
            "backend.services.container_manager.is_shared_project",
            side_effect=publish_feishu_share_after_initial_check,
        ),
        patch("backend.services.ask_user_settings.ensure_ask_user_hook"),
        pytest.raises(
            SharedProjectAgentLaunchDisabledError,
            match="is shared",
        ),
    ):
        await manager.launch(
            instance_id,
            "must not cross final gate",
            task_id=task_id,
            cwd=str(tmp_path),
            provider="claude",
        )

    assert checks == 2
    manager._persist_actual_turn_transport.assert_not_awaited()
    manager._spawn_managed_direct_process.assert_not_awaited()
