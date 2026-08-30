import asyncio
import json
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from backend.config import settings
from backend.models.instance import Instance
from backend.models.plan import Plan
from backend.models.plan_agent import (
    PlanAgentRun,
    PlanAgentRuntimeReceipt,
    PlanAgentStep,
)
from backend.models.project import Project
from backend.models.task import Task
from backend.schemas.plan import PlanModelRoute, PlanPipelineConfig
from backend.services.codex_app_server import (
    CodexRequiredMcpPreTurnError,
    CodexTurnProcess,
)
from backend.services.plan_agent_runner import (
    PLANNER_SCHEMA,
    PLANNER_SCHEMA_V2,
    REVIEWER_SCHEMA_V2,
    PlanAgentError,
    PlanAgentOutputRunaway,
    PlanAgentResponseError,
    PlanAgentRunner,
    PlanAgentTimeout,
    PlanRouteUnavailable,
    _CLAUDE_STREAM_READER_LIMIT_BYTES,
    _StructuredJsonWhitespaceGuard,
    _build_command,
    _extract_provider_content,
    _plan_request_with_attachments,
    _repository_instruction_manifest,
    _validate_structured,
    _validate_structured_v2,
    _versioned_planner_prompt,
    _versioned_reference_files,
    _versioned_reviewer_prompt,
)
from backend.services.plan_runtime_receipt import prepare_runtime_attempt
from backend.services.task_queue import TaskQueue
from backend.services.task_runtime_secrets import (
    create_private_runtime_temp_dir,
)


TASK_INCARNATION = "b" * 32


def _plan_runtime_tmp(owner_id: int, *, attempt: int = 1):
    return create_private_runtime_temp_dir(
        runtime_namespace="plan-run",
        owner_id=owner_id,
        generation_components={
            "step": owner_id,
            "run_generation": 1,
            "attempt": attempt,
        },
    )


async def _seed_first_class_provider_boundary(
    db_factory,
    *,
    target_status: str = "pending",
    with_project: bool = False,
):
    async with db_factory() as db:
        project_id = None
        if with_project:
            project = Project(name="provider-boundary-project", status="ready")
            db.add(project)
            await db.flush()
            project_id = project.id
        instance = Instance(name="provider-boundary-instance", status="running")
        db.add(instance)
        await db.flush()
        target = Task(
            title="provider boundary target",
            status=target_status,
            provider="codex",
            incarnation_id="7" * 32,
            project_id=project_id,
        )
        db.add(target)
        await db.flush()
        plan = Plan(
            title="provider boundary Plan",
            initial_request="admit exactly once",
            target_task_id=target.id,
            project_id=project_id,
            pipeline_config={},
        )
        db.add(plan)
        await db.flush()
        run = PlanAgentRun(
            plan_id=plan.id,
            run_type="standalone",
            status="running",
            generation=1,
            instance_id=instance.id,
        )
        db.add(run)
        await db.flush()
        step = PlanAgentStep(
            run_id=run.id,
            plan_id=plan.id,
            generation=1,
            step_type="planner",
            provider="codex",
            status="running",
        )
        db.add(step)
        await db.flush()
        plan.active_run_id = run.id
        instance.current_plan_run_id = run.id
        await db.commit()
        ids = (target.id, plan.id, run.id, step.id, instance.id)
    receipt = await prepare_runtime_attempt(db_factory, ids[3])
    return (*ids, receipt)


def test_claude_plan_command_is_read_only():
    command = _build_command(
        provider="claude",
        model="claude-opus-4-6",
        effort="high",
        schema=PLANNER_SCHEMA,
        isolation_settings_path="/private/runtime/plan-security.json",
    )

    assert command[0] == settings.claude_binary
    assert command[command.index("--output-format") + 1] == "stream-json"
    assert "--verbose" in command
    assert command[command.index("--permission-mode") + 1] == "default"
    assert "--no-session-persistence" in command
    assert "--safe-mode" in command
    assert command[command.index("--tools") + 1] == "Glob,Grep,Read"
    assert command[command.index("--allowedTools") + 1] == "Glob,Grep,Read"
    assert command[command.index("--settings") + 1] == (
        "/private/runtime/plan-security.json"
    )
    assert command[command.index("--setting-sources") + 1] == ""
    assert "Bash" in command[command.index("--disallowed-tools") + 1]
    assert "--dangerously-skip-permissions" not in command


def test_versioned_prompts_do_not_turn_unavailable_repo_facts_into_questions():
    from backend.services.plan_agent_runner import (
        _versioned_planner_prompt,
        _versioned_reviewer_prompt,
    )

    common = {
        "original_request": "Add a status endpoint.",
        "run_type": "initial",
        "planning_request": "Add a status endpoint.",
        "reference_files": "",
        "target_context": "",
        "interaction_history": "",
        "repository_context": '{"changed_since_run_start": false}',
    }
    planner = _versioned_planner_prompt(
        **common,
        base_plan=None,
        current_candidate=None,
        base_review_context="(none)",
        reviewer_feedback=None,
    )
    reviewer = _versioned_reviewer_prompt(
        **common,
        base_plan=None,
        base_review_context="(none)",
        previous_reviewer_feedback=None,
        plan_content="Inspect the existing route conventions.",
    )

    assert "header must be at most 20 characters" in planner
    assert "not user decisions" in planner
    assert "header must be at most 20 characters" in reviewer
    assert "not be converted into a user question" in reviewer
    assert "instruction_manifest" in planner
    assert "manifested symlink" in reviewer
    assert "shortest sufficient" in planner
    assert "byte identity of unrelated" in reviewer


def test_repository_instruction_manifest_records_agents_symlink(tmp_path):
    (tmp_path / "CLAUDE.md").write_text("rules", encoding="utf-8")
    (tmp_path / "AGENTS.md").symlink_to("CLAUDE.md")

    assert _repository_instruction_manifest(str(tmp_path)) == {
        "AGENTS.md": {"kind": "symlink", "target": "CLAUDE.md"},
        "CLAUDE.md": {"kind": "file"},
    }


def test_claude_plan_host_unrestricted_command_keeps_read_only_tools():
    command = _build_command(
        provider="claude",
        model="claude-opus-4-6",
        effort="high",
        schema=PLANNER_SCHEMA,
        isolation_settings_path=None,
    )

    assert "--settings" not in command
    assert command[command.index("--setting-sources") + 1] == ""
    assert command[command.index("--permission-mode") + 1] == "default"
    assert command[command.index("--tools") + 1] == "Glob,Grep,Read"
    assert command[command.index("--allowedTools") + 1] == "Glob,Grep,Read"
    assert "Bash" in command[command.index("--disallowed-tools") + 1]
    assert "--dangerously-skip-permissions" not in command


def test_structured_output_parsers_accept_native_provider_envelopes():
    claude_raw = json.dumps({
        "type": "result",
        "structured_output": {"plan": "Do the work safely"},
    })
    claude_content = _extract_provider_content("claude", claude_raw)
    assert _validate_structured("planner", claude_content) == {
        "plan": "Do the work safely"
    }

    claude_stream_raw = "\n".join([
        json.dumps({"type": "system", "subtype": "init"}),
        json.dumps({
            "type": "assistant",
            "message": {
                "content": [{"type": "tool_use", "name": "Read"}],
            },
        }),
        json.dumps({
            "type": "result",
            "structured_output": {"plan": "Use the streamed result"},
        }),
    ])
    claude_stream_content = _extract_provider_content(
        "claude", claude_stream_raw
    )
    assert _validate_structured("planner", claude_stream_content) == {
        "plan": "Use the streamed result"
    }

    codex_raw = "\n".join([
        json.dumps({"type": "thread.started"}),
        json.dumps({
            "type": "item.completed",
            "item": {
                "type": "agent_message",
                "text": '{"verdict":"approve","feedback":"Looks good"}',
            },
        }),
    ])
    codex_content = _extract_provider_content("codex", codex_raw)
    assert _validate_structured("reviewer", codex_content) == {
        "verdict": "approve",
        "feedback": "Looks good",
    }


def test_interactive_planner_accepts_all_known_questions_without_count_limit():
    questions = [
        {
            "id": f"required_{index}",
            "header": f"Q{index}",
            "question": f"Required decision {index}",
            "response_type": "text",
            "options": [],
            "is_required": index % 2 == 0,
        }
        for index in range(12)
    ]
    payload = {
        "action": "request_input",
        "plan": "",
        "reason": "All decisions materially affect the Plan",
        "questions": questions,
    }
    expected = {
        "action": "request_input",
        "reason": payload["reason"],
        "questions": [
            {
                **{
                    key: value
                    for key, value in question.items()
                    if key != "is_required"
                },
                "required": question["is_required"],
            }
            for question in questions
        ],
    }

    assert PLANNER_SCHEMA_V2["type"] == "object"
    assert REVIEWER_SCHEMA_V2["type"] == "object"

    def assert_portable_schema(schema):
        assert not {
            "oneOf", "allOf", "anyOf", "const", "minLength", "maxLength",
        } & schema.keys()
        properties = schema.get("properties")
        if isinstance(properties, dict):
            assert set(schema["required"]) == set(properties)
            for child in properties.values():
                assert_portable_schema(child)
        items = schema.get("items")
        if isinstance(items, dict):
            assert_portable_schema(items)

    assert_portable_schema(PLANNER_SCHEMA_V2)
    assert_portable_schema(REVIEWER_SCHEMA_V2)
    planner_response = PLANNER_SCHEMA_V2["properties"]["response"]
    reviewer_response = REVIEWER_SCHEMA_V2["properties"]["response"]
    question_schema = planner_response["properties"]["questions"]["items"]
    assert "maxItems" not in planner_response["properties"]["questions"]
    assert "is_required" in question_schema["properties"]
    assert "required" not in question_schema["properties"]
    assert question_schema["required"] == [
        "id", "header", "question", "response_type", "options", "is_required",
    ]
    assert planner_response["required"] == ["action", "plan", "reason", "questions"]
    assert reviewer_response["required"] == [
        "action", "feedback", "reason", "questions",
    ]
    assert _validate_structured_v2("planner", json.dumps(payload)) == expected
    assert _validate_structured_v2(
        "planner",
        json.dumps({"response": payload}),
    ) == expected
    assert _validate_structured_v2(
        "planner",
        json.dumps({
            "response": {
                "action": "propose",
                "plan": "# Safe implementation plan",
                "reason": "",
                "questions": [],
            },
        }),
    ) == {"action": "propose", "plan": "# Safe implementation plan"}
    assert _validate_structured_v2(
        "reviewer",
        json.dumps({
            "response": {
                "action": "approve",
                "feedback": "Ready to implement",
                "reason": "",
                "questions": [],
            },
        }),
    ) == {"action": "approve", "feedback": "Ready to implement"}
    with pytest.raises(ValueError, match="valid reason"):
        _validate_structured_v2(
            "planner",
            json.dumps({"response": {**payload, "reason": ""}}),
        )


def test_versioned_revision_prompts_preserve_original_scope_and_review_closure():
    references = _versioned_reference_files(
        [{"name": "requirements.pdf", "path": "/tmp/requirements.pdf"}],
        [
            {"name": "requirements.pdf", "path": "/tmp/requirements.pdf"},
            {"name": "revision.md", "path": "/tmp/revision.md"},
        ],
    )
    assert references.count("/tmp/requirements.pdf") == 1
    assert "[initial Plan] requirements.pdf" in references
    assert "[current Run] revision.md" in references

    planner_prompt = _versioned_planner_prompt(
        original_request="Implement authentication, caching, and audit logs.",
        run_type="user_revision",
        planning_request="Change only the cache invalidation strategy.",
        reference_files=references,
        target_context="user (initial task): keep every accepted requirement",
        base_plan="# Base\nAuthentication\nCaching\nAudit logs",
        current_candidate="# Draft\nAuthentication\nCaching\nAudit logs",
        base_review_context='{"review_verdict":"exhausted"}',
        reviewer_feedback="Specify cache rollback behavior.",
        interaction_history="(none)",
        repository_context='{"changed_since_run_start":false}',
    )
    assert "Original Plan request (authoritative scope)" in planner_prompt
    assert "authentication, caching, and audit logs" in planner_prompt
    assert "incremental revision" in planner_prompt
    assert "Change only the cache invalidation strategy" in planner_prompt
    assert "# Base" in planner_prompt
    assert "# Draft" in planner_prompt
    assert "Specify cache rollback behavior" in planner_prompt
    assert "do not classify unchanged" in planner_prompt

    reviewer_prompt = _versioned_reviewer_prompt(
        original_request="Implement authentication, caching, and audit logs.",
        run_type="user_revision",
        planning_request="Change only the cache invalidation strategy.",
        reference_files=references,
        target_context="user (initial task): keep every accepted requirement",
        base_plan="# Base\nAuthentication\nCaching\nAudit logs",
        base_review_context='{"review_verdict":"exhausted"}',
        previous_reviewer_feedback="Specify cache rollback behavior.",
        plan_content="# Candidate\nAuthentication\nNew caching\nAudit logs",
        interaction_history="(none)",
        repository_context='{"changed_since_run_start":false}',
    )
    assert "Original Plan request (authoritative scope)" in reviewer_prompt
    assert "Base Plan Version selected for this Run" in reviewer_prompt
    assert "# Base" in reviewer_prompt
    assert "Previous Reviewer feedback to verify" in reviewer_prompt
    assert "Specify cache rollback behavior" in reviewer_prompt
    assert "unrequested removals, regressions, or scope expansion" in reviewer_prompt
    assert "unchanged original requirements" in reviewer_prompt


@pytest.mark.parametrize("wire_required", [None, "true", 1])
def test_interactive_question_wire_contract_rejects_invalid_is_required(
    wire_required,
):
    question = {
        "id": "window",
        "header": "Rollout",
        "question": "Which rollout window should be used?",
        "response_type": "text",
        "options": [],
        "is_required": wire_required,
    }
    payload = {
        "action": "request_input",
        "plan": "",
        "reason": "Choose a rollout window",
        "questions": [question],
    }

    with pytest.raises(ValueError, match="invalid questions"):
        _validate_structured_v2("planner", json.dumps(payload))


def test_interactive_question_wire_contract_requires_is_required():
    payload = {
        "action": "request_input",
        "plan": "",
        "reason": "Choose a rollout window",
        "questions": [{
            "id": "window",
            "header": "Rollout",
            "question": "Which rollout window should be used?",
            "response_type": "text",
            "options": [],
        }],
    }

    with pytest.raises(ValueError, match="invalid questions"):
        _validate_structured_v2("planner", json.dumps(payload))


@pytest.mark.parametrize("include_wire_field", [False, True])
def test_interactive_question_wire_contract_rejects_required_alias(
    include_wire_field,
):
    question = {
        "id": "window",
        "header": "Rollout",
        "question": "Which rollout window should be used?",
        "response_type": "text",
        "options": [],
        "required": True,
    }
    if include_wire_field:
        question["is_required"] = True
    payload = {
        "action": "request_input",
        "plan": "",
        "reason": "Choose a rollout window",
        "questions": [question],
    }

    with pytest.raises(ValueError, match="invalid questions"):
        _validate_structured_v2("planner", json.dumps(payload))


def test_interactive_schema_rejects_inactive_action_fields():
    with pytest.raises(ValueError, match="propose or request_input"):
        _validate_structured_v2(
            "planner",
            json.dumps({
                "action": "propose",
                "plan": "Do the work",
                "reason": "must remain rejected",
                "questions": [],
            }),
        )

    with pytest.raises(ValueError, match="request_input response contains invalid fields"):
        _validate_structured_v2(
            "reviewer",
            json.dumps({
                "action": "request_input",
                "feedback": "must remain rejected",
                "reason": "Choose a rollout window",
                "questions": [{
                    "id": "window",
                    "header": "Rollout",
                    "question": "Which rollout window should be used?",
                    "response_type": "text",
                    "options": [],
                    "is_required": True,
                }],
            }),
        )


def test_plan_request_includes_user_attachment_paths_and_names():
    task = Task(
        description="Review the proposed UI",
        metadata_={
            "file_paths": ["/srv/uploads/mockup.png", "/srv/uploads/notes.txt"],
            "attachments": [
                {"name": "modal mockup.png", "is_image": True},
                {"name": "interaction notes.txt", "is_image": False},
            ],
        },
    )

    request = _plan_request_with_attachments(task)

    assert "Review the proposed UI" in request
    assert "modal mockup.png: /srv/uploads/mockup.png" in request
    assert "interaction notes.txt: /srv/uploads/notes.txt" in request
    assert "untrusted reference data" in request


@pytest.mark.asyncio
async def test_pipeline_rejects_unknown_planner_provider(db_factory):
    runner = PlanAgentRunner(
        db_factory=db_factory,
        instance_manager=MagicMock(),
    )
    task = Task(
        title="Invalid route",
        description="Plan this",
        mode="plan",
        provider="unexpected",
    )

    with pytest.raises(PlanAgentError, match="provider must be"):
        await runner.run(task, cwd="/tmp")


@pytest.mark.asyncio
async def test_first_class_plan_target_project_drift_fails_provider_gate(
    db_factory,
    tmp_path,
):
    async with db_factory() as db:
        project = Project(name="provider-gate-project", status="ready")
        instance = Instance(
            name="provider-gate-instance",
            status="running",
        )
        db.add_all([project, instance])
        await db.flush()
        target = Task(
            title="provider gate target",
            status="pending",
            project_id=project.id,
            provider="codex",
            incarnation_id="9" * 32,
        )
        db.add(target)
        await db.flush()
        plan = Plan(
            title="corrupt target Project",
            initial_request="must fail closed",
            target_task_id=target.id,
            project_id=None,
            pipeline_config={},
        )
        db.add(plan)
        await db.flush()
        run = PlanAgentRun(
            plan_id=plan.id,
            run_type="standalone",
            status="running",
            generation=1,
            instance_id=instance.id,
        )
        db.add(run)
        await db.flush()
        step = PlanAgentStep(
            run_id=run.id,
            plan_id=plan.id,
            generation=1,
            step_type="planner",
            provider="codex",
            status="running",
        )
        db.add(step)
        await db.flush()
        plan.active_run_id = run.id
        instance.current_plan_run_id = run.id
        await db.commit()
        run_id = run.id
        step_id = step.id

    receipt = await prepare_runtime_attempt(db_factory, step_id)
    runner = PlanAgentRunner(
        db_factory=db_factory,
        instance_manager=MagicMock(),
    )

    with pytest.raises(
        PlanAgentError,
        match="target Task changed Project",
    ):
        await runner._prepare_provider_effect_boundary(
            task_id=-run_id,
            provider="codex",
            cwd=str(tmp_path),
            admitted_home="/private/codex-home",
            runtime_receipt=receipt,
        )


@pytest.mark.asyncio
async def test_projectless_plan_target_passes_provider_gate(
    db_factory,
    tmp_path,
):
    async with db_factory() as db:
        instance = Instance(name="projectless-provider-gate", status="running")
        db.add(instance)
        await db.flush()
        target = Task(
            title="projectless target",
            status="pending",
            project_id=None,
            provider="codex",
            incarnation_id="8" * 32,
        )
        db.add(target)
        await db.flush()
        plan = Plan(
            title="projectless Plan",
            initial_request="admit without a Project",
            target_task_id=target.id,
            project_id=None,
            pipeline_config={},
        )
        db.add(plan)
        await db.flush()
        run = PlanAgentRun(
            plan_id=plan.id,
            run_type="standalone",
            status="running",
            generation=1,
            instance_id=instance.id,
        )
        db.add(run)
        await db.flush()
        step = PlanAgentStep(
            run_id=run.id,
            plan_id=plan.id,
            generation=1,
            step_type="planner",
            provider="codex",
            status="running",
        )
        db.add(step)
        await db.flush()
        plan.active_run_id = run.id
        instance.current_plan_run_id = run.id
        await db.commit()
        run_id = run.id
        step_id = step.id

    receipt = await prepare_runtime_attempt(db_factory, step_id)
    runner = PlanAgentRunner(
        db_factory=db_factory,
        instance_manager=MagicMock(),
    )

    boundary = await runner._prepare_provider_effect_boundary(
        task_id=-run_id,
        provider="codex",
        cwd=str(tmp_path),
        admitted_home="/private/codex-home",
        runtime_receipt=receipt,
    )

    assert "/private/codex-home" in boundary[0]
    boundary[-1].cleanup()


@pytest.mark.asyncio
async def test_first_class_provider_gate_locks_target_before_plan_graph(
    db_factory,
    monkeypatch,
    tmp_path,
):
    (
        _target_id,
        _plan_id,
        run_id,
        _step_id,
        _instance_id,
        receipt,
    ) = await _seed_first_class_provider_boundary(
        db_factory,
        with_project=True,
    )
    updates: list[str] = []
    locked_gets: list[str] = []
    original_execute = AsyncSession.execute
    original_get = AsyncSession.get

    async def traced_execute(self, statement, *args, **kwargs):
        table = getattr(statement, "table", None)
        if getattr(statement, "is_update", False) and table is not None:
            updates.append(table.name)
        return await original_execute(self, statement, *args, **kwargs)

    async def traced_get(self, entity, ident, *args, **kwargs):
        if kwargs.get("with_for_update") is True:
            locked_gets.append(entity.__tablename__)
        return await original_get(self, entity, ident, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "execute", traced_execute)
    monkeypatch.setattr(AsyncSession, "get", traced_get)
    runner = PlanAgentRunner(
        db_factory=db_factory,
        instance_manager=MagicMock(),
    )

    boundary = await runner._prepare_provider_effect_boundary(
        task_id=-run_id,
        provider="codex",
        cwd=str(tmp_path),
        admitted_home="/private/codex-home",
        runtime_receipt=receipt,
    )

    assert updates[:3] == ["projects", "tasks", "plan_agent_runs"]
    assert locked_gets[:5] == [
        "plan_agent_runs",
        "plans",
        "plan_agent_steps",
        "plan_agent_runtime_receipts",
        "instances",
    ]
    boundary[-1].cleanup()


@pytest.mark.asyncio
async def test_first_class_provider_gate_fails_closed_on_target_probe_drift(
    db_factory,
    tmp_path,
):
    (
        target_id,
        _plan_id,
        run_id,
        _step_id,
        _instance_id,
        receipt,
    ) = await _seed_first_class_provider_boundary(db_factory)
    opens = 0

    @asynccontextmanager
    async def drift_factory():
        nonlocal opens
        opens += 1
        if opens == 2:
            async with db_factory() as writer:
                await writer.execute(
                    update(Task)
                    .where(Task.id == target_id)
                    .values(worker_id=4815)
                )
                await writer.commit()
        async with db_factory() as db:
            yield db

    runner = PlanAgentRunner(
        db_factory=drift_factory,
        instance_manager=MagicMock(),
    )

    with pytest.raises(PlanAgentError, match="target Task changed"):
        await runner._prepare_provider_effect_boundary(
            task_id=-run_id,
            provider="codex",
            cwd=str(tmp_path),
            admitted_home="/private/codex-home",
            runtime_receipt=receipt,
        )

    async with db_factory() as db:
        current_receipt = await db.get(PlanAgentRuntimeReceipt, receipt.id)
        assert current_receipt is not None
        assert current_receipt.status == "admitting"


@pytest.mark.asyncio
async def test_sqlite_wal_provider_admission_serializes_terminal_task_delete(
    tmp_path,
):
    from backend.database import Base

    task_fenced = asyncio.Event()
    release_admission = asyncio.Event()
    delete_task_update_attempted = asyncio.Event()

    class HeldAdmissionSession(AsyncSession):
        async def execute(self, statement, *args, **kwargs):
            table = getattr(statement, "table", None)
            if (
                getattr(statement, "is_update", False)
                and getattr(table, "name", None) == Task.__tablename__
                and not task_fenced.is_set()
            ):
                result = await super().execute(statement, *args, **kwargs)
                task_fenced.set()
                await release_admission.wait()
                return result
            return await super().execute(statement, *args, **kwargs)

    class ObservedDeleteSession(AsyncSession):
        async def execute(self, statement, *args, **kwargs):
            table = getattr(statement, "table", None)
            if (
                getattr(statement, "is_update", False)
                and getattr(table, "name", None) == Task.__tablename__
                and not delete_task_update_attempted.is_set()
            ):
                delete_task_update_attempted.set()
            return await super().execute(statement, *args, **kwargs)

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'plan-provider-delete-wal.db'}",
        connect_args={"timeout": 2},
    )
    admission = None
    deleting = None
    try:
        async with engine.begin() as connection:
            journal_mode = await connection.exec_driver_sql("PRAGMA journal_mode=WAL")
            assert journal_mode.scalar_one().lower() == "wal"
            await connection.run_sync(Base.metadata.create_all)
        factory = async_sessionmaker(
            engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        admission_factory = async_sessionmaker(
            engine,
            class_=HeldAdmissionSession,
            expire_on_commit=False,
        )
        delete_factory = async_sessionmaker(
            engine,
            class_=ObservedDeleteSession,
            expire_on_commit=False,
        )
        (
            target_id,
            plan_id,
            run_id,
            _step_id,
            _instance_id,
            receipt,
        ) = await _seed_first_class_provider_boundary(
            factory,
            target_status="completed",
        )
        runner = PlanAgentRunner(
            db_factory=admission_factory,
            instance_manager=MagicMock(),
        )
        admission = asyncio.create_task(
            runner._prepare_provider_effect_boundary(
                task_id=-run_id,
                provider="codex",
                cwd=str(tmp_path),
                admitted_home="/private/codex-home",
                runtime_receipt=receipt,
            )
        )
        await asyncio.wait_for(task_fenced.wait(), timeout=1)

        async def delete_target() -> bool:
            async with delete_factory() as db:
                return await TaskQueue(db).delete(target_id)

        deleting = asyncio.create_task(delete_target())
        await asyncio.wait_for(delete_task_update_attempted.wait(), timeout=1)
        release_admission.set()
        boundary, deleted = await asyncio.wait_for(
            asyncio.gather(admission, deleting),
            timeout=3,
        )

        assert deleted is False
        boundary[-1].cleanup()
        async with factory() as db:
            target = await db.get(Task, target_id)
            plan = await db.get(Plan, plan_id)
            run = await db.get(PlanAgentRun, run_id)
            current_receipt = await db.get(PlanAgentRuntimeReceipt, receipt.id)
            assert target is not None and target.incarnation_id == "7" * 32
            assert plan is not None and plan.active_run_id == run_id
            assert run is not None and run.generation == receipt.run_generation
            assert current_receipt is not None
            assert current_receipt.runtime_token == receipt.runtime_token
            assert current_receipt.status == "admitting"
    finally:
        release_admission.set()
        for operation in (admission, deleting):
            if operation is not None and not operation.done():
                operation.cancel()
                await asyncio.gather(operation, return_exceptions=True)
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("app_server_enabled", "admitted_home", "message"),
    [
        (False, "/private/codex-home", "transport is disabled"),
        (True, None, "explicit CODEX_HOME"),
    ],
)
async def test_codex_plan_preflight_failure_cleans_private_tmpdir(
    db_factory,
    monkeypatch,
    app_server_enabled,
    admitted_home,
    message,
):
    runtime_temp_dir = _plan_runtime_tmp(706)
    runner = PlanAgentRunner(
        db_factory=db_factory,
        instance_manager=MagicMock(),
    )
    runner._prepare_provider_effect_boundary = AsyncMock(return_value=(
        (),
        (),
        (),
        runtime_temp_dir,
    ))

    @asynccontextmanager
    async def runtime_admission(**_kwargs):
        yield admitted_home, None

    runner._runtime_admission = runtime_admission
    monkeypatch.setattr(
        settings,
        "codex_app_server_enabled",
        app_server_enabled,
    )

    with pytest.raises(PlanRouteUnavailable, match=message):
        await runner._run_process_attempt(
            task_id=706,
            provider="codex",
            model="gpt-5.6-sol",
            effort="medium",
            cwd="/tmp",
            prompt="must not run",
            schema=PLANNER_SCHEMA,
            timeout=2,
            home=admitted_home,
            step_id=706,
            step_type="planner",
            runtime_receipt=None,
        )

    assert runtime_temp_dir.cleaned is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("configured_provider", "primary_provider", "fallback_provider"),
    [
        ("claude", "codex", "claude"),
        ("codex", "claude", "codex"),
    ],
)
async def test_stage_skips_unconfigured_provider_and_uses_same_provider_fallback(
    db_factory,
    monkeypatch,
    configured_provider,
    primary_provider,
    fallback_provider,
):
    monkeypatch.setattr(settings, "provider_options", configured_provider)
    pipeline = PlanPipelineConfig.model_validate({
        "version": 1,
        "planner": {
            "primary": {
                "provider": "claude",
                "model": "claude-fable-5",
                "effort": "high",
            },
            "fallback": {
                "provider": "codex",
                "model": "gpt-5.6-terra",
                "effort": "xhigh",
            },
        },
        "reviewer": {
            "enabled": True,
            "primary": {
                "provider": primary_provider,
                "model": (
                    "gpt-5.6-sol"
                    if primary_provider == "codex"
                    else "claude-sonnet-5"
                ),
                "effort": "high",
            },
            "fallback": {
                "provider": fallback_provider,
                "model": (
                    "gpt-5.6-terra"
                    if fallback_provider == "codex"
                    else "claude-sonnet-5"
                ),
                "effort": "high",
            },
        },
        "max_revision_cycles": 0,
    })
    task = Task(
        id=707 if configured_provider == "claude" else 708,
        title=f"{configured_provider}-only review",
        description="review without the other provider",
        mode="plan",
    )
    runner = PlanAgentRunner(
        db_factory=db_factory,
        instance_manager=MagicMock(),
    )
    run_id = await runner._create_run(task=task, pipeline=pipeline)
    runner._run_process = AsyncMock(return_value=(
        {"action": "approve", "feedback": ""},
        '{"action":"approve","feedback":""}',
    ))

    result, _raw, route, route_slot, account_id = await runner._run_stage(
        run_id=run_id,
        task_id=task.id,
        step_type="reviewer",
        round_number=1,
        routes=pipeline.reviewer,
        cwd="/tmp",
        prompt="review",
        schema=REVIEWER_SCHEMA_V2,
        timeout=30,
    )

    assert result == {"action": "approve", "feedback": ""}
    assert route.provider == configured_provider
    assert route_slot == "fallback"
    assert account_id == "__default__"
    runner._run_process.assert_awaited_once()
    assert runner._run_process.await_args.kwargs["provider"] == configured_provider
    async with db_factory() as db:
        steps = list(
            (
                await db.execute(
                    select(PlanAgentStep)
                    .where(PlanAgentStep.run_id == run_id)
                    .order_by(PlanAgentStep.id)
                )
            ).scalars()
        )
    assert [step.route_slot for step in steps] == ["primary", "fallback"]
    assert [step.status for step in steps] == ["failed", "completed"]
    assert "not configured" in steps[0].error


@pytest.mark.asyncio
async def test_codex_pre_turn_startup_failure_is_route_unavailable(
    db_factory,
    monkeypatch,
):
    runtime_temp_dir = _plan_runtime_tmp(709)
    runner = PlanAgentRunner(
        db_factory=db_factory,
        instance_manager=MagicMock(),
    )
    runner._prepare_provider_effect_boundary = AsyncMock(return_value=(
        (),
        (),
        (),
        runtime_temp_dir,
    ))

    @asynccontextmanager
    async def runtime_admission(**_kwargs):
        yield "/private/codex-home", None

    async def fail_before_turn(**kwargs):
        kwargs["runtime_temp_dir"].cleanup_if_unbound()
        raise CodexRequiredMcpPreTurnError(
            "Codex app-server could not start required task context: "
            "[Errno 2] No such file or directory: 'codex'"
        )

    runner._runtime_admission = runtime_admission
    runner._run_codex_turn = AsyncMock(side_effect=fail_before_turn)
    monkeypatch.setattr(settings, "codex_app_server_enabled", True)

    with pytest.raises(
        PlanRouteUnavailable,
        match="pre-turn route is unavailable",
    ):
        await runner._run_process_attempt(
            task_id=709,
            provider="codex",
            model="gpt-5.6-sol",
            effort="high",
            cwd="/tmp",
            prompt="review",
            schema=REVIEWER_SCHEMA_V2,
            timeout=30,
            home="/private/codex-home",
            step_id=709,
            step_type="reviewer",
            runtime_receipt=None,
        )

    assert runtime_temp_dir.cleaned is True


@pytest.mark.asyncio
async def test_claude_missing_binary_before_spawn_is_route_unavailable(
    db_factory,
    monkeypatch,
    tmp_path,
):
    runtime_temp_dir = _plan_runtime_tmp(710)
    runner = PlanAgentRunner(
        db_factory=db_factory,
        instance_manager=MagicMock(),
    )
    runner._prepare_provider_effect_boundary = AsyncMock(return_value=(
        (),
        (),
        (),
        runtime_temp_dir,
    ))

    @asynccontextmanager
    async def runtime_admission(**_kwargs):
        yield None, None

    async def missing_binary(*_args, **kwargs):
        assert kwargs["limit"] == _CLAUDE_STREAM_READER_LIMIT_BYTES
        raise FileNotFoundError("No such file or directory: 'claude'")

    runner._runtime_admission = runtime_admission
    monkeypatch.setattr(
        "backend.services.task_agent_isolation."
        "generate_claude_read_only_isolation_settings",
        lambda *_args, **_kwargs: tmp_path / "plan-security.json",
    )
    monkeypatch.setattr(
        "backend.services.task_agent_isolation."
        "validate_claude_task_isolation_settings",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "backend.services.plan_agent_runner._settle_spawn",
        missing_binary,
    )

    with pytest.raises(
        PlanRouteUnavailable,
        match="became unavailable before process admission",
    ):
        await runner._run_process_attempt(
            task_id=710,
            provider="claude",
            model="claude-sonnet-5",
            effort="high",
            cwd="/tmp",
            prompt="review",
            schema=REVIEWER_SCHEMA_V2,
            timeout=30,
            home=None,
            step_id=710,
            step_type="reviewer",
            runtime_receipt=None,
        )

    assert runtime_temp_dir.cleaned is True


@pytest.mark.asyncio
async def test_claude_plan_projects_api_account_auth_into_process(
    db_factory,
    monkeypatch,
):
    runtime_temp_dir = _plan_runtime_tmp(712)
    instance_manager = MagicMock()
    cloudrouter_store = MagicMock()
    runner = PlanAgentRunner(
        db_factory=db_factory,
        instance_manager=instance_manager,
        cloudrouter_store=cloudrouter_store,
    )
    runner._prepare_provider_effect_boundary = AsyncMock(return_value=(
        (),
        (),
        (),
        runtime_temp_dir,
    ))

    @asynccontextmanager
    async def runtime_admission(**_kwargs):
        yield "/private/claude-api-home", True

    captured_env = None

    async def missing_binary(*_command, **kwargs):
        nonlocal captured_env
        captured_env = kwargs["env"]
        raise FileNotFoundError("No such file or directory: 'claude'")

    def inject_auth(environment, store, config_dir):
        assert store is cloudrouter_store
        assert config_dir == "/private/claude-api-home"
        environment["ANTHROPIC_API_KEY"] = "projected-secret"
        environment["ANTHROPIC_BASE_URL"] = "https://api.example.invalid"
        return True

    runner._runtime_admission = runtime_admission
    monkeypatch.setattr(
        "backend.services.claude_auth_projection."
        "inject_cloudrouter_claude_direct_auth",
        inject_auth,
    )
    monkeypatch.setattr(
        "backend.services.plan_agent_runner._settle_spawn",
        missing_binary,
    )

    with pytest.raises(
        PlanRouteUnavailable,
        match="became unavailable before process admission",
    ):
        await runner._run_process_attempt(
            task_id=712,
            provider="claude",
            model="claude-sonnet-5",
            effort="high",
            cwd="/tmp",
            prompt="review",
            schema=REVIEWER_SCHEMA_V2,
            timeout=30,
            home="/private/claude-api-home",
            step_id=712,
            step_type="reviewer",
            runtime_receipt=None,
        )

    assert captured_env is not None
    assert captured_env["CLAUDE_CONFIG_DIR"] == "/private/claude-api-home"
    assert captured_env["ANTHROPIC_API_KEY"] == "projected-secret"
    assert captured_env["ANTHROPIC_BASE_URL"] == "https://api.example.invalid"
    assert captured_env["CLAUDE_CODE_SUBPROCESS_ENV_SCRUB"] == "1"
    assert runtime_temp_dir.cleaned is True


@pytest.mark.asyncio
async def test_cancelled_pipeline_marks_active_step_cancelled(db_factory):
    pipeline = PlanPipelineConfig.model_validate({
        "version": 1,
        "planner": {
            "primary": {
                "provider": "claude",
                "model": "claude-fable-5",
                "effort": "high",
            },
            "fallback": {
                "provider": "codex",
                "model": "gpt-5.6-sol",
                "effort": "xhigh",
            },
        },
        "reviewer": {
            "enabled": False,
            "primary": {
                "provider": "codex",
                "model": "gpt-5.6-sol",
                "effort": "xhigh",
            },
            "fallback": {
                "provider": "claude",
                "model": "claude-fable-5",
                "effort": "high",
            },
        },
        "max_revision_cycles": 0,
    })
    async with db_factory() as db:
        task = Task(
            title="Cancelled Plan",
            description="Stop this Plan",
            target_repo="/tmp",
            mode="plan",
            provider="claude",
            model="claude-fable-5",
            plan_pipeline_config=pipeline.model_dump(mode="json"),
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    runner = PlanAgentRunner(
        db_factory=db_factory,
        instance_manager=MagicMock(),
    )
    runner._run_route = AsyncMock(side_effect=asyncio.CancelledError())

    async with db_factory() as db:
        task = await db.get(Task, task_id)
    with pytest.raises(asyncio.CancelledError):
        await runner.run(task, cwd="/tmp")

    async with db_factory() as db:
        run = (
            await db.execute(
                select(PlanAgentRun).where(
                    PlanAgentRun.plan_task_id == task_id
                )
            )
        ).scalar_one()
        step = (
            await db.execute(
                select(PlanAgentStep).where(PlanAgentStep.run_id == run.id)
            )
        ).scalar_one()
    assert run.status == "cancelled"
    assert run.error == "Plan pipeline cancelled"
    assert step.status == "cancelled"
    assert step.error == "Plan step cancelled"


@pytest.mark.asyncio
async def test_codex_plan_uses_disposable_read_only_app_server_thread(
    db_factory,
):
    calls: list[str | None] = []
    deleted: list[tuple[str, str]] = []
    interrupted = AsyncMock()
    process = CodexTurnProcess(
        123,
        interrupted,
        thread_id="plan-thread",
    )
    process.feed({
        "type": "item.completed",
        "item": {
            "type": "agent_message",
            "text": '{"plan":"safe plan"}',
        },
    })
    process.finish(0)

    registry = MagicMock()
    registry.start_turn = AsyncMock(return_value=(process, "plan-thread"))

    async def delete_thread(home, thread_id):
        deleted.append((home, thread_id))

    registry.delete_thread = delete_thread

    class Manager:
        @asynccontextmanager
        async def codex_home_app_server_guard(self, home):
            calls.append(home)
            yield home

        def _ensure_codex_app_server_registry(self):
            return registry

    runner = PlanAgentRunner(
        db_factory=db_factory,
        instance_manager=Manager(),
    )

    stdout, stderr, returncode = await runner._run_codex_turn(
        task_id=7,
        home="/canonical/default-codex-home",
        model="gpt-5.6-sol",
        effort="xhigh",
        cwd="/tmp",
        prompt="plan safely",
        schema=PLANNER_SCHEMA,
        timeout=10,
        runtime_temp_dir=_plan_runtime_tmp(7),
    )

    assert returncode == 0
    assert stderr == b""
    assert b"safe plan" in stdout
    assert calls == [
        "/canonical/default-codex-home",
        "/canonical/default-codex-home",
    ]
    assert deleted == [
        ("/canonical/default-codex-home", "plan-thread")
    ]
    kwargs = registry.start_turn.await_args.kwargs
    assert kwargs["sandbox_mode"] == "read-only"
    assert kwargs["disable_project_config"] is True
    assert kwargs["disable_user_mcp"] is True
    assert kwargs["disable_autonomous_features"] is True
    assert kwargs["task_ssh_disable_network"] is True
    assert kwargs["task_git_read_paths"] == ()
    assert kwargs["task_git_boundary_fingerprint"] == ()
    assert kwargs["task_private_tmpdir"].cleaned is True
    assert kwargs["output_schema"] == PLANNER_SCHEMA
    assert kwargs["resume_session_id"] is None


@pytest.mark.asyncio
async def test_codex_plan_cleans_private_tmpdir_when_thread_admission_fails(
    db_factory,
):
    captured_tmpdir = None

    async def fail_start(**kwargs):
        nonlocal captured_tmpdir
        captured_tmpdir = kwargs["task_private_tmpdir"]
        raise RuntimeError("thread admission failed")

    registry = MagicMock()
    registry.start_turn = AsyncMock(side_effect=fail_start)

    class Manager:
        @asynccontextmanager
        async def codex_home_app_server_guard(self, home):
            yield home

        def _ensure_codex_app_server_registry(self):
            return registry

    runner = PlanAgentRunner(
        db_factory=db_factory,
        instance_manager=Manager(),
    )

    with pytest.raises(RuntimeError, match="thread admission failed"):
        await runner._run_codex_turn(
            task_id=704,
            home="/canonical/default-codex-home",
            model="gpt-5.6-sol",
            effort="medium",
            cwd="/tmp",
            prompt="must not run",
            schema=PLANNER_SCHEMA,
            timeout=2,
            runtime_temp_dir=_plan_runtime_tmp(704),
        )

    assert captured_tmpdir is not None
    assert captured_tmpdir.cleaned is True


def test_structured_json_whitespace_guard_ignores_string_content_and_escapes():
    leading_guard = _StructuredJsonWhitespaceGuard(limit=4)
    assert not leading_guard.feed(" \n")
    assert leading_guard.feed("\t ")

    guard = _StructuredJsonWhitespaceGuard(limit=8)

    assert not guard.feed('{"plan":"long        markdown\\')
    assert not guard.feed('" still inside        string",')
    assert guard.consecutive == 0
    assert not guard.feed(" \r\n   ")
    assert guard.consecutive == 6
    assert guard.feed("  ")
    assert guard.maximum == 8


@pytest.mark.asyncio
async def test_codex_plan_json_whitespace_runaway_is_interrupted_and_cleaned(
    db_factory,
):
    process: CodexTurnProcess | None = None
    interrupted = asyncio.Event()

    async def interrupt():
        interrupted.set()
        assert process is not None
        process.finish(130)

    process = CodexTurnProcess(123, interrupt, thread_id="runaway-plan")
    process.feed({
        "type": "item.agent_message.delta",
        "delta": '{"response":{"action":"request_input",',
    })
    process.feed({
        "type": "item.agent_message.delta",
        "delta": " \r    \r    \r    ",
    })

    registry = MagicMock()
    registry.start_turn = AsyncMock(
        return_value=(process, "runaway-plan")
    )
    registry.delete_thread = AsyncMock()

    class Manager:
        @asynccontextmanager
        async def codex_home_app_server_guard(self, home):
            yield home

        def _ensure_codex_app_server_registry(self):
            return registry

    runner = PlanAgentRunner(
        db_factory=db_factory,
        instance_manager=Manager(),
    )

    with pytest.raises(
        PlanAgentOutputRunaway,
        match=(
            r"16 consecutive JSON whitespace characters outside a string .*"
            r"limit=16"
        ),
    ):
        await runner._run_codex_turn(
            task_id=703,
            home="/canonical/default-codex-home",
            model="gpt-5.6-sol",
            effort="medium",
            cwd="/tmp",
            prompt="plan safely",
            schema=PLANNER_SCHEMA_V2,
            timeout=2,
            json_whitespace_limit=16,
            runtime_temp_dir=_plan_runtime_tmp(703),
        )

    assert interrupted.is_set()
    registry.delete_thread.assert_awaited_once_with(
        "/canonical/default-codex-home",
        "runaway-plan",
    )


@pytest.mark.asyncio
async def test_codex_reviewer_delta_stall_is_persisted_and_cleaned(
    db_factory,
):
    async with db_factory() as db:
        step = PlanAgentStep(
            run_id=701,
            step_type="reviewer",
            round=1,
            provider="codex",
            model="gpt-5.6-sol",
            effort="xhigh",
            route_slot="primary",
            status="running",
        )
        db.add(step)
        await db.commit()
        await db.refresh(step)
        step_id = step.id

    process: CodexTurnProcess | None = None

    async def interrupt():
        assert process is not None
        process.finish(130)

    process = CodexTurnProcess(123, interrupt, thread_id="stalled-review")
    process.feed({"type": "item.reasoning.delta", "delta": "分析"})
    process.feed({"type": "item.agent_message.delta", "delta": "{"})

    registry = MagicMock()
    registry.start_turn = AsyncMock(
        return_value=(process, "stalled-review")
    )
    registry.delete_thread = AsyncMock()

    class Manager:
        @asynccontextmanager
        async def codex_home_app_server_guard(self, home):
            yield home

        def _ensure_codex_app_server_registry(self):
            return registry

    runner = PlanAgentRunner(
        db_factory=db_factory,
        instance_manager=Manager(),
    )

    with pytest.raises(
        PlanAgentTimeout,
        match=(
            r"stream stalled after 0\.05s without a delta .*"
            r"streamed_output_chars=3.*"
            r"last_event_type=item\.agent_message\.delta"
        ),
    ):
        await runner._run_codex_turn(
            task_id=701,
            home="/canonical/default-codex-home",
            model="gpt-5.6-sol",
            effort="xhigh",
            cwd="/tmp",
            prompt="review safely",
            schema=REVIEWER_SCHEMA_V2,
            timeout=2,
            step_id=step_id,
            delta_idle_timeout=0.05,
            runtime_temp_dir=_plan_runtime_tmp(701),
        )

    registry.delete_thread.assert_awaited_once_with(
        "/canonical/default-codex-home",
        "stalled-review",
    )
    async with db_factory() as db:
        persisted = await db.get(PlanAgentStep, step_id)
    assert persisted.last_delta_at is not None
    assert persisted.streamed_output_chars == 3
    assert persisted.last_event_type == "item.agent_message.delta"


@pytest.mark.asyncio
async def test_codex_delta_idle_watchdog_allows_long_initial_reasoning(
    db_factory,
):
    process: CodexTurnProcess | None = None

    async def interrupt():
        assert process is not None
        process.finish(130)

    process = CodexTurnProcess(123, interrupt, thread_id="slow-first-delta")

    async def complete_after_initial_reasoning():
        await asyncio.sleep(0.08)
        assert process is not None
        process.feed({"type": "item.agent_message.delta", "delta": "{}"})
        process.feed({
            "type": "item.completed",
            "item": {"type": "agent_message", "text": "{}"},
        })
        process.finish(0)

    completion = asyncio.create_task(complete_after_initial_reasoning())
    registry = MagicMock()
    registry.start_turn = AsyncMock(
        return_value=(process, "slow-first-delta")
    )
    registry.delete_thread = AsyncMock()

    class Manager:
        @asynccontextmanager
        async def codex_home_app_server_guard(self, home):
            yield home

        def _ensure_codex_app_server_registry(self):
            return registry

    runner = PlanAgentRunner(
        db_factory=db_factory,
        instance_manager=Manager(),
    )
    stdout, _stderr, returncode = await runner._run_codex_turn(
        task_id=702,
        home="/canonical/default-codex-home",
        model="gpt-5.6-sol",
        effort="xhigh",
        cwd="/tmp",
        prompt="review safely",
        schema=REVIEWER_SCHEMA_V2,
        timeout=2,
        delta_idle_timeout=0.05,
        runtime_temp_dir=_plan_runtime_tmp(702),
    )
    await completion

    assert returncode == 0
    assert b'item.completed' in stdout


def test_retained_plan_agent_is_exposed_as_update_blocker(monkeypatch):
    from backend.services.dispatcher import GlobalDispatcher

    dispatcher = MagicMock(spec=GlobalDispatcher)
    dispatcher._active_auxiliary_session_ids.return_value = (set(), set())
    dispatcher._monitor_processes = {}
    dispatcher._monitor_turn_handles = {}
    dispatcher._monitor_active_turns = set()
    monkeypatch.setattr(
        "backend.services.plan_agent_runner.active_plan_agent_task_ids",
        lambda: {42},
    )

    blockers = GlobalDispatcher.active_auxiliary_blockers(dispatcher)

    assert blockers == [{
        "id": 42,
        "title": "Plan Agent Task #42",
        "status": "running_auxiliary",
        "kind": "plan_agent",
    }]


@pytest.mark.asyncio
async def test_pipeline_revises_then_persists_audited_approval(
    db_factory,
):
    pipeline = PlanPipelineConfig.model_validate({
        "version": 1,
        "planner": {
            "primary": {
                "provider": "claude",
                "model": "claude-fable-5",
                "effort": "high",
            },
            "fallback": {
                "provider": "codex",
                "model": "gpt-5.6-terra",
                "effort": "xhigh",
            },
        },
        "reviewer": {
            "enabled": True,
            "primary": {
                "provider": "codex",
                "model": "gpt-5.6-sol",
                "effort": "xhigh",
            },
            "fallback": {
                "provider": "claude",
                "model": "claude-sonnet-5",
                "effort": "high",
            },
        },
        "max_revision_cycles": 2,
    })

    async with db_factory() as db:
        task = Task(
            title="Plan",
            description="Design the change",
            target_repo="/tmp",
            mode="plan",
            provider="claude",
            model="claude-fable-5",
            effort_level="high",
            plan_pipeline_config=pipeline.model_dump(mode="json"),
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    runner = PlanAgentRunner(
        db_factory=db_factory,
        instance_manager=MagicMock(),
        broadcaster=broadcaster,
    )
    runner._run_route = AsyncMock(side_effect=[
        ({"plan": "Plan v1"}, '{"plan":"Plan v1"}', "claude-1"),
        (
            {"verdict": "revise", "feedback": "Add rollback"},
            '{"verdict":"revise","feedback":"Add rollback"}',
            "codex-1",
        ),
        (
            {"plan": "Plan v2 with rollback"},
            '{"plan":"Plan v2 with rollback"}',
            "claude-1",
        ),
        (
            {"verdict": "approve", "feedback": "Complete"},
            '{"verdict":"approve","feedback":"Complete"}',
            "codex-1",
        ),
    ])

    async with db_factory() as db:
        task = await db.get(Task, task_id)
    result = await runner.run(task, cwd="/tmp")

    assert result.plan_content == "Plan v2 with rollback"
    assert result.verdict == "approve"
    assert result.review_exhausted is False
    assert runner._run_route.await_count == 4
    second_planner_prompt = (
        runner._run_route.await_args_list[2].kwargs["prompt"]
    )
    assert "Add rollback" in second_planner_prompt

    async with db_factory() as db:
        run = (
            await db.execute(
                select(PlanAgentRun).where(
                    PlanAgentRun.plan_task_id == task_id
                )
            )
        ).scalar_one()
        steps = list(
            (
                await db.execute(
                    select(PlanAgentStep)
                    .where(PlanAgentStep.run_id == run.id)
                    .order_by(PlanAgentStep.id)
                )
            ).scalars().all()
        )
        task_state = await db.get(Task, task_id)
    assert run.status == "completed"
    assert run.round == 2
    assert task_state.plan_stage == "completed"
    assert task_state.plan_stage_round == 2
    assert task_state.plan_stage_provider == "codex"
    assert task_state.plan_stage_model == "gpt-5.6-sol"
    assert task_state.plan_stage_effort == "xhigh"
    assert task_state.plan_stage_route_slot == "primary"
    assert run.review_verdict == "approve"
    assert [step.step_type for step in steps] == [
        "planner",
        "reviewer",
        "planner",
        "reviewer",
    ]
    assert all(step.status == "completed" for step in steps)
    assert [step.route_slot for step in steps] == ["primary"] * 4
    assert [step.provider for step in steps] == [
        "claude",
        "codex",
        "claude",
        "codex",
    ]
    assert run.pipeline_config == pipeline.model_dump(mode="json")
    stage_events = [
        call.args[1]
        for call in broadcaster.broadcast.await_args_list
        if call.args[1]["event"] == "plan_stage_change"
    ]
    assert [
        (event["plan_stage"], event["plan_stage_round"])
        for event in stage_events
    ] == [
        ("planning", 1),
        ("reviewing", 1),
        ("planning", 2),
        ("reviewing", 2),
        ("completed", 2),
    ]
    assert [
        (
            event.get("plan_stage_provider"),
            event.get("plan_stage_model"),
            event.get("plan_stage_route_slot"),
        )
        for event in stage_events[:-1]
    ] == [
        ("claude", "claude-fable-5", "primary"),
        ("codex", "gpt-5.6-sol", "primary"),
        ("claude", "claude-fable-5", "primary"),
        ("codex", "gpt-5.6-sol", "primary"),
    ]


@pytest.mark.asyncio
async def test_maximum_two_rounds_never_starts_a_third_planner(db_factory):
    pipeline = PlanPipelineConfig.model_validate({
        "version": 1,
        "planner": {
            "primary": {
                "provider": "claude",
                "model": "claude-fable-5",
                "effort": "high",
            },
            "fallback": {
                "provider": "codex",
                "model": "gpt-5.6-terra",
                "effort": "xhigh",
            },
        },
        "reviewer": {
            "enabled": True,
            "primary": {
                "provider": "codex",
                "model": "gpt-5.6-sol",
                "effort": "xhigh",
            },
            "fallback": {
                "provider": "claude",
                "model": "claude-sonnet-5",
                "effort": "high",
            },
        },
        "max_revision_cycles": 2,
    })
    async with db_factory() as db:
        task = Task(
            title="Bounded Plan",
            description="Plan twice only",
            target_repo="/tmp",
            mode="plan",
            provider="claude",
            plan_pipeline_config=pipeline.model_dump(mode="json"),
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    runner = PlanAgentRunner(
        db_factory=db_factory,
        instance_manager=MagicMock(),
    )
    runner._run_route = AsyncMock(side_effect=[
        ({"plan": "Plan v1"}, '{"plan":"Plan v1"}', "claude-1"),
        (
            {"verdict": "revise", "feedback": "Revise once"},
            '{"verdict":"revise","feedback":"Revise once"}',
            "codex-1",
        ),
        ({"plan": "Plan v2"}, '{"plan":"Plan v2"}', "claude-1"),
        (
            {"verdict": "revise", "feedback": "Still revise"},
            '{"verdict":"revise","feedback":"Still revise"}',
            "codex-1",
        ),
    ])

    async with db_factory() as db:
        task = await db.get(Task, task_id)
    result = await runner.run(task, cwd="/tmp")

    assert result.plan_content == "Plan v2"
    assert result.review_exhausted is True
    assert runner._run_route.await_count == 4


@pytest.mark.asyncio
async def test_stage_uses_fallback_only_after_primary_route_is_unavailable(
    db_factory,
):
    pipeline = PlanPipelineConfig.model_validate({
        "version": 1,
        "planner": {
            "primary": {
                "provider": "claude",
                "model": "claude-fable-5",
                "effort": "high",
            },
            "fallback": {
                "provider": "codex",
                "model": "gpt-5.6-terra",
                "effort": "xhigh",
            },
        },
        "reviewer": {
            "enabled": False,
            "primary": {
                "provider": "codex",
                "model": "gpt-5.6-sol",
                "effort": "xhigh",
            },
            "fallback": {
                "provider": "claude",
                "model": "claude-sonnet-5",
                "effort": "high",
            },
        },
        "max_revision_cycles": 0,
    })
    async with db_factory() as db:
        task = Task(
            title="Fallback Plan",
            description="Plan with fallback",
            target_repo="/tmp",
            mode="plan",
            provider="claude",
            model="claude-fable-5",
            effort_level="high",
            plan_pipeline_config=pipeline.model_dump(mode="json"),
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    runner = PlanAgentRunner(
        db_factory=db_factory,
        instance_manager=MagicMock(),
    )
    runner._run_route = AsyncMock(side_effect=[
        PlanRouteUnavailable(
            "Fable unavailable",
            provider="claude",
        ),
        (
            {"plan": "Fallback plan"},
            '{"plan":"Fallback plan"}',
            "codex-1",
        ),
    ])
    async with db_factory() as db:
        task = await db.get(Task, task_id)
    result = await runner.run(task, cwd="/tmp")

    assert result.plan_content == "Fallback plan"
    async with db_factory() as db:
        run = (
            await db.execute(
                select(PlanAgentRun).where(
                    PlanAgentRun.plan_task_id == task_id
                )
            )
        ).scalar_one()
        steps = list(
            (
                await db.execute(
                    select(PlanAgentStep)
                    .where(PlanAgentStep.run_id == run.id)
                    .order_by(PlanAgentStep.id)
                )
            ).scalars().all()
        )
        task_state = await db.get(Task, task_id)
    assert [step.route_slot for step in steps] == [
        "primary",
        "fallback",
    ]
    assert [step.status for step in steps] == ["failed", "completed"]
    assert run.planner_provider == "codex"
    assert run.planner_model == "gpt-5.6-terra"
    assert task_state.plan_stage_provider == "codex"
    assert task_state.plan_stage_model == "gpt-5.6-terra"
    assert task_state.plan_stage_route_slot == "fallback"


@pytest.mark.asyncio
async def test_stage_switches_to_fallback_after_confirmed_primary_timeout(
    db_factory,
):
    pipeline = PlanPipelineConfig.model_validate({
        "version": 1,
        "planner": {
            "primary": {
                "provider": "claude",
                "model": "claude-fable-5",
                "effort": "high",
            },
            "fallback": {
                "provider": "codex",
                "model": "gpt-5.6-terra",
                "effort": "xhigh",
            },
        },
        "reviewer": {
            "enabled": True,
            "primary": {
                "provider": "codex",
                "model": "gpt-5.6-sol",
                "effort": "xhigh",
            },
            "fallback": {
                "provider": "claude",
                "model": "claude-sonnet-5",
                "effort": "high",
            },
        },
        "max_revision_cycles": 0,
    })
    task = Task(
        id=702,
        title="Reviewer fallback",
        description="review",
        mode="plan",
    )
    runner = PlanAgentRunner(
        db_factory=db_factory,
        instance_manager=MagicMock(),
    )
    run_id = await runner._create_run(task=task, pipeline=pipeline)
    runner._run_route = AsyncMock(side_effect=[
        PlanAgentTimeout(
            "Codex Plan Agent stream stalled",
            provider="codex",
        ),
        (
            {"action": "approve", "feedback": ""},
            '{"action":"approve","feedback":""}',
            "claude-1",
        ),
    ])

    result, _raw, route, route_slot, account_id = await runner._run_stage(
        run_id=run_id,
        task_id=task.id,
        step_type="reviewer",
        round_number=1,
        routes=pipeline.reviewer,
        cwd="/tmp",
        prompt="review",
        schema=REVIEWER_SCHEMA_V2,
        timeout=900,
    )

    assert result == {"action": "approve", "feedback": ""}
    assert route.provider == "claude"
    assert route_slot == "fallback"
    assert account_id == "claude-1"
    async with db_factory() as db:
        steps = list(
            (
                await db.execute(
                    select(PlanAgentStep)
                    .where(PlanAgentStep.run_id == run_id)
                    .order_by(PlanAgentStep.id)
                )
            ).scalars()
        )
    assert [step.route_slot for step in steps] == ["primary", "fallback"]
    assert [step.status for step in steps] == ["failed", "completed"]
    assert "stream stalled" in steps[0].error


@pytest.mark.asyncio
async def test_stage_switches_to_fallback_after_invalid_primary_response(
    db_factory,
    monkeypatch,
):
    pipeline = PlanPipelineConfig.model_validate({
        "version": 1,
        "planner": {
            "primary": {
                "provider": "codex",
                "model": "gpt-5.6-sol",
                "effort": "xhigh",
            },
            "fallback": {
                "provider": "claude",
                "model": "claude-sonnet-5",
                "effort": "high",
            },
        },
        "reviewer": {
            "enabled": True,
            "primary": {
                "provider": "claude",
                "model": "claude-opus-4-6",
                "effort": "high",
            },
            "fallback": {
                "provider": "codex",
                "model": "gpt-5.6-terra",
                "effort": "xhigh",
            },
        },
        "max_revision_cycles": 1,
    })
    task = Task(
        id=704,
        title="Reviewer response fallback",
        description="review",
        mode="plan",
    )
    claude_pool = MagicMock()
    claude_pool.select.return_value = "/claude/one"
    claude_pool.account_id_from_config_dir.return_value = "claude-1"
    codex_pool = MagicMock()
    codex_pool.select.return_value = "/codex/one"
    codex_pool.canonical_home.side_effect = lambda home: home
    codex_pool.account_id_for_home.return_value = "codex-1"
    runner = PlanAgentRunner(
        db_factory=db_factory,
        instance_manager=MagicMock(),
        claude_pool=claude_pool,
        codex_pool=codex_pool,
    )
    run_id = await runner._create_run(task=task, pipeline=pipeline)
    monkeypatch.setattr(settings, "transient_retry_enabled", True)
    monkeypatch.setattr(settings, "transient_retry_max", 2)
    monkeypatch.setattr(settings, "transient_retry_base_delay", 0)
    monkeypatch.setattr(settings, "transient_retry_max_delay", 0)
    runner._run_process = AsyncMock(side_effect=[
        PlanAgentResponseError(
            "request_input requires a valid reason",
            provider="claude",
            stdout=json.dumps({
                "type": "result",
                "stop_reason": "tool_use",
                "structured_output": {
                    "response": {
                        "action": "request_input",
                        "feedback": "",
                        "reason": "",
                        "questions": [{
                            "question": "Did the request timed out?",
                        }],
                    },
                },
            }),
        ),
        (
            {"action": "approve", "feedback": ""},
            '{"action":"approve","feedback":""}',
        ),
    ])

    result, _raw, route, route_slot, account_id = await runner._run_stage(
        run_id=run_id,
        task_id=task.id,
        step_type="reviewer",
        round_number=1,
        routes=pipeline.reviewer,
        cwd="/tmp",
        prompt="review",
        schema=REVIEWER_SCHEMA_V2,
        timeout=900,
    )

    assert result == {"action": "approve", "feedback": ""}
    assert route.provider == "codex"
    assert route_slot == "fallback"
    assert account_id == "codex-1"
    assert runner._run_process.await_count == 2
    assert [call.kwargs["provider"] for call in runner._run_process.await_args_list] == [
        "claude",
        "codex",
    ]
    claude_pool.select.assert_called_once()
    claude_pool.mark_rate_limited.assert_not_called()
    claude_pool.mark_auth_failure.assert_not_called()
    async with db_factory() as db:
        steps = list(
            (
                await db.execute(
                    select(PlanAgentStep)
                    .where(PlanAgentStep.run_id == run_id)
                    .order_by(PlanAgentStep.id)
                )
            ).scalars()
        )
    assert [step.route_slot for step in steps] == ["primary", "fallback"]
    assert [step.status for step in steps] == ["failed", "completed"]
    assert "request_input requires a valid reason" in steps[0].error


@pytest.mark.asyncio
async def test_invalid_response_text_cannot_rotate_or_cool_down_account(
    db_factory,
):
    pool = MagicMock()
    pool.select.side_effect = ["/claude/one", None]
    pool.account_id_from_config_dir.return_value = "claude-1"
    runner = PlanAgentRunner(
        db_factory=db_factory,
        instance_manager=MagicMock(),
        claude_pool=pool,
    )
    runner._record_unavailable_account = MagicMock(
        wraps=runner._record_unavailable_account,
    )
    response_error = PlanAgentResponseError(
        "reviewer feedback is invalid",
        provider="claude",
        stdout=(
            '{"response":{"action":"revise","feedback":'
            '"The user said you have hit your limit"}}'
        ),
    )
    runner._run_fixed_route_with_retry = AsyncMock(
        side_effect=response_error,
    )
    route = PlanModelRoute(
        provider="claude",
        model="claude-opus-4-6",
        effort="high",
    )

    with pytest.raises(PlanAgentResponseError) as raised:
        await runner._run_route(
            task_id=705,
            route=route,
            cwd="/tmp",
            prompt="review",
            schema=REVIEWER_SCHEMA_V2,
            timeout=900,
        )

    assert raised.value is response_error
    runner._run_fixed_route_with_retry.assert_awaited_once()
    runner._record_unavailable_account.assert_not_called()
    pool.select.assert_called_once()
    pool.mark_rate_limited.assert_not_called()
    pool.mark_auth_failure.assert_not_called()


@pytest.mark.asyncio
async def test_route_exhausts_quota_limited_accounts_before_model_fallback(
    db_factory,
):
    pool = MagicMock()
    pool.select.side_effect = ["/codex/one", "/codex/two"]
    pool.canonical_home.side_effect = lambda home: home
    pool.account_id_for_home.side_effect = {
        "/codex/one": "one",
        "/codex/two": "two",
    }.get
    runner = PlanAgentRunner(
        db_factory=db_factory,
        instance_manager=MagicMock(),
        codex_pool=pool,
    )
    runner._run_fixed_route_with_retry = AsyncMock(side_effect=[
        PlanAgentError(
            "quota",
            provider="codex",
            stderr="You have hit your usage limit",
        ),
        ({"plan": "second account"}, '{"plan":"second account"}'),
    ])
    pipeline = PlanPipelineConfig.model_validate({
        "version": 1,
        "planner": {
            "primary": {
                "provider": "codex",
                "model": "gpt-5.6-sol",
                "effort": "xhigh",
            },
            "fallback": {
                "provider": "claude",
                "model": "claude-fable-5",
                "effort": "high",
            },
        },
        "reviewer": {
            "enabled": False,
            "primary": {
                "provider": "codex",
                "model": "gpt-5.6-sol",
                "effort": "xhigh",
            },
            "fallback": {
                "provider": "claude",
                "model": "claude-fable-5",
                "effort": "high",
            },
        },
        "max_revision_cycles": 0,
    })

    result, _raw, account_id = await runner._run_route(
        task_id=19,
        route=pipeline.planner.primary,
        cwd="/tmp",
        prompt="plan",
        schema=PLANNER_SCHEMA,
        timeout=30,
    )

    assert result == {"plan": "second account"}
    assert account_id == "two"
    assert pool.select.call_args_list[1].kwargs["exclude"] == {"one"}
    pool.mark_rate_limited.assert_called_once_with("/codex/one")


@pytest.mark.asyncio
async def test_stage_fails_after_primary_and_fallback_are_unavailable(
    db_factory,
):
    runner = PlanAgentRunner(
        db_factory=db_factory,
        instance_manager=MagicMock(),
    )
    runner._run_route = AsyncMock(side_effect=[
        PlanRouteUnavailable("primary unavailable", provider="claude"),
        PlanRouteUnavailable("fallback unavailable", provider="codex"),
    ])
    pipeline = PlanPipelineConfig.model_validate({
        "version": 1,
        "planner": {
            "primary": {
                "provider": "claude",
                "model": "claude-fable-5",
                "effort": "high",
            },
            "fallback": {
                "provider": "codex",
                "model": "gpt-5.6-terra",
                "effort": "xhigh",
            },
        },
        "reviewer": {
            "enabled": False,
            "primary": {
                "provider": "codex",
                "model": "gpt-5.6-sol",
                "effort": "xhigh",
            },
            "fallback": {
                "provider": "claude",
                "model": "claude-sonnet-5",
                "effort": "high",
            },
        },
        "max_revision_cycles": 0,
    })
    task = Task(
        id=23,
        title="terminal fallback",
        description="plan",
        mode="plan",
    )
    run_id = await runner._create_run(task=task, pipeline=pipeline)

    with pytest.raises(
        PlanRouteUnavailable,
        match="primary and fallback routes are unavailable",
    ):
        await runner._run_stage(
            run_id=run_id,
            task_id=task.id,
            step_type="planner",
            round_number=1,
            routes=pipeline.planner,
            cwd="/tmp",
            prompt="plan",
            schema=PLANNER_SCHEMA,
            timeout=30,
        )

    async with db_factory() as db:
        steps = (
            await db.execute(
                select(PlanAgentStep)
                .where(PlanAgentStep.run_id == run_id)
                .order_by(PlanAgentStep.id)
            )
        ).scalars().all()
    assert [step.route_slot for step in steps] == ["primary", "fallback"]
    assert [step.status for step in steps] == ["failed", "failed"]
