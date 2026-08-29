import asyncio
import json
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import select

from backend.config import settings
from backend.models.plan_agent import PlanAgentRun, PlanAgentStep
from backend.models.task import Task
from backend.schemas.plan import PlanModelRoute, PlanPipelineConfig
from backend.services.codex_app_server import CodexTurnProcess
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
    _StructuredJsonWhitespaceGuard,
    _build_command,
    _extract_provider_content,
    _plan_request_with_attachments,
    _validate_structured,
    _validate_structured_v2,
    _versioned_planner_prompt,
    _versioned_reference_files,
    _versioned_reviewer_prompt,
)


def test_claude_plan_command_is_read_only():
    command = _build_command(
        provider="claude",
        model="claude-opus-4-6",
        effort="high",
        schema=PLANNER_SCHEMA,
    )

    assert command[0] == settings.claude_binary
    assert command[command.index("--permission-mode") + 1] == "plan"
    assert "--no-session-persistence" in command
    assert "--safe-mode" in command
    assert command[command.index("--tools") + 1] == "Read,Grep,Glob"
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
    assert kwargs["output_schema"] == PLANNER_SCHEMA
    assert kwargs["resume_session_id"] is None


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
            task_id=-703,
            home="/canonical/default-codex-home",
            model="gpt-5.6-sol",
            effort="medium",
            cwd="/tmp",
            prompt="plan safely",
            schema=PLANNER_SCHEMA_V2,
            timeout=2,
            json_whitespace_limit=16,
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
            task_id=-701,
            home="/canonical/default-codex-home",
            model="gpt-5.6-sol",
            effort="xhigh",
            cwd="/tmp",
            prompt="review safely",
            schema=REVIEWER_SCHEMA_V2,
            timeout=2,
            step_id=step_id,
            delta_idle_timeout=0.05,
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
        task_id=-702,
        home="/canonical/default-codex-home",
        model="gpt-5.6-sol",
        effort="xhigh",
        cwd="/tmp",
        prompt="review safely",
        schema=REVIEWER_SCHEMA_V2,
        timeout=2,
        delta_idle_timeout=0.05,
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
