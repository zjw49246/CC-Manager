"""Provider-aware context-window compaction helpers."""

import json

import pytest

from backend.models.log_entry import LogEntry
from backend.models.task import Task
from backend.services.context_compaction import (
    build_compacted_resume_prompt,
    build_compacted_task_retry_prompt,
    context_tokens_used,
    is_context_window_exceeded,
    read_codex_rollout_last_usage,
    recoverable_chat_context_failure,
)


def test_compacted_prompt_makes_recent_information_authoritative():
    prompt = build_compacted_resume_prompt(
        (
            "## 近期对话（按真实发生顺序，越靠后越新）\n"
            "现在使用 20 台节点\n\n"
            "## 原始任务背景（最低优先级，可能已被近期信息取代）\n"
            "最初计划使用 24 台节点"
        ),
        "现在训练情况怎么样？",
    )

    assert prompt.startswith("[压缩后会话恢复优先级]")
    assert "当前消息”默认优先级最高" in prompt
    assert "当前消息执行期间的后续补充/纠正" in prompt
    assert "冲突时以该补充/纠正为准" in prompt
    assert "以近期信息为准" in prompt
    assert prompt.index("现在使用 20 台节点") < prompt.index("最初计划使用 24 台节点")
    assert prompt.endswith(
        "[基础当前消息 — 默认最高优先级]\n现在训练情况怎么样？"
    )
    assert prompt.count("现在训练情况怎么样？") == 1


def test_compacted_lifecycle_retry_continues_recent_work():
    prompt = build_compacted_task_retry_prompt(
        "近期结论：改为 20 台\n\n原始背景：最初计划 24 台"
    )

    assert prompt.startswith("[Context compacted")
    assert "近期状态和结论优先" in prompt
    assert "不要从头重做旧任务" in prompt
    assert prompt.endswith("原始背景：最初计划 24 台")


def test_codex_context_limit_classifier_accepts_protocol_code_and_cli_text():
    assert is_context_window_exceeded(
        "codex",
        {"codexErrorInfo": "contextWindowExceeded"},
    )
    assert is_context_window_exceeded(
        "codex",
        "stream disconnected: Your input exceeds the context window of this model.",
    )
    assert is_context_window_exceeded("codex", "context_length_exceeded")


def test_context_limit_classifier_keeps_benign_window_metadata_out():
    assert not is_context_window_exceeded(
        "codex",
        {"modelContextWindow": 258_400, "message": "turn completed"},
    )
    assert not is_context_window_exceeded("codex", "usage limit exceeded")


def test_codex_context_tokens_use_protocol_value_with_safe_legacy_fallback():
    assert context_tokens_used(
        "codex",
        {
            "input_tokens": 100_000,
            "cache_read_input_tokens": 80_000,
            "output_tokens": 20_000,
            "context_tokens": 205_000,
        },
    ) == 205_000
    assert context_tokens_used(
        "codex",
        {
            "input_tokens": 20_000,
            "cache_read_input_tokens": 170_000,
            "output_tokens": 15_000,
        },
    ) == 205_000


def test_claude_context_tokens_keep_existing_input_only_semantics():
    assert context_tokens_used(
        "claude",
        {
            "input_tokens": 20_000,
            "cache_read_input_tokens": 170_000,
            "cache_creation_input_tokens": 5_000,
            "output_tokens": 15_000,
        },
    ) == 195_000


def test_rollout_usage_uses_last_request_instead_of_cumulative_total(tmp_path):
    rollout = tmp_path / "rollout.jsonl"
    rollout.write_text("\n".join((
        json.dumps({"type": "response_item", "payload": {"type": "message"}}),
        json.dumps({
            "type": "event_msg",
            "payload": {
                "type": "token_count",
                "info": {
                    "total_token_usage": {
                        "input_tokens": 1_505_114,
                        "cached_input_tokens": 1_300_000,
                        "output_tokens": 50_000,
                        "reasoning_output_tokens": 20_000,
                        "total_tokens": 1_555_114,
                    },
                    "last_token_usage": {
                        "input_tokens": 210_000,
                        "cached_input_tokens": 180_000,
                        "output_tokens": 8_000,
                        "reasoning_output_tokens": 2_000,
                        "total_tokens": 218_000,
                    },
                    "model_context_window": 258_400,
                },
            },
        }),
    )) + "\n")

    assert read_codex_rollout_last_usage(rollout) == {
        "input_tokens": 210_000,
        "cached_input_tokens": 180_000,
        "output_tokens": 8_000,
        "reasoning_output_tokens": 2_000,
        "total_tokens": 218_000,
        "context_window": 258_400,
    }


async def _failed_task(db_factory, *, provider: str = "claude") -> int:
    async with db_factory() as db:
        task = Task(
            title="failed context turn",
            description="continue",
            status="failed",
            provider=provider,
            session_id="native-session",
            retry_count=2,
            turn_generation=7,
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        return task.id


@pytest.mark.asyncio
async def test_recoverable_chat_failure_accepts_exact_claude_api_error(
    db_factory,
):
    task_id = await _failed_task(db_factory)
    async with db_factory() as db:
        db.add_all(
            [
                LogEntry(
                    task_id=task_id,
                    task_retry_count=2,
                    task_turn_generation=7,
                    turn_scope="foreground",
                    event_type="tool_use",
                    role="assistant",
                    tool_name="Read",
                    raw_json=json.dumps({"type": "assistant"}),
                    is_error=False,
                ),
                LogEntry(
                    task_id=task_id,
                    task_retry_count=2,
                    task_turn_generation=7,
                    turn_scope="foreground",
                    event_type="message",
                    role="assistant",
                    content="Prompt is too long",
                    raw_json=json.dumps(
                        {
                            "type": "assistant",
                            "isApiErrorMessage": True,
                            "error": "invalid_request",
                            "message": {
                                "usage": {
                                    "input_tokens": 0,
                                    "output_tokens": 0,
                                    "cache_creation_input_tokens": 0,
                                    "cache_read_input_tokens": 0,
                                }
                            },
                        }
                    ),
                    is_error=True,
                ),
            ]
        )
        await db.commit()
        task = await db.get(Task, task_id)

        assert (
            await recoverable_chat_context_failure(db, task)
            == "prompt_too_long"
        )


@pytest.mark.asyncio
async def test_recoverable_chat_failure_accepts_claude_usage_metadata(
    db_factory,
):
    """Real Claude envelopes include nested zero-valued bookkeeping fields."""

    task_id = await _failed_task(db_factory)
    async with db_factory() as db:
        db.add(
            LogEntry(
                task_id=task_id,
                task_retry_count=2,
                task_turn_generation=7,
                turn_scope="foreground",
                event_type="message",
                role="assistant",
                content="Prompt is too long",
                raw_json=json.dumps(
                    {
                        "type": "assistant",
                        "isApiErrorMessage": True,
                        "error": "invalid_request",
                        "message": {
                            "usage": {
                                "input_tokens": 0,
                                "output_tokens": 0,
                                "output_tokens_details": None,
                                "server_tool_use": {
                                    "web_search_requests": 0,
                                    "web_fetch_requests": 0,
                                },
                                "cache_creation": {
                                    "ephemeral_1h_input_tokens": 0,
                                    "ephemeral_5m_input_tokens": 0,
                                },
                                "service_tier": None,
                                "inference_geo": None,
                                "iterations": None,
                                "speed": None,
                            },
                        },
                    }
                ),
                is_error=True,
            )
        )
        await db.commit()
        task = await db.get(Task, task_id)

        assert (
            await recoverable_chat_context_failure(db, task)
            == "prompt_too_long"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("provider", "entry", "expected"),
    [
        (
            "claude",
            {
                "event_type": "system_event",
                "role": None,
                "content": "Response timed out after 900.0s",
                "raw_json": None,
                "is_error": True,
            },
            "response_timeout",
        ),
        (
            "codex",
            {
                "event_type": "system_event",
                "role": None,
                "content": "request failed",
                "raw_json": json.dumps(
                    {
                        "type": "turn.failed",
                        "error": {
                            "message": "request failed",
                            "codexErrorInfo": "contextWindowExceeded",
                        },
                    }
                ),
                "is_error": True,
            },
            "context_window_exceeded",
        ),
    ],
)
async def test_recoverable_chat_failure_accepts_terminal_protocol_shapes(
    db_factory,
    provider,
    entry,
    expected,
):
    task_id = await _failed_task(db_factory, provider=provider)
    async with db_factory() as db:
        db.add(
            LogEntry(
                task_id=task_id,
                task_retry_count=2,
                task_turn_generation=7,
                turn_scope="foreground",
                **entry,
            )
        )
        await db.commit()
        task = await db.get(Task, task_id)
        assert await recoverable_chat_context_failure(db, task) == expected


@pytest.mark.asyncio
async def test_recoverable_chat_failure_rejects_tool_text_and_stale_generation(
    db_factory,
):
    task_id = await _failed_task(db_factory)
    async with db_factory() as db:
        db.add_all(
            [
                LogEntry(
                    task_id=task_id,
                    task_retry_count=2,
                    task_turn_generation=7,
                    turn_scope="foreground",
                    event_type="tool_result",
                    role="tool",
                    tool_output="Prompt is too long",
                    is_error=True,
                ),
                LogEntry(
                    task_id=task_id,
                    task_retry_count=1,
                    task_turn_generation=6,
                    turn_scope="foreground",
                    event_type="system_event",
                    role=None,
                    content="Response timed out after 900.0s",
                    is_error=True,
                ),
            ]
        )
        await db.commit()
        task = await db.get(Task, task_id)
        assert await recoverable_chat_context_failure(db, task) is None


@pytest.mark.asyncio
async def test_recoverable_chat_timeout_survives_event_persistence_failure(
    db_factory,
):
    task_id = await _failed_task(db_factory)
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        task.error_message = "Response timed out after 900.0s"
        await db.commit()

        assert (
            await recoverable_chat_context_failure(db, task)
            == "response_timeout"
        )


@pytest.mark.asyncio
async def test_recoverable_chat_timeout_fallback_allows_prior_activity(
    db_factory,
):
    """The exact terminal Task error survives a dropped timeout event."""

    task_id = await _failed_task(db_factory)
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        task.error_message = "Response timed out after 900.0s"
        db.add(
            LogEntry(
                task_id=task_id,
                task_retry_count=2,
                task_turn_generation=7,
                turn_scope="foreground",
                event_type="tool_use",
                role="assistant",
                tool_name="Bash",
                is_error=False,
            )
        )
        await db.commit()

        assert (
            await recoverable_chat_context_failure(db, task)
            == "response_timeout"
        )


@pytest.mark.asyncio
async def test_recoverable_chat_timeout_rejects_inexact_task_error(
    db_factory,
):
    task_id = await _failed_task(db_factory)
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        task.error_message = "Response timed out after unknowns"
        await db.commit()

        assert await recoverable_chat_context_failure(db, task) is None


@pytest.mark.asyncio
async def test_recoverable_chat_failure_rejects_context_marker_with_later_activity(
    db_factory,
):
    task_id = await _failed_task(db_factory)
    async with db_factory() as db:
        db.add_all(
            [
                LogEntry(
                    task_id=task_id,
                    task_retry_count=2,
                    task_turn_generation=7,
                    turn_scope="foreground",
                    event_type="system_event",
                    role=None,
                    content="Response timed out after 900.0s",
                    is_error=True,
                ),
                LogEntry(
                    task_id=task_id,
                    task_retry_count=2,
                    task_turn_generation=7,
                    turn_scope="foreground",
                    event_type="tool_result",
                    role="tool",
                    tool_output="late activity",
                    is_error=False,
                ),
            ]
        )
        await db.commit()
        task = await db.get(Task, task_id)
        assert await recoverable_chat_context_failure(db, task) is None
