"""Tests for chat history timestamp serialization — must include Z suffix."""
import json
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from backend.api.chat import get_chat_history


def _fake_request():
    """require_task_access 需要请求身份；super_admin 直接短路，不触发 DB 查询。"""
    from types import SimpleNamespace
    return SimpleNamespace(state=SimpleNamespace(user_id=1, user_role="super_admin"))


def _make_log_row(timestamp: datetime | None = None, **kwargs):
    defaults = dict(
        id=1, role="assistant", event_type="message", content="hello",
        tool_name=None, tool_input=None, tool_output=None,
        is_error=False, loop_iteration=None,
        timestamp=timestamp or datetime(2026, 6, 10, 14, 30, 45, 123456),
        raw_json=None, task_retry_count=None, task_turn_generation=None,
        native_turn_id=None,
    )
    defaults.update(kwargs)
    row = MagicMock()
    for k, v in defaults.items():
        setattr(row, k, v)
    return row


@pytest.mark.asyncio
async def test_chat_history_timestamp_has_z_suffix():
    """Naive UTC datetime from DB should be serialized with Z suffix."""
    mock_task = MagicMock()
    mock_task.__bool__ = lambda self: True
    mock_task.sort_order = None
    mock_task.starred = False

    row = _make_log_row(timestamp=datetime(2026, 6, 10, 14, 30, 45, 123456))

    mock_db = AsyncMock()
    mock_db.get.return_value = mock_task

    mock_result = MagicMock()
    mock_result.all.return_value = [row]
    mock_result.scalar.return_value = None  # 访问置顶的组内 max-key 子查询
    mock_db.execute.return_value = mock_result

    messages = await get_chat_history(task_id=1, request=_fake_request(), limit=0, compact=True, db=mock_db)

    assert len(messages) == 1
    ts = messages[0]["timestamp"]
    assert ts.endswith("Z"), f"Expected Z suffix, got: {ts}"
    assert ts == "2026-06-10T14:30:45.123456Z"


@pytest.mark.asyncio
async def test_chat_history_null_timestamp():
    """Null timestamp should remain None."""
    mock_task = MagicMock()
    mock_task.__bool__ = lambda self: True
    mock_task.sort_order = None
    mock_task.starred = False

    row = MagicMock()
    row.id = 2
    row.role = "assistant"
    row.event_type = "message"
    row.content = "hello"
    row.tool_name = None
    row.tool_input = None
    row.tool_output = None
    row.is_error = False
    row.loop_iteration = None
    row.timestamp = None
    row.raw_json = None
    row.task_retry_count = None
    row.task_turn_generation = None
    row.native_turn_id = None

    mock_db = AsyncMock()
    mock_db.get.return_value = mock_task

    mock_result = MagicMock()
    mock_result.all.return_value = [row]
    mock_result.scalar.return_value = None  # 访问置顶的组内 max-key 子查询
    mock_db.execute.return_value = mock_result

    messages = await get_chat_history(task_id=1, request=_fake_request(), limit=0, compact=True, db=mock_db)

    assert len(messages) == 1
    assert messages[0]["timestamp"] is None
