"""Durable, structured work-state snapshots for context recovery.

Native session files are intentionally discarded after a context overflow.  A
replacement model turn therefore needs more than a few recent chat messages:
it also needs the durable operational state that was visible in tool calls,
attachments, workspace references, and sub-agent reports.  Snapshots are
stored as hidden ``LogEntry`` rows so they travel with Task history without
exposing internal recovery data through the public chat-history API.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.log_entry import LogEntry
from backend.models.sub_agent import SubAgentSession
from backend.models.task import Task


CONTEXT_SNAPSHOT_EVENT_TYPE = "context_snapshot"
CONTEXT_SNAPSHOT_TYPE = "ccm.context_recovery_snapshot"
CONTEXT_SNAPSHOT_VERSION = 1
MAX_RENDERED_WORK_STATE_CHARS = 30_000

_SECRET_ASSIGNMENT_RE = re.compile(
    r"(?i)\b([A-Z][A-Z0-9_]*(?:TOKEN|PASSWORD|PASSWD|SECRET|API_KEY|AUTH)"
    r"[A-Z0-9_]*)\s*=\s*([^\s,;]+)"
)
_JSON_SECRET_RE = re.compile(
    r'(?i)(["\'](?:token|password|passwd|secret|api[_-]?key|authorization)'
    r'["\']\s*:\s*["\'])([^"\']+)(["\'])'
)
_BEARER_RE = re.compile(r"(?i)(authorization\s*:\s*bearer\s+)[^\s]+")
_URL_USERINFO_RE = re.compile(r"(https?://)[^/@\s]+@")
_URL_SECRET_QUERY_RE = re.compile(
    r"(?i)([?&](?:token|access_token|api[_-]?key|key|secret|signature)=)"
    r"[^&#\s]+"
)
_PRIVATE_KEY_RE = re.compile(
    r"-----BEGIN [^-]*PRIVATE KEY-----.*?-----END [^-]*PRIVATE KEY-----",
    re.DOTALL,
)
_KNOWN_TOKEN_RE = re.compile(
    r"\b(?:sk-[A-Za-z0-9_-]{16,}|gh[opusr]_[A-Za-z0-9_]{16,})\b"
)
_GIT_COMMAND_RE = re.compile(
    r"(?:^|[;&|\n]\s*)git(?:\s+-C\s+\S+)?\s+"
    r"(?:status|diff|log|show|rev-parse|branch|commit|push|pull|fetch)\b",
    re.IGNORECASE,
)
_MUTATING_TOOL_NAMES = frozenset(
    {
        "edit",
        "write",
        "notebookedit",
        "apply_patch",
        "write_file",
        "filechange",
        "file_change",
    }
)
_PATH_KEYS = frozenset(
    {
        "file_path",
        "path",
        "notebook_path",
        "target_file",
        "source_file",
        "destination",
        "cwd",
        "workdir",
    }
)


def _redact_sensitive_text(value: Any, limit: int) -> str:
    text = str(value or "")
    text = _PRIVATE_KEY_RE.sub("[private key redacted]", text)
    text = _BEARER_RE.sub(r"\1[redacted]", text)
    text = _SECRET_ASSIGNMENT_RE.sub(r"\1=[redacted]", text)
    text = _JSON_SECRET_RE.sub(r"\1[redacted]\3", text)
    text = _URL_USERINFO_RE.sub(r"\1[credentials-redacted]@", text)
    text = _URL_SECRET_QUERY_RE.sub(r"\1[redacted]", text)
    text = _KNOWN_TOKEN_RE.sub("[token redacted]", text)
    text = "".join(
        character
        for character in text
        if character in "\n\t" or ord(character) >= 32
    ).strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 18)].rstrip() + "\n…(snapshot truncated)"


def _raw_mapping(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError, RecursionError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _record_key(value: Any) -> str:
    if isinstance(value, Mapping):
        stable = {
            key: nested
            for key, nested in value.items()
            if key not in {"log_id", "result_log_id"}
        }
        return json.dumps(stable, ensure_ascii=False, sort_keys=True)
    return str(value)


def _merge_recent(
    previous: Iterable[Any],
    current: Iterable[Any],
    *,
    limit: int,
) -> list[Any]:
    merged: list[Any] = []
    positions: dict[str, int] = {}
    for value in (*tuple(previous), *tuple(current)):
        key = _record_key(value)
        old_position = positions.pop(key, None)
        if old_position is not None:
            merged.pop(old_position)
            positions = {_record_key(item): i for i, item in enumerate(merged)}
        positions[key] = len(merged)
        merged.append(value)
    return merged[-limit:]


def _iter_paths(value: Any, *, key: str | None = None) -> Iterable[str]:
    if isinstance(value, Mapping):
        for nested_key, nested_value in value.items():
            yield from _iter_paths(nested_value, key=str(nested_key).lower())
        return
    if isinstance(value, list):
        for nested in value:
            yield from _iter_paths(nested, key=key)
        return
    if key not in _PATH_KEYS and not (key or "").endswith("_path"):
        return
    if isinstance(value, str) and value.strip():
        yield _redact_sensitive_text(value.strip(), 500)


def _tool_input_summary(
    tool_name: str | None,
    tool_input: str | None,
) -> tuple[str, list[str], bool]:
    parsed: Any = None
    if tool_input:
        try:
            parsed = json.loads(tool_input)
        except (TypeError, ValueError, RecursionError):
            parsed = None
    paths = list(dict.fromkeys(_iter_paths(parsed))) if parsed is not None else []
    git_related = False
    if isinstance(parsed, Mapping):
        description = parsed.get("description")
        command = parsed.get("command") or parsed.get("cmd")
        if isinstance(command, str):
            git_related = bool(_GIT_COMMAND_RE.search(command))
        preferred: dict[str, Any] = {}
        for key in (
            "description",
            "command",
            "cmd",
            "file_path",
            "path",
            "notebook_path",
            "pattern",
            "query",
            "url",
            "old_string",
            "new_string",
        ):
            if key in parsed:
                preferred[key] = parsed[key]
        changes = parsed.get("changes")
        if isinstance(changes, list):
            summarized_changes = []
            for change in changes:
                if not isinstance(change, Mapping):
                    continue
                kind = change.get("kind")
                if isinstance(kind, Mapping):
                    kind = kind.get("type")
                summarized_changes.append(
                    {
                        "path": change.get("path"),
                        "kind": kind,
                    }
                )
            if summarized_changes:
                preferred["changes"] = summarized_changes
        if preferred:
            if description and command:
                preferred = {
                    "description": description,
                    "command": command,
                    **{
                        key: value
                        for key, value in preferred.items()
                        if key not in {"description", "command"}
                    },
                }
            summary = json.dumps(preferred, ensure_ascii=False, sort_keys=True)
        else:
            summary = json.dumps(parsed, ensure_ascii=False, sort_keys=True)
    else:
        summary = tool_input or ""
        git_related = bool(_GIT_COMMAND_RE.search(summary))
    label = tool_name or "tool"
    return _redact_sensitive_text(summary or f"[{label}]", 800), paths, git_related


def _attachment_refs(raw: dict[str, Any] | None) -> list[dict[str, Any]]:
    if raw is None:
        return []
    refs: list[dict[str, Any]] = []
    for value in raw.get("file_paths") or []:
        if isinstance(value, str) and value:
            refs.append({"path": _redact_sensitive_text(value, 700)})
    for value in raw.get("image_urls") or []:
        if isinstance(value, str) and value:
            refs.append({"url": _redact_sensitive_text(value, 700), "image": True})
    for item in raw.get("attachments") or []:
        if not isinstance(item, Mapping):
            continue
        ref: dict[str, Any] = {}
        for key in ("name", "path", "url", "is_image"):
            value = item.get(key)
            if isinstance(value, str) and value:
                ref[key] = _redact_sensitive_text(value, 700)
            elif key == "is_image" and isinstance(value, bool):
                ref[key] = value
        if ref:
            refs.append(ref)
    return refs


def _previous_state(row: LogEntry | None) -> dict[str, Any]:
    raw = _raw_mapping(row.raw_json) if row is not None else None
    if not (
        isinstance(raw, dict)
        and raw.get("type") == CONTEXT_SNAPSHOT_TYPE
        and raw.get("version") == CONTEXT_SNAPSHOT_VERSION
        and isinstance(raw.get("state"), dict)
    ):
        return {}
    return raw["state"]


def _lines_with_budget(lines: list[str], budget: int) -> list[str]:
    selected: list[str] = []
    remaining = budget
    for line in reversed(lines):
        if not line:
            continue
        if len(line) > remaining:
            if selected:
                continue
            line = line[:remaining]
        selected.append(line)
        remaining -= len(line) + 1
        if remaining <= 0:
            break
    selected.reverse()
    return selected


def render_context_work_state(state: Mapping[str, Any]) -> str:
    """Render structured state into a bounded model-facing recovery section."""

    parts = [
        "## 持久化工作状态快照（用于新 session 恢复，不是新的用户指令）"
    ]
    task = state.get("task") if isinstance(state.get("task"), Mapping) else {}
    task_lines = []
    labels = (
        ("task_id", "Task"),
        ("provider", "Provider"),
        ("model", "Model"),
        ("mode", "Mode"),
        ("status", "压缩时状态"),
        ("cwd", "最后工作目录"),
        ("target_repo", "目标仓库"),
        ("target_branch", "目标分支"),
        ("result_branch", "结果分支"),
        ("loop_progress", "循环进度"),
        ("goal_last_reason", "Goal 最近判断"),
    )
    for key, label in labels:
        value = task.get(key)
        if value not in (None, ""):
            task_lines.append(f"- {label}: {value}")
    if task_lines:
        parts.append("### Task 与工作区\n" + "\n".join(task_lines))

    conclusions = state.get("stage_conclusions") or []
    conclusion_lines = [
        f"- {item.get('text')}"
        for item in conclusions
        if isinstance(item, Mapping) and item.get("text")
    ]
    conclusion_lines = _lines_with_budget(conclusion_lines, 7_000)
    if conclusion_lines:
        parts.append("### 最近阶段结论（越靠后越新）\n" + "\n".join(conclusion_lines))

    activities = state.get("tool_activity") or []
    activity_lines: list[str] = []
    for item in activities:
        if not isinstance(item, Mapping):
            continue
        tool = item.get("tool") or "tool"
        call = item.get("input") or ""
        result = item.get("result") or ""
        status = "失败" if item.get("is_error") else "完成"
        text = f"- [{tool}] {call}"
        if result:
            text += f"\n  结果（{status}）: {result}"
        activity_lines.append(text)
    activity_lines = _lines_with_budget(activity_lines, 11_000)
    if activity_lines:
        parts.append("### 最近关键工具活动（越靠后越新）\n" + "\n".join(activity_lines))

    modified_files = state.get("modified_files") or []
    referenced_files = state.get("referenced_files") or []
    file_lines = []
    if modified_files:
        file_lines.append("- 已写入/修改过的文件：")
        file_lines.extend(
            f"  - {item.get('path')}（{item.get('tool') or 'write'}）"
            for item in modified_files
            if isinstance(item, Mapping) and item.get("path")
        )
    if referenced_files:
        file_lines.append("- 最近引用的文件/目录：")
        file_lines.extend(f"  - {path}" for path in referenced_files if path)
    git_evidence = state.get("git_evidence") or []
    if git_evidence:
        file_lines.append("- 最近一次 Git 工具证据（不是实时重新探测）：")
        for item in git_evidence:
            if not isinstance(item, Mapping):
                continue
            file_lines.append(
                f"  - {item.get('command') or 'git'}"
                + (f"\n    {item.get('result')}" if item.get("result") else "")
            )
    file_lines = _lines_with_budget(file_lines, 5_500)
    if file_lines:
        parts.append("### 文件与 Git 状态\n" + "\n".join(file_lines))

    attachments = state.get("attachments") or []
    attachment_lines = []
    for item in attachments:
        if isinstance(item, Mapping):
            reference = item.get("path") or item.get("url") or item.get("name")
            if reference:
                attachment_lines.append(f"- {reference}")
    attachment_lines = _lines_with_budget(attachment_lines, 2_500)
    if attachment_lines:
        parts.append("### 历史附件引用\n" + "\n".join(attachment_lines))

    sub_agents = state.get("sub_agents") or []
    sub_agent_lines = []
    for item in sub_agents:
        if not isinstance(item, Mapping):
            continue
        line = (
            f"- #{item.get('id')} {item.get('type')}: "
            f"{item.get('status')} — {item.get('description') or ''}"
        ).rstrip()
        if item.get("summary"):
            line += f"\n  最近结果: {item['summary']}"
        if item.get("error"):
            line += f"\n  错误: {item['error']}"
        sub_agent_lines.append(line)
    sub_agent_lines = _lines_with_budget(sub_agent_lines, 3_000)
    if sub_agent_lines:
        parts.append("### 子 Agent / Monitor 状态\n" + "\n".join(sub_agent_lines))

    rendered = "\n\n".join(parts)
    return rendered[:MAX_RENDERED_WORK_STATE_CHARS]


async def capture_context_recovery_snapshot(
    db: AsyncSession,
    task: Task,
    *,
    session_id: str | None,
    reason: str,
    before_log_entry_id: int | None = None,
    include_post_source_injections: bool = False,
) -> str:
    """Persist and render one bounded, generation-bound recovery snapshot.

    The caller commits this row in the same transaction that clears the old
    native ``session_id``.  If either operation loses its generation CAS, both
    are rolled back and the stale session remains available for diagnosis.
    """

    previous_conditions = [
        LogEntry.task_id == task.id,
        LogEntry.event_type == CONTEXT_SNAPSHOT_EVENT_TYPE,
    ]
    # A compact-retry deliberately reuses the original source LogEntry id.
    # Its previous snapshot is therefore newer than ``before_log_entry_id``.
    # Always inherit the latest committed snapshot; the caller's exact Task
    # generation CAS still decides whether this newly built snapshot may be
    # committed with the session reset.
    previous_row = (
        await db.execute(
            select(LogEntry)
            .where(*previous_conditions)
            .order_by(LogEntry.id.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    previous = _previous_state(previous_row)
    previous_id = previous_row.id if previous_row is not None else None

    event_conditions = [LogEntry.task_id == task.id]
    if previous_id is not None:
        event_conditions.append(LogEntry.id > previous_id)
    if before_log_entry_id is not None:
        event_conditions.append(LogEntry.id < before_log_entry_id)

    tool_rows_desc = list(
        (
            await db.execute(
                select(
                    LogEntry.id,
                    LogEntry.event_type,
                    LogEntry.tool_name,
                    LogEntry.tool_input,
                    LogEntry.tool_output,
                    LogEntry.content,
                    LogEntry.is_error,
                )
                .where(
                    *event_conditions,
                    LogEntry.event_type.in_(("tool_use", "tool_result")),
                )
                .order_by(LogEntry.id.desc())
                .limit(96)
            )
        ).all()
    )
    tool_rows = list(reversed(tool_rows_desc))
    current_activity: list[dict[str, Any]] = []
    pending_indexes: list[int] = []
    referenced_files: list[str] = []
    modified_files: list[dict[str, Any]] = []
    for row in tool_rows:
        if row.event_type == "tool_use":
            summary, paths, git_related = _tool_input_summary(
                row.tool_name,
                row.tool_input,
            )
            activity = {
                "log_id": row.id,
                "tool": _redact_sensitive_text(row.tool_name or "tool", 120),
                "input": summary,
                "git_related": git_related,
                "result": None,
                "is_error": False,
            }
            current_activity.append(activity)
            pending_indexes.append(len(current_activity) - 1)
            referenced_files.extend(paths)
            normalized_tool = (row.tool_name or "").lower().split("__")[-1]
            if normalized_tool in _MUTATING_TOOL_NAMES:
                modified_files.extend(
                    {
                        "path": path,
                        "tool": row.tool_name or "write",
                        "log_id": row.id,
                    }
                    for path in paths
                )
        else:
            result_text = row.tool_output or row.content or ""
            result_text = _redact_sensitive_text(result_text, 1_400)
            if pending_indexes:
                activity = current_activity[pending_indexes.pop(0)]
                activity["result_log_id"] = row.id
                activity["result"] = result_text
                activity["is_error"] = bool(row.is_error)
            elif result_text:
                current_activity.append(
                    {
                        "log_id": row.id,
                        "tool": "tool result",
                        "input": "",
                        "result": result_text,
                        "is_error": bool(row.is_error),
                        "git_related": False,
                    }
                )

    git_evidence = [
        {
            "log_id": item.get("log_id"),
            "command": item.get("input"),
            "result": item.get("result"),
            "is_error": item.get("is_error", False),
        }
        for item in current_activity
        if item.get("git_related")
    ][-8:]
    critical_activity = [
        item
        for item in current_activity
        if item.get("git_related")
        or item.get("is_error")
        or (str(item.get("tool") or "").lower().split("__")[-1])
        in _MUTATING_TOOL_NAMES
    ][-10:]
    selected_activity = _merge_recent(
        critical_activity,
        current_activity[-12:],
        limit=18,
    )
    for item in selected_activity:
        item.pop("git_related", None)
    current_activity = selected_activity

    conclusion_timeline_desc = list(
        (
            await db.execute(
                select(LogEntry.id, LogEntry.event_type, LogEntry.content)
                .where(
                    *event_conditions,
                    or_(
                        LogEntry.event_type == "user_message",
                        and_(
                            LogEntry.event_type.in_(("message", "result")),
                            LogEntry.role == "assistant",
                            LogEntry.is_error.is_(False),
                            LogEntry.content.is_not(None),
                            LogEntry.content != "",
                        ),
                    ),
                )
                .order_by(LogEntry.id.desc())
                .limit(80)
            )
        ).all()
    )
    conclusion_timeline = list(reversed(conclusion_timeline_desc))
    current_conclusions: list[dict[str, Any]] = []
    stage_final = None
    for row in conclusion_timeline:
        if row.event_type == "user_message":
            if stage_final is not None:
                current_conclusions.append(stage_final)
                stage_final = None
            continue
        if row.content:
            # Multiple assistant text events can be emitted in one stage.
            # Preserve only its final durable conclusion, not progress chatter.
            stage_final = {
                "log_id": row.id,
                "text": _redact_sensitive_text(row.content, 1_600),
            }
    if stage_final is not None:
        current_conclusions.append(stage_final)
    current_conclusions = current_conclusions[-10:]

    attachment_conditions = list(event_conditions)
    attachment_rows = list(
        (
            await db.execute(
                select(LogEntry.id, LogEntry.raw_json)
                .where(
                    *attachment_conditions,
                    LogEntry.event_type == "user_message",
                )
                .order_by(LogEntry.id.desc())
                .limit(60)
            )
        ).all()
    )
    if include_post_source_injections and before_log_entry_id is not None:
        post_rows = list(
            (
                await db.execute(
                    select(LogEntry.id, LogEntry.raw_json)
                    .where(
                        LogEntry.task_id == task.id,
                        LogEntry.event_type == "user_message",
                        LogEntry.id > before_log_entry_id,
                    )
                    .order_by(LogEntry.id.asc())
                    .limit(30)
                )
            ).all()
        )
        attachment_rows.extend(
            row
            for row in post_rows
            if (_raw_mapping(row.raw_json) or {}).get("source") == "inject"
        )
    current_attachments: list[dict[str, Any]] = []
    for row in reversed(attachment_rows):
        current_attachments.extend(_attachment_refs(_raw_mapping(row.raw_json)))

    sub_agent_rows = list(
        (
            await db.execute(
                select(SubAgentSession)
                .where(SubAgentSession.task_id == task.id)
                .order_by(SubAgentSession.id.desc())
                .limit(12)
            )
        ).scalars()
    )
    sub_agents = [
        {
            "id": row.id,
            "type": row.agent_type,
            "status": row.status,
            "description": _redact_sensitive_text(row.description, 500),
            "summary": _redact_sensitive_text(row.last_summary, 1_000)
            if row.last_summary
            else None,
            "error": _redact_sensitive_text(row.last_error, 700)
            if row.last_error
            else None,
        }
        for row in reversed(sub_agent_rows)
    ]

    previous_task = previous.get("task") if isinstance(previous.get("task"), dict) else {}
    task_state = {
        **previous_task,
        "task_id": task.id,
        "incarnation_id": task.incarnation_id,
        "provider": task.provider or "claude",
        "model": task.model,
        "mode": task.mode,
        "status": task.status,
        "cwd": _redact_sensitive_text(task.last_cwd, 700) if task.last_cwd else None,
        "target_repo": _redact_sensitive_text(task.target_repo, 700)
        if task.target_repo
        else None,
        "target_branch": task.target_branch,
        "result_branch": task.result_branch,
        "loop_progress": task.loop_progress,
        "goal_last_reason": _redact_sensitive_text(task.goal_last_reason, 1_000)
        if task.goal_last_reason
        else None,
    }
    state = {
        "task": task_state,
        "stage_conclusions": _merge_recent(
            previous.get("stage_conclusions") or (),
            current_conclusions,
            limit=12,
        ),
        "tool_activity": _merge_recent(
            previous.get("tool_activity") or (),
            current_activity,
            limit=18,
        ),
        "referenced_files": _merge_recent(
            previous.get("referenced_files") or (),
            referenced_files,
            limit=30,
        ),
        "modified_files": _merge_recent(
            previous.get("modified_files") or (),
            modified_files,
            limit=20,
        ),
        "git_evidence": _merge_recent(
            previous.get("git_evidence") or (),
            git_evidence,
            limit=8,
        ),
        "attachments": _merge_recent(
            previous.get("attachments") or (),
            current_attachments,
            limit=24,
        ),
        "sub_agents": sub_agents,
    }
    canonical_state = json.dumps(
        state,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    digest = hashlib.sha256(canonical_state.encode("utf-8")).hexdigest()
    created_at = datetime.now(UTC).replace(tzinfo=None)
    payload = {
        "type": CONTEXT_SNAPSHOT_TYPE,
        "version": CONTEXT_SNAPSHOT_VERSION,
        "reason": _redact_sensitive_text(reason, 80),
        "created_at": created_at.isoformat(timespec="microseconds"),
        "source": {
            "session_id": session_id,
            "retry_count": task.retry_count,
            "turn_generation": task.turn_generation,
            "turn_source_log_id": task.turn_source_log_id,
            "before_log_entry_id": before_log_entry_id,
            "previous_snapshot_log_id": previous_id,
        },
        "state_sha256": digest,
        "state": state,
    }
    db.add(
        LogEntry(
            instance_id=task.instance_id,
            task_id=task.id,
            task_retry_count=task.retry_count,
            task_turn_generation=task.turn_generation,
            turn_scope=None,
            event_type=CONTEXT_SNAPSHOT_EVENT_TYPE,
            role="system",
            content=(
                f"Context recovery snapshot v{CONTEXT_SNAPSHOT_VERSION}: "
                f"{payload['reason']} ({digest[:12]})"
            ),
            raw_json=json.dumps(payload, ensure_ascii=False, sort_keys=True),
            is_error=False,
            timestamp=created_at,
        )
    )
    await db.flush()
    return render_context_work_state(state)
