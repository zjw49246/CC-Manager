import asyncio
from copy import deepcopy
from datetime import datetime
import hashlib
import json
import logging
import os
import time
import uuid
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Request
from backend.api.deps import (
    get_current_user_id,
    require_internal_service,
    require_task_access,
    require_task_control,
)
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import and_, not_, select, update as sa_update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from backend.database import get_db
from backend.models.task import Task
from backend.models.log_entry import LogEntry
from backend.models.user_skill import UserSkill
from backend.api.uploads import (
    UploadAttachmentValidationError,
    ValidatedUploadAttachment,
    validate_upload_attachments,
)
from backend.schemas.task import TaskResponse, TaskRoutingExpectation
from backend.services.task_creation import stage_task_record
from backend.services.chat_event_identity import persisted_chat_event
from backend.services.task_queue import TaskQueue, task_is_pr_review_superseded
from backend.services.pr_review_runtime import (
    PR_REVIEW_TERMINAL_CHAT_HEADER,
    PR_REVIEW_TERMINAL_CHAT_HEADER_VALUE,
)
from backend.services.worker_proxy import get_task_operation_lock
from backend.services.worker_relay import (
    worker_task_generation,
    worker_task_generation_predicates,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/tasks", tags=["chat"])


def _plan_delivery_digest(payload: dict) -> str:
    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
    ).hexdigest()


def _trusted_terminal_pr_review_chat(request: Request) -> bool:
    """Validate the Manager-only assertion used by Worker PR chat mirrors."""

    headers = getattr(request, "headers", None)
    value = (
        headers.get(PR_REVIEW_TERMINAL_CHAT_HEADER)
        if headers is not None
        else None
    )
    if value is None:
        return False
    require_internal_service(request)
    if value != PR_REVIEW_TERMINAL_CHAT_HEADER_VALUE:
        raise HTTPException(
            403,
            "Invalid internal PR review chat authorization",
        )
    return True


async def _sender_display_name(
    request: Request,
    db: AsyncSession,
) -> str | None:
    """Resolve the presentation identity without changing access ownership."""
    user_id = getattr(request.state, "user_id", None)
    if user_id:
        from backend.models.user import User

        sender = await db.get(User, user_id)
        if sender:
            return sender.name

    # The deployment service token is a real super-admin identity, but it is
    # intentionally not bound to a disabled/deleted User row.  Give it the
    # same stable presentation name returned by the frontend login fallback.
    if getattr(request.state, "auth_type", None) == "token":
        return "Admin"
    return None


class ChatMessage(BaseModel):
    message: str
    image_paths: list[str] | None = None  # kept for backwards compatibility
    file_paths: list[str] | None = None
    secret_ids: list[int] | None = None
    # One-shot model override for this message (does not change task.model)
    model: str | None = None
    # The route rendered by the caller. A mismatch is rejected before the
    # user row is persisted, so a stale Fast tab cannot launch Standard.
    expected_routing: TaskRoutingExpectation | None = None
    # Approved independent Plan Tasks explicitly attached to this real user
    # turn. Approval alone never starts a model turn.
    plan_task_ids: list[int] | None = None
    confirmed_stale_plan_task_ids: list[int] | None = None
    plan_version_ids: list[int] | None = None
    confirmed_stale_plan_version_ids: list[int] | None = None
    # Manager→Worker only. It makes a successful remote application replayable
    # when the response or Manager commit is lost.
    plan_application_receipt_key: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    )

    @model_validator(mode="after")
    def validate_plan_attachment_generation(self):
        if self.plan_task_ids and self.plan_version_ids:
            raise ValueError("Use plan_version_ids or legacy plan_task_ids, not both")
        return self


class FrontendReviewGoalMessage(BaseModel):
    """Start a repeatable frontend review from an existing Task chat."""

    message: str = Field(min_length=1)
    image_paths: list[str] | None = None
    file_paths: list[str] | None = None
    secret_ids: list[int] | None = None
    profile: Literal["standard", "exhaustive"] = "standard"
    max_iterations: int = Field(default=5, ge=1, le=10)
    expected_routing: TaskRoutingExpectation | None = None

    @model_validator(mode="after")
    def normalize_message(self):
        self.message = self.message.strip()
        if not self.message:
            raise ValueError("message is required")
        return self


class FrontendReviewGoalCapabilities(BaseModel):
    """Whether an existing Task can safely edit a local Git worktree."""

    available: bool
    reason: str | None = None
    repo_path: str | None = None


class ForkAnchor(BaseModel):
    type: Literal["initial", "latest", "user_message"]
    id: int | None = None

    @model_validator(mode="after")
    def validate_anchor(self):
        if self.type == "user_message" and (self.id is None or self.id <= 0):
            raise ValueError("user message fork anchors require a positive id")
        if self.type in {"initial", "latest"} and self.id is not None:
            raise ValueError(f"{self.type} fork anchors cannot include an id")
        return self


class CodexForkRequest(BaseModel):
    anchor: ForkAnchor
    title: str | None = None


def _validate_chat_service_tier(task: Task, model_override: str | None) -> None:
    """Reject an unsupported one-turn model before persisting the message."""

    from backend.services.codex_models import validate_codex_service_tier

    try:
        validate_codex_service_tier(
            task.provider,
            model_override or task.model,
            task.codex_service_tier,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def _parse_chat_command(message: str):
    """Parse one leading command and reject unknown command-like input."""

    from backend.services.command_registry import parse_command

    command, command_args = parse_command(message)
    stripped = message.strip()
    if stripped.startswith("$") and command is None:
        unknown_cmd = stripped.split(None, 1)[0]
        raise HTTPException(
            400,
            f"未知命令 {unknown_cmd}，输入 $help 查看可用命令",
        )
    return command, command_args


async def _validate_chat_command_admission(
    task: Task,
    command,
    db: AsyncSession,
) -> None:
    """Validate a command's temporary Skills against the task provider."""

    if command is None or not command.required_skills:
        return

    from backend.api.tasks import _validate_skill_configuration

    await _validate_skill_configuration(
        db,
        provider=task.provider,
        enabled_skills=command.required_skills,
        selected_user_skills=None,
        worker_id=task.worker_id,
        shared_from_id=task.shared_from_id,
        metadata=task.metadata_,
    )


def _native_ids(raw_json: str | None) -> tuple[str | None, str | None]:
    """Return (item_id, turn_id) from one persisted normalized event."""

    if not raw_json:
        return None, None
    try:
        raw = json.loads(raw_json)
    except (TypeError, ValueError):
        return None, None
    if not isinstance(raw, dict):
        return None, None
    item = raw.get("item")
    turn = raw.get("turn")
    item_id = (
        raw.get("item_id")
        or raw.get("itemId")
        or (item.get("id") if isinstance(item, dict) else None)
    )
    turn_id = (
        raw.get("turn_id")
        or raw.get("turnId")
        or (turn.get("id") if isinstance(turn, dict) else None)
    )
    return (
        str(item_id) if item_id not in (None, "") else None,
        str(turn_id) if turn_id not in (None, "") else None,
    )


def _is_legacy_codex_collab_completed(
    event_type: str | None,
    content: str | None,
    raw_json: str | None,
) -> bool:
    """Identify only the historical false-completed Codex item rows.

    Older app-server parsing promoted a collab tool's item-local
    ``status=completed`` to a chat ``system_event``.  Bare system messages
    with the same text must remain visible, so every native discriminator is
    checked against the persisted raw event before filtering.
    """

    if (
        event_type != "system_event"
        or content != "completed"
        or not raw_json
    ):
        return False
    try:
        raw = json.loads(raw_json)
    except (TypeError, ValueError):
        return False
    if not isinstance(raw, dict) or raw.get("type") != "item.completed":
        return False
    item = raw.get("item")
    return bool(
        isinstance(item, dict)
        and item.get("type")
        in {"collabAgentToolCall", "collab_agent_tool_call"}
        and item.get("status") == "completed"
    )


def _turn_item_ids(item: object) -> set[str]:
    """Collect native item ids from the lossy thread/read response."""

    found: set[str] = set()
    if isinstance(item, dict):
        value = item.get("id")
        if value not in (None, ""):
            found.add(str(value))
        for child in item.values():
            if isinstance(child, (dict, list)):
                found.update(_turn_item_ids(child))
    elif isinstance(item, list):
        for child in item:
            found.update(_turn_item_ids(child))
    return found


def _raw_log_metadata(row: LogEntry) -> dict:
    if not row.raw_json:
        return {}
    try:
        value = json.loads(row.raw_json)
    except (TypeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def _fork_seed_uploads(metadata: dict) -> list[dict]:
    """Rebuild composer-ready upload records without trusting client paths."""

    from backend.api.uploads import UPLOAD_DIR

    attachments = metadata.get("attachments") or []
    explicit_paths = (
        metadata.get("file_paths") or metadata.get("image_paths") or []
    )
    upload_root = UPLOAD_DIR.resolve()
    uploads: list[dict] = []
    for index, attachment in enumerate(attachments):
        if not isinstance(attachment, dict):
            continue
        url = attachment.get("url")
        name = attachment.get("name")
        if not isinstance(url, str) or not isinstance(name, str):
            continue

        path: str | None = None
        if index < len(explicit_paths) and isinstance(explicit_paths[index], str):
            candidate = os.path.realpath(explicit_paths[index])
            try:
                if (
                    os.path.commonpath((candidate, str(upload_root)))
                    == str(upload_root)
                ):
                    path = candidate
            except ValueError:
                pass
        if path is None and url.startswith("/api/uploads/"):
            filename = url.removeprefix("/api/uploads/")
            candidate_path = (upload_root / filename).resolve()
            try:
                candidate_path.relative_to(upload_root)
            except ValueError:
                continue
            path = str(candidate_path)
        if path is None:
            continue

        uploads.append({
            "id": f"fork-seed-{index}",
            "filename": name,
            "path": path,
            "url": url,
            "is_image": bool(attachment.get("is_image")),
        })
    return uploads


def _is_forkable_user_message(row: LogEntry) -> bool:
    """Only ordinary human follow-up messages are precise fork boundaries."""

    if row.event_type != "user_message" or row.role != "user":
        return False
    # Injected text belongs to the middle of an active native turn. Monitor,
    # sub-agent, and other sourced messages are likewise not human turn starts.
    return not _raw_log_metadata(row).get("source")


def _resolve_fork_turn(
    *,
    anchor: ForkAnchor,
    rows: list[LogEntry],
    turns: list[dict],
) -> tuple[str, int]:
    """Resolve the completed native turn immediately before a user message."""

    if not turns:
        raise HTTPException(409, "Codex session has no persisted turns to fork")
    turn_ids = [str(turn.get("id") or "") for turn in turns]
    if any(not turn_id for turn_id in turn_ids):
        raise HTTPException(409, "Codex returned an invalid turn history")
    turn_index = {turn_id: index for index, turn_id in enumerate(turn_ids)}
    item_to_turn: dict[str, str] = {}
    for turn_id, turn in zip(turn_ids, turns):
        for item_id in _turn_item_ids(turn.get("items") or []):
            item_to_turn[item_id] = turn_id

    row_turns: dict[int, str] = {}
    for row in rows:
        item_id, direct_turn_id = _native_ids(row.raw_json)
        resolved = direct_turn_id or (item_to_turn.get(item_id) if item_id else None)
        if resolved in turn_index:
            row_turns[row.id] = resolved

    selected_index = next(
        (index for index, row in enumerate(rows) if row.id == anchor.id),
        None,
    )
    if selected_index is None:
        raise HTTPException(404, "Fork anchor message not found")
    selected = rows[selected_index]
    if not _is_forkable_user_message(selected):
        raise HTTPException(
            400,
            "Fork anchors must be ordinary user messages, not injected or generated events",
        )

    selected_turn_id = row_turns.get(selected.id)
    if selected_turn_id is None:
        # A CCM user row is committed before turn/start returns. Associate it
        # with the first native event before the next real user message.
        for candidate in rows[selected_index + 1:]:
            if _is_forkable_user_message(candidate):
                break
            selected_turn_id = row_turns.get(candidate.id)
            if selected_turn_id:
                break
    if selected_turn_id is None:
        # Legacy logs predate persisted turn ids. The initial Task description
        # owns turn zero, so the Nth ordinary follow-up user row owns turn N.
        ordinal = sum(
            1
            for row in rows[:selected_index + 1]
            if _is_forkable_user_message(row)
        )
        if ordinal < len(turn_ids):
            selected_turn_id = turn_ids[ordinal]
    if selected_turn_id is None:
        raise HTTPException(
            409,
            "This user message cannot be mapped safely to a Codex turn",
        )

    selected_turn_index = turn_index[selected_turn_id]
    if selected_turn_index == 0:
        raise HTTPException(
            409,
            "There is no completed Codex turn before this user message",
        )
    target_index = selected_turn_index - 1
    target_turn_id = turn_ids[target_index]
    status = str(turns[target_index].get("status") or "")
    if status in {"inProgress", "in_progress", "running"}:
        raise HTTPException(409, "The preceding Codex turn is still running")

    return target_turn_id, selected.id - 1


def _resolve_latest_fork_turn(
    turns: list[dict],
    rows: list[LogEntry],
) -> tuple[str, int]:
    """Resolve an exact full-context copy through the latest completed turn."""

    if not turns:
        raise HTTPException(409, "Codex session has no persisted turns to copy")
    latest = turns[-1]
    turn_id = str(latest.get("id") or "")
    if not turn_id:
        raise HTTPException(409, "Codex returned an invalid turn history")
    if str(latest.get("status") or "") != "completed":
        raise HTTPException(
            409,
            "The latest Codex turn is not completed and cannot be copied exactly",
        )
    return turn_id, (rows[-1].id if rows else -1)


def _codex_fork_home(task: Task) -> tuple[str, str | None]:
    """Resolve the one proven account home containing the source rollout."""

    from backend.main import codex_pool
    from backend.services.codex_app_server import normalize_codex_home

    account_id = (task.metadata_ or {}).get("codex_account_id")
    if codex_pool:
        if account_id:
            home = codex_pool.home_for_account(str(account_id))
            if not home:
                raise HTTPException(
                    409,
                    "The Codex account bound to this task no longer exists",
                )
            matches = codex_pool.locate_session_homes(task.session_id)
            canonical = codex_pool.canonical_home(home)
            if matches and canonical not in matches:
                raise HTTPException(
                    409,
                    "The bound Codex account does not contain this session",
                )
            return canonical, str(account_id)
        matches = codex_pool.locate_session_homes(task.session_id)
        if len(matches) > 1:
            raise HTTPException(
                409,
                "Codex session has multiple rollout copies without an account binding",
            )
        if len(matches) == 1:
            home = matches[0]
            return home, codex_pool.account_id_for_home(home)

    from backend.api.tasks import _find_session_jsonl

    rollout = _find_session_jsonl(task.session_id, provider="codex")
    if rollout is None:
        raise HTTPException(409, "Codex rollout file was not found")
    sessions_dir = next(
        (parent for parent in rollout.parents if parent.name == "sessions"),
        None,
    )
    if sessions_dir is None:
        raise HTTPException(409, "Codex rollout is outside a valid CODEX_HOME")
    return normalize_codex_home(sessions_dir.parent), (
        str(account_id) if account_id else None
    )


@router.post("/{task_id}/chat")
async def send_chat_message(
    task_id: int,
    body: ChatMessage,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Send a follow-up message on a task, resuming its previous session."""
    if body.plan_application_receipt_key:
        from backend.api.deps import require_internal_service

        require_internal_service(request)
    task = await db.get(Task, task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    await require_task_access(request, task, db)
    from backend.api.tasks import _require_expected_task_routing

    _require_expected_task_routing(
        task,
        body.expected_routing,
        effective_model=body.model or task.model,
    )
    _validate_chat_service_tier(task, body.model)
    if task_is_pr_review_superseded(task):
        raise HTTPException(
            409,
            "This PR review task was superseded by a newer push",
        )
    command, command_args = _parse_chat_command(body.message)
    if task.shared_from_id is not None:
        if body.plan_task_ids or body.plan_version_ids:
            raise HTTPException(
                400,
                "Shared shadow tasks do not support Plan attachments",
            )
        return await _send_shared_chat(
            task,
            body,
            db,
            command=command,
        )
    if task.worker_id is not None:
        return await _send_worker_chat(
            task,
            body,
            db,
            request,
            command=command,
        )
    if body.secret_ids:
        from backend.api.deps import require_admin

        require_admin(request)

    approved_plans: list[Task] = []
    approved_versions = []

    # Worker-local stage/ack/reconcile and direct chat admission share this
    # process-wide lock.  A stage that wins first returns 409 here; a stage
    # that wins after this check is still caught by the queued turn's final DB
    # launch barrier.
    await db.rollback()
    async with get_task_operation_lock(task_id):
        db.expire_all()
        task = await db.get(Task, task_id)
        if task is None:
            raise HTTPException(404, "Task not found")
        await require_task_access(request, task, db)
        if task.worker_id is not None or task.shared_from_id is not None:
            raise HTTPException(
                409,
                "Task routing changed while chat admission was in progress",
            )
        from backend.api.tasks import (
            _MANUAL_RETRYABLE_STATUSES,
            _require_expected_task_routing,
            _require_no_pending_worker_routing,
            _require_pr_review_chat_allowed,
        )

        _require_no_pending_worker_routing(task)
        await _require_pr_review_chat_allowed(
            db,
            task_id,
            trusted_unlinked_terminal=_trusted_terminal_pr_review_chat(
                request
            ),
        )
        if task.status in _MANUAL_RETRYABLE_STATUSES:
            from backend.services.frontend_review_goal import (
                frontend_review_goal_terminal_updates,
            )

            restore_updates = frontend_review_goal_terminal_updates(task)
            if restore_updates:
                for field, value in restore_updates.items():
                    setattr(task, field, value)
                await db.commit()
                await db.refresh(task)
        admitted_routing = _require_expected_task_routing(
            task,
            body.expected_routing,
            effective_model=body.model or task.model,
        )
        _validate_chat_service_tier(task, body.model)
        application_receipt = None
        if body.plan_application_receipt_key:
            from backend.models.plan import PlanApplicationReceipt

            application_receipt = (
                await db.execute(
                    select(PlanApplicationReceipt).where(
                        PlanApplicationReceipt.receipt_key
                        == body.plan_application_receipt_key
                    )
                )
            ).scalar_one_or_none()
            if application_receipt is not None:
                if (
                    application_receipt.target_task_id != task_id
                    or application_receipt.plan_version_ids
                    != (body.plan_version_ids or [])
                ):
                    raise HTTPException(
                        409, "Plan application receipt identity changed"
                    )
                if application_receipt.delivery_status in {
                    "failed",
                    "cancelled",
                }:
                    raise HTTPException(
                        409,
                        "Plan application delivery failed before launch; "
                        "the Version may be applied again",
                    )
                if application_receipt.delivery_status == "uncertain":
                    raise HTTPException(
                        409,
                        "Plan application launch outcome is uncertain; "
                        "automatic replay was blocked",
                    )
                if (
                    application_receipt.status == "committed"
                    and application_receipt.response
                ):
                    payload = application_receipt.outbox_payload or {}
                    if (
                        payload.get("user_message_text") != body.message
                        or payload.get("attachment_paths")
                        != (body.file_paths or body.image_paths or [])
                        or payload.get("model_override") != body.model
                        or payload.get("expected_task_routing")
                        != list(admitted_routing)
                        or payload.get("source_log_id")
                        != application_receipt.manager_user_log_id
                    ):
                        raise HTTPException(
                            409, "Plan application receipt payload changed"
                        )
                    from backend.main import dispatcher

                    admitted = await dispatcher.enqueue_plan_application_receipt(
                        application_receipt.receipt_key
                    )
                    if not admitted:
                        raise HTTPException(
                            409,
                            "Plan application delivery is no longer admissible",
                        )
                    return application_receipt.response
                if application_receipt.outbox_payload:
                    from backend.main import dispatcher

                    admitted = await dispatcher.enqueue_plan_application_receipt(
                        application_receipt.receipt_key
                    )
                    if not admitted:
                        raise HTTPException(
                            409,
                            "Plan application delivery is no longer admissible",
                        )
                    if application_receipt.response:
                        return application_receipt.response
                raise HTTPException(
                    409, "Plan application receipt is still being processed"
                )
        if not task.session_id:
            raise HTTPException(
                400,
                "No previous session on this task. Run the task first.",
            )
        await _validate_chat_command_admission(task, command, db)
        from backend.services.plan_tasks import approved_plans_for_message

        try:
            if body.plan_version_ids:
                from backend.services.plan_service import (
                    approved_versions_for_message,
                )

                approved_versions = await approved_versions_for_message(
                    db,
                    target=task,
                    version_ids=body.plan_version_ids,
                    confirmed_stale_version_ids=(
                        body.confirmed_stale_plan_version_ids
                    ),
                )
            else:
                approved_plans = await approved_plans_for_message(
                    db,
                    task,
                    body.plan_task_ids,
                    confirmed_stale_plan_task_ids=(
                        body.confirmed_stale_plan_task_ids
                    ),
                )
        except ValueError as exc:
            staleness = getattr(exc, "staleness", None)
            if staleness is not None:
                raise HTTPException(
                    409,
                    detail={
                        "message": str(exc),
                        "plan_task_id": getattr(exc, "plan_task_id", None),
                        "plan_version_id": getattr(
                            exc, "plan_version_id", None
                        ),
                        "staleness": staleness,
                    },
                ) from exc
            raise HTTPException(400, str(exc)) from exc

    command_skills: dict | None = None

    # Keep sender identity presentation-only.  The raw text is what the model
    # receives; the prefixed form is only stored/broadcast for the chat UI.
    model_message = body.message
    display_content = model_message
    sender_display_name = await _sender_display_name(request, db)
    if sender_display_name:
        display_content = f"[{sender_display_name}] {model_message}"

    # Explicit commands append their invocation instructions. Permanently
    # enabled skills are advertised by the launch-time skill directory; merely
    # enabling one must not be represented as a fresh user invocation.
    prompt_parts = [model_message]
    review_routing_prompt: str | None = None
    if not command:
        from backend.services.workspace_review_intent import (
            workspace_browser_review_routing_prompt,
        )

        review_routing_prompt = workspace_browser_review_routing_prompt(
            model_message,
        )
        if review_routing_prompt:
            prompt_parts.append(review_routing_prompt)
    if command:
        # $command detected: inject command prompt and set temporary skills
        prompt_parts.append(command.prompt_template)
        if command_args:
            prompt_parts[0] = command_args
        command_skills = command.required_skills or None
    if body.secret_ids:
        from backend.services.dispatcher import _build_secrets_block
        from backend.database import async_session
        secrets_block = await _build_secrets_block(async_session, body.secret_ids)
        if secrets_block:
            prompt_parts.append(secrets_block)
    all_paths = body.file_paths or body.image_paths or []
    if all_paths:
        file_list = "\n".join(f"- {p}" for p in all_paths)
        prompt_parts.append(f"请用 Read 工具查看以下文件：\n{file_list}")
    prompt = "\n\n".join(prompt_parts)
    workspace_review_baseline_run_id: str | None = None
    if review_routing_prompt is not None:
        from backend.models.test_harness import TestHarnessRun

        baseline = await db.execute(
            select(TestHarnessRun.id)
            .where(TestHarnessRun.task_id == task_id)
            .order_by(
                TestHarnessRun.created_at.desc(),
                TestHarnessRun.id.desc(),
            )
            .limit(1)
        )
        workspace_review_baseline_run_id = baseline.scalar_one_or_none()
    if approved_plans:
        from backend.services.plan_tasks import build_approved_plan_prompt

        prompt = build_approved_plan_prompt(approved_plans, prompt)
    elif approved_versions:
        from backend.services.plan_service import build_versioned_plan_prompt

        prompt = build_versioned_plan_prompt(approved_versions, prompt)

    # Build file attachment metadata for storage and display
    _IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".gif", ".webp"}
    attachments: list[dict] = []
    for p in all_paths:
        filename = os.path.basename(p)
        ext = os.path.splitext(filename)[1].lower()
        attachments.append({
            "url": f"/api/uploads/{filename}",
            "name": filename,
            "is_image": ext in _IMAGE_EXTS,
        })

    applied_plan_data: list[dict[str, object]] = []
    if approved_plans:
        from backend.services.plan_tasks import applied_plan_snapshots

        applied_plan_data = applied_plan_snapshots(approved_plans)
    elif approved_versions:
        from backend.services.plan_service import versioned_plan_snapshots

        applied_plan_data = versioned_plan_snapshots(approved_versions)

    # Store user message as a log entry (use instance_id=1 as placeholder)
    log_metadata: dict = {"raw_content": model_message}
    if attachments:
        log_metadata["attachments"] = attachments
        log_metadata["file_paths"] = all_paths
    if sender_display_name:
        # Model-facing history rebuilds must use this exact original text,
        # never guess by regex (the user's real message may start with [BUG]).
        log_metadata["sender_name"] = sender_display_name
    if applied_plan_data:
        # Persist the exact approved version that was prepended to this turn.
        # The Plan row may later be revised or deleted, but chat history must
        # still explain what context the model actually received.
        log_metadata["applied_plans"] = applied_plan_data
    user_log = LogEntry(
        instance_id=1,
        task_id=task_id,
        event_type="user_message",
        role="user",
        content=display_content,
        raw_json=json.dumps(log_metadata) if log_metadata else None,
        is_error=False,
    )
    db.add(user_log)
    await db.flush()
    # Persist an epoch timestamp in the durable outbox. Unlike a monotonic
    # process clock, this remains comparable with messages admitted after a
    # process or host restart.
    queue_timestamp = time.time()
    plan_queue_admission_fence = None
    if approved_versions:
        from backend.main import dispatcher
        from backend.services.dispatcher import TaskStartPausedError

        try:
            plan_queue_admission_fence = (
                await dispatcher.snapshot_plan_queue_admission(task_id)
            )
        except (TaskStartPausedError, RuntimeError) as exc:
            await db.rollback()
            raise HTTPException(
                status_code=409,
                detail="Task queue is stopping; the Plan Version was not applied",
            ) from exc
    application_receipt_key = (
        body.plan_application_receipt_key or str(uuid.uuid4())
        if approved_versions
        else None
    )
    response = {
        "ok": True,
        "queued": True,
        "session_id": task.session_id,
        "applied_plan_task_ids": [plan.id for plan in approved_plans],
        "applied_plan_version_ids": [
            version.id for _plan, version in approved_versions
        ],
    }
    if application_receipt_key is not None:
        response["plan_application_receipt_key"] = application_receipt_key
    if application_receipt_key and approved_versions:
        from backend.models.plan import PlanApplicationReceipt

        outbox_payload = {
            "prompt": prompt,
            "priority": 0,
            "source": "user",
            "command_skills": command_skills,
            "model_override": body.model,
            "expected_task_routing": list(admitted_routing),
            "source_log_id": user_log.id,
            "user_message_text": model_message,
            # Keep the full immutable model request (including the approved
            # Plan) for compaction/session-recovery rebuilds. The raw user
            # text above is the separate HTTP replay identity.
            "current_message": prompt,
            "attachment_paths": all_paths,
            "queue_timestamp": queue_timestamp,
            "queue_admission_fence": plan_queue_admission_fence,
        }
        application_receipt = PlanApplicationReceipt(
            receipt_key=application_receipt_key,
            target_task_id=task_id,
            manager_user_log_id=user_log.id,
            plan_version_ids=[version.id for _plan, version in approved_versions],
            status="committed",
            response=response,
            delivery_status="pending",
            outbox_payload=outbox_payload,
            payload_digest=_plan_delivery_digest(outbox_payload),
        )
        db.add(application_receipt)
        await db.flush()
    if approved_plans:
        from sqlalchemy import update as sa_update

        applied_at = datetime.utcnow()
        for plan in approved_plans:
            applied = await db.execute(
                sa_update(Task)
                .where(
                    Task.id == plan.id,
                    Task.mode == "plan",
                    Task.plan_target_task_id == task_id,
                    Task.plan_approved.is_(True),
                    Task.status == "completed",
                    Task.plan_applied_at.is_(None),
                )
                .values(
                    plan_applied_at=applied_at,
                    plan_applied_to_session_id=task.session_id,
                    plan_applied_log_id=user_log.id,
                )
            )
            if applied.rowcount != 1:
                await db.rollback()
                raise HTTPException(
                    409,
                    f"Plan Task #{plan.id} was applied concurrently",
                )
    elif approved_versions:
        from backend.models.plan import PlanApplication

        for plan, version in approved_versions:
            db.add(
                PlanApplication(
                    plan_id=plan.id,
                    plan_version_id=version.id,
                    application_type="chat_message",
                    target_task_id=task_id,
                    target_session_id=task.session_id,
                    user_log_id=user_log.id,
                    applied_by=get_current_user_id(request),
                    application_receipt_key=application_receipt_key,
                )
            )
        try:
            await db.flush()
        except IntegrityError as exc:
            await db.rollback()
            raise HTTPException(
                409,
                "A selected Plan Version was applied concurrently",
            ) from exc
    await db.commit()

    # Broadcast user message to task channel
    from backend.main import broadcaster
    image_urls = [a["url"] for a in attachments if a.get("is_image")]
    broadcast_data = persisted_chat_event(user_log, {
        "event_type": "user_message",
        "role": "user",
        "content": display_content,
        "raw_content": model_message,
        "image_urls": image_urls,
        "attachments": attachments,
        "applied_plans": applied_plan_data,
    })
    if sender_display_name:
        broadcast_data["sender_name"] = sender_display_name
    await broadcaster.broadcast(f"task:{task_id}", broadcast_data)

    # Enqueue for serial processing (replaces direct launch)
    from backend.main import dispatcher
    from backend.services.dispatcher import PRIORITY_USER, TaskStartPausedError
    try:
        if application_receipt is not None:
            admitted = await dispatcher.enqueue_plan_application_receipt(
                application_receipt.receipt_key
            )
            if not admitted:
                raise HTTPException(
                    409,
                    "Plan application was cancelled before queue admission",
                )
        else:
            await dispatcher.enqueue_message(
                task_id=task_id,
                prompt=prompt,
                priority=PRIORITY_USER,
                source="user",
                command_skills=command_skills,
                model_override=body.model,
                expected_task_routing=admitted_routing,
                source_log_id=user_log.id,
                queue_timestamp=queue_timestamp,
            )
    except (TaskStartPausedError, RuntimeError) as exc:
        if application_receipt is None:
            if approved_plans:
                # Legacy Task-backed Plans have no durable outbox. Queue
                # admission failures are therefore known pre-delivery and
                # must restore their one-shot application markers exactly as
                # before the first-class Version path was introduced.
                async with get_task_operation_lock(task_id):
                    for plan in approved_plans:
                        await db.execute(
                            sa_update(Task)
                            .where(
                                Task.id == plan.id,
                                Task.plan_applied_log_id == user_log.id,
                            )
                            .values(
                                plan_applied_at=None,
                                plan_applied_to_session_id=None,
                                plan_applied_log_id=None,
                            )
                        )
                    log_metadata.pop("applied_plans", None)
                    user_log.raw_json = json.dumps(log_metadata)
                    await db.commit()
            raise HTTPException(
                status_code=409,
                detail="服务即将重启，消息未进入执行队列，请重连后重试",
            ) from exc
        # The application and exact queue envelope are already durable. A
        # restart recovers pending/queued delivery. Cancellation or a
        # permanent admission failure may have won the same dispatcher gate,
        # though, so do not acknowledge a message that was synchronously
        # released for retry.
        await db.rollback()
        delivery_status = (
            await db.execute(
                select(PlanApplicationReceipt.delivery_status).where(
                    PlanApplicationReceipt.receipt_key
                    == application_receipt.receipt_key
                )
            )
        ).scalar_one_or_none()
        if delivery_status in {"failed", "cancelled"}:
            raise HTTPException(
                status_code=409,
                detail=(
                    "Plan application was cancelled before launch; "
                    "the Version may be applied again"
                ),
            ) from exc
        if delivery_status == "uncertain":
            raise HTTPException(
                status_code=409,
                detail=(
                    "Plan application launch outcome is uncertain; "
                    "administrator reconciliation is required"
                ),
            ) from exc
        if delivery_status not in {"pending", "queued", "launching", "launched"}:
            raise HTTPException(
                status_code=409,
                detail="Plan application delivery state could not be confirmed",
            ) from exc
        logger.info(
            "Deferred durable Plan application delivery %s: %s",
            application_receipt.receipt_key,
            exc,
        )
    if approved_versions:
        from backend.services.plan_events import broadcast_plan_event

        for plan, version in approved_versions:
            await broadcast_plan_event(
                event="plan_version_applied",
                plan_id=plan.id,
                target_task_id=task_id,
                version_id=version.id,
                user_log_id=user_log.id,
            )
    response["workspace_review_expected"] = review_routing_prompt is not None
    response["workspace_review_baseline_run_id"] = (
        workspace_review_baseline_run_id
    )
    return response


@router.get(
    "/{task_id}/frontend-review-goal/capabilities",
    response_model=FrontendReviewGoalCapabilities,
)
async def get_frontend_review_goal_capabilities(
    task_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Inspect the Task's real resume cwd without mutating the repository."""

    task = await db.get(Task, task_id)
    if task is None:
        raise HTTPException(404, "Task not found")
    await require_task_control(request, task, db)

    from backend.services.frontend_review_goal import (
        inspect_frontend_review_local_repository,
    )

    return await inspect_frontend_review_local_repository(task, db)


@router.post(
    "/{task_id}/frontend-review-goal",
    response_model=TaskResponse,
)
async def start_frontend_review_goal(
    task_id: int,
    body: FrontendReviewGoalMessage,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Turn an idle existing Task into a repeatable Browser Review Goal."""

    task = await db.get(Task, task_id)
    if task is None:
        raise HTTPException(404, "Task not found")
    await require_task_control(request, task, db)
    if body.secret_ids:
        from backend.api.deps import require_admin

        require_admin(request)

    await db.rollback()
    async with get_task_operation_lock(task_id):
        db.expire_all()
        current = await db.get(Task, task_id)
        if current is None:
            raise HTTPException(404, "Task not found")
        await require_task_control(request, current, db)

        from backend.api.tasks import (
            _MANUAL_RETRYABLE_STATUSES,
            _require_expected_task_routing,
            _require_no_pending_worker_routing,
            _retry_local_task_safely,
        )
        from backend.services.task_creation import (
            validate_task_service_tier_configuration,
        )

        _require_expected_task_routing(
            current,
            body.expected_routing,
            effective_model=current.model,
        )
        if task_is_pr_review_superseded(current):
            raise HTTPException(
                409,
                "This PR review task was superseded by a newer push",
            )
        if current.worker_id is not None:
            raise HTTPException(
                400,
                "Frontend Review Goal currently requires a Manager-local Task",
            )
        if current.shared_from_id is not None:
            raise HTTPException(
                400,
                "Frontend Review Goal cannot start from a shared Task",
            )
        _require_no_pending_worker_routing(current)
        if not current.session_id:
            raise HTTPException(
                400,
                "Run the task once before starting a Frontend Review Goal",
            )
        if current.status not in _MANUAL_RETRYABLE_STATUSES:
            raise HTTPException(
                409,
                f"Task status {current.status} is not idle; wait for it to finish",
            )
        if current.pty_background_generation is not None:
            raise HTTPException(
                409,
                "Task still has active Claude PTY background output",
            )
        if int(current.active_sub_agents or 0) > 0:
            raise HTTPException(
                409,
                "Task still has active Monitor or Sub-Agent sessions",
            )
        from backend.services.frontend_review_goal import (
            inspect_frontend_review_local_repository,
        )

        repository_capability = await inspect_frontend_review_local_repository(
            current,
            db,
        )
        if not repository_capability["available"]:
            raise HTTPException(
                409,
                repository_capability["reason"]
                or "无法确认可修改的本地 Git 仓库",
            )
        from backend.models.project import Project

        project = await db.get(Project, current.project_id) if current.project_id else None
        from backend.services.workspace_review import workspace_review_capability

        review_capability = workspace_review_capability(current, project)
        if not review_capability["available"]:
            raise HTTPException(
                409,
                review_capability["reason"]
                or "Project 尚未确认可信 Preview 配置",
            )
        try:
            validate_task_service_tier_configuration(
                provider=current.provider,
                model=current.model,
                codex_service_tier=current.codex_service_tier,
                mode="goal",
                goal_evaluator_model=current.goal_evaluator_model,
            )
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc

        from backend.services.frontend_review_goal import (
            FRONTEND_REVIEW_ACTIVATION_METADATA_KEY,
            FRONTEND_REVIEW_METADATA_KEY,
            build_frontend_review_goal_condition,
            frontend_review_goal_config,
            frontend_review_goal_restore_snapshot,
        )

        review_config = frontend_review_goal_config({
            FRONTEND_REVIEW_METADATA_KEY: {
                "mode": "goal",
                "profile": body.profile,
                "max_iterations": body.max_iterations,
            },
        })
        if review_config is None:  # Pydantic already validates; fail closed.
            raise HTTPException(422, "Invalid Frontend Review Goal configuration")
        all_paths = body.file_paths or body.image_paths or []
        metadata = deepcopy(current.metadata_ or {})
        metadata[FRONTEND_REVIEW_METADATA_KEY] = review_config
        metadata[FRONTEND_REVIEW_ACTIVATION_METADATA_KEY] = {
            "message": body.message,
            "file_paths": list(all_paths),
            "secret_ids": list(body.secret_ids or []),
            "restore": frontend_review_goal_restore_snapshot(current),
        }
        task_updates = {
            "mode": "goal",
            "goal_condition": build_frontend_review_goal_condition(body.message),
            "goal_max_turns": review_config["max_iterations"],
            "goal_turns_used": 0,
            "goal_last_reason": None,
            "retry_count": 0,
            "metadata_": metadata,
        }

        display_content = body.message
        sender_display_name = await _sender_display_name(request, db)
        if sender_display_name:
            display_content = f"[{sender_display_name}] {body.message}"
        image_exts = {".png", ".jpg", ".jpeg", ".gif", ".webp"}
        attachments = [{
            "url": f"/api/uploads/{os.path.basename(path)}",
            "name": os.path.basename(path),
            "is_image": os.path.splitext(path)[1].lower() in image_exts,
        } for path in all_paths]

        from backend.main import dispatcher
        from backend.services.dispatcher import (
            TaskStartConflictError,
            TaskStartPausedError,
        )

        if dispatcher is None:
            raise HTTPException(503, "Dispatcher is unavailable")
        try:
            async with dispatcher.task_start_guard(
                require_idle_task_id=task_id,
            ):
                queue = TaskQueue(db)
                activated = await _retry_local_task_safely(
                    task_id,
                    queue,
                    db,
                    task_updates=task_updates,
                    commit=False,
                )
                if activated is None:
                    raise HTTPException(409, "Task disappeared during Goal activation")
                log_metadata: dict[str, Any] = {
                    "source": "frontend-review-goal",
                    "raw_content": body.message,
                }
                if attachments:
                    log_metadata.update({
                        "attachments": attachments,
                        "file_paths": list(all_paths),
                    })
                if sender_display_name:
                    log_metadata["sender_name"] = sender_display_name
                user_log = LogEntry(
                    instance_id=None,
                    task_id=task_id,
                    event_type="user_message",
                    role="user",
                    content=display_content,
                    raw_json=json.dumps(log_metadata, ensure_ascii=False),
                    is_error=False,
                )
                db.add(user_log)
                await db.commit()
                await db.refresh(user_log)
                await db.refresh(activated)
        except TaskStartPausedError as exc:
            raise HTTPException(
                409,
                "服务即将重启，暂时不能启动循环审查，请稍后重试",
            ) from exc
        except TaskStartConflictError as exc:
            raise HTTPException(
                409,
                "Task 已有一条消息等待执行，请等待完成后再启动循环审查",
            ) from exc

    from backend.main import broadcaster

    image_urls = [
        attachment["url"]
        for attachment in attachments
        if attachment["is_image"]
    ]
    event = persisted_chat_event(user_log, {
        "event_type": "user_message",
        "role": "user",
        "content": display_content,
        "source": "frontend-review-goal",
        "raw_content": body.message,
        "image_urls": image_urls,
        "attachments": attachments,
    })
    if sender_display_name:
        event["sender_name"] = sender_display_name
    await broadcaster.broadcast(f"task:{task_id}", event)
    from backend.services.task_events import broadcast_status_change

    await broadcast_status_change(task_id, "pending")
    dispatcher.wake()
    return activated


@router.get("/{task_id}/fork-anchors")
async def list_codex_fork_anchors(
    task_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """List ordinary user follow-ups that can serve as fork boundaries."""

    source = await db.get(Task, task_id)
    if not source:
        raise HTTPException(404, "Task not found")
    await require_task_control(request, source, db)
    if (source.provider or "claude").lower() != "codex":
        raise HTTPException(400, "Only Codex sessions support native forks")
    if not source.session_id:
        raise HTTPException(400, "This task has no Codex session to fork")

    rows = list((await db.execute(
        select(LogEntry)
        .where(LogEntry.task_id == task_id)
        .order_by(LogEntry.id.asc())
    )).scalars().all())
    anchors = [{
        "type": "latest",
        "id": None,
        "content": "完整复制当前上下文",
        "timestamp": (
            source.completed_at.isoformat() + "Z"
            if source.completed_at else None
        ),
        "attachments": [],
    }]
    if source.description:
        anchors.append({
            "type": "initial",
            "id": None,
            "content": source.description,
            "timestamp": (
                source.created_at.isoformat() + "Z"
                if source.created_at else None
            ),
            "attachments": (source.metadata_ or {}).get("attachments") or [],
        })
    for row in rows:
        if not _is_forkable_user_message(row):
            continue
        metadata = _raw_log_metadata(row)
        anchors.append({
            "type": "user_message",
            "id": row.id,
            "content": metadata.get("raw_content") or row.content or "",
            "timestamp": (
                row.timestamp.isoformat() + "Z" if row.timestamp else None
            ),
            "attachments": metadata.get("attachments") or [],
        })
    return anchors


@router.post("/{task_id}/fork", response_model=TaskResponse, status_code=201)
async def fork_codex_task(
    task_id: int,
    body: CodexForkRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Create an independent Codex task by forking through one chat turn."""

    source = await db.get(Task, task_id)
    if not source:
        raise HTTPException(404, "Task not found")
    await require_task_control(request, source, db)
    if (source.provider or "claude").lower() != "codex":
        raise HTTPException(400, "Only Codex sessions support native forks")
    if source.shared_from_id is not None:
        raise HTTPException(409, "Shared shadow tasks cannot fork native sessions")
    if source.worker_id is not None:
        raise HTTPException(409, "Remote Worker task forks are not supported yet")
    if not source.session_id:
        raise HTTPException(400, "This task has no Codex session to fork")
    if source.status in {"in_progress", "executing", "migrating"}:
        raise HTTPException(409, "Wait for the current Codex turn to finish")

    rows = list((await db.execute(
        select(LogEntry)
        .where(LogEntry.task_id == task_id)
        .order_by(LogEntry.id.asc())
    )).scalars().all())
    selected: LogEntry | None = None
    seed_message: str | None = None
    selected_metadata: dict = {}
    if body.anchor.type == "initial":
        if not source.description:
            raise HTTPException(404, "Initial prompt not found")
        seed_message = source.description
        selected_metadata = source.metadata_ or {}
    elif body.anchor.type == "user_message":
        selected = next(
            (row for row in rows if row.id == body.anchor.id),
            None,
        )
        if selected is None:
            raise HTTPException(404, "Fork anchor message not found")
        if not _is_forkable_user_message(selected):
            raise HTTPException(
                400,
                "Fork anchors must be ordinary user messages, not injected or generated events",
            )
        selected_metadata = _raw_log_metadata(selected)
        seed_message = (
            selected_metadata.get("raw_content") or selected.content or ""
        )

    codex_home, account_id = _codex_fork_home(source)
    from backend.main import instance_manager
    from backend.services.codex_app_server import (
        CodexAppServerBusyError,
        CodexAppServerError,
    )

    try:
        if body.anchor.type == "initial":
            last_turn_id = None
            cutoff = -1
            forked_thread = await instance_manager.create_codex_thread(
                codex_home,
                cwd=source.last_cwd or source.target_repo or os.getcwd(),
                model=source.model,
            )
        else:
            native_thread = await instance_manager.read_codex_thread(
                codex_home,
                source.session_id,
            )
            turns = [
                turn for turn in (native_thread.get("turns") or [])
                if isinstance(turn, dict)
            ]
            if body.anchor.type == "latest":
                last_turn_id, cutoff = _resolve_latest_fork_turn(turns, rows)
            else:
                last_turn_id, cutoff = _resolve_fork_turn(
                    anchor=body.anchor,
                    rows=rows,
                    turns=turns,
                )
            forked_thread = await instance_manager.fork_codex_thread(
                codex_home,
                source.session_id,
                last_turn_id=last_turn_id,
            )
    except CodexAppServerBusyError as exc:
        raise HTTPException(409, str(exc)) from exc
    except CodexAppServerError as exc:
        raise HTTPException(502, f"Codex thread fork failed: {exc}") from exc

    forked_thread_id = str(forked_thread["id"])
    committed = False
    try:
        metadata = deepcopy(source.metadata_ or {})
        if account_id:
            metadata["codex_account_id"] = account_id
        metadata["forked_from_task_id"] = source.id
        metadata["forked_from_log_id"] = (
            body.anchor.id if body.anchor.type == "user_message" else None
        )
        metadata["forked_from_turn_id"] = last_turn_id
        metadata["fork_mode"] = (
            "full_copy" if body.anchor.type == "latest" else "branch"
        )
        if seed_message is not None:
            metadata["fork_seed_message"] = seed_message
            metadata["fork_seed_log_id"] = (
                body.anchor.id if body.anchor.type == "user_message" else None
            )
            metadata["fork_seed_uploads"] = _fork_seed_uploads(selected_metadata)
        else:
            metadata.pop("fork_seed_message", None)
            metadata.pop("fork_seed_log_id", None)
            metadata.pop("fork_seed_uploads", None)
        if body.anchor.type == "initial":
            # The empty native thread has not consumed the initial prompt or
            # its files yet. Keep them only in the editable seed composer.
            metadata.pop("attachments", None)
            metadata.pop("image_paths", None)

        default_title = (
            f"Fork of #{source.id}: {source.title}"
            if source.title
            else f"Fork of #{source.id}"
        )
        now = datetime.utcnow()
        forked_task = await stage_task_record(
            db,
            title=(body.title.strip() if body.title and body.title.strip() else default_title)[:200],
            description=(
                source.description
                if body.anchor.type in {"user_message", "latest"}
                else None
            ),
            status="completed",
            priority=source.priority,
            project_id=source.project_id,
            target_repo=source.target_repo,
            target_branch=source.target_branch,
            merge_status="pending",
            worker_id=None,
            created_by=get_current_user_id(request),
            max_retries=source.max_retries,
            mode="auto",
            session_id=forked_thread_id,
            last_cwd=source.last_cwd,
            provider="codex",
            model=source.model,
            codex_service_tier=source.codex_service_tier,
            effort_level=source.effort_level,
            thinking_budget=source.thinking_budget,
            system_prompt_mode=source.system_prompt_mode,
            timeout_hours=source.timeout_hours,
            enable_workflows=source.enable_workflows,
            enabled_skills=deepcopy(source.enabled_skills),
            selected_user_skills=deepcopy(source.selected_user_skills),
            tags=deepcopy(source.tags),
            attention_tag=source.attention_tag,
            metadata_=metadata,
            started_at=now,
            completed_at=now,
        )
        for row in rows:
            if row.id > cutoff:
                break
            db.add(LogEntry(
                instance_id=None,
                task_id=forked_task.id,
                event_type=row.event_type,
                role=row.role,
                content=row.content,
                tool_name=row.tool_name,
                tool_input=row.tool_input,
                tool_output=row.tool_output,
                raw_json=row.raw_json,
                is_error=row.is_error,
                loop_iteration=row.loop_iteration,
                timestamp=row.timestamp,
            ))
        db.add(LogEntry(
            instance_id=None,
            task_id=forked_task.id,
            event_type="system_event",
            role="system",
            content=f"Forked from Task #{source.id}",
            raw_json=json.dumps({
                "forked_from_task_id": source.id,
                "forked_from_log_id": metadata["forked_from_log_id"],
                "forked_from_turn_id": last_turn_id,
            }),
            is_error=False,
        ))
        # A committed Task and its native fork must never split under request
        # cancellation. Settle the commit before deciding whether compensation
        # is still allowed.
        commit_task = asyncio.create_task(db.commit())
        cancellation: asyncio.CancelledError | None = None
        while not commit_task.done():
            try:
                await asyncio.shield(commit_task)
            except asyncio.CancelledError as exc:
                cancellation = exc
        commit_task.result()
        committed = True
        if cancellation is not None:
            raise cancellation
        await db.refresh(forked_task)
    except BaseException:
        await db.rollback()
        if not committed:
            cleanup = asyncio.create_task(
                instance_manager.delete_codex_thread(
                    codex_home,
                    forked_thread_id,
                )
            )
            while not cleanup.done():
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    continue
            cleanup.result()
        raise

    if forked_task.project_id:
        try:
            from backend.services.task_sharing import auto_share_new_task
            await auto_share_new_task(
                db,
                forked_task.id,
                forked_task.project_id,
            )
        except Exception:
            logger.exception(
                "Could not auto-share forked task %s",
                forked_task.id,
            )
    return forked_task


async def _send_shared_chat(
    task: Task,
    body: ChatMessage,
    db: AsyncSession,
    *,
    command=None,
):
    """Shared (shadow) task: store locally, broadcast, proxy to sharer CCM."""
    from backend.main import broadcaster
    from backend.models.task_share import SharedTaskReceived
    from backend.services.shared_proxy import proxy_chat

    if command is None:
        command, _command_args = _parse_chat_command(body.message)
    await db.refresh(task)
    await _validate_chat_command_admission(task, command, db)

    # Find the shared record
    result = await db.execute(
        select(SharedTaskReceived).where(SharedTaskReceived.id == task.shared_from_id)
    )
    shared = result.scalar_one_or_none()
    if not shared:
        raise HTTPException(400, "Shared task record not found")
    owner_ccm_url = shared.owner_ccm_url
    remote_task_id = shared.remote_task_id
    share_token = shared.share_token

    # Get sender name for prefix
    from backend.models.feishu_binding import FeishuUserBinding
    binding_result = await db.execute(select(FeishuUserBinding).limit(1))
    binding = binding_result.scalar_one_or_none()
    sender_name = binding.feishu_name if binding else None
    prefixed = f"[{sender_name}] {body.message}" if sender_name else body.message

    log_metadata: dict = {"raw_content": body.message}
    if sender_name:
        log_metadata["sender_name"] = sender_name

    async def proxy_to_owner() -> None:
        try:
            await proxy_chat(
                owner_ccm_url,
                remote_task_id,
                share_token,
                message=body.message,
                sender_name=sender_name,
            )
        except Exception as exc:
            response = getattr(exc, "response", None)
            if getattr(response, "status_code", None) == 409:
                try:
                    detail = response.json().get("detail")
                except Exception:
                    detail = None
                raise HTTPException(
                    409,
                    detail or "Sharer rejected the chat generation",
                ) from exc
            raise HTTPException(502, f"Cannot reach sharer CCM: {exc}") from exc

    # The receiver is never authoritative for admission (and its shadow does
    # not carry every owner-only marker such as PRReview state).  Let the owner
    # accept first, before creating a local message, so any 4xx/5xx rejection
    # cannot leave a ghost bubble on the shadow.
    await proxy_to_owner()

    # Store user message locally WITH prefix (same as what sharer sees)
    user_log = LogEntry(
        instance_id=None,
        task_id=task.id,
        event_type="user_message",
        role="user",
        content=prefixed,
        raw_json=json.dumps(log_metadata),
        is_error=False,
    )
    db.add(user_log)
    await db.commit()

    # Broadcast to local frontend WITH prefix
    await broadcaster.broadcast(f"task:{task.id}", persisted_chat_event(user_log, {
        "event_type": "user_message",
        "role": "user",
        "content": prefixed,
        "raw_content": body.message,
        "sender_name": sender_name,
    }))

    return {"ok": True, "queued": True}


async def _preserve_remote_uncertain_plan_receipt(
    db: AsyncSession,
    *,
    receipt,
    remote_receipt: dict,
    request: Request | None,
) -> None:
    """Mirror a Worker's ambiguous launch as a consumed, visible Version."""

    from backend.services.plan_events import broadcast_plan_event
    from backend.services.plan_service import preserve_uncertain_plan_application

    error = str(
        remote_receipt.get("delivery_error")
        or "Worker restarted after the launch claim; automatic replay was blocked"
    )[:2000]
    evidence = remote_receipt.get("launch_evidence")
    response = remote_receipt.get("response")
    plan_ids = await preserve_uncertain_plan_application(
        db,
        receipt=receipt,
        error=error,
        launch_evidence=evidence if isinstance(evidence, dict) else None,
        response=response if isinstance(response, dict) else None,
        applied_by=(get_current_user_id(request) if request is not None else None),
    )
    await db.commit()
    for plan_id in plan_ids:
        await broadcast_plan_event(
            event="plan_application_delivery_uncertain",
            plan_id=plan_id,
            target_task_id=receipt.target_task_id,
            receipt_key=receipt.receipt_key,
            delivery_status="uncertain",
        )


async def _send_worker_chat(
    task: Task,
    body: ChatMessage,
    db: AsyncSession,
    request: Request | None = None,
    *,
    command=None,
):
    """Worker task 的 chat 代理。"""
    from backend.main import broadcaster, worker_proxy
    if worker_proxy is None:
        raise HTTPException(503, "Worker 功能未启用")
    if body.secret_ids:
        raise HTTPException(400, "Worker task 暂不支持引用 Secrets（Phase 3）")

    # Drop the route's read snapshot before waiting for the process-wide lock.
    # TaskMigrator holds the same lock for its complete copy/rebind workflow.
    task_id = task.id
    await db.rollback()
    async with get_task_operation_lock(task_id):
        db.expire_all()
        current = await db.get(Task, task_id)
        observed = (
            worker_task_generation(current)
            if current is not None
            else None
        )
        if observed is None:
            raise HTTPException(
                409,
                "Task moved away from its Worker before chat could be sent",
            )
        if task_is_pr_review_superseded(current):
            raise HTTPException(
                409,
                "This PR review task was superseded by a newer push",
            )
        if command is None:
            command, _command_args = _parse_chat_command(body.message)
        from backend.api.tasks import _ensure_worker_routing_ready
        from backend.api.tasks import (
            _require_expected_task_routing,
            _require_pr_review_chat_allowed,
        )

        _require_expected_task_routing(
            current,
            body.expected_routing,
            effective_model=body.model or current.model,
        )
        _validate_chat_service_tier(current, body.model)
        terminal_pr_review_chat = await _require_pr_review_chat_allowed(
            db,
            task_id,
        )
        await _validate_chat_command_admission(current, command, db)
        await _ensure_worker_routing_ready(
            current,
            operation_lock_held=True,
        )

        approved_versions = []
        remote_version_ids: list[int] = []
        remote_confirmed_version_ids: list[int] = []
        if body.plan_version_ids:
            from backend.services.plan_service import approved_versions_for_message

            try:
                approved_versions = await approved_versions_for_message(
                    db,
                    target=current,
                    version_ids=body.plan_version_ids,
                    confirmed_stale_version_ids=(
                        body.confirmed_stale_plan_version_ids
                    ),
                )
            except ValueError as exc:
                staleness = getattr(exc, "staleness", None)
                if staleness is not None:
                    raise HTTPException(
                        409,
                        detail={
                            "message": str(exc),
                            "plan_version_id": getattr(
                                exc, "plan_version_id", None
                            ),
                            "staleness": staleness,
                        },
                    ) from exc
                raise HTTPException(400, str(exc)) from exc
            for plan, version in approved_versions:
                if plan.target_task_id != current.id:
                    raise HTTPException(
                        409,
                        f"Plan Version #{version.id} no longer targets this Task",
                    )

        # Preserve the sender prefix for the Manager UI, but forward only the
        # raw user text so it never becomes part of the model prompt.
        model_message = body.message
        display_content = model_message
        sender_display_name = None
        if request:
            sender_display_name = await _sender_display_name(request, db)
            if sender_display_name:
                display_content = f"[{sender_display_name}] {model_message}"

        worker = await worker_proxy.require_ready_worker(observed.worker_id)
        if terminal_pr_review_chat:
            # Old Workers permanently freeze pr-review chat. Confirm the
            # matching endpoint contract before the Manager stores a user
            # bubble, otherwise a mixed-version rollout leaves a ghost row.
            await worker_proxy.require_terminal_pr_review_chat_support(worker)

        # Reconcile a Worker commit whose HTTP ACK or Manager-side commit was
        # lost. The prepared row is durable and binds the exact Manager log and
        # Version set, so retrying cannot enqueue the model turn twice.
        if approved_versions:
            from backend.models.plan import (
                PlanApplication,
                PlanApplicationReceipt,
            )
            from backend.services.plan_service import versioned_plan_snapshots

            wanted_ids = [version.id for _plan, version in approved_versions]
            prepared_rows = list((await db.execute(
                select(PlanApplicationReceipt).where(
                    PlanApplicationReceipt.target_task_id == current.id,
                    PlanApplicationReceipt.worker_id == worker.id,
                    PlanApplicationReceipt.status == "prepared",
                    PlanApplicationReceipt.delivery_status.not_in(
                        ["failed", "cancelled"]
                    ),
                )
            )).scalars())
            prepared = next(
                (row for row in prepared_rows if row.plan_version_ids == wanted_ids),
                None,
            )
            if prepared is not None:
                prior_log = await db.get(LogEntry, prepared.manager_user_log_id)
                if prior_log is None:
                    raise HTTPException(
                        409,
                        "Prepared Plan application lost its user log",
                    )
                prior_metadata = _raw_log_metadata(prior_log)
                prior_paths = prior_metadata.get("file_paths") or []
                requested_paths = body.file_paths or body.image_paths or []
                if (
                    prior_metadata.get("raw_content") != body.message
                    or prior_paths != requested_paths
                ):
                    raise HTTPException(
                        409,
                        "A previous Worker Plan application is bound to a "
                        "different message or attachment set",
                    )
                remote_receipt = await worker_proxy.get_plan_application_receipt(
                    worker, prepared.receipt_key
                )
                remote_delivery_status = (
                    remote_receipt.get("delivery_status")
                    if isinstance(remote_receipt, dict)
                    else None
                )
                if remote_delivery_status in {"failed", "cancelled"}:
                    from backend.services.plan_service import (
                        release_unstarted_plan_application,
                    )

                    await release_unstarted_plan_application(
                        db,
                        receipt_key=prepared.receipt_key,
                        delivery_status=remote_delivery_status,
                        error=str(remote_receipt.get("delivery_error") or "")[:2000],
                        expected_worker_id=worker.id,
                    )
                    await db.commit()
                    raise HTTPException(
                        409,
                        "Worker rejected the Plan application before launch; retry it",
                    )
                if remote_delivery_status == "uncertain":
                    await _preserve_remote_uncertain_plan_receipt(
                        db,
                        receipt=prepared,
                        remote_receipt=remote_receipt,
                        request=request,
                    )
                    raise HTTPException(
                        409,
                        "Worker Plan application launch outcome is uncertain; "
                        "automatic replay was blocked",
                    )
                if (
                    remote_receipt is not None
                    and remote_receipt.get("status") == "committed"
                    and isinstance(remote_receipt.get("response"), dict)
                ):
                    remote_result = dict(remote_receipt["response"])
                    receipt_committed = await db.execute(
                        sa_update(PlanApplicationReceipt)
                        .where(
                            PlanApplicationReceipt.receipt_key
                            == prepared.receipt_key,
                            PlanApplicationReceipt.status == "prepared",
                            PlanApplicationReceipt.delivery_status == "pending",
                        )
                        .values(
                            status="committed",
                            delivery_status=(
                                "launched"
                                if remote_delivery_status == "launched"
                                else "queued"
                            ),
                            response=remote_result,
                            updated_at=datetime.utcnow(),
                        )
                    )
                    if receipt_committed.rowcount != 1:
                        await db.rollback()
                        raise HTTPException(
                            409,
                            "Worker Plan delivery changed during reconciliation",
                        )
                    for plan, version in approved_versions:
                        db.add(PlanApplication(
                            plan_id=plan.id,
                            plan_version_id=version.id,
                            application_type="chat_message",
                            target_task_id=current.id,
                            target_session_id=remote_result.get("session_id") or current.session_id,
                            user_log_id=prior_log.id,
                            applied_by=get_current_user_id(request) if request else None,
                            application_receipt_key=prepared.receipt_key,
                        ))
                    metadata = _raw_log_metadata(prior_log)
                    metadata["applied_plans"] = versioned_plan_snapshots(approved_versions)
                    prior_log.raw_json = json.dumps(metadata)
                    await db.commit()
                    from backend.services.plan_events import broadcast_plan_event

                    for plan, version in approved_versions:
                        await broadcast_plan_event(
                            event="plan_version_applied",
                            plan_id=plan.id,
                            target_task_id=current.id,
                            version_id=version.id,
                            user_log_id=prior_log.id,
                        )
                    remote_result["instance_id"] = None
                    remote_result["applied_plan_version_ids"] = wanted_ids
                    return remote_result
                raise HTTPException(
                    409,
                    "A previous Worker Plan application is awaiting receipt reconciliation; retry shortly",
                )

        remote_version_ids = []
        remote_confirmed_version_ids = []
        for plan, version in approved_versions:
            try:
                remote_version_id = await worker_proxy.materialize_plan_version(
                    worker=worker,
                    plan=plan,
                    version=version,
                )
            except Exception as exc:
                raise HTTPException(
                    502,
                    f"Plan Version #{version.id} could not be materialized on Worker",
                ) from exc
            remote_version_ids.append(remote_version_id)
            # Manager already performed the authoritative staleness check (and
            # required user confirmation when needed). Worker log ids belong
            # to a different database namespace, so re-evaluating them could
            # create a false stale conflict after Task migration.
            remote_confirmed_version_ids.append(remote_version_id)

        all_paths = body.file_paths or body.image_paths or []
        _IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".gif", ".webp"}
        attachments = [
            {
                "url": f"/api/uploads/{os.path.basename(p)}",
                "name": os.path.basename(p),
                "is_error": False,
                "is_image": os.path.splitext(p)[1].lower() in _IMAGE_EXTS,
            }
            for p in all_paths
        ]

        # 1. Persist the display copy only if the exact pre-network Worker
        # generation is still current.
        guarded = await db.execute(
            sa_update(Task)
            .where(*worker_task_generation_predicates(observed))
            .values(status=observed.status)
        )
        if guarded.rowcount != 1:
            await db.rollback()
            raise HTTPException(
                409,
                "Task Worker generation changed before chat could be sent",
            )
        log_metadata: dict = {"raw_content": model_message}
        if attachments:
            log_metadata["attachments"] = attachments
            log_metadata["file_paths"] = all_paths
        if sender_display_name:
            log_metadata["sender_name"] = sender_display_name
        manager_user_log = LogEntry(
            instance_id=None,
            task_id=current.id,
            event_type="user_message",
            role="user",
            content=display_content,
            raw_json=json.dumps(log_metadata) if log_metadata else None,
            is_error=False,
        )
        db.add(manager_user_log)
        application_receipt_key = str(uuid.uuid4()) if approved_versions else None
        manager_receipt = None
        await db.commit()

        # 2. Broadcast to the Manager frontend.
        broadcast_data = persisted_chat_event(manager_user_log, {
            "event_type": "user_message",
            "role": "user",
            "content": display_content,
            "raw_content": model_message,
            "image_paths": body.image_paths or [],
        })
        if sender_display_name:
            broadcast_data["sender_name"] = sender_display_name
        await broadcaster.broadcast(f"task:{current.id}", broadcast_data)

        # 3. Push attachments to the same Worker path.
        if all_paths:
            try:
                await worker_proxy.push_files(worker, all_paths)
            except Exception as e:
                raise HTTPException(503, f"附件同步到 Worker 失败: {e}")

        # 4. Ensure relay subscription before the remote turn can emit events.
        await worker_proxy.relay.subscribe_task(worker, current.id)
        if not terminal_pr_review_chat:
            await worker_proxy.sync_task_skill_selection(worker, current)
        # PR review mirrors stay permanently tool-free.  Their Worker-side
        # configuration is intentionally immutable and therefore needs no
        # Skill synchronization before a terminal discussion turn.

        # Persist the handoff receipt only after every preflight step succeeds.
        # From this commit onward the next network action is the idempotent
        # Worker request carrying this exact key.
        if approved_versions:
            from backend.models.plan import PlanApplicationReceipt

            manager_receipt = PlanApplicationReceipt(
                receipt_key=application_receipt_key,
                target_task_id=current.id,
                worker_id=worker.id,
                manager_user_log_id=manager_user_log.id,
                plan_version_ids=[version.id for _plan, version in approved_versions],
                status="prepared",
            )
            db.add(manager_receipt)
            await db.commit()

        # 5. The common operation lock is already held; asking WorkerProxy to
        # acquire it again would deadlock.
        try:
            result = await worker_proxy.proxy_to_worker(
                current,
                "POST",
                f"/api/tasks/{current.id}/chat",
                body={
                    "message": model_message,
                    "image_paths": body.image_paths,
                    "file_paths": body.file_paths,
                    "model": body.model,
                    "plan_task_ids": body.plan_task_ids,
                    "plan_version_ids": remote_version_ids or None,
                    "confirmed_stale_plan_version_ids": (
                        remote_confirmed_version_ids or None
                    ),
                    "confirmed_stale_plan_task_ids": (
                        body.confirmed_stale_plan_task_ids
                    ),
                    "expected_routing": (
                        body.expected_routing.model_dump(mode="json")
                        if body.expected_routing is not None
                        else None
                    ),
                    "plan_application_receipt_key": application_receipt_key,
                },
                operation_lock_held=True,
                pr_review_terminal_chat=terminal_pr_review_chat,
            )
        except Exception:
            remote_receipt = (
                await worker_proxy.get_plan_application_receipt(
                    worker, application_receipt_key
                )
                if application_receipt_key is not None
                else None
            )
            if (
                manager_receipt is not None
                and isinstance(remote_receipt, dict)
                and remote_receipt.get("delivery_status") == "uncertain"
            ):
                await _preserve_remote_uncertain_plan_receipt(
                    db,
                    receipt=manager_receipt,
                    remote_receipt=remote_receipt,
                    request=request,
                )
                raise HTTPException(
                    409,
                    "Worker Plan application launch outcome is uncertain; "
                    "administrator reconciliation is required",
                )
            if (
                remote_receipt is None
                or remote_receipt.get("status") != "committed"
                or not isinstance(remote_receipt.get("response"), dict)
                or remote_receipt.get("delivery_status")
                in {"failed", "cancelled", "uncertain"}
            ):
                raise
            result = remote_receipt["response"]

        # 6. A delayed response can only update the generation that issued the
        # request.  Even responses without a session id perform a no-op CAS so
        # reassignment/retry during the network await is reported as conflict.
        values = {"status": observed.status}
        if isinstance(result, dict) and result.get("session_id"):
            values["session_id"] = result["session_id"]
        changed = await db.execute(
            sa_update(Task)
            .where(*worker_task_generation_predicates(observed))
            .values(**values)
        )
        if changed.rowcount != 1:
            await db.rollback()
            raise HTTPException(
                409,
                "Task Worker assignment or generation changed while chat was in flight",
            )
        applied_plan_ids = (
            result.get("applied_plan_task_ids")
            if isinstance(result, dict)
            else None
        )
        if isinstance(applied_plan_ids, list):
            applied_at = datetime.utcnow()
            normalized_applied_ids: list[int] = []
            for plan_id in applied_plan_ids:
                if isinstance(plan_id, bool) or not isinstance(plan_id, int):
                    continue
                normalized_applied_ids.append(plan_id)
                local_applied = await db.execute(
                    sa_update(Task)
                    .where(
                        Task.id == plan_id,
                        Task.mode == "plan",
                        Task.plan_target_task_id == current.id,
                        Task.plan_approved.is_(True),
                        Task.plan_applied_at.is_(None),
                    )
                    .values(
                        plan_applied_at=applied_at,
                        plan_applied_to_session_id=(
                            result.get("session_id") or current.session_id
                        ),
                        plan_applied_log_id=manager_user_log.id,
                    )
                )
                if local_applied.rowcount != 1:
                    logger.warning(
                        "Worker applied Plan Task %s but Manager mirror could "
                        "not claim its local application row",
                        plan_id,
                    )
            if normalized_applied_ids:
                from backend.services.plan_tasks import applied_plan_snapshots

                rows = await db.execute(
                    select(Task).where(Task.id.in_(normalized_applied_ids))
                )
                plans_by_id = {plan.id: plan for plan in rows.scalars().all()}
                snapshot_plans = [
                    plans_by_id[plan_id]
                    for plan_id in normalized_applied_ids
                    if plan_id in plans_by_id
                ]
                manager_metadata = _raw_log_metadata(manager_user_log)
                manager_metadata["applied_plans"] = applied_plan_snapshots(
                    snapshot_plans
                )
                manager_user_log.raw_json = json.dumps(manager_metadata)
        applied_remote_version_ids = (
            result.get("applied_plan_version_ids")
            if isinstance(result, dict)
            else None
        )
        if approved_versions:
            if applied_remote_version_ids != remote_version_ids:
                await db.rollback()
                raise HTTPException(
                    502,
                    "Worker did not confirm the exact selected Plan Versions",
                )
            from backend.models.plan import PlanApplication
            from backend.services.plan_service import versioned_plan_snapshots

            if manager_receipt is not None:
                receipt_committed = await db.execute(
                    sa_update(PlanApplicationReceipt)
                    .where(
                        PlanApplicationReceipt.receipt_key
                        == manager_receipt.receipt_key,
                        PlanApplicationReceipt.status == "prepared",
                        PlanApplicationReceipt.delivery_status == "pending",
                    )
                    .values(
                        status="committed",
                        delivery_status="queued",
                        response=(result if isinstance(result, dict) else None),
                        updated_at=datetime.utcnow(),
                    )
                )
                if receipt_committed.rowcount != 1:
                    await db.rollback()
                    raise HTTPException(
                        409,
                        "Worker Plan delivery changed before Manager commit",
                    )

            for plan, version in approved_versions:
                db.add(
                    PlanApplication(
                        plan_id=plan.id,
                        plan_version_id=version.id,
                        application_type="chat_message",
                        target_task_id=current.id,
                        target_session_id=(
                            result.get("session_id") or current.session_id
                        ),
                        user_log_id=manager_user_log.id,
                        applied_by=(
                            get_current_user_id(request)
                            if request is not None
                            else None
                        ),
                        application_receipt_key=application_receipt_key,
                    )
                )
            manager_metadata = _raw_log_metadata(manager_user_log)
            manager_metadata["applied_plans"] = versioned_plan_snapshots(
                approved_versions
            )
            manager_user_log.raw_json = json.dumps(manager_metadata)
            try:
                await db.flush()
            except IntegrityError as exc:
                await db.rollback()
                raise HTTPException(
                    409,
                    "A selected Plan Version was applied concurrently",
                ) from exc
        await db.commit()

        if approved_versions:
            from backend.services.plan_events import broadcast_plan_event

            for plan, version in approved_versions:
                await broadcast_plan_event(
                    event="plan_version_applied",
                    plan_id=plan.id,
                    target_task_id=current.id,
                    version_id=version.id,
                    user_log_id=manager_user_log.id,
                )

        if isinstance(result, dict):
            result["instance_id"] = None  # Worker instance ids are not Manager ids.
            result["applied_plan_version_ids"] = [
                version.id for _plan, version in approved_versions
            ]
            if application_receipt_key is not None:
                result["plan_application_receipt_key"] = application_receipt_key
        return result


def _tool_summary(tool_input: str | None) -> str:
    """Extract a short one-line summary from tool_input JSON."""
    if not tool_input:
        return ""
    try:
        parsed = json.loads(tool_input)
        if isinstance(parsed, dict):
            if cmd := parsed.get("command"):
                return cmd[:120] + "..." if len(cmd) > 120 else cmd
            if fp := parsed.get("file_path"):
                return fp
            if pat := parsed.get("pattern"):
                path = parsed.get("path", "")
                return f"{pat} in {path}" if path else pat
            if q := parsed.get("query"):
                return q[:120] + "..." if len(q) > 120 else q
    except (json.JSONDecodeError, TypeError):
        pass
    return ""


@router.get("/{task_id}/chat/history")
async def get_chat_history(
    task_id: int, request: Request,
    limit: int = 0,
    before_id: int = 0,
    compact: bool = True,
    touch: bool = False,
    db: AsyncSession = Depends(get_db),
):
    """Get chat-formatted history for a task.

    compact=True (default): tool_input/tool_output replaced with short summary.
    compact=False: full tool_input/tool_output included (truncated at 20k chars).
    before_id: only return messages with id < before_id (for pagination).
    touch=True: count this fetch as a user access (move-to-front). Only the
    frontend's initial page load sends it — pagination, background polling and
    stale old-version clients must NOT reorder tasks (prod task 68 实录：
    一个旧版前端残留标签页每隔十几分钟轮询一次，任务在列表里来回跳).
    """
    from backend.models.task import Task as _T2
    _task_check = await db.get(_T2, task_id)
    if _task_check:
        await require_task_access(request, _task_check, db)

    task = await db.get(Task, task_id)
    if not task:
        raise HTTPException(404, "Task not found")

    if touch:
        from datetime import datetime as _dt
        task.last_accessed_at = _dt.utcnow()
        await db.commit()

    allowed = ["user_message", "message", "result", "tool_use", "tool_result", "system_init", "system_event", "thinking", "process_exit"]
    # Noisy telemetry must be excluded in SQL, before LIMIT applies. Filtering
    # after the query made pages come back short (< limit), which the client
    # reads as "history exhausted" — older messages became unreachable.
    noisy_system = ["task_progress", "thinking_tokens", "token_usage", "api_request", "api_response"]
    cols = [
        LogEntry.id, LogEntry.role, LogEntry.event_type, LogEntry.content,
        LogEntry.tool_name, LogEntry.tool_input, LogEntry.tool_output,
        LogEntry.is_error, LogEntry.loop_iteration, LogEntry.timestamp,
        LogEntry.raw_json, LogEntry.task_retry_count,
    ]
    conditions = [
        LogEntry.task_id == task_id,
        LogEntry.event_type.in_(allowed),
        not_(and_(
            LogEntry.event_type == "system_event",
            LogEntry.content.in_(noisy_system),
        )),
    ]
    if limit > 0:
        # Over-fetch to compensate for Python-level filtering (message+user
        # rows are skipped below). Historical collab noise can occur in long
        # consecutive runs, so keep paging until it cannot consume the whole
        # visible page.
        visible_target = limit + 20
        batch_size = max(visible_target, 500)
        rows_desc = []
        cursor = before_id if before_id > 0 else None
        while len(rows_desc) < visible_target:
            page_conditions = list(conditions)
            if cursor is not None:
                page_conditions.append(LogEntry.id < cursor)
            stmt = (
                select(*cols)
                .where(*page_conditions)
                .order_by(LogEntry.id.desc())
                .limit(batch_size)
            )
            result = await db.execute(stmt)
            batch = result.all()
            if not batch:
                break
            for row in batch:
                if _is_legacy_codex_collab_completed(
                    row.event_type,
                    row.content,
                    row.raw_json,
                ):
                    continue
                rows_desc.append(row)
                if len(rows_desc) >= visible_target:
                    break
            if len(batch) < batch_size:
                break
            cursor = batch[-1].id
        rows = list(reversed(rows_desc))
    else:
        if before_id > 0:
            conditions.append(LogEntry.id < before_id)
        stmt = (
            select(*cols)
            .where(*conditions)
            .order_by(LogEntry.id.asc())
        )
        result = await db.execute(stmt)
        rows = result.all()

    _TRUNCATE = 20_000  # chars; tool outputs can be huge (file reads, bash output)

    # Older application rows predate message-level Plan snapshots. Reconstruct
    # them while the Plan still exists so already-applied production history is
    # upgraded on read; new rows keep an immutable copy in raw_json and remain
    # explainable even if the Plan is later deleted.
    historical_applied_plans: dict[int, list[dict[str, object]]] = {}
    history_log_ids = [
        row.id for row in rows if row.event_type == "user_message"
    ]
    if history_log_ids:
        from backend.services.plan_tasks import applied_plan_snapshots

        historical_rows = await db.execute(
            select(Task)
            .where(
                Task.mode == "plan",
                Task.plan_applied_log_id.in_(history_log_ids),
            )
            .order_by(Task.id.asc())
        )
        for historical_plan in historical_rows.scalars().all():
            log_id = historical_plan.plan_applied_log_id
            if log_id is None:
                continue
            historical_applied_plans.setdefault(log_id, []).extend(
                applied_plan_snapshots([historical_plan])
            )

    messages = []
    current_source = None  # track monitor context
    for row in rows:
        if _is_legacy_codex_collab_completed(
            row.event_type,
            row.content,
            row.raw_json,
        ):
            continue
        tool_input = row.tool_input
        tool_output = row.tool_output

        if compact and row.event_type in ("tool_use", "tool_result"):
            summary = _tool_summary(tool_input) if row.event_type == "tool_use" else None
            tool_input = summary or None
            tool_output = None
        else:
            if tool_input and len(tool_input) > _TRUNCATE:
                tool_input = tool_input[:_TRUNCATE] + "\n…(truncated)"
            if tool_output and len(tool_output) > _TRUNCATE:
                tool_output = tool_output[:_TRUNCATE] + "\n…(truncated)"

        attachments = None
        image_urls = None
        source = None
        raw_content = None
        applied_plans = None
        item_id = None
        turn_id = None
        native_item_type = None
        native_item_status = None
        if row.raw_json:
            try:
                raw = json.loads(row.raw_json)
                if isinstance(raw, dict):
                    item = raw.get("item")
                    turn = raw.get("turn")
                    native_item = (
                        raw.get("item_id")
                        or raw.get("itemId")
                        or (item.get("id") if isinstance(item, dict) else None)
                    )
                    native_turn = (
                        raw.get("turn_id")
                        or raw.get("turnId")
                        or (turn.get("id") if isinstance(turn, dict) else None)
                    )
                    item_id = str(native_item) if native_item else None
                    turn_id = str(native_turn) if native_turn else None
                    if isinstance(item, dict):
                        item_type = item.get("type")
                        item_status = item.get("status")
                        native_item_type = (
                            str(item_type) if item_type not in (None, "") else None
                        )
                        native_item_status = (
                            str(item_status)
                            if item_status not in (None, "")
                            else None
                        )
                    if raw.get("attachments"):
                        attachments = raw["attachments"]
                        image_urls = [a["url"] for a in attachments if a.get("is_image")]
                    elif raw.get("image_urls"):
                        image_urls = raw["image_urls"]
                        attachments = [{"url": u, "name": u.split("/")[-1], "is_image": True} for u in image_urls]
                    if raw.get("source"):
                        source = raw["source"]
                    if isinstance(raw.get("raw_content"), str):
                        raw_content = raw["raw_content"]
                    if isinstance(raw.get("applied_plans"), list):
                        applied_plans = raw["applied_plans"]
            except (json.JSONDecodeError, TypeError):
                pass
        if applied_plans is None:
            applied_plans = historical_applied_plans.get(row.id)

        if row.event_type in ("user_message", "system_event") and source:
            current_source = source
        elif row.event_type == "user_message":
            current_source = None
        msg_source = current_source

        # event_type=message with role=user are CC internal messages (compact
        # summaries, task-notifications, local-command caveats) — not real user
        # input (which uses event_type=user_message). Hide them from chat.
        if row.event_type == "message" and row.role == "user":
            continue

        messages.append({
            "id": row.id,
            "role": row.role or ("assistant" if row.event_type in ("message", "result") else "system"),
            "event_type": row.event_type,
            "content": row.content,
            "tool_name": row.tool_name,
            "tool_input": tool_input,
            "tool_output": tool_output,
            "is_error": row.is_error,
            "loop_iteration": row.loop_iteration,
            "task_retry_count": row.task_retry_count,
            "timestamp": (row.timestamp.isoformat() + "Z") if row.timestamp else None,
            "image_urls": image_urls or None,
            "attachments": attachments,
            "source": msg_source,
            "raw_content": raw_content,
            "applied_plans": applied_plans,
            "item_id": item_id,
            "turn_id": turn_id,
            "native_item_type": native_item_type,
            "native_item_status": native_item_status,
        })

    # Trim back to requested limit (we over-fetched to compensate for
    # Python-level filtering). Keep the newest messages (end of list).
    if limit > 0 and len(messages) > limit:
        messages = messages[-limit:]
    return messages


@router.get("/{task_id}/chat/{message_id}/detail")
async def get_message_detail(
    task_id: int,
    message_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    _t = await db.get(Task, task_id)
    if _t:
        await require_task_access(request, _t, db)
    """Get full tool_input/tool_output for a single message (lazy-load on expand)."""
    _TRUNCATE = 20_000

    stmt = (
        select(LogEntry.id, LogEntry.tool_input, LogEntry.tool_output, LogEntry.content)
        .where(LogEntry.id == message_id, LogEntry.task_id == task_id)
    )
    result = await db.execute(stmt)
    row = result.one_or_none()
    if not row:
        raise HTTPException(404, "Message not found")

    tool_input = row.tool_input
    tool_output = row.tool_output
    if tool_input and len(tool_input) > _TRUNCATE:
        tool_input = tool_input[:_TRUNCATE] + "\n…(truncated)"
    if tool_output and len(tool_output) > _TRUNCATE:
        tool_output = tool_output[:_TRUNCATE] + "\n…(truncated)"

    return {
        "id": row.id,
        "tool_input": tool_input,
        "tool_output": tool_output,
        "content": row.content,
    }


class InjectMessage(BaseModel):
    message: str = ""
    image_paths: list[str] | None = None
    file_paths: list[str] | None = None
    attachments: list[dict[str, Any]] | None = None
    expected_routing: TaskRoutingExpectation | None = None

    @model_validator(mode="after")
    def require_text_or_attachment(self):
        if not self.message.strip() and not (
            self.file_paths or self.image_paths
        ):
            raise ValueError("message or attachment is required")
        return self


def _validated_inject_attachments(
    body: InjectMessage,
) -> list[ValidatedUploadAttachment]:
    try:
        return validate_upload_attachments(
            file_paths=body.file_paths,
            image_paths=body.image_paths,
            attachments=body.attachments,
        )
    except UploadAttachmentValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def _inject_transport_content(
    message: str,
    uploads: list[ValidatedUploadAttachment],
) -> str:
    """Build the text seen by PTY and the text item seen by Codex."""

    if not uploads:
        return message
    lines = [
        "用户在当前执行中补充了以下附件。请先实际读取附件，再结合本次补充继续工作："
    ]
    for upload in uploads:
        tool = "Read/View Image" if upload.is_image else "Read"
        lines.append(f"- {upload.path}（使用 {tool}）")
    attachment_instruction = "\n".join(lines)
    return (
        f"{message}\n\n{attachment_instruction}"
        if message
        else attachment_instruction
    )


def _codex_inject_input_items(
    content: str,
    uploads: list[ValidatedUploadAttachment],
) -> list[dict[str, str]]:
    items: list[dict[str, str]] = [{"type": "text", "text": content}]
    for upload in uploads:
        if upload.is_image:
            items.append({"type": "localImage", "path": upload.path})
        else:
            items.append({
                "type": "mention",
                "name": upload.name,
                "path": upload.path,
            })
    return items


async def _inject_display_content(
    request: Request,
    db: AsyncSession,
    raw_content: str,
) -> tuple[str, str | None]:
    display_content = raw_content
    sender_display_name = await _sender_display_name(request, db)
    if sender_display_name:
        display_content = f"[{sender_display_name}] {raw_content}"
    return display_content, sender_display_name


async def _store_injected_message(
    *,
    db: AsyncSession,
    broadcaster,
    task: Task,
    raw_content: str,
    display_content: str,
    sender_display_name: str | None,
    uploads: list[ValidatedUploadAttachment],
    instance_id: int | None,
) -> None:
    attachments = [upload.public_dict() for upload in uploads]
    file_paths = [upload.path for upload in uploads]
    image_paths = [
        upload.path for upload in uploads if upload.is_image
    ]
    raw_metadata: dict[str, Any] = {
        "source": "inject",
        "raw_content": raw_content,
    }
    if attachments:
        raw_metadata.update({
            "attachments": attachments,
            "file_paths": file_paths,
            "image_paths": image_paths,
        })
    if sender_display_name:
        raw_metadata["sender_name"] = sender_display_name
    entry = LogEntry(
        instance_id=instance_id,
        task_id=task.id,
        event_type="user_message",
        role="user",
        content=display_content,
        raw_json=json.dumps(raw_metadata, ensure_ascii=False),
        is_error=False,
    )
    db.add(entry)
    await db.commit()

    event: dict[str, Any] = persisted_chat_event(entry, {
        "event_type": "user_message",
        "role": "user",
        "content": display_content,
        "source": "inject",
        "raw_content": raw_content,
        "attachments": attachments,
        "image_urls": [
            attachment["url"]
            for attachment in attachments
            if attachment["is_image"]
        ],
    })
    if sender_display_name:
        event["sender_name"] = sender_display_name
    await broadcaster.broadcast(f"task:{task.id}", event)


@router.get("/{task_id}/inject-capabilities")
async def inject_capabilities(
    task_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    task = await db.get(Task, task_id)
    if task is None:
        raise HTTPException(404, "Task not found")
    await require_task_access(request, task, db)
    return {
        "attachment_protocol": 1,
        "codex_native_inputs": True,
    }


@router.post("/{task_id}/inject")
async def inject_message(
    task_id: int,
    body: InjectMessage,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Inject a message into the task's currently running turn."""
    task = await db.get(Task, task_id)
    if task is None:
        raise HTTPException(404, "Task not found")
    await require_task_access(request, task, db)

    # Serialize with routing edits and migration.  Re-read after acquiring the
    # lock so the expected route and transport are one admission decision.
    await db.rollback()
    async with get_task_operation_lock(task_id):
        db.expire_all()
        task = await db.get(Task, task_id)
        if task is None:
            raise HTTPException(404, "Task not found")
        await require_task_access(request, task, db)
        if task_is_pr_review_superseded(task):
            raise HTTPException(
                409,
                "This PR review task was superseded by a newer push",
            )

        from backend.api.tasks import (
            _require_expected_task_routing,
            _require_no_pending_worker_routing,
            _require_pr_review_chat_allowed,
        )

        if task.worker_id is not None:
            raise HTTPException(
                400,
                "Worker task 暂不支持执行中注入",
            )
        _require_no_pending_worker_routing(task)
        await _require_pr_review_chat_allowed(
            db,
            task_id,
        )

        _require_expected_task_routing(
            task,
            body.expected_routing,
            effective_model=task.model,
        )
        if task.shared_from_id is not None:
            raise HTTPException(
                400,
                "Shared task 暂不支持执行中注入",
            )
        if not task.session_id:
            raise HTTPException(400, "Task has no session yet")

        uploads = _validated_inject_attachments(body)

        from backend.main import instance_manager, broadcaster

        provider = (task.provider or "claude").lower()
        transport_content = _inject_transport_content(
            body.message,
            uploads,
        )
        if provider == "codex":
            from backend.config import settings

            if not settings.codex_app_server_enabled:
                raise HTTPException(
                    400,
                    "Codex app-server 未开启，当前 exec 链路不支持执行中注入",
                )
            if uploads:
                ok = await instance_manager.inject_codex_message(
                    task.session_id,
                    transport_content,
                    input_items=_codex_inject_input_items(
                        transport_content,
                        uploads,
                    ),
                )
            else:
                ok = await instance_manager.inject_codex_message(
                    task.session_id,
                    transport_content,
                )
            unavailable_detail = (
                "注入失败：当前 Codex turn 已结束、暂不可 steer、附件输入被 "
                "transport 拒绝，或正在使用 exec fallback；空闲时请关闭注入"
                "模式直接发普通消息"
            )
        elif provider == "claude":
            if not instance_manager.has_pty_session(task.session_id):
                raise HTTPException(
                    400,
                    (
                        "当前 Claude turn 使用直连进程，不支持执行中注入；"
                        "请关闭注入模式后发送普通消息"
                    )
                    if instance_manager.pty_mode_enabled
                    else "当前 Claude turn 不由 PTY 管理，无法执行中注入",
                )
            try:
                if uploads:
                    ok = await instance_manager.inject_pty_message(
                        task.session_id,
                        transport_content,
                        require_host_file_access=True,
                    )
                else:
                    ok = await instance_manager.inject_pty_message(
                        task.session_id,
                        transport_content,
                    )
            except Exception as exc:
                from backend.services.instance_manager import (
                    LiveAttachmentInjectionUnsupportedError,
                )

                if isinstance(
                    exc,
                    LiveAttachmentInjectionUnsupportedError,
                ):
                    raise HTTPException(
                        409,
                        "当前 Claude PTY 运行在隔离容器中，无法安全访问上传"
                        "附件；附件未注入。请在非隔离任务中使用执行中附件注入",
                    ) from exc
                raise
            unavailable_detail = (
                "注入失败：没有正在运行的 turn。注入仅在任务执行中可用"
                "（用于中途补充指令）；空闲时请关闭注入模式直接发普通消息"
            )
        else:
            raise HTTPException(
                400,
                f"Provider {provider} 不支持执行中注入",
            )

        if not ok:
            raise HTTPException(409, unavailable_detail)

        display_content, sender_display_name = await _inject_display_content(
            request,
            db,
            body.message,
        )
        await _store_injected_message(
            db=db,
            broadcaster=broadcaster,
            task=task,
            raw_content=body.message,
            display_content=display_content,
            sender_display_name=sender_display_name,
            uploads=uploads,
            instance_id=task.instance_id,
        )
        return {
            "ok": True,
            "injected": True,
            "attachment_count": len(uploads),
        }


class PermissionDecision(BaseModel):
    behavior: str  # "allow" | "deny"


@router.post("/{task_id}/permissions/{request_id}")
async def resolve_permission(
    task_id: int,
    request_id: str,
    body: PermissionDecision,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    if body.behavior not in ("allow", "deny"):
        raise HTTPException(400, "behavior must be 'allow' or 'deny'")

    task = await db.get(Task, task_id)
    if task:
        await require_task_access(request, task, db)
    if not task:
        raise HTTPException(404, "Task not found")

    from backend.main import instance_manager
    ok = await instance_manager.resolve_pty_permission(request_id, body.behavior)
    if not ok:
        raise HTTPException(410, "权限请求已过期或不存在（CC 侧可能已超时默认拒绝）")
    return {"ok": True, "behavior": body.behavior}


# ---------------------------------------------------------------------------
# Task Distill — extract reusable skill from conversation history
# ---------------------------------------------------------------------------

async def _collect_conversation_for_distill(task_id: int, db: AsyncSession) -> str:
    """Collect conversation history for task skill distillation."""
    from backend.services.skill_distill import TASK_DISTILL_MAX_CHARS

    result = await db.execute(
        select(
            LogEntry.event_type,
            LogEntry.role,
            LogEntry.content,
            LogEntry.tool_name,
            LogEntry.is_error,
            LogEntry.raw_json,
        )
        .where(
            LogEntry.task_id == task_id,
            LogEntry.event_type.in_(["user_message", "message", "tool_use", "tool_result"]),
        )
        .order_by(LogEntry.id.asc())
    )
    rows = result.all()

    parts: list[str] = []
    total = 0
    for row in rows:
        event_type, role, content, tool_name, is_error, raw_json = row
        if not content:
            continue

        if event_type == "user_message":
            model_content = content
            if raw_json:
                try:
                    raw = json.loads(raw_json)
                    if isinstance(raw, dict) and isinstance(raw.get("raw_content"), str):
                        model_content = raw["raw_content"]
                except (json.JSONDecodeError, TypeError):
                    pass
            line = f"[User]: {model_content[:2000]}"
        elif event_type == "message" and role == "assistant":
            line = f"[Assistant]: {content[:2000]}"
        elif event_type == "tool_use" and tool_name:
            line = f"[Tool: {tool_name}]: {content[:500]}"
        elif event_type == "tool_result":
            prefix = "[Error]" if is_error else "[Result]"
            line = f"{prefix}: {content[:500]}"
        else:
            continue

        total += len(line)
        if total > TASK_DISTILL_MAX_CHARS:
            parts.append("... (conversation truncated)")
            break
        parts.append(line)

    return "\n".join(parts)


class DistillRequest(BaseModel):
    custom_instruction: str | None = None
    expected_routing: TaskRoutingExpectation | None = None


class DistillSaveRequest(BaseModel):
    name: str
    description: str = ""
    content: str


@router.post("/{task_id}/distill")
async def distill_task(
    task_id: int,
    request: Request,
    body: DistillRequest = DistillRequest(),
    db: AsyncSession = Depends(get_db),
):
    """Distill a task's conversation into a reusable skill (markdown).

    Uses the task's provider and returns a card for user preview/editing.
    """
    # A distill request starts a separate provider call.  Keep the same
    # operation barrier used by Task routing updates for the entire call:
    # otherwise a Standard preflight can race with a Standard→Fast update and
    # silently issue a non-priority request after the UI already shows Fast.
    await db.rollback()
    async with get_task_operation_lock(task_id):
        db.expire_all()
        task = await db.get(Task, task_id)
        if not task:
            raise HTTPException(404, "Task not found")
        await require_task_control(request, task, db)
        from backend.api.tasks import _require_expected_task_routing

        _require_expected_task_routing(
            task,
            body.expected_routing,
            effective_model=task.model,
        )
        if (
            (task.provider or "claude").lower() == "codex"
            and (task.codex_service_tier or "default") == "priority"
        ):
            raise HTTPException(
                409,
                "Codex Fast distillation is not available because the distill "
                "transport cannot confirm priority admission; switch this Task "
                "to Standard before distilling",
            )

        conversation = await _collect_conversation_for_distill(task_id, db)
        if not conversation.strip():
            raise HTTPException(400, "No conversation history to distill")

        from backend.main import (
            cloudrouter_store,
            codex_pool,
            dispatcher,
            instance_manager,
        )
        from backend.services.skill_distill import (
            CodexDistillAccountUnavailableError,
            TaskDistillError,
            TaskDistillTimeoutError,
            distill_task_conversation,
        )
        title = (
            task.title
            or (task.description[:100] if task.description else "")
            or "Untitled"
        )
        try:
            result = await distill_task_conversation(
                title=title,
                conversation=conversation,
                provider=task.provider or "claude",
                custom_instruction=body.custom_instruction,
                claude_pool=dispatcher.pool,
                codex_pool=codex_pool,
                codex_account_id=(task.metadata_ or {}).get(
                    "codex_account_id"
                ),
                instance_manager=instance_manager,
                cloudrouter_store=cloudrouter_store,
            )
        except TaskDistillTimeoutError as exc:
            raise HTTPException(504, str(exc)) from exc
        except CodexDistillAccountUnavailableError as exc:
            raise HTTPException(503, str(exc)) from exc
        except TaskDistillError as exc:
            detail = (exc.stderr or exc.stdout).strip()[:500]
            logger.error(
                "distill: %s failed. stdout=%s stderr=%s",
                exc.provider,
                exc.stdout[:500],
                exc.stderr[:500],
            )
            message = str(exc)
            if detail:
                message = f"{message}: {detail}"
            raise HTTPException(502, message) from exc

        suggested_name = (
            task.title or task.description or "untitled"
        )[:50].strip()

        return {
            "task_id": task_id,
            "suggested_name": suggested_name,
            "content": result["content"],
            "provider": result["provider"],
            "model": result["model"],
        }


@router.post("/{task_id}/distill/save")
async def save_distilled_skill(
    task_id: int, request: Request,
    body: DistillSaveRequest,
    db: AsyncSession = Depends(get_db),
):
    """Save a distilled skill as a UserSkill."""
    task = await db.get(Task, task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    await require_task_control(request, task, db)

    existing = await db.execute(
        select(UserSkill).where(UserSkill.name == body.name)
    )
    if existing.scalar_one_or_none():
        raise HTTPException(409, f"Skill with name '{body.name}' already exists")

    skill = UserSkill(
        name=body.name,
        description=body.description or f"Distilled from task #{task_id}",
        content=body.content,
    )
    db.add(skill)
    await db.commit()
    await db.refresh(skill)

    return {
        "id": skill.id,
        "name": skill.name,
        "description": skill.description,
        "content": skill.content,
    }
