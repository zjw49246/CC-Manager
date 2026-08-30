"""ask_user 端点 — 拦截内置 AskUserQuestion，转前端卡片再把答案喂回模型。

- POST /api/ask-user/wait        ：hook 脚本调用，阻塞直到用户回答 / 超时
- GET  /api/tasks/{id}/ask-user/pending     ：前端重连时回填活跃卡片
- POST /api/tasks/{id}/ask-user/{request_id}：前端卡片回包 → resolve

阻塞等待期间**不持有任何 DB 连接**：所有 DB 操作都用独立的短生命周期 session，
await future 时不占连接（否则一个挂起的提问会长时间占住连接池）。
"""
from __future__ import annotations

import asyncio
import json

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.api.deps import lock_task_effect_access, require_task_access
from backend.database import async_session, get_db
from backend.models.log_entry import LogEntry
from backend.models.task import Task
from backend.services.ask_user import (
    AskUserRevocation,
    ask_user_registry,
    format_answer_reason,
)
from backend.services.cancellation import settle_awaitable

router = APIRouter(prefix="/api", tags=["ask-user"])


class AskUserWaitRequest(BaseModel):
    session_id: str
    questions: list[dict]
    cwd: str | None = None
    tool_use_id: str | None = None


class AskUserAnswerItem(BaseModel):
    labels: list[str] = Field(default_factory=list, max_length=100)
    text: str | None = Field(default=None, max_length=4000)


class AskUserAnswer(BaseModel):
    # 与 questions 对齐：每项一个回答
    answers: list[AskUserAnswerItem] = Field(max_length=100)


async def _settle_despite_cancellation(awaitable):
    """Finish an answer-audit commit before delivering request cancellation."""

    return await settle_awaitable(awaitable)


@router.post("/ask-user/wait")
async def ask_user_wait(body: AskUserWaitRequest, request: Request):
    """hook 脚本调用：登记提问、广播卡片，阻塞直到用户回答或超时。

    返回 {answered: true, reason} → hook 用 deny+reason 把答案喂回模型；
    返回 {answered: false, ...} → hook 放行原生 AskUserQuestion（兜底）。
    """
    from backend.config import settings
    from backend.main import broadcaster

    # This endpoint is called by CCM's local hook, which normally authenticates
    # with a Task-bound scoped token. A user JWT must never impersonate a
    # model tool call, create a fake prompt card, or receive another user's
    # answer. No-auth deployments intentionally preserve their open semantics.
    auth_type = getattr(request.state, "auth_type", None)
    if settings.auth_token and auth_type not in {"token", "internal_service"}:
        raise HTTPException(403, "Internal hook authentication required")

    if not body.questions:
        return {"answered": False, "reason": "no questions"}

    claims = getattr(request.state, "internal_service_claims", None)
    task_predicates = [
        Task.session_id == body.session_id,
        Task.status.in_(["in_progress", "executing"]),
    ]
    if auth_type == "internal_service":
        from backend.services.internal_service_auth import internal_task_id

        claimed_task_id = internal_task_id(claims)
        claimed_incarnation = getattr(claims, "task_incarnation_id", None)
        claimed_retry = getattr(claims, "task_retry_count", None)
        claimed_turn = getattr(claims, "task_turn_generation", None)
        claimed_status = getattr(claims, "task_status", None)
        if (
            claimed_task_id is None
            or not claimed_incarnation
            or claimed_retry is None
            or claimed_turn is None
            or claimed_status not in {"in_progress", "executing"}
        ):
            raise HTTPException(403, "Internal hook Task generation is invalid")
        task_predicates.extend((
            Task.id == claimed_task_id,
            Task.incarnation_id == claimed_incarnation,
            Task.retry_count == claimed_retry,
            Task.turn_generation == claimed_turn,
            Task.status == claimed_status,
        ))

    # Bind the session to the exact active Task generation carried by a scoped
    # hook. Deployment-token compatibility still resolves the newest active
    # owner, but cannot create a card for a terminal Task.
    async with async_session() as db:
        task = (
            await db.execute(
                select(Task)
                .where(*task_predicates)
                .order_by(Task.id.desc())
            )
        ).scalars().first()
        if task is None:
            if auth_type == "internal_service":
                raise HTTPException(403, "Internal hook Task generation is stale")
            # 非 CCM 管理的 session → 放行，让原生工具按默认行为处理
            return {"answered": False, "no_session": True}
        task_id = task.id
        task_incarnation_id = task.incarnation_id
        task_retry_count = task.retry_count
        task_turn_generation = task.turn_generation
        task_status = task.status
        if not task_incarnation_id:
            raise HTTPException(
                403,
                "Internal hook Task has no stable incarnation identity",
            )

    timeout = max(10, int(getattr(settings, "ask_user_timeout", 1800)))

    summary = _questions_summary(body.questions)

    # 落库（审计用，不进 chat 历史 allowed）+ 标记 task 未读 + 广播活跃卡片
    # has_unread 让任务列表亮起未读点，即便用户当前不在该 task 页面也能察觉。
    pending = None
    async with async_session() as db:
        fenced = await db.execute(
            update(Task)
            .where(
                Task.id == task_id,
                Task.incarnation_id == task_incarnation_id,
                Task.session_id == body.session_id,
                Task.retry_count == task_retry_count,
                Task.turn_generation == task_turn_generation,
                Task.status == task_status,
                Task.status.in_(["in_progress", "executing"]),
            )
            .values(has_unread=True)
        )
        if fenced.rowcount != 1:
            raise HTTPException(
                403,
                "Internal hook Task generation is stale",
            )
        locked_task = await db.get(Task, task_id, populate_existing=True)
        if locked_task is None:
            raise HTTPException(
                403,
                "Internal hook Task generation is stale",
            )
        pending = ask_user_registry.create(
            task_id=task_id,
            task_incarnation_id=task_incarnation_id,
            task_retry_count=task_retry_count,
            task_turn_generation=task_turn_generation,
            task_status=task_status,
            session_id=body.session_id,
            questions=body.questions,
            tool_use_id=body.tool_use_id,
        )
        try:
            db.add(LogEntry(
                instance_id=locked_task.instance_id or 1,
                task_id=task_id,
                event_type="ask_user_question",
                role="system",
                content=summary,
                tool_name="AskUserQuestion",
                tool_input=json.dumps(body.questions, ensure_ascii=False),
                raw_json=json.dumps(
                    {
                        "request_id": pending.request_id,
                        "task_incarnation_id": task_incarnation_id,
                    },
                    ensure_ascii=False,
                ),
            ))
            await db.commit()
        except BaseException:
            ask_user_registry.discard(pending.request_id)
            raise

    assert pending is not None

    # 该 task 频道：渲染内联卡片（用户正在看这个 task 时）
    await broadcaster.broadcast(f"task:{task_id}", {
        "event_type": "ask_user_question",
        "request_id": pending.request_id,
        "task_incarnation_id": task_incarnation_id,
        "questions": body.questions,
        "timeout_seconds": timeout,
    })
    # 全局 tasks 频道：弹出全局通知，让在别的页面的用户也能看到并跳转过来
    await broadcaster.broadcast("tasks", {
        "event": "ask_user_pending",
        "task_id": task_id,
        "request_id": pending.request_id,
        "task_incarnation_id": task_incarnation_id,
        "summary": summary,
    })

    try:
        answers = await asyncio.wait_for(pending.future, timeout=timeout)
        if isinstance(answers, AskUserRevocation):
            await broadcaster.broadcast(f"task:{task_id}", {
                "event_type": "ask_user_resolved",
                "request_id": pending.request_id,
                "task_incarnation_id": task_incarnation_id,
                "revoked": True,
            })
            await broadcaster.broadcast("tasks", {
                "event": "ask_user_resolved",
                "task_id": task_id,
                "request_id": pending.request_id,
                "task_incarnation_id": task_incarnation_id,
            })
            return {
                "answered": False,
                "revoked": True,
                "reason": answers.reason,
            }
    except asyncio.TimeoutError:
        ask_user_registry.discard(pending.request_id)
        await broadcaster.broadcast(f"task:{task_id}", {
            "event_type": "ask_user_resolved",
            "request_id": pending.request_id,
            "task_incarnation_id": task_incarnation_id,
            "timed_out": True,
        })
        await broadcaster.broadcast("tasks", {
            "event": "ask_user_resolved",
            "task_id": task_id,
            "request_id": pending.request_id,
            "task_incarnation_id": task_incarnation_id,
        })
        return {"answered": False, "timed_out": True}
    except asyncio.CancelledError:
        # hook 断开连接 → 清理 pending，避免泄漏
        ask_user_registry.discard(pending.request_id)
        raise
    finally:
        ask_user_registry.discard(pending.request_id)

    reason = format_answer_reason(body.questions, answers)
    return {"answered": True, "reason": reason, "answers": answers}


@router.get("/tasks/{task_id}/ask-user/pending")
async def ask_user_pending(
    task_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """前端重连时回填仍在等待回答的卡片。"""
    task = await db.get(Task, task_id)
    if task is None:
        raise HTTPException(404, "Task not found")
    await require_task_access(request, task, db)
    if not task.incarnation_id:
        return {"pending": []}
    ask_user_registry.discard_stale_for_task(
        task_id,
        task.incarnation_id,
        task.retry_count,
        task.turn_generation,
        task.status,
    )
    if task.status not in {"in_progress", "executing"}:
        return {"pending": []}
    pendings = ask_user_registry.list_for_task(
        task_id,
        task.incarnation_id,
        task.retry_count,
        task.turn_generation,
        task.status,
    )
    return {
        "pending": [
            {"request_id": p.request_id, "questions": p.questions}
            for p in pendings
        ]
    }


@router.get("/ask-user/pending")
async def ask_user_pending_all(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """全局：所有仍在等待回答的提问。

    前端刷新/重连时回填全局通知，让用户即便不在对应 task 页面也能看到
    哪些任务正在等待回答（避免 WS 卡片 live-only 在刷新后丢失）。
    """
    pendings = []
    for pending in ask_user_registry.list_all():
        task = await db.get(Task, pending.task_id)
        if (
            task is None
            or task.incarnation_id != pending.task_incarnation_id
            or task.session_id != pending.session_id
            or task.retry_count != pending.task_retry_count
            or task.turn_generation != pending.task_turn_generation
            or task.status != pending.task_status
            or task.status not in {"in_progress", "executing"}
        ):
            ask_user_registry.discard(pending.request_id)
            continue
        try:
            await require_task_access(request, task, db)
        except HTTPException:
            continue
        pendings.append(pending)
    return {
        "pending": [
            {
                "task_id": p.task_id,
                "request_id": p.request_id,
                "summary": _questions_summary(p.questions),
            }
            for p in pendings
        ]
    }


@router.post("/tasks/{task_id}/ask-user/{request_id}")
async def ask_user_submit(
    task_id: int,
    request_id: str,
    body: AskUserAnswer,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """前端卡片回包：把用户的选择 resolve 给阻塞中的 hook。"""
    from backend.main import broadcaster

    task = await db.get(Task, task_id)
    if task is None:
        raise HTTPException(404, "Task not found")
    await require_task_access(request, task, db)

    pending = ask_user_registry.get(request_id)
    if (
        pending is None
        or pending.task_id != task_id
        or pending.task_incarnation_id != task.incarnation_id
        or pending.session_id != task.session_id
        or pending.task_retry_count != task.retry_count
        or pending.task_turn_generation != task.turn_generation
        or pending.task_status != task.status
        or task.status not in {"in_progress", "executing"}
    ):
        if pending is not None:
            ask_user_registry.discard(pending.request_id)
        raise HTTPException(410, "提问已过期或不存在（hook 侧可能已超时放行）")

    answers = [
        answer.model_dump(exclude_none=True)
        for answer in body.answers
    ]
    claim_id = ask_user_registry.claim_answer(
        request_id,
        task_id=task_id,
        task_incarnation_id=pending.task_incarnation_id,
        task_retry_count=pending.task_retry_count,
        task_turn_generation=pending.task_turn_generation,
        task_status=pending.task_status,
        session_id=pending.session_id,
    )
    if claim_id is None:
        raise HTTPException(410, "提问正在回答、已过期或不存在")

    # End the authorization read transaction, then acquire Project -> Task ->
    # group membership -> User writer fences before attaching an answer log.
    # This serializes Task-share/Project-share revocation and role changes with
    # the exact generation check. The audit commit deliberately precedes a
    # second identical fence: a turn/ACL transition in that commit-to-wake
    # window must revoke the old hook instead of receiving a stale answer.
    delayed_cancellation: asyncio.CancelledError | None = None
    try:
        current_task = await lock_task_effect_access(
            request,
            task,
            db,
            allow_chat_share=True,
        )
        if (
            current_task.incarnation_id != pending.task_incarnation_id
            or current_task.session_id != pending.session_id
            or current_task.retry_count != pending.task_retry_count
            or current_task.turn_generation != pending.task_turn_generation
            or current_task.status != pending.task_status
            or current_task.status not in {"in_progress", "executing"}
        ):
            await db.rollback()
            ask_user_registry.discard(pending.request_id)
            raise HTTPException(410, "提问对应的 Task 代次已失效")

        # 持久化一条人类可读的回答记录（system_event 进 chat 历史）
        db.add(LogEntry(
            instance_id=1,
            task_id=task_id,
            event_type="system_event",
            role="system",
            content=_answer_summary(pending.questions, answers),
            raw_json=json.dumps(
                {
                    "request_id": request_id,
                    "task_incarnation_id": pending.task_incarnation_id,
                    "task_retry_count": pending.task_retry_count,
                    "task_turn_generation": pending.task_turn_generation,
                },
                ensure_ascii=False,
            ),
        ))
        commit, delayed_cancellation = await _settle_despite_cancellation(
            db.commit()
        )
        commit.result()

        async def fulfill_under_exact_fence() -> None:
            try:
                final_task = await lock_task_effect_access(
                    request,
                    current_task,
                    db,
                    allow_chat_share=True,
                )
                if (
                    final_task.incarnation_id != pending.task_incarnation_id
                    or final_task.session_id != pending.session_id
                    or final_task.retry_count != pending.task_retry_count
                    or final_task.turn_generation
                    != pending.task_turn_generation
                    or final_task.status != pending.task_status
                    or final_task.status not in {"in_progress", "executing"}
                ):
                    ask_user_registry.discard(pending.request_id)
                    raise HTTPException(410, "提问对应的 Task 代次已失效")

                # ``set_result`` is synchronous and runs while the exact Task
                # and ACL/role writer fences are still held. A retry, ACL, or
                # role transition can only win before this fence (and be
                # rejected above) or after the old hook received its answer.
                fulfilled = ask_user_registry.fulfill_answer(
                    request_id,
                    claim_id,
                    answers,
                )
                if not fulfilled:
                    raise HTTPException(
                        410,
                        "提问已过期或不存在（hook 侧可能已超时放行）",
                    )
            finally:
                # The second fence has no durable mutation; rollback is its
                # explicit release point and is shielded with this operation.
                await db.rollback()

        finalization, final_cancellation = await _settle_despite_cancellation(
            fulfill_under_exact_fence()
        )
        finalization.result()
        if delayed_cancellation is None:
            delayed_cancellation = final_cancellation
    except BaseException:
        ask_user_registry.release_answer_claim(request_id, claim_id)
        raise

    await broadcaster.broadcast(f"task:{task_id}", {
        "event_type": "ask_user_resolved",
        "request_id": request_id,
        "task_incarnation_id": pending.task_incarnation_id,
        "answers": answers,
    })
    # 关掉别的页面上挂着的全局通知
    await broadcaster.broadcast("tasks", {
        "event": "ask_user_resolved",
        "task_id": task_id,
        "request_id": request_id,
        "task_incarnation_id": pending.task_incarnation_id,
    })
    if delayed_cancellation is not None:
        raise delayed_cancellation
    return {"ok": True}


def _questions_summary(questions: list[dict]) -> str:
    qs = [q.get("question") or q.get("header") or "?" for q in questions]
    return "AskUserQuestion: " + " | ".join(qs)


def _answer_summary(questions: list[dict], answers: list[dict]) -> str:
    parts = []
    for idx, q in enumerate(questions):
        ans = answers[idx] if idx < len(answers) else {}
        labels = list(ans.get("labels") or [])
        text = (ans.get("text") or "").strip()
        if text:
            labels.append(f'"{text}"')
        header = q.get("header") or q.get("question") or f"Q{idx + 1}"
        parts.append(f"{header} → {', '.join(labels) if labels else '(无选择)'}")
    return "已回答: " + " | ".join(parts)
