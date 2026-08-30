import asyncio
import json
import logging
import re
from types import SimpleNamespace

from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect
from sqlalchemy import select

from backend.config import settings

logger = logging.getLogger(__name__)

router = APIRouter()

_MAX_WS_CHANNELS = 100
_WS_ACL_RECHECK_SECONDS = 5.0
_GLOBAL_CHANNELS = {
    "tasks",
    "plans",
    "workers",
    "system",
    "system_update",
    "pr-monitor",
    "capabilities",
    "deliveries",
}
_INSTANCE_CHANNEL_RE = re.compile(r"instance:([1-9]\d*)\Z")
_TASK_CHANNEL_RE = re.compile(r"task:([1-9]\d*)\Z")
_PLAN_CHANNEL_RE = re.compile(r"plan:([1-9]\d*)\Z")
_DELIVERY_CHANNEL_RE = re.compile(r"delivery:([1-9]\d*)\Z")
_WORKER_CHANNEL_RE = re.compile(r"worker:([1-9]\d*)\Z")
_DISCUSSION_CHANNEL_RE = re.compile(
    r"discussion:([1-9]\d*)(?::agent:[1-9]\d*)?\Z"
)
_ROLE_RANK = {"member": 0, "admin": 1, "super_admin": 2}


def _ws_identity(ws: WebSocket) -> dict | None:
    """Return the authenticated WS identity, including its role."""
    auth = ws.headers.get("authorization", "")
    token = ws.query_params.get("token", "")
    if settings.ccm_node_role == "worker":
        # The sole Worker socket is the Manager relay/control channel.  Human
        # JWT sessions and legacy auth-disabled super-admin semantics belong
        # only to the authoritative Manager.
        if not settings.auth_token:
            return None
        if (
            auth.startswith("Bearer ")
            and auth[7:] == settings.auth_token
        ) or token == settings.auth_token:
            return {
                "user_id": None,
                "role": "super_admin",
                "auth_type": "worker_control_plane",
            }
        return None
    # Legacy admin token (header or query param)
    if settings.auth_token and auth.startswith("Bearer ") and auth[7:] == settings.auth_token:
        return {"user_id": None, "role": "super_admin", "auth_type": "token"}
    if settings.auth_token and token == settings.auth_token:
        return {"user_id": None, "role": "super_admin", "auth_type": "token"}
    # JWT token (query param — browser WS can't set headers)
    if token:
        from backend.api.auth import decode_jwt

        payload = decode_jwt(token)
        if payload is not None:
            return {**payload, "auth_type": "jwt"}
    if auth.startswith("Bearer "):
        from backend.api.auth import decode_jwt

        payload = decode_jwt(auth[7:])
        if payload is not None:
            return {**payload, "auth_type": "jwt"}
    if not settings.auth_token:
        return {"user_id": None, "role": "super_admin", "auth_type": "none"}
    return None


def _ws_token_ok(ws: WebSocket) -> bool:
    """WS 认证：AUTH_TOKEN 或 JWT 都接受。"""
    return _ws_identity(ws) is not None


async def _revalidate_ws_identity(
    ws: WebSocket,
    original_identity: dict,
    db,
) -> dict | None:
    """Re-decode persistent WS credentials and refresh JWT role from the DB."""

    fresh_identity = _ws_identity(ws)
    if fresh_identity is None:
        return None
    if fresh_identity.get("auth_type") != original_identity.get("auth_type"):
        return None
    if fresh_identity.get("auth_type") != "jwt":
        return fresh_identity

    user_id = fresh_identity.get("user_id")
    if (
        not isinstance(user_id, int)
        or isinstance(user_id, bool)
        or user_id <= 0
        or user_id != original_identity.get("user_id")
    ):
        return None

    from backend.models.user import User

    user = await db.get(User, user_id)
    if user is None or not user.is_active:
        return None
    # JWT role claims are only a login-time snapshot. DB state is authoritative
    # for a long-lived socket, including admin demotion.
    fresh_identity["role"] = user.role
    return fresh_identity


async def _current_ws_identity(ws: WebSocket, db) -> dict | None:
    """Authenticate a new socket and replace JWT snapshots with DB state."""

    identity = _ws_identity(ws)
    if identity is None or identity.get("auth_type") != "jwt":
        return identity
    return await _revalidate_ws_identity(ws, identity, db)


def _acl_request(identity: dict) -> SimpleNamespace:
    return SimpleNamespace(
        state=SimpleNamespace(
            user_id=identity.get("user_id"),
            user_role=identity.get("role", "member"),
        )
    )


async def _ws_task_channel_allowed(identity: dict, task_id: int, db=None) -> bool:
    """Apply the normal HTTP task ACL to a WebSocket task subscription."""

    from backend.api.deps import require_task_access
    from backend.database import async_session
    from backend.models.task import Task

    async def check(session) -> bool:
        task = await session.get(Task, task_id)
        if task is None:
            return False
        try:
            await require_task_access(_acl_request(identity), task, session)
        except HTTPException:
            return False
        return True

    if db is not None:
        return await check(db)
    async with async_session() as session:
        return await check(session)


async def _ws_worker_channel_allowed(
    identity: dict,
    worker_id: int,
    db=None,
) -> bool:
    """Allow only administrators or the current Worker owner."""

    from backend.api.deps import require_worker_access
    from backend.database import async_session
    from backend.models.worker import Worker

    async def check(session) -> bool:
        worker = await session.get(Worker, worker_id)
        if worker is None:
            return False
        try:
            await require_worker_access(_acl_request(identity), worker)
        except HTTPException:
            return False
        return True

    if db is not None:
        return await check(db)
    async with async_session() as session:
        return await check(session)


async def _ws_plan_channel_allowed(identity: dict, plan_id: int, db=None) -> bool:
    """Mirror canonical HTTP Plan access without importing the API router."""

    from backend.database import async_session
    from backend.models.plan import Plan
    from backend.api.deps import has_project_access

    async def check(session) -> bool:
        plan = await session.get(Plan, plan_id)
        if plan is None:
            return False
        if plan.target_task_id is not None:
            return await _ws_task_channel_allowed(
                identity, plan.target_task_id, session
            )
        user_id = identity.get("user_id")
        if user_id is not None and plan.created_by == user_id:
            return True
        request = _acl_request(identity)
        if plan.project_id is not None and await has_project_access(
            request, plan.project_id, session
        ):
            return True
        return False

    if db is not None:
        return await check(db)
    async with async_session() as session:
        return await check(session)


async def _ws_delivery_channel_allowed(
    identity: dict,
    run_id: int,
    db=None,
) -> bool:
    """Authorize scoped Delivery updates through the owning Project ACL."""

    from backend.api.deps import has_project_access
    from backend.database import async_session
    from backend.models.delivery import DeliveryRun

    async def check(session) -> bool:
        run = await session.get(DeliveryRun, run_id)
        if run is None:
            return False
        return await has_project_access(
            _acl_request(identity),
            run.project_id,
            session,
        )

    if db is not None:
        return await check(db)
    async with async_session() as session:
        return await check(session)


async def _ws_discussion_channel_allowed(
    identity: dict,
    discussion_id: int,
    db=None,
) -> bool:
    """Allow a member to observe only discussions they created."""

    from backend.database import async_session
    from backend.models.discussion import Discussion

    user_id = identity.get("user_id")
    if not user_id:
        return False

    async def check(session) -> bool:
        discussion = await session.get(Discussion, discussion_id)
        return bool(
            discussion is not None
            and discussion.creator_user_id == user_id
        )

    if db is not None:
        return await check(db)
    async with async_session() as session:
        return await check(session)


async def _ws_channel_allowed(channel: object, identity: dict, db=None) -> bool:
    """Default-deny member subscriptions; authorize scoped resources only."""

    if not isinstance(channel, str):
        return False
    instance_match = _INSTANCE_CHANNEL_RE.fullmatch(channel)
    task_match = _TASK_CHANNEL_RE.fullmatch(channel)
    plan_match = _PLAN_CHANNEL_RE.fullmatch(channel)
    delivery_match = _DELIVERY_CHANNEL_RE.fullmatch(channel)
    worker_match = _WORKER_CHANNEL_RE.fullmatch(channel)
    discussion_match = _DISCUSSION_CHANNEL_RE.fullmatch(channel)
    known_channel = bool(
        channel in _GLOBAL_CHANNELS
        or instance_match
        or task_match
        or plan_match
        or delivery_match
        or worker_match
        or discussion_match
    )
    if not known_channel:
        return False
    if identity.get("role") in ("admin", "super_admin"):
        return True
    if instance_match:
        return False
    if task_match:
        return await _ws_task_channel_allowed(
            identity,
            int(task_match.group(1)),
            db,
        )
    if plan_match:
        return await _ws_plan_channel_allowed(
            identity,
            int(plan_match.group(1)),
            db,
        )
    if delivery_match:
        return await _ws_delivery_channel_allowed(
            identity,
            int(delivery_match.group(1)),
            db,
        )
    if worker_match:
        return await _ws_worker_channel_allowed(
            identity,
            int(worker_match.group(1)),
            db,
        )
    if discussion_match:
        return await _ws_discussion_channel_allowed(
            identity,
            int(discussion_match.group(1)),
            db,
        )
    # Global channels carry events for many owners and cannot be safely
    # filtered by the broadcaster. Members use resource-scoped channels and
    # existing HTTP polling fallbacks.
    return False


@router.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    from backend.database import async_session
    from backend.main import broadcaster

    async with async_session() as acl_db:
        identity = await _current_ws_identity(ws, acl_db)
    if identity is None:
        await ws.close(code=4401)
        return
    await ws.accept()
    decisions: dict[str, bool] = {}
    subscribed_channels: set[str] = set()

    async def revalidate_access() -> None:
        try:
            while True:
                await asyncio.sleep(_WS_ACL_RECHECK_SECONDS)
                channels = list(subscribed_channels)
                async with async_session() as acl_db:
                    fresh_identity = await _revalidate_ws_identity(
                        ws,
                        identity,
                        acl_db,
                    )
                    if fresh_identity is None:
                        await ws.close(
                            code=4401,
                            reason="WebSocket authentication revoked",
                        )
                        return
                    if _ROLE_RANK.get(
                        fresh_identity.get("role"),
                        0,
                    ) < _ROLE_RANK.get(identity.get("role"), 0):
                        await ws.close(
                            code=4403,
                            reason="WebSocket privileges revoked",
                        )
                        return
                    if fresh_identity != identity:
                        decisions.clear()
                        identity.clear()
                        identity.update(fresh_identity)
                    for channel in channels:
                        if not await _ws_channel_allowed(
                            channel,
                            identity,
                            acl_db,
                        ):
                            await ws.close(
                                code=4403,
                                reason="WebSocket channel access revoked",
                            )
                            return
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("WebSocket ACL revalidation failed")
            try:
                await ws.close(code=1011, reason="ACL revalidation failed")
            except Exception:
                pass

    acl_task = asyncio.create_task(revalidate_access())
    try:
        while True:
            text = await ws.receive_text()
            try:
                msg = json.loads(text)
            except json.JSONDecodeError:
                continue

            if not isinstance(msg, dict):
                await ws.send_text(json.dumps({
                    "action": "error",
                    "detail": "WebSocket message must be an object",
                }))
                continue

            action = msg.get("action")
            channels = msg.get("channels", [])

            if action == "worker_launch_admission_response":
                # This reverse control message exists only on a Worker.  The
                # HMAC is checked again by the one-shot waiter, while requiring
                # the deployment token here prevents a local JWT administrator
                # from acting as the Manager control plane.
                if (
                    settings.ccm_node_role != "worker"
                    or identity.get("auth_type")
                    != "worker_control_plane"
                ):
                    continue
                from backend.services.worker_launch_admission import (
                    accept_worker_launch_admission_response,
                )

                accept_worker_launch_admission_response(
                    msg,
                    control_token=settings.auth_token,
                )
            elif action == "subscribe" and channels:
                if not isinstance(channels, list):
                    continue
                # Stable de-duplication prevents repeated broadcaster entries
                # and repeated ACL database lookups. The cumulative limit also
                # bounds clients that subscribe in many small messages.
                unique_channels = list(dict.fromkeys(
                    channel
                    for channel in channels
                    if isinstance(channel, str)
                ))
                if (
                    len(channels) > _MAX_WS_CHANNELS
                    or len(set(decisions).union(unique_channels))
                    > _MAX_WS_CHANNELS
                ):
                    await ws.close(code=4408, reason="Too many channels")
                    return
                allowed: list[str] = []
                denied: list[object] = [
                    channel
                    for channel in channels
                    if not isinstance(channel, str)
                ]
                # Reuse one DB session across all resource ACL checks in this
                # subscription message.
                async with async_session() as acl_db:
                    for channel in unique_channels:
                        decision = decisions.get(channel)
                        if decision is None:
                            decision = await _ws_channel_allowed(
                                channel,
                                identity,
                                acl_db,
                            )
                            decisions[channel] = decision
                        if decision:
                            allowed.append(channel)
                        else:
                            denied.append(channel)
                if allowed:
                    await broadcaster.subscribe(ws, allowed)
                    subscribed_channels.update(allowed)
                # Always acknowledge the authorized subset, including an empty
                # one, so clients can distinguish a completed handshake from a
                # transport that never reached subscription processing.
                await ws.send_text(json.dumps({
                    "action": "subscribed",
                    "channels": allowed,
                }))
                if denied:
                    await ws.send_text(json.dumps({
                        "action": "error",
                        "detail": "Channel access denied",
                        "channels": denied,
                    }))
            elif action == "unsubscribe" and channels:
                await broadcaster.unsubscribe(ws)
                decisions.clear()
                subscribed_channels.clear()
                await ws.send_text(json.dumps({"action": "unsubscribed"}))
    except WebSocketDisconnect:
        pass
    except Exception:
        logger.exception("WebSocket connection failed")
    finally:
        acl_task.cancel()
        await asyncio.gather(acl_task, return_exceptions=True)
        await broadcaster.unsubscribe(ws)
        await broadcaster.unsubscribe(ws)
