"""Worker 管理 API（elastic-worker 设计 §18）。

长流程（创建/开关机/销毁）全部 fire-and-forget 后台执行，
进度经 "workers" WS channel 实时广播，API 立即返回当前记录。
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import secrets
import shlex
import socket
import time
from contextlib import asynccontextmanager
from urllib.parse import quote

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import desc, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.config import settings
from backend.database import get_db
from backend.models.worker import Worker
from backend.schemas.global_settings import RuntimeSettingsUpdate
from backend.schemas.worker import WorkerCreate, WorkerLogsResponse, WorkerResponse
from backend.services.worker_provisioner import (
    CLAUDE_LOGIN_IDENTITY_KEY,
    claude_login_identity_matches,
    worker_control_plane_enabled,
)

logger = logging.getLogger(__name__)

def _require_worker_control_plane_auth() -> None:
    """Keep every Worker route closed in unauthenticated open mode."""

    if not worker_control_plane_enabled():
        raise HTTPException(
            status_code=503,
            detail=(
                "Worker control plane requires CCM_NODE_ROLE=manager and a "
                "non-empty AUTH_TOKEN"
            ),
        )


router = APIRouter(
    prefix="/api/workers",
    tags=["workers"],
    dependencies=[Depends(_require_worker_control_plane_auth)],
)

# 后台任务强引用：event loop 只持弱引用，长耗时 bootstrap 任务可能被 GC
# 掐死在半路（asyncio 文档明确的坑）
_background_tasks: set[asyncio.Task] = set()
# Ready-Worker account logins outlive their initiating HTTP request.  Keep only
# challenge metadata here; passwords, mailbox tokens and OTP codes never enter
# this process-wide status store.
_worker_login_state: dict[str, dict] = {}
_worker_login_admission_lock = asyncio.Lock()
# Background logins for different accounts can finish at the same time.  The
# accounts column is one JSON value, so serialize its read/modify/write cycle
# to prevent one successful login from overwriting another.
_worker_account_store_lock = asyncio.Lock()
# Lifecycle endpoints perform a durable compare-and-set before spawning their
# background operation.  Keep same-process transitions for one Worker inside a
# single transaction boundary.  Besides preventing duplicate coordinators,
# this is required for SQLite's single-connection configurations where a
# concurrent losing rollback could otherwise undo the winning request's
# uncommitted CAS while it is still checking destroy blockers.


class _WorkerLifecycleTransitionLock:
    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.users = 0


_worker_lifecycle_transition_locks: dict[
    tuple[asyncio.AbstractEventLoop, int], _WorkerLifecycleTransitionLock
] = {}


@asynccontextmanager
async def _worker_lifecycle_transaction_lock(worker_id: int):
    """Serialize same-process Worker-row transactions for one Worker.

    This also protects test and embedded deployments backed by one SQLite
    StaticPool connection: a rollback from another AsyncSession must never
    undo an uncommitted lifecycle, assignment, or account-delete fence.
    Network effects must remain outside this lock.
    """

    loop = asyncio.get_running_loop()
    lock_key = (loop, worker_id)
    entry = _worker_lifecycle_transition_locks.setdefault(
        lock_key, _WorkerLifecycleTransitionLock(),
    )
    entry.users += 1
    try:
        async with entry.lock:
            yield
    finally:
        entry.users -= 1
        if (
            entry.users == 0
            and _worker_lifecycle_transition_locks.get(lock_key) is entry
        ):
            _worker_lifecycle_transition_locks.pop(lock_key, None)

_LOGIN_METHODS = frozenset({"", "171mail", "mailcom", "onet", "gazeta"})
_CODEX_LOGIN_METHODS = _LOGIN_METHODS | {"mailcatcher"}
_WORKER_ACCOUNT_PROVIDERS = frozenset({"claude", "codex"})
_WORKER_FRESH_DESTROYABLE_STATUSES = frozenset({"ready"})
_WORKER_DESTROY_RECOVERY_STATUSES = frozenset({"ready", "error"})
_WORKER_RENAMEABLE_STATUSES = frozenset({"ready", "stopped", "error"})
_WORKER_AUTH_FAILURE_STATUSES = frozenset({401, 403})
_WORKER_ACTIVE_LOGIN_STATUSES = frozenset({
    "running", "awaiting_otp", "verifying_otp", "finalizing", "cancelling",
})
_WORKER_LOGIN_TERMINAL_FAILURE_STATUSES = frozenset({
    "failed", "expired", "cancelled", "recovery_failed",
})
_WORKER_ACCOUNT_DELETE_PROTOCOL = 1
_WORKER_ACCOUNT_DELETE_RECEIPT_KEY = "delete_receipt_v1"
_WORKER_ACCOUNT_DELETE_STATUS = "deleting"
_WORKER_ACCOUNT_DELETE_REPLAY_STATUSES = frozenset({"ready", "error"})
_NO_WORKER_JSON = object()


async def _lock_worker_effect_access(
    request: Request,
    db: AsyncSession,
    worker_id: int,
    *,
    predicates: tuple = (),
    conflict_detail: str,
) -> Worker:
    """Fence one Worker mutation against ownership and User-role changes.

    Route-level access checks use the authentication snapshot captured before
    the endpoint starts.  A Worker may be reassigned, or that JWT user may be
    disabled/demoted, while the request validates input or waits for an
    in-process lock.  Make the Worker row the common writer fence, bind a
    member's current ownership into that same CAS, then hold the exact active
    User/role fence until the caller commits its durable mutation/admission.

    The caller owns the transaction and must commit before starting any
    remote, SSH, or cloud effect.
    """

    from backend.api.deps import (
        get_current_user_id,
        get_current_user_role,
        lock_request_user_authority,
    )

    await db.rollback()
    actor_auth_type = getattr(request.state, "auth_type", None)
    actor_role = get_current_user_role(request)
    actor_user_id = get_current_user_id(request)
    worker_predicates = [Worker.id == worker_id, *predicates]
    if actor_auth_type == "jwt" and actor_role == "member":
        if (
            isinstance(actor_user_id, bool)
            or not isinstance(actor_user_id, int)
            or actor_user_id <= 0
        ):
            raise HTTPException(403, "User authority is invalid")
        worker_predicates.append(Worker.owner_user_id == actor_user_id)

    fenced = await db.execute(
        update(Worker)
        .where(*worker_predicates)
        # ``updated_at`` has a Python onupdate hook.  Explicit self-assignment
        # keeps this authorization fence a true no-op until the caller writes
        # its intended field(s).
        .values(status=Worker.status, updated_at=Worker.updated_at)
        .execution_options(synchronize_session=False)
    )
    if fenced.rowcount != 1:
        await db.rollback()
        exists = await db.scalar(select(Worker.id).where(Worker.id == worker_id))
        if exists is None:
            raise HTTPException(404, "Worker not found")
        raise HTTPException(409, conflict_detail)

    try:
        await lock_request_user_authority(request, db)
    except Exception:
        # The Worker no-op UPDATE above is already part of this transaction.
        # Roll it back before releasing a same-connection transaction guard;
        # request dependency cleanup may run too late for SQLite StaticPool.
        await db.rollback()
        raise
    worker = (
        await db.execute(
            select(Worker)
            .where(Worker.id == worker_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if worker is None:  # Defensive: the writer fence already proved it exists.
        await db.rollback()
        raise HTTPException(404, "Worker not found")
    return worker


async def _lock_worker_assignment_users(
    request: Request,
    db: AsyncSession,
    target_user_id: int | None,
) -> None:
    """Fence assignment actor/recipient User rows in deterministic order.

    The Worker row is already locked by ``assign_worker``.  Two administrators
    can otherwise assign different Workers to each other and take the target
    and actor User rows in opposite order.  Numeric ordering makes the common
    lock order Worker -> User ids ascending, while preserving the actor's exact
    active role and the recipient's active-state requirement.
    """

    from backend.api.deps import get_current_user_id, get_current_user_role
    from backend.models.user import User

    actor_is_jwt = getattr(request.state, "auth_type", None) == "jwt"
    actor_user_id = get_current_user_id(request) if actor_is_jwt else None
    actor_role = get_current_user_role(request) if actor_is_jwt else None
    if actor_is_jwt and (
        isinstance(actor_user_id, bool)
        or not isinstance(actor_user_id, int)
        or actor_user_id <= 0
        or actor_role not in {"admin", "super_admin"}
    ):
        raise HTTPException(403, "User authority is invalid")

    user_ids = sorted({
        user_id
        for user_id in (actor_user_id, target_user_id)
        if user_id is not None
    })
    for user_id in user_ids:
        predicates = [User.id == user_id, User.is_active.is_(True)]
        if user_id == actor_user_id:
            predicates.append(User.role == actor_role)
        fenced = await db.execute(
            update(User)
            .where(*predicates)
            .values(id=User.id)
            .execution_options(synchronize_session=False)
        )
        if fenced.rowcount == 1:
            continue
        if user_id == actor_user_id:
            raise HTTPException(
                409,
                "User was disabled or changed role while authorizing the "
                "effect",
            )
        raise HTTPException(400, "Worker owner must be an active user")


def _normalize_login_method(value: str | None) -> str:
    if value is not None and not isinstance(value, str):
        raise HTTPException(400, "login_method 必须是字符串")
    method = (value or "").strip().lower()
    if method not in _LOGIN_METHODS:
        raise HTTPException(400, f"不支持的登录方式: {method}")
    return method


def _normalize_worker_account_provider(value: str | None) -> str:
    if not isinstance(value, str):
        raise HTTPException(400, "provider 必须是字符串")
    provider = value.strip().lower()
    if provider not in _WORKER_ACCOUNT_PROVIDERS:
        raise HTTPException(400, f"不支持的 Worker 账号 provider: {provider}")
    return provider


def _normalize_worker_login_method(value: str | None, provider: str) -> str:
    if value is not None and not isinstance(value, str):
        raise HTTPException(400, "login_method 必须是字符串")
    method = (value or "").strip().lower()
    allowed = _CODEX_LOGIN_METHODS if provider == "codex" else _LOGIN_METHODS
    if method not in allowed:
        raise HTTPException(400, f"不支持的 {provider} 登录方式: {method}")
    return method


def _normalize_worker_account(
    *,
    email: str,
    provider: str,
    token: str | None,
    password: str | None,
    login_method: str | None,
    require_unattended: bool = False,
) -> dict:
    normalized_email = email.strip()
    if not normalized_email:
        raise HTTPException(400, "账号 email 必填")

    normalized_provider = _normalize_worker_account_provider(provider)
    normalized_token = (token or "").strip()
    # OpenAI passwords are opaque. In particular, never trim leading/trailing
    # characters while moving them through Manager storage into the Worker.
    normalized_password = password or ""
    if normalized_provider == "claude":
        if not normalized_token:
            raise HTTPException(400, f"Claude 账号 {normalized_email} 缺少 token")
    elif not normalized_token and not normalized_password:
        raise HTTPException(
            400,
            f"Codex 账号 {normalized_email} 的 token 和 password 至少填写一项",
        )
    elif normalized_provider == "codex" and require_unattended and not normalized_token:
        raise HTTPException(
            400,
            f"Codex 账号 {normalized_email} 的 Worker 自动 bootstrap 必须提供邮箱 token",
        )

    return {
        "email": normalized_email,
        "provider": normalized_provider,
        "token": normalized_token,
        "password": normalized_password,
        "login_method": _normalize_worker_login_method(
            login_method, normalized_provider
        ),
    }


def _reject_duplicate_worker_accounts(accounts: list[dict]) -> None:
    """Reject identities that would resolve to the same remote pool slot."""
    seen: set[tuple[str, str]] = set()
    seen_slots: set[tuple[str, str]] = set()
    for account in accounts:
        provider = str(account.get("provider") or "claude").lower()
        identity = (
            provider,
            str(account.get("email") or "").strip().casefold(),
        )
        if identity in seen:
            raise HTTPException(
                400,
                f"重复的 Worker 账号: {account.get('email')} ({identity[0]})",
            )
        seen.add(identity)
        account_id = str(account.get("account_id") or "").strip()
        if account_id:
            slot = (provider, account_id)
            if slot in seen_slots:
                raise HTTPException(
                    400,
                    f"重复的 Worker 账号槽位: {account_id} ({provider})",
                )
            seen_slots.add(slot)


def _build_add_account_command(
    remote_dir: str,
    *,
    email: str,
    token: str,
    slot: str,
    login_method: str,
) -> str:
    """Build the remote login command with every dynamic argv shell-quoted."""
    argv = [
        "xvfb-run",
        "--auto-servernum",
        "--server-args=-screen 0 1920x1080x24",
        "uv",
        "run",
        "python",
        "scripts/auto_login.py",
        "--email",
        email,
        "--token",
        token,
        "--add-to-pool",
        slot,
        "--save-token",
    ]
    if login_method:
        argv.extend(["--login-method", login_method])
    return (
        f"cd {shlex.quote(remote_dir)} && "
        'export PATH="$HOME/.local/bin:$PATH" && '
        f"{shlex.join(argv)}"
    )


def _canonicalize_persisted_worker_account_slots(
    accounts: list | None,
) -> list:
    """Freeze historical positional Claude slots before any list mutation."""

    if accounts is not None and not isinstance(accounts, list):
        raise HTTPException(
            409,
            "Worker 保存的账号列表格式无效，无法安全修改",
        )
    canonical: list = []
    claude_index = 0
    for raw_account in accounts or []:
        if not isinstance(raw_account, dict):
            canonical.append(raw_account)
            continue
        account = dict(raw_account)
        provider = str(account.get("provider") or "claude").lower()
        if provider == "claude":
            claude_index += 1
            if not account.get("account_id"):
                account["account_id"] = (
                    "default"
                    if claude_index == 1
                    else f"account-{claude_index}"
                )
        canonical.append(account)
    return canonical


def _worker_account_delete_remote_path(provider: str, account_id: str) -> str:
    return (
        f"/api/codex-pool/accounts/{quote(account_id, safe='')}"
        if provider == "codex"
        else f"/api/pool/accounts/{quote(account_id, safe='')}"
    )


def _worker_control_credential_digest(auth_token: object) -> str:
    value = auth_token if isinstance(auth_token, str) else ""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _worker_account_delete_actor(request: Request) -> dict[str, object]:
    from backend.api.deps import get_current_user_id, get_current_user_role

    return {
        "actor_user_id": get_current_user_id(request),
        "actor_user_role": get_current_user_role(request),
        "authorization_type": getattr(request.state, "auth_type", None),
    }


def _new_worker_account_delete_receipt(
    worker: Worker,
    request: Request,
    *,
    provider: str,
    account_id: str,
    email: str,
) -> dict:
    return {
        "protocol_version": _WORKER_ACCOUNT_DELETE_PROTOCOL,
        "state": "prepared",
        "operation_id": secrets.token_hex(16),
        "worker_id": worker.id,
        "worker_owner_user_id": worker.owner_user_id,
        "worker_status": worker.status,
        "worker_bootstrap_step": worker.bootstrap_step,
        "worker_destroy_lifecycle_nonce": worker.destroy_lifecycle_nonce,
        "worker_private_ip": worker.private_ip,
        "worker_ccm_port": worker.ccm_port,
        "worker_auth_token_sha256": _worker_control_credential_digest(
            worker.auth_token
        ),
        **_worker_account_delete_actor(request),
        "provider": provider,
        "account_id": account_id,
        "email": email,
        "remote_path": _worker_account_delete_remote_path(
            provider,
            account_id,
        ),
        "prepared_at": time.time(),
    }


def _worker_account_delete_receipt(
    account: object,
) -> dict | None:
    if not isinstance(account, dict):
        return None
    receipt = account.get(_WORKER_ACCOUNT_DELETE_RECEIPT_KEY)
    return dict(receipt) if isinstance(receipt, dict) else None


def _has_worker_account_delete_outbox(accounts: list | None) -> bool:
    """Treat malformed tombstones as active so every writer fails closed."""

    if accounts is None:
        return False
    if not isinstance(accounts, list):
        return True
    return any(
        isinstance(account, dict)
        and (
            account.get("status") == _WORKER_ACCOUNT_DELETE_STATUS
            or _WORKER_ACCOUNT_DELETE_RECEIPT_KEY in account
        )
        for account in accounts
    )


def _require_worker_account_delete_receipt(
    account: object,
    worker: Worker,
    *,
    provider: str,
    account_id: str,
    request: Request | None,
) -> dict:
    expected_account_keys = {
        "email",
        "provider",
        "account_id",
        "status",
        _WORKER_ACCOUNT_DELETE_RECEIPT_KEY,
    }
    if (
        not isinstance(account, dict)
        or set(account) != expected_account_keys
        or account.get("status") != _WORKER_ACCOUNT_DELETE_STATUS
        or account.get("provider") != provider
        or account.get("account_id") != account_id
        or not isinstance(account.get("email"), str)
    ):
        raise HTTPException(409, "Worker 账号删除 tombstone 已损坏")
    receipt = _worker_account_delete_receipt(account)
    expected_receipt_keys = {
        "protocol_version",
        "state",
        "operation_id",
        "worker_id",
        "worker_owner_user_id",
        "worker_status",
        "worker_bootstrap_step",
        "worker_destroy_lifecycle_nonce",
        "worker_private_ip",
        "worker_ccm_port",
        "worker_auth_token_sha256",
        "actor_user_id",
        "actor_user_role",
        "authorization_type",
        "provider",
        "account_id",
        "email",
        "remote_path",
        "prepared_at",
    }
    operation_id = receipt.get("operation_id") if receipt is not None else None
    if (
        receipt is None
        or set(receipt) != expected_receipt_keys
        or receipt.get("protocol_version")
        != _WORKER_ACCOUNT_DELETE_PROTOCOL
        or receipt.get("state") != "prepared"
        or not isinstance(operation_id, str)
        or len(operation_id) != 32
        or any(char not in "0123456789abcdef" for char in operation_id)
        or isinstance(receipt.get("prepared_at"), bool)
        or not isinstance(receipt.get("prepared_at"), (int, float))
        or receipt.get("worker_id") != worker.id
        or receipt.get("worker_owner_user_id") != worker.owner_user_id
        # The operation is admitted only from ready. A later error is treated
        # solely as health-state drift so the exact prepared DELETE can remain
        # live; every other frozen lifecycle identity still has to match.
        or receipt.get("worker_status") != "ready"
        or worker.status not in _WORKER_ACCOUNT_DELETE_REPLAY_STATUSES
        or receipt.get("worker_bootstrap_step") is not None
        or receipt.get("worker_bootstrap_step") != worker.bootstrap_step
        or receipt.get("worker_destroy_lifecycle_nonce")
        != worker.destroy_lifecycle_nonce
        or receipt.get("worker_private_ip") != worker.private_ip
        or receipt.get("worker_ccm_port") != worker.ccm_port
        or receipt.get("worker_auth_token_sha256")
        != _worker_control_credential_digest(worker.auth_token)
        or receipt.get("provider") != provider
        or receipt.get("account_id") != account_id
        or receipt.get("email") != account.get("email")
        or receipt.get("remote_path")
        != _worker_account_delete_remote_path(provider, account_id)
    ):
        raise HTTPException(409, "Worker 账号删除 receipt 与当前 Worker 不匹配")
    if request is not None:
        actor = _worker_account_delete_actor(request)
        if any(receipt.get(key) != value for key, value in actor.items()):
            raise HTTPException(
                409,
                "Worker 账号删除操作属于另一授权主体",
            )
    return receipt


def _prepare_persisted_worker_account_delete(
    accounts: list | None,
    worker: Worker,
    request: Request,
    *,
    provider: str,
    account_id: str,
) -> tuple[list, dict]:
    canonical = _canonicalize_persisted_worker_account_slots(accounts)
    matches = [
        index
        for index, account in enumerate(canonical)
        if isinstance(account, dict)
        and str(account.get("provider") or "claude").lower() == provider
        and account.get("account_id") == account_id
    ]
    if len(matches) > 1:
        raise HTTPException(
            409,
            "Manager 中存在重复的 Worker 账号槽位，无法安全删除",
        )
    if worker.status != "ready" and not (
        len(matches) == 1
        and (
            canonical[matches[0]].get("status")
            == _WORKER_ACCOUNT_DELETE_STATUS
            or _WORKER_ACCOUNT_DELETE_RECEIPT_KEY
            in canonical[matches[0]]
        )
    ):
        # ``error`` is a replay-only health state. Never scrub credentials or
        # create a fresh remote effect unless a complete ready-time tombstone
        # already proves that the DELETE was prepared durably.
        raise HTTPException(
            409,
            "Worker 非 ready 状态只允许重放已持久化的账号删除 operation",
        )
    if matches:
        index = matches[0]
        account = canonical[index]
        if (
            account.get("status") == _WORKER_ACCOUNT_DELETE_STATUS
            or _WORKER_ACCOUNT_DELETE_RECEIPT_KEY in account
        ):
            receipt = _require_worker_account_delete_receipt(
                account,
                worker,
                provider=provider,
                account_id=account_id,
                request=request,
            )
            return canonical, receipt
        email = str(account.get("email") or "").strip()
    else:
        # The remote pool can contain a slot that predates Manager credential
        # persistence.  Keep an email-less tombstone and block every add for
        # this Worker until the exact idempotent DELETE is reconciled.
        index = len(canonical)
        email = ""

    receipt = _new_worker_account_delete_receipt(
        worker,
        request,
        provider=provider,
        account_id=account_id,
        email=email,
    )
    tombstone = {
        "email": email,
        "provider": provider,
        "account_id": account_id,
        "status": _WORKER_ACCOUNT_DELETE_STATUS,
        _WORKER_ACCOUNT_DELETE_RECEIPT_KEY: receipt,
    }
    if index == len(canonical):
        canonical.append(tombstone)
    else:
        canonical[index] = tombstone
    return canonical, receipt


def _settle_persisted_worker_account_delete(
    accounts: list | None,
    worker: Worker,
    receipt: dict,
) -> list:
    canonical = _canonicalize_persisted_worker_account_slots(accounts)
    provider = receipt.get("provider")
    account_id = receipt.get("account_id")
    matches = [
        index
        for index, account in enumerate(canonical)
        if isinstance(account, dict)
        and account.get("provider") == provider
        and account.get("account_id") == account_id
    ]
    if len(matches) != 1:
        raise HTTPException(
            409,
            "Worker 账号删除 tombstone 在结算前发生变化",
        )
    index = matches[0]
    persisted_receipt = _require_worker_account_delete_receipt(
        canonical[index],
        worker,
        provider=provider,
        account_id=account_id,
        request=None,
    )
    if persisted_receipt != receipt:
        raise HTTPException(409, "Worker 账号删除 operation 已被替换")
    return canonical[:index] + canonical[index + 1 :]


def _remove_persisted_worker_account(
    accounts: list | None,
    *,
    provider: str,
    account_id: str,
) -> tuple[list, bool]:
    """Remove a remotely deleted account from bootstrap retry credentials.

    New records persist ``account_id``.  Historical Claude-only records did
    not, so reconstruct their deterministic legacy slots as a compatibility
    fallback.  Codex never had historical provider-less Worker records.
    """
    kept: list = []
    removed = False
    for account in _canonicalize_persisted_worker_account_slots(accounts):
        if not isinstance(account, dict):
            kept.append(account)
            continue
        account_provider = str(account.get("provider") or "claude").lower()
        persisted_id = account.get("account_id")
        if (
            not removed
            and account_provider == provider
            and persisted_id == account_id
        ):
            removed = True
            continue
        kept.append(account)
    return kept, removed


def _spawn(coro) -> asyncio.Task:
    task = asyncio.create_task(coro)
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    return task


async def _worker_http_request(
    worker: Worker,
    method: str,
    path: str,
    *,
    timeout: float,
    payload: object = _NO_WORKER_JSON,
    allow_statuses: frozenset[int] = frozenset(),
    client: httpx.AsyncClient | None = None,
):
    """Call a Worker without leaking its auth/upstream errors to the client.

    A Worker bearer token is an internal Manager-to-Worker credential.  In
    particular, forwarding an upstream 401 would make the frontend treat the
    *Manager* session as expired and clear the user's Manager token.
    """
    if not worker.private_ip:
        raise HTTPException(502, "Worker 网关缺少目标地址")
    url = f"http://{worker.private_ip}:{worker.ccm_port}{path}"
    kwargs: dict = {
        "headers": {"Authorization": f"Bearer {worker.auth_token}"},
    }
    if payload is not _NO_WORKER_JSON:
        kwargs["json"] = payload

    async def _send(active_client):
        sender = getattr(active_client, method.lower())
        return await sender(url, **kwargs)

    try:
        if client is None:
            async with httpx.AsyncClient(timeout=timeout) as active_client:
                response = await _send(active_client)
        else:
            response = await _send(client)
    except (httpx.RequestError, OSError, TimeoutError) as exc:
        raise HTTPException(
            502,
            f"Worker 网关连接失败: {type(exc).__name__}: {str(exc)[:200]}",
        ) from exc

    status_code = response.status_code
    if status_code in _WORKER_AUTH_FAILURE_STATUSES:
        raise HTTPException(
            502,
            f"Worker 认证失败（远端 HTTP {status_code}），请重试 Worker 引导以同步认证凭据",
        )
    if not 200 <= status_code < 300 and status_code not in allow_statuses:
        raise HTTPException(
            502,
            f"Worker 上游请求失败（远端 HTTP {status_code}）",
        )
    return response


def _worker_response_json(response) -> object:
    """Decode a Worker response or surface malformed upstream data as 502."""
    try:
        return response.json()
    except (TypeError, ValueError) as exc:
        raise HTTPException(502, "Worker 上游返回了无效 JSON") from exc


def _upsert_persisted_worker_account(
    accounts: list | None,
    account: dict,
    *,
    status: str,
    account_id: str | None = None,
) -> list:
    """Return one canonical account JSON value without mutating its input."""

    provider = account["provider"]
    updated_accounts = [
        item for item in (accounts or [])
        if not (
            isinstance(item, dict)
            and str(item.get("provider") or "claude").lower() == provider
            and (
                (account_id and item.get("account_id") == account_id)
                or (
                    str(item.get("email") or "").strip().casefold()
                    == account["email"].casefold()
                )
            )
        )
    ]
    updated_accounts.append({
        **account,
        **({"account_id": account_id} if account_id else {}),
        "status": status,
    })
    return updated_accounts


async def _persist_worker_account_state(
    provisioner,
    worker_id: int,
    account: dict,
    *,
    status: str,
    account_id: str | None = None,
) -> None:
    """Upsert login intent/result so process restarts cannot lose credentials."""
    async with _worker_account_store_lock:
        async with provisioner.db_factory() as db:
            worker = await db.get(Worker, worker_id)
            if worker is None:
                raise RuntimeError("Worker record disappeared after account login")
            if worker.status != "ready" or worker.bootstrap_step is not None:
                # A late browser callback must never repopulate credentials
                # after destroy has started draining.  ``ready/destroy`` is a
                # reconciliation-only state and must not accept credentials.
                # This also closes the race where /pool/add read ready
                # immediately before the destroy lifecycle CAS.
                raise RuntimeError(
                    "Worker account persistence rejected while "
                    f"{worker.status}/{worker.bootstrap_step or 'normal'}"
                )
            if _has_worker_account_delete_outbox(worker.accounts):
                # A detached login callback must never replace a durable
                # deletion tombstone with credentials or reuse its remote
                # slot after an ACK-loss boundary.
                raise RuntimeError(
                    "Worker account persistence rejected while account "
                    "deletion is awaiting reconciliation"
                )
            # End the snapshot read transaction, then acquire the Worker row
            # as the cross-process writer fence.  The in-memory account lock
            # only serializes callbacks in this process; another Manager
            # process may have committed a deletion tombstone after the read.
            # Re-reading the JSON under this fence is therefore mandatory:
            # merge unrelated account changes, but never overwrite a durable
            # tombstone with credentials from a late login callback.
            await db.rollback()
            fenced = await db.execute(
                update(Worker)
                .where(
                    Worker.id == worker_id,
                    Worker.status == "ready",
                    Worker.bootstrap_step.is_(None),
                )
                .values(status=Worker.status, updated_at=Worker.updated_at)
                .execution_options(synchronize_session=False)
            )
            if fenced.rowcount != 1:
                await db.rollback()
                current = (
                    await db.execute(
                        select(Worker.status, Worker.bootstrap_step).where(
                            Worker.id == worker_id
                        )
                    )
                ).one_or_none()
                state = (
                    f"{current.status}/{current.bootstrap_step or 'normal'}"
                    if current is not None
                    else "missing"
                )
                raise RuntimeError(
                    f"Worker account persistence rejected while {state}"
                )
            worker = (
                await db.execute(
                    select(Worker)
                    .where(Worker.id == worker_id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one()
            if _has_worker_account_delete_outbox(worker.accounts):
                await db.rollback()
                raise RuntimeError(
                    "Worker account persistence rejected while account "
                    "deletion is awaiting reconciliation"
                )
            worker.accounts = _upsert_persisted_worker_account(
                worker.accounts,
                account,
                status=status,
                account_id=account_id,
            )
            await db.commit()


async def _settle_worker_account_delete(
    db: AsyncSession,
    worker_id: int,
    receipt: dict,
) -> Worker:
    """Remove only the exact tombstone after a confirmed idempotent DELETE."""

    await db.rollback()
    owner_user_id = receipt.get("worker_owner_user_id")
    destroy_lifecycle_nonce = receipt.get("worker_destroy_lifecycle_nonce")
    predicates = [
        Worker.id == worker_id,
        Worker.status.in_(tuple(_WORKER_ACCOUNT_DELETE_REPLAY_STATUSES)),
        Worker.bootstrap_step.is_(None),
        Worker.private_ip == receipt.get("worker_private_ip"),
        Worker.ccm_port == receipt.get("worker_ccm_port"),
        (
            Worker.owner_user_id.is_(None)
            if owner_user_id is None
            else Worker.owner_user_id == owner_user_id
        ),
        (
            Worker.destroy_lifecycle_nonce.is_(None)
            if destroy_lifecycle_nonce is None
            else Worker.destroy_lifecycle_nonce == destroy_lifecycle_nonce
        ),
    ]
    fenced = await db.execute(
        update(Worker)
        .where(*predicates)
        .values(status=Worker.status, updated_at=Worker.updated_at)
        .execution_options(synchronize_session=False)
    )
    if fenced.rowcount != 1:
        await db.rollback()
        raise HTTPException(
            409,
            "Worker 生命周期或 owner 在账号删除结算前发生变化；"
            "删除 tombstone 已保留",
        )
    worker = await db.get(Worker, worker_id, populate_existing=True)
    if worker is None:
        await db.rollback()
        raise HTTPException(404, "Worker not found")
    remaining = _settle_persisted_worker_account_delete(
        worker.accounts,
        worker,
        receipt,
    )
    worker.accounts = remaining
    await db.commit()
    await db.refresh(worker)
    return worker


def _provisioner():
    from backend.main import worker_provisioner

    if worker_provisioner is None:
        raise HTTPException(503, "Worker 功能未启用（WORKER_ENABLED=false 或缺少 boto3）")
    return worker_provisioner


@router.get("", response_model=list[WorkerResponse])
async def list_workers(request: Request, db: AsyncSession = Depends(get_db)):
    from backend.api.deps import get_current_user_id, get_current_user_role
    user_id = get_current_user_id(request)
    user_role = get_current_user_role(request)
    stmt = select(Worker).where(Worker.status != "terminated").order_by(desc(Worker.created_at))
    if user_role not in ("admin", "super_admin"):
        stmt = stmt.where(Worker.owner_user_id == user_id)
    result = await db.execute(stmt)
    return result.scalars().all()


@router.post("", response_model=WorkerResponse)
async def create_worker(body: WorkerCreate, request: Request, db: AsyncSession = Depends(get_db)):
    from backend.api.deps import lock_request_user_authority, require_admin
    require_admin(request)
    prov = _provisioner()
    if not body.name or not body.name.strip():
        raise HTTPException(400, "请填写 Worker 名称")
    # Fail before creating a DB job or a billable EC2 instance.  The same
    # preflight is repeated inside the background provisioner to close races
    # where a key is replaced between request validation and instance launch.
    from backend.services.ssh_executor import SSHKeyPreflightError
    try:
        prov.preflight_ssh_key()
    except SSHKeyPreflightError as exc:
        raise HTTPException(
            503,
            f"Worker SSH 密钥配置无效（{exc.code}）：{exc.detail}",
        ) from exc
    accounts = []
    for account in body.accounts:
        accounts.append(_normalize_worker_account(
            email=account.email,
            provider=account.provider,
            token=account.token,
            password=account.password,
            login_method=account.login_method,
            require_unattended=True,
        ))
    _reject_duplicate_worker_accounts(accounts)
    worker = Worker(
        name=body.name.strip(),
        status="creating",
        auth_token=secrets.token_hex(24),
        ssh_user=settings.worker_ssh_user,
        ssh_key_path=settings.worker_ssh_key_path,
        accounts=[{**account, "status": "pending"} for account in accounts],
    )
    # The JWT role cached by authentication is only a preliminary check.  Hold
    # the exact active User/role writer fence through creation so a concurrent
    # demotion cannot commit between ``require_admin`` and the durable job that
    # authorizes the background cloud effect.
    await lock_request_user_authority(request, db)
    db.add(worker)
    await db.commit()
    await db.refresh(worker)

    _spawn(
        prov.create_worker(worker.id, accounts=accounts)
    )
    return worker


@router.get("/{worker_id}", response_model=WorkerResponse)
async def get_worker(worker_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    from backend.api.deps import require_worker_access
    worker = await db.get(Worker, worker_id)
    if not worker:
        raise HTTPException(404, "Worker not found")
    await require_worker_access(request, worker)
    return worker


@router.get("/{worker_id}/logs", response_model=WorkerLogsResponse)
async def get_worker_logs(worker_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    from backend.api.deps import require_worker_access
    worker = await db.get(Worker, worker_id)
    if not worker:
        raise HTTPException(404, "Worker not found")
    await require_worker_access(request, worker)
    return WorkerLogsResponse(id=worker.id, bootstrap_log=worker.bootstrap_log)


async def _transition_worker_status(
    db: AsyncSession,
    request: Request,
    worker_id: int,
    *,
    allowed_statuses: tuple[str, ...] | frozenset[str],
    target_status: str,
    block_active_task_terminations: bool = False,
    destroy_recovery: bool = False,
    destroy_lifecycle_nonce: str | None = None,
    expected_destroy_lifecycle_nonce: str | None = None,
    require_bootstrap_step_none: bool = False,
) -> Worker:
    async with _worker_lifecycle_transaction_lock(worker_id):
        try:
            return await _transition_worker_status_locked(
                db,
                request,
                worker_id,
                allowed_statuses=allowed_statuses,
                target_status=target_status,
                block_active_task_terminations=block_active_task_terminations,
                destroy_recovery=destroy_recovery,
                destroy_lifecycle_nonce=destroy_lifecycle_nonce,
                expected_destroy_lifecycle_nonce=(
                    expected_destroy_lifecycle_nonce
                ),
                require_bootstrap_step_none=require_bootstrap_step_none,
            )
        except Exception:
            # Never release the same-connection transaction guard with a
            # rejected lifecycle CAS still pending.
            await db.rollback()
            raise


async def _transition_worker_status_locked(
    db: AsyncSession,
    request: Request,
    worker_id: int,
    *,
    allowed_statuses: tuple[str, ...] | frozenset[str],
    target_status: str,
    block_active_task_terminations: bool = False,
    destroy_recovery: bool = False,
    destroy_lifecycle_nonce: str | None = None,
    expected_destroy_lifecycle_nonce: str | None = None,
    require_bootstrap_step_none: bool = False,
) -> Worker:
    """Atomically claim a Worker lifecycle transition.

    Routes perform authorization from a read first.  End that read transaction
    before the compare-and-set so concurrent SQLite requests do not both try to
    upgrade a shared read lock.  Only the UPDATE winner may spawn background
    lifecycle work.
    """
    await db.rollback()
    from backend.api.deps import (
        get_current_user_id,
        get_current_user_role,
        lock_request_user_authority,
    )

    actor_auth_type = getattr(request.state, "auth_type", None)
    actor_role = get_current_user_role(request)
    actor_user_id = get_current_user_id(request)
    task_ids: list[int] = []
    if block_active_task_terminations:
        from backend.models.task import Task
        from backend.services.worker_task_termination import (
            active_worker_task_termination_receipt,
        )

        # Receipt admission and this destroy claim share the Task write lock.
        # SELECT FOR UPDATE covers PostgreSQL/MySQL; the exact no-op UPDATE is
        # the corresponding SQLite/MySQL CAS barrier.  Check only after those
        # locks so a concurrently committed receipt cannot be crossed by the
        # Worker lifecycle transition.
        task_ids = list(
            (
                await db.execute(
                    select(Task.id)
                    .where(Task.worker_id == worker_id)
                    .order_by(Task.id)
                    .with_for_update()
                )
            ).scalars()
        )
        await db.execute(
            update(Task)
            .where(Task.worker_id == worker_id)
            .values(status=Task.status)
        )
    worker_predicates = [
        Worker.id == worker_id,
        Worker.status.in_(tuple(allowed_statuses)),
    ]
    if require_bootstrap_step_none:
        worker_predicates.append(Worker.bootstrap_step.is_(None))
    # A member may operate only their own legacy Worker.  Bind that ownership
    # to the same Worker-row CAS that claims the lifecycle transition; an
    # ownership transfer which wins first therefore rejects this stale actor.
    # JWT admins are instead fenced by their exact role below.  Deployment and
    # scoped service credentials have no mutable User row and retain the route
    # authorization already enforced by the caller.
    if actor_auth_type == "jwt" and actor_role == "member":
        if (
            isinstance(actor_user_id, bool)
            or not isinstance(actor_user_id, int)
            or actor_user_id <= 0
        ):
            raise HTTPException(403, "User authority is invalid")
        worker_predicates.append(Worker.owner_user_id == actor_user_id)
    if target_status == "destroying":
        if (
            not isinstance(destroy_lifecycle_nonce, str)
            or len(destroy_lifecycle_nonce) != 32
            or any(
                char not in "0123456789abcdef"
                for char in destroy_lifecycle_nonce
            )
        ):
            raise ValueError("destroying transition requires a valid nonce")
    elif destroy_lifecycle_nonce is not None:
        raise ValueError("destroy nonce is valid only for destroying transition")
    if block_active_task_terminations:
        # A restart recovery is a narrower lifecycle than an ordinary destroy:
        # it may resume the exact Manager stop receipt admitted by the previous
        # claim, but only while the durable restart marker still identifies an
        # interrupted destroy.  Conversely, an ordinary destroy must not race
        # across a row which became a recovery lifecycle after its initial read.
        if destroy_recovery:
            worker_predicates.extend(
                (
                    Worker.status.in_(("ready", "error")),
                    Worker.bootstrap_step == "destroy",
                )
            )
            worker_predicates.append(
                Worker.destroy_lifecycle_nonce.is_(None)
                if expected_destroy_lifecycle_nonce is None
                else Worker.destroy_lifecycle_nonce
                == expected_destroy_lifecycle_nonce
            )
        else:
            worker_predicates.append(
                or_(
                    Worker.bootstrap_step.is_(None),
                    Worker.bootstrap_step != "destroy",
                )
            )
    transition_values = {
        "status": target_status,
        # Every ordinary lifecycle invalidates any historical destroy claim.
        # A fresh/recovery destroy atomically installs or retains its dedicated
        # nonce in the same Worker-row CAS.
        "destroy_lifecycle_nonce": (
            destroy_lifecycle_nonce
            if target_status == "destroying"
            else None
        ),
    }
    if target_status != "destroying" or not destroy_recovery:
        # A fresh lifecycle may never inherit cloud-effect authority from an
        # older nonce. Recovery retains the exact receipt installed by that
        # same nonce so an ambiguous termination can be replayed safely.
        transition_values["destroy_termination_receipt"] = None
    result = await db.execute(
        update(Worker)
        .where(*worker_predicates)
        .values(**transition_values)
    )
    if result.rowcount != 1:
        await db.rollback()
        current_status = await db.scalar(
            select(Worker.status).where(Worker.id == worker_id)
        )
        if current_status is None:
            raise HTTPException(404, "Worker not found")
        raise HTTPException(
            409,
            f"Worker 当前状态 {current_status}，不允许该操作",
        )
    transitioned_worker = await db.get(
        Worker,
        worker_id,
        populate_existing=True,
    )
    if (
        transitioned_worker is None
        or _has_worker_account_delete_outbox(transitioned_worker.accounts)
    ):
        await db.rollback()
        if transitioned_worker is None:
            raise HTTPException(404, "Worker not found")
        raise HTTPException(
            409,
            "Worker 有未结算的账号删除操作，请先重试该账号 DELETE",
        )
    if transitioned_worker.rename_tag_outbox is not None:
        await db.rollback()
        raise HTTPException(
            409,
            "Worker 有未结算的云标签重命名，请先重试 rename",
        )
    # Keep the exact active User/role fence in this transaction until the
    # lifecycle CAS commits.  It precedes receipt/aggregate inspection so the
    # global writer order remains Task -> Worker -> User -> receipt.  This
    # closes demotion/disablement after the route's preliminary authorization
    # and before its background cloud work.
    try:
        await lock_request_user_authority(request, db)
    except Exception:
        await db.rollback()
        raise
    if block_active_task_terminations:
        # Plan/Run writers take this same Worker row as their admission fence.
        # Once the CAS above owns ``destroying``, no new Worker Plan generation
        # can commit. Historical terminal rows are audit, not runtime owners;
        # only active/unarchived/native-runtime evidence blocks destruction.
        plan_rows, run_rows = await _worker_plan_runtime_blockers(db, worker_id)
        if plan_rows or run_rows:
            await db.rollback()
            raise HTTPException(
                409,
                _worker_plan_ownership_block_detail(plan_rows, run_rows),
            )
        pr_rows = await _worker_pr_monitor_runtime_blockers(db, worker_id)
        if pr_rows:
            await db.rollback()
            raise HTTPException(
                409,
                _worker_pr_monitor_ownership_block_detail(pr_rows),
            )

        # Global cross-process order is Task -> Worker -> User -> receipt. The
        # status change and User fence remain uncommitted until every receipt
        # has been checked; a blocker rolls the lifecycle claim back atomically.
        blocked_task_id = None
        for task_id in task_ids:
            receipt = await active_worker_task_termination_receipt(
                db,
                task_id,
                for_update=True,
            )
            if receipt is None:
                continue
            if destroy_recovery and (
                receipt.side == "manager"
                and receipt.worker_id == worker_id
                and receipt.operation == "stop_session"
                and receipt.status in {"pending_remote", "awaiting_ack"}
            ):
                continue
            blocked_task_id = task_id
            break
        if blocked_task_id is not None:
            await db.rollback()
            raise HTTPException(
                409,
                "Worker destroy is blocked by active Task termination "
                f"receipt on Task {blocked_task_id}",
            )
    await db.commit()
    worker = await db.get(Worker, worker_id)
    if worker is None:  # Defensive: the row cannot normally disappear here.
        raise HTTPException(404, "Worker not found")
    await db.refresh(worker)
    return worker


async def _worker_plan_runtime_blockers(
    db: AsyncSession,
    worker_id: int,
) -> tuple[
    list[tuple[int, int | None, int | None]],
    list[tuple[int, int | None, str, int | None]],
]:
    """Return active or unclean first-class Plan evidence for one Worker."""

    from backend.main import dispatcher
    from backend.models.instance import Instance
    from backend.models.plan import Plan
    from backend.models.plan_agent import (
        PlanAgentRun,
        PlanAgentWorkerDispatchReceipt,
    )
    from backend.services.plan_agent_runner import active_plan_run_ids
    from backend.services.worker_plan_dispatch import (
        WorkerPlanDispatchConflict,
        snapshot_worker_dispatch_receipt,
        worker_mirror_run_is_clean,
    )

    worker_runs = list(
        (
            await db.execute(
                select(
                    PlanAgentRun.id,
                    PlanAgentRun.plan_id,
                    PlanAgentRun.status,
                    PlanAgentRun.instance_id,
                )
                .where(PlanAgentRun.worker_id == worker_id)
                .order_by(PlanAgentRun.id)
            )
        ).all()
    )
    worker_run_ids = {int(row.id) for row in worker_runs}
    unclean_run_ids: set[int] = set()
    for run_id in sorted(worker_run_ids):
        # A status string is not cleanup proof. Manager-side Worker Runs own
        # remote Step mirrors, so validate their exact dispatch history and
        # reject any contradictory local provider-runtime receipt.
        if not await worker_mirror_run_is_clean(db, run_id=run_id):
            unclean_run_ids.add(run_id)

    # Dispatch ownership is frozen on the receipt.  Query it directly instead
    # of reaching it only through the Run's current worker_id: a Run may have
    # drifted, been detached, or been lost while the old Worker still owns an
    # uncertain remote boundary.
    dispatch_receipts = list(
        (
            await db.execute(
                select(PlanAgentWorkerDispatchReceipt)
                .where(PlanAgentWorkerDispatchReceipt.worker_id == worker_id)
                .order_by(PlanAgentWorkerDispatchReceipt.id)
            )
        ).scalars()
    )
    dispatch_run_ids = {int(receipt.run_id) for receipt in dispatch_receipts}
    dispatch_runs = (
        {
            int(row.id): row
            for row in (
                await db.execute(
                    select(
                        PlanAgentRun.id,
                        PlanAgentRun.plan_id,
                        PlanAgentRun.status,
                        PlanAgentRun.instance_id,
                        PlanAgentRun.worker_id,
                        PlanAgentRun.generation,
                    ).where(PlanAgentRun.id.in_(sorted(dispatch_run_ids)))
                )
            ).all()
        }
        if dispatch_run_ids
        else {}
    )
    dispatch_plan_ids = {int(receipt.plan_id) for receipt in dispatch_receipts}
    dispatch_plans = (
        {
            int(row.id): row
            for row in (
                await db.execute(
                    select(
                        Plan.id,
                        Plan.target_task_id,
                        Plan.worker_id,
                    ).where(Plan.id.in_(sorted(dispatch_plan_ids)))
                )
            ).all()
        }
        if dispatch_plan_ids
        else {}
    )
    detached_dispatch_blockers: list[
        tuple[int, int | None, str, int | None]
    ] = []
    for receipt in dispatch_receipts:
        # Complete receipt history for Runs still owned by this Worker was
        # validated above, including historical settled generations.  This
        # second pass exists to catch frozen receipts whose Run/Plan drifted
        # away from the Worker and must remain fail-closed.
        if receipt.run_id in worker_run_ids:
            continue
        run = dispatch_runs.get(int(receipt.run_id))
        plan = dispatch_plans.get(int(receipt.plan_id))
        try:
            snapshot = snapshot_worker_dispatch_receipt(receipt)
            valid_shape = True
        except WorkerPlanDispatchConflict:
            snapshot = None
            valid_shape = False
        exact_identity = bool(
            run is not None
            and plan is not None
            and run.plan_id == receipt.plan_id
            and run.worker_id == receipt.worker_id
            and run.generation == receipt.run_generation
            and plan.worker_id == receipt.worker_id
            and plan.target_task_id == receipt.target_task_id
        )
        if (
            valid_shape
            and snapshot is not None
            and snapshot.status == "settled"
            and exact_identity
        ):
            continue
        blocker_status = (
            f"dispatch:{receipt.status}"
            if valid_shape
            else f"dispatch:malformed-{receipt.status}"
        )
        detached_dispatch_blockers.append(
            (
                int(receipt.run_id),
                int(receipt.plan_id),
                blocker_status,
                run.instance_id if run is not None else None,
            )
        )
    reverse_owner_run_ids = (
        set(
            (
                await db.execute(
                    select(Instance.current_plan_run_id).where(
                        Instance.current_plan_run_id.in_(worker_run_ids)
                    )
                )
            ).scalars()
        )
        if worker_run_ids
        else set()
    )
    live_run_ids = set(active_plan_run_ids())
    if dispatcher is not None:
        for lifecycle in getattr(dispatcher, "_running_tasks", {}).values():
            if lifecycle.done():
                continue
            for attribute in ("_ccm_plan_run_id", "_ccm_worker_plan_run_id"):
                run_id = getattr(lifecycle, attribute, None)
                if type(run_id) is int:
                    live_run_ids.add(run_id)

    terminal_statuses = {"completed", "failed", "cancelled"}
    run_rows = [
        tuple(row)
        for row in worker_runs
        if row.status not in terminal_statuses
        or row.instance_id is not None
        or row.id in unclean_run_ids
        or row.id in reverse_owner_run_ids
        or row.id in live_run_ids
    ]
    run_rows.extend(detached_dispatch_blockers)
    run_rows.sort(key=lambda row: (row[0], row[2]))
    plan_rows = [
        tuple(row)
        for row in (
            await db.execute(
                select(Plan.id, Plan.target_task_id, Plan.active_run_id)
                .where(
                    Plan.worker_id == worker_id,
                    Plan.active_run_id.is_not(None),
                )
                .order_by(Plan.id)
            )
        ).all()
    ]
    return plan_rows, run_rows


def _worker_plan_ownership_block_detail(
    plan_rows: list[tuple[int, int | None, int | None]],
    run_rows: list[tuple[int, int | None, str, int | None]],
) -> str:
    parts: list[str] = []
    if plan_rows:
        parts.append(
            "Plans "
            + ", ".join(
                f"{plan_id}(task={target_task_id}, active_run={active_run_id})"
                for plan_id, target_task_id, active_run_id in plan_rows[:20]
            )
        )
    if run_rows:
        parts.append(
            "Plan Runs "
            + ", ".join(
                f"{run_id}(plan={plan_id}, status={status}, instance={instance_id})"
                for run_id, plan_id, status, instance_id in run_rows[:20]
            )
        )
    return (
        "Worker destroy is blocked by active first-class Plan runtime: "
        + "; ".join(parts)
        + ". Cancel or finish the active Runs before retrying."
    )


async def _worker_pr_monitor_runtime_blockers(
    db: AsyncSession,
    worker_id: int,
) -> list[tuple[int, int, int, str]]:
    """Return active PR Monitor Runs whose repository routes to a Worker."""

    from backend.models.pr_monitor import MonitoredRepo, PRMonitorRun

    return [
        tuple(row)
        for row in (
            await db.execute(
                select(
                    PRMonitorRun.id,
                    PRMonitorRun.repo_id,
                    PRMonitorRun.pr_number,
                    PRMonitorRun.status,
                )
                .join(
                    MonitoredRepo,
                    MonitoredRepo.id == PRMonitorRun.repo_id,
                )
                .where(
                    MonitoredRepo.worker_id == worker_id,
                    PRMonitorRun.status.not_in(("merged", "closed")),
                )
                .order_by(PRMonitorRun.id)
            )
        ).all()
    ]


def _worker_pr_monitor_ownership_block_detail(
    rows: list[tuple[int, int, int, str]],
) -> str:
    return (
        "Worker destroy is blocked by active PR Monitor runtime: "
        + ", ".join(
            f"Run {run_id}(repo={repo_id}, PR={pr_number}, status={status})"
            for run_id, repo_id, pr_number, status in rows[:20]
        )
        + ". Finish or close these PR Monitor Runs before retrying."
    )


@router.post("/{worker_id}/stop", response_model=WorkerResponse)
async def stop_worker(worker_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    from backend.api.deps import require_worker_access
    worker = await db.get(Worker, worker_id)
    if not worker:
        raise HTTPException(404, "Worker not found")
    await require_worker_access(request, worker)
    if worker.bootstrap_step == "destroy":
        raise HTTPException(409, "Worker 有未完成的销毁操作，只能重试销毁")
    if worker.bootstrap_step is not None:
        raise HTTPException(
            409,
            "Worker 有未完成的 bootstrap 操作，只能使用 retry 恢复",
        )
    prov = _provisioner()
    worker = await _transition_worker_status(
        db,
        request,
        worker_id,
        allowed_statuses=("ready", "error"),
        target_status="stopping",
        require_bootstrap_step_none=True,
    )
    _spawn(prov.stop_worker(worker.id))
    return worker


@router.post("/{worker_id}/start", response_model=WorkerResponse)
async def start_worker(worker_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    from backend.api.deps import require_worker_access
    worker = await db.get(Worker, worker_id)
    if not worker:
        raise HTTPException(404, "Worker not found")
    await require_worker_access(request, worker)
    if worker.bootstrap_step == "destroy":
        raise HTTPException(409, "Worker 有未完成的销毁操作，只能重试销毁")
    if worker.bootstrap_step is not None:
        raise HTTPException(
            409,
            "Worker 有未完成的 bootstrap 操作，只能使用 retry 恢复",
        )
    prov = _provisioner()
    worker = await _transition_worker_status(
        db,
        request,
        worker_id,
        allowed_statuses=("stopped", "error"),
        target_status="starting",
        require_bootstrap_step_none=True,
    )
    _spawn(prov.start_worker(worker.id))
    return worker


@router.post("/{worker_id}/destroy", response_model=WorkerResponse)
async def destroy_worker(worker_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    from backend.api.deps import require_admin
    from backend.services.worker_proxy import (
        capture_worker_destroy_lifecycle_claim,
    )

    require_admin(request)
    prov = _provisioner()
    worker = await db.get(Worker, worker_id)
    if not worker:
        raise HTTPException(404, "Worker not found")
    destroy_recovery = worker.bootstrap_step == "destroy"
    if not destroy_recovery and worker.status == "stopped":
        raise HTTPException(
            409,
            "已关机的 Worker 必须先启动并完成远端排空协议，才能销毁",
        )
    if not destroy_recovery and worker.status == "error":
        raise HTTPException(
            409,
            "普通 error Worker 必须先重试创建/启动并用 ClientToken 对账，"
            "不能直接销毁",
        )
    expected_destroy_lifecycle_nonce = (
        worker.destroy_lifecycle_nonce if destroy_recovery else None
    )
    destroy_lifecycle_nonce = (
        expected_destroy_lifecycle_nonce
        if (
            isinstance(expected_destroy_lifecycle_nonce, str)
            and len(expected_destroy_lifecycle_nonce) == 32
            and all(
                char in "0123456789abcdef"
                for char in expected_destroy_lifecycle_nonce
            )
        )
        else secrets.token_hex(16)
    )
    worker = await _transition_worker_status(
        db,
        request,
        worker_id,
        allowed_statuses=(
            _WORKER_DESTROY_RECOVERY_STATUSES
            if destroy_recovery
            else _WORKER_FRESH_DESTROYABLE_STATUSES
        ),
        target_status="destroying",
        block_active_task_terminations=True,
        destroy_recovery=destroy_recovery,
        destroy_lifecycle_nonce=destroy_lifecycle_nonce,
        expected_destroy_lifecycle_nonce=(
            expected_destroy_lifecycle_nonce
        ),
    )
    destroy_claim = capture_worker_destroy_lifecycle_claim(worker)
    # 先把该 worker 的 task 全部迁回本机（执行态无损），再销毁实例
    _spawn(_migrate_back_then_destroy(prov, worker.id, destroy_claim))
    return worker


async def _persist_worker_destroy_termination_authorization(
    db_factory,
    *,
    provisioner,
    destroy_claim,
    proof: dict,
) -> dict:
    """Commit final proof authority before the first cloud termination call."""

    from backend.services.worker_proxy import (
        _worker_destroy_lifecycle_predicates,
        build_worker_destroy_termination_receipt,
        worker_destroy_provision_spec_digest,
        worker_destroy_termination_receipt_matches,
    )

    # Before installing irreversible authority, prove that both halves of the
    # row identity point at one EC2 instance: the durable RunInstances token
    # resolves to cloud_instance_id and EC2 reports the exact private endpoint
    # that signed the Worker drain proof.  Legacy journals are backfilled only
    # inside that proof path.
    async with db_factory() as db:
        worker_snapshot = await db.scalar(
            select(Worker).where(
                *_worker_destroy_lifecycle_predicates(destroy_claim)
            )
        )
    if worker_snapshot is None or worker_snapshot.auth_token != destroy_claim.auth_token:
        raise RuntimeError(
            "Worker destroy lifecycle changed before cloud identity proof"
        )
    identity = await provisioner.require_worker_cloud_identity(
        worker_snapshot,
        verify_private_ip=True,
    )
    cloud_scope = identity["cloud_scope"]
    client_token_digest = identity["client_token_digest"]
    provision_spec_digest = identity["provision_spec_digest"]

    async with db_factory() as db:
        # This no-op UPDATE is the cross-dialect Worker-row writer barrier.
        # It prevents endpoint/token/lifecycle replacement from crossing the
        # proof-to-outbox commit, including on SQLite where FOR UPDATE is inert.
        locked = await db.execute(
            update(Worker)
            .where(*_worker_destroy_lifecycle_predicates(destroy_claim))
            .values(status=Worker.status)
            .execution_options(synchronize_session=False)
        )
        if locked.rowcount != 1:
            await db.rollback()
            raise RuntimeError(
                "Worker destroy lifecycle changed before termination authority"
            )
        worker = await db.get(Worker, destroy_claim.worker_id, populate_existing=True)
        if worker is None or worker.auth_token != destroy_claim.auth_token:
            await db.rollback()
            raise RuntimeError(
                "Worker control credential changed before termination authority"
            )
        if (
            worker_destroy_provision_spec_digest(worker.provision_spec)
            != provision_spec_digest
        ):
            await db.rollback()
            raise RuntimeError(
                "Worker provision journal changed before termination authority"
            )
        candidate = build_worker_destroy_termination_receipt(
            destroy_claim,
            proof,
            cloud_scope=cloud_scope,
            provision_spec_digest=provision_spec_digest,
            client_token_digest=client_token_digest,
        )
        if worker.destroy_termination_receipt is not None:
            if not worker_destroy_termination_receipt_matches(
                worker,
                cloud_scope=cloud_scope,
                client_token_digest=client_token_digest,
            ):
                await db.rollback()
                raise RuntimeError(
                    "Worker has malformed durable termination authority"
                )
            existing = dict(worker.destroy_termination_receipt)
            await db.rollback()
            return existing
        worker.destroy_termination_receipt = candidate
        await db.commit()
        return candidate


async def _authorized_destroy_manager_blocker(
    db_factory,
    *,
    worker_id: int,
) -> str | None:
    """Recheck local ownership before replaying an authorized cloud effect."""

    from backend.models.pr_monitor import MonitoredRepo
    from backend.models.plan import Plan
    from backend.models.project import Project
    from backend.models.task import Task

    async with db_factory() as db:
        task_id = await db.scalar(
            select(Task.id).where(Task.worker_id == worker_id).limit(1)
        )
        project_id = await db.scalar(
            select(Project.id).where(Project.worker_id == worker_id).limit(1)
        )
        plan_id = await db.scalar(
            select(Plan.id).where(Plan.worker_id == worker_id).limit(1)
        )
        repo_id = await db.scalar(
            select(MonitoredRepo.id)
            .where(MonitoredRepo.worker_id == worker_id)
            .limit(1)
        )
        plan_rows, run_rows = await _worker_plan_runtime_blockers(db, worker_id)
        pr_rows = await _worker_pr_monitor_runtime_blockers(db, worker_id)
    identities = {
        "Task": task_id,
        "Project": project_id,
        "Plan": plan_id,
        "MonitoredRepo": repo_id,
    }
    local = [
        f"{kind} {identity}"
        for kind, identity in identities.items()
        if identity is not None
    ]
    if plan_rows or run_rows:
        local.append(_worker_plan_ownership_block_detail(plan_rows, run_rows))
    if pr_rows:
        local.append(_worker_pr_monitor_ownership_block_detail(pr_rows))
    return "; ".join(local) if local else None


async def _migrate_back_then_destroy(
    prov,
    worker_id: int,
    destroy_claim,
    db_factory=None,
):
    """Move every live Worker owner to Manager, then terminate the node.

    This workflow is intentionally fail-closed.  A failed Task stop, workspace
    copy, Task migration, or ownership check keeps every remaining pointer on
    the live Worker and restores it to ``ready`` for reconciliation.  There is
    no lossy ``worker_id = NULL`` fallback.
    """
    from backend.main import task_migrator, worker_relay
    from backend.api.tasks import _stop_worker_task_for_destroy
    from backend.models.pr_monitor import MonitoredRepo
    from backend.models.plan import Plan
    from backend.models.project import Project
    from backend.models.task import Task
    from backend.services.task_migrator import shared_workspace_sync_cache
    from backend.services.worker_proxy import WorkerProxy
    from sqlalchemy import select

    if db_factory is None:
        from backend.database import async_session as db_factory

    if destroy_claim.worker_id != worker_id:
        raise ValueError("Worker destroy claim does not match its coordinator")
    destroy_proxy = WorkerProxy(db_factory, worker_relay)
    try:
        claimed_worker = await destroy_proxy._require_destroy_lifecycle_claim(
            destroy_claim
        )
    except Exception as e:
        detail = f"Worker 销毁已拒绝：destroy lifecycle claim 已失效（{e}）"
        logger.error("destroy: worker %s blocked: %s", worker_id, detail)
        await _mark_worker_destroy_blocked(
            db_factory,
            destroy_claim=destroy_claim,
            detail=detail,
        )
        return

    # Final proof authority is a durable outbox.  If the Manager crashed after
    # committing it—or AWS accepted termination before the response/terminal
    # DB commit—never contact the now sealed or absent Worker again.  Recheck
    # Manager ownership and idempotently resume only the exact cloud effect.
    if claimed_worker.destroy_termination_receipt is not None:
        from backend.services.worker_proxy import (
            worker_destroy_termination_receipt_matches,
        )
        from backend.services.worker_provisioner import (
            worker_create_client_token_digest,
        )

        try:
            replay_scope = await prov._current_cloud_scope()
            replay_token_digest = worker_create_client_token_digest(
                claimed_worker.id,
                claimed_worker.auth_token,
            )
        except Exception:
            replay_scope = None
            replay_token_digest = ""
        if not worker_destroy_termination_receipt_matches(
            claimed_worker,
            cloud_scope=replay_scope,
            client_token_digest=replay_token_digest,
        ):
            detail = (
                "Worker 销毁已拒绝：durable cloud termination authority 损坏"
            )
            await _mark_worker_destroy_blocked(
                db_factory,
                destroy_claim=destroy_claim,
                detail=detail,
            )
            return
        blocker = await _authorized_destroy_manager_blocker(
            db_factory,
            worker_id=worker_id,
        )
        if blocker is not None:
            detail = (
                "Worker 销毁已拒绝：已授权的云终止重放发现 Manager 所有权回流（"
                + blocker
                + "）"
            )
            await _mark_worker_destroy_blocked(
                db_factory,
                destroy_claim=destroy_claim,
                detail=detail,
            )
            return
        if worker_relay is not None:
            try:
                await worker_relay.stop_worker(worker_id)
            except Exception as exc:
                logger.warning(
                    "destroy: authorized replay relay stop %s failed: %s",
                    worker_id,
                    exc,
                )
        await prov.destroy_worker(worker_id, destroy_claim=destroy_claim)
        return

    # Admission and background coordination are separate transactions. A Plan
    # may have committed just after the HTTP transition's snapshot, so repeat
    # the fail-closed ownership check before mutating or migrating any Task.
    async with db_factory() as db:
        plan_rows, run_rows = await _worker_plan_runtime_blockers(db, worker_id)
        pr_rows = await _worker_pr_monitor_runtime_blockers(db, worker_id)
    if plan_rows or run_rows:
        detail = _worker_plan_ownership_block_detail(plan_rows, run_rows)
        logger.error("destroy: worker %s blocked: %s", worker_id, detail)
        await _mark_worker_destroy_blocked(
            db_factory,
            destroy_claim=destroy_claim,
            detail=detail,
        )
        return
    if pr_rows:
        detail = _worker_pr_monitor_ownership_block_detail(pr_rows)
        logger.error("destroy: worker %s blocked: %s", worker_id, detail)
        await _mark_worker_destroy_blocked(
            db_factory,
            destroy_claim=destroy_claim,
            detail=detail,
        )
        return

    # All read-only Manager preflight blockers are clear. Close the Worker-side
    # admission gate before taking the first Task snapshot or issuing a stop.
    # The claim is durable and irreversible: after Task drain starts, failure
    # leaves ``ready/destroy`` for reconciliation and only the same claim may
    # resume; new Task/runtime/login mutations never reopen the node.
    try:
        await destroy_proxy.begin_claimed_destroy_drain(destroy_claim)
    except Exception as e:
        detail = f"Worker 销毁已拒绝：无法安装远端节点排空围栏（{e}）"
        logger.error("destroy: worker %s blocked: %s", worker_id, detail)
        await _mark_worker_destroy_blocked(
            db_factory,
            destroy_claim=destroy_claim,
            detail=detail,
        )
        return

    # TaskMigrator 已接受 destroying 状态作为迁移源，无需临时改 ready
    async with db_factory() as db:
        result = await db.execute(select(Task).where(Task.worker_id == worker_id))
        tasks = result.scalars().all()
    # Resume the exact stop receipt for every Task before migration.  The helper
    # is a no-op for an inert Task without a receipt, while terminal Tasks with
    # an awaiting ACK still need this call to finish durable reconciliation.
    for task in tasks:
        try:
            async with db_factory() as db:
                await _stop_worker_task_for_destroy(
                    task.id,
                    destroy_claim,
                    destroy_proxy,
                    db,
                )
            logger.info("destroy: settled task %s before migration", task.id)
        except Exception as e:
            logger.warning("destroy: failed to settle task %s: %s", task.id, e)
            detail = (
                f"Worker 销毁已拒绝：Task {task.id} 无法完成远端终止对账（{e}）"
            )
            await _mark_worker_destroy_blocked(
                db_factory,
                destroy_claim=destroy_claim,
                detail=detail,
            )
            return
    # Refresh task statuses after stopping
    async with db_factory() as db:
        result = await db.execute(select(Task).where(Task.worker_id == worker_id))
        tasks = result.scalars().all()
    # Every exact stop receipt has now waited for its process/output consumer.
    # Close cross-thread callbacks and terminal publication recovery, then
    # install the node-row runtime seal.  Its writer lock drains callbacks
    # already committing their exact generation and rejects every later one.
    # Log history is stable only after this acknowledgement.
    try:
        await destroy_proxy.seal_claimed_destroy_runtime(destroy_claim)
    except Exception as e:
        detail = f"Worker 销毁已拒绝：远端 runtime seal 未完成（{e}）"
        logger.error("destroy: worker %s blocked: %s", worker_id, detail)
        await _mark_worker_destroy_blocked(
            db_factory,
            destroy_claim=destroy_claim,
            detail=detail,
        )
        return
    # A live relay socket is not a durable delivery receipt.  After every
    # remote stop and the runtime writer seal have converged, fetch and
    # atomically import each exact current generation's complete non-user log
    # tail. Missing coverage keeps the Worker alive: otherwise a disconnect
    # immediately before the terminal assistant/tool row would make cloud
    # termination erase user history.
    try:
        await destroy_proxy.require_claimed_destroy_log_backfill(
            destroy_claim,
            {task.id for task in tasks},
        )
    except Exception as e:
        detail = f"Worker 销毁已拒绝：远端 Task 日志未完整回填（{e}）"
        logger.error("destroy: worker %s blocked: %s", worker_id, detail)
        await _mark_worker_destroy_blocked(
            db_factory,
            destroy_claim=destroy_claim,
            detail=detail,
        )
        return
    # Backfill revalidates and may adopt an exact authoritative terminal
    # snapshot.  Do not carry the pre-network ORM rows into migration.
    async with db_factory() as db:
        result = await db.execute(select(Task).where(Task.worker_id == worker_id))
        tasks = result.scalars().all()
    # Refuse to cut Manager routing away from the Worker when the remote node
    # still owns Browser/Harness evidence that TaskMigrator cannot import.
    # This first proof runs after every per-Task receipt/gate converged but
    # before any Task/Project pointer moves; the final proof below repeats it
    # immediately before cloud termination to close later races.
    try:
        await destroy_proxy.require_claimed_destroy_drain_proof(destroy_claim)
    except Exception as e:
        detail = f"Worker 销毁已拒绝：远端节点未完成迁移前排空证明（{e}）"
        logger.error("destroy: worker %s blocked: %s", worker_id, detail)
        await _mark_worker_destroy_blocked(
            db_factory,
            destroy_claim=destroy_claim,
            detail=detail,
        )
        return
    if task_migrator is None:
        detail = "Worker 销毁已拒绝：Task migrator is unavailable"
        await _mark_worker_destroy_blocked(
            db_factory,
            destroy_claim=destroy_claim,
            detail=detail,
        )
        return

    # Project is durable workspace ownership even when it has zero Tasks.
    # Stop every Task first, then copy each unique path once before any pointer
    # is moved. TaskMigrator's ContextVar also deduplicates projectless Tasks
    # that name the same explicit target_repo.
    async with db_factory() as db:
        source_worker = await db.get(Worker, worker_id)
        projects = list(
            (
                await db.execute(
                    select(Project)
                    .where(Project.worker_id == worker_id)
                    .order_by(Project.id)
                )
            ).scalars()
        )
    if source_worker is None:
        detail = "Worker 销毁已拒绝：Worker record disappeared"
        await _mark_worker_destroy_blocked(
            db_factory,
            destroy_claim=destroy_claim,
            detail=detail,
        )
        return

    try:
        async with shared_workspace_sync_cache():
            for project in projects:
                if project.local_path:
                    await task_migrator.sync_workspace_once(
                        source_worker,
                        None,
                        project.local_path,
                    )
            for task in tasks:
                await task_migrator.migrate(task.id, None)

                # A successful return must have cut the source pointer.  Never
                # let a buggy/mixed-version migrator response authorize cloud
                # termination while the durable Task still names this Worker.
                async with db_factory() as db:
                    remaining_worker_id = await db.scalar(
                        select(Task.worker_id).where(Task.id == task.id)
                    )
                if remaining_worker_id == worker_id:
                    raise RuntimeError(
                        f"Task {task.id}:{task.status} migration returned "
                        "without changing Worker ownership"
                    )
    except Exception as e:
        logger.warning("destroy: ownership migration failed: %s", e)
        detail = f"Worker 销毁已拒绝：所有权迁移失败（{e}）"
        await _mark_worker_destroy_blocked(
            db_factory,
            destroy_claim=destroy_claim,
            detail=detail,
        )
        return

    # Rehome durable routing configuration only after every workspace and Task
    # has moved.  Lock MonitoredRepo rows before the final PR runtime check so a
    # webhook using the canonical repo writer fence cannot cross this cut.
    async with db_factory() as db:
        await db.execute(
            update(MonitoredRepo)
            .where(MonitoredRepo.worker_id == worker_id)
            .values(worker_id=MonitoredRepo.worker_id)
        )
        plan_rows, run_rows = await _worker_plan_runtime_blockers(db, worker_id)
        pr_rows = await _worker_pr_monitor_runtime_blockers(db, worker_id)
        if plan_rows or run_rows or pr_rows:
            await db.rollback()
            detail = (
                _worker_plan_ownership_block_detail(plan_rows, run_rows)
                if plan_rows or run_rows
                else _worker_pr_monitor_ownership_block_detail(pr_rows)
            )
            await _mark_worker_destroy_blocked(
                db_factory,
                destroy_claim=destroy_claim,
                detail=detail,
            )
            return
        await db.execute(
            update(Project)
            .where(Project.worker_id == worker_id)
            .values(worker_id=None)
        )
        # PlanAgentRun.worker_id remains immutable historical execution
        # evidence.  Plan.worker_id is the mutable routing owner and can move
        # to Manager only after every active/unclean Run gate above is clear.
        await db.execute(
            update(Plan)
            .where(
                Plan.worker_id == worker_id,
                Plan.active_run_id.is_(None),
            )
            .values(worker_id=None)
        )
        await db.execute(
            update(MonitoredRepo)
            .where(MonitoredRepo.worker_id == worker_id)
            .values(worker_id=None)
        )
        await db.commit()

    # Final durable gate: normal assignment rejects a ``destroying`` target,
    # but an older process or a failed fallback must still not strand a Task on
    # an instance we are about to terminate.
    async with db_factory() as db:
        remaining = list(
            (
                await db.execute(
                    select(Task.id, Task.status).where(
                        Task.worker_id == worker_id
                    )
                )
            ).all()
        )
        remaining_projects = list(
            (
                await db.execute(
                    select(Project.id, Project.status).where(
                        Project.worker_id == worker_id
                    )
                )
            ).all()
        )
        remaining_repos = list(
            (
                await db.execute(
                    select(MonitoredRepo.id, MonitoredRepo.status).where(
                        MonitoredRepo.worker_id == worker_id
                    )
                )
            ).all()
        )
        remaining_plans = list(
            (
                await db.execute(
                    select(Plan.id, Plan.active_run_id).where(
                        Plan.worker_id == worker_id
                    )
                )
            ).all()
        )
        plan_rows, run_rows = await _worker_plan_runtime_blockers(db, worker_id)
        pr_rows = await _worker_pr_monitor_runtime_blockers(db, worker_id)
    if (
        remaining
        or remaining_projects
        or remaining_repos
        or remaining_plans
        or plan_rows
        or run_rows
        or pr_rows
    ):
        blockers: list[str] = []
        if remaining:
            blockers.append(
                "Tasks "
                + ", ".join(
                    f"{task_id}:{status}" for task_id, status in remaining[:20]
                )
            )
        if remaining_projects:
            blockers.append(
                "Projects "
                + ", ".join(
                    f"{project_id}:{status}"
                    for project_id, status in remaining_projects[:20]
                )
            )
        if remaining_repos:
            blockers.append(
                "MonitoredRepos "
                + ", ".join(
                    f"{repo_id}:{status}"
                    for repo_id, status in remaining_repos[:20]
                )
            )
        if remaining_plans:
            blockers.append(
                "Plans "
                + ", ".join(
                    f"{plan_id}:active_run={active_run_id}"
                    for plan_id, active_run_id in remaining_plans[:20]
                )
            )
        if plan_rows or run_rows:
            blockers.append(
                _worker_plan_ownership_block_detail(plan_rows, run_rows)
            )
        if pr_rows:
            blockers.append(
                _worker_pr_monitor_ownership_block_detail(pr_rows)
            )
        detail = "Worker 销毁已拒绝：仍有持久所有权指向该 Worker（" + "; ".join(blockers) + "）"
        logger.error("destroy: worker %s blocked: %s", worker_id, detail)
        await _mark_worker_destroy_blocked(
            db_factory,
            destroy_claim=destroy_claim,
            detail=detail,
        )
        return
    try:
        await destroy_proxy._require_destroy_lifecycle_claim(destroy_claim)
    except Exception as e:
        detail = f"Worker 销毁已拒绝：destroy lifecycle claim 已失效（{e}）"
        logger.error("destroy: worker %s blocked: %s", worker_id, detail)
        await _mark_worker_destroy_blocked(
            db_factory,
            destroy_claim=destroy_claim,
            detail=detail,
        )
        return
    # Manager ownership pointers prove only that routing moved.  The remote
    # database must additionally prove that every low-range mirror completed
    # its exact stop receipt and owner gate, no Worker-local high-range child
    # remains active, and no Instance/Harness/Workspace/Sandbox/Binding owner
    # would be lost with the node.  The proof is fresh, nonce-bound, signed by
    # the Worker control credential, and deliberately runs after all per-Task
    # receipts have converged.
    try:
        final_proof = await destroy_proxy.require_claimed_destroy_drain_proof(
            destroy_claim
        )
    except Exception as e:
        detail = f"Worker 销毁已拒绝：远端节点未完成安全排空证明（{e}）"
        logger.error("destroy: worker %s blocked: %s", worker_id, detail)
        await _mark_worker_destroy_blocked(
            db_factory,
            destroy_claim=destroy_claim,
            detail=detail,
        )
        return
    try:
        await _persist_worker_destroy_termination_authorization(
            db_factory,
            provisioner=prov,
            destroy_claim=destroy_claim,
            proof=final_proof,
        )
    except Exception as e:
        detail = (
            "Worker 销毁已拒绝：无法持久化云终止授权（"
            + str(e)
            + "）"
        )
        logger.error("destroy: worker %s blocked: %s", worker_id, detail)
        await _mark_worker_destroy_blocked(
            db_factory,
            destroy_claim=destroy_claim,
            detail=detail,
        )
        return
    if worker_relay is not None:
        try:
            await worker_relay.stop_worker(worker_id)
        except Exception as e:
            # Relay is Manager-local cleanup.  A stale relay must not prevent
            # the cloud termination attempt or strand the row in destroying.
            logger.warning("destroy: stop worker relay %s failed: %s", worker_id, e)
    await prov.destroy_worker(worker_id, destroy_claim=destroy_claim)


async def _mark_worker_destroy_blocked(
    db_factory,
    *,
    destroy_claim,
    detail: str,
) -> None:
    """Restore reconciliation without reopening new Worker assignment.

    Handoff recovery deliberately runs only for ``ready`` Workers.  Leaving a
    blocked destroy in ``error`` would therefore make a handoff impossible to
    settle.  ``ready/destroy`` keeps the relay alive, while all public routing
    and durable assignment fences require ``bootstrap_step IS NULL``.  A later
    destroy treats this exact marker as recovery instead of ordinary admission.
    """
    from backend.services.worker_proxy import (
        _worker_destroy_lifecycle_predicates,
    )

    async with db_factory() as db:
        result = await db.execute(
            update(Worker)
            .where(*_worker_destroy_lifecycle_predicates(destroy_claim))
            .values(
                status="ready",
                # Keep relay/handoff reconciliation alive without reopening
                # this Worker to new Project/Task/Plan/Repo assignments.
                # Public routing and assignment fences require step=None.
                bootstrap_step="destroy",
                bootstrap_error=detail[:2000],
            )
        )
        if result.rowcount == 1:
            await db.commit()
        else:
            await db.rollback()


@router.post("/{worker_id}/retry", response_model=WorkerResponse)
async def retry_bootstrap(worker_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    """error 状态下重跑创建/bootstrap 流程。"""
    from backend.api.deps import require_worker_access
    worker = await db.get(Worker, worker_id)
    if worker:
        await require_worker_access(request, worker)
    prov = _provisioner()
    if worker is None:
        raise HTTPException(404, "Worker not found")
    if worker.status != "error":
        raise HTTPException(
            409,
            f"Worker 当前状态 {worker.status}，不允许该操作",
        )
    if worker.bootstrap_step == "destroy":
        raise HTTPException(409, "Worker 有未完成的销毁操作，只能重试销毁")
    if _has_worker_account_delete_outbox(worker.accounts):
        raise HTTPException(
            409,
            "Worker 有未结算的账号删除操作，请先恢复 Worker 并重试该账号 "
            "DELETE",
        )
    # 从 DB 读已有账号信息，retry 时重新登录。历史记录没有
    # provider，它们均由旧 Claude-only Worker 链路创建。
    saved_accounts = worker.accounts or []
    accounts = []
    for account in saved_accounts:
        email = str(account.get("email", "")).strip()
        if not email:
            raise HTTPException(409, "Worker 保存的账号缺少 email，无法重试")
        try:
            provider = _normalize_worker_account_provider(
                account.get("provider") or "claude"
            )
            saved_status = account.get("status")
            saved_status_is_conclusive = (
                isinstance(saved_status, str)
                and saved_status in {"logged_in", "failed"}
            )
            if (
                worker.bootstrap_step == "account-login"
                and provider == "claude"
                and not saved_status_is_conclusive
            ):
                raise HTTPException(
                    409,
                    "Claude 账号登录结果不确定，不能安全重放；"
                    "请先人工核验或重建 Worker",
                )
            token = account.get("token") or ""
            password = account.get("password") or ""
            if not isinstance(token, str) or not isinstance(password, str):
                raise HTTPException(400, "保存的账号凭据格式无效")
            normalized = _normalize_worker_account(
                email=email,
                provider=provider,
                token=token,
                password=password,
                login_method=account.get("login_method"),
                require_unattended=True,
            )
            account_id = account.get("account_id") or ""
            if not isinstance(account_id, str):
                raise HTTPException(400, "保存的账号 account_id 格式无效")
            if account_id.strip():
                normalized["account_id"] = account_id.strip()
            if provider == "claude" and saved_status_is_conclusive:
                # Claude has no live, idempotent slot verification comparable
                # to Codex. A logged_in outcome is reusable only when it is
                # bound to this exact cloud/provision generation; legacy or
                # replaced-node outcomes are safely downgraded to a retry.
                if (
                    saved_status == "logged_in"
                    and claude_login_identity_matches(worker, account)
                ):
                    normalized["status"] = "logged_in"
                    normalized[CLAUDE_LOGIN_IDENTITY_KEY] = dict(
                        account[CLAUDE_LOGIN_IDENTITY_KEY]
                    )
                else:
                    normalized["status"] = "failed"
        except HTTPException as exc:
            raise HTTPException(
                409,
                f"账号 {email} 的保存登录信息无效，无法重试：{exc.detail}",
            ) from exc
        accounts.append(normalized)
    try:
        _reject_duplicate_worker_accounts(accounts)
    except HTTPException as exc:
        raise HTTPException(409, f"Worker 保存了重复账号，无法重试：{exc.detail}") from exc
    worker = await _transition_worker_status(
        db,
        request,
        worker_id,
        allowed_statuses=("error",),
        target_status="creating",
    )
    _spawn(
        prov.create_worker(worker.id, accounts=accounts)
    )
    return worker


@router.get("/{worker_id}/pool")
async def get_worker_pool(
    worker_id: int,
    request: Request,
    provider: str = "codex",
    db: AsyncSession = Depends(get_db),
):
    """实时拉取 Worker 上指定 provider 的账号池状态。"""
    worker = await db.get(Worker, worker_id)
    if not worker:
        raise HTTPException(404, "Worker not found")
    from backend.api.deps import require_worker_access as _rwa
    await _rwa(request, worker)
    if worker.status != "ready" or not worker.private_ip:
        raise HTTPException(409, f"Worker 未就绪（{worker.status}）")
    provider = _normalize_worker_account_provider(provider)
    worker = await _lock_worker_effect_access(
        request,
        db,
        worker_id,
        predicates=(
            Worker.status == "ready",
            Worker.bootstrap_step.is_(None),
            Worker.private_ip.is_not(None),
        ),
        conflict_detail=(
            "Worker ownership, authority, or lifecycle changed before pool "
            "status read"
        ),
    )
    await db.commit()
    status_path = (
        "/api/codex-pool/status" if provider == "codex" else "/api/pool/status"
    )
    r = await _worker_http_request(
        worker,
        "GET",
        status_path,
        timeout=10,
        allow_statuses=frozenset({404}) if provider == "claude" else frozenset(),
    )
    if provider == "claude" and r.status_code == 404:
        # worker 端 POOL_ENABLED=false：单账号模式。
        # 老版 worker 没有账号查询端点，经 SSH 读 ~/.claude.json
        # 的 oauthAccount.emailAddress 兜底，让用户知道用的是哪个号
        email = None
        try:
            from backend.services.ssh_executor import (
                SSHExecutor,
                worker_known_hosts_path,
            )
            ssh = SSHExecutor(
                host=worker.private_ip,
                user=worker.ssh_user,
                key_path=(worker.ssh_key_path or settings.worker_ssh_key_path),
                known_hosts_path=(
                    worker_known_hosts_path(worker.cloud_instance_id)
                    if worker.cloud_instance_id else None
                ),
            )
            code, out = await ssh.run(
                "python3 -c \"import json;"
                "print(json.load(open('/home/'+__import__('getpass').getuser()+'/.claude.json'))"
                ".get('oauthAccount',{}).get('emailAddress',''))\"",
                timeout=15,
            )
            if code == 0 and out.strip():
                email = out.strip().splitlines()[-1]
        except Exception:
            email = None
        accounts = (
            [{"id": "default", "email": email, "enabled": True,
              "available": True, "cooldown_remaining": 0}]
            if email else []
        )
        return {"enabled": True, "total": len(accounts),
                "available": len(accounts), "accounts": accounts}
    return _worker_response_json(r)


@router.post("/{worker_id}/pool/add")
async def add_worker_account(worker_id: int, request: Request, body: dict, db: AsyncSession = Depends(get_db)):
    """在 Worker 上添加 Codex（默认）或兼容 Claude 账号。"""
    worker = await db.get(Worker, worker_id)
    if not worker:
        raise HTTPException(404, "Worker not found")
    from backend.api.deps import require_worker_access as _rwa
    await _rwa(request, worker)
    if (
        worker.status != "ready"
        or worker.bootstrap_step is not None
        or not worker.private_ip
    ):
        raise HTTPException(
            409,
            "Worker 未就绪"
            f"（{worker.status}/{worker.bootstrap_step or 'normal'}）",
        )

    raw_email = body.get("email", "")
    raw_token = body.get("token", "")
    raw_password = body.get("password", "")
    raw_provider = body.get("provider", "codex")
    if not all(
        isinstance(value, str)
        for value in (raw_email, raw_token, raw_password, raw_provider)
    ):
        raise HTTPException(400, "email/provider/token/password 必须是字符串")
    account = _normalize_worker_account(
        email=raw_email,
        provider=raw_provider,
        token=raw_token,
        password=raw_password,
        login_method=body.get("login_method"),
        require_unattended=True,
    )
    email = account["email"]
    provider = account["provider"]
    if provider == "claude":
        # The historical dynamic path started ``auto_login.py`` over SSH and
        # returned immediately.  No Worker-local journal or terminal receipt
        # survived a Manager restart, so a destroy proof could race a detached
        # credential writer.  Worker bootstrap can still provision Claude
        # accounts; ready-node dynamic add stays disabled until it is moved to
        # a Worker-local recoverable transaction protocol.
        raise HTTPException(
            409,
            "动态添加 Claude Worker 账号暂不可用：该旧 SSH 登录路径无法提供"
            "可恢复的终态证明。请在创建/重新引导 Worker 时配置 Claude 账号。",
        )

    # Email identity is case-insensitive.  Normalize the in-memory admission
    # key as well as the persisted lookup so differently-cased concurrent
    # requests cannot start two browser logins for the same account.
    state_key = f"{worker_id}:{provider}:{email.casefold()}"

    if provider == "codex":
        prov = _provisioner()
        async with _worker_login_admission_lock:
            existing_state = _worker_login_state.get(state_key, {})
            if existing_state.get("status") in _WORKER_ACTIVE_LOGIN_STATUSES:
                return {
                    "ok": True,
                    "provider": provider,
                    **{
                        key: existing_state[key]
                        for key in (
                            "status", "attempt_id", "challenge_id",
                            "expires_at", "account_id",
                        )
                        if existing_state.get(key) is not None
                    },
                }
            async with _worker_account_store_lock:
                # Ownership/role authorization and the durable login intent
                # are one transaction.  An owner transfer or admin demotion
                # that commits first therefore prevents the detached browser
                # flow from ever being launched.
                current_worker = await _lock_worker_effect_access(
                    request,
                    db,
                    worker_id,
                    predicates=(
                        Worker.status == "ready",
                        Worker.bootstrap_step.is_(None),
                        Worker.private_ip.is_not(None),
                    ),
                    conflict_detail=(
                        "Worker ownership, authority, or lifecycle changed "
                        "before account login admission"
                    ),
                )
                if _has_worker_account_delete_outbox(current_worker.accounts):
                    raise HTTPException(
                        409,
                        "Worker 有未结算的账号删除操作；为避免复用正在删除的"
                        "远端槽位，暂不能添加账号",
                    )
                persisted_matches = [
                    item for item in (current_worker.accounts or [])
                    if isinstance(item, dict)
                    and str(item.get("provider") or "claude").lower()
                    == provider
                    and str(item.get("email") or "").strip().casefold()
                    == email.casefold()
                ]
                if len(persisted_matches) > 1:
                    raise HTTPException(
                        409,
                        "Manager 中存在重复的 Worker 账号记录，请先清理",
                    )
                if persisted_matches:
                    persisted = persisted_matches[0]
                    persisted_status = str(persisted.get("status") or "")
                    if persisted_status == "logged_in":
                        raise HTTPException(409, "该 Codex 邮箱已在 Worker 号池中")
                    if persisted_status == "pending":
                        # Resume an intent that survived Manager restart
                        # without replacing its known-good credentials from
                        # an add form.
                        account = dict(persisted)
                    elif persisted.get("account_id"):
                        # A failed slot is an explicit retry: retain its
                        # identity while allowing corrected credentials.
                        account["account_id"] = persisted["account_id"]
                current_worker.accounts = _upsert_persisted_worker_account(
                    current_worker.accounts,
                    account,
                    status="pending",
                )
                await db.commit()

            worker = current_worker
            _worker_login_state[state_key] = {
                "status": "running",
                "provider": provider,
                "started_at": time.time(),
            }

        async def _publish_codex_status(remote_state: dict) -> None:
            current = _worker_login_state.get(state_key, {})
            safe = {
                key: remote_state[key]
                for key in (
                    "status", "detail", "attempt_id", "challenge_id",
                    "expires_at", "account_id",
                )
                if remote_state.get(key) is not None
            }
            # No remote terminal status is the Manager transaction boundary:
            # credentials/account_id or retryable failure still need to commit
            # to Worker.accounts.  Keep DELETE/retry blocked until _run_codex
            # performs the final DB write and publishes the sole terminal
            # state.  This includes unexpected/idle remote states because
            # ensure_codex_account raises only after this callback returns.
            remote_status = safe.get("status")
            if (
                remote_status is not None
                and remote_status not in _WORKER_ACTIVE_LOGIN_STATUSES
            ):
                safe["status"] = (
                    "cancelling" if remote_status == "cancelled" else "finalizing"
                )
            _worker_login_state[state_key] = {
                **current,
                **safe,
                "provider": provider,
            }
            remote_account_id = str(remote_state.get("account_id") or "").strip()
            if remote_account_id and account.get("account_id") != remote_account_id:
                account["account_id"] = remote_account_id
                await _persist_worker_account_state(
                    prov,
                    worker_id,
                    account,
                    status="pending",
                    account_id=remote_account_id,
                )

        async def _run_codex():
            try:
                account_id = await prov.ensure_codex_account(
                    worker,
                    account,
                    allow_manual_otp=True,
                    on_status=_publish_codex_status,
                )
                if not account_id:
                    raise RuntimeError("Worker Codex login returned no account id")
                await _persist_worker_account_state(
                    prov,
                    worker_id,
                    account,
                    status="logged_in",
                    account_id=account_id,
                )
                _worker_login_state[state_key] = {
                    "status": "success",
                    "provider": provider,
                    "account_id": account_id,
                }
            except Exception as exc:
                logger.warning(
                    "Worker %s Codex account login failed for %s: %s",
                    worker_id,
                    email,
                    exc,
                )
                failed_state = {
                    **_worker_login_state.get(state_key, {}),
                    "status": "failed",
                    "provider": provider,
                    "detail": str(exc)[-1000:],
                }
                try:
                    await _persist_worker_account_state(
                        prov,
                        worker_id,
                        account,
                        status="failed",
                        account_id=(
                            str(account.get("account_id") or "").strip() or None
                        ),
                    )
                except Exception:
                    logger.exception(
                        "Failed to persist Worker %s Codex login failure for %s",
                        worker_id,
                        email,
                    )
                finally:
                    # A terminal state is also the promise that no later DB
                    # write from this login remains.  DELETE relies on that
                    # ordering to prevent removed credentials being revived.
                    _worker_login_state[state_key] = failed_state

        _spawn(_run_codex())
        return {"ok": True, "status": "running", "provider": provider}

    _worker_login_state[state_key] = {
        "status": "running",
        "provider": provider,
        "started_at": time.time(),
    }

    from backend.config import settings
    from backend.services.ssh_executor import SSHExecutor, worker_known_hosts_path
    ssh = SSHExecutor(host=worker.private_ip, user=worker.ssh_user,
                      key_path=worker.ssh_key_path or settings.worker_ssh_key_path,
                      known_hosts_path=(
                          worker_known_hosts_path(worker.cloud_instance_id)
                          if worker.cloud_instance_id else None
                      ))

    # 算 slot 名：查 worker 现有账号数
    try:
        r = await _worker_http_request(
            worker,
            "GET",
            "/api/pool/status",
            timeout=10,
            allow_statuses=frozenset({404}),
        )
    except HTTPException as exc:
        _worker_login_state[state_key] = {
            "status": "failed",
            "provider": provider,
            "detail": str(exc.detail),
        }
        raise
    if r.status_code == 404:
        # Explicit legacy POOL_ENABLED=false is the only safe empty-pool
        # fallback.  Auth/5xx/connectivity failures must stop before choosing
        # ``default`` and potentially colliding with an existing account.
        existing = 0
    else:
        pool_status = _worker_response_json(r)
        if not isinstance(pool_status, dict) or not isinstance(
            pool_status.get("accounts"), list
        ):
            raise HTTPException(502, "Worker Claude 号池返回了无效状态")
        existing = len(pool_status["accounts"])

    slot = f"account-{existing + 1}" if existing > 0 else "default"
    remote_dir = settings.worker_remote_dir

    # 后台跑 auto_login（xvfb-run 包装）
    cmd = _build_add_account_command(
        remote_dir,
        email=email,
        token=account["token"],
        slot=slot,
        login_method=account["login_method"],
    )

    # 这个任务可能跑 1-2 分钟，用 fire-and-forget
    async def _run():
        code, out = await ssh.run(cmd, timeout=600, sensitive=True)
        _worker_login_state[state_key] = {
            "status": "success" if code == 0 else "failed",
            "provider": provider,
            "detail": out[-1000:],
        }

    _spawn(_run())
    return {"ok": True, "status": "running", "provider": provider, "slot": slot}


async def _settle_recovered_worker_codex_login(
    worker: Worker,
    account: dict,
    remote: dict,
) -> None:
    """Import a terminal Worker-local login after Manager process loss."""

    status = str(remote.get("status") or "idle")
    if status in _WORKER_ACTIVE_LOGIN_STATUSES or status == "idle":
        return
    prov = _provisioner()
    account_id = str(remote.get("account_id") or "").strip() or None
    if status == "success":
        if account_id is None:
            raise HTTPException(
                502,
                "Worker Codex 登录成功状态缺少 account_id",
            )
        # Worker add-state is process-local and can outlive later pool-slot
        # deletion.  Re-prove the exact id/email before making Manager
        # credentials durable as logged_in.
        pool = await prov.worker_local_api(
            worker,
            "GET",
            "/api/codex-pool/status",
            timeout=30,
        )
        accounts = pool.get("accounts") if isinstance(pool, dict) else None
        matches = [
            item
            for item in (accounts or [])
            if isinstance(item, dict)
            and item.get("id") == account_id
            and str(item.get("email") or "").strip().casefold()
            == str(account.get("email") or "").strip().casefold()
        ]
        if not isinstance(accounts, list) or len(matches) != 1:
            raise HTTPException(
                409,
                "Worker Codex 登录成功记录与当前远端账号槽位不一致",
            )
        await _persist_worker_account_state(
            prov,
            worker.id,
            dict(account),
            status="logged_in",
            account_id=account_id,
        )
        return
    if status in _WORKER_LOGIN_TERMINAL_FAILURE_STATUSES:
        await _persist_worker_account_state(
            prov,
            worker.id,
            dict(account),
            status="failed",
            account_id=account_id,
        )
        return
    raise HTTPException(502, f"Worker Codex 登录返回未知状态: {status}")


@router.get("/{worker_id}/pool/add/{email}")
async def worker_add_status(
    worker_id: int,
    email: str,
    request: Request,
    provider: str = "codex",
    db: AsyncSession = Depends(get_db),
):
    worker = await db.get(Worker, worker_id)
    if not worker:
        raise HTTPException(404, "Worker not found")
    from backend.api.deps import require_worker_access as _rwa
    await _rwa(request, worker)
    provider = _normalize_worker_account_provider(provider)
    worker = await _lock_worker_effect_access(
        request,
        db,
        worker_id,
        conflict_detail=(
            "Worker ownership or authority changed before login status read"
        ),
    )
    await db.commit()
    state_key = f"{worker_id}:{provider}:{email.casefold()}"
    cached = _worker_login_state.get(state_key)
    if provider != "codex":
        return cached or {"status": "idle", "provider": provider}

    persisted = next(
        (
            account
            for account in (worker.accounts or [])
            if isinstance(account, dict)
            and str(account.get("provider") or "claude").lower() == "codex"
            and str(account.get("email") or "").strip().casefold()
            == email.casefold()
        ),
        None,
    )
    if cached is not None:
        if persisted is not None and persisted.get("status") == "pending":
            await _settle_recovered_worker_codex_login(
                worker,
                persisted,
                cached,
            )
        return cached
    if persisted is None:
        return {"status": "idle", "provider": provider}
    persisted_status = str(persisted.get("status") or "")
    if persisted_status == "logged_in":
        return {
            "status": "success",
            "provider": provider,
            **(
                {"account_id": persisted["account_id"]}
                if persisted.get("account_id") else {}
            ),
        }
    if persisted_status == "failed":
        return {
            "status": "failed",
            "provider": provider,
            **(
                {"account_id": persisted["account_id"]}
                if persisted.get("account_id") else {}
            ),
        }
    if persisted_status != "pending":
        raise HTTPException(
            409,
            "Manager 中的 Worker Codex 登录状态无法恢复",
        )
    remote = await _provisioner().worker_local_api(
        worker,
        "GET",
        f"/api/codex-pool/add/{quote(email, safe='')}",
        timeout=30,
    )
    if not isinstance(remote, dict):
        raise HTTPException(502, "Worker Codex 登录状态格式无效")
    await _settle_recovered_worker_codex_login(worker, persisted, remote)
    safe = {
        key: remote[key]
        for key in (
            "status",
            "detail",
            "attempt_id",
            "challenge_id",
            "expires_at",
            "account_id",
        )
        if remote.get(key) is not None
    }
    _worker_login_state[state_key] = {**safe, "provider": "codex"}
    return _worker_login_state[state_key]


def _worker_login_attempt_state(worker_id: int, attempt_id: str) -> dict | None:
    prefix = f"{worker_id}:codex:"
    matches = [
        state for key, state in _worker_login_state.items()
        if key.startswith(prefix) and state.get("attempt_id") == attempt_id
    ]
    return matches[0] if len(matches) == 1 else None


async def _resolve_worker_login_attempt_state(
    worker: Worker,
    attempt_id: str,
) -> dict | None:
    """Recover Manager process-local OTP routing from the Worker source."""

    cached = _worker_login_attempt_state(worker.id, attempt_id)
    if cached is not None:
        return cached
    candidates = [
        account
        for account in (worker.accounts or [])
        if isinstance(account, dict)
        and str(account.get("provider") or "claude").lower() == "codex"
        and account.get("status") == "pending"
        and isinstance(account.get("email"), str)
        and account["email"].strip()
    ]
    matches: list[tuple[str, dict]] = []
    for account in candidates:
        email = account["email"].strip()
        remote = await _provisioner().worker_local_api(
            worker,
            "GET",
            f"/api/codex-pool/add/{quote(email, safe='')}",
            timeout=30,
        )
        if not isinstance(remote, dict) or remote.get("attempt_id") != attempt_id:
            continue
        await _settle_recovered_worker_codex_login(worker, account, remote)
        safe = {
            key: remote[key]
            for key in (
                "status",
                "detail",
                "attempt_id",
                "challenge_id",
                "expires_at",
                "account_id",
            )
            if remote.get(key) is not None
        }
        matches.append((email, safe))
    if len(matches) != 1:
        return None
    email, state = matches[0]
    state_key = f"{worker.id}:codex:{email.casefold()}"
    _worker_login_state[state_key] = {
        **state,
        "provider": "codex",
    }
    return _worker_login_state[state_key]


@router.post("/{worker_id}/pool/login-attempts/{attempt_id}/otp")
async def submit_worker_login_otp(
    worker_id: int,
    attempt_id: str,
    request: Request,
    body: dict,
    db: AsyncSession = Depends(get_db),
):
    """Relay a one-time code over the Worker's SSH loopback API channel."""
    worker = await db.get(Worker, worker_id)
    if not worker:
        raise HTTPException(404, "Worker not found")
    from backend.api.deps import require_worker_access as _rwa
    await _rwa(request, worker)
    if (
        worker.status != "ready"
        or worker.bootstrap_step is not None
        or not worker.private_ip
    ):
        raise HTTPException(
            409,
            "Worker 未就绪"
            f"（{worker.status}/{worker.bootstrap_step or 'normal'}）",
        )
    state = await _resolve_worker_login_attempt_state(worker, attempt_id)
    if not state:
        raise HTTPException(404, "Worker 登录流程已结束或不存在")
    challenge_id = body.get("challenge_id")
    code = body.get("code")
    if not isinstance(challenge_id, str) or challenge_id != state.get("challenge_id"):
        raise HTTPException(409, "验证码挑战已更新")
    if not isinstance(code, str) or not code.strip().isdigit() or len(code.strip()) != 6:
        raise HTTPException(422, "请输入 6 位数字验证码")
    worker = await _lock_worker_effect_access(
        request,
        db,
        worker_id,
        predicates=(
            Worker.status == "ready",
            Worker.bootstrap_step.is_(None),
            Worker.private_ip.is_not(None),
        ),
        conflict_detail=(
            "Worker ownership, authority, or lifecycle changed before OTP "
            "submission"
        ),
    )
    # Linearize authorization immediately before the remote one-time effect;
    # never retain a database lock across the Worker's login API request.
    await db.commit()
    response = await _provisioner().worker_local_api(
        worker,
        "POST",
        f"/api/codex-pool/login-attempts/{quote(attempt_id, safe='')}/otp",
        payload={"challenge_id": challenge_id, "code": code.strip()},
        timeout=30,
    )
    state.update({"status": "verifying_otp"})
    return response


@router.delete("/{worker_id}/pool/login-attempts/{attempt_id}")
async def cancel_worker_login(
    worker_id: int,
    attempt_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    worker = await db.get(Worker, worker_id)
    if not worker:
        raise HTTPException(404, "Worker not found")
    from backend.api.deps import require_worker_access as _rwa
    await _rwa(request, worker)
    state = await _resolve_worker_login_attempt_state(worker, attempt_id)
    if not state:
        raise HTTPException(404, "Worker 登录流程已结束或不存在")
    worker = await _lock_worker_effect_access(
        request,
        db,
        worker_id,
        predicates=(
            Worker.status.in_(("ready", "destroying", "error")),
            Worker.private_ip.is_not(None),
        ),
        conflict_detail=(
            "Worker ownership, authority, or lifecycle changed before login "
            "cancellation"
        ),
    )
    await db.commit()
    response = await _provisioner().worker_local_api(
        worker,
        "DELETE",
        f"/api/codex-pool/login-attempts/{quote(attempt_id, safe='')}",
        timeout=45,
    )
    # The background poller may have replaced the state dict while the remote
    # cancellation request was in flight.  Re-resolve it before mutating so we
    # never update an orphaned object or overwrite a completed terminal state.
    current_state = _worker_login_attempt_state(worker_id, attempt_id)
    if (
        current_state is not None
        and current_state.get("status") in _WORKER_ACTIVE_LOGIN_STATUSES
    ):
        # The background poller still has to observe cancellation and persist
        # its retryable failure record.  Keep deletion blocked until then.
        current_state.update({"status": "cancelling", "detail": "正在取消登录"})
    return {
        "ok": bool(response.get("ok", True)) if isinstance(response, dict) else True,
        "status": (
            current_state.get("status", "cancelling")
            if current_state is not None else "cancelling"
        ),
    }


@router.delete("/{worker_id}/pool/{account_id}")
async def delete_worker_account(
    worker_id: int,
    request: Request,
    account_id: str,
    provider: str = "codex",
    db: AsyncSession = Depends(get_db),
):
    """从 worker 的号池中删除账号。"""
    worker = await db.get(Worker, worker_id)
    if not worker:
        raise HTTPException(404, "Worker not found")
    from backend.api.deps import require_worker_access as _rwa
    await _rwa(request, worker)
    if (
        worker.status not in _WORKER_ACCOUNT_DELETE_REPLAY_STATUSES
        or worker.bootstrap_step is not None
        or not worker.private_ip
    ):
        raise HTTPException(
            409,
            "Worker 未就绪"
            f"（{worker.status}/{worker.bootstrap_step or 'normal'}）",
        )
    provider = _normalize_worker_account_provider(provider)

    # Commit one complete non-secret deletion outbox before the remote effect.
    # Transport/ACK loss leaves the exact operation available for an
    # idempotent replay; credentials disappear in the same actor/owner-fenced
    # commit and can never be revived by bootstrap or a late login callback.
    async with _worker_login_admission_lock:
        prefix = f"{worker_id}:{provider}:"
        if any(
            key.startswith(prefix)
            and state.get("status") in _WORKER_ACTIVE_LOGIN_STATUSES
            for key, state in _worker_login_state.items()
        ):
            raise HTTPException(
                409,
                "Worker 账号登录仍在进行中，请先取消并等待登录结束后再删除",
            )
        async with _worker_account_store_lock:
            async with _worker_lifecycle_transaction_lock(worker_id):
                try:
                    worker = await _lock_worker_effect_access(
                        request,
                        db,
                        worker_id,
                        predicates=(
                            Worker.status.in_(
                                tuple(_WORKER_ACCOUNT_DELETE_REPLAY_STATUSES)
                            ),
                            Worker.bootstrap_step.is_(None),
                            Worker.private_ip.is_not(None),
                        ),
                        conflict_detail=(
                            "Worker ownership, authority, or lifecycle changed "
                            "before account deletion"
                        ),
                    )
                    prepared_accounts, receipt = (
                        _prepare_persisted_worker_account_delete(
                            worker.accounts,
                            worker,
                            request,
                            provider=provider,
                            account_id=account_id,
                        )
                    )
                    worker.accounts = prepared_accounts
                    await db.commit()
                except Exception:
                    await db.rollback()
                    raise

        # Admission stays closed through the remote request and exact
        # settlement.  A same-provider add cannot reuse the slot while the
        # DELETE result is unknown.  On a later HTTP retry the durable receipt
        # yields this same path and operation instead of creating a new one.
        r = await _worker_http_request(
            worker,
            "DELETE",
            receipt["remote_path"],
            timeout=10,
            allow_statuses=frozenset({404}),
        )
        async with _worker_account_store_lock:
            async with _worker_lifecycle_transaction_lock(worker_id):
                try:
                    await _settle_worker_account_delete(
                        db,
                        worker_id,
                        receipt,
                    )
                except Exception:
                    await db.rollback()
                    raise
        if r.status_code == 404:
            return {"ok": True, "already_absent": True}
        return _worker_response_json(r)


@router.get("/{worker_id}/pool/usage")
async def get_worker_pool_usage(
    worker_id: int,
    request: Request,
    provider: str = "codex",
    db: AsyncSession = Depends(get_db),
):
    """拉取 Worker 指定 provider 的账号额度。"""
    worker = await db.get(Worker, worker_id)
    if not worker:
        raise HTTPException(404, "Worker not found")
    from backend.api.deps import require_worker_access as _rwa
    await _rwa(request, worker)
    if worker.status != "ready" or not worker.private_ip:
        raise HTTPException(409, f"Worker 未就绪（{worker.status}）")
    provider = _normalize_worker_account_provider(provider)
    worker = await _lock_worker_effect_access(
        request,
        db,
        worker_id,
        predicates=(
            Worker.status == "ready",
            Worker.bootstrap_step.is_(None),
            Worker.private_ip.is_not(None),
        ),
        conflict_detail=(
            "Worker ownership, authority, or lifecycle changed before pool "
            "usage read"
        ),
    )
    await db.commit()
    usage_path = (
        "/api/codex-pool/usage?force=true"
        if provider == "codex"
        else "/api/pool/usage"
    )
    status_path = (
        "/api/codex-pool/status"
        if provider == "codex"
        else "/api/pool/status"
    )
    timeout = 60 if provider == "codex" else 15
    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await _worker_http_request(
            worker,
            "GET",
            usage_path,
            timeout=timeout,
            allow_statuses=frozenset({404}),
            client=client,
        )
        if r.status_code != 404:
            return _worker_response_json(r)

        # Compatibility only: an old Worker can expose pool status but have
        # no usage endpoint, while a disabled legacy pool returns 404 for
        # both.  Auth, quota and 5xx failures never enter this fallback.
        r2 = await _worker_http_request(
            worker,
            "GET",
            status_path,
            timeout=timeout,
            allow_statuses=frozenset({404}),
            client=client,
        )
        if r2.status_code == 404:
            return {"enabled": False, "total": 0, "available": 0, "accounts": []}
        return _worker_response_json(r2)


@router.get("/{worker_id}/settings/runtime")
async def get_worker_runtime_settings(worker_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    worker = await db.get(Worker, worker_id)
    if not worker:
        raise HTTPException(404, "Worker not found")
    from backend.api.deps import require_worker_access as _rwa
    await _rwa(request, worker)
    if worker.status != "ready" or not worker.private_ip:
        raise HTTPException(409, f"Worker 未就绪（{worker.status}）")
    worker = await _lock_worker_effect_access(
        request,
        db,
        worker_id,
        predicates=(
            Worker.status == "ready",
            Worker.bootstrap_step.is_(None),
            Worker.private_ip.is_not(None),
        ),
        conflict_detail=(
            "Worker ownership, authority, or lifecycle changed before runtime "
            "settings read"
        ),
    )
    await db.commit()
    r = await _worker_http_request(
        worker, "GET", "/api/settings/runtime", timeout=10,
    )
    return _worker_response_json(r)


@router.put("/{worker_id}/settings/runtime")
async def update_worker_runtime_settings(
    worker_id: int,
    request: Request,
    body: RuntimeSettingsUpdate,
    db: AsyncSession = Depends(get_db),
):
    worker = await db.get(Worker, worker_id)
    if not worker:
        raise HTTPException(404, "Worker not found")
    from backend.api.deps import require_worker_access as _rwa
    await _rwa(request, worker)
    payload = body.model_dump(exclude_unset=True)
    # Worker owners may keep using the historical low-risk runtime controls,
    # but must not inherit future Manager-wide switches merely because the
    # shared response/update schema grows.  Anything outside this explicit
    # owner allowlist requires an administrator before the deployment bearer
    # token forwards it to the Worker.
    owner_fields = {
        "use_pty_mode",
        "auto_sort_on_access",
        "context_compact_threshold",
    }
    if set(payload) - owner_fields:
        from backend.api.deps import require_admin
        require_admin(request)
    if (
        worker.status != "ready"
        or worker.bootstrap_step is not None
        or not worker.private_ip
    ):
        raise HTTPException(
            409,
            "Worker 未就绪"
            f"（{worker.status}/{worker.bootstrap_step or 'normal'}）",
        )
    worker = await _lock_worker_effect_access(
        request,
        db,
        worker_id,
        predicates=(
            Worker.status == "ready",
            Worker.bootstrap_step.is_(None),
            Worker.private_ip.is_not(None),
        ),
        conflict_detail=(
            "Worker ownership, authority, or lifecycle changed before runtime "
            "settings update"
        ),
    )
    await db.commit()
    r = await _worker_http_request(
        worker,
        "PUT",
        "/api/settings/runtime",
        timeout=10,
        payload=payload,
    )
    return _worker_response_json(r)


# --- Team CCM: Worker rename ---

from pydantic import BaseModel as _BaseModel


class RenameWorkerBody(_BaseModel):
    name: str


@router.patch("/{worker_id}/rename", response_model=WorkerResponse)
async def rename_worker(worker_id: int, body: RenameWorkerBody, request: Request, db: AsyncSession = Depends(get_db)):
    """Commit a Worker name with a monotonic, replayable cloud-tag outbox."""
    from backend.api.deps import require_worker_access
    from backend.services.worker_provisioner import (
        build_worker_rename_tag_outbox,
    )

    worker = await db.get(Worker, worker_id)
    if not worker:
        raise HTTPException(404, "Worker not found")
    await require_worker_access(request, worker)
    new_name = body.name.strip()
    if not new_name:
        raise HTTPException(400, "Worker 名称不能为空")
    if len(new_name) > 200:
        raise HTTPException(422, "Worker 名称不能超过 200 个字符")

    if worker.rename_tag_outbox is None:
        # Fail stale ownership/role and lifecycle snapshots before provider
        # discovery.  This is only a no-op writer fence; the same predicates
        # are repeated when the durable outbox is actually installed.
        worker = await _lock_worker_effect_access(
            request,
            db,
            worker_id,
            predicates=(
                Worker.status.in_(tuple(_WORKER_RENAMEABLE_STATUSES)),
                Worker.bootstrap_step.is_(None),
                Worker.cloud_instance_id.is_not(None),
                Worker.rename_tag_outbox.is_(None),
            ),
            conflict_detail=(
                "Worker 权限、生命周期或重命名 generation 已变化"
            ),
        )
        db.expunge(worker)
        await db.rollback()

    prov = _provisioner()
    # A previous response may have been lost after AWS accepted create_tags.
    # Reconcile that exact generation before admitting a different value; the
    # provider API offers no conditional generation that could make two names
    # safe to race.
    if worker.rename_tag_outbox is not None:
        pending_name = (
            worker.rename_tag_outbox.get("desired_name")
            if isinstance(worker.rename_tag_outbox, dict)
            else None
        )
        try:
            await prov.reconcile_worker_rename_tag_outbox(worker_id)
        except Exception as exc:
            raise HTTPException(
                409,
                "Worker 上一次重命名仍在等待云标签确认，请稍后重试",
            ) from exc
        await db.rollback()
        worker = await db.get(Worker, worker_id, populate_existing=True)
        if worker is None:
            raise HTTPException(404, "Worker not found")
        if pending_name == new_name and worker.rename_tag_outbox is None:
            worker = await _lock_worker_effect_access(
                request,
                db,
                worker_id,
                predicates=(
                    Worker.status.in_(tuple(_WORKER_RENAMEABLE_STATUSES)),
                    Worker.bootstrap_step.is_(None),
                    Worker.rename_tag_outbox.is_(None),
                    Worker.name == new_name,
                ),
                conflict_detail=(
                    "Worker 权限或重命名状态在结算后发生变化"
                ),
            )
            await db.commit()
            return worker

    # Resolve provider account/region + ClientToken before preparing effect
    # authority.  This read may backfill a legacy provision journal, so end the
    # route's preliminary read transaction first.
    if worker in db:
        db.expunge(worker)
    await db.rollback()
    try:
        identity = await prov.require_worker_cloud_identity(worker)
    except Exception as exc:
        raise HTTPException(
            409,
            "Worker 云账号、区域或 ClientToken 身份无法确认，未执行重命名",
        ) from exc
    identity_worker = identity["worker"]

    worker = await _lock_worker_effect_access(
        request,
        db,
        worker_id,
        predicates=(
            Worker.status.in_(tuple(_WORKER_RENAMEABLE_STATUSES)),
            Worker.bootstrap_step.is_(None),
            Worker.cloud_instance_id == identity_worker.cloud_instance_id,
            Worker.rename_tag_outbox.is_(None),
        ),
        conflict_detail=(
            "Worker 权限、生命周期或重命名 generation 已变化"
        ),
    )
    if (
        worker.auth_token != identity_worker.auth_token
        or worker.provision_spec != identity_worker.provision_spec
    ):
        await db.rollback()
        raise HTTPException(
            409,
            "Worker 云身份在重命名 admission 前发生变化",
        )
    generation = worker.rename_generation + 1
    outbox = build_worker_rename_tag_outbox(
        worker,
        desired_name=new_name,
        generation=generation,
        cloud_scope=identity["cloud_scope"],
        client_token_digest=identity["client_token_digest"],
    )
    worker.name = new_name
    worker.rename_generation = generation
    worker.rename_tag_outbox = outbox
    await db.commit()
    await db.refresh(worker)

    try:
        settled = await prov.reconcile_worker_rename_tag_outbox(
            worker_id,
            expected_operation_id=outbox["operation_id"],
        )
        if settled is not None:
            worker = settled
    except Exception:
        # The local name and exact remote intent are already durable.  Keep the
        # outbox for health/startup replay; a later rename cannot pass it.
        logger.warning(
            "Worker %s cloud Name tag generation %s remains pending",
            worker.id,
            generation,
            exc_info=True,
        )
    # Broadcast
    from backend.main import broadcaster
    if broadcaster:
        await broadcaster.broadcast("workers", {
            "event_type": "worker_update",
            "worker_id": worker.id,
            "status": worker.status,
        })
    return worker


# --- Team CCM: Worker assignment ---


class AssignWorkerBody(_BaseModel):
    owner_user_id: int | None = None


@router.put("/{worker_id}/assign", response_model=WorkerResponse)
async def assign_worker(worker_id: int, body: AssignWorkerBody, request: Request, db: AsyncSession = Depends(get_db)):
    """Assign a worker to a user (admin only). Set owner_user_id=null for public pool."""
    from backend.api.deps import require_admin
    from backend.models.user import User

    require_admin(request)
    worker = await db.get(Worker, worker_id)
    if not worker:
        raise HTTPException(404, "Worker not found")
    prev_owner = worker.owner_user_id
    # Assignment is metadata, not lifecycle authority.  Serialize it on the
    # Worker row and refuse every destroy/terminal state so it cannot cross an
    # in-flight destroy claim or mutate an already scrubbed historical row.
    async with _worker_lifecycle_transaction_lock(worker_id):
        await db.rollback()
        assigned = await db.execute(
            update(Worker)
            .where(
                Worker.id == worker_id,
                (
                    Worker.owner_user_id.is_(None)
                    if prev_owner is None
                    else Worker.owner_user_id == prev_owner
                ),
                Worker.status.not_in(("destroying", "terminated")),
                or_(
                    Worker.bootstrap_step.is_(None),
                    Worker.bootstrap_step != "destroy",
                ),
            )
            .values(owner_user_id=body.owner_user_id)
        )
        if assigned.rowcount != 1:
            await db.rollback()
            raise HTTPException(
                409,
                "Worker 正在销毁、等待销毁恢复或已终止，不能变更分配",
            )
        assignment_worker = await db.get(
            Worker,
            worker_id,
            populate_existing=True,
        )
        if (
            assignment_worker is None
            or _has_worker_account_delete_outbox(assignment_worker.accounts)
        ):
            await db.rollback()
            if assignment_worker is None:
                raise HTTPException(404, "Worker not found")
            raise HTTPException(
                409,
                "Worker 有未结算的账号删除操作，不能变更 owner",
            )
        # Validate the exact active recipient and the authenticated JWT actor
        # in one deterministic User-row order. A missing/disabled recipient or
        # actor demotion rolls the Worker CAS back before assignment publishes.
        try:
            await _lock_worker_assignment_users(request, db, body.owner_user_id)
        except Exception:
            await db.rollback()
            raise
        await db.commit()
    worker = await db.get(Worker, worker_id)
    if worker is None:
        raise HTTPException(404, "Worker not found")
    await db.refresh(worker)
    from backend.api.deps import get_current_user_id
    admin_id = get_current_user_id(request)
    # Notify new owner
    if body.owner_user_id:
        try:
            from backend.services.feishu_notify import notify_worker_assigned
            admin = await db.get(User, admin_id) if admin_id else None
            import asyncio
            asyncio.create_task(notify_worker_assigned(
                admin.name if admin else "Admin",
                worker.name,
                body.owner_user_id,
            ))
        except Exception:
            pass
    # Notify previous owner (if changed and not self-revoke)
    if prev_owner and prev_owner != body.owner_user_id and prev_owner != admin_id:
        try:
            from backend.services.feishu_notify import notify_worker_unassigned
            admin = await db.get(User, admin_id) if admin_id else None
            import asyncio
            asyncio.create_task(notify_worker_unassigned(
                admin.name if admin else "Admin",
                worker.name,
                prev_owner,
            ))
        except Exception:
            pass
    return worker
