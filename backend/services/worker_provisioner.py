"""Worker 生命周期编排（elastic-worker 设计 §3/§14/§16.4）。

- 创建：开新 EC2（配置自举继承 Manager）或收养已有实例 → bootstrap → ready
- 部署走 rsync（Manager 本地仓库 → Worker），天然实现"版本锁定到 Manager
  当前 commit"，且 Worker 无需任何 GitHub 凭证
- 关机/开机：EC2 stop/start，数据零迁移（private IP 在 VPC 内保持不变）
- 销毁：terminate（任务迁移由上层 TaskMigrator 先行完成；收养实例只 stop）
- 状态变化广播到 "workers" channel
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import os
import secrets as pysecrets
import shlex
from datetime import datetime
from urllib.parse import quote

import httpx
from sqlalchemy import select, update

from backend.config import settings
from backend.models.worker import Worker
from backend.services.cloud_provider import CloudProvider
from backend.services.git_info import REPO_ROOT, git_head_commit
from backend.services.ssh_executor import (
    SSHExecutor,
    SSHKeyMaterial,
    SSHKeyPreflightError,
    preflight_private_key,
    worker_known_hosts_path,
)

logger = logging.getLogger(__name__)

# rsync 部署时排除。.gitignore 经 --filter 自动生效（.venv/node_modules/db 等），
# 这里只列 .gitignore 之外必须排除的。.git 不带：worktree 的 .git 是指向
# Manager 本地路径的指针文件（rsync 过去即悬空），且 .git 目录体积大——
# 版本锁定改走 .deploy_commit 文件（git_info.git_head_commit 的回退路径）
DEPLOY_EXCLUDES = [
    ".git", ".env", ".env.*", "uploads/", ".claude-manager/", "archive-do-not-use/",
]

CLAUDE_LOGIN_METHODS = frozenset({"", "171mail", "mailcom", "onet", "gazeta"})
CODEX_LOGIN_METHODS = frozenset(
    {"", "171mail", "mailcatcher", "mailcom", "onet", "gazeta"}
)
CODEX_ACTIVE_LOGIN_STATUSES = frozenset(
    {"running", "awaiting_otp", "verifying_otp", "finalizing"}
)
CODEX_TERMINAL_FAILURE_STATUSES = frozenset(
    {"failed", "expired", "cancelled", "recovery_failed"}
)
# Keep Worker app-server/serde behavior identical to the Manager revision.
# Do not use npm "latest": a retry must not silently upgrade the protocol.
WORKER_CODEX_CLI_VERSION = "0.144.6"
_DESTROYED_ACCOUNT_AUDIT_FIELDS = (
    "email", "provider", "status", "account_id",
)

# The helper source is intentionally constant: URL, bearer token and optional
# JSON payload are all read from stdin, so neither Worker credentials nor
# account credentials are visible in the remote process list.
_WORKER_LOCAL_API_HELPER = r"""
import json
import sys
import urllib.error
import urllib.request

envelope = json.load(sys.stdin)
body = None
headers = {
    "Accept": "application/json",
    "Authorization": "Bearer " + envelope["auth_token"],
}
if envelope["has_payload"]:
    body = json.dumps(
        envelope["payload"], ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    headers["Content-Type"] = "application/json"
request = urllib.request.Request(
    envelope["url"],
    data=body,
    headers=headers,
    method=envelope["method"],
)
# This endpoint is deliberately loopback-only.  Never inherit HTTP(S)_PROXY:
# an enterprise proxy must not receive the Worker bearer or login payload.
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
try:
    with opener.open(request, timeout=envelope["timeout"]) as response:
        sys.stdout.buffer.write(response.read())
except urllib.error.HTTPError as exc:
    sys.stdout.buffer.write(exc.read())
    raise SystemExit(22)
""".strip()


def _scrub_destroyed_worker_accounts(accounts: list | None) -> list[dict]:
    """Retain non-secret audit metadata and fail closed on every other key."""
    return [
        {
            key: account[key]
            for key in _DESTROYED_ACCOUNT_AUDIT_FIELDS
            if key in account
        }
        for account in accounts or []
        if isinstance(account, dict)
    ]


def _build_account_login_script(
    remote_dir: str,
    *,
    email: str,
    token: str,
    slot: str,
    login_method: str,
) -> str:
    """Build the login script without interpolating unquoted account data."""
    config_name = ".claude" if slot == "default" else f".claude-{slot}"
    argv = [
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
    return "\n".join([
        "#!/bin/bash",
        "set +e",
        'export PATH="$HOME/.local/bin:$PATH"',
        f"cd {shlex.quote(remote_dir)}",
        "pkill -f 'Xvfb :99' 2>/dev/null",
        "sleep 0.5",
        "Xvfb :99 -screen 0 1920x1080x24 -nolisten tcp -ac > /dev/null 2>&1 &",
        "sleep 1",
        "export DISPLAY=:99",
        f'CONFIG_DIR="$HOME/"{shlex.quote(config_name)}',
        f'{shlex.join(argv)} --config-dir "$CONFIG_DIR"',
        "",
    ])


def _build_script_upload_command(script: str, remote_path: str) -> str:
    """Transfer a script as base64 so its contents cannot terminate a heredoc."""
    encoded = base64.b64encode(script.encode()).decode("ascii")
    quoted_path = shlex.quote(remote_path)
    return (
        f"umask 077 && printf %s {shlex.quote(encoded)} | "
        f"base64 -d > {quoted_path} && chmod 700 {quoted_path}"
    )


class BootstrapError(Exception):
    def __init__(self, step: str, detail: str):
        super().__init__(f"[{step}] {detail}")
        self.step = step
        self.detail = detail


class WorkerProvisioner:
    def __init__(self, db_factory, cloud: CloudProvider, broadcaster=None, relay=None):
        self.db_factory = db_factory
        self.cloud = cloud
        self.broadcaster = broadcaster
        self.relay = relay  # WorkerRelay（可选；关机/销毁前断流，恢复后重建）
        # "."（默认）解析为本仓库根（从 __file__ 推导），不依赖进程 cwd——
        # cwd 配错时 rsync --delete 整个 $HOME 到 worker 是灾难
        src = settings.worker_deploy_source_dir
        self._repo_dir = REPO_ROOT if src in (".", "") else os.path.abspath(src)
        # HTTP routes use DB compare-and-set transitions; this second layer
        # serializes direct/background lifecycle calls for the same Worker.
        self._lifecycle_locks: dict[int, asyncio.Lock] = {}

    def _lifecycle_lock(self, worker_id: int) -> asyncio.Lock:
        return self._lifecycle_locks.setdefault(worker_id, asyncio.Lock())

    @staticmethod
    def _build_ec2_overrides() -> dict:
        """Build EC2 overrides from fixed config (non-empty values only)."""
        o: dict = {}
        if settings.worker_instance_type:
            o["instance_type"] = settings.worker_instance_type
        if settings.worker_image_id:
            o["image_id"] = settings.worker_image_id
        if settings.worker_subnet_id:
            o["subnet_id"] = settings.worker_subnet_id
        if settings.worker_security_group_ids:
            o["security_group_ids"] = [s.strip() for s in settings.worker_security_group_ids.split(",") if s.strip()]
        if settings.worker_key_name:
            o["key_name"] = settings.worker_key_name
        return o

    @staticmethod
    def preflight_ssh_key(key_path: str | None = None) -> SSHKeyMaterial:
        """Validate the exact unattended key before any paid cloud mutation."""
        return preflight_private_key(key_path or settings.worker_ssh_key_path)

    # ------------------------------------------------------------------
    # 工具
    # ------------------------------------------------------------------

    async def _update(
        self, worker_id: int, log_line: str | None = None,
        broadcast: bool = True, **fields,
    ) -> Worker | None:
        """更新字段 +（可选）追加日志行，一次 DB 往返。worker 已删除返回 None。"""
        async with self.db_factory() as db:
            worker = await db.get(Worker, worker_id)
            if worker is None:
                return None
            if log_line is not None:
                stamp = datetime.utcnow().strftime("%H:%M:%S")
                worker.bootstrap_log = (worker.bootstrap_log or "") + f"[{stamp}] {log_line}\n"
            for k, v in fields.items():
                setattr(worker, k, v)
            await db.commit()
            await db.refresh(worker)
        if broadcast:
            await self._broadcast(worker, log_line)
        return worker

    async def _broadcast(self, worker: Worker, log_line: str | None = None):
        if self.broadcaster:
            await self.broadcaster.broadcast("workers", {
                "event_type": "worker_update",
                "worker_id": worker.id,
                "status": worker.status,
                "bootstrap_step": worker.bootstrap_step,
                "bootstrap_error": worker.bootstrap_error,
                "private_ip": worker.private_ip,
                # 带增量日志行，前端无需回头拉全量日志
                "log_line": log_line,
            })

    async def _log(self, worker_id: int, line: str, **fields):
        logger.info("worker %s: %s", worker_id, line.strip())
        return await self._update(worker_id, log_line=line, **fields)

    def _ssh(self, worker: Worker) -> SSHExecutor:
        return SSHExecutor(
            host=worker.private_ip,
            user=worker.ssh_user,
            key_path=worker.ssh_key_path or settings.worker_ssh_key_path,
            known_hosts_path=(
                worker_known_hosts_path(worker.cloud_instance_id)
                if worker.cloud_instance_id else None
            ),
        )

    async def _probe_health(self, worker: Worker, client: httpx.AsyncClient) -> dict:
        """探活 worker CCM。返回 health JSON；非 200/连接失败抛异常。"""
        r = await client.get(
            f"http://{worker.private_ip}:{worker.ccm_port}/api/system/health",
            timeout=10,
        )
        r.raise_for_status()
        return r.json()

    async def _probe_auth(self, worker: Worker, client: httpx.AsyncClient) -> None:
        """验证 auth_token 真的可用。/api/system/health 在 PUBLIC_PATHS 不校验
        token，必须打一个需认证的端点，否则 .env 没写对也会被标 ready。"""
        r = await client.get(
            f"http://{worker.private_ip}:{worker.ccm_port}/api/system/stats",
            headers={"Authorization": f"Bearer {worker.auth_token}"},
            timeout=10,
        )
        if r.status_code == 401:
            raise BootstrapError("health-check", "auth_token 校验失败（worker .env 未生效？）")
        r.raise_for_status()

    async def worker_local_api(
        self,
        worker: Worker,
        method: str,
        path: str,
        *,
        payload: dict | None = None,
        timeout: int = 30,
    ) -> dict:
        """Call a Worker API through its own SSH loopback interface.

        Credential-bearing payloads are written to the SSH channel's stdin;
        they never appear in argv, process listings, VPC plaintext traffic, or
        debug logs.  This is intentionally used for Codex login rather than
        duplicating the transaction/rollback logic from ``api/codex_pool.py``.
        """
        if not path.startswith("/api/") or any(c in path for c in "\r\n"):
            raise ValueError("invalid Worker API path")
        method = method.upper()
        if method not in {"GET", "POST", "DELETE"}:
            raise ValueError(f"unsupported Worker API method: {method}")
        if not worker.private_ip:
            raise RuntimeError("Worker has no private IP")
        if not worker.auth_token:
            raise RuntimeError("Worker has no auth token")

        # Keep the command fixed and send the complete request envelope via
        # SSH stdin.  Suppressing debug output alone is insufficient: argv is
        # visible to other local processes on the Worker.
        command = shlex.join(["python3", "-c", _WORKER_LOCAL_API_HELPER])
        input_data = json.dumps({
            "url": f"http://127.0.0.1:{worker.ccm_port}{path}",
            "method": method,
            "timeout": timeout,
            "auth_token": worker.auth_token,
            "has_payload": payload is not None,
            "payload": payload,
        }, ensure_ascii=False)
        ssh = self._ssh(worker)
        code, out = await ssh.run_with_input(
            command,
            input_data,
            timeout=timeout + 5,
            sensitive=True,
        )
        if code != 0:
            detail = out.strip()[-1200:] or f"curl exit {code}"
            raise RuntimeError(f"Worker API {method} {path} failed: {detail}")
        try:
            body = json.loads(out)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                f"Worker API {method} {path} returned invalid JSON"
            ) from exc
        if not isinstance(body, dict):
            raise RuntimeError(f"Worker API {method} {path} returned a non-object")
        return body

    async def login_codex_account(
        self,
        worker: Worker,
        account: dict,
        *,
        timeout: int = 900,
        allow_manual_otp: bool = False,
        on_status=None,
    ) -> str | None:
        """Start and await one Worker-local Codex pool login transaction."""
        email = str(account.get("email") or "").strip()
        if not email:
            raise ValueError("Codex account email is required")
        response = await self.worker_local_api(
            worker,
            "POST",
            "/api/codex-pool/add",
            payload={
                "email": email,
                "token": str(account.get("token") or "").strip(),
                # Passwords are opaque and must retain leading/trailing bytes.
                "password": str(account.get("password") or ""),
                "login_method": str(account.get("login_method") or ""),
            },
            timeout=45,
        )
        account_id = response.get("account_id")
        if account_id:
            account_id = str(account_id)
            # Keep the allocation even if a following SSH status request is
            # interrupted. The failed bootstrap record can then reclaim this
            # exact slot instead of allocating codex-N+1 on retry.
            account["account_id"] = account_id
        final_response = await self._await_codex_login(
            worker,
            response=response,
            status_path=f"/api/codex-pool/add/{quote(email, safe='')}",
            timeout=timeout,
            allow_manual_otp=allow_manual_otp,
            on_status=on_status,
        )
        account_id = account_id or final_response.get("account_id")
        if not account_id:
            raise RuntimeError(
                "Codex login completed without account_id; refusing a non-idempotent retry"
            )
        account["account_id"] = str(account_id)
        return str(account_id)

    async def _cancel_codex_login(self, worker: Worker, response: dict) -> None:
        """Stop a Worker-local login so its lock/journal cannot strand retries."""
        attempt_id = str(response.get("attempt_id") or "").strip()
        if not attempt_id:
            raise RuntimeError("login response omitted attempt_id; cannot cancel safely")
        await self.worker_local_api(
            worker,
            "DELETE",
            f"/api/codex-pool/login-attempts/{quote(attempt_id, safe='')}",
            timeout=45,
        )

    async def _await_codex_login(
        self,
        worker: Worker,
        *,
        response: dict,
        status_path: str,
        timeout: int,
        allow_manual_otp: bool = False,
        on_status=None,
    ) -> dict:
        """Poll one Worker-local Codex add/relogin transaction."""
        deadline = asyncio.get_running_loop().time() + timeout
        status = str(response.get("status") or "running")
        if on_status is not None:
            await on_status(dict(response))
        while status != "success":
            if status == "awaiting_otp" and not allow_manual_otp:
                try:
                    await self._cancel_codex_login(worker, response)
                except Exception as cancel_exc:
                    raise RuntimeError(
                        "OpenAI 要求人工输入邮箱验证码，且远端登录清理失败："
                        f"{cancel_exc}"
                    ) from cancel_exc
                raise RuntimeError(
                    "OpenAI 要求人工输入邮箱验证码；登录已安全取消。"
                    "Worker 自动 bootstrap 请提供可自动取码的邮箱 token"
                )
            if status in CODEX_TERMINAL_FAILURE_STATUSES:
                raise RuntimeError(
                    str(response.get("detail") or f"Codex login {status}")[-1200:]
                )
            if status not in CODEX_ACTIVE_LOGIN_STATUSES:
                raise RuntimeError(f"unexpected Codex login status: {status}")
            if asyncio.get_running_loop().time() >= deadline:
                try:
                    await self._cancel_codex_login(worker, response)
                except Exception as cancel_exc:
                    raise RuntimeError(
                        f"Codex login timed out after {timeout} seconds and cleanup failed: "
                        f"{cancel_exc}"
                    ) from cancel_exc
                raise RuntimeError(
                    f"Codex login timed out after {timeout} seconds and was cancelled"
                )
            await asyncio.sleep(2)
            response = await self.worker_local_api(
                worker,
                "GET",
                status_path,
                timeout=30,
            )
            status = str(response.get("status") or "idle")
            if on_status is not None:
                await on_status(dict(response))
        return response

    async def ensure_codex_account(
        self,
        worker: Worker,
        account: dict,
        *,
        timeout: int = 900,
        allow_manual_otp: bool = False,
        on_status=None,
    ) -> str | None:
        """Idempotently keep a persisted Codex slot logged in on bootstrap.

        Retrying an existing Worker must never call ``/add`` for a slot that
        already exists: the remote allocator intentionally does not de-dup by
        email and would create codex-2, codex-3, ... on every retry.
        """
        persisted_id = str(account.get("account_id") or "").strip()
        if not persisted_id:
            email = str(account.get("email") or "").strip()
            if not email:
                raise ValueError("Codex account email is required")

            # Reclaim a transaction whose first POST response was lost. Calling
            # /add again without this check can allocate another account home.
            add_path = f"/api/codex-pool/add/{quote(email, safe='')}"
            add_state = await self.worker_local_api(
                worker, "GET", add_path, timeout=30,
            )
            add_status = str(add_state.get("status") or "idle")
            if add_status in CODEX_ACTIVE_LOGIN_STATUSES | {"success"}:
                claimed_id = str(add_state.get("account_id") or "").strip()
                if not claimed_id:
                    raise RuntimeError(
                        f"Codex add for {email} is {add_status} without account_id; "
                        "refusing to allocate a duplicate slot"
                    )
                account["account_id"] = claimed_id
                if add_status != "success":
                    await self._await_codex_login(
                        worker,
                        response=add_state,
                        status_path=add_path,
                        timeout=timeout,
                        allow_manual_otp=allow_manual_otp,
                        on_status=on_status,
                    )
                    return claimed_id

                # A completed in-memory add record can outlive deletion of its
                # pool slot. Verify the claimed id/email still exists before
                # treating success as authoritative.
                claimed_pool = await self.worker_local_api(
                    worker, "GET", "/api/codex-pool/status", timeout=30,
                )
                claimed_remote = next(
                    (
                        item for item in claimed_pool.get("accounts", [])
                        if isinstance(item, dict) and item.get("id") == claimed_id
                    ),
                    None,
                )
                if (
                    claimed_remote is not None
                    and str(claimed_remote.get("email") or "").strip().casefold()
                    == email.casefold()
                ):
                    persisted_id = claimed_id
                else:
                    account.pop("account_id", None)

            # A service restart clears transient add state but not the pool.
            # Adopt only one exact email match; ambiguity must fail closed.
            pool_status = await self.worker_local_api(
                worker, "GET", "/api/codex-pool/status", timeout=30,
            )
            matches = [
                item for item in pool_status.get("accounts", [])
                if isinstance(item, dict)
                and str(item.get("email") or "").strip().casefold()
                == email.casefold()
            ]
            if len(matches) > 1:
                raise RuntimeError(
                    f"Multiple remote Codex slots match {email}; refusing an ambiguous retry"
                )
            if len(matches) == 1:
                claimed_id = str(matches[0].get("id") or "").strip()
                if not claimed_id:
                    raise RuntimeError(f"Remote Codex account for {email} has no id")
                account["account_id"] = claimed_id
                persisted_id = claimed_id
            else:
                return await self.login_codex_account(
                    worker,
                    account,
                    timeout=timeout,
                    allow_manual_otp=allow_manual_otp,
                    on_status=on_status,
                )

        status = await self.worker_local_api(
            worker, "GET", "/api/codex-pool/status", timeout=30,
        )
        remote_account = next(
            (
                item for item in status.get("accounts", [])
                if isinstance(item, dict) and item.get("id") == persisted_id
            ),
            None,
        )
        if remote_account is None:
            # The instance/pool was replaced or reset.  Its empty allocator can
            # safely recreate the missing lowest slot from Manager credentials.
            return await self.login_codex_account(
                worker,
                account,
                timeout=timeout,
                allow_manual_otp=allow_manual_otp,
                on_status=on_status,
            )

        expected_email = str(account.get("email") or "").strip()
        remote_email = str(remote_account.get("email") or "").strip()
        if (
            expected_email
            and remote_email
            and expected_email.casefold() != remote_email.casefold()
        ):
            raise RuntimeError(
                f"Codex slot {persisted_id} belongs to {remote_email}, not {expected_email}"
            )

        encoded_id = quote(persisted_id, safe="")
        verification = await self.worker_local_api(
            worker,
            "GET",
            f"/api/codex-pool/accounts/{encoded_id}/verify?live=true",
            timeout=30,
        )
        if verification.get("logged_in") is True:
            if on_status is not None:
                await on_status({
                    "status": "success",
                    "account_id": persisted_id,
                })
            return persisted_id
        if verification.get("logged_in") is None:
            raise RuntimeError(
                f"Cannot live-verify Codex slot {persisted_id}: "
                f"{verification.get('detail') or 'temporary verification failure'}"
            )

        response = await self.worker_local_api(
            worker,
            "POST",
            f"/api/codex-pool/accounts/{encoded_id}/relogin",
            timeout=45,
        )
        await self._await_codex_login(
            worker,
            response=response,
            status_path=f"/api/codex-pool/accounts/{encoded_id}/relogin",
            timeout=timeout,
            allow_manual_otp=allow_manual_otp,
            on_status=on_status,
        )
        return persisted_id

    # ------------------------------------------------------------------
    # 创建 / 收养
    # ------------------------------------------------------------------

    async def create_worker(
        self,
        worker_id: int,
        accounts: list[dict] | None = None,
    ):
        async with self._lifecycle_lock(worker_id):
            await self._create_worker_locked(worker_id, accounts)

    async def _create_worker_locked(
        self,
        worker_id: int,
        accounts: list[dict] | None = None,
    ):
        """完整创建流程（后台任务）。失败 → status=error + 记录步骤与原因。"""
        step = "provision"
        try:
            worker = await self._update(
                worker_id, status="creating", bootstrap_step=step, bootstrap_error=None
            )
            if worker is None:
                raise BootstrapError(step, "Worker record disappeared")

            try:
                key_material = self.preflight_ssh_key(worker.ssh_key_path)
            except SSHKeyPreflightError as exc:
                raise BootstrapError(
                    "provision-config",
                    f"SSH 密钥配置无效（{exc.code}）：{exc.detail}",
                ) from exc
            if worker.ssh_key_path != key_material.private_key_path:
                worker = await self._update(
                    worker_id, ssh_key_path=key_material.private_key_path,
                )
            if not worker.auth_token:
                worker = await self._update(
                    worker_id, auth_token=pysecrets.token_hex(24),
                )

            # retry 场景：DB 里已有实例 ID 且实例还在 → 跳过创建直接 bootstrap
            existing_iid = worker.cloud_instance_id if worker else None
            if existing_iid:
                try:
                    info = await self.cloud.describe_instance(existing_iid)
                    if info["state"] in ("pending", "running", "stopped"):
                        await self._log(worker_id, f"reusing existing instance {existing_iid} ({info['state']})")
                        if info["state"] == "stopped":
                            await self.cloud.start_instance(existing_iid)
                        iid = existing_iid
                    elif info["state"] in ("terminated", "shutting-down"):
                        existing_iid = None
                        # A replacement is a new EC2 idempotency scope and a
                        # new Worker service. Rotate the API credential before
                        # deriving its stable create token.
                        worker = await self._update(
                            worker_id,
                            cloud_instance_id=None,
                            private_ip=None,
                            public_ip=None,
                            auth_token=pysecrets.token_hex(24),
                            provision_spec=None,
                        )
                    else:
                        raise BootstrapError(
                            step,
                            f"已有实例 {existing_iid} 当前为 {info['state']}，"
                            "为避免创建重复计费实例，本次未新建",
                        )
                except BootstrapError:
                    raise
                except Exception as exc:
                    # A describe timeout/IAM error is not proof that the old
                    # instance disappeared.  Creating another one here leaves
                    # an orphaned, billable EC2 on transient AWS failures.
                    raise BootstrapError(
                        step,
                        f"无法确认已有实例 {existing_iid} 的状态，"
                        f"为避免重复创建已停止：{exc}",
                    ) from exc

            if not existing_iid:
                spec = worker.provision_spec
                if spec is None:
                    frozen_overrides = self._build_ec2_overrides()
                    has_fixed_overrides = bool(frozen_overrides)
                    frozen_overrides.update({
                        "ssh_public_key": key_material.openssh_public_key,
                        "ssh_user": worker.ssh_user,
                        "ccm_port": worker.ccm_port,
                    })
                    spec = {
                        "version": 1,
                        "name": worker.name,
                        "has_fixed_overrides": has_fixed_overrides,
                        "overrides": frozen_overrides,
                    }
                    # This commit is the create-request journal.  It must land
                    # before RunInstances so every retry after a lost response
                    # reuses byte-equivalent semantic parameters.
                    worker = await self._update(
                        worker_id, provision_spec=spec, broadcast=False,
                    )
                if (
                    not isinstance(spec, dict)
                    or spec.get("version") != 1
                    or not isinstance(spec.get("name"), str)
                    or not spec["name"].strip()
                    or not isinstance(spec.get("overrides"), dict)
                    or "client_token" in spec["overrides"]
                ):
                    raise BootstrapError(
                        "provision-config",
                        "Worker 保存的 EC2 创建请求日志无效；为避免重复实例已停止",
                    )
                overrides = dict(spec["overrides"])
                if overrides.get("ssh_public_key") != key_material.openssh_public_key:
                    raise BootstrapError(
                        "provision-config",
                        "WORKER_SSH_KEY_PATH 的公钥在 EC2 创建请求后发生变化；"
                        "请恢复原私钥后重试，以免无法认领已创建的实例",
                    )
                has_fixed_overrides = bool(spec.get("has_fixed_overrides"))
                # EC2 may create the instance even when the API response is
                # lost. Reusing both the token and the frozen provision_spec
                # returns that instance instead of creating a billable orphan.
                overrides["client_token"] = "ccm-" + hashlib.sha256(
                    f"{worker.id}:{worker.auth_token}".encode("utf-8")
                ).hexdigest()[:48]
                src = "fixed config" if has_fixed_overrides else "inherited from manager"
                await self._log(worker_id, f"creating EC2 instance ({src})")
                iid = await self.cloud.create_instance(spec["name"], overrides)
                worker = await self._update(worker_id, cloud_instance_id=iid)

            private_ip = await self.cloud.wait_until_running(iid)
            info = await self.cloud.describe_instance(iid)
            worker = await self._update(
                worker_id, private_ip=private_ip, public_ip=info.get("public_ip"),
            )
            await self._log(worker_id, f"instance running, private_ip={private_ip}")
            await self._bootstrap(worker_id, accounts or [])

            worker = await self._update(
                worker_id, status="ready", bootstrap_step=None,
                last_heartbeat=datetime.utcnow(),
            )
            await self._log(worker_id, "worker ready")
        except BootstrapError as e:
            await self._update(
                worker_id, status="error", bootstrap_step=e.step, bootstrap_error=e.detail
            )
            await self._log(worker_id, f"FAILED at {e.step}: {e.detail}")
        except Exception as e:
            await self._update(
                worker_id, status="error", bootstrap_step=step, bootstrap_error=str(e)
            )
            await self._log(worker_id, f"FAILED: {e}")

    # ------------------------------------------------------------------
    # Bootstrap pipeline
    # ------------------------------------------------------------------

    async def _bootstrap(self, worker_id: int, accounts: list[dict]):
        async with self.db_factory() as db:
            worker = await db.get(Worker, worker_id)

        ssh = self._ssh(worker)

        async def run_step(step: str, coro):
            await self._log(
                worker_id, f"step: {step}",
                status="bootstrapping", bootstrap_step=step,
            )
            try:
                await coro
            except BootstrapError:
                raise
            except Exception as e:
                raise BootstrapError(step, str(e))

        await run_step("ssh-wait", self._step_ssh_wait(ssh))
        await run_step("system-init", self._step_system_init(ssh, worker_id))
        await run_step("ccm-deploy", self._step_ccm_deploy(ssh, worker, worker_id))
        await run_step("ccm-config", self._step_ccm_config(ssh, worker_id))
        await run_step("docker-sandbox", self._step_docker_sandbox(ssh, worker_id))
        await run_step("ccm-service", self._step_ccm_service(ssh, worker))
        await run_step("health-check", self._step_health_check(worker_id))
        # Codex login reuses the Worker-local /api/codex-pool transaction
        # machinery, so the service must be healthy first.  The Worker remains
        # bootstrapping and cannot receive tasks until all steps finish.
        await run_step("account-login", self._step_account_login(ssh, worker_id, accounts))
        if any(str(a.get("provider") or "claude").lower() == "claude" for a in accounts):
            await run_step("claude-warmup", self._step_claude_warmup(ssh, worker_id))

    async def _step_ssh_wait(self, ssh: SSHExecutor, timeout: int = 180):
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        attempts = 0
        last_result = None
        while loop.time() < deadline:
            attempts += 1
            remaining = max(1, int(deadline - loop.time()))
            last_result = await ssh.probe(timeout=min(10, remaining))
            if last_result.ok:
                return
            if loop.time() < deadline:
                await asyncio.sleep(min(5, max(0, deadline - loop.time())))
        reason = "unknown"
        if last_result is not None:
            reason = (
                f"{last_result.error_code or 'unknown'}: "
                f"{last_result.detail or 'no detail'}"
            )
        raise BootstrapError(
            "ssh-wait",
            f"SSH 不可达: {ssh.host}（{attempts} 次探测，最后原因 {reason}）。"
            "新建实例请检查 cloud-init 日志；旧实例若密钥不匹配需销毁后重建。",
        )

    async def _step_system_init(self, ssh: SSHExecutor, worker_id: int):
        # 幂等：已装则跳过；node 走 nodesource，uv 走官方脚本
        script = r"""
set -e
export DEBIAN_FRONTEND=noninteractive
# 8GB swap (idempotent)
if [ ! -f /swapfile ]; then
  sudo fallocate -l 8G /swapfile
  sudo chmod 600 /swapfile
  sudo mkswap /swapfile
  sudo swapon /swapfile
  echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
fi
sudo apt-get update -qq
sudo apt-get install -y -qq git curl rsync python3-venv > /dev/null
if ! command -v node >/dev/null; then
  curl -fsSL https://deb.nodesource.com/setup_22.x | sudo -E bash - > /dev/null
  sudo apt-get install -y -qq nodejs > /dev/null
fi
command -v uv >/dev/null || curl -LsSf https://astral.sh/uv/install.sh | sh > /dev/null
sudo npm ls -g @anthropic-ai/claude-code --depth=0 >/dev/null 2>&1 || sudo npm install -g @anthropic-ai/claude-code@latest > /dev/null
# Bootstrap/retry must not silently retain an old CLI that lacks the app-server
# login and account/rateLimits protocol used by this deployed CCM revision.
CODEX_CLI_VERSION="__CCM_CODEX_CLI_VERSION__"
sudo npm install -g "@openai/codex@$CODEX_CLI_VERSION" > /dev/null
test "$(codex --version 2>/dev/null | head -1)" = "codex-cli $CODEX_CLI_VERSION"
# Chrome CDP 自动登录依赖（Chrome + xvfb + xauth + xdotool + websockets）
sudo apt-get install -y -qq xvfb xauth xdotool python3-pip > /dev/null
# 与 scripts/setup.sh 保持同一已验证版本；Chrome 150+ 在 Xvfb 下曾
# renderer crash，不能让 Worker 随 latest 漂移后把自动登录全部打挂。
CHROME_VERSION="149.0.7827.53-1"
CHROME_INSTALLED="$(google-chrome --version 2>/dev/null | grep -oE '[0-9]+(\.[0-9]+){3}' || true)"
if [ "$CHROME_INSTALLED" != "149.0.7827.53" ]; then
  curl -fsSL "https://dl.google.com/linux/chrome/deb/pool/main/g/google-chrome-stable/google-chrome-stable_${CHROME_VERSION}_amd64.deb" -o /tmp/chrome.deb
  (sudo dpkg -i /tmp/chrome.deb > /dev/null 2>&1 || true)
  sudo apt-get -f install -y -qq > /dev/null 2>&1 || true
  rm -f /tmp/chrome.deb
  sudo apt-mark hold google-chrome-stable > /dev/null 2>&1 || true
fi
pip3 install --break-system-packages websockets > /dev/null 2>&1 || true
# Docker for shared project container isolation
if ! command -v docker >/dev/null; then
  sudo apt-get install -y -qq docker.io > /dev/null 2>&1 || true
  sudo usermod -aG docker ubuntu 2>/dev/null || true
  sudo systemctl enable docker > /dev/null 2>&1 || true
  sudo systemctl start docker > /dev/null 2>&1 || true
fi
echo "node=$(node --version) uv=$($HOME/.local/bin/uv --version 2>/dev/null || uv --version) claude=$(claude --version 2>/dev/null | head -1) codex=$(codex --version 2>/dev/null | head -1) chrome=$(google-chrome --version 2>/dev/null | head -1) docker=$(docker --version 2>/dev/null | head -1)"
""".replace("__CCM_CODEX_CLI_VERSION__", WORKER_CODEX_CLI_VERSION)
        code, out = await ssh.run(script, timeout=900)
        if code != 0:
            raise BootstrapError("system-init", out[-2000:])
        await self._log(worker_id, out.strip().splitlines()[-1] if out.strip() else "system-init done")

    async def _step_ccm_deploy(self, ssh: SSHExecutor, worker: Worker, worker_id: int):
        remote_dir = settings.worker_remote_dir
        commit = git_head_commit(self._repo_dir)
        await self._log(worker_id, f"rsync repo @ {commit[:8]} -> {ssh.host}:{remote_dir}")
        await ssh.run(f"mkdir -p {remote_dir}")
        # 版本锁定：直接同步 Manager 工作区（含 .git），Worker 上即 Manager 同款 commit
        await ssh.rsync_to(
            self._repo_dir.rstrip("/") + "/", remote_dir, excludes=DEPLOY_EXCLUDES,
            timeout=1200,
        )
        script = f"""
set -e
cd {remote_dir}
# 版本锁定标记：rsync 不带 .git，worker 侧 git_head_commit 回退读此文件
echo {commit} > .deploy_commit
export PATH="$HOME/.local/bin:$PATH"
uv sync --quiet
cd frontend && npm install --silent > /dev/null && npm run build > /dev/null 2>&1
echo deploy-ok
"""
        code, out = await ssh.run(script, timeout=1800)
        if code != 0:
            raise BootstrapError("ccm-deploy", out[-2000:])
        await self._update(worker_id, ccm_commit=commit)

    async def _step_ccm_config(self, ssh: SSHExecutor, worker_id: int):
        async with self.db_factory() as db:
            worker = await db.get(Worker, worker_id)
        token = worker.auth_token or pysecrets.token_hex(24)
        await self._update(worker_id, auth_token=token)
        remote_dir = settings.worker_remote_dir
        env = "\n".join([
            f"AUTH_TOKEN={token}",
            f"PORT={worker.ccm_port}",
            "HOST=0.0.0.0",
            "AUTO_START_DISPATCHER=true",
            "WORKER_ENABLED=false",
            f"WORKSPACE_DIR={settings.workspace_dir}",  # 必须与 Manager 一致（session 路径对齐）
            "POOL_ENABLED=true",
            "CODEX_POOL_ENABLED=true",
            "DEFAULT_PROVIDER=codex",
            f"CODEX_MAIN_MCP_ENABLED={'true' if settings.codex_main_mcp_enabled else 'false'}",
            f"USE_PTY_MODE={'true' if settings.use_pty_mode else 'false'}",
        ])
        env_path = remote_dir.rstrip("/") + "/.env"
        temp_path = env_path + ".ccm-tmp"
        # Write atomically with owner-only permissions.  The complete .env,
        # including AUTH_TOKEN, travels only through SSH stdin and never argv.
        command = (
            f"umask 077 && cat > {shlex.quote(temp_path)} && "
            f"chmod 600 {shlex.quote(temp_path)} && "
            f"mv -f {shlex.quote(temp_path)} {shlex.quote(env_path)} && "
            f"chmod 600 {shlex.quote(env_path)}"
        )
        code, out = await ssh.run_with_input(
            command,
            env + "\n",
            sensitive=True,
        )
        if code != 0:
            raise BootstrapError("ccm-config", out[-1000:])

    async def _step_account_login(self, ssh: SSHExecutor, worker_id: int, accounts: list[dict]):
        if not accounts:
            await self._log(worker_id, "no accounts given, skipping login (worker 已有凭证或稍后手动登录)")
            return
        async with self.db_factory() as db:
            worker = await db.get(Worker, worker_id)
        if worker is None:
            raise BootstrapError("account-login", "Worker record disappeared")

        remote_dir = settings.worker_remote_dir
        normalized_accounts = []
        seen_identities: set[tuple[str, str]] = set()
        seen_slots: set[tuple[str, str]] = set()
        for account in accounts:
            email = str(account.get("email", "")).strip()
            raw_token = account.get("token") or ""
            raw_password = account.get("password") or ""
            raw_account_id = account.get("account_id") or ""
            # Missing provider means a historical Worker account, which was
            # always Claude.  New API records explicitly persist "codex".
            provider = str(account.get("provider") or "claude").strip().lower()
            login_method = str(account.get("login_method") or "").strip().lower()
            if not email:
                raise BootstrapError("account-login", "账号 email 必填")
            if provider not in {"claude", "codex"}:
                raise BootstrapError("account-login", f"账号 {email} 的 provider 无效: {provider}")
            identity = (provider, email.casefold())
            if identity in seen_identities:
                raise BootstrapError(
                    "account-login", f"重复的 Worker 账号: {email} ({provider})",
                )
            seen_identities.add(identity)
            if not isinstance(raw_token, str) or not isinstance(raw_password, str):
                raise BootstrapError("account-login", f"账号 {email} 的凭据格式无效")
            if not isinstance(raw_account_id, str):
                raise BootstrapError("account-login", f"账号 {email} 的 account_id 格式无效")
            token = raw_token.strip()
            password = raw_password
            if provider == "claude" and not token:
                raise BootstrapError("account-login", f"账号 {email} 缺少 token")
            if provider == "codex" and not token:
                raise BootstrapError(
                    "account-login", f"Codex 账号 {email} 的 Worker 自动登录缺少邮箱 token",
                )
            valid_methods = CODEX_LOGIN_METHODS if provider == "codex" else CLAUDE_LOGIN_METHODS
            if login_method not in valid_methods:
                raise BootstrapError("account-login", f"账号 {email} 的登录方式无效: {login_method}")
            normalized_accounts.append({
                "email": email,
                "token": token,
                "password": password,
                "provider": provider,
                "login_method": login_method,
            })
            if raw_account_id.strip():
                account_id = raw_account_id.strip()
                slot = (provider, account_id)
                if slot in seen_slots:
                    raise BootstrapError(
                        "account-login",
                        f"重复的 Worker 账号槽位: {account_id} ({provider})",
                    )
                seen_slots.add(slot)
                normalized_accounts[-1]["account_id"] = account_id

        results = []
        claude_index = 0
        for acct in normalized_accounts:
            email = acct["email"]
            token = acct["token"]
            password = acct["password"]
            provider = acct["provider"]
            login_method = acct["login_method"]
            account_id = None
            out = ""
            if provider == "codex":
                await self._log(
                    worker_id,
                    f"login Codex {email} through worker-local account service "
                    f"(method: {login_method or 'auto'})",
                )
                try:
                    account_id = await self.ensure_codex_account(worker, acct)
                    code = 0
                except Exception as exc:
                    account_id = str(acct.get("account_id") or "").strip() or None
                    code = 1
                    out = str(exc)
            else:
                claude_index += 1
                name = str(acct.get("account_id") or "").strip() or (
                    "default" if claude_index == 1 else f"account-{claude_index}"
                )
                account_id = name
                await self._log(
                    worker_id,
                    f"login Claude {email} -> pool slot {name} "
                    f"(method: {login_method or 'auto'})",
                )
                login_script = _build_account_login_script(
                    remote_dir,
                    email=email,
                    token=token,
                    slot=name,
                    login_method=login_method,
                )
                remote_script = f"/tmp/ccm_login_{worker_id}_{claude_index}.sh"
                upload_cmd = _build_script_upload_command(login_script, remote_script)
                code, out = await ssh.run(upload_cmd, sensitive=True)
                if code == 0:
                    quoted_script = shlex.quote(remote_script)
                    cmd = (
                        f"bash {quoted_script}; rc=$?; "
                        f"rm -f {quoted_script}; exit $rc"
                    )
                    code, out = await ssh.run(cmd, timeout=600)
                else:
                    await ssh.run(f"rm -f {shlex.quote(remote_script)}")

            status = "logged_in" if code == 0 else "failed"
            result = {**acct, "status": status}
            if account_id:
                result["account_id"] = account_id
            results.append(result)
            await self._log(worker_id, f"login {email}: {status}")
            if code != 0:
                await self._log(worker_id, f"login output: {out[-500:]}")
        await self._update(worker_id, accounts=results)
        if all(r["status"] == "failed" for r in results):
            raise BootstrapError("account-login", "全部账号登录失败")

    async def _step_docker_sandbox(self, ssh: SSHExecutor, worker_id: int):
        """Build ccm-sandbox Docker image on the worker (for shared project isolation)."""
        await self._log(worker_id, "building ccm-sandbox Docker image...")
        script = r"""
if command -v docker >/dev/null; then
  if ! docker images -q ccm-sandbox:latest 2>/dev/null | grep -q .; then
    mkdir -p /tmp/ccm-docker-build
    cat > /tmp/ccm-docker-build/Dockerfile << 'DEOF'
FROM node:22-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl git ssh-client ca-certificates python3 \
    && rm -rf /var/lib/apt/lists/*
RUN npm install -g @anthropic-ai/claude-code
RUN groupadd -g 1000 sandbox 2>/dev/null; useradd -m -u 1000 -g 1000 sandbox 2>/dev/null; exit 0
USER 1000
WORKDIR /workspace
DEOF
    docker build -t ccm-sandbox:latest /tmp/ccm-docker-build
    echo "ccm-sandbox built"
  else
    echo "ccm-sandbox already exists"
  fi
else
  echo "docker not available, skipping sandbox build"
fi
"""
        code, out = await ssh.run(script, timeout=600)
        await self._log(worker_id, f"docker-sandbox: {out.strip()[-200:]}")

    async def _step_claude_warmup(self, ssh: SSHExecutor, worker_id: int):
        """Interactive PTY warmup to complete all onboarding dialogs.

        Fresh Claude Code installs show theme picker, login method selector,
        etc. in interactive mode. A -p warmup skips these (non-interactive),
        so the first real PTY session still hits them and stalls.

        This runs a short PTY session that lets the drain loop auto-confirm
        all dialogs, then sends a test prompt. After this, .claude.json has
        the full onboarding state and subsequent PTY sessions start clean.
        """
        remote_dir = settings.worker_remote_dir
        script = f"""
set -e
cd {remote_dir}
# Phase 1: -p warmup for GrowthBook cache + credential verify
timeout 30 claude -p 'reply: ok' --dangerously-skip-permissions 2>/dev/null || true
# Phase 2: interactive PTY warmup to complete onboarding dialogs
.venv/bin/python3 -c '
import asyncio

async def warmup():
    from claude_pty.session import Session
    from claude_pty.config import PTYConfig
    from claude_pty.bridge import BridgeHub

    bridge = BridgeHub()
    bridge.start()
    try:
        cfg = PTYConfig(default_model="claude-opus-4-6", dangerously_skip_permissions=True)
        s = Session(cwd="{remote_dir}", config=cfg, bridge=bridge)
        await s.start()
        count = 0
        async for ev in s.send_prompt("reply: ok"):
            if ev.content:
                count += 1
                if count >= 2:
                    break
        await s.stop()
        print("pty-warmup-ok")
    except Exception as e:
        print(f"pty-warmup-failed: {{e}}")
    finally:
        bridge.stop()

asyncio.run(warmup())
' 2>&1 | tail -1
echo warmup-ok
"""
        code, out = await ssh.run(script, timeout=120)
        last_line = out.strip().splitlines()[-1] if out.strip() else ""
        await self._log(worker_id, f"claude warmup done ({last_line})")

    async def _step_ccm_service(self, ssh: SSHExecutor, worker: Worker):
        remote_dir = settings.worker_remote_dir
        unit = f"""
[Unit]
Description=Claude Code Manager (worker)
After=network.target

[Service]
Type=simple
User={worker.ssh_user}
WorkingDirectory={remote_dir}
EnvironmentFile={remote_dir}/.env
ExecStart={remote_dir}/.venv/bin/python -m uvicorn backend.main:app --host 0.0.0.0 --port {worker.ccm_port}
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
"""
        script = f"""
set -e
sudo tee /etc/systemd/system/ccm-worker.service > /dev/null << 'UNIT'
{unit}
UNIT
sudo systemctl daemon-reload
sudo systemctl enable ccm-worker > /dev/null 2>&1
sudo systemctl restart ccm-worker
"""
        code, out = await ssh.run(script, timeout=120)
        if code != 0:
            raise BootstrapError("ccm-service", out[-2000:])

    async def _step_health_check(self, worker_id: int, timeout: int = 120):
        async with self.db_factory() as db:
            worker = await db.get(Worker, worker_id)
        deadline = asyncio.get_event_loop().time() + timeout
        last_err = ""
        async with httpx.AsyncClient() as c:
            while asyncio.get_event_loop().time() < deadline:
                try:
                    body = await self._probe_health(worker, c)
                    # health 是 PUBLIC 路径不校验 token——必须再打一个需认证端点
                    await self._probe_auth(worker, c)
                    # 顺手记录 worker 自报 commit（应与部署 commit 一致）
                    if body.get("commit"):
                        await self._update(worker_id, ccm_commit=body["commit"], broadcast=False)
                    return
                except BootstrapError:
                    raise
                except Exception as e:
                    last_err = str(e)
                await asyncio.sleep(5)
        raise BootstrapError(
            "health-check",
            f"{last_err}（若连接被拒，检查安全组是否放行 Manager→Worker:{worker.ccm_port}）",
        )

    # ------------------------------------------------------------------
    # 关机 / 开机 / 销毁
    # ------------------------------------------------------------------

    async def stop_worker(self, worker_id: int):
        async with self._lifecycle_lock(worker_id):
            await self._stop_worker_locked(worker_id)

    async def _stop_worker_locked(self, worker_id: int):
        worker = await self._update(worker_id, status="stopping")
        # 必须先断 relay 再关机，否则触发约 17 分钟的指数退避重连风暴
        if self.relay is not None:
            try:
                await self.relay.stop_worker(worker_id)
            except Exception as exc:
                # Relay cleanup is Manager-local best effort.  Do not leave
                # the lifecycle permanently stuck in ``stopping`` when it
                # fails; stopping EC2 also ends the remote connection.
                logger.warning(
                    "worker %s stop: relay cleanup failed: %s",
                    worker_id,
                    exc,
                )
        try:
            if not worker.cloud_instance_id:
                # bootstrap 在开机前就失败过的 worker：没有实例可停
                await self._update(worker_id, status="stopped")
                return
            try:
                ssh = self._ssh(worker)
                await ssh.run("sudo systemctl stop ccm-worker", timeout=60)
            except Exception as e:
                logger.warning("worker %s: graceful service stop failed: %s", worker_id, e)
            await self.cloud.stop_instance(worker.cloud_instance_id)
            # 等到真正 stopped
            for _ in range(60):
                info = await self.cloud.describe_instance(worker.cloud_instance_id)
                if info["state"] == "stopped":
                    break
                await asyncio.sleep(5)
            await self._update(worker_id, status="stopped")
        except Exception as e:
            # 不留 "stopping" 终态卡死——回 error 让用户可 stop/start/destroy
            await self._update(
                worker_id, status="error", bootstrap_step=None,
                bootstrap_error=f"关机失败: {e}",
            )

    async def start_worker(self, worker_id: int):
        async with self._lifecycle_lock(worker_id):
            await self._start_worker_locked(worker_id)

    async def _start_worker_locked(self, worker_id: int):
        worker = await self._update(worker_id, status="starting")
        try:
            await self.cloud.start_instance(worker.cloud_instance_id)
            private_ip = await self.cloud.wait_until_running(worker.cloud_instance_id)
            info = await self.cloud.describe_instance(worker.cloud_instance_id)
            worker = await self._update(
                worker_id, private_ip=private_ip, public_ip=info.get("public_ip"),
            )
            ssh = self._ssh(worker)
            await self._step_ssh_wait(ssh)
            # systemd enable 过，等服务自启
            await self._step_health_check(worker_id, timeout=180)
            # Keep the Worker unavailable to dynamic account mutations until
            # the startup snapshot has been verified and merged.  Publishing
            # ``ready`` first let a concurrent /pool/add write be overwritten
            # by _check_pool_accounts' saved snapshot.
            await self._check_pool_accounts(worker)
            worker = await self._update(
                worker_id, status="ready", last_heartbeat=datetime.utcnow(),
                bootstrap_error=None, bootstrap_step=None,
            )
            if self.relay is not None and worker is not None:
                await self.relay.recover(worker)
        except BootstrapError as e:
            await self._update(
                worker_id,
                status="error",
                bootstrap_step=e.step,
                bootstrap_error=e.detail,
            )
        except Exception as e:
            # bootstrap_step=None：允许健康检查在服务自行恢复后自动回 ready
            await self._update(
                worker_id, status="error", bootstrap_step=None, bootstrap_error=str(e),
            )

    async def destroy_worker(self, worker_id: int):
        async with self._lifecycle_lock(worker_id):
            await self._destroy_worker_locked(worker_id)

    async def _destroy_worker_locked(self, worker_id: int):
        """销毁实例。任务迁移由调用方先行完成（Phase 3 接 TaskMigrator）。"""
        worker = await self._update(worker_id, status="destroying")
        if worker is None:
            return
        if self.relay is not None:
            try:
                await self.relay.stop_worker(worker_id)
            except Exception as e:
                # Relay cleanup is best-effort and must not strand a billable
                # instance.  Terminating EC2 also makes the relay unusable.
                logger.warning(
                    "worker %s destroy: relay stop failed: %s", worker_id, e,
                )
        try:
            if worker.cloud_instance_id:
                await self.cloud.terminate_instance(worker.cloud_instance_id)
        except Exception as e:
            logger.warning("worker %s destroy: %s", worker_id, e)
            await self._update(
                worker_id,
                status="error",
                bootstrap_step="destroy",
                bootstrap_error=f"销毁失败: {e}",
            )
            return

        # The cloud provider confirmed termination (or that the instance was
        # already absent).  Only now hide the row and erase every credential
        # that could authenticate to the former Worker or recreate its pools.
        await self._update(
            worker_id,
            status="terminated",
            bootstrap_step=None,
            bootstrap_error=None,
            auth_token=None,
            accounts=_scrub_destroyed_worker_accounts(worker.accounts),
        )

    # ------------------------------------------------------------------
    # 健康监控（lifespan 起一个循环）
    # ------------------------------------------------------------------

    async def health_check_loop(self, interval: int = 30):
        fail_counts: dict[int, int] = {}
        while True:
            try:
                await self._health_check_once(fail_counts)
            except Exception:
                logger.exception("worker health check loop error")
            await asyncio.sleep(interval)

    async def _health_check_once(self, fail_counts: dict[int, int]):
        async with self.db_factory() as db:
            result = await db.execute(
                select(Worker).where(Worker.status.in_(["ready", "error"]))
            )
            workers = result.scalars().all()
        if not workers:
            return
        # 并发探测 + 共享连接池：周期 = max(timeout) 而非 sum(timeouts)
        async with httpx.AsyncClient() as client:
            await asyncio.gather(
                *(self._health_check_worker(w, fail_counts, client) for w in workers),
                return_exceptions=True,
            )

    async def _health_check_worker(
        self, worker: Worker, fail_counts: dict[int, int], client: httpx.AsyncClient
    ):
        try:
            body = await self._probe_health(worker, client)
            fail_counts.pop(worker.id, None)
            fields = {"last_heartbeat": datetime.utcnow()}
            commit = body.get("commit")
            if commit:
                fields["ccm_commit"] = commit
            # The probe runs against a detached snapshot.  Every lifecycle
            # change below must therefore be compare-and-set: an in-flight
            # probe must never turn starting/stopping/destroying back into
            # ready/error.  Heartbeat metadata is also limited to monitored
            # states so a stale result does not touch a terminated row.
            recovered = False
            updated = None
            async with self.db_factory() as db:
                await db.execute(
                    update(Worker)
                    .where(
                        Worker.id == worker.id,
                        Worker.status.in_(("ready", "error")),
                    )
                    .values(**fields)
                )
                recovery = await db.execute(
                    update(Worker)
                    .where(
                        Worker.id == worker.id,
                        Worker.status == "error",
                        Worker.bootstrap_step.is_(None),
                    )
                    .values(status="ready", bootstrap_error=None)
                )
                recovered = recovery.rowcount == 1
                await db.commit()
                if recovered:
                    updated = await db.get(Worker, worker.id)
                    if updated is not None:
                        await db.refresh(updated)
            if recovered and updated is not None:
                try:
                    await self._broadcast(updated)
                except Exception:
                    logger.exception(
                        "worker %s health recovery broadcast failed", worker.id,
                    )
                if self.relay is not None:
                    try:
                        await self.relay.recover(updated)
                    except Exception:
                        # Relay recovery failure is not a failed Worker health
                        # probe and must not contribute to degradation counts.
                        logger.exception(
                            "worker %s relay recovery failed", worker.id,
                        )
        except Exception:
            # Re-read before counting: the detached snapshot may have entered
            # a lifecycle transition while the network request was pending.
            async with self.db_factory() as db:
                current_status = await db.scalar(
                    select(Worker.status).where(Worker.id == worker.id)
                )
            if current_status != "ready":
                fail_counts.pop(worker.id, None)
                return
            fail_counts[worker.id] = fail_counts.get(worker.id, 0) + 1
            if fail_counts[worker.id] < 3:
                return
            async with self.db_factory() as db:
                degraded = await db.execute(
                    update(Worker)
                    .where(Worker.id == worker.id, Worker.status == "ready")
                    .values(
                        status="error",
                        bootstrap_step=None,
                        bootstrap_error="健康检查连续 3 次失败",
                    )
                )
                changed = degraded.rowcount == 1
                await db.commit()
                updated = await db.get(Worker, worker.id) if changed else None
                if updated is not None:
                    await db.refresh(updated)
            fail_counts.pop(worker.id, None)
            if updated is not None:
                try:
                    await self._broadcast(updated)
                except Exception:
                    logger.exception(
                        "worker %s health degradation broadcast failed", worker.id,
                    )

    async def _check_pool_accounts(self, worker: Worker):
        """开机后 live-verify provider pools and recover saved credentials."""
        saved_accounts = [
            dict(account) for account in (worker.accounts or [])
            if isinstance(account, dict)
        ]
        codex_indexes = [
            index for index, account in enumerate(saved_accounts)
            if str(account.get("provider") or "claude").lower() == "codex"
        ]
        codex_successes = 0
        for index in codex_indexes:
            account = saved_accounts[index]
            try:
                account_id = await self.ensure_codex_account(worker, account)
                account["status"] = "logged_in"
                if account_id:
                    account["account_id"] = account_id
                codex_successes += 1
                await self._log(
                    worker.id,
                    f"Codex 账号 {account_id or account.get('email')} live 验证成功",
                )
            except Exception as exc:
                account["status"] = "failed"
                await self._log(
                    worker.id,
                    f"Codex 账号 {account.get('email')} 恢复失败: {str(exc)[-500:]}",
                )
        if codex_indexes:
            await self._update(worker.id, accounts=saved_accounts, broadcast=False)
            if codex_successes == 0:
                raise BootstrapError(
                    "account-login",
                    "Worker 开机后所有 Codex 账号 live 验证/恢复均失败",
                )

        try:
            async with httpx.AsyncClient(timeout=15) as c:
                r = await c.get(
                    f"http://{worker.private_ip}:{worker.ccm_port}/api/pool/usage",
                    headers={"Authorization": f"Bearer {worker.auth_token}"},
                )
                if r.status_code != 200:
                    return
                data = r.json()
                for acct in data.get("accounts", []):
                    if acct.get("usage_error") in ("no_credentials", "token_expired"):
                        aid = acct.get("id", "")
                        await self._log(
                            worker.id,
                            f"账号 {aid} 凭证过期，尝试 OAuth refresh...",
                        )
                        try:
                            rr = await c.post(
                                f"http://{worker.private_ip}:{worker.ccm_port}/api/pool/accounts/{aid}/relogin",
                                headers={"Authorization": f"Bearer {worker.auth_token}"},
                            )
                            await self._log(
                                worker.id,
                                f"账号 {aid} refresh: {rr.json().get('status', rr.status_code)}",
                            )
                        except Exception as e:
                            await self._log(worker.id, f"账号 {aid} refresh 失败: {e}")
        except Exception as e:
            logger.warning("worker %s pool check failed: %s", worker.id, e)
