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
from sqlalchemy import and_, or_, select, update

from backend.config import settings
from backend.models.worker import Worker
from backend.services.cancellation import settle_awaitable
from backend.services.cloud_provider import (
    CloudProvider,
    canonical_cloud_termination_scope,
)
from backend.services.git_info import REPO_ROOT, git_head_commit
from backend.services.ssh_executor import (
    SSHExecutor,
    SSHKeyMaterial,
    SSHKeyPreflightError,
    preflight_private_key,
    worker_known_hosts_path,
)

logger = logging.getLogger(__name__)


def worker_control_plane_enabled() -> bool:
    """Return whether Worker cloud/SSH effects have an authenticated plane."""

    return bool(
        settings.ccm_node_role == "manager"
        and isinstance(settings.auth_token, str)
        and settings.auth_token.strip()
    )


def require_worker_control_plane_enabled() -> None:
    """Fail before any Worker DB lifecycle mutation or external effect."""

    if not worker_control_plane_enabled():
        raise RuntimeError(
            "Worker control plane requires CCM_NODE_ROLE=manager and "
            "AUTH_TOKEN to be configured"
        )

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
WORKER_CODEX_CLI_VERSION = "0.147.0"
CLAUDE_LOGIN_IDENTITY_KEY = "claude_login_identity_v1"
CLAUDE_LOGIN_IDENTITY_VERSION = 1
_DESTROYED_ACCOUNT_AUDIT_FIELDS = (
    "email", "provider", "status", "account_id",
)
WORKER_PROVISION_SPEC_VERSION = 1
WORKER_RENAME_TAG_PROTOCOL_VERSION = 1
WORKER_RENAMEABLE_STATUSES = frozenset({"ready", "stopped", "error"})


def worker_create_client_token(worker_id: int, auth_token: str) -> str:
    """Return the stable RunInstances idempotency identity for one Worker."""

    if (
        isinstance(worker_id, bool)
        or not isinstance(worker_id, int)
        or worker_id <= 0
        or not isinstance(auth_token, str)
        or not auth_token
    ):
        raise ValueError("Worker create token identity is invalid")
    return "ccm-" + hashlib.sha256(
        f"{worker_id}:{auth_token}".encode("utf-8")
    ).hexdigest()[:48]


def worker_create_client_token_digest(worker_id: int, auth_token: str) -> str:
    """Return a non-secret receipt binding for the EC2 idempotency token."""

    token = worker_create_client_token(worker_id, auth_token)
    return hashlib.sha256(token.encode("ascii")).hexdigest()


def _canonical_json_digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def build_worker_rename_tag_outbox(
    worker: Worker,
    *,
    desired_name: str,
    generation: int,
    cloud_scope: dict[str, str],
    client_token_digest: str,
) -> dict:
    """Build exact durable authority for one cloud ``Name`` tag effect."""

    if (
        worker is None
        or isinstance(worker.id, bool)
        or not isinstance(worker.id, int)
        or worker.id <= 0
        or not isinstance(worker.cloud_instance_id, str)
        or not worker.cloud_instance_id
        or not isinstance(desired_name, str)
        or not desired_name
        or len(desired_name) > 200
        or isinstance(generation, bool)
        or not isinstance(generation, int)
        or generation <= 0
        or not isinstance(client_token_digest, str)
        or len(client_token_digest) != 64
        or any(char not in "0123456789abcdef" for char in client_token_digest)
    ):
        raise ValueError("Worker rename tag identity is invalid")
    spec = _validated_worker_provision_spec(
        worker.provision_spec,
        require_cloud_identity=True,
    )
    canonical_scope = canonical_cloud_termination_scope(cloud_scope)
    if spec["cloud_scope"] != canonical_scope:
        raise ValueError("Worker rename cloud scope differs from provision journal")
    return {
        "protocol_version": WORKER_RENAME_TAG_PROTOCOL_VERSION,
        "operation_id": pysecrets.token_hex(16),
        "generation": generation,
        "worker_id": worker.id,
        "cloud_instance_id": worker.cloud_instance_id,
        "desired_name": desired_name,
        "cloud_scope": canonical_scope,
        "client_token_digest": client_token_digest,
        "provision_spec_digest": _canonical_json_digest(spec),
        "prepared_at": datetime.utcnow().isoformat(timespec="microseconds"),
    }


def require_worker_rename_tag_outbox(worker: Worker) -> dict:
    """Validate one pending rename against the exact current Worker row."""

    receipt = worker.rename_tag_outbox
    expected_keys = {
        "protocol_version",
        "operation_id",
        "generation",
        "worker_id",
        "cloud_instance_id",
        "desired_name",
        "cloud_scope",
        "client_token_digest",
        "provision_spec_digest",
        "prepared_at",
    }
    if not isinstance(receipt, dict) or set(receipt) != expected_keys:
        raise RuntimeError("Worker rename tag outbox has an invalid shape")
    operation_id = receipt.get("operation_id")
    generation = receipt.get("generation")
    desired_name = receipt.get("desired_name")
    prepared_at = receipt.get("prepared_at")
    if (
        receipt.get("protocol_version") != WORKER_RENAME_TAG_PROTOCOL_VERSION
        or not isinstance(operation_id, str)
        or len(operation_id) != 32
        or any(char not in "0123456789abcdef" for char in operation_id)
        or isinstance(generation, bool)
        or not isinstance(generation, int)
        or generation <= 0
        or generation != worker.rename_generation
        or receipt.get("worker_id") != worker.id
        or receipt.get("cloud_instance_id") != worker.cloud_instance_id
        or not isinstance(desired_name, str)
        or not desired_name
        or len(desired_name) > 200
        or desired_name != worker.name
        or not isinstance(prepared_at, str)
        or not prepared_at
        or worker.status not in WORKER_RENAMEABLE_STATUSES
        or worker.bootstrap_step is not None
        or not isinstance(worker.auth_token, str)
        or not worker.auth_token
    ):
        raise RuntimeError("Worker rename tag outbox differs from the Worker row")
    scope = canonical_cloud_termination_scope(receipt.get("cloud_scope"))
    spec = _validated_worker_provision_spec(
        worker.provision_spec,
        require_cloud_identity=True,
    )
    expected_token_digest = worker_create_client_token_digest(
        worker.id,
        worker.auth_token,
    )
    if (
        scope != spec["cloud_scope"]
        or not pysecrets.compare_digest(
            str(receipt.get("client_token_digest") or ""),
            expected_token_digest,
        )
        or not pysecrets.compare_digest(
            str(receipt.get("provision_spec_digest") or ""),
            _canonical_json_digest(spec),
        )
    ):
        raise RuntimeError("Worker rename tag outbox cloud identity is invalid")
    return json.loads(json.dumps(receipt))


def worker_claude_login_identity(worker: Worker) -> dict:
    """Build a non-secret binding for one exact Worker login generation."""

    if (
        worker is None
        or isinstance(worker.id, bool)
        or not isinstance(worker.id, int)
        or worker.id <= 0
        or not isinstance(worker.cloud_instance_id, str)
        or not worker.cloud_instance_id
        or not isinstance(worker.auth_token, str)
        or not worker.auth_token
    ):
        raise ValueError("Worker lacks the identity required for Claude login")
    spec = _validated_worker_provision_spec(
        worker.provision_spec,
        require_cloud_identity=True,
    )
    client_token_digest = worker_create_client_token_digest(
        worker.id,
        worker.auth_token,
    )
    if not pysecrets.compare_digest(
        spec["client_token_digest"],
        client_token_digest,
    ):
        raise ValueError(
            "Worker provision journal differs from its Claude login identity"
        )
    return {
        "version": CLAUDE_LOGIN_IDENTITY_VERSION,
        "worker_id": worker.id,
        "cloud_instance_id": worker.cloud_instance_id,
        "client_token_digest": client_token_digest,
        "provision_spec_digest": _canonical_json_digest(spec),
    }


def claude_login_identity_matches(worker: Worker, account: dict) -> bool:
    """Return whether a logged-in Claude account belongs to this exact node."""

    if not isinstance(account, dict):
        return False
    actual = account.get(CLAUDE_LOGIN_IDENTITY_KEY)
    if not isinstance(actual, dict):
        return False
    try:
        expected = worker_claude_login_identity(worker)
    except (TypeError, ValueError):
        return False
    if (
        type(actual.get("version")) is not int
        or actual.get("version") != CLAUDE_LOGIN_IDENTITY_VERSION
        or isinstance(actual.get("worker_id"), bool)
        or not isinstance(actual.get("worker_id"), int)
        or actual.get("worker_id") != expected["worker_id"]
        or actual.get("cloud_instance_id") != expected["cloud_instance_id"]
    ):
        return False
    for key in ("client_token_digest", "provision_spec_digest"):
        value = actual.get(key)
        if (
            not isinstance(value, str)
            or len(value) != 64
            or any(char not in "0123456789abcdef" for char in value)
            or not pysecrets.compare_digest(value, expected[key])
        ):
            return False
    return True


def _validated_worker_provision_spec(
    value: object,
    *,
    require_cloud_identity: bool,
) -> dict:
    """Validate and detach the durable RunInstances request journal.

    Version 1 deliberately permits additive metadata so installations that
    predate cloud-scope fencing can be upgraded in place.  The request fields
    themselves remain exact and ``client_token`` is never stored in the JSON;
    only its SHA-256 binding is retained.
    """

    if (
        not isinstance(value, dict)
        or type(value.get("version")) is not int
        or value.get("version") != WORKER_PROVISION_SPEC_VERSION
        or not isinstance(value.get("name"), str)
        or not value["name"].strip()
        or type(value.get("has_fixed_overrides")) is not bool
        or not isinstance(value.get("overrides"), dict)
        or "client_token" in value["overrides"]
    ):
        raise ValueError("Worker provision spec has an invalid request journal")
    detached = json.loads(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    )
    if require_cloud_identity:
        detached["cloud_scope"] = canonical_cloud_termination_scope(
            detached.get("cloud_scope")
        )
        digest = detached.get("client_token_digest")
        if (
            not isinstance(digest, str)
            or len(digest) != 64
            or any(char not in "0123456789abcdef" for char in digest)
        ):
            raise ValueError("Worker provision spec lacks ClientToken identity")
    return detached

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
            and type(account[key]) in {str, int, float, bool}
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
            if "status" in fields:
                next_status = fields["status"]
                next_step = fields.get("bootstrap_step", worker.bootstrap_step)
                if next_status != "destroying" and next_step != "destroy":
                    worker.destroy_lifecycle_nonce = None
                    worker.destroy_termination_receipt = None
            await db.commit()
            await db.refresh(worker)
        if broadcast:
            await self._broadcast(worker, log_line)
        return worker

    async def _current_cloud_scope(self) -> dict[str, str]:
        """Read and strictly canonicalize the provider effect boundary."""

        return canonical_cloud_termination_scope(
            await self.cloud.termination_scope()
        )

    async def _persist_worker_provision_identity(
        self,
        worker: Worker,
        *,
        provision_spec: dict,
        adopted_instance_id: str | None = None,
    ) -> Worker:
        """CAS one legacy cloud-identity backfill under the Worker writer lock."""

        expected_spec_digest = _canonical_json_digest(worker.provision_spec)
        async with self.db_factory() as db:
            predicates = [
                Worker.id == worker.id,
                Worker.status == worker.status,
                (
                    Worker.cloud_instance_id.is_(None)
                    if worker.cloud_instance_id is None
                    else Worker.cloud_instance_id == worker.cloud_instance_id
                ),
                (
                    Worker.destroy_lifecycle_nonce.is_(None)
                    if worker.destroy_lifecycle_nonce is None
                    else Worker.destroy_lifecycle_nonce
                    == worker.destroy_lifecycle_nonce
                ),
            ]
            locked = await db.execute(
                update(Worker)
                .where(*predicates)
                .values(status=Worker.status)
                .execution_options(synchronize_session=False)
            )
            if locked.rowcount != 1:
                await db.rollback()
                raise RuntimeError(
                    "Worker lifecycle changed during cloud identity reconciliation"
                )
            current = await db.get(Worker, worker.id, populate_existing=True)
            if (
                current is None
                or current.auth_token != worker.auth_token
                or _canonical_json_digest(current.provision_spec)
                != expected_spec_digest
            ):
                await db.rollback()
                raise RuntimeError(
                    "Worker credential or provision journal changed during "
                    "cloud identity reconciliation"
                )
            if adopted_instance_id is not None:
                if current.cloud_instance_id not in (None, adopted_instance_id):
                    await db.rollback()
                    raise RuntimeError(
                        "Worker instance changed during ClientToken reconciliation"
                    )
                current.cloud_instance_id = adopted_instance_id
            current.provision_spec = provision_spec
            await db.commit()
            await db.refresh(current)
            return current

    async def require_worker_cloud_identity(
        self,
        worker: Worker,
        *,
        verify_private_ip: bool = False,
        include_terminated: bool = False,
    ) -> dict:
        """Prove one row belongs to the current provider scope and ClientToken.

        Existing version-1 journals created before cloud-scope fencing are
        backfilled only after the deterministic ClientToken resolves uniquely
        to the exact row instance.  Missing or ambiguous evidence fails closed.
        """

        if (
            worker is None
            or not isinstance(worker.cloud_instance_id, str)
            or not worker.cloud_instance_id
            or not isinstance(worker.auth_token, str)
            or not worker.auth_token
        ):
            raise RuntimeError(
                "Worker lacks the cloud instance/credential identity required "
                "for a lifecycle effect"
            )
        current_scope = await self._current_cloud_scope()
        client_token = worker_create_client_token(worker.id, worker.auth_token)
        client_token_digest = worker_create_client_token_digest(
            worker.id,
            worker.auth_token,
        )

        raw_spec = worker.provision_spec
        if raw_spec is None:
            base_spec = {
                "version": WORKER_PROVISION_SPEC_VERSION,
                "name": worker.name,
                "has_fixed_overrides": False,
                "overrides": {},
            }
        else:
            base_spec = _validated_worker_provision_spec(
                raw_spec,
                require_cloud_identity=False,
            )

        frozen_scope_value = base_spec.get("cloud_scope")
        frozen_digest = base_spec.get("client_token_digest")
        needs_backfill = (
            frozen_scope_value is None
            or frozen_digest is None
        )
        if frozen_scope_value is not None:
            frozen_scope = canonical_cloud_termination_scope(
                frozen_scope_value
            )
            if frozen_scope != current_scope:
                raise RuntimeError(
                    "Worker cloud scope differs from the durable provision journal"
                )
        if frozen_digest is not None:
            if (
                not isinstance(frozen_digest, str)
                or len(frozen_digest) != 64
                or any(
                    char not in "0123456789abcdef"
                    for char in frozen_digest
                )
                or not pysecrets.compare_digest(
                    frozen_digest,
                    client_token_digest,
                )
            ):
                raise RuntimeError(
                    "Worker ClientToken identity differs from its provision journal"
                )

        resolved_instance_id = await self.cloud.find_instance_by_create_token(
            client_token,
            include_terminated=include_terminated,
        )
        if resolved_instance_id != worker.cloud_instance_id:
            raise RuntimeError(
                "Worker ClientToken did not resolve to the exact cloud instance"
            )

        if needs_backfill:
            base_spec["cloud_scope"] = current_scope
            base_spec["client_token_digest"] = client_token_digest
            base_spec["identity_reconciliation"] = {
                "version": 1,
                "method": "client_token",
                "instance_id": worker.cloud_instance_id,
            }
            worker = await self._persist_worker_provision_identity(
                worker,
                provision_spec=base_spec,
            )
        spec = _validated_worker_provision_spec(
            worker.provision_spec,
            require_cloud_identity=True,
        )
        if spec["cloud_scope"] != current_scope:
            raise RuntimeError(
                "Worker cloud scope changed after identity reconciliation"
            )
        if not pysecrets.compare_digest(
            spec["client_token_digest"],
            client_token_digest,
        ):
            raise RuntimeError(
                "Worker ClientToken changed after identity reconciliation"
            )

        instance_info = None
        if verify_private_ip:
            instance_info = await self.cloud.describe_instance(
                worker.cloud_instance_id
            )
            if (
                not isinstance(instance_info, dict)
                or instance_info.get("instance_id")
                != worker.cloud_instance_id
                or not isinstance(worker.private_ip, str)
                or not worker.private_ip
                or instance_info.get("private_ip") != worker.private_ip
            ):
                raise RuntimeError(
                    "Worker endpoint does not match the exact cloud instance"
                )
        return {
            "worker": worker,
            "cloud_scope": current_scope,
            "client_token": client_token,
            "client_token_digest": client_token_digest,
            "provision_spec_digest": _canonical_json_digest(spec),
            "instance_info": instance_info,
        }

    async def reconcile_worker_rename_tag_outbox(
        self,
        worker_id: int,
        *,
        expected_operation_id: str | None = None,
    ) -> Worker | None:
        """Replay and settle one exact monotonic cloud ``Name`` tag outbox.

        EC2's create_tags API has no conditional generation parameter.  The
        Worker row is therefore kept writer-locked across the short provider
        call: a newer generation cannot be admitted while an older request is
        still able to arrive at AWS.  Process loss releases the database lock
        while retaining the outbox, and replaying the same Name value is
        idempotent whether or not the previous response was acknowledged.
        """

        require_worker_control_plane_enabled()
        async with self.db_factory() as db:
            cancellation: asyncio.CancelledError | None = None
            reconciled_worker: Worker | None = None
            try:
                locked = await db.execute(
                    update(Worker)
                    .where(
                        Worker.id == worker_id,
                        Worker.rename_tag_outbox.is_not(None),
                    )
                    .values(rename_generation=Worker.rename_generation)
                    .execution_options(synchronize_session=False)
                )
                if locked.rowcount != 1:
                    await db.rollback()
                    return await db.get(Worker, worker_id)

                worker = await db.get(
                    Worker,
                    worker_id,
                    populate_existing=True,
                )
                if worker is None:
                    raise RuntimeError("Worker disappeared during rename replay")
                receipt = require_worker_rename_tag_outbox(worker)
                if (
                    expected_operation_id is not None
                    and receipt["operation_id"] != expected_operation_id
                ):
                    raise RuntimeError(
                        "Worker rename generation changed before cloud replay"
                    )

                # Repeat live scope + ClientToken resolution while the row is
                # locked. No stale receipt can target a replacement instance
                # or a provider account/region selected after admission.
                identity = await self.require_worker_cloud_identity(worker)
                if (
                    identity["worker"].id != worker.id
                    or identity["worker"].cloud_instance_id
                    != receipt["cloud_instance_id"]
                    or identity["cloud_scope"] != receipt["cloud_scope"]
                    or not pysecrets.compare_digest(
                        identity["client_token_digest"],
                        receipt["client_token_digest"],
                    )
                ):
                    raise RuntimeError(
                        "Worker cloud identity changed before rename replay"
                    )

                # boto-backed provider calls run in a thread. Cancelling the
                # asyncio waiter does not stop that thread, so releasing the
                # row lock immediately would let a newer generation reach AWS
                # while this older request was still in flight. Delay caller
                # cancellation until the provider attempt has actually
                # settled, keeping the monotonic writer fence intact.
                async def apply_effect_and_ack() -> Worker:
                    await self.cloud.update_instance_tags(
                        receipt["cloud_instance_id"],
                        {"Name": receipt["desired_name"]},
                    )
                    # This attached row is still protected by the same writer
                    # transaction. Clearing exactly this outbox and committing
                    # publishes the provider acknowledgement atomically.
                    worker.rename_tag_outbox = None
                    await db.commit()
                    await db.refresh(worker)
                    return worker

                operation, cancellation = await settle_awaitable(
                    apply_effect_and_ack()
                )
                reconciled_worker = operation.result()
            except BaseException:
                rollback, _rollback_cancellation = await settle_awaitable(
                    db.rollback()
                )
                rollback.result()
                raise
            if cancellation is not None:
                raise cancellation
            return reconciled_worker

    async def recover_worker_rename_tag_outboxes(self) -> tuple[int, int]:
        """Best-effort startup/health recovery for every pending rename."""

        require_worker_control_plane_enabled()
        async with self.db_factory() as db:
            worker_ids = list(
                (
                    await db.execute(
                        select(Worker.id)
                        .where(Worker.rename_tag_outbox.is_not(None))
                        .order_by(Worker.id)
                    )
                ).scalars()
            )
        recovered = 0
        failed = 0
        for pending_worker_id in worker_ids:
            try:
                await self.reconcile_worker_rename_tag_outbox(
                    pending_worker_id
                )
                recovered += 1
            except Exception:
                failed += 1
                logger.exception(
                    "worker %s cloud Name tag outbox recovery failed",
                    pending_worker_id,
                )
        return recovered, failed

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

    async def _probe_auth(self, worker: Worker, client: httpx.AsyncClient) -> dict:
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
        payload = r.json()
        if not isinstance(payload, dict):
            raise BootstrapError("health-check", "Worker stats response is invalid")
        if payload.get("ccm_node_role") != "worker":
            raise BootstrapError(
                "health-check",
                "Worker node role mismatch: expected CCM_NODE_ROLE=worker, "
                f"got {payload.get('ccm_node_role')!r}",
            )
        from backend.services.task_id_namespace import (
            TASK_ID_NAMESPACE_PROTOCOL,
            TASK_ID_WORKER_NAMESPACE_START,
        )

        if (
            payload.get("task_id_namespace_protocol")
            != TASK_ID_NAMESPACE_PROTOCOL
            or payload.get("task_id_namespace_boundary")
            != TASK_ID_WORKER_NAMESPACE_START
        ):
            raise BootstrapError(
                "health-check",
                "Worker Task id namespace protocol is missing or incompatible",
            )
        return payload

    @staticmethod
    def _require_expected_commit(worker: Worker, health: object) -> str:
        """Verify the remote binary against the Manager's immutable expectation.

        ``Worker.ccm_commit`` is written by the rsync deployment step.  A
        health response is evidence about the remote node; it must never be
        allowed to replace that expected value, otherwise a stale or tampered
        Worker can make itself appear current simply by reporting its own
        commit.
        """
        expected = worker.ccm_commit
        if not isinstance(expected, str) or not expected or expected != expected.strip():
            raise BootstrapError(
                "health-check",
                "Manager 缺少有效的 Worker 预期 commit；请重新部署该 Worker，"
                "不能采信远端自报版本",
            )
        if not isinstance(health, dict):
            raise BootstrapError("health-check", "Worker health response is invalid")
        actual = health.get("commit")
        if not isinstance(actual, str) or not actual or actual != actual.strip():
            raise BootstrapError(
                "health-check",
                f"Worker 未返回有效 commit（期望 {expected}）；请重新部署该 Worker",
            )
        if actual != expected:
            raise BootstrapError(
                "health-check",
                "Worker commit 不匹配："
                f"Manager 期望 {expected}，远端报告 {actual}；"
                "必须重新部署后才能启动或接收任务",
            )
        return actual

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
        require_worker_control_plane_enabled()
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
        require_worker_control_plane_enabled()
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
        require_worker_control_plane_enabled()
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
        require_worker_control_plane_enabled()
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

            # retry 场景：DB 里已有实例 ID 且实例还在 → 跳过创建直接 bootstrap。
            # Before any start/reuse effect, prove that the row still belongs
            # to this exact provider account/region and deterministic
            # RunInstances ClientToken.
            existing_iid = worker.cloud_instance_id if worker else None
            if existing_iid:
                try:
                    identity = await self.require_worker_cloud_identity(
                        worker,
                        include_terminated=True,
                    )
                    worker = identity["worker"]
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
                    cloud_scope = await self._current_cloud_scope()
                    client_token_digest = worker_create_client_token_digest(
                        worker.id,
                        worker.auth_token,
                    )
                    spec = {
                        "version": WORKER_PROVISION_SPEC_VERSION,
                        "name": worker.name,
                        "has_fixed_overrides": has_fixed_overrides,
                        "overrides": frozen_overrides,
                        "cloud_scope": cloud_scope,
                        "client_token_digest": client_token_digest,
                    }
                    # This commit is the create-request journal.  It must land
                    # before RunInstances so every retry after a lost response
                    # reuses byte-equivalent semantic parameters.
                    worker = await self._persist_worker_provision_identity(
                        worker,
                        provision_spec=spec,
                    )
                else:
                    try:
                        legacy_spec = _validated_worker_provision_spec(
                            spec,
                            require_cloud_identity=False,
                        )
                    except (TypeError, ValueError) as exc:
                        raise BootstrapError(
                            "provision-config",
                            "Worker 保存的 EC2 创建请求日志无效；"
                            "为避免重复实例已停止",
                        ) from exc
                    missing_scope = legacy_spec.get("cloud_scope") is None
                    missing_token_digest = (
                        legacy_spec.get("client_token_digest") is None
                    )
                    if missing_scope or missing_token_digest:
                        # An old journal may have been committed immediately
                        # before RunInstances in another account/region.  The
                        # current scope can be adopted only when ClientToken
                        # discovery proves the exact already-created instance.
                        cloud_scope = await self._current_cloud_scope()
                        client_token = worker_create_client_token(
                            worker.id,
                            worker.auth_token,
                        )
                        resolved = await self.cloud.find_instance_by_create_token(
                            client_token
                        )
                        if not isinstance(resolved, str) or not resolved:
                            raise BootstrapError(
                                "provision-config",
                                "旧版 EC2 创建请求日志没有冻结云账号/区域，且当前"
                                " ClientToken 无法认领实例；为避免跨账号重复计费已停止",
                            )
                        legacy_spec["cloud_scope"] = cloud_scope
                        legacy_spec["client_token_digest"] = (
                            worker_create_client_token_digest(
                                worker.id,
                                worker.auth_token,
                            )
                        )
                        legacy_spec["identity_reconciliation"] = {
                            "version": 1,
                            "method": "client_token",
                            "instance_id": resolved,
                        }
                        worker = await self._persist_worker_provision_identity(
                            worker,
                            provision_spec=legacy_spec,
                            adopted_instance_id=resolved,
                        )
                        spec = legacy_spec
                        existing_iid = resolved
                    else:
                        current_scope = await self._current_cloud_scope()
                        frozen_scope = canonical_cloud_termination_scope(
                            legacy_spec["cloud_scope"]
                        )
                        expected_token_digest = (
                            worker_create_client_token_digest(
                                worker.id,
                                worker.auth_token,
                            )
                        )
                        if (
                            frozen_scope != current_scope
                            or not pysecrets.compare_digest(
                                legacy_spec["client_token_digest"],
                                expected_token_digest,
                            )
                        ):
                            raise BootstrapError(
                                "provision-config",
                                "Worker EC2 创建请求的云作用域或 ClientToken "
                                "与当前运行身份不一致",
                            )
                        spec = legacy_spec
                try:
                    spec = _validated_worker_provision_spec(
                        spec,
                        require_cloud_identity=True,
                    )
                except (TypeError, ValueError) as exc:
                    raise BootstrapError(
                        "provision-config",
                        "Worker 保存的 EC2 创建请求日志无效；为避免重复实例已停止",
                    ) from exc
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
                overrides["client_token"] = worker_create_client_token(
                    worker.id,
                    worker.auth_token,
                )
                if existing_iid:
                    info = await self.cloud.describe_instance(existing_iid)
                    if info.get("state") not in ("pending", "running", "stopped"):
                        raise BootstrapError(
                            step,
                            f"ClientToken 认领的实例 {existing_iid} 当前为 "
                            f"{info.get('state') or 'unknown'}",
                        )
                    if info.get("state") == "stopped":
                        await self.cloud.start_instance(existing_iid)
                    iid = existing_iid
                else:
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

            await self._log(worker_id, "worker ready")
            worker = await self._update(
                worker_id, status="ready", bootstrap_step=None,
                last_heartbeat=datetime.utcnow(),
            )
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
        await run_step("ccm-quiesce", self._step_ccm_quiesce(ssh))
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
sudo apt-get install -y -qq git curl rsync python3-venv bubblewrap socat > /dev/null
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

    async def _step_ccm_quiesce(self, ssh: SSHExecutor) -> None:
        """Stop an adopted/legacy service before replacing code or config."""

        code, out = await ssh.run(
            "sudo systemctl stop ccm-worker.service >/dev/null 2>&1 || true",
            timeout=60,
        )
        if code != 0:
            raise BootstrapError("ccm-quiesce", out[-1000:])

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
            # Worker-local derived Tasks use the disjoint high id namespace;
            # Manager mirrors continue to retain their low global id.
            "CCM_NODE_ROLE=worker",
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
            raw_status = account.get("status")
            raw_claude_identity = account.get(CLAUDE_LOGIN_IDENTITY_KEY)
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
            if (
                provider == "claude"
                and isinstance(raw_status, str)
                and raw_status in {"logged_in", "failed"}
            ):
                normalized_accounts[-1]["status"] = raw_status
                if raw_status == "logged_in" and isinstance(
                    raw_claude_identity,
                    dict,
                ):
                    normalized_accounts[-1][CLAUDE_LOGIN_IDENTITY_KEY] = dict(
                        raw_claude_identity
                    )
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

        claude_login_identity = None
        if any(account["provider"] == "claude" for account in normalized_accounts):
            try:
                claude_login_identity = worker_claude_login_identity(worker)
            except (TypeError, ValueError) as exc:
                raise BootstrapError(
                    "account-login",
                    "Worker 云实例/创建日志身份不完整，无法安全记录 Claude 登录",
                ) from exc

        results = []

        async def persist_login_progress(snapshot: list[dict]) -> None:
            """Journal account outcomes around non-idempotent remote effects."""

            updated = await self._update(
                worker_id,
                broadcast=False,
                accounts=[dict(item) for item in snapshot],
            )
            if updated is None:
                raise BootstrapError(
                    "account-login",
                    "Worker record disappeared while saving account login state",
                )

        claude_index = 0
        for account_index, acct in enumerate(normalized_accounts):
            email = acct["email"]
            token = acct["token"]
            password = acct["password"]
            provider = acct["provider"]
            login_method = acct["login_method"]
            remaining_accounts = normalized_accounts[account_index + 1:]
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
                if (
                    acct.get("status") == "logged_in"
                    and claude_login_identity_matches(worker, acct)
                ):
                    results.append({
                        **acct,
                        "account_id": name,
                        CLAUDE_LOGIN_IDENTITY_KEY: dict(claude_login_identity),
                    })
                    await self._log(
                        worker_id,
                        f"Claude {email} already logged in as pool slot {name}; "
                        "skipping remote login",
                    )
                    await persist_login_progress([
                        *results,
                        *remaining_accounts,
                    ])
                    continue
                if acct.get("status") == "logged_in":
                    acct.pop(CLAUDE_LOGIN_IDENTITY_KEY, None)
                    await self._log(
                        worker_id,
                        f"Claude {email} login belongs to an older Worker "
                        "generation; running login again",
                    )
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

                # auto_login replaces the remote credential files and drives
                # an OAuth flow.  A lost SSH result cannot prove whether that
                # effect committed, so move every retryable/conclusive old
                # state to a durable non-terminal marker before the first
                # remote command.  Startup recovery then preserves
                # account-login and retry admission fails closed on pending.
                pending_account = {
                    **acct,
                    "status": "pending",
                    "account_id": name,
                }
                pending_account.pop(CLAUDE_LOGIN_IDENTITY_KEY, None)
                await persist_login_progress([
                    *results,
                    pending_account,
                    *remaining_accounts,
                ])
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
            if provider == "claude":
                result.pop(CLAUDE_LOGIN_IDENTITY_KEY, None)
                if status == "logged_in":
                    result[CLAUDE_LOGIN_IDENTITY_KEY] = dict(
                        claude_login_identity
                    )
            if account_id:
                result["account_id"] = account_id
            results.append(result)
            # Publish each known result immediately.  If the process dies
            # after the remote effect but before this commit, the pre-effect
            # pending marker remains and blocks an unsafe replay.  If this
            # commit wins first, retry can safely reuse logged_in or rerun a
            # deterministically failed attempt.
            await persist_login_progress([
                *results,
                *remaining_accounts,
            ])
            await self._log(worker_id, f"login {email}: {status}")
            if code != 0:
                await self._log(worker_id, f"login output: {out[-500:]}")
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

    async def _step_ccm_service(
        self,
        ssh: SSHExecutor,
        worker: Worker,
        *,
        restart: bool = True,
    ):
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
Environment=CCM_NODE_ROLE=worker
Environment=WORKER_ENABLED=false
ExecStart={remote_dir}/.venv/bin/python -m uvicorn backend.main:app --host 0.0.0.0 --port {worker.ccm_port}
Restart=always
RestartSec=5
# Let the manager record and retry a model subprocess killed under memory
# pressure instead of systemd stopping the entire worker cgroup.
OOMPolicy=continue

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
sudo systemctl {"restart" if restart else "stop"} ccm-worker
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
                    # ccm_commit 是部署时冻结的期望值；远端 health 只能作为
                    # 比对证据，绝不能反向覆盖期望值。
                    self._require_expected_commit(worker, body)
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
        require_worker_control_plane_enabled()
        async with self._lifecycle_lock(worker_id):
            await self._stop_worker_locked(worker_id)

    async def _stop_worker_locked(self, worker_id: int):
        worker = await self._update(worker_id, status="stopping")
        if worker is None:
            return
        if worker.cloud_instance_id:
            try:
                identity = await self.require_worker_cloud_identity(
                    worker,
                    verify_private_ip=True,
                )
                worker = identity["worker"]
            except Exception as exc:
                await self._update(
                    worker_id,
                    status="error",
                    bootstrap_step=None,
                    bootstrap_error=(
                        "关机失败: Worker 云账号/区域/ClientToken 身份无法确认: "
                        f"{exc}"
                    ),
                )
                return
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
            ssh = self._ssh(worker)
            # Persist the node role in both .env and the unit before EC2 is
            # stopped.  Failure is not best-effort: powering off without this
            # durable convergence would let the next boot run upgraded code as
            # a Manager and permanently bind the Worker's Task namespace wrong.
            await self._step_ccm_config(ssh, worker_id)
            await self._step_ccm_service(ssh, worker, restart=False)
            await self.cloud.stop_instance(worker.cloud_instance_id)
            # 等到真正 stopped。API 接受 stop 请求不代表 EC2 已进入终态；
            # 超时或未知结果都必须保留为 error，不能伪造 stopped 证明。
            final_state: str | None = None
            for _ in range(60):
                info = await self.cloud.describe_instance(worker.cloud_instance_id)
                final_state = info.get("state") if isinstance(info, dict) else None
                if final_state == "stopped":
                    break
                await asyncio.sleep(5)
            if final_state != "stopped":
                raise RuntimeError(
                    "云平台在关机轮询结束后仍未确认实例 stopped"
                    f"（最后状态: {final_state or 'unknown'}）"
                )
            await self._update(worker_id, status="stopped")
        except Exception as e:
            # 不留 "stopping" 终态卡死——回 error 让用户可 stop/start/destroy
            await self._update(
                worker_id, status="error", bootstrap_step=None,
                bootstrap_error=f"关机失败: {e}",
            )

    async def start_worker(self, worker_id: int):
        require_worker_control_plane_enabled()
        async with self._lifecycle_lock(worker_id):
            await self._start_worker_locked(worker_id)

    async def _start_worker_locked(self, worker_id: int):
        worker = await self._update(worker_id, status="starting")
        try:
            if worker is None:
                return
            identity = await self.require_worker_cloud_identity(worker)
            worker = identity["worker"]
            await self.cloud.start_instance(worker.cloud_instance_id)
            private_ip = await self.cloud.wait_until_running(worker.cloud_instance_id)
            info = await self.cloud.describe_instance(worker.cloud_instance_id)
            worker = await self._update(
                worker_id, private_ip=private_ip, public_ip=info.get("public_ip"),
            )
            ssh = self._ssh(worker)
            await self._step_ssh_wait(ssh)
            # A legacy enabled unit may have auto-started with a stale .env.
            # Quiesce it, atomically converge the role, replace the unit with
            # an explicit role override, then perform the only trusted start.
            await self._step_ccm_quiesce(ssh)
            await self._step_ccm_config(ssh, worker_id)
            await self._step_ccm_service(ssh, worker)
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

    @staticmethod
    def _destroy_claim_identity_predicates(destroy_claim) -> tuple:
        """Return claim predicates without hard-coding one transient status."""

        from backend.services.worker_proxy import (
            _worker_destroy_lifecycle_predicates,
        )

        predicates = _worker_destroy_lifecycle_predicates(destroy_claim)
        # The helper's second predicate is ``status == destroying``.  Outcome
        # CAS operations need the same sealed identity while allowing an exact
        # same-receipt error to converge monotonically to terminated.
        return (predicates[0], *predicates[2:])

    @staticmethod
    def _destroy_receipt_digest_predicate(receipt_digest: str):
        return (
            Worker.destroy_termination_receipt["receipt_digest"].as_string()
            == receipt_digest
        )

    async def _load_destroy_effect_authority(
        self,
        destroy_claim,
        *,
        cloud_scope: dict[str, str],
    ) -> tuple[Worker, str]:
        """Writer-fence and validate the exact durable cloud outbox."""

        from backend.services.worker_proxy import (
            worker_destroy_termination_receipt_matches,
        )

        identity_predicates = self._destroy_claim_identity_predicates(
            destroy_claim
        )
        async with self.db_factory() as db:
            locked = await db.execute(
                update(Worker)
                .where(
                    *identity_predicates,
                    Worker.status == "destroying",
                )
                .values(status=Worker.status)
                .execution_options(synchronize_session=False)
            )
            if locked.rowcount != 1:
                await db.rollback()
                current = await db.get(Worker, destroy_claim.worker_id)
                if current is not None and current.status == "terminated":
                    raise FileExistsError(
                        "Worker cloud termination already reached terminal state"
                    )
                raise RuntimeError(
                    "Worker destroy lifecycle changed before the cloud effect"
                )
            worker = await db.get(
                Worker,
                destroy_claim.worker_id,
                populate_existing=True,
            )
            if worker is None or worker.auth_token != destroy_claim.auth_token:
                await db.rollback()
                raise RuntimeError(
                    "Worker credential changed before the cloud effect"
                )
            token_digest = worker_create_client_token_digest(
                worker.id,
                worker.auth_token,
            )
            if not worker_destroy_termination_receipt_matches(
                worker,
                cloud_scope=cloud_scope,
                client_token_digest=token_digest,
            ):
                await db.rollback()
                raise RuntimeError(
                    "missing or malformed durable cloud termination authority"
                )
            receipt_digest = worker.destroy_termination_receipt.get(
                "receipt_digest"
            )
            if (
                not isinstance(receipt_digest, str)
                or len(receipt_digest) != 64
                or any(
                    char not in "0123456789abcdef"
                    for char in receipt_digest
                )
            ):
                await db.rollback()
                raise RuntimeError(
                    "Worker cloud termination receipt digest is invalid"
                )
            # No state change is needed; rollback releases the writer barrier
            # without advancing updated_at merely because authority was read.
            db.expunge(worker)
            await db.rollback()
            return worker, receipt_digest

    async def _mark_destroy_effect_error(
        self,
        destroy_claim,
        *,
        detail: str,
        receipt_digest: str | None,
    ) -> Worker | None:
        """Record a retryable exact-claim failure without reviving terminal."""

        predicates = [
            *self._destroy_claim_identity_predicates(destroy_claim),
            Worker.status == "destroying",
        ]
        if receipt_digest is not None:
            predicates.append(
                self._destroy_receipt_digest_predicate(receipt_digest)
            )
        async with self.db_factory() as db:
            result = await db.execute(
                update(Worker)
                .where(*predicates)
                .values(
                    status="error",
                    bootstrap_step="destroy",
                    bootstrap_error=detail,
                )
                .execution_options(synchronize_session=False)
            )
            if result.rowcount != 1:
                await db.rollback()
                return None
            await db.commit()
            worker = await db.get(
                Worker,
                destroy_claim.worker_id,
                populate_existing=True,
            )
        if worker is not None:
            await self._broadcast(worker)
        return worker

    async def _commit_destroy_effect_success(
        self,
        destroy_claim,
        *,
        receipt_digest: str,
        accounts: list | None,
    ) -> Worker | None:
        """Monotonically terminalize only the exact nonce/receipt identity."""

        async with self.db_factory() as db:
            result = await db.execute(
                update(Worker)
                .where(
                    *self._destroy_claim_identity_predicates(destroy_claim),
                    or_(
                        Worker.status == "destroying",
                        and_(
                            Worker.status == "error",
                            Worker.bootstrap_step == "destroy",
                        ),
                    ),
                    self._destroy_receipt_digest_predicate(receipt_digest),
                )
                .values(
                    status="terminated",
                    bootstrap_step=None,
                    bootstrap_error=None,
                    auth_token=None,
                    accounts=_scrub_destroyed_worker_accounts(accounts),
                    destroy_lifecycle_nonce=None,
                    destroy_termination_receipt=None,
                )
                .execution_options(synchronize_session=False)
            )
            if result.rowcount != 1:
                await db.rollback()
                current = await db.get(Worker, destroy_claim.worker_id)
                if current is not None and current.status == "terminated":
                    return current
                raise RuntimeError(
                    "Cloud termination succeeded but the exact Worker receipt "
                    "could not be terminalized"
                )
            await db.commit()
            worker = await db.get(
                Worker,
                destroy_claim.worker_id,
                populate_existing=True,
            )
        if worker is not None:
            await self._broadcast(worker)
        return worker

    async def destroy_worker(self, worker_id: int, *, destroy_claim):
        require_worker_control_plane_enabled()
        if destroy_claim.worker_id != worker_id:
            raise ValueError("Worker destroy claim does not match worker_id")
        async with self._lifecycle_lock(worker_id):
            await self._destroy_worker_locked(
                worker_id,
                destroy_claim=destroy_claim,
            )

    async def _destroy_worker_locked(self, worker_id: int, *, destroy_claim):
        """Terminate only an exact durable destroy claim and receipt."""

        receipt_digest: str | None = None
        try:
            cloud_scope = await self._current_cloud_scope()
            worker, receipt_digest = await self._load_destroy_effect_authority(
                destroy_claim,
                cloud_scope=cloud_scope,
            )
        except FileExistsError:
            return
        except Exception as exc:
            logger.warning("worker %s destroy authority: %s", worker_id, exc)
            await self._mark_destroy_effect_error(
                destroy_claim,
                detail=f"销毁失败: {exc}",
                receipt_digest=receipt_digest,
            )
            return

        if self.relay is not None:
            try:
                await self.relay.stop_worker(worker_id)
            except Exception as exc:
                # Relay cleanup is best-effort and must not strand a billable
                # instance.  Terminating EC2 also makes the relay unusable.
                logger.warning(
                    "worker %s destroy: relay stop failed: %s",
                    worker_id,
                    exc,
                )

        try:
            # The relay await above is an intentional scheduling point.  Repeat
            # both live STS scope and DB receipt CAS immediately before the
            # irreversible provider call.
            cloud_scope = await self._current_cloud_scope()
            worker, receipt_digest = await self._load_destroy_effect_authority(
                destroy_claim,
                cloud_scope=cloud_scope,
            )
            await self.cloud.terminate_instance(
                worker.cloud_instance_id,
                allow_not_found=True,
            )
        except FileExistsError:
            return
        except asyncio.CancelledError:
            # A cancellation may hide an accepted cloud response.  Preserve the
            # destroying state and exact outbox for restart replay.
            raise
        except Exception as exc:
            logger.warning("worker %s destroy: %s", worker_id, exc)
            await self._mark_destroy_effect_error(
                destroy_claim,
                detail=f"销毁失败: {exc}",
                receipt_digest=receipt_digest,
            )
            return

        # The provider confirmed termination—or absence in the already matched
        # exact scope.  A late failure coordinator cannot overwrite this CAS.
        await self._commit_destroy_effect_success(
            destroy_claim,
            receipt_digest=receipt_digest,
            accounts=worker.accounts,
        )

    # ------------------------------------------------------------------
    # 健康监控（lifespan 起一个循环）
    # ------------------------------------------------------------------

    async def health_check_loop(self, interval: int = 30):
        if not worker_control_plane_enabled():
            logger.warning(
                "Worker health checks disabled: control plane requires "
                "CCM_NODE_ROLE=manager and a non-empty AUTH_TOKEN"
            )
            return
        fail_counts: dict[int, int] = {}
        while True:
            try:
                await self.recover_worker_rename_tag_outboxes()
                await self._health_check_once(fail_counts)
            except Exception:
                logger.exception("worker health check loop error")
            await asyncio.sleep(interval)

    async def _health_check_once(self, fail_counts: dict[int, int]):
        require_worker_control_plane_enabled()
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
            await self._probe_auth(worker, client)
            self._require_expected_commit(worker, body)
            fail_counts.pop(worker.id, None)
            fields = {"last_heartbeat": datetime.utcnow()}
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
                    .where(
                        Worker.id == worker.id,
                        Worker.status == "ready",
                        Worker.bootstrap_step.is_(None),
                    )
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
