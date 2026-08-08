"""Fail-closed runtime contract for untrusted Test Harness environments.

The existing shared-project ``ContainerManager`` is deliberately not reused as
the security boundary for PR/ref reviews: it bind-mounts a host checkout and
may mount account or Git credentials.  Harness sandboxes are ephemeral,
credential-free resources whose availability must be proven before an
untrusted target can be admitted.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import secrets
import shutil
import signal
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import PurePosixPath
from typing import Awaitable, Callable
from urllib.parse import urlsplit

import httpx
from sqlalchemy import select

from backend.database import async_session
from backend.models.test_harness import TestHarnessRun, TestHarnessSandboxLease
from backend.services.test_harness_egress_proxy import normalize_allowed_hosts
from backend.services.test_harness_git_targets import ResolvedGitTarget

from backend.config import settings


_DOCKER_IMAGE_ID_RE = re.compile(r"sha256:[0-9a-f]{64}\Z")
_GIT_SHA_RE = re.compile(r"[0-9a-f]{40}\Z")
_HEX_ID_RE = re.compile(r"[0-9a-f]{32}\Z")
_LEASE_NONCE_RE = re.compile(r"[0-9a-f]{48}\Z")
_CONTAINER_ID_RE = re.compile(r"[0-9a-f]{12,64}\Z")
_MEMORY_RE = re.compile(r"[1-9][0-9]*(?:[kKmMgG])?\Z")
_OWNER_LABEL = "com.ccm.owner"
_RUN_LABEL = "com.ccm.harness.run-id"
_LEASE_LABEL = "com.ccm.harness.lease-id"
_NONCE_LABEL = "com.ccm.harness.lease-nonce"
_ROLE_LABEL = "com.ccm.harness.role"
_OWNER_VALUE = "test-harness"
_DEFAULT_GIT_EGRESS_HOSTS = frozenset(
    {
        "github.com",
        "api.github.com",
        "codeload.github.com",
        "objects.githubusercontent.com",
    }
)
_MAX_RUNTIME_OUTPUT_BYTES = 8 * 1024 * 1024
_SANDBOX_PLACEHOLDERS = frozenset(
    {"workspace", "preview_port", "temp_dir", "temp_db", "python"}
)
_SANDBOX_ENV_KEY_RE = re.compile(r"[A-Z_][A-Z0-9_]{0,79}\Z")
_SANDBOX_SENSITIVE_ENV = frozenset(
    {
        "ANTHROPIC_API_KEY",
        "OPENAI_API_KEY",
        "CLAUDE_CODE_OAUTH_TOKEN",
        "CODEX_API_KEY",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "GITHUB_TOKEN",
        "GH_TOKEN",
    }
)
SandboxCommandRunner = Callable[[list[str], float], Awaitable[tuple[int, str]]]


@dataclass(frozen=True, slots=True)
class SandboxCapability:
    """One immutable capability observation returned to admission callers."""

    available: bool
    backend: str
    reason: str | None
    image: str | None = None
    image_id: str | None = None
    runtime_version: str | None = None
    public_repositories_only: bool = True

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


class TestHarnessSandboxError(RuntimeError):
    """The sandbox runtime could not safely satisfy an operation."""


class TestHarnessSandboxRuntime:
    """Provider-neutral interface used by PR/ref target admission."""

    async def probe(self, *, force: bool = False) -> SandboxCapability:
        raise NotImplementedError

    async def provision(
        self,
        *,
        run_id: str,
        lease_id: str,
        lease_nonce: str,
    ) -> "SandboxResource":
        raise NotImplementedError

    async def cleanup_identity(
        self,
        *,
        run_id: str,
        lease_id: str,
        lease_nonce: str,
    ) -> int:
        raise NotImplementedError

    async def acquire_source(
        self,
        *,
        run_id: str,
        lease_id: str,
        lease_nonce: str,
        resource_id: str,
        resource_name: str,
        target: ResolvedGitTarget,
        additional_allowed_hosts: tuple[str, ...] = (),
    ) -> "SandboxSourceSnapshot":
        raise NotImplementedError

    async def prepare_preview(
        self,
        *,
        run_id: str,
        lease_id: str,
        lease_nonce: str,
        resource_id: str,
        source: "SandboxSourceSnapshot",
        preview_config: dict[str, object],
        startup_timeout_seconds: float,
        url_template: str,
        health_url_template: str,
    ) -> "SandboxPreviewSnapshot":
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class SandboxResource:
    backend: str
    resource_id: str
    resource_name: str
    image_ref: str
    image_digest: str
    metadata: dict[str, object]


@dataclass(frozen=True, slots=True)
class SandboxSourceSnapshot:
    repository_path: str
    head_sha: str
    internal_network_id: str
    egress_network_id: str
    proxy_container_id: str
    allowed_hosts: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SandboxPreviewSnapshot:
    url: str
    health_url: str
    host_port: int
    internal_port: int
    process_names: tuple[str, ...]
    setup_logs: tuple[dict[str, object], ...]
    relay_container_id: str | None = None


async def _terminate_process(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        await process.wait()
        return
    try:
        if os.name == "posix" and type(process.pid) is int and process.pid > 1:
            os.killpg(process.pid, signal.SIGTERM)
        else:
            process.terminate()
    except ProcessLookupError:
        pass
    try:
        await asyncio.wait_for(process.wait(), timeout=2)
        return
    except asyncio.TimeoutError:
        pass
    try:
        if os.name == "posix" and type(process.pid) is int and process.pid > 1:
            os.killpg(process.pid, signal.SIGKILL)
        else:
            process.kill()
    except ProcessLookupError:
        pass
    await process.wait()


async def _run_command(argv: list[str], timeout: float) -> tuple[int, str]:
    """Run one Docker command with bounded output and exact process cleanup."""

    process = await asyncio.create_subprocess_exec(
        *argv,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        start_new_session=(os.name == "posix"),
    )
    if process.stdout is None:  # pragma: no cover - PIPE above is authoritative.
        await _terminate_process(process)
        raise TestHarnessSandboxError("sandbox runtime output pipe is unavailable")

    async def read_bounded() -> bytes:
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = await process.stdout.read(64 * 1024)
            if not chunk:
                return b"".join(chunks)
            total += len(chunk)
            if total > _MAX_RUNTIME_OUTPUT_BYTES:
                raise TestHarnessSandboxError(
                    "sandbox runtime command output exceeded its limit"
                )
            chunks.append(chunk)

    collect = asyncio.create_task(read_bounded())
    try:
        stdout = await asyncio.wait_for(asyncio.shield(collect), timeout)
        await process.wait()
    except BaseException:
        await asyncio.shield(_terminate_process(process))
        if not collect.done():
            collect.cancel()
        await asyncio.gather(collect, return_exceptions=True)
        raise
    return process.returncode or 0, (stdout or b"").decode(
        "utf-8", errors="replace"
    )


def _format_sandbox_preview_value(
    value: object,
    variables: dict[str, str],
) -> str:
    if not isinstance(value, str) or len(value) > 4000 or "\x00" in value:
        raise TestHarnessSandboxError("sandbox preview value is invalid")
    result = value
    for key, replacement in variables.items():
        result = result.replace("{" + key + "}", replacement)
    unknown = re.findall(r"\{([a-zA-Z0-9_]+)\}", result)
    if unknown:
        if unknown[0] not in _SANDBOX_PLACEHOLDERS:
            raise TestHarnessSandboxError(
                f"sandbox preview placeholder is invalid: {unknown[0]}"
            )
        raise TestHarnessSandboxError(
            f"sandbox preview placeholder was not resolved: {unknown[0]}"
        )
    return result


def _sandbox_repo_workdir(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise TestHarnessSandboxError("sandbox preview cwd is invalid")
    relative = PurePosixPath(value)
    if relative.is_absolute() or ".." in relative.parts:
        raise TestHarnessSandboxError("sandbox preview cwd escapes the repository")
    return str(PurePosixPath("/workspace/repo") / relative)


def _sandbox_preview_env(
    value: object,
    variables: dict[str, str],
    *,
    with_proxy: bool,
) -> dict[str, str]:
    if not isinstance(value, dict) or len(value) > 64:
        raise TestHarnessSandboxError("sandbox preview env is invalid")
    env = {
        "HOME": "/home/sandbox",
        "CCM_PREVIEW": "1",
        "PYTHONUNBUFFERED": "1",
        "NO_COLOR": "1",
        "UV_CACHE_DIR": "/workspace/.cache/uv",
        "npm_config_cache": "/workspace/.cache/npm",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_ASKPASS": "/bin/false",
    }
    if with_proxy:
        env.update(
            {
                "HTTPS_PROXY": "http://egress-proxy:3128",
                "HTTP_PROXY": "http://egress-proxy:3128",
                "ALL_PROXY": "http://egress-proxy:3128",
                "NO_PROXY": "localhost,127.0.0.1",
            }
        )
    for key, raw in value.items():
        if (
            not isinstance(key, str)
            or _SANDBOX_ENV_KEY_RE.fullmatch(key) is None
            or key in _SANDBOX_SENSITIVE_ENV
        ):
            raise TestHarnessSandboxError("sandbox preview env key is invalid")
        env[key] = _format_sandbox_preview_value(raw, variables)
    return env


class DockerTestHarnessSandboxRuntime(TestHarnessSandboxRuntime):
    """Probe the administrator-owned Docker runtime and pinned local image."""

    def __init__(
        self,
        *,
        enabled: bool | None = None,
        docker_binary: str | None = None,
        image: str | None = None,
        runner: SandboxCommandRunner | None = None,
        probe_ttl_seconds: float = 15.0,
        memory: str | None = None,
        cpus: float | None = None,
        pids_limit: int | None = None,
        workspace_bytes: int | None = None,
        tmp_bytes: int | None = None,
    ) -> None:
        self.enabled = (
            settings.test_harness_sandbox_enabled if enabled is None else enabled
        )
        self.docker_binary = (
            docker_binary or settings.test_harness_sandbox_docker_binary
        )
        self.image = image or settings.test_harness_sandbox_image
        self._runner = runner or _run_command
        self.memory = memory or settings.test_harness_sandbox_memory
        self.cpus = float(
            settings.test_harness_sandbox_cpus if cpus is None else cpus
        )
        self.pids_limit = int(
            settings.test_harness_sandbox_pids_limit
            if pids_limit is None
            else pids_limit
        )
        self.workspace_bytes = int(
            settings.test_harness_sandbox_workspace_bytes
            if workspace_bytes is None
            else workspace_bytes
        )
        self.tmp_bytes = int(
            settings.test_harness_sandbox_tmp_bytes
            if tmp_bytes is None
            else tmp_bytes
        )
        self._probe_ttl_seconds = max(0.0, probe_ttl_seconds)
        self._probe_lock = asyncio.Lock()
        self._cached: tuple[float, SandboxCapability] | None = None

    async def probe(self, *, force: bool = False) -> SandboxCapability:
        loop = asyncio.get_running_loop()
        now = loop.time()
        cached = self._cached
        if (
            not force
            and cached is not None
            and now - cached[0] < self._probe_ttl_seconds
        ):
            return cached[1]
        async with self._probe_lock:
            now = loop.time()
            cached = self._cached
            if (
                not force
                and cached is not None
                and now - cached[0] < self._probe_ttl_seconds
            ):
                return cached[1]
            capability = await self._probe_uncached()
            self._cached = (loop.time(), capability)
            return capability

    async def _probe_uncached(self) -> SandboxCapability:
        if not self.enabled:
            return SandboxCapability(
                available=False,
                backend="docker",
                reason=(
                    "PR/ref isolated sandbox runtime is disabled by "
                    "administrator configuration"
                ),
                image=self.image,
            )
        try:
            self._validate_security_limits()
        except TestHarnessSandboxError as exc:
            return SandboxCapability(
                available=False,
                backend="docker",
                reason=f"PR/ref isolated sandbox configuration is invalid: {exc}",
                image=self.image,
            )
        binary = shutil.which(self.docker_binary)
        if binary is None:
            return SandboxCapability(
                available=False,
                backend="docker",
                reason="PR/ref isolated sandbox Docker client is unavailable",
                image=self.image,
            )
        try:
            version_code, version_output = await self._runner(
                [binary, "version", "--format", "{{.Server.Version}}"],
                5.0,
            )
        except (OSError, asyncio.TimeoutError) as exc:
            return SandboxCapability(
                available=False,
                backend="docker",
                reason=(
                    "PR/ref isolated sandbox Docker daemon probe failed: "
                    f"{type(exc).__name__}"
                ),
                image=self.image,
            )
        version = version_output.strip()
        if version_code != 0 or not version or len(version) > 100:
            return SandboxCapability(
                available=False,
                backend="docker",
                reason="PR/ref isolated sandbox Docker daemon is unavailable",
                image=self.image,
            )
        try:
            image_code, image_output = await self._runner(
                [binary, "image", "inspect", "--format", "{{.Id}}", self.image],
                5.0,
            )
        except (OSError, asyncio.TimeoutError) as exc:
            return SandboxCapability(
                available=False,
                backend="docker",
                reason=(
                    "PR/ref isolated sandbox image probe failed: "
                    f"{type(exc).__name__}"
                ),
                image=self.image,
                runtime_version=version,
            )
        image_id = image_output.strip().lower()
        if image_code != 0 or _DOCKER_IMAGE_ID_RE.fullmatch(image_id) is None:
            return SandboxCapability(
                available=False,
                backend="docker",
                reason=(
                    "PR/ref isolated sandbox image is missing or has an "
                    "invalid identity"
                ),
                image=self.image,
                runtime_version=version,
            )
        return SandboxCapability(
            available=True,
            backend="docker",
            reason=None,
            image=self.image,
            image_id=image_id,
            runtime_version=version,
        )

    def _validate_security_limits(self) -> None:
        if _MEMORY_RE.fullmatch(self.memory) is None:
            raise TestHarnessSandboxError("sandbox memory limit is invalid")
        if not 0.1 <= self.cpus <= 32:
            raise TestHarnessSandboxError("sandbox CPU limit is invalid")
        if not 16 <= self.pids_limit <= 4096:
            raise TestHarnessSandboxError("sandbox PID limit is invalid")
        if not 128 * 1024 * 1024 <= self.workspace_bytes <= 16 * 1024**3:
            raise TestHarnessSandboxError("sandbox workspace limit is invalid")
        if not 64 * 1024 * 1024 <= self.tmp_bytes <= 4 * 1024**3:
            raise TestHarnessSandboxError("sandbox tmpfs limit is invalid")
        if not 1024 <= settings.test_harness_sandbox_preview_port <= 65535:
            raise TestHarnessSandboxError("sandbox preview port is invalid")
        if not 1024 * 1024 <= settings.test_harness_sandbox_proxy_max_bytes <= 4 * 1024**3:
            raise TestHarnessSandboxError("sandbox proxy byte limit is invalid")

    @staticmethod
    def _validate_identity(run_id: str, lease_id: str, lease_nonce: str) -> None:
        if _HEX_ID_RE.fullmatch(run_id) is None:
            raise TestHarnessSandboxError("sandbox run identity is invalid")
        if _HEX_ID_RE.fullmatch(lease_id) is None:
            raise TestHarnessSandboxError("sandbox lease identity is invalid")
        if _LEASE_NONCE_RE.fullmatch(lease_nonce) is None:
            raise TestHarnessSandboxError("sandbox lease nonce is invalid")

    @staticmethod
    def _resource_name(run_id: str, lease_nonce: str) -> str:
        return f"ccm-harness-{run_id[:16]}-{lease_nonce[:8]}"

    def _labels(
        self,
        run_id: str,
        lease_id: str,
        lease_nonce: str,
        *,
        role: str,
    ) -> list[str]:
        if not re.fullmatch(r"[a-z][a-z0-9-]{0,31}", role):
            raise TestHarnessSandboxError("sandbox resource role is invalid")
        return [
            "--label",
            f"{_OWNER_LABEL}={_OWNER_VALUE}",
            "--label",
            f"{_RUN_LABEL}={run_id}",
            "--label",
            f"{_LEASE_LABEL}={lease_id}",
            "--label",
            f"{_NONCE_LABEL}={lease_nonce}",
            "--label",
            f"{_ROLE_LABEL}={role}",
        ]

    async def provision(
        self,
        *,
        run_id: str,
        lease_id: str,
        lease_nonce: str,
    ) -> SandboxResource:
        self._validate_identity(run_id, lease_id, lease_nonce)
        self._validate_security_limits()
        capability = await self.probe()
        if not capability.available or capability.image_id is None:
            raise TestHarnessSandboxError(
                capability.reason or "isolated sandbox runtime is unavailable"
            )
        binary = shutil.which(self.docker_binary)
        if binary is None:  # pragma: no cover - probe is the authority.
            raise TestHarnessSandboxError("isolated sandbox Docker client disappeared")
        name = self._resource_name(run_id, lease_nonce)
        try:
            internal_id = await self._create_network(
                binary=binary,
                name=f"{name}-int",
                run_id=run_id,
                lease_id=lease_id,
                lease_nonce=lease_nonce,
                role="internal-network",
                internal=True,
            )
            create_args = [
                binary,
                "create",
                "--name",
                name,
                *self._labels(run_id, lease_id, lease_nonce, role="source"),
                "--init",
                "--user",
                "10001:10001",
                "--cap-drop",
                "ALL",
                "--security-opt",
                "no-new-privileges:true",
                "--read-only",
                "--network",
                internal_id,
                "--network-alias",
                "source",
                "--pids-limit",
                str(self.pids_limit),
                "--memory",
                self.memory,
                "--cpus",
                str(self.cpus),
                "--stop-timeout",
                "5",
                "--tmpfs",
                (
                    "/workspace:rw,nosuid,nodev,exec,mode=1777,"
                    f"size={self.workspace_bytes}"
                ),
                "--tmpfs",
                f"/tmp:rw,noexec,nosuid,nodev,size={self.tmp_bytes}",
                "--tmpfs",
                (
                    "/home/sandbox:rw,noexec,nosuid,nodev,"
                    f"uid=10001,gid=10001,mode=0700,size={self.tmp_bytes}"
                ),
                "--tmpfs",
                "/run:rw,noexec,nosuid,nodev,mode=1777,size=67108864",
                "--workdir",
                "/workspace",
                "--entrypoint",
                "/usr/bin/tail",
                capability.image_id,
                "-f",
                "/dev/null",
            ]
            code, output = await self._runner(create_args, 30.0)
            container_id = output.strip().lower()
            if code != 0 or _CONTAINER_ID_RE.fullmatch(container_id) is None:
                raise TestHarnessSandboxError(
                    "isolated sandbox container creation failed"
                )
            start_code, _ = await self._runner([binary, "start", container_id], 30.0)
            if start_code != 0:
                raise TestHarnessSandboxError("isolated sandbox container start failed")
            await self._verify_resource(
                binary=binary,
                resource_id=container_id,
                run_id=run_id,
                lease_id=lease_id,
                lease_nonce=lease_nonce,
                expected_role="source",
                require_running=True,
            )
            await self._verify_source_internal_only(
                binary=binary,
                resource_id=container_id,
                internal_network_id=internal_id,
            )
            return SandboxResource(
                backend="docker",
                resource_id=container_id,
                resource_name=name,
                image_ref=self.image,
                image_digest=capability.image_id,
                metadata={
                    "network_mode": "internal-only",
                    "internal_network_id": internal_id,
                    "read_only_root": True,
                    "host_mounts": 0,
                    "credential_mounts": 0,
                    "workspace_bytes": self.workspace_bytes,
                    "tmp_bytes": self.tmp_bytes,
                    "memory": self.memory,
                    "cpus": self.cpus,
                    "pids_limit": self.pids_limit,
                },
            )
        except BaseException:
            try:
                await asyncio.shield(
                    self.cleanup_identity(
                        run_id=run_id,
                        lease_id=lease_id,
                        lease_nonce=lease_nonce,
                    )
                )
            except BaseException as cleanup_exc:
                raise TestHarnessSandboxError(
                    "sandbox provisioning failed and cleanup could not be proven"
                ) from cleanup_exc
            raise

    async def _verify_resource(
        self,
        *,
        binary: str,
        resource_id: str,
        run_id: str,
        lease_id: str,
        lease_nonce: str,
        expected_role: str | None,
        require_running: bool,
    ) -> None:
        if _CONTAINER_ID_RE.fullmatch(resource_id) is None:
            raise TestHarnessSandboxError("sandbox container identity is invalid")
        template = (
            '{{.Id}}\t{{index .Config.Labels "' + _OWNER_LABEL + '"}}\t'
            '{{index .Config.Labels "' + _RUN_LABEL + '"}}\t'
            '{{index .Config.Labels "' + _LEASE_LABEL + '"}}\t'
            '{{index .Config.Labels "' + _NONCE_LABEL + '"}}\t'
            '{{index .Config.Labels "' + _ROLE_LABEL + '"}}\t'
            "{{.State.Running}}\t{{.HostConfig.ReadonlyRootfs}}\t"
            "{{.HostConfig.NetworkMode}}"
        )
        code, output = await self._runner(
            [binary, "inspect", "--format", template, resource_id],
            10.0,
        )
        fields = output.strip().split("\t")
        expected = [
            resource_id,
            _OWNER_VALUE,
            run_id,
            lease_id,
            lease_nonce,
            expected_role if expected_role is not None else fields[5] if len(fields) > 5 else "",
            "true" if require_running else fields[6] if len(fields) > 6 else "",
            "true",
            fields[8] if len(fields) > 8 else "",
        ]
        if code != 0 or len(fields) != len(expected) or fields != expected:
            raise TestHarnessSandboxError(
                "sandbox container identity or security profile could not be proven"
            )

    async def _verify_network(
        self,
        *,
        binary: str,
        network_id: str,
        run_id: str,
        lease_id: str,
        lease_nonce: str,
    ) -> str:
        if _CONTAINER_ID_RE.fullmatch(network_id) is None:
            raise TestHarnessSandboxError("sandbox network identity is invalid")
        template = (
            '{{.Id}}\t{{index .Labels "' + _OWNER_LABEL + '"}}\t'
            '{{index .Labels "' + _RUN_LABEL + '"}}\t'
            '{{index .Labels "' + _LEASE_LABEL + '"}}\t'
            '{{index .Labels "' + _NONCE_LABEL + '"}}\t'
            '{{index .Labels "' + _ROLE_LABEL + '"}}\t{{.Internal}}'
        )
        code, output = await self._runner(
            [binary, "network", "inspect", "--format", template, network_id],
            10.0,
        )
        fields = output.strip().split("\t")
        if code != 0 or len(fields) != 7:
            raise TestHarnessSandboxError("sandbox network identity could not be proven")
        role = fields[5]
        if role not in {"internal-network", "egress-network"}:
            raise TestHarnessSandboxError("sandbox network role is invalid")
        expected_internal = "true" if role == "internal-network" else "false"
        if fields != [
            network_id,
            _OWNER_VALUE,
            run_id,
            lease_id,
            lease_nonce,
            role,
            expected_internal,
        ]:
            raise TestHarnessSandboxError("sandbox network identity could not be proven")
        return role

    async def _create_network(
        self,
        *,
        binary: str,
        name: str,
        run_id: str,
        lease_id: str,
        lease_nonce: str,
        role: str,
        internal: bool,
    ) -> str:
        argv = [
            binary,
            "network",
            "create",
            "--driver",
            "bridge",
            *self._labels(
                run_id,
                lease_id,
                lease_nonce,
                role=role,
            ),
        ]
        if internal:
            argv.append("--internal")
        argv.append(name)
        code, output = await self._runner(argv, 30.0)
        network_id = output.strip().lower()
        if code != 0 or _CONTAINER_ID_RE.fullmatch(network_id) is None:
            raise TestHarnessSandboxError("sandbox network creation failed")
        observed_role = await self._verify_network(
            binary=binary,
            network_id=network_id,
            run_id=run_id,
            lease_id=lease_id,
            lease_nonce=lease_nonce,
        )
        if observed_role != role:
            raise TestHarnessSandboxError("sandbox network role mismatch")
        return network_id

    async def _find_network(
        self,
        *,
        binary: str,
        run_id: str,
        lease_id: str,
        lease_nonce: str,
        role: str,
    ) -> str:
        code, output = await self._runner(
            [
                binary,
                "network",
                "ls",
                "-q",
                "--no-trunc",
                "--filter",
                f"label={_OWNER_LABEL}={_OWNER_VALUE}",
                "--filter",
                f"label={_RUN_LABEL}={run_id}",
                "--filter",
                f"label={_LEASE_LABEL}={lease_id}",
                "--filter",
                f"label={_NONCE_LABEL}={lease_nonce}",
                "--filter",
                f"label={_ROLE_LABEL}={role}",
            ],
            10.0,
        )
        network_ids = [line.strip().lower() for line in output.splitlines() if line.strip()]
        if code != 0 or len(network_ids) != 1:
            raise TestHarnessSandboxError(
                f"sandbox {role} identity could not be resolved"
            )
        network_id = network_ids[0]
        observed_role = await self._verify_network(
            binary=binary,
            network_id=network_id,
            run_id=run_id,
            lease_id=lease_id,
            lease_nonce=lease_nonce,
        )
        if observed_role != role:
            raise TestHarnessSandboxError("sandbox network role mismatch")
        return network_id

    async def _verify_source_internal_only(
        self,
        *,
        binary: str,
        resource_id: str,
        internal_network_id: str,
    ) -> None:
        template = "{{json .NetworkSettings.Networks}}"
        inspect_code, inspect_output = await self._runner(
            [binary, "inspect", "--format", template, resource_id],
            10.0,
        )
        try:
            networks = json.loads(inspect_output)
        except (TypeError, json.JSONDecodeError) as exc:
            raise TestHarnessSandboxError(
                "sandbox source network state is invalid"
            ) from exc
        if inspect_code != 0 or not isinstance(networks, dict) or not networks:
            raise TestHarnessSandboxError(
                "sandbox source network state could not be proven"
            )
        network_ids = {
            item.get("NetworkID")
            for item in networks.values()
            if isinstance(item, dict)
        }
        if network_ids != {internal_network_id}:
            raise TestHarnessSandboxError(
                "sandbox source retained an unexpected network attachment"
            )

    async def _exec_source(
        self,
        *,
        binary: str,
        resource_id: str,
        env: dict[str, str],
        argv: list[str],
        timeout: float,
        workdir: str = "/workspace",
    ) -> str:
        parsed_workdir = PurePosixPath(workdir)
        if (
            not parsed_workdir.is_absolute()
            or (
                parsed_workdir != PurePosixPath("/workspace")
                and parsed_workdir.parts[:3] != ("/", "workspace", "repo")
            )
            or ".." in parsed_workdir.parts
        ):
            raise TestHarnessSandboxError("sandbox command workdir is invalid")
        command = [
            binary,
            "exec",
            "-i",
            "--user",
            "10001:10001",
            "--workdir",
            str(parsed_workdir),
        ]
        for key, value in env.items():
            if (
                re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,79}", key) is None
                or not isinstance(value, str)
                or "\x00" in value
            ):
                raise TestHarnessSandboxError("sandbox command environment is invalid")
            command.extend(["--env", f"{key}={value}"])
        command.extend([resource_id, *argv])
        code, output = await self._runner(command, timeout)
        if code != 0:
            # Commands execute untrusted repository content without host
            # credentials.  Preserve a small, printable-only output tail so an
            # administrator can distinguish dependency, proxy, disk and build
            # failures without allowing terminal control sequences or unbounded
            # attacker-controlled logs into Harness events.
            safe_tail = re.sub(
                r"[^\x20-\x7e\n\r\t]",
                "?",
                output[-2000:],
            ).strip()
            diagnostic = f": {safe_tail}" if safe_tail else ""
            raise TestHarnessSandboxError(
                f"sandbox source command failed: {argv[0]}{diagnostic}"
            )
        if len(output.encode("utf-8", errors="replace")) > 4 * 1024 * 1024:
            raise TestHarnessSandboxError("sandbox source command output is too large")
        return output.strip()

    async def acquire_source(
        self,
        *,
        run_id: str,
        lease_id: str,
        lease_nonce: str,
        resource_id: str,
        resource_name: str,
        target: ResolvedGitTarget,
        additional_allowed_hosts: tuple[str, ...] = (),
    ) -> SandboxSourceSnapshot:
        """Fetch and checkout one exact public target entirely inside Docker."""

        self._validate_identity(run_id, lease_id, lease_nonce)
        if _CONTAINER_ID_RE.fullmatch(resource_id) is None:
            raise TestHarnessSandboxError("sandbox source container is invalid")
        expected_name = self._resource_name(run_id, lease_nonce)
        if resource_name != expected_name:
            raise TestHarnessSandboxError("sandbox source container name is invalid")
        if _GIT_SHA_RE.fullmatch(target.head_sha) is None:
            raise TestHarnessSandboxError("sandbox source target SHA is invalid")
        capability = await self.probe()
        if not capability.available or capability.image_id is None:
            raise TestHarnessSandboxError(
                capability.reason or "isolated sandbox runtime is unavailable"
            )
        binary = shutil.which(self.docker_binary)
        if binary is None:
            raise TestHarnessSandboxError("isolated sandbox Docker client disappeared")
        await self._verify_resource(
            binary=binary,
            resource_id=resource_id,
            run_id=run_id,
            lease_id=lease_id,
            lease_nonce=lease_nonce,
            expected_role="source",
            require_running=True,
        )
        if any(not isinstance(host, str) for host in additional_allowed_hosts):
            raise TestHarnessSandboxError("sandbox egress allowlist is invalid")
        allowed = normalize_allowed_hosts(
            ",".join(sorted(_DEFAULT_GIT_EGRESS_HOSTS | set(additional_allowed_hosts)))
        )
        egress_name = f"{resource_name}-out"
        proxy_name = f"{resource_name}-proxy"
        try:
            internal_id = await self._find_network(
                binary=binary,
                run_id=run_id,
                lease_id=lease_id,
                lease_nonce=lease_nonce,
                role="internal-network",
            )
            await self._verify_source_internal_only(
                binary=binary,
                resource_id=resource_id,
                internal_network_id=internal_id,
            )
            egress_id = await self._create_network(
                binary=binary,
                name=egress_name,
                run_id=run_id,
                lease_id=lease_id,
                lease_nonce=lease_nonce,
                role="egress-network",
                internal=False,
            )
            proxy_args = [
                binary,
                "create",
                "--name",
                proxy_name,
                *self._labels(
                    run_id,
                    lease_id,
                    lease_nonce,
                    role="egress-proxy",
                ),
                "--init",
                "--user",
                "10001:10001",
                "--cap-drop",
                "ALL",
                "--security-opt",
                "no-new-privileges:true",
                "--read-only",
                "--network",
                egress_id,
                "--pids-limit",
                "64",
                "--memory",
                "256m",
                "--cpus",
                "0.5",
                "--tmpfs",
                "/tmp:rw,noexec,nosuid,nodev,size=67108864",
                "--env",
                f"CCM_ALLOWED_HOSTS={','.join(sorted(allowed))}",
                "--env",
                (
                    "CCM_PROXY_MAX_BYTES="
                    f"{settings.test_harness_sandbox_proxy_max_bytes}"
                ),
                "--entrypoint",
                "/usr/bin/python3",
                capability.image_id,
                "/opt/ccm/egress_proxy.py",
            ]
            proxy_code, proxy_output = await self._runner(proxy_args, 30.0)
            proxy_id = proxy_output.strip().lower()
            if proxy_code != 0 or _CONTAINER_ID_RE.fullmatch(proxy_id) is None:
                raise TestHarnessSandboxError("sandbox egress proxy creation failed")
            proxy_connect_code, _ = await self._runner(
                [
                    binary,
                    "network",
                    "connect",
                    "--alias",
                    "egress-proxy",
                    internal_id,
                    proxy_id,
                ],
                30.0,
            )
            if proxy_connect_code != 0:
                raise TestHarnessSandboxError(
                    "sandbox egress proxy could not join its internal relay network"
                )
            proxy_start_code, _ = await self._runner(
                [binary, "start", proxy_id],
                30.0,
            )
            if proxy_start_code != 0:
                raise TestHarnessSandboxError("sandbox egress proxy start failed")
            await self._verify_resource(
                binary=binary,
                resource_id=proxy_id,
                run_id=run_id,
                lease_id=lease_id,
                lease_nonce=lease_nonce,
                expected_role="egress-proxy",
                require_running=True,
            )
            proxy_env = {
                "HOME": "/home/sandbox",
                "GIT_CONFIG_NOSYSTEM": "1",
                "GIT_TERMINAL_PROMPT": "0",
                "GIT_ASKPASS": "/bin/false",
                "HTTPS_PROXY": "http://egress-proxy:3128",
                "HTTP_PROXY": "http://egress-proxy:3128",
                "ALL_PROXY": "http://egress-proxy:3128",
                "NO_PROXY": "localhost,127.0.0.1",
            }
            health_script = (
                "import socket,time,sys;"
                "ok=False;"
                "\nfor _ in range(50):\n"
                " try:\n"
                "  s=socket.create_connection(('egress-proxy',3128),.2);s.close();ok=True;break\n"
                " except OSError: time.sleep(.1)\n"
                "sys.exit(0 if ok else 1)"
            )
            await self._exec_source(
                binary=binary,
                resource_id=resource_id,
                env=proxy_env,
                argv=["/usr/bin/python3", "-c", health_script],
                timeout=10.0,
            )
            await self._exec_source(
                binary=binary,
                resource_id=resource_id,
                env=proxy_env,
                argv=[
                    "/usr/bin/git",
                    "init",
                    "--initial-branch=ccm-target",
                    "/workspace/repo",
                ],
                timeout=30.0,
            )
            git_prefix = [
                "/usr/bin/git",
                "-c",
                "core.hooksPath=/dev/null",
                "-c",
                "credential.helper=",
                "-c",
                "protocol.file.allow=never",
                "-C",
                "/workspace/repo",
            ]
            await self._exec_source(
                binary=binary,
                resource_id=resource_id,
                env=proxy_env,
                argv=[*git_prefix, "remote", "add", "origin", target.clone_url],
                timeout=30.0,
            )
            await self._exec_source(
                binary=binary,
                resource_id=resource_id,
                env=proxy_env,
                argv=[
                    *git_prefix,
                    "fetch",
                    "--no-tags",
                    "--depth=1",
                    "origin",
                    target.fetch_ref,
                ],
                timeout=600.0,
            )
            fetched = await self._exec_source(
                binary=binary,
                resource_id=resource_id,
                env=proxy_env,
                argv=[*git_prefix, "rev-parse", "FETCH_HEAD"],
                timeout=30.0,
            )
            if fetched.lower() != target.head_sha:
                raise TestHarnessSandboxError(
                    "sandbox fetched commit does not match the resolved target SHA"
                )
            await self._exec_source(
                binary=binary,
                resource_id=resource_id,
                env=proxy_env,
                argv=[
                    *git_prefix,
                    "checkout",
                    "--detach",
                    "--force",
                    target.head_sha,
                ],
                timeout=120.0,
            )
            observed = await self._exec_source(
                binary=binary,
                resource_id=resource_id,
                env=proxy_env,
                argv=[*git_prefix, "rev-parse", "HEAD"],
                timeout=30.0,
            )
            if observed.lower() != target.head_sha:
                raise TestHarnessSandboxError(
                    "sandbox checkout identity does not match the resolved target SHA"
                )
            return SandboxSourceSnapshot(
                repository_path="/workspace/repo",
                head_sha=target.head_sha,
                internal_network_id=internal_id,
                egress_network_id=egress_id,
                proxy_container_id=proxy_id,
                allowed_hosts=tuple(sorted(allowed)),
            )
        except BaseException:
            try:
                await asyncio.shield(
                    self.cleanup_identity(
                        run_id=run_id,
                        lease_id=lease_id,
                        lease_nonce=lease_nonce,
                    )
                )
            except BaseException as cleanup_exc:
                raise TestHarnessSandboxError(
                    "sandbox source acquisition failed and cleanup could not be proven"
                ) from cleanup_exc
            raise

    async def _revoke_source_egress(
        self,
        *,
        binary: str,
        run_id: str,
        lease_id: str,
        lease_nonce: str,
        resource_id: str,
        source: SandboxSourceSnapshot,
    ) -> None:
        """Remove the only outbound member, then prove source is internal-only."""

        await self._verify_resource(
            binary=binary,
            resource_id=source.proxy_container_id,
            run_id=run_id,
            lease_id=lease_id,
            lease_nonce=lease_nonce,
            expected_role="egress-proxy",
            require_running=True,
        )
        remove_code, _ = await self._runner(
            [binary, "rm", "-f", source.proxy_container_id],
            30.0,
        )
        if remove_code != 0:
            raise TestHarnessSandboxError("sandbox egress proxy removal failed")
        role = await self._verify_network(
            binary=binary,
            network_id=source.egress_network_id,
            run_id=run_id,
            lease_id=lease_id,
            lease_nonce=lease_nonce,
        )
        if role != "egress-network":
            raise TestHarnessSandboxError("sandbox egress network role mismatch")
        network_remove_code, _ = await self._runner(
            [binary, "network", "rm", source.egress_network_id],
            30.0,
        )
        if network_remove_code != 0:
            raise TestHarnessSandboxError("sandbox egress network removal failed")

        await self._verify_source_internal_only(
            binary=binary,
            resource_id=resource_id,
            internal_network_id=source.internal_network_id,
        )

    async def _published_preview_port(
        self,
        *,
        binary: str,
        resource_id: str,
    ) -> int:
        internal_port = settings.test_harness_sandbox_preview_port
        code, output = await self._runner(
            [binary, "port", resource_id, f"{internal_port}/tcp"],
            10.0,
        )
        lines = [line.strip() for line in output.splitlines() if line.strip()]
        if code != 0 or len(lines) != 1:
            raise TestHarnessSandboxError(
                "sandbox preview loopback port could not be resolved"
            )
        match = re.fullmatch(r"127\.0\.0\.1:([1-9][0-9]{0,4})", lines[0])
        if match is None:
            raise TestHarnessSandboxError(
                "sandbox preview was not published to exact IPv4 loopback"
            )
        port = int(match.group(1))
        if not 1024 <= port <= 65535:
            raise TestHarnessSandboxError("sandbox preview host port is invalid")
        return port

    async def _start_preview_relay(
        self,
        *,
        binary: str,
        run_id: str,
        lease_id: str,
        lease_nonce: str,
        internal_network_id: str,
    ) -> tuple[str, int]:
        capability = await self.probe()
        if not capability.available or capability.image_id is None:
            raise TestHarnessSandboxError(
                capability.reason or "isolated sandbox runtime is unavailable"
            )
        relay_name = f"{self._resource_name(run_id, lease_nonce)}-preview"
        create_args = [
            binary,
            "create",
            "--name",
            relay_name,
            *self._labels(
                run_id,
                lease_id,
                lease_nonce,
                role="preview-relay",
            ),
            "--init",
            "--user",
            "10001:10001",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges:true",
            "--read-only",
            "--network",
            "bridge",
            "--pids-limit",
            "64",
            "--memory",
            "128m",
            "--cpus",
            "0.25",
            "--publish",
            f"127.0.0.1::{settings.test_harness_sandbox_preview_port}",
            "--tmpfs",
            "/tmp:rw,noexec,nosuid,nodev,size=33554432",
            "--entrypoint",
            "/usr/bin/python3",
            capability.image_id,
            "/opt/ccm/preview_relay.py",
        ]
        code, output = await self._runner(create_args, 30.0)
        relay_id = output.strip().lower()
        if code != 0 or _CONTAINER_ID_RE.fullmatch(relay_id) is None:
            raise TestHarnessSandboxError("sandbox preview relay creation failed")
        connect_code, _ = await self._runner(
            [binary, "network", "connect", internal_network_id, relay_id],
            30.0,
        )
        if connect_code != 0:
            raise TestHarnessSandboxError(
                "sandbox preview relay could not join the internal network"
            )
        start_code, _ = await self._runner([binary, "start", relay_id], 30.0)
        if start_code != 0:
            raise TestHarnessSandboxError("sandbox preview relay could not start")
        await self._verify_resource(
            binary=binary,
            resource_id=relay_id,
            run_id=run_id,
            lease_id=lease_id,
            lease_nonce=lease_nonce,
            expected_role="preview-relay",
            require_running=True,
        )
        host_port = await self._published_preview_port(
            binary=binary,
            resource_id=relay_id,
        )
        return relay_id, host_port

    async def _preview_process_error(
        self,
        *,
        binary: str,
        resource_id: str,
        process_count: int,
    ) -> str | None:
        for index in range(process_count):
            status_path = f"/run/ccm-preview-{index}.status"
            code, output = await self._runner(
                [
                    binary,
                    "exec",
                    "--user",
                    "10001:10001",
                    resource_id,
                    "/usr/bin/cat",
                    status_path,
                ],
                5.0,
            )
            if code == 0:
                status = output.strip()
                if re.fullmatch(r"-?[0-9]{1,10}", status) is None:
                    status = "unknown"
                log_code, log_output = await self._runner(
                    [
                        binary,
                        "exec",
                        "--user",
                        "10001:10001",
                        resource_id,
                        "/usr/bin/tail",
                        "-c",
                        "8192",
                        f"/run/ccm-preview-{index}.log",
                    ],
                    5.0,
                )
                tail = log_output.strip()[-4000:] if log_code == 0 else ""
                return (
                    f"sandbox preview process {index} exited with code {status}"
                    + (f": {tail}" if tail else "")
                )
        return None

    async def prepare_preview(
        self,
        *,
        run_id: str,
        lease_id: str,
        lease_nonce: str,
        resource_id: str,
        source: SandboxSourceSnapshot,
        preview_config: dict[str, object],
        startup_timeout_seconds: float,
        url_template: str,
        health_url_template: str,
    ) -> SandboxPreviewSnapshot:
        """Run approved setup, revoke egress, and expose one loopback preview."""

        self._validate_identity(run_id, lease_id, lease_nonce)
        if _CONTAINER_ID_RE.fullmatch(resource_id) is None:
            raise TestHarnessSandboxError("sandbox source container is invalid")
        if not 3 <= startup_timeout_seconds <= 300:
            raise TestHarnessSandboxError("sandbox preview startup timeout is invalid")
        setup = preview_config.get("setup")
        processes = preview_config.get("processes")
        if (
            not isinstance(setup, list)
            or len(setup) > 6
            or not isinstance(processes, list)
            or not 1 <= len(processes) <= 4
        ):
            raise TestHarnessSandboxError("sandbox preview profile is invalid")
        binary = shutil.which(self.docker_binary)
        if binary is None:
            raise TestHarnessSandboxError("isolated sandbox Docker client disappeared")
        await self._verify_resource(
            binary=binary,
            resource_id=resource_id,
            run_id=run_id,
            lease_id=lease_id,
            lease_nonce=lease_nonce,
            expected_role="source",
            require_running=True,
        )
        internal_port = settings.test_harness_sandbox_preview_port
        variables = {
            "workspace": "/workspace/repo",
            "preview_port": str(internal_port),
            "temp_dir": "/workspace/runtime",
            "temp_db": "/workspace/runtime/preview.db",
            "python": "/workspace/repo/.venv/bin/python",
        }
        setup_logs: list[dict[str, object]] = []
        await self._exec_source(
            binary=binary,
            resource_id=resource_id,
            env={},
            argv=["/usr/bin/mkdir", "-p", "/workspace/runtime"],
            timeout=10.0,
            workdir="/workspace/repo",
        )
        try:
            for index, raw in enumerate(setup):
                if not isinstance(raw, dict):
                    raise TestHarnessSandboxError(
                        "sandbox preview setup entry is invalid"
                    )
                raw_argv = raw.get("command")
                if not isinstance(raw_argv, list) or not raw_argv:
                    raise TestHarnessSandboxError(
                        "sandbox preview setup command is invalid"
                    )
                argv = [
                    _format_sandbox_preview_value(item, variables)
                    for item in raw_argv
                ]
                cwd = _sandbox_repo_workdir(raw.get("cwd", "."))
                env = _sandbox_preview_env(
                    raw.get("env", {}),
                    variables,
                    with_proxy=True,
                )
                timeout = float(raw.get("timeout_seconds", 300))
                if not 1 <= timeout <= 1200:
                    raise TestHarnessSandboxError(
                        "sandbox preview setup timeout is invalid"
                    )
                output = await self._exec_source(
                    binary=binary,
                    resource_id=resource_id,
                    env=env,
                    argv=argv,
                    timeout=timeout,
                    workdir=cwd,
                )
                setup_logs.append(
                    {
                        "index": index,
                        "executable": PurePosixPath(argv[0]).name,
                        "cwd": str(raw.get("cwd", ".")),
                        "output_tail": output[-4000:],
                    }
                )

            process_names: list[str] = []
            for index, raw in enumerate(processes):
                if not isinstance(raw, dict):
                    raise TestHarnessSandboxError(
                        "sandbox preview process entry is invalid"
                    )
                name = raw.get("name")
                raw_argv = raw.get("command")
                if (
                    not isinstance(name, str)
                    or not name
                    or name in process_names
                    or not isinstance(raw_argv, list)
                    or not raw_argv
                ):
                    raise TestHarnessSandboxError(
                        "sandbox preview process contract is invalid"
                    )
                process_names.append(name)
                argv = [
                    _format_sandbox_preview_value(item, variables)
                    for item in raw_argv
                ]
                cwd = _sandbox_repo_workdir(raw.get("cwd", "."))
                env = _sandbox_preview_env(
                    raw.get("env", {}),
                    variables,
                    with_proxy=False,
                )
                command = [
                    binary,
                    "exec",
                    "-d",
                    "--user",
                    "10001:10001",
                    "--workdir",
                    cwd,
                ]
                for key, value in env.items():
                    command.extend(["--env", f"{key}={value}"])
                command.extend(
                    [
                        resource_id,
                        "/usr/bin/python3",
                        "/opt/ccm/process_wrapper.py",
                        f"/run/ccm-preview-{index}.pid",
                        f"/run/ccm-preview-{index}.status",
                        f"/run/ccm-preview-{index}.log",
                        "--",
                        *argv,
                    ]
                )
                code, _ = await self._runner(command, 30.0)
                if code != 0:
                    raise TestHarnessSandboxError(
                        f"sandbox preview process {index} could not start"
                    )

            await self._revoke_source_egress(
                binary=binary,
                run_id=run_id,
                lease_id=lease_id,
                lease_nonce=lease_nonce,
                resource_id=resource_id,
                source=source,
            )
            relay_container_id, host_port = await self._start_preview_relay(
                binary=binary,
                run_id=run_id,
                lease_id=lease_id,
                lease_nonce=lease_nonce,
                internal_network_id=source.internal_network_id,
            )
            host_variables = {**variables, "preview_port": str(host_port)}
            url = _format_sandbox_preview_value(url_template, host_variables)
            health_url = _format_sandbox_preview_value(
                health_url_template,
                host_variables,
            )
            for candidate in (url, health_url):
                parsed = urlsplit(candidate)
                if (
                    parsed.scheme != "http"
                    or parsed.hostname != "127.0.0.1"
                    or parsed.port != host_port
                    or parsed.username is not None
                    or parsed.password is not None
                ):
                    raise TestHarnessSandboxError(
                        "sandbox preview URL is not exact loopback"
                    )

            deadline = asyncio.get_running_loop().time() + startup_timeout_seconds
            last_error = "health check did not respond"
            async with httpx.AsyncClient(
                timeout=2.0,
                follow_redirects=False,
                trust_env=False,
            ) as client:
                while asyncio.get_running_loop().time() < deadline:
                    process_error = await self._preview_process_error(
                        binary=binary,
                        resource_id=resource_id,
                        process_count=len(processes),
                    )
                    if process_error:
                        raise TestHarnessSandboxError(process_error)
                    try:
                        response = await client.get(health_url)
                        if 200 <= response.status_code < 500:
                            return SandboxPreviewSnapshot(
                                url=url,
                                health_url=health_url,
                                host_port=host_port,
                                internal_port=internal_port,
                                process_names=tuple(process_names),
                                setup_logs=tuple(setup_logs),
                                relay_container_id=relay_container_id,
                            )
                        last_error = (
                            f"health check returned HTTP {response.status_code}"
                        )
                    except httpx.HTTPError as exc:
                        last_error = type(exc).__name__
                    await asyncio.sleep(0.5)
            raise TestHarnessSandboxError(
                f"sandbox preview did not become ready: {last_error}"
            )
        except BaseException:
            try:
                await asyncio.shield(
                    self.cleanup_identity(
                        run_id=run_id,
                        lease_id=lease_id,
                        lease_nonce=lease_nonce,
                    )
                )
            except BaseException as cleanup_exc:
                raise TestHarnessSandboxError(
                    "sandbox preview failed and cleanup could not be proven"
                ) from cleanup_exc
            raise

    async def cleanup_identity(
        self,
        *,
        run_id: str,
        lease_id: str,
        lease_nonce: str,
    ) -> int:
        self._validate_identity(run_id, lease_id, lease_nonce)
        binary = shutil.which(self.docker_binary)
        if binary is None:
            raise TestHarnessSandboxError(
                "Docker client unavailable; sandbox cleanup cannot be proven"
            )
        code, output = await self._runner(
            [
                binary,
                "ps",
                "-aq",
                "--no-trunc",
                "--filter",
                f"label={_OWNER_LABEL}={_OWNER_VALUE}",
                "--filter",
                f"label={_RUN_LABEL}={run_id}",
                "--filter",
                f"label={_LEASE_LABEL}={lease_id}",
                "--filter",
                f"label={_NONCE_LABEL}={lease_nonce}",
            ],
            10.0,
        )
        if code != 0:
            raise TestHarnessSandboxError("sandbox cleanup discovery failed")
        resources = [item.strip().lower() for item in output.splitlines() if item.strip()]
        if any(_CONTAINER_ID_RE.fullmatch(item) is None for item in resources):
            raise TestHarnessSandboxError("sandbox cleanup returned an invalid resource")
        for resource_id in resources:
            await self._verify_resource(
                binary=binary,
                resource_id=resource_id,
                run_id=run_id,
                lease_id=lease_id,
                lease_nonce=lease_nonce,
                expected_role=None,
                require_running=False,
            )
            remove_code, _ = await self._runner(
                [binary, "rm", "-f", resource_id],
                30.0,
            )
            if remove_code != 0:
                raise TestHarnessSandboxError("sandbox container removal failed")
        verify_code, verify_output = await self._runner(
            [
                binary,
                "ps",
                "-aq",
                "--no-trunc",
                "--filter",
                f"label={_OWNER_LABEL}={_OWNER_VALUE}",
                "--filter",
                f"label={_RUN_LABEL}={run_id}",
                "--filter",
                f"label={_LEASE_LABEL}={lease_id}",
                "--filter",
                f"label={_NONCE_LABEL}={lease_nonce}",
            ],
            10.0,
        )
        if verify_code != 0 or verify_output.strip():
            raise TestHarnessSandboxError("sandbox cleanup could not be proven")
        network_code, network_output = await self._runner(
            [
                binary,
                "network",
                "ls",
                "-q",
                "--no-trunc",
                "--filter",
                f"label={_OWNER_LABEL}={_OWNER_VALUE}",
                "--filter",
                f"label={_RUN_LABEL}={run_id}",
                "--filter",
                f"label={_LEASE_LABEL}={lease_id}",
                "--filter",
                f"label={_NONCE_LABEL}={lease_nonce}",
            ],
            10.0,
        )
        if network_code != 0:
            raise TestHarnessSandboxError("sandbox network cleanup discovery failed")
        networks = [
            item.strip().lower()
            for item in network_output.splitlines()
            if item.strip()
        ]
        if any(_CONTAINER_ID_RE.fullmatch(item) is None for item in networks):
            raise TestHarnessSandboxError("sandbox cleanup returned an invalid network")
        for network_id in networks:
            await self._verify_network(
                binary=binary,
                network_id=network_id,
                run_id=run_id,
                lease_id=lease_id,
                lease_nonce=lease_nonce,
            )
            remove_code, _ = await self._runner(
                [binary, "network", "rm", network_id],
                30.0,
            )
            if remove_code != 0:
                raise TestHarnessSandboxError("sandbox network removal failed")
        network_verify_code, network_verify_output = await self._runner(
            [
                binary,
                "network",
                "ls",
                "-q",
                "--no-trunc",
                "--filter",
                f"label={_OWNER_LABEL}={_OWNER_VALUE}",
                "--filter",
                f"label={_RUN_LABEL}={run_id}",
                "--filter",
                f"label={_LEASE_LABEL}={lease_id}",
                "--filter",
                f"label={_NONCE_LABEL}={lease_nonce}",
            ],
            10.0,
        )
        if network_verify_code != 0 or network_verify_output.strip():
            raise TestHarnessSandboxError("sandbox network cleanup could not be proven")
        return len(resources) + len(networks)


class TestHarnessSandboxManager:
    """Persist each external resource identity before and after Docker calls."""

    def __init__(
        self,
        *,
        runtime: TestHarnessSandboxRuntime | None = None,
        db_factory=async_session,
    ) -> None:
        self.runtime = runtime or test_harness_sandbox_runtime
        self.db_factory = db_factory
        self._lock = asyncio.Lock()

    async def provision(self, run_id: str) -> TestHarnessSandboxLease:
        if _HEX_ID_RE.fullmatch(run_id) is None:
            raise TestHarnessSandboxError("sandbox run identity is invalid")
        async with self._lock:
            capability = await self.runtime.probe()
            if not capability.available or not capability.image:
                raise TestHarnessSandboxError(
                    capability.reason or "isolated sandbox runtime is unavailable"
                )
            async with self.db_factory() as db:
                if await db.get(TestHarnessRun, run_id) is None:
                    raise TestHarnessSandboxError("Harness run not found")
                existing = await db.scalar(
                    select(TestHarnessSandboxLease).where(
                        TestHarnessSandboxLease.run_id == run_id
                    )
                )
                if existing is not None:
                    raise TestHarnessSandboxError(
                        "Harness run already owns a sandbox lease"
                    )
                lease = TestHarnessSandboxLease(
                    id=uuid.uuid4().hex,
                    run_id=run_id,
                    backend=capability.backend,
                    lease_nonce=secrets.token_hex(24),
                    image_ref=capability.image,
                    image_digest=capability.image_id,
                    status="provisioning",
                    phase="creating_container",
                    runtime_metadata={},
                    cleanup_status="pending",
                )
                db.add(lease)
                await db.commit()
                await db.refresh(lease)
                lease_id = lease.id
                lease_nonce = lease.lease_nonce
        try:
            resource = await self.runtime.provision(
                run_id=run_id,
                lease_id=lease_id,
                lease_nonce=lease_nonce,
            )
        except BaseException as exc:
            cleanup_error: str | None = None
            try:
                await asyncio.shield(
                    self.runtime.cleanup_identity(
                        run_id=run_id,
                        lease_id=lease_id,
                        lease_nonce=lease_nonce,
                    )
                )
            except BaseException as cleanup_exc:
                cleanup_error = str(cleanup_exc)[:4000]
            await asyncio.shield(
                self._mark_failed(
                    lease_id,
                    error=(str(exc) or type(exc).__name__)[:4000],
                    cleanup_error=cleanup_error,
                )
            )
            raise
        try:
            async with self.db_factory() as db:
                lease = await db.get(TestHarnessSandboxLease, lease_id)
                if lease is None:
                    raise TestHarnessSandboxError("sandbox lease disappeared")
                lease.resource_id = resource.resource_id
                lease.resource_name = resource.resource_name
                lease.image_digest = resource.image_digest
                lease.runtime_metadata = resource.metadata
                lease.status = "ready"
                lease.phase = "isolated_idle"
                lease.started_at = datetime.utcnow()
                await db.commit()
                await db.refresh(lease)
                return lease
        except BaseException as exc:
            cleanup_error: str | None = None
            try:
                await asyncio.shield(
                    self.runtime.cleanup_identity(
                        run_id=run_id,
                        lease_id=lease_id,
                        lease_nonce=lease_nonce,
                    )
                )
            except BaseException as cleanup_exc:
                cleanup_error = str(cleanup_exc)[:4000]
            try:
                await asyncio.shield(
                    self._mark_failed(
                        lease_id,
                        error=(str(exc) or type(exc).__name__)[:4000],
                        cleanup_error=cleanup_error,
                    )
                )
            finally:
                if cleanup_error:
                    raise TestHarnessSandboxError(
                        "sandbox persistence failed and cleanup could not be proven"
                    ) from exc
            raise

    async def _mark_failed(
        self,
        lease_id: str,
        *,
        error: str,
        cleanup_error: str | None,
    ) -> None:
        async with self.db_factory() as db:
            lease = await db.get(TestHarnessSandboxLease, lease_id)
            if lease is None:
                return
            lease.status = "failed"
            lease.phase = "failed"
            lease.error = error
            lease.cleanup_status = "failed" if cleanup_error else "completed"
            lease.cleanup_error = cleanup_error
            lease.completed_at = datetime.utcnow()
            await db.commit()

    async def acquire_source(
        self,
        run_id: str,
        target: ResolvedGitTarget,
        *,
        additional_allowed_hosts: tuple[str, ...] = (),
    ) -> SandboxSourceSnapshot:
        """Persist the frozen target, then acquire it inside the owned sandbox."""

        target_payload = target.as_dict()
        async with self._lock:
            async with self.db_factory() as db:
                run = await db.get(TestHarnessRun, run_id)
                lease = await db.scalar(
                    select(TestHarnessSandboxLease).where(
                        TestHarnessSandboxLease.run_id == run_id
                    )
                )
                if run is None or lease is None:
                    raise TestHarnessSandboxError(
                        "Harness run or sandbox lease was not found"
                    )
                if lease.status != "ready" or lease.cleanup_status != "pending":
                    raise TestHarnessSandboxError(
                        "sandbox lease is not ready for source acquisition"
                    )
                if not lease.resource_id or not lease.resource_name:
                    raise TestHarnessSandboxError(
                        "sandbox lease has no proven source container"
                    )
                if run.resolved_target is not None and run.resolved_target != target_payload:
                    raise TestHarnessSandboxError(
                        "Harness run target was already frozen to different input"
                    )
                run.resolved_target = target_payload
                run.source_git_head = target.head_sha
                run.source_fingerprint = target.fingerprint
                lease.status = "preparing"
                lease.phase = "acquiring_source"
                await db.commit()
                lease_id = lease.id
                lease_nonce = lease.lease_nonce
                resource_id = lease.resource_id
                resource_name = lease.resource_name
        try:
            snapshot = await self.runtime.acquire_source(
                run_id=run_id,
                lease_id=lease_id,
                lease_nonce=lease_nonce,
                resource_id=resource_id,
                resource_name=resource_name,
                target=target,
                additional_allowed_hosts=additional_allowed_hosts,
            )
        except BaseException as exc:
            cleanup_error: str | None = None
            try:
                await asyncio.shield(
                    self.runtime.cleanup_identity(
                        run_id=run_id,
                        lease_id=lease_id,
                        lease_nonce=lease_nonce,
                    )
                )
            except BaseException as cleanup_exc:
                cleanup_error = str(cleanup_exc)[:4000]
            await asyncio.shield(
                self._mark_failed(
                    lease_id,
                    error=(str(exc) or type(exc).__name__)[:4000],
                    cleanup_error=cleanup_error,
                )
            )
            raise
        try:
            async with self.db_factory() as db:
                run = await db.get(TestHarnessRun, run_id)
                lease = await db.get(TestHarnessSandboxLease, lease_id)
                if run is None or lease is None:
                    raise TestHarnessSandboxError(
                        "Harness run or sandbox lease disappeared"
                    )
                if run.resolved_target != target_payload:
                    raise TestHarnessSandboxError(
                        "Harness run target changed during source acquisition"
                    )
                lease.runtime_metadata = {
                    **dict(lease.runtime_metadata or {}),
                    "repository_path": snapshot.repository_path,
                    "head_sha": snapshot.head_sha,
                    "internal_network_id": snapshot.internal_network_id,
                    "egress_network_id": snapshot.egress_network_id,
                    "proxy_container_id": snapshot.proxy_container_id,
                    "allowed_hosts": list(snapshot.allowed_hosts),
                }
                lease.status = "source_ready"
                lease.phase = "source_ready"
                await db.commit()
            return snapshot
        except BaseException as exc:
            cleanup_error: str | None = None
            try:
                await asyncio.shield(
                    self.runtime.cleanup_identity(
                        run_id=run_id,
                        lease_id=lease_id,
                        lease_nonce=lease_nonce,
                    )
                )
            except BaseException as cleanup_exc:
                cleanup_error = str(cleanup_exc)[:4000]
            await asyncio.shield(
                self._mark_failed(
                    lease_id,
                    error=(str(exc) or type(exc).__name__)[:4000],
                    cleanup_error=cleanup_error,
                )
            )
            raise

    async def prepare_preview(
        self,
        run_id: str,
        source: SandboxSourceSnapshot,
        *,
        preview_config: dict[str, object],
        startup_timeout_seconds: float,
        url_template: str,
        health_url_template: str,
    ) -> SandboxPreviewSnapshot:
        """Persist preview ownership around one cancellation-safe runtime call."""

        async with self._lock:
            async with self.db_factory() as db:
                lease = await db.scalar(
                    select(TestHarnessSandboxLease).where(
                        TestHarnessSandboxLease.run_id == run_id
                    )
                )
                if lease is None:
                    raise TestHarnessSandboxError("sandbox lease was not found")
                if lease.status != "source_ready" or lease.cleanup_status != "pending":
                    raise TestHarnessSandboxError(
                        "sandbox source is not ready for preview"
                    )
                if not lease.resource_id:
                    raise TestHarnessSandboxError(
                        "sandbox lease has no source container"
                    )
                metadata = dict(lease.runtime_metadata or {})
                expected = {
                    "repository_path": source.repository_path,
                    "head_sha": source.head_sha,
                    "internal_network_id": source.internal_network_id,
                    "egress_network_id": source.egress_network_id,
                    "proxy_container_id": source.proxy_container_id,
                }
                if any(metadata.get(key) != value for key, value in expected.items()):
                    raise TestHarnessSandboxError(
                        "sandbox source identity does not match its durable lease"
                    )
                lease.status = "preparing"
                lease.phase = "starting_preview"
                await db.commit()
                lease_id = lease.id
                lease_nonce = lease.lease_nonce
                resource_id = lease.resource_id
        try:
            preview = await self.runtime.prepare_preview(
                run_id=run_id,
                lease_id=lease_id,
                lease_nonce=lease_nonce,
                resource_id=resource_id,
                source=source,
                preview_config=preview_config,
                startup_timeout_seconds=startup_timeout_seconds,
                url_template=url_template,
                health_url_template=health_url_template,
            )
        except BaseException as exc:
            cleanup_error: str | None = None
            try:
                await asyncio.shield(
                    self.runtime.cleanup_identity(
                        run_id=run_id,
                        lease_id=lease_id,
                        lease_nonce=lease_nonce,
                    )
                )
            except BaseException as cleanup_exc:
                cleanup_error = str(cleanup_exc)[:4000]
            await asyncio.shield(
                self._mark_failed(
                    lease_id,
                    error=(str(exc) or type(exc).__name__)[:4000],
                    cleanup_error=cleanup_error,
                )
            )
            raise
        try:
            async with self.db_factory() as db:
                lease = await db.get(TestHarnessSandboxLease, lease_id)
                if lease is None:
                    raise TestHarnessSandboxError("sandbox lease disappeared")
                lease.runtime_metadata = {
                    **dict(lease.runtime_metadata or {}),
                    "preview_url": preview.url,
                    "health_url": preview.health_url,
                    "host_port": preview.host_port,
                    "internal_port": preview.internal_port,
                    "process_names": list(preview.process_names),
                    "setup_logs": list(preview.setup_logs),
                    "preview_relay_container_id": preview.relay_container_id,
                    "egress_revoked": True,
                }
                lease.status = "preview_ready"
                lease.phase = "preview_ready"
                await db.commit()
            return preview
        except BaseException as exc:
            cleanup_error: str | None = None
            try:
                await asyncio.shield(
                    self.runtime.cleanup_identity(
                        run_id=run_id,
                        lease_id=lease_id,
                        lease_nonce=lease_nonce,
                    )
                )
            except BaseException as cleanup_exc:
                cleanup_error = str(cleanup_exc)[:4000]
            await asyncio.shield(
                self._mark_failed(
                    lease_id,
                    error=(str(exc) or type(exc).__name__)[:4000],
                    cleanup_error=cleanup_error,
                )
            )
            raise

    async def cleanup(self, run_id: str) -> TestHarnessSandboxLease | None:
        async with self._lock:
            async with self.db_factory() as db:
                lease = await db.scalar(
                    select(TestHarnessSandboxLease).where(
                        TestHarnessSandboxLease.run_id == run_id
                    )
                )
                if lease is None:
                    return None
                if lease.cleanup_status == "completed":
                    return lease
                lease.status = "cleaning"
                lease.phase = "cleaning"
                lease.cleanup_status = "cleaning"
                await db.commit()
                lease_id = lease.id
                lease_nonce = lease.lease_nonce
            try:
                await self.runtime.cleanup_identity(
                    run_id=run_id,
                    lease_id=lease_id,
                    lease_nonce=lease_nonce,
                )
            except BaseException as exc:
                async with self.db_factory() as db:
                    lease = await db.get(TestHarnessSandboxLease, lease_id)
                    if lease is not None:
                        lease.status = "cleanup_failed"
                        lease.phase = "cleanup_failed"
                        lease.cleanup_status = "failed"
                        lease.cleanup_error = (str(exc) or type(exc).__name__)[:4000]
                        await db.commit()
                raise
            async with self.db_factory() as db:
                lease = await db.get(TestHarnessSandboxLease, lease_id)
                if lease is None:
                    return None
                lease.status = "cleaned"
                lease.phase = "cleaned"
                lease.cleanup_status = "completed"
                lease.cleanup_error = None
                lease.completed_at = datetime.utcnow()
                await db.commit()
                await db.refresh(lease)
                return lease

    async def recover_interrupted(self) -> int:
        async with self.db_factory() as db:
            run_ids = list(
                await db.scalars(
                    select(TestHarnessSandboxLease.run_id).where(
                        TestHarnessSandboxLease.cleanup_status != "completed"
                    )
                )
            )
        recovered = 0
        for run_id in run_ids:
            try:
                await self.cleanup(run_id)
            except asyncio.CancelledError:
                raise
            except Exception:
                continue
            recovered += 1
        return recovered


test_harness_sandbox_runtime = DockerTestHarnessSandboxRuntime()
test_harness_sandbox_manager = TestHarnessSandboxManager(
    runtime=test_harness_sandbox_runtime
)
