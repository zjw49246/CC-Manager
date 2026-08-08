from __future__ import annotations

import asyncio
import os

import httpx
import pytest
from sqlalchemy import select

from backend.models.project import Project
from backend.models.test_harness import (
    TestHarnessSandboxLease as SandboxLeaseModel,
)
from backend.services.test_harness_sandbox import (
    DockerTestHarnessSandboxRuntime,
    SandboxCapability,
    SandboxPreviewSnapshot,
    SandboxResource,
    SandboxSourceSnapshot,
    TestHarnessSandboxError as SandboxError,
    TestHarnessSandboxManager as SandboxManager,
)
from backend.services.test_harness_git_targets import (
    PublicGitTargetResolver,
    ResolvedGitTarget,
)
from backend.services import test_harness_sandbox as sandbox_module
from backend.services.test_harness import TestHarnessService as HarnessService
from backend.services.test_harness_contracts import TestHarnessSpec as HarnessSpec


_RUN_DOCKER_INTEGRATION = (
    os.getenv("CCM_RUN_DOCKER_SANDBOX_INTEGRATION", "").strip() == "1"
)


@pytest.mark.asyncio
async def test_disabled_sandbox_never_invokes_docker():
    calls: list[list[str]] = []

    async def runner(argv: list[str], timeout: float) -> tuple[int, str]:
        calls.append(argv)
        return 0, "unexpected"

    runtime = DockerTestHarnessSandboxRuntime(
        enabled=False,
        docker_binary="docker",
        runner=runner,
        probe_ttl_seconds=0,
    )

    capability = await runtime.probe()

    assert capability.available is False
    assert "disabled" in (capability.reason or "")
    assert calls == []


@pytest.mark.asyncio
async def test_sandbox_probe_requires_daemon_and_valid_local_image(monkeypatch):
    calls: list[list[str]] = []

    async def runner(argv: list[str], timeout: float) -> tuple[int, str]:
        assert timeout == 5.0
        calls.append(argv)
        if argv[1] == "version":
            return 0, "27.5.1\n"
        return 0, "sha256:" + "b" * 64 + "\n"

    monkeypatch.setattr("shutil.which", lambda _value: "/usr/bin/docker")
    runtime = DockerTestHarnessSandboxRuntime(
        enabled=True,
        docker_binary="docker",
        image="ccm-test-harness-sandbox:test",
        runner=runner,
        probe_ttl_seconds=60,
    )

    first, second = await asyncio.gather(runtime.probe(), runtime.probe())

    assert first == second
    assert first.available is True
    assert first.image_id == "sha256:" + "b" * 64
    assert first.runtime_version == "27.5.1"
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_sandbox_probe_rejects_unverifiable_image_identity(monkeypatch):
    async def runner(argv: list[str], _timeout: float) -> tuple[int, str]:
        if argv[1] == "version":
            return 0, "27.5.1"
        return 0, "ccm-test-harness-sandbox:latest"

    monkeypatch.setattr("shutil.which", lambda _value: "/usr/bin/docker")
    runtime = DockerTestHarnessSandboxRuntime(
        enabled=True,
        runner=runner,
        probe_ttl_seconds=0,
    )

    capability = await runtime.probe()

    assert capability.available is False
    assert "invalid identity" in (capability.reason or "")


@pytest.mark.asyncio
async def test_sandbox_command_failure_preserves_only_bounded_printable_tail():
    async def runner(_argv: list[str], _timeout: float) -> tuple[int, str]:
        return 1, "discard-me" * 300 + "\x1b[31mnpm dependency failed\x00"

    runtime = DockerTestHarnessSandboxRuntime(
        enabled=True,
        runner=runner,
    )

    with pytest.raises(SandboxError) as exc_info:
        await runtime._exec_source(
            binary="/usr/bin/docker",
            resource_id="d" * 64,
            env={},
            argv=["npm", "ci"],
            timeout=30,
        )

    message = str(exc_info.value)
    assert message.startswith("sandbox source command failed: npm: ")
    assert "npm dependency failed" in message
    assert "\x1b" not in message
    assert "\x00" not in message
    assert len(message) < 2100


@pytest.mark.asyncio
async def test_docker_sandbox_provision_has_no_host_mount_or_network(monkeypatch):
    run_id = "a" * 32
    lease_id = "b" * 32
    nonce = "c" * 48
    container_id = "d" * 64
    internal_id = "f" * 64
    calls: list[list[str]] = []

    async def runner(argv: list[str], _timeout: float) -> tuple[int, str]:
        calls.append(argv)
        action = argv[1]
        if action == "version":
            return 0, "27.5.1"
        if action == "image":
            return 0, "sha256:" + "e" * 64
        if argv[1:3] == ["network", "create"]:
            return 0, internal_id
        if argv[1:3] == ["network", "inspect"]:
            return 0, "\t".join(
                [
                    internal_id,
                    "test-harness",
                    run_id,
                    lease_id,
                    nonce,
                    "internal-network",
                    "true",
                ]
            )
        if action == "create":
            return 0, container_id
        if action == "start":
            return 0, container_id
        if action == "inspect":
            if any("{{json" in item for item in argv):
                return 0, '{"internal":{"NetworkID":"' + internal_id + '"}}'
            return 0, "\t".join(
                [
                    container_id,
                    "test-harness",
                    run_id,
                    lease_id,
                    nonce,
                    "source",
                    "true",
                    "true",
                    internal_id,
                ]
            )
        raise AssertionError(argv)

    monkeypatch.setattr("shutil.which", lambda _value: "/usr/bin/docker")
    runtime = DockerTestHarnessSandboxRuntime(
        enabled=True,
        runner=runner,
        probe_ttl_seconds=0,
        memory="2g",
        cpus=1.5,
        pids_limit=128,
        workspace_bytes=512 * 1024 * 1024,
        tmp_bytes=128 * 1024 * 1024,
    )

    resource = await runtime.provision(
        run_id=run_id,
        lease_id=lease_id,
        lease_nonce=nonce,
    )

    create = next(call for call in calls if call[1] == "create")
    assert resource.resource_id == container_id
    assert "--read-only" in create
    assert create[create.index("--network") + 1] == internal_id
    assert create[create.index("--network-alias") + 1] == "source"
    assert create[create.index("--cap-drop") + 1] == "ALL"
    assert create[create.index("--user") + 1] == "10001:10001"
    assert "--mount" not in create
    assert "-v" not in create
    assert "--publish" not in create
    assert "/var/run/docker.sock" not in " ".join(create)
    assert all(".claude" not in value and ".codex" not in value for value in create)
    tmpfs_specs = [
        create[index + 1]
        for index, value in enumerate(create[:-1])
        if value == "--tmpfs"
    ]
    assert (
        "/workspace:rw,nosuid,nodev,exec,mode=1777,size=536870912"
        in tmpfs_specs
    )
    assert "/tmp:rw,noexec,nosuid,nodev,size=134217728" in tmpfs_specs
    assert (
        "/home/sandbox:rw,noexec,nosuid,nodev,"
        "uid=10001,gid=10001,mode=0700,size=134217728"
        in tmpfs_specs
    )
    network_create = next(call for call in calls if call[1:3] == ["network", "create"])
    assert "--internal" in network_create
    assert resource.metadata["network_mode"] == "internal-only"
    assert resource.metadata["internal_network_id"] == internal_id


@pytest.mark.asyncio
async def test_docker_cleanup_requires_exact_labels_before_removal(monkeypatch):
    run_id = "a" * 32
    lease_id = "b" * 32
    nonce = "c" * 48
    container_id = "d" * 64
    calls: list[list[str]] = []

    async def runner(argv: list[str], _timeout: float) -> tuple[int, str]:
        calls.append(argv)
        if argv[1] == "ps":
            return 0, container_id
        if argv[1] == "inspect":
            return 0, "\t".join(
                [
                    container_id,
                    "test-harness",
                    run_id,
                    lease_id,
                    "f" * 48,
                    "source",
                    "true",
                    "true",
                    "none",
                ]
            )
        raise AssertionError(argv)

    monkeypatch.setattr("shutil.which", lambda _value: "/usr/bin/docker")
    runtime = DockerTestHarnessSandboxRuntime(
        enabled=True,
        runner=runner,
        probe_ttl_seconds=0,
    )

    with pytest.raises(SandboxError, match="could not be proven"):
        await runtime.cleanup_identity(
            run_id=run_id,
            lease_id=lease_id,
            lease_nonce=nonce,
        )

    assert all(call[1] != "rm" for call in calls)


class _ManagedRuntime:
    def __init__(self, *, fail: BaseException | None = None):
        self.fail = fail
        self.cleaned: list[tuple[str, str, str]] = []

    async def probe(self, *, force: bool = False) -> SandboxCapability:
        _ = force
        return SandboxCapability(
            available=True,
            backend="docker",
            reason=None,
            image="ccm-test-harness-sandbox:test",
            image_id="sha256:" + "e" * 64,
        )

    async def provision(
        self,
        *,
        run_id: str,
        lease_id: str,
        lease_nonce: str,
    ) -> SandboxResource:
        if self.fail is not None:
            raise self.fail
        return SandboxResource(
            backend="docker",
            resource_id="d" * 64,
            resource_name=f"ccm-harness-{run_id[:16]}-{lease_nonce[:8]}",
            image_ref="ccm-test-harness-sandbox:test",
            image_digest="sha256:" + "e" * 64,
            metadata={"host_mounts": 0},
        )

    async def cleanup_identity(
        self,
        *,
        run_id: str,
        lease_id: str,
        lease_nonce: str,
    ) -> int:
        self.cleaned.append((run_id, lease_id, lease_nonce))
        return 1

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
        _ = (
            run_id,
            lease_id,
            lease_nonce,
            resource_id,
            resource_name,
            additional_allowed_hosts,
        )
        return SandboxSourceSnapshot(
            repository_path="/workspace/repo",
            head_sha=target.head_sha,
            internal_network_id="e" * 64,
            egress_network_id="f" * 64,
            proxy_container_id="1" * 64,
            allowed_hosts=("github.com",),
        )

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
        _ = (
            run_id,
            lease_id,
            lease_nonce,
            resource_id,
            source,
            preview_config,
            startup_timeout_seconds,
            url_template,
            health_url_template,
        )
        return SandboxPreviewSnapshot(
            url="http://127.0.0.1:43123/",
            health_url="http://127.0.0.1:43123/health",
            host_port=43123,
            internal_port=4173,
            process_names=("web",),
            setup_logs=({"index": 0, "executable": "npm"},),
        )


async def _fixed_url_run(db_factory):
    from backend.models.task import Task

    async with db_factory() as db:
        task = Task(title="Sandbox owner", status="completed")
        db.add(task)
        await db.commit()
        task_id = task.id
    return await HarnessService(db_factory=db_factory).start_task_run(
        task_id=task_id,
        spec=HarnessSpec(
            target_kind="fixed_url",
            target={"url": "https://example.com"},
            goal="Create a durable sandbox owner",
        ),
    )


@pytest.mark.asyncio
@pytest.mark.skipif(
    not _RUN_DOCKER_INTEGRATION,
    reason="set CCM_RUN_DOCKER_SANDBOX_INTEGRATION=1 to use local Docker and GitHub",
)
async def test_real_pr99_source_preview_and_identity_cleanup(db_factory):
    """Opt-in proof that an exact public PR never executes on the host."""

    run = await _fixed_url_run(db_factory)
    runtime = DockerTestHarnessSandboxRuntime(
        enabled=True,
        image=os.getenv(
            "TEST_HARNESS_SANDBOX_IMAGE",
            "ccm-test-harness-sandbox:local",
        ),
        probe_ttl_seconds=0,
    )
    capability = await runtime.probe(force=True)
    assert capability.available, capability.reason
    resolved = await PublicGitTargetResolver().resolve(
        project=Project(
            name="CCM PR #99 integration fixture",
            git_url="https://github.com/zjw49246/CC-Manager.git",
        ),
        kind="pull_request",
        target={"pr_number": 99},
    )
    assert resolved.head_sha == "512e9baa10a8f361e8f732c28a14e3554880d612"

    manager = SandboxManager(runtime=runtime, db_factory=db_factory)
    try:
        lease = await manager.provision(run.id)
        assert lease.runtime_metadata["host_mounts"] == 0
        source = await manager.acquire_source(
            run.id,
            resolved,
            additional_allowed_hosts=(
                "files.pythonhosted.org",
                "pypi.org",
                "registry.npmjs.org",
            ),
        )
        assert source.head_sha == resolved.head_sha
        preview = await manager.prepare_preview(
            run.id,
            source,
            preview_config={
                "setup": [
                    {
                        "command": ["uv", "sync", "--frozen", "--no-dev"],
                        "cwd": ".",
                        "env": {},
                        "timeout_seconds": 1200,
                    },
                    {
                        "command": ["npm", "ci", "--no-audit", "--no-fund"],
                        "cwd": "frontend",
                        "env": {},
                        "timeout_seconds": 900,
                    },
                    {
                        "command": ["npm", "run", "build"],
                        "cwd": "frontend",
                        "env": {},
                        "timeout_seconds": 600,
                    },
                ],
                "processes": [
                    {
                        "name": "web",
                        "command": [
                            "{workspace}/.venv/bin/python",
                            "-m",
                            "uvicorn",
                            "backend.main:app",
                            "--host",
                            "0.0.0.0",
                            "--port",
                            "{preview_port}",
                        ],
                        "cwd": ".",
                        "env": {
                            "DATABASE_URL": "sqlite+aiosqlite:///{temp_db}",
                            "AUTH_TOKEN": "",
                            "WORKSPACE_DIR": "{temp_dir}/workspace",
                            "AUTO_START_DISPATCHER": "false",
                            "AUTO_PUSH_TO_ORIGIN": "false",
                            "WORKER_ENABLED": "false",
                            "POOL_ENABLED": "false",
                            "CODEX_POOL_ENABLED": "false",
                            "BACKUP_ENABLED": "false",
                            "TMP_CLEANUP_ENABLED": "false",
                        },
                    }
                ],
            },
            startup_timeout_seconds=180,
            url_template="http://127.0.0.1:{preview_port}/",
            health_url_template=(
                "http://127.0.0.1:{preview_port}/api/system/health"
            ),
        )
        async with httpx.AsyncClient(timeout=3, trust_env=False) as client:
            response = await client.get(preview.health_url)
        assert response.json()["status"] == "ok"
    finally:
        cleaned = await manager.cleanup(run.id)

    assert cleaned is not None
    assert cleaned.cleanup_status == "completed"
    assert cleaned.status == "cleaned"


@pytest.mark.asyncio
async def test_sandbox_manager_persists_identity_before_cleanup(db_factory):
    run = await _fixed_url_run(db_factory)
    runtime = _ManagedRuntime()
    manager = SandboxManager(runtime=runtime, db_factory=db_factory)

    lease = await manager.provision(run.id)

    assert lease.status == "ready"
    assert lease.resource_id == "d" * 64
    assert lease.runtime_metadata == {"host_mounts": 0}
    cleaned = await manager.cleanup(run.id)
    assert cleaned is not None
    assert cleaned.status == "cleaned"
    assert cleaned.cleanup_status == "completed"
    assert runtime.cleaned == [(run.id, lease.id, lease.lease_nonce)]


@pytest.mark.asyncio
async def test_sandbox_manager_freezes_resolved_target_before_source_ready(db_factory):
    run = await _fixed_url_run(db_factory)
    runtime = _ManagedRuntime()
    manager = SandboxManager(runtime=runtime, db_factory=db_factory)
    lease = await manager.provision(run.id)
    target = ResolvedGitTarget(
        kind="pull_request",
        repository="zjw49246/CC-Manager",
        clone_url="https://github.com/zjw49246/CC-Manager.git",
        base_sha="a" * 40,
        head_sha="b" * 40,
        fetch_ref="refs/pull/99/head",
        source_repository="fork/CC-Manager",
        source_ref="feature",
        pr_number=99,
        changed_files=(),
        fingerprint="c" * 64,
    )

    snapshot = await manager.acquire_source(run.id, target)

    assert snapshot.head_sha == "b" * 40
    async with db_factory() as db:
        stored_run = await db.get(type(run), run.id)
        stored_lease = await db.get(SandboxLeaseModel, lease.id)
    assert stored_run is not None
    assert stored_run.resolved_target == target.as_dict()
    assert stored_run.source_git_head == "b" * 40
    assert stored_run.source_fingerprint == "c" * 64
    assert stored_lease is not None
    assert stored_lease.status == "source_ready"
    assert stored_lease.runtime_metadata["repository_path"] == "/workspace/repo"

    preview = await manager.prepare_preview(
        run.id,
        snapshot,
        preview_config={
            "setup": [],
            "processes": [
                {
                    "name": "web",
                    "command": ["npm", "run", "dev", "--", "--port", "{preview_port}"],
                    "cwd": ".",
                    "env": {},
                }
            ],
        },
        startup_timeout_seconds=30,
        url_template="http://127.0.0.1:{preview_port}/",
        health_url_template="http://127.0.0.1:{preview_port}/health",
    )

    assert preview.host_port == 43123
    async with db_factory() as db:
        stored_lease = await db.get(SandboxLeaseModel, lease.id)
    assert stored_lease is not None
    assert stored_lease.status == "preview_ready"
    assert stored_lease.runtime_metadata["egress_revoked"] is True


@pytest.mark.asyncio
async def test_source_acquisition_uses_internal_network_and_exact_sha(monkeypatch):
    run_id = "a" * 32
    lease_id = "b" * 32
    nonce = "c" * 48
    source_id = "d" * 64
    internal_id = "e" * 64
    egress_id = "f" * 64
    proxy_id = "1" * 64
    source_name = f"ccm-harness-{run_id[:16]}-{nonce[:8]}"
    head_sha = "2" * 40
    calls: list[list[str]] = []

    async def runner(argv: list[str], _timeout: float) -> tuple[int, str]:
        calls.append(argv)
        if argv[1] == "version":
            return 0, "27.5.1"
        if argv[1:3] == ["image", "inspect"]:
            return 0, "sha256:" + "3" * 64
        if argv[1:3] == ["network", "ls"]:
            return 0, internal_id
        if argv[1:3] == ["network", "create"]:
            return 0, egress_id
        if argv[1:3] == ["network", "inspect"]:
            network_id = argv[-1]
            if network_id == internal_id:
                role, internal = "internal-network", "true"
            else:
                role, internal = "egress-network", "false"
            return 0, "\t".join(
                [
                    network_id,
                    "test-harness",
                    run_id,
                    lease_id,
                    nonce,
                    role,
                    internal,
                ]
            )
        if argv[1:3] == ["network", "connect"]:
            assert argv[-2:] == [internal_id, proxy_id]
            assert argv[3:5] == ["--alias", "egress-proxy"]
            return 0, ""
        if argv[1] == "create":
            assert any(value.endswith("egress-proxy") for value in argv)
            return 0, proxy_id
        if argv[1] == "start":
            return 0, proxy_id
        if argv[1] == "inspect":
            if any("{{json" in item for item in argv):
                return 0, '{"internal":{"NetworkID":"' + internal_id + '"}}'
            resource_id = argv[-1]
            role = "source" if resource_id == source_id else "egress-proxy"
            network = internal_id
            return 0, "\t".join(
                [
                    resource_id,
                    "test-harness",
                    run_id,
                    lease_id,
                    nonce,
                    role,
                    "true",
                    "true",
                    network,
                ]
            )
        if argv[1] == "exec":
            if "rev-parse" in argv:
                return 0, head_sha
            return 0, "ok"
        raise AssertionError(argv)

    monkeypatch.setattr("shutil.which", lambda _value: "/usr/bin/docker")
    runtime = DockerTestHarnessSandboxRuntime(
        enabled=True,
        runner=runner,
        probe_ttl_seconds=0,
    )
    target = ResolvedGitTarget(
        kind="pull_request",
        repository="zjw49246/CC-Manager",
        clone_url="https://github.com/zjw49246/CC-Manager.git",
        base_sha="4" * 40,
        head_sha=head_sha,
        fetch_ref="refs/pull/99/head",
        source_repository="fork/CC-Manager",
        source_ref="feature",
        pr_number=99,
        changed_files=(),
        fingerprint="5" * 64,
    )

    snapshot = await runtime.acquire_source(
        run_id=run_id,
        lease_id=lease_id,
        lease_nonce=nonce,
        resource_id=source_id,
        resource_name=source_name,
        target=target,
        additional_allowed_hosts=("registry.npmjs.org",),
    )

    assert isinstance(snapshot, SandboxSourceSnapshot)
    assert snapshot.head_sha == head_sha
    assert snapshot.repository_path == "/workspace/repo"
    assert snapshot.internal_network_id == internal_id
    assert snapshot.egress_network_id == egress_id
    assert snapshot.proxy_container_id == proxy_id
    network_creates = [call for call in calls if call[1:3] == ["network", "create"]]
    assert len(network_creates) == 1
    assert "--internal" not in network_creates[0]
    assert any(call[1:3] == ["network", "ls"] for call in calls)
    network_connects = [
        call for call in calls if call[1:3] == ["network", "connect"]
    ]
    assert len(network_connects) == 1
    assert network_connects[0][-2:] == [internal_id, proxy_id]
    assert network_connects[0][3:5] == ["--alias", "egress-proxy"]
    proxy_create = next(call for call in calls if call[1] == "create")
    assert "--read-only" in proxy_create
    assert proxy_create[proxy_create.index("--network") + 1] == egress_id
    assert "--mount" not in proxy_create and "-v" not in proxy_create
    assert any(
        value
        == (
            "CCM_ALLOWED_HOSTS=api.github.com,codeload.github.com,github.com,"
            "objects.githubusercontent.com,registry.npmjs.org"
        )
        for value in proxy_create
    )
    fetch = next(call for call in calls if "fetch" in call)
    assert "HTTPS_PROXY=http://egress-proxy:3128" in fetch
    assert "refs/pull/99/head" in fetch
    assert not any("token" in value.lower() for value in fetch)


@pytest.mark.asyncio
async def test_sandbox_preview_revokes_egress_before_loopback_health(monkeypatch):
    run_id = "a" * 32
    lease_id = "b" * 32
    nonce = "c" * 48
    source_id = "d" * 64
    internal_id = "e" * 64
    egress_id = "f" * 64
    proxy_id = "1" * 64
    relay_id = "3" * 64
    calls: list[list[str]] = []

    async def runner(argv: list[str], _timeout: float) -> tuple[int, str]:
        calls.append(argv)
        if argv[1] == "version":
            return 0, "27.5.1"
        if argv[1:3] == ["image", "inspect"]:
            return 0, "sha256:" + "4" * 64
        if argv[1] == "inspect" and any("{{json" in item for item in argv):
            return 0, '{"internal":{"NetworkID":"' + internal_id + '"}}'
        if argv[1] == "inspect":
            resource_id = argv[-1]
            if resource_id == source_id:
                role, network = "source", internal_id
            elif resource_id == proxy_id:
                role, network = "egress-proxy", egress_id
            else:
                assert resource_id == relay_id
                role, network = "preview-relay", "bridge"
            return 0, "\t".join(
                [
                    resource_id,
                    "test-harness",
                    run_id,
                    lease_id,
                    nonce,
                    role,
                    "true",
                    "true",
                    network,
                ]
            )
        if argv[1:3] == ["network", "inspect"]:
            return 0, "\t".join(
                [
                    egress_id,
                    "test-harness",
                    run_id,
                    lease_id,
                    nonce,
                    "egress-network",
                    "false",
                ]
            )
        if argv[1:3] == ["network", "rm"] or argv[1] == "rm":
            return 0, ""
        if argv[1:3] == ["network", "connect"]:
            assert argv[-2:] == [internal_id, relay_id]
            return 0, ""
        if argv[1] == "create":
            assert any(value.endswith("preview-relay") for value in argv)
            assert argv[argv.index("--network") + 1] == "bridge"
            assert "--publish" in argv
            return 0, relay_id
        if argv[1] == "start":
            assert argv[-1] == relay_id
            return 0, relay_id
        if argv[1] == "port":
            assert argv[-2] == relay_id
            return 0, "127.0.0.1:43123\n"
        if argv[1] == "exec":
            if "/usr/bin/cat" in argv:
                return 1, ""
            return 0, "setup complete"
        raise AssertionError(argv)

    class _Response:
        status_code = 200

    class _Client:
        def __init__(self, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def get(self, url):
            assert url == "http://127.0.0.1:43123/health"
            assert any(call[1:3] == ["network", "rm"] for call in calls)
            return _Response()

    monkeypatch.setattr("shutil.which", lambda _value: "/usr/bin/docker")
    monkeypatch.setattr(sandbox_module.httpx, "AsyncClient", _Client)
    runtime = DockerTestHarnessSandboxRuntime(
        enabled=True,
        runner=runner,
        probe_ttl_seconds=0,
    )
    source = SandboxSourceSnapshot(
        repository_path="/workspace/repo",
        head_sha="2" * 40,
        internal_network_id=internal_id,
        egress_network_id=egress_id,
        proxy_container_id=proxy_id,
        allowed_hosts=("github.com", "registry.npmjs.org"),
    )

    preview = await runtime.prepare_preview(
        run_id=run_id,
        lease_id=lease_id,
        lease_nonce=nonce,
        resource_id=source_id,
        source=source,
        preview_config={
            "setup": [
                {
                    "command": ["npm", "ci"],
                    "cwd": ".",
                    "env": {},
                    "timeout_seconds": 30,
                }
            ],
            "processes": [
                {
                    "name": "web",
                    "command": ["npm", "run", "dev", "--", "--port", "{preview_port}"],
                    "cwd": ".",
                    "env": {"AUTH_TOKEN": ""},
                }
            ],
        },
        startup_timeout_seconds=30,
        url_template="http://127.0.0.1:{preview_port}/",
        health_url_template="http://127.0.0.1:{preview_port}/health",
    )

    assert preview.url == "http://127.0.0.1:43123/"
    assert preview.relay_container_id == relay_id
    setup_call = next(
        call for call in calls if call[1] == "exec" and "npm" in call and "ci" in call
    )
    process_call = next(
        call for call in calls if call[1:3] == ["exec", "-d"]
    )
    assert "HTTPS_PROXY=http://egress-proxy:3128" in setup_call
    assert not any(
        value.startswith(("HTTPS_PROXY=", "HTTP_PROXY=", "ALL_PROXY="))
        for value in process_call
    )
    assert "/opt/ccm/process_wrapper.py" in process_call


@pytest.mark.asyncio
async def test_sandbox_manager_cancellation_cleans_reserved_identity(db_factory):
    run = await _fixed_url_run(db_factory)
    runtime = _ManagedRuntime(fail=asyncio.CancelledError())
    manager = SandboxManager(runtime=runtime, db_factory=db_factory)

    with pytest.raises(asyncio.CancelledError):
        await manager.provision(run.id)

    async with db_factory() as db:
        lease = await db.scalar(
            select(SandboxLeaseModel).where(
                SandboxLeaseModel.run_id == run.id
            )
        )
    assert lease is not None
    assert lease.status == "failed"
    assert lease.cleanup_status == "completed"
    assert runtime.cleaned == [(run.id, lease.id, lease.lease_nonce)]
