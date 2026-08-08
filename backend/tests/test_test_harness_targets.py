from __future__ import annotations

import pytest

from backend.models.project import Project
from backend.models.task import Task
from backend.services.test_harness_git_targets import ResolvedGitTarget
from backend.services.test_harness_sandbox import (
    SandboxCapability,
    SandboxPreviewSnapshot,
    SandboxSourceSnapshot,
)
from backend.services.test_harness_targets import (
    TestHarnessTargetError as HarnessTargetError,
    TestHarnessTargetManager as HarnessTargetManager,
    untrusted_git_target_capability,
)


class _Runtime:
    def __init__(self, capability: SandboxCapability):
        self.capability = capability

    async def probe(self, *, force: bool = False) -> SandboxCapability:
        _ = force
        return self.capability


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kind", "target"),
    [
        ("pull_request", {"remote": "origin", "pr_number": 99}),
        ("git_ref", {"remote": "origin", "ref": "feature", "fetch": True}),
    ],
)
async def test_untrusted_git_targets_fail_before_workspace_or_git(
    kind,
    target,
):
    task = Task(
        id=17,
        title="Untrusted target",
        target_repo="/path/that/must/not/be/inspected",
        last_cwd="/path/that/must/not/be/inspected",
    )
    manager = HarnessTargetManager(
        _Runtime(
            SandboxCapability(
                available=False,
                backend="docker",
                reason="PR/ref isolated sandbox is unavailable",
            )
        )
    )

    with pytest.raises(HarnessTargetError, match="isolated sandbox") as exc:
        await manager.prepare(
            run_id="a" * 32,
            task=task,
            project=None,
            kind=kind,
            target=target,
        )

    assert str(exc.value) == "PR/ref isolated sandbox is unavailable"


@pytest.mark.asyncio
async def test_ready_runtime_advertises_connected_target_pipeline():
    project = Project(
        git_url="https://github.com/acme/ui.git",
        preview_config={
            "sandbox": {"setup": [], "processes": [], "allowed_hosts": []}
        },
    )
    capability = await untrusted_git_target_capability(
        _Runtime(
            SandboxCapability(
                available=True,
                backend="docker",
                reason=None,
                image="ccm-test-harness-sandbox:local",
                image_id="sha256:" + "a" * 64,
            )
        ),
        project=project,
    )

    assert capability.available is True
    assert capability.sandbox.available is True
    assert capability.reason is None


@pytest.mark.asyncio
async def test_ready_runtime_still_rejects_task_without_project():
    capability = await untrusted_git_target_capability(
        _Runtime(
            SandboxCapability(
                available=True,
                backend="docker",
                reason=None,
                image="ccm-test-harness-sandbox:local",
                image_id="sha256:" + "1" * 64,
            )
        )
    )

    assert capability.available is False
    assert "Task Project" in (capability.reason or "")


@pytest.mark.asyncio
async def test_target_manager_prepares_exact_sha_with_approved_sandbox_profile():
    resolved = ResolvedGitTarget(
        kind="pull_request",
        repository="acme/ui",
        clone_url="https://github.com/acme/ui.git",
        base_sha="a" * 40,
        head_sha="b" * 40,
        fetch_ref="refs/pull/7/head",
        source_repository="fork/ui",
        source_ref="feature",
        pr_number=7,
        changed_files=(),
        fingerprint="c" * 64,
    )
    source = SandboxSourceSnapshot(
        repository_path="/workspace/repo",
        head_sha=resolved.head_sha,
        internal_network_id="d" * 64,
        egress_network_id="e" * 64,
        proxy_container_id="f" * 64,
        allowed_hosts=("github.com", "registry.npmjs.org"),
    )
    preview = SandboxPreviewSnapshot(
        url="http://127.0.0.1:43123/",
        health_url="http://127.0.0.1:43123/health",
        host_port=43123,
        internal_port=4173,
        process_names=("web",),
        setup_logs=(),
    )
    calls: list[tuple] = []

    class _Resolver:
        async def resolve(self, **kwargs):
            calls.append(("resolve", kwargs["kind"], kwargs["target"]))
            return resolved

    class _SandboxManager:
        async def provision(self, run_id):
            calls.append(("provision", run_id))

        async def acquire_source(self, run_id, target, **kwargs):
            calls.append(("source", run_id, target.head_sha, kwargs))
            return source

        async def prepare_preview(self, run_id, snapshot, **kwargs):
            calls.append(("preview", run_id, snapshot.head_sha, kwargs))
            return preview

    runtime = _Runtime(
        SandboxCapability(
            available=True,
            backend="docker",
            reason=None,
            image="ccm-test-harness-sandbox:local",
            image_id="sha256:" + "1" * 64,
        )
    )
    manager = HarnessTargetManager(
        runtime,
        resolver=_Resolver(),
        sandbox_manager=_SandboxManager(),
    )
    project = Project(
        git_url="https://github.com/acme/ui.git",
        preview_config={
            "url": "http://127.0.0.1:{preview_port}/",
            "health_url": "http://127.0.0.1:{preview_port}/health",
            "startup_timeout_seconds": 30,
            "sandbox": {
                "setup": [],
                "processes": [{"name": "web", "command": ["serve"]}],
                "allowed_hosts": ["registry.npmjs.org"],
            },
        },
    )
    progress: list[tuple[str, str, str | None]] = []

    async def _progress(stage, title, detail):
        progress.append((stage, title, detail))

    prepared = await manager.prepare(
        run_id="2" * 32,
        task=Task(id=9, title="owner"),
        project=project,
        kind="pull_request",
        target={"remote": "origin", "pr_number": 7},
        on_progress=_progress,
    )

    assert prepared.resolved.head_sha == "b" * 40
    assert prepared.preview.host_port == 43123
    assert calls[0][0] == "resolve"
    assert calls[1] == ("provision", "2" * 32)
    assert calls[2][3]["additional_allowed_hosts"] == (
        "registry.npmjs.org",
    )
    assert [item[0] for item in progress] == [
        "target_resolved",
        "preparing_sandbox",
        "acquiring_source",
        "preparing_preview",
    ]
