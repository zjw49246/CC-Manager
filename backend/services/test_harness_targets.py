"""Fail-closed preparation of public GitHub PR/ref Harness targets."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from backend.models.project import Project
from backend.models.task import Task
from backend.services.workspace_review import WorkspaceReviewError
from backend.services.test_harness_sandbox import (
    SandboxCapability,
    SandboxPreviewSnapshot,
    SandboxSourceSnapshot,
    TestHarnessSandboxManager,
    TestHarnessSandboxRuntime,
    test_harness_sandbox_manager,
    test_harness_sandbox_runtime,
)
from backend.services.test_harness_git_targets import (
    PublicGitTargetResolver,
    ResolvedGitTarget,
)


_TARGET_PIPELINE_AVAILABLE = True


@dataclass(frozen=True, slots=True)
class UntrustedGitTargetCapability:
    available: bool
    reason: str | None
    sandbox: SandboxCapability

    def as_dict(self) -> dict[str, object]:
        return {
            "available": self.available,
            "reason": self.reason,
            "sandbox": self.sandbox.as_dict(),
        }


async def untrusted_git_target_capability(
    runtime: TestHarnessSandboxRuntime | None = None,
    *,
    project: Project | None = None,
    force: bool = False,
) -> UntrustedGitTargetCapability:
    sandbox = await (runtime or test_harness_sandbox_runtime).probe(force=force)
    if not sandbox.available:
        return UntrustedGitTargetCapability(False, sandbox.reason, sandbox)
    if not _TARGET_PIPELINE_AVAILABLE:
        return UntrustedGitTargetCapability(
            False,
            "PR/ref sandbox target pipeline is unavailable",
            sandbox,
        )
    if project is None:
        return UntrustedGitTargetCapability(
            False,
            "PR/ref tests require a Task Project with a confirmed sandbox Preview profile",
            sandbox,
        )
    preview = project.preview_config
    profile = preview.get("sandbox") if isinstance(preview, dict) else None
    if not isinstance(profile, dict):
        return UntrustedGitTargetCapability(
            False,
            "Project has no confirmed sandbox Preview profile",
            sandbox,
        )
    return UntrustedGitTargetCapability(True, None, sandbox)


class TestHarnessTargetError(WorkspaceReviewError):
    """An untrusted Git target cannot be admitted safely."""


@dataclass(frozen=True, slots=True)
class PreparedGitTarget:
    resolved: ResolvedGitTarget
    source: SandboxSourceSnapshot
    preview: SandboxPreviewSnapshot


TargetProgressCallback = Callable[[str, str, str | None], Awaitable[None]]


class TestHarnessTargetManager:
    """Resolve, acquire and preview one exact target without host execution."""

    def __init__(
        self,
        runtime: TestHarnessSandboxRuntime | None = None,
        *,
        resolver: PublicGitTargetResolver | None = None,
        sandbox_manager: TestHarnessSandboxManager | None = None,
    ) -> None:
        self.runtime = runtime or test_harness_sandbox_runtime
        self.resolver = resolver or PublicGitTargetResolver()
        self.sandbox_manager = sandbox_manager or (
            test_harness_sandbox_manager
            if runtime is None
            else TestHarnessSandboxManager(runtime=self.runtime)
        )

    async def prepare(
        self,
        *,
        run_id: str,
        task: Task,
        project: Project | None,
        kind: str,
        target: dict[str, Any],
        on_progress: TargetProgressCallback | None = None,
    ) -> PreparedGitTarget:
        _ = task
        if kind not in {"pull_request", "git_ref"}:
            raise TestHarnessTargetError(
                f"target kind {kind!r} does not use the untrusted Git sandbox gate"
            )
        capability = await untrusted_git_target_capability(
            self.runtime,
            project=project,
        )
        if not capability.available:
            raise TestHarnessTargetError(
                capability.reason or "PR/ref sandbox target is unavailable"
            )
        if project is None:  # Kept explicit even though capability rejects it.
            raise TestHarnessTargetError(
                "PR/ref tests require a Task Project"
            )
        root_config = project.preview_config
        if not isinstance(root_config, dict):
            raise TestHarnessTargetError(
                "Project has no confirmed Preview configuration"
            )
        sandbox_config = root_config.get("sandbox")
        if not isinstance(sandbox_config, dict):
            raise TestHarnessTargetError(
                "Project has no confirmed sandbox Preview profile"
            )
        allowed_hosts = sandbox_config.get("allowed_hosts", [])
        if (
            not isinstance(allowed_hosts, list)
            or any(not isinstance(host, str) for host in allowed_hosts)
        ):
            raise TestHarnessTargetError(
                "Project sandbox Preview allowlist is invalid"
            )
        resolved = await self.resolver.resolve(
            project=project,
            kind=kind,
            target=target,
        )
        if on_progress is not None:
            await on_progress(
                "target_resolved",
                "已锁定精确 Git 提交",
                (
                    f"HEAD {resolved.head_sha[:12]}；"
                    f"{len(resolved.changed_files)} 个变更文件。"
                ),
            )
            await on_progress(
                "preparing_sandbox",
                "正在创建隔离 Sandbox",
                None,
            )
        await self.sandbox_manager.provision(run_id)
        if on_progress is not None:
            await on_progress(
                "acquiring_source",
                "正在 Sandbox 内获取精确源码",
                f"只接受 HEAD {resolved.head_sha[:12]}。",
            )
        source = await self.sandbox_manager.acquire_source(
            run_id,
            resolved,
            additional_allowed_hosts=tuple(allowed_hosts),
        )
        if on_progress is not None:
            await on_progress(
                "preparing_preview",
                "正在隔离环境安装依赖并启动 Preview",
                "依赖出口完成后会被撤销，Preview 仅映射到 Manager loopback。",
            )
        preview = await self.sandbox_manager.prepare_preview(
            run_id,
            source,
            preview_config=sandbox_config,
            startup_timeout_seconds=float(
                root_config.get("startup_timeout_seconds", 90)
            ),
            url_template=str(root_config.get("url", "")),
            health_url_template=str(root_config.get("health_url", "")),
        )
        return PreparedGitTarget(
            resolved=resolved,
            source=source,
            preview=preview,
        )


test_harness_target_manager = TestHarnessTargetManager()
