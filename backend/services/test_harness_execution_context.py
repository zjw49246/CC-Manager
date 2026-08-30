"""Frozen, durable execution routes for Task-owned Test Harness runs."""

from __future__ import annotations

import json
import re
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from backend.models.project import Project
from backend.models.task import Task


HARNESS_EXECUTION_CONTEXT_KEY = "_execution_context_v1"
HARNESS_EXECUTION_CONTEXT_VERSION = 1
_GITHUB_REPOSITORY_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")


class TestHarnessExecutionContextError(ValueError):
    """A Harness route cannot be frozen or its durable shape is invalid."""


def _json_copy(value: Any) -> Any:
    try:
        return json.loads(
            json.dumps(
                value,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
        )
    except (TypeError, ValueError, UnicodeError) as exc:
        raise TestHarnessExecutionContextError(
            "Harness execution context is not canonical JSON"
        ) from exc


def freeze_harness_execution_context(
    *,
    task: Task,
    project: Project | None,
    target_kind: str,
    target: dict[str, Any] | None = None,
    preview_config_override: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Freeze every mutable route/config value used after Run admission."""

    project_id = task.project_id
    if project_id is not None and (
        project is None or project.id != project_id
    ):
        raise TestHarnessExecutionContextError(
            "Harness owner Project disappeared during admission"
        )
    if project_id is None and project is not None:
        raise TestHarnessExecutionContextError(
            "Harness execution Project does not match its owner Task"
        )
    context: dict[str, Any] = {
        "version": HARNESS_EXECUTION_CONTEXT_VERSION,
        "target_kind": target_kind,
        "project_id": project_id,
    }
    if target_kind == "current_workspace":
        if project is None or (
            project.preview_config is None and preview_config_override is None
        ):
            raise TestHarnessExecutionContextError(
                "Project has no confirmed Preview configuration"
            )
        try:
            from backend.services.workspace_review import (
                _task_workspace,
                resolve_preview_config,
            )

            workspace = _task_workspace(task, project)
            profile_id = (
                target.get("preview_profile_id")
                if isinstance(target, dict)
                else None
            )
            resolved_profiles = resolve_preview_config(
                (
                    preview_config_override
                    if preview_config_override is not None
                    else project.preview_config
                ),
                workspace,
                profile_ids=[profile_id] if profile_id else None,
            )
            if len(resolved_profiles) != 1:
                raise TestHarnessExecutionContextError(
                    "Current-workspace Harness requires exactly one Preview profile"
                )
            selected = dict(resolved_profiles[0])
            selected_profile_id = selected.pop("id")
            for metadata_key in ("match_paths", "enabled", "selection_reason"):
                selected.pop(metadata_key, None)
            preview_config = selected
        except Exception as exc:
            raise TestHarnessExecutionContextError(str(exc)) from exc
        context.update(
            {
                "workspace_path": str(workspace),
                "preview_config": _json_copy(preview_config),
                "preview_profile_id": selected_profile_id,
            }
        )
    elif target_kind in {"pull_request", "git_ref"}:
        if project is None or not isinstance(project.preview_config, dict):
            raise TestHarnessExecutionContextError(
                "PR/ref tests require a Task Project with a confirmed "
                "sandbox Preview profile"
            )
        try:
            from backend.services.test_harness_git_targets import (
                github_repository_from_project,
            )

            repository = github_repository_from_project(project)
        except Exception as exc:
            raise TestHarnessExecutionContextError(str(exc)) from exc
        if not isinstance(project.preview_config.get("sandbox"), dict):
            raise TestHarnessExecutionContextError(
                "Project has no confirmed sandbox Preview profile"
            )
        context.update(
            {
                "repository": repository,
                "git_url": f"https://github.com/{repository}.git",
                "preview_config": _json_copy(project.preview_config),
            }
        )
    elif target_kind != "fixed_url":
        raise TestHarnessExecutionContextError(
            f"unsupported Harness target kind {target_kind!r}"
        )
    return _json_copy(context)


def execution_context_from_runtime(
    runtime: dict[str, Any] | None,
    *,
    target_kind: str,
) -> dict[str, Any]:
    """Validate and copy the private execution context frozen in a Run."""

    raw = (
        runtime.get(HARNESS_EXECUTION_CONTEXT_KEY)
        if isinstance(runtime, dict)
        else None
    )
    if not isinstance(raw, dict):
        raise TestHarnessExecutionContextError(
            "Harness Run has no frozen execution context"
        )
    context = _json_copy(raw)
    if (
        context.get("version") != HARNESS_EXECUTION_CONTEXT_VERSION
        or context.get("target_kind") != target_kind
    ):
        raise TestHarnessExecutionContextError(
            "Harness Run execution context version or target changed"
        )
    project_id = context.get("project_id")
    if project_id is not None and (
        isinstance(project_id, bool) or not isinstance(project_id, int)
    ):
        raise TestHarnessExecutionContextError(
            "Harness Run execution Project identity is invalid"
        )
    if target_kind == "current_workspace":
        workspace_path = context.get("workspace_path")
        if (
            not isinstance(workspace_path, str)
            or not workspace_path
            or "\x00" in workspace_path
            or not Path(workspace_path).is_absolute()
            or not isinstance(context.get("preview_config"), dict)
        ):
            raise TestHarnessExecutionContextError(
                "Harness Run workspace execution route is invalid"
            )
    elif target_kind in {"pull_request", "git_ref"}:
        repository = context.get("repository")
        expected_url = (
            f"https://github.com/{repository}.git"
            if isinstance(repository, str)
            else None
        )
        preview_config = context.get("preview_config")
        if (
            not isinstance(repository, str)
            or _GITHUB_REPOSITORY_RE.fullmatch(repository) is None
            or context.get("git_url") != expected_url
            or not isinstance(preview_config, dict)
            or not isinstance(preview_config.get("sandbox"), dict)
        ):
            raise TestHarnessExecutionContextError(
                "Harness Run Git execution route is invalid"
            )
    return context


def runtime_with_execution_context(
    runtime: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    frozen = dict(runtime)
    frozen[HARNESS_EXECUTION_CONTEXT_KEY] = _json_copy(context)
    return frozen


def public_harness_runtime(runtime: dict[str, Any]) -> dict[str, Any]:
    """Hide host paths/config from the public runtime projection."""

    return {
        key: _json_copy(value)
        for key, value in runtime.items()
        if key != HARNESS_EXECUTION_CONTEXT_KEY
    }


def frozen_git_project(context: dict[str, Any]) -> SimpleNamespace:
    """Build the minimal immutable Project view consumed by Git targets."""

    return SimpleNamespace(
        id=context["project_id"],
        git_url=context["git_url"],
        preview_config=_json_copy(context["preview_config"]),
    )
