"""Startup-frozen entrypoints for Task-controlled child processes.

An Agent may legitimately edit a checkout of CCM itself.  Hooks and scoped
MCP children must therefore never execute their entrypoint back through that
mutable checkout after the turn has started.  This module snapshots the small,
standalone entrypoints when the Manager imports its runtime, then materializes
content-addressed copies inside the private Task runtime root for each owner.

Every snapshotted child is standalone and HTTP-only. Privileged effects stay
inside the already-imported Manager process; no Task child imports the mutable
``backend`` package or reads the application database directly.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import shlex
import stat
import sys
import zipfile
from pathlib import Path
from types import MappingProxyType
from typing import Mapping


class TrustedRuntimeError(RuntimeError):
    """The running Manager could not prove a trusted child entrypoint."""


_CCM_ROOT = Path(__file__).resolve().parents[2]
RUNNING_CCM_CHECKOUT = str(_CCM_ROOT.resolve(strict=True))
_ASSET_PATHS: Mapping[str, Path] = MappingProxyType({
    "ask_user_hook": _CCM_ROOT / "backend" / "hooks" / "ask_user_hook.py",
    "task_ssh_guard_hook": (
        _CCM_ROOT / "backend" / "hooks" / "task_ssh_guard_hook.py"
    ),
    "ccm_ssh_server": _CCM_ROOT / "backend" / "mcp" / "ccm_ssh_server.py",
    "ccm_skills_http_server": (
        _CCM_ROOT / "backend" / "mcp" / "ccm_skills_http_server.py"
    ),
    "ccm_monitor_agent_server": (
        _CCM_ROOT / "backend" / "mcp" / "ccm_monitor_agent_server.py"
    ),
    "ccm_sub_agent_server": (
        _CCM_ROOT / "backend" / "mcp" / "ccm_sub_agent_server.py"
    ),
    "ccm_workspace_review_server": (
        _CCM_ROOT / "backend" / "mcp" / "ccm_workspace_review_server.py"
    ),
})
_BUNDLED_ASSET_PATHS: Mapping[str, Mapping[str, Path]] = MappingProxyType({
    # The Browser child owns Playwright state, but authorization and durable
    # effect receipts remain Manager HTTP calls.  Its browser/network helpers
    # are frozen into the same zipapp so ``python -I`` never falls back to a
    # Task-mutable checkout import.
    "ccm_browser_review_server": MappingProxyType({
        "__main__.py": (
            _CCM_ROOT / "backend" / "mcp" / "ccm_browser_review_server.py"
        ),
        "backend/services/browser_review.py": (
            _CCM_ROOT / "backend" / "services" / "browser_review.py"
        ),
        "backend/services/browser_network.py": (
            _CCM_ROOT / "backend" / "services" / "browser_network.py"
        ),
        "backend/services/test_harness_contracts.py": (
            _CCM_ROOT / "backend" / "services" / "test_harness_contracts.py"
        ),
    }),
})
_MAX_ASSET_BYTES = 1024 * 1024


def _read_startup_asset(name: str, path: Path) -> bytes:
    try:
        path_info = path.lstat()
    except OSError as exc:
        raise TrustedRuntimeError(
            f"Trusted runtime asset is unavailable: {name}"
        ) from exc
    if stat.S_ISLNK(path_info.st_mode) or not stat.S_ISREG(path_info.st_mode):
        raise TrustedRuntimeError(
            f"Trusted runtime asset must be a regular file: {name}"
        )
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise TrustedRuntimeError(
            f"Trusted runtime asset could not be opened safely: {name}"
        ) from exc
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_dev != path_info.st_dev
            or opened.st_ino != path_info.st_ino
            or opened.st_size < 1
            or opened.st_size > _MAX_ASSET_BYTES
        ):
            raise TrustedRuntimeError(
                f"Trusted runtime asset identity is invalid: {name}"
            )
        chunks: list[bytes] = []
        remaining = opened.st_size
        while remaining:
            chunk = os.read(descriptor, min(remaining, 128 * 1024))
            if not chunk:
                raise TrustedRuntimeError(
                    f"Trusted runtime asset was truncated while reading: {name}"
                )
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise TrustedRuntimeError(
                f"Trusted runtime asset grew while reading: {name}"
            )
        finished = os.fstat(descriptor)
        if (
            finished.st_dev != opened.st_dev
            or finished.st_ino != opened.st_ino
            or finished.st_size != opened.st_size
            or finished.st_mtime_ns != opened.st_mtime_ns
        ):
            raise TrustedRuntimeError(
                f"Trusted runtime asset changed while reading: {name}"
            )
    finally:
        os.close(descriptor)
    payload = b"".join(chunks)
    try:
        compile(payload, f"<ccm-trusted-runtime:{name}>", "exec")
    except (SyntaxError, ValueError) as exc:
        raise TrustedRuntimeError(
            f"Trusted runtime asset is not valid Python: {name}"
        ) from exc
    return payload


def _build_startup_bundle(
    name: str,
    paths: Mapping[str, Path],
) -> bytes:
    """Build one deterministic, import-isolated zipapp from startup bytes."""

    members = {
        archive_path: _read_startup_asset(
            f"{name}:{archive_path}",
            source_path,
        )
        for archive_path, source_path in paths.items()
    }
    # Explicit package markers make absolute ``backend.services`` imports
    # resolve inside the zipapp on every supported Python/zipimport version.
    members["backend/__init__.py"] = b"# frozen trusted runtime package\n"
    members["backend/services/__init__.py"] = (
        b"# frozen trusted runtime package\n"
    )
    output = io.BytesIO()
    with zipfile.ZipFile(output, mode="w", compression=zipfile.ZIP_STORED) as bundle:
        for archive_path in sorted(members):
            info = zipfile.ZipInfo(archive_path, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_STORED
            info.create_system = 3
            info.external_attr = (stat.S_IFREG | 0o400) << 16
            bundle.writestr(info, members[archive_path])
    payload = output.getvalue()
    if not payload or len(payload) > _MAX_ASSET_BYTES:
        raise TrustedRuntimeError(
            f"Trusted runtime bundle size is invalid: {name}"
        )
    try:
        with zipfile.ZipFile(io.BytesIO(payload), mode="r") as bundle:
            if bundle.testzip() is not None or "__main__.py" not in bundle.namelist():
                raise TrustedRuntimeError(
                    f"Trusted runtime bundle is invalid: {name}"
                )
    except (OSError, zipfile.BadZipFile) as exc:
        raise TrustedRuntimeError(
            f"Trusted runtime bundle could not be verified: {name}"
        ) from exc
    return payload


def _resolve_running_python() -> str:
    # Preserve the invocation path.  A venv's ``bin/python`` is commonly a
    # symlink to the base interpreter, but Python discovers ``pyvenv.cfg`` from
    # the symlink path. Resolving it would silently drop FastMCP/httpx and run
    # the frozen child under the system environment instead.
    try:
        executable = Path(sys.executable).expanduser()
        if not executable.is_absolute():
            executable = Path.cwd() / executable
        invocation_info = executable.lstat()
        resolved = executable.resolve(strict=True)
        resolved_info = resolved.stat()
    except (OSError, RuntimeError) as exc:
        raise TrustedRuntimeError(
            "The running Python executable could not be resolved"
        ) from exc
    if not (
        stat.S_ISREG(invocation_info.st_mode)
        or stat.S_ISLNK(invocation_info.st_mode)
    ) or not stat.S_ISREG(resolved_info.st_mode):
        raise TrustedRuntimeError(
            "The running Python executable is not an executable regular file"
        )
    if not os.access(executable, os.X_OK):
        raise TrustedRuntimeError(
            "The running Python executable is not executable"
        )
    return str(executable)


def _startup_protected_roots() -> tuple[str, ...]:
    """Paths a Task must not read or mutate while the Manager is running."""

    # Protect only assets used by the live Manager.  The conventional managed
    # worktree root lives below ``<checkout>/.claude-manager/worktrees`` and
    # must remain usable for CCM self-maintenance Tasks; denying the complete
    # checkout would accidentally deny every such isolated worktree too.
    roots: set[Path] = set()
    for relative in (
        "backend",
        "frontend",
        "skills",
        "scripts",
        "alembic",
        ".git",
        ".env",
        "alembic.ini",
        "pyproject.toml",
        "uv.lock",
        "package.json",
        "package-lock.json",
        "pnpm-lock.yaml",
        "yarn.lock",
    ):
        candidate = _CCM_ROOT / relative
        try:
            candidate.lstat()
            roots.add(candidate.resolve(strict=True))
        except FileNotFoundError:
            continue
        except (OSError, RuntimeError) as exc:
            raise TrustedRuntimeError(
                f"The running CCM asset could not be resolved: {relative}"
            ) from exc
    prefix = Path(sys.prefix).expanduser()
    if not prefix.is_absolute():
        prefix = Path.cwd() / prefix
    try:
        resolved_prefix = prefix.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise TrustedRuntimeError(
            "The running Python environment could not be resolved"
        ) from exc
    if resolved_prefix == Path(os.path.sep):
        raise TrustedRuntimeError(
            "The running Python environment cannot be filesystem root"
        )
    try:
        resolved_base_prefix = Path(sys.base_prefix).resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise TrustedRuntimeError(
            "The base Python environment could not be resolved"
        ) from exc
    if resolved_prefix != resolved_base_prefix:
        # The venv owns FastMCP/httpx and their transitive imports. Protecting
        # the complete prefix is smaller and more auditable than chasing every
        # package file after Task execution has begun.
        roots.add(resolved_prefix)
        invocation_parent = Path(RUNNING_PYTHON).parent.resolve(strict=True)
        roots.add(invocation_parent)
    # ``-I`` disables the user site, so an immutable system prefix needs no
    # Task deny rule.  A writable non-venv prefix cannot safely host the MCP
    # child, but that must fail the relevant Task admission rather than make a
    # root/system-Python deployment unable to start at all.  The late check is
    # performed by ``require_trusted_python_runtime`` during materialization.
    return tuple(sorted(str(root) for root in roots))


def require_trusted_python_runtime() -> None:
    """Fail a trusted child launch if its isolated imports are mutable."""

    try:
        prefix = Path(sys.prefix).expanduser().resolve(strict=True)
        base_prefix = Path(sys.base_prefix).expanduser().resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise TrustedRuntimeError(
            "The running Python environment could not be resolved"
        ) from exc
    if prefix == Path(os.path.sep):
        raise TrustedRuntimeError(
            "The running Python environment cannot be filesystem root"
        )
    if prefix == base_prefix and os.access(prefix, os.W_OK):
        raise TrustedRuntimeError(
            "A writable non-venv Python environment cannot host trusted MCP"
        )


# These constants are deliberately evaluated at module import.  A Task that
# later edits this checkout cannot alter the bytes used by its hook/MCP child.
RUNNING_PYTHON = _resolve_running_python()
TRUSTED_RUNTIME_PROTECTED_ROOTS = _startup_protected_roots()
_TRUSTED_ASSETS: Mapping[str, bytes] = MappingProxyType(
    {
        **{
            name: _read_startup_asset(name, path)
            for name, path in _ASSET_PATHS.items()
        },
        **{
            name: _build_startup_bundle(name, paths)
            for name, paths in _BUNDLED_ASSET_PATHS.items()
        },
    }
)
_TRUSTED_DIGESTS: Mapping[str, str] = MappingProxyType({
    name: hashlib.sha256(payload).hexdigest()
    for name, payload in _TRUSTED_ASSETS.items()
})


def prime_trusted_runtime() -> None:
    """Import-time startup hook; evaluation above is the actual priming."""


def trusted_runtime_protected_roots() -> tuple[str, ...]:
    """Return the immutable Manager/runtime roots denied to every Task."""

    return TRUSTED_RUNTIME_PROTECTED_ROOTS


def trusted_asset_digest(name: str) -> str:
    try:
        return _TRUSTED_DIGESTS[name]
    except KeyError as exc:
        raise ValueError(f"Unknown trusted runtime asset: {name}") from exc


def trusted_python_asset_filename(name: str) -> str:
    """Return the content-addressed filename for one frozen entrypoint."""

    digest = trusted_asset_digest(name)
    suffix = ".pyz" if name in _BUNDLED_ASSET_PATHS else ".py"
    return f"{name.replace('_', '-')}-{digest[:16]}{suffix}"


def verify_materialized_trusted_python_asset(name: str, path: Path) -> bytes:
    """Fail unless ``path`` is an unchanged private copy of the startup bytes."""

    try:
        expected = _TRUSTED_ASSETS[name]
    except KeyError as exc:
        raise ValueError(f"Unknown trusted runtime asset: {name}") from exc
    try:
        info = path.lstat()
    except OSError as exc:
        raise TrustedRuntimeError(
            f"Materialized trusted runtime asset is unavailable: {name}"
        ) from exc
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.geteuid()
        or info.st_nlink != 1
        or stat.S_IMODE(info.st_mode) != 0o500
        or path.name != trusted_python_asset_filename(name)
    ):
        raise TrustedRuntimeError(
            f"Materialized trusted runtime asset identity is invalid: {name}"
        )
    try:
        actual = path.read_bytes()
    except OSError as exc:
        raise TrustedRuntimeError(
            f"Materialized trusted runtime asset could not be read: {name}"
        ) from exc
    if not hmac_compare_digest(actual, expected):
        raise TrustedRuntimeError(
            f"Materialized trusted runtime asset changed: {name}"
        )
    return actual


def hmac_compare_digest(left: bytes, right: bytes) -> bool:
    """Local constant-time comparison without exposing mutable asset bytes."""

    import hmac

    return hmac.compare_digest(left, right)


def trusted_hook_components_from_settings(
    settings_path: Path,
) -> tuple[tuple[str, bytes], ...]:
    """Return verified frozen hook bytes referenced by exact Task settings."""

    try:
        payload = json.loads(settings_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise TrustedRuntimeError(
            "Task settings could not be read for trusted runtime fingerprinting"
        ) from exc
    hooks = payload.get("hooks") if isinstance(payload, dict) else None
    entries = hooks.get("PreToolUse") if isinstance(hooks, dict) else None
    if entries is None:
        return ()
    if not isinstance(entries, list):
        raise TrustedRuntimeError("Task settings hooks have an invalid shape")
    assets_by_filename = {
        trusted_python_asset_filename(name): name
        for name in ("ask_user_hook", "task_ssh_guard_hook")
    }
    components: dict[str, bytes] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            raise TrustedRuntimeError("Task settings hook entry is invalid")
        commands = entry.get("hooks")
        if not isinstance(commands, list):
            raise TrustedRuntimeError("Task settings hook commands are invalid")
        for command_entry in commands:
            command = (
                command_entry.get("command")
                if isinstance(command_entry, dict)
                else None
            )
            if not isinstance(command, str):
                raise TrustedRuntimeError("Task settings hook command is invalid")
            try:
                argv = shlex.split(command)
            except ValueError as exc:
                raise TrustedRuntimeError(
                    "Task settings hook command could not be parsed"
                ) from exc
            if len(argv) < 2 or argv[0] != RUNNING_PYTHON:
                raise TrustedRuntimeError(
                    "Task settings hook does not use the running Python"
                )
            script_path = Path(argv[1])
            asset_name = assets_by_filename.get(script_path.name)
            if (
                asset_name is None
                or script_path.parent != settings_path.parent
                or asset_name in components
            ):
                raise TrustedRuntimeError(
                    "Task settings hook does not reference one exact frozen asset"
                )
            components[asset_name] = verify_materialized_trusted_python_asset(
                asset_name,
                script_path,
            )
    return tuple(sorted(components.items()))


def materialize_trusted_python_asset(
    name: str,
    *,
    namespace: str,
    identifier: int,
) -> Path:
    """Write one startup snapshot into an owner's private runtime scope."""

    require_trusted_python_runtime()
    try:
        payload = _TRUSTED_ASSETS[name]
    except KeyError as exc:
        raise ValueError(f"Unknown trusted runtime asset: {name}") from exc
    from backend.services.task_runtime_secrets import write_private_bytes

    filename = trusted_python_asset_filename(name)
    return write_private_bytes(
        namespace,
        identifier,
        filename,
        payload,
        mode=0o500,
    )
