"""Minimal per-workload Claude config homes for untrusted Agent prompts.

Claude's normal OAuth path reads ``.credentials.json`` from
``CLAUDE_CONFIG_DIR``.  The same directory can also contain user plugins,
skills, hooks, and auto-memory.  Pointing an untrusted workload at the real
account home therefore grants more ambient authority than authentication.

This module projects only a bounded OAuth access token (or caller-supplied
direct provider auth) into a Manager-private config home.  Refresh tokens never
leave the selected account home.  Session files created by Claude remain
scoped to that workload, while the caller keeps the whole projection in its
protected-path deny set.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping, MutableMapping

from backend.services.task_runtime_secrets import runtime_secret_root


_NAMESPACE_RE = re.compile(r"^[a-z][a-z0-9-]{0,31}$")
_MAX_CREDENTIAL_BYTES = 1024 * 1024
_MIN_OAUTH_REMAINING_SECONDS = 300
_DIRECT_SECRET_AUTH_ENV_KEYS = frozenset(
    {
        "ANTHROPIC_API_KEY",
        "ANTHROPIC_AUTH_TOKEN",
        "CLAUDE_CODE_OAUTH_TOKEN",
    }
)
_THIRD_PARTY_AUTH_FLAGS = frozenset(
    {
        "CLAUDE_CODE_USE_BEDROCK",
        "CLAUDE_CODE_USE_VERTEX",
        "CLAUDE_CODE_USE_FOUNDRY",
    }
)
_AMBIENT_DISABLE_ENV = {
    "CLAUDE_CODE_DISABLE_AUTO_MEMORY": "1",
    "CLAUDE_CODE_ENABLE_BACKGROUND_PLUGIN_REFRESH": "0",
    "CLAUDE_CODE_SYNC_PLUGIN_INSTALL": "0",
    "CLAUDE_CODE_SYNC_PLUGINS": "0",
    "CLAUDE_CODE_SYNC_SKILLS": "0",
    "DISABLE_PLUGIN_AUTOLOAD": "1",
}
_FORBIDDEN_AMBIENT_NAMES = frozenset(
    {
        ".mcp.json",
        "agents",
        "commands",
        "memory",
        "plugins",
        "settings.json",
        "skills",
    }
)


class ClaudeAuthProjectionError(RuntimeError):
    """A minimal Claude authentication projection could not be proven safe."""


@dataclass(frozen=True)
class ClaudeAuthProjection:
    config_dir: Path
    source_config_dir: Path | None
    uses_environment_auth: bool
    oauth_access_token: str | None = field(default=None, repr=False)


def _require_namespace(namespace: str, identifier: int) -> None:
    if not _NAMESPACE_RE.fullmatch(namespace):
        raise ValueError("Invalid Claude auth projection namespace")
    if isinstance(identifier, bool) or not isinstance(identifier, int) or identifier <= 0:
        raise ValueError("Claude auth projection identifier must be positive")


def _ensure_private_directory(path: Path) -> None:
    """Create/validate a service-owned, non-symlink 0700 directory."""

    for ancestor in path.parents:
        try:
            ancestor_info = ancestor.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(ancestor_info.st_mode):
            raise ClaudeAuthProjectionError(
                f"Claude auth projection has a symlink ancestor: {path}"
            )

    missing: list[Path] = []
    current = path
    while True:
        try:
            info = current.lstat()
        except FileNotFoundError:
            missing.append(current)
            if current.parent == current:
                raise ClaudeAuthProjectionError(
                    "Claude auth projection has no existing filesystem ancestor"
                )
            current = current.parent
            continue
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise ClaudeAuthProjectionError(
                f"Claude auth projection path is not a real directory: {current}"
            )
        break

    for directory in reversed(missing):
        try:
            directory.mkdir(mode=0o700)
        except FileExistsError:
            pass

    try:
        info = path.lstat()
    except OSError as exc:
        raise ClaudeAuthProjectionError(
            "Claude auth projection directory is unavailable"
        ) from exc
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.geteuid()
    ):
        raise ClaudeAuthProjectionError(
            "Claude auth projection directory has an unsafe identity"
        )
    try:
        os.chmod(path, 0o700)
    except OSError as exc:
        raise ClaudeAuthProjectionError(
            "Claude auth projection permissions could not be secured"
        ) from exc


def _projection_prefix(namespace: str, identifier: int) -> str:
    _require_namespace(namespace, identifier)
    return f"claude-auth-{namespace}-{identifier}-"


def _projection_name(
    namespace: str,
    identifier: int,
    binding: str,
) -> str:
    if not isinstance(binding, str) or not binding:
        raise ValueError("Claude auth projection binding must be non-empty")
    suffix = hashlib.sha256(binding.encode("utf-8")).hexdigest()[:20]
    return f"{_projection_prefix(namespace, identifier)}{suffix}"


def _projection_directory(
    namespace: str,
    identifier: int,
    binding: str,
) -> Path:
    root = runtime_secret_root()
    _ensure_private_directory(root)
    projection = root / _projection_name(namespace, identifier, binding)
    _ensure_private_directory(projection)
    return projection


def _read_private_oauth_access_token(source: Path) -> str:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(source, flags)
    except OSError as exc:
        raise ClaudeAuthProjectionError(
            "Claude OAuth credentials are unavailable for clean projection"
        ) from exc
    try:
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.geteuid()
            or info.st_nlink != 1
            or stat.S_IMODE(info.st_mode) & 0o077
            or info.st_size <= 0
            or info.st_size > _MAX_CREDENTIAL_BYTES
        ):
            raise ClaudeAuthProjectionError(
                "Claude OAuth credentials must be a private service-owned regular file"
            )
        chunks: list[bytes] = []
        remaining = info.st_size
        while remaining:
            chunk = os.read(descriptor, min(remaining, 64 * 1024))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        if len(payload) != info.st_size:
            raise ClaudeAuthProjectionError(
                "Claude OAuth credentials changed while being projected"
            )
        try:
            parsed = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ClaudeAuthProjectionError(
                "Claude OAuth credentials are not valid JSON"
            ) from exc
        oauth = parsed.get("claudeAiOauth") if isinstance(parsed, dict) else None
        access_token = oauth.get("accessToken") if isinstance(oauth, dict) else None
        expires_at = oauth.get("expiresAt") if isinstance(oauth, dict) else None
        if (
            not isinstance(access_token, str)
            or not access_token
            or isinstance(expires_at, bool)
            or not isinstance(expires_at, (int, float))
        ):
            raise ClaudeAuthProjectionError(
                "Claude OAuth credentials do not contain a bounded access token"
            )
        remaining_seconds = (float(expires_at) / 1000.0) - time.time()
        if remaining_seconds < _MIN_OAUTH_REMAINING_SECONDS:
            raise ClaudeAuthProjectionError(
                "Claude OAuth access token is expired or too close to expiry; "
                "refresh the selected account before launching this workload"
            )
        return access_token
    finally:
        os.close(descriptor)


def _remove_regular_file(path: Path) -> None:
    try:
        info = path.lstat()
    except FileNotFoundError:
        return
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.geteuid()
    ):
        raise ClaudeAuthProjectionError(
            f"Unexpected file in Claude auth projection: {path}"
        )
    path.unlink()


def _validate_no_ambient_customization(path: Path) -> None:
    """Reject settings/plugin/skill/memory injection in a reused projection."""

    pending = [path]
    while pending:
        current = pending.pop()
        try:
            entries = list(os.scandir(current))
        except OSError as exc:
            raise ClaudeAuthProjectionError(
                "Claude auth projection could not be inspected"
            ) from exc
        for entry in entries:
            info = entry.stat(follow_symlinks=False)
            if stat.S_ISLNK(info.st_mode):
                raise ClaudeAuthProjectionError(
                    "Claude auth projection contains a symlink"
                )
            if entry.name in _FORBIDDEN_AMBIENT_NAMES:
                raise ClaudeAuthProjectionError(
                    "Claude auth projection contains ambient customization"
                )
            if stat.S_ISDIR(info.st_mode):
                pending.append(Path(entry.path))
            elif not stat.S_ISREG(info.st_mode):
                raise ClaudeAuthProjectionError(
                    "Claude auth projection contains an unsupported entry"
                )


def environment_has_direct_claude_auth(environment: Mapping[str, str]) -> bool:
    """Return whether a clean Claude home can authenticate from env alone."""

    if any(
        isinstance(environment.get(key), str) and bool(environment[key].strip())
        for key in _DIRECT_SECRET_AUTH_ENV_KEYS
    ):
        return True
    return any(
        str(environment.get(key) or "").strip().lower()
        in {"1", "true", "yes", "on"}
        for key in _THIRD_PARTY_AUTH_FLAGS
    )


def inject_cloudrouter_claude_direct_auth(
    environment: MutableMapping[str, str],
    cloudrouter_store,
    source_config_dir: str | os.PathLike[str] | None,
) -> bool:
    """Project one managed API account into a zero/read-only Claude route.

    The managed key remains in process memory/environment only.  It is never
    written into the clean config home, settings file, argv, or model-tool
    environment (callers also set ``CLAUDE_CODE_SUBPROCESS_ENV_SCRUB``).
    """

    if cloudrouter_store is None or not source_config_dir:
        return False
    finder = getattr(cloudrouter_store, "account_for_claude_config_dir", None)
    reader = getattr(cloudrouter_store, "_read_api_key", None)
    if not callable(finder) or not callable(reader):
        raise ClaudeAuthProjectionError(
            "Managed Claude API account projection is unavailable"
        )
    try:
        account = finder(source_config_dir)
        if account is None:
            return False
        api_key = reader(account)
        from backend.services.cloudrouter_accounts import API_PROVIDER_SPECS

        spec = API_PROVIDER_SPECS[account.api_provider]
        base_url = spec.claude_base_url
    except ClaudeAuthProjectionError:
        raise
    except Exception as exc:
        raise ClaudeAuthProjectionError(
            "Managed Claude API account credentials are unavailable"
        ) from exc
    if (
        not isinstance(api_key, str)
        or not api_key
        or api_key.strip() != api_key
        or not isinstance(base_url, str)
        or not base_url.startswith("https://")
    ):
        raise ClaudeAuthProjectionError(
            "Managed Claude API account projection is invalid"
        )
    for key in _DIRECT_SECRET_AUTH_ENV_KEYS:
        environment.pop(key, None)
    # Claude Code's interactive/PTY path only accepts ANTHROPIC_API_KEY after
    # its suffix has been recorded in ``customApiKeyResponses.approved``.
    # Headless ``-p`` skips that consent gate, which made unit/smoke probes pass
    # while real CCM chat turns failed locally with "Not logged in" whenever a
    # prior hidden prompt had recorded the managed key as rejected.  Managed
    # gateway credentials are bearer tokens, so use the explicit token route;
    # it is non-interactive and does not depend on mutable CLI consent state.
    environment["ANTHROPIC_AUTH_TOKEN"] = api_key
    environment["ANTHROPIC_BASE_URL"] = base_url
    return True


def prepare_claude_auth_projection(
    source_config_dir: str | os.PathLike[str] | None,
    *,
    namespace: str,
    identifier: int,
    binding: str,
    environment: Mapping[str, str],
) -> ClaudeAuthProjection:
    """Prepare a clean config home using direct env auth or OAuth credentials.

    OAuth uses only the currently bounded access token.  The refresh token is
    never copied or exposed to the child, so refresh rotation remains owned by
    the selected account home.  Settings, hooks, MCP declarations, plugins,
    skills, and memory are intentionally not projected.
    """

    projection = _projection_directory(namespace, identifier, binding)
    uses_environment_auth = environment_has_direct_claude_auth(environment)
    source: Path | None = None
    oauth_access_token: str | None = None
    # A clean projection must never retain a credential capsule.  Claude's
    # oauth-token env route neither needs nor receives the refresh token.
    _remove_regular_file(projection / ".credentials.json")
    _validate_no_ambient_customization(projection)
    if uses_environment_auth:
        pass
    else:
        raw_source = source_config_dir or str(Path.home() / ".claude")
        expanded = os.path.abspath(
            os.path.expandvars(os.path.expanduser(os.fspath(raw_source)))
        )
        try:
            source = Path(expanded).resolve(strict=True)
        except OSError as exc:
            raise ClaudeAuthProjectionError(
                "Selected Claude config home is unavailable"
            ) from exc
        try:
            source_info = source.lstat()
        except OSError as exc:
            raise ClaudeAuthProjectionError(
                "Selected Claude config home is unavailable"
            ) from exc
        if stat.S_ISLNK(source_info.st_mode) or not stat.S_ISDIR(source_info.st_mode):
            raise ClaudeAuthProjectionError(
                "Selected Claude config home must be a real directory"
            )
        oauth_access_token = _read_private_oauth_access_token(
            source / ".credentials.json"
        )

    return ClaudeAuthProjection(
        config_dir=projection,
        source_config_dir=source,
        uses_environment_auth=uses_environment_auth,
        oauth_access_token=oauth_access_token,
    )


def apply_claude_auth_projection(
    environment: MutableMapping[str, str],
    projection: ClaudeAuthProjection,
) -> None:
    """Route Claude to the clean home and disable ambient customization."""

    environment["CLAUDE_CONFIG_DIR"] = str(projection.config_dir)
    if projection.oauth_access_token is not None:
        environment["CLAUDE_CODE_OAUTH_TOKEN"] = projection.oauth_access_token
    environment.update(_AMBIENT_DISABLE_ENV)


def _remove_tree(path: Path) -> None:
    try:
        entries = list(os.scandir(path))
    except FileNotFoundError:
        return
    for entry in entries:
        info = entry.stat(follow_symlinks=False)
        child = Path(entry.path)
        if stat.S_ISDIR(info.st_mode) and not stat.S_ISLNK(info.st_mode):
            _remove_tree(child)
            child.rmdir()
        elif stat.S_ISREG(info.st_mode) and not stat.S_ISLNK(info.st_mode):
            child.unlink()
        else:
            raise ClaudeAuthProjectionError(
                f"Unsafe entry in Claude auth projection cleanup: {child}"
            )


def remove_claude_auth_projection(
    *,
    namespace: str,
    identifier: int,
    binding: str | None = None,
) -> None:
    """Delete every exact binding projection for one stopped workload id."""

    prefix = _projection_prefix(namespace, identifier)
    root = runtime_secret_root()
    _ensure_private_directory(root)
    exact_name = (
        _projection_name(namespace, identifier, binding)
        if binding is not None
        else None
    )
    for entry in os.scandir(root):
        if (
            entry.name != exact_name
            if exact_name is not None
            else not entry.name.startswith(prefix)
        ):
            continue
        info = entry.stat(follow_symlinks=False)
        projection = Path(entry.path)
        if (
            stat.S_ISLNK(info.st_mode)
            or not stat.S_ISDIR(info.st_mode)
            or info.st_uid != os.geteuid()
        ):
            raise ClaudeAuthProjectionError(
                f"Unsafe Claude auth projection during cleanup: {projection}"
            )
        _remove_tree(projection)
        projection.rmdir()
