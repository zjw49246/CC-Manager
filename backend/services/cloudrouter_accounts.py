"""Private on-disk API gateway accounts.

An API account deliberately looks like an ordinary Claude/Codex pool account:
it owns one ``CLAUDE_CONFIG_DIR`` and one ``CODEX_HOME``.  The API key is kept
outside both CLI configuration files and is only exposed through a small
credential helper.  The historical module/class names remain for compatibility
with existing CloudRouter installations; account metadata identifies the
actual gateway.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import re
import shlex
import shutil
import stat
import tempfile
import time
import tomllib
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import httpx

from backend.services.cancellation import finish_awaitable

logger = logging.getLogger(__name__)

CLAUDE_BASE_URL = "https://console.cloudrouter.online"
CODEX_BASE_URL = "https://console.cloudrouter.online/v1"
MODELS_URL = f"{CODEX_BASE_URL}/models"
USAGE_URL = f"{CODEX_BASE_URL}/usage"
APEX_CODEX_BASE_URL = "https://api.apexin.ai/v1"
APEX_CLAUDE_BASE_URL = "https://api.apexin.ai"
APEX_MODELS_URL = f"{APEX_CODEX_BASE_URL}/models"
APEX_USAGE_URL = f"{APEX_CODEX_BASE_URL}/usage"
APIBEST_CLAUDE_BASE_URL = "https://apibest.ai"
APIBEST_CODEX_BASE_URL = f"{APIBEST_CLAUDE_BASE_URL}/v1"
APIBEST_MODELS_URL = f"{APIBEST_CODEX_BASE_URL}/models"
APIBEST_PRICING_URL = f"{APIBEST_CLAUDE_BASE_URL}/api/pricing"
LEGACY_APEX_CODEX_BASE_URL = "https://35-75-22-186.sslip.io/v1"
LEGACY_APEX_ENDPOINTS = {
    "claude_base_url": None,
    "codex_base_url": LEGACY_APEX_CODEX_BASE_URL,
    "models_url": f"{LEGACY_APEX_CODEX_BASE_URL}/models",
    "usage_url": f"{LEGACY_APEX_CODEX_BASE_URL}/usage",
}
LEGACY_APEX_CODEX_ONLY_ENDPOINTS = {
    "claude_base_url": None,
    "codex_base_url": APEX_CODEX_BASE_URL,
    "models_url": APEX_MODELS_URL,
    "usage_url": APEX_USAGE_URL,
}
# Keep this aligned with the Codex CLI version pinned by scripts/setup.sh and
# WorkerProvisioner.  Apex exposes the native Codex model catalog endpoint,
# which requires the caller version in order to filter compatible models.
APEX_CODEX_CLIENT_VERSION = "0.147.0"
API_PROVIDER_CLOUDROUTER = "cloudrouter"
API_PROVIDER_APEX = "apex"
API_PROVIDER_APIBEST = "apibest"
APEX_CODEX_PROVIDER = "apexrouter"
# Existing installs may already have the pre-rename provider in their managed
# config. Accept only its exact CCM-owned shape and atomically rewrite it.
LEGACY_APEX_CODEX_PROVIDER = "apex_gateway"
LEGACY_APEX_LABEL = "Apex Gateway"
ENDPOINTS = {
    "claude_base_url": CLAUDE_BASE_URL,
    "codex_base_url": CODEX_BASE_URL,
    "models_url": MODELS_URL,
    "usage_url": USAGE_URL,
}


@dataclass(frozen=True, slots=True)
class ApiProviderSpec:
    id: str
    label: str
    account_prefix: str
    codex_provider: str
    codex_base_url: str
    models_url: str
    usage_url: str | None
    claude_base_url: str | None = None

    @property
    def endpoints(self) -> dict[str, str | None]:
        return {
            "claude_base_url": self.claude_base_url,
            "codex_base_url": self.codex_base_url,
            "models_url": self.models_url,
            "usage_url": self.usage_url,
        }


API_PROVIDER_SPECS = {
    API_PROVIDER_CLOUDROUTER: ApiProviderSpec(
        id=API_PROVIDER_CLOUDROUTER,
        label="CloudRouter",
        account_prefix="cloudrouter",
        codex_provider="cloudrouter",
        claude_base_url=CLAUDE_BASE_URL,
        codex_base_url=CODEX_BASE_URL,
        models_url=MODELS_URL,
        usage_url=USAGE_URL,
    ),
    API_PROVIDER_APEX: ApiProviderSpec(
        id=API_PROVIDER_APEX,
        label="ApexRouter",
        account_prefix="apex",
        codex_provider=APEX_CODEX_PROVIDER,
        claude_base_url=APEX_CLAUDE_BASE_URL,
        codex_base_url=APEX_CODEX_BASE_URL,
        models_url=APEX_MODELS_URL,
        usage_url=APEX_USAGE_URL,
    ),
    API_PROVIDER_APIBEST: ApiProviderSpec(
        id=API_PROVIDER_APIBEST,
        label="APIBest",
        account_prefix="apibest",
        codex_provider="apibest",
        claude_base_url=APIBEST_CLAUDE_BASE_URL,
        codex_base_url=APIBEST_CODEX_BASE_URL,
        models_url=APIBEST_MODELS_URL,
        usage_url=None,
    ),
}
ACCOUNT_ID_RE = re.compile(
    r"^(?P<provider>cloudrouter|apex|apibest)-(?P<number>[1-9][0-9]*)$"
)
MAX_METADATA_BYTES = 256 * 1024
MAX_API_RESPONSE_BYTES = 1024 * 1024
MAX_API_KEY_BYTES = 16 * 1024
MAX_DISCOVERED_MODELS = 1024
MAX_MODEL_ID_BYTES = 512
MAX_SERVICE_TIERS_PER_MODEL = 16
MAX_SERVICE_TIER_ID_BYTES = 64
MAX_CODEX_MODELS_CACHE_BYTES = 4 * 1024 * 1024
MAX_CODEX_MODELS_CACHE_MODELS = 2048
MAX_CLEANUP_CODE_BYTES = 64
MAX_CLEANUP_REASON_BYTES = 512
SERVICE_TIER_SOURCE_NONE = "none"
SERVICE_TIER_SOURCE_UPSTREAM = "upstream"
DEFAULT_HTTP_TIMEOUT = httpx.Timeout(15.0, connect=10.0)
DEFAULT_QUOTA_CACHE_TTL = 60.0
CLAUDE_SKIP_DANGEROUS_PROMPT = "skipDangerousModePermissionPrompt"


class CloudRouterAccountError(RuntimeError):
    """Base error for account storage and upstream validation."""


class CloudRouterAccountNotFound(CloudRouterAccountError):
    """The requested local API account does not exist."""


class CloudRouterAccountBusyError(CloudRouterAccountError):
    """The account still has a credential/runtime consumer."""

    def __init__(self, message: str, *, code: str = "cleanup_blocked"):
        self.code = _normalise_cleanup_code(code)
        self.reason = _sanitise_cleanup_reason(message)
        super().__init__(self.reason)


class CloudRouterUnsafePathError(CloudRouterAccountError):
    """A managed path failed a no-symlink/type/containment check."""


class CloudRouterUpstreamError(CloudRouterAccountError):
    """An API gateway rejected or could not complete a request."""

    def __init__(self, code: str, *, status_code: int | None = None):
        self.code = code
        self.status_code = status_code
        super().__init__(code)


def _now() -> float:
    return time.time()


def _normalise_cleanup_code(value: object) -> str:
    code = str(value or "").strip().lower()
    if (
        not re.fullmatch(r"[a-z][a-z0-9_]*", code)
        or len(code.encode("utf-8")) > MAX_CLEANUP_CODE_BYTES
    ):
        return "cleanup_blocked"
    return code


def _bounded_utf8(value: str, maximum: int) -> str:
    payload = value.encode("utf-8")
    if len(payload) <= maximum:
        return value
    return payload[:maximum].decode("utf-8", errors="ignore").rstrip()


def _sanitise_cleanup_reason(value: object) -> str:
    """Return a bounded, single-line administrator-safe cleanup reason."""

    reason = " ".join(str(value or "").split())
    reason = re.sub(
        r"(?i)\b(?:bearer\s+)?(?:sk|lck|cr)-[a-z0-9._-]{4,}\b",
        "[redacted]",
        reason,
    )
    reason = _bounded_utf8(reason, MAX_CLEANUP_REASON_BYTES)
    return reason or "API account cleanup is blocked"


def normalize_api_provider(value: str | None) -> str:
    provider = str(value or API_PROVIDER_CLOUDROUTER).strip().lower()
    if provider not in API_PROVIDER_SPECS:
        raise ValueError("Unknown API provider")
    return provider


def api_auth_kind(api_provider: str | None) -> str:
    return f"{normalize_api_provider(api_provider)}_api"


def is_api_auth_kind(value: str | None) -> bool:
    return str(value or "").lower() in {
        api_auth_kind(provider) for provider in API_PROVIDER_SPECS
    }


def _is_codex_managed_projects_state(value: Any) -> bool:
    """Accept only the project-trust shape Codex itself persists.

    Codex 0.144.6 writes the canonical Git root to ``config.toml`` when
    ``thread/start`` receives a writable sandbox.  Keep this CLI-owned state
    compatible without turning the managed user config into a general-purpose
    configuration surface.
    """

    if value is None:
        return True
    if not isinstance(value, dict):
        return False
    for project_path, project in value.items():
        if (
            not isinstance(project_path, str)
            or not project_path
            or "\x00" in project_path
            or not os.path.isabs(project_path)
            or os.path.normpath(project_path) != project_path
            or not isinstance(project, dict)
            or project.get("trust_level") not in {"trusted", "untrusted"}
            or set(project) != {"trust_level"}
        ):
            return False
    return True


def _mode(path: Path) -> int:
    return stat.S_IMODE(path.lstat().st_mode)


def _ensure_private_directory(path: Path, *, create: bool = True) -> None:
    """Create/check one managed directory without accepting a symlink."""

    for ancestor in path.parents:
        if ancestor.is_symlink():
            raise CloudRouterUnsafePathError(
                f"Managed directory has a symlink ancestor: {path}",
            )
    if not path.exists() and not path.is_symlink():
        if not create:
            raise CloudRouterUnsafePathError(f"Missing managed directory: {path}")
        path.mkdir(parents=True, mode=0o700)
    metadata = path.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise CloudRouterUnsafePathError(f"Unsafe managed directory: {path}")
    if metadata.st_uid != os.getuid():
        raise CloudRouterUnsafePathError(f"Managed directory has another owner: {path}")
    if _mode(path) != 0o700:
        os.chmod(path, 0o700, follow_symlinks=False)


def _open_regular_nofollow(path: Path, *, maximum: int) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise CloudRouterUnsafePathError(f"Not a regular file: {path}")
        if metadata.st_uid != os.getuid():
            raise CloudRouterUnsafePathError(f"Managed file has another owner: {path}")
        if metadata.st_size > maximum:
            raise CloudRouterUnsafePathError(f"Managed file is too large: {path}")
        chunks: list[bytes] = []
        remaining = maximum + 1
        while remaining:
            chunk = os.read(descriptor, min(65536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        if len(payload) > maximum:
            raise CloudRouterUnsafePathError(f"Managed file is too large: {path}")
        return payload
    finally:
        os.close(descriptor)


def codex_models_cache_service_tiers(
    codex_home: str | os.PathLike[str],
    allowed_models: list[str] | set[str],
) -> dict[str, list[str]]:
    """Read exact tier evidence from one bounded, non-symlink Codex catalog.

    This is candidate-routing evidence only. Fast turns still require the
    app-server's live ``model/list`` response before any thread is created.
    """

    allowed = {
        model
        for model in allowed_models
        if isinstance(model, str) and model
    }
    if not allowed:
        return {}
    try:
        payload = json.loads(
            _open_regular_nofollow(
                Path(codex_home) / "models_cache.json",
                maximum=MAX_CODEX_MODELS_CACHE_BYTES,
            ).decode("utf-8"),
        )
    except (
        CloudRouterUnsafePathError,
        OSError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ):
        return {}
    models = payload.get("models") if isinstance(payload, dict) else None
    if (
        not isinstance(models, list)
        or len(models) > MAX_CODEX_MODELS_CACHE_MODELS
    ):
        return {}

    result: dict[str, list[str]] = {}
    seen: set[str] = set()
    for item in models:
        if not isinstance(item, dict):
            return {}
        model = item.get("slug")
        if not isinstance(model, str):
            return {}
        if model in seen:
            return {}
        seen.add(model)
        if model not in allowed:
            continue
        raw_tiers = item.get("service_tiers")
        if (
            not isinstance(raw_tiers, list)
            or len(raw_tiers) > MAX_SERVICE_TIERS_PER_MODEL
        ):
            return {}
        tier_ids: set[str] = set()
        for tier in raw_tiers:
            tier_id = tier.get("id") if isinstance(tier, dict) else None
            if not isinstance(tier_id, str):
                return {}
            tier_id = tier_id.strip()
            if (
                not tier_id
                or len(tier_id.encode("utf-8"))
                > MAX_SERVICE_TIER_ID_BYTES
                or any(character.isspace() for character in tier_id)
            ):
                return {}
            tier_ids.add(tier_id)
        if tier_ids:
            result[model] = sorted(tier_ids)
    return result


def _require_owned_regular(path: Path, expected_mode: int) -> None:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise CloudRouterUnsafePathError(f"Missing managed file: {path}") from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or stat.S_IMODE(metadata.st_mode) != expected_mode
    ):
        raise CloudRouterUnsafePathError(f"Unsafe managed file: {path}")


def _converge_cli_mutable_private_file(path: Path) -> None:
    """Safely restore 0600 on an owned regular file a CLI may rewrite."""

    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise CloudRouterUnsafePathError(f"Missing managed file: {path}") from exc
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode) or opened.st_uid != os.getuid():
            raise CloudRouterUnsafePathError(f"Unsafe managed file: {path}")
        if stat.S_IMODE(opened.st_mode) != 0o600:
            os.fchmod(descriptor, 0o600)
            os.fsync(descriptor)
        converged = os.fstat(descriptor)
        current = path.stat(follow_symlinks=False)
        if (
            not stat.S_ISREG(converged.st_mode)
            or converged.st_uid != os.getuid()
            or stat.S_IMODE(converged.st_mode) != 0o600
            or not stat.S_ISREG(current.st_mode)
            or current.st_uid != os.getuid()
            or stat.S_IMODE(current.st_mode) != 0o600
            or current.st_dev != converged.st_dev
            or current.st_ino != converged.st_ino
        ):
            raise CloudRouterUnsafePathError(f"Unsafe managed file: {path}")
    finally:
        os.close(descriptor)


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _directory_open_flags() -> int:
    if not hasattr(os, "O_NOFOLLOW") or not hasattr(os, "O_DIRECTORY"):
        raise CloudRouterUnsafePathError(
            "Safe directory-descriptor traversal is unavailable",
        )
    return (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | os.O_NOFOLLOW
        | os.O_DIRECTORY
    )


def _open_directory_chain_nofollow(path: Path) -> int:
    """Open an absolute directory one component at a time without symlinks."""

    absolute = Path(os.path.abspath(os.fspath(path)))
    if not absolute.is_absolute() or not absolute.anchor:
        raise CloudRouterUnsafePathError("Managed directory must be absolute")
    flags = _directory_open_flags()
    descriptor = os.open(absolute.anchor, flags)
    try:
        for component in absolute.parts[1:]:
            child = os.open(component, flags, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _read_regular_at(
    parent_fd: int,
    name: str,
    *,
    maximum: int,
) -> bytes:
    """Read one owned regular child without following a replacement symlink."""

    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor = os.open(name, flags, dir_fd=parent_fd)
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_size > maximum
        ):
            raise CloudRouterUnsafePathError(
                f"Unsafe managed file: {name}",
            )
        chunks: list[bytes] = []
        remaining = maximum + 1
        while remaining:
            chunk = os.read(descriptor, min(65536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        if len(payload) > maximum:
            raise CloudRouterUnsafePathError(
                f"Managed file is too large: {name}",
            )
        return payload
    finally:
        os.close(descriptor)


def _atomic_private_json_at(
    parent_fd: int,
    name: str,
    value: dict[str, Any],
    *,
    maximum: int | None = None,
) -> None:
    """Atomically replace JSON relative to one already-verified directory."""

    payload = (json.dumps(value, ensure_ascii=False, indent=2) + "\n").encode()
    if maximum is not None and len(payload) > maximum:
        raise CloudRouterUpstreamError("metadata_too_large")
    temporary = f".{name}.{os.urandom(12).hex()}.tmp"
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor = os.open(temporary, flags, 0o600, dir_fd=parent_fd)
    try:
        os.fchmod(descriptor, 0o600)
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("Could not write account metadata")
            view = view[written:]
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        os.replace(
            temporary,
            name,
            src_dir_fd=parent_fd,
            dst_dir_fd=parent_fd,
        )
        os.fsync(parent_fd)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            os.unlink(temporary, dir_fd=parent_fd)
        except FileNotFoundError:
            pass


def _atomic_private_write(path: Path, payload: bytes, *, mode: int = 0o600) -> None:
    _ensure_private_directory(path.parent)
    if path.is_symlink():
        raise CloudRouterUnsafePathError(f"Refusing symlink target: {path}")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent,
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, mode)
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = -1
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        os.chmod(path, mode, follow_symlinks=False)
        _fsync_directory(path.parent)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def _atomic_private_json(
    path: Path,
    value: dict[str, Any],
    *,
    maximum: int | None = None,
) -> None:
    payload = (json.dumps(value, ensure_ascii=False, indent=2) + "\n").encode()
    if maximum is not None and len(payload) > maximum:
        raise CloudRouterUpstreamError("metadata_too_large")
    _atomic_private_write(path, payload)


def _codex_config_payload(
    spec: ApiProviderSpec,
    helper_path: Path,
    *,
    personality: str | None = None,
) -> bytes:
    """Render the complete CCM-owned Codex user configuration."""

    personality_line = (
        f"personality = {json.dumps(personality)}\n"
        if personality is not None
        else ""
    )
    provider_entries = [(spec.codex_provider, spec.label)]
    if spec.id == API_PROVIDER_APEX:
        # Native Codex rollouts and state_5.sqlite persist the provider id.
        # Keep the exact old route as an alias so pre-rename threads can resume
        # while every newly started thread defaults to ``apexrouter``.
        provider_entries.append((LEGACY_APEX_CODEX_PROVIDER, spec.label))
    provider_config = ""
    for provider_id, provider_label in provider_entries:
        provider_config += (
            f"[model_providers.{provider_id}]\n"
            f"name = {json.dumps(provider_label)}\n"
            f"base_url = {json.dumps(spec.codex_base_url)}\n"
            'wire_api = "responses"\n'
            "supports_websockets = false\n\n"
            f"[model_providers.{provider_id}.auth]\n"
            f"command = {json.dumps(str(helper_path))}\n"
            "timeout_ms = 5000\n"
            "refresh_interval_ms = 0\n\n"
        )
    config = (
        f"model_provider = {json.dumps(spec.codex_provider)}\n"
        f"{personality_line}\n"
        f"{provider_config}"
    )
    return config.encode("utf-8")


def _validate_account_id(account_id: str) -> str:
    if not isinstance(account_id, str) or not ACCOUNT_ID_RE.fullmatch(account_id):
        raise CloudRouterAccountNotFound("Unknown API account")
    return account_id


def _key_hint(api_key: str) -> str:
    if len(api_key) <= 8:
        return f"…{api_key[-2:]}"
    return f"{api_key[:3]}…{api_key[-4:]}"


def _claude_helper_command(account_root: Path) -> str:
    container_helper = "/home/sandbox/.ccm-api-account/key-helper"
    runtime_helper = account_root / "key-helper"
    return (
        f"if [ -x {shlex.quote(container_helper)} ]; then "
        f"{shlex.quote(container_helper)}; else "
        f"{shlex.quote(str(runtime_helper))}; fi"
    )


def _normalise_model(model: str) -> str:
    value = str(model or "").strip()
    if value.endswith("[1m]"):
        value = value[:-4]
    return value


def _provider_for_model(model: str) -> str | None:
    value = _normalise_model(model).lower()
    if value.startswith("claude-"):
        return "claude"
    if value.startswith(("gpt-", "o1", "o3", "o4", "codex-")):
        return "codex"
    return None


def _normalise_model_item_service_tiers(item: dict[str, Any]) -> list[str]:
    """Validate optional Codex service-tier capability metadata."""

    raw_tiers = item.get("service_tiers", [])
    if (
        not isinstance(raw_tiers, list)
        or len(raw_tiers) > MAX_SERVICE_TIERS_PER_MODEL
    ):
        raise CloudRouterUpstreamError("invalid_models_response")
    tier_ids: set[str] = set()
    for tier in raw_tiers:
        tier_id = tier.get("id") if isinstance(tier, dict) else None
        if not isinstance(tier_id, str):
            raise CloudRouterUpstreamError("invalid_models_response")
        tier_id = tier_id.strip()
        if (
            not tier_id
            or len(tier_id.encode("utf-8")) > MAX_SERVICE_TIER_ID_BYTES
            or any(character.isspace() for character in tier_id)
        ):
            raise CloudRouterUpstreamError("invalid_models_response")
        tier_ids.add(tier_id)
    return sorted(tier_ids)


def _normalise_models(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
        raise CloudRouterUpstreamError("invalid_models_response")
    if len(payload["data"]) > MAX_DISCOVERED_MODELS:
        raise CloudRouterUpstreamError("too_many_models")
    result: dict[str, Any] = {"claude": [], "codex": []}
    service_tiers: dict[str, list[str]] = {}
    seen: set[str] = set()
    for item in payload["data"]:
        model_id = item.get("id") if isinstance(item, dict) else None
        if not isinstance(model_id, str):
            continue
        model_id = model_id.strip()
        if (
            not model_id
            or len(model_id.encode("utf-8")) > MAX_MODEL_ID_BYTES
            or any(character.isspace() for character in model_id)
        ):
            raise CloudRouterUpstreamError("invalid_models_response")
        provider = _provider_for_model(model_id)
        if not provider or model_id in seen:
            continue
        seen.add(model_id)
        result[provider].append(model_id)
        if provider == "codex":
            tier_ids = _normalise_model_item_service_tiers(item)
            if tier_ids:
                service_tiers[model_id] = tier_ids
    for values in result.values():
        values.sort()
    if not any(result.values()):
        raise CloudRouterUpstreamError("no_supported_models")
    if service_tiers:
        result["service_tiers"] = service_tiers
    return result


def _normalise_apibest_pricing(payload: Any) -> dict[str, Any]:
    """Build APIBest's CCM catalog from its public pricing response.

    APIBest validates the token at ``/v1/models`` but currently returns an
    empty ``data`` array for valid credentials. Its pricing endpoint exposes
    the actual model ids and supported endpoint types, so use that catalog
    only after the authenticated models probe succeeds.
    """

    if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
        raise CloudRouterUpstreamError("invalid_models_response")
    items = payload["data"]
    if len(items) > MAX_DISCOVERED_MODELS:
        raise CloudRouterUpstreamError("too_many_models")
    result: dict[str, Any] = {"claude": [], "codex": []}
    service_tiers: dict[str, list[str]] = {}
    seen: set[str] = set()
    for item in items:
        model_id = item.get("model_name") if isinstance(item, dict) else None
        endpoint_types = (
            item.get("supported_endpoint_types", [])
            if isinstance(item, dict)
            else []
        )
        if not isinstance(model_id, str) or not isinstance(endpoint_types, list):
            continue
        if any(not isinstance(endpoint_type, str) for endpoint_type in endpoint_types):
            raise CloudRouterUpstreamError("invalid_models_response")
        model_id = model_id.strip()
        if (
            not model_id
            or len(model_id.encode("utf-8")) > MAX_MODEL_ID_BYTES
            or any(character.isspace() for character in model_id)
        ):
            raise CloudRouterUpstreamError("invalid_models_response")
        provider = _provider_for_model(model_id)
        if not provider or model_id in seen:
            continue
        compatible_endpoints = (
            {"anthropic"}
            if provider == "claude"
            else {"openai", "openai-response"}
        )
        if not compatible_endpoints.intersection(endpoint_types):
            continue
        seen.add(model_id)
        result[provider].append(model_id)
        if provider == "codex":
            tier_ids = _normalise_model_item_service_tiers(item)
            if tier_ids:
                service_tiers[model_id] = tier_ids
    for values in result.values():
        values.sort()
    if not any(result.values()):
        raise CloudRouterUpstreamError("no_supported_models")
    if service_tiers:
        result["service_tiers"] = service_tiers
    return result


def _normalise_apex_models(payload: Any) -> dict[str, Any]:
    """Normalise Apex's native or OpenAI-compatible model catalog response.

    Apex deployments may expose either the Codex client shape
    (``models[].slug``) or the OpenAI-compatible shape (``data[].id``).  The
    native shape additionally carries visibility and API support flags;
    hidden/internal and explicitly unsupported models must not become
    selectable CCM API models.
    """

    if not isinstance(payload, dict):
        raise CloudRouterUpstreamError("invalid_models_response")
    if "models" in payload:
        items = payload["models"]
        model_id_field = "slug"
        native_shape = True
    elif "data" in payload:
        items = payload["data"]
        model_id_field = "id"
        native_shape = False
    else:
        raise CloudRouterUpstreamError("invalid_models_response")
    if not isinstance(items, list):
        raise CloudRouterUpstreamError("invalid_models_response")
    if len(items) > MAX_DISCOVERED_MODELS:
        raise CloudRouterUpstreamError("too_many_models")
    result: dict[str, Any] = {"claude": [], "codex": []}
    service_tiers: dict[str, list[str]] = {}
    seen: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        if native_shape and (
            item.get("supported_in_api") is False
            or item.get("visibility") == "hide"
        ):
            continue
        model_id = item.get(model_id_field)
        if not isinstance(model_id, str):
            continue
        model_id = model_id.strip()
        if (
            not model_id
            or len(model_id.encode("utf-8")) > MAX_MODEL_ID_BYTES
            or any(character.isspace() for character in model_id)
        ):
            raise CloudRouterUpstreamError("invalid_models_response")
        provider = _provider_for_model(model_id)
        if provider is None or model_id in seen:
            continue
        seen.add(model_id)
        result[provider].append(model_id)
        if provider == "codex":
            tier_ids = _normalise_model_item_service_tiers(item)
            if tier_ids:
                service_tiers[model_id] = tier_ids
    for models in result.values():
        models.sort()
    if not any(result.values()):
        raise CloudRouterUpstreamError("no_supported_models")
    # This internal probe field is split into a top-level metadata field before
    # persistence; it never changes the public ``models`` shape.
    result["service_tiers"] = service_tiers
    return result


def _normalise_service_tiers(
    value: Any,
    codex_models: list[str],
    *,
    unsafe_metadata: bool,
) -> dict[str, list[str]]:
    """Validate a bounded model-to-tier capability map."""

    def invalid() -> Exception:
        if unsafe_metadata:
            return CloudRouterUnsafePathError(
                "Invalid API account service tier metadata"
            )
        return CloudRouterUpstreamError("invalid_models_response")

    if value is None:
        return {}
    if not isinstance(value, dict):
        raise invalid()
    available = set(codex_models)
    result: dict[str, list[str]] = {}
    for model, tiers in value.items():
        if (
            not isinstance(model, str)
            or model not in available
            or not isinstance(tiers, list)
            or len(tiers) > MAX_SERVICE_TIERS_PER_MODEL
        ):
            raise invalid()
        tier_ids: set[str] = set()
        for tier_id in tiers:
            if not isinstance(tier_id, str):
                raise invalid()
            tier_id = tier_id.strip()
            if (
                not tier_id
                or len(tier_id.encode("utf-8")) > MAX_SERVICE_TIER_ID_BYTES
                or any(character.isspace() for character in tier_id)
            ):
                raise invalid()
            tier_ids.add(tier_id)
        if tier_ids:
            result[model] = sorted(tier_ids)
    return result


def _split_model_probe(
    value: dict[str, Any],
) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """Separate the stable provider model map from optional tier metadata."""

    if not isinstance(value, dict):
        raise CloudRouterUpstreamError("invalid_models_response")
    models: dict[str, list[str]] = {}
    for provider in ("claude", "codex"):
        raw_models = value.get(provider)
        if not isinstance(raw_models, list) or any(
            not isinstance(model, str)
            or _provider_for_model(model) != provider
            for model in raw_models
        ):
            raise CloudRouterUpstreamError("invalid_models_response")
        models[provider] = sorted(set(raw_models))
    service_tiers = _normalise_service_tiers(
        value.get("service_tiers"),
        models["codex"],
        unsafe_metadata=False,
    )
    return models, service_tiers


def _decimal(value: Any) -> Decimal | None:
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result.is_finite() else None


def _json_number(value: Decimal | None) -> float | int | None:
    if value is None:
        return None
    if value == value.to_integral_value():
        return int(value)
    return float(value)


def _number(value: Any) -> float | int | None:
    return _json_number(_decimal(value))


def _window_numbers(
    raw_used: Any, raw_limit: Any, raw_remaining: Any = None,
) -> tuple[Decimal | None, Decimal | None, Decimal | None]:
    used = _decimal(raw_used)
    limit = _decimal(raw_limit)
    remaining = _decimal(raw_remaining)
    if remaining is None and used is not None and limit is not None:
        remaining = limit - used
    return used, limit, remaining


def _window(
    *,
    window_id: str,
    label: str,
    currency: str,
    raw_used: Any,
    raw_limit: Any,
    raw_remaining: Any = None,
    reset_at: Any = None,
) -> tuple[dict[str, Any], bool]:
    unlimited = _decimal(raw_remaining) == Decimal("-1")
    used, limit, remaining = _window_numbers(
        raw_used, raw_limit, raw_remaining,
    )
    item: dict[str, Any] = {
        "id": window_id,
        "label": label,
        "currency": currency,
    }
    for key, value in (("used", used), ("limit", limit), ("remaining", remaining)):
        if (parsed := _json_number(value)) is not None:
            item[key] = parsed
    if limit is not None and limit > 0 and used is not None:
        item["utilization"] = float((used / limit) * Decimal(100))
    if isinstance(reset_at, (str, int, float)):
        item["reset_at"] = reset_at
    if unlimited:
        item["unlimited"] = True
    exhausted = bool(
        not unlimited
        and
        limit is not None
        and limit > 0
        and (
            (remaining is not None and remaining <= 0)
            or (used is not None and used >= limit)
        )
    )
    return item, exhausted


def _usage_metrics(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    keys = (
        "requests", "input_tokens", "output_tokens", "total_tokens",
        "cache_creation_tokens", "cache_write_tokens", "cache_read_tokens",
        "cost", "actual_cost", "account_cost",
        "rpm", "tpm", "average_duration_ms",
    )
    result = {key: parsed for key in keys if (parsed := _number(value.get(key))) is not None}
    return result or None


def _usage_detail_record(value: Any) -> dict[str, Any] | None:
    """Return one bounded, JSON-safe usage breakdown row."""

    if not isinstance(value, dict):
        return None
    result: dict[str, Any] = {}
    for key in ("date", "model"):
        raw = value.get(key)
        if isinstance(raw, str) and raw.strip():
            result[key] = raw.strip()
    if metrics := _usage_metrics(value):
        result.update(metrics)
    return result or None


def _usage_details(value: Any) -> dict[str, Any] | list[dict[str, Any]] | None:
    """Normalise the list or keyed-map shapes used by CloudRouter details."""

    if isinstance(value, list):
        result = [
            record
            for item in value
            if (record := _usage_detail_record(item)) is not None
        ]
        return result or None
    if not isinstance(value, dict):
        return None
    if record := _usage_detail_record(value):
        return record
    result = {
        key: record
        for key, item in value.items()
        if isinstance(key, str)
        and (record := _usage_detail_record(item)) is not None
    }
    return result or None


def _normalise_usage(account_id: str, payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise CloudRouterUpstreamError("invalid_usage_response")

    upstream_status = str(payload.get("status") or "active").lower()
    mode = str(payload.get("mode") or "").lower()
    # CloudRouter reports Keys without independent spend or time limits as
    # ``unrestricted``. Their effective cap and remaining amount follow the
    # owning account. The zero balance/remaining fields are informational,
    # not an exhaustion signal or the owning account's numeric quota.
    if mode not in {"quota_limited", "subscription", "wallet", "unrestricted"}:
        mode = "subscription" if isinstance(payload.get("subscription"), dict) else "wallet"
    expired = upstream_status == "expired"
    exhausted = upstream_status in {"quota_exhausted", "exhausted"}
    invalid = payload.get("isValid") is False
    if payload.get("isValid") is False:
        if upstream_status == "active":
            upstream_status = "invalid"

    subscription = payload.get("subscription")
    subscription_uses_usd = isinstance(subscription, dict) and any(
        key.endswith("_usd") for key in subscription
    )
    currency = (
        "USD"
        if mode in {"quota_limited", "wallet", "unrestricted"} or subscription_uses_usd
        else "credits"
    )
    quota_value = payload.get("quota")
    quota: dict[str, Any] | None = None
    if isinstance(quota_value, dict):
        quota_unlimited = _decimal(quota_value.get("remaining")) == Decimal("-1")
        quota_used, quota_limit, quota_remaining = _window_numbers(
            quota_value.get("used"),
            quota_value.get("limit"),
            quota_value.get("remaining"),
        )
        quota = {}
        for key, value in (
            ("limit", quota_limit),
            ("used", quota_used),
            ("remaining", quota_remaining),
        ):
            if (parsed := _json_number(value)) is not None:
                quota[key] = parsed
        quota = quota or None
        if quota is not None:
            quota["currency"] = currency
            if quota_unlimited:
                quota["unlimited"] = True
        if (
            not quota_unlimited
            and
            quota_remaining is not None
            and quota_remaining <= 0
            and quota_limit is not None
            and quota_limit > 0
        ):
            exhausted = True
        elif (
            not quota_unlimited
            and
            quota_used is not None
            and quota_limit is not None
            and quota_limit > 0
            and quota_used >= quota_limit
        ):
            exhausted = True

    windows: list[dict[str, Any]] = []
    rate_limits = payload.get("rate_limits")
    if isinstance(rate_limits, list):
        for raw in rate_limits:
            if not isinstance(raw, dict):
                continue
            window = str(raw.get("window") or "").strip()
            if not window:
                continue
            item, window_exhausted = _window(
                window_id=window,
                label=window,
                currency=currency,
                raw_used=raw.get("used"),
                raw_limit=raw.get("limit"),
                raw_remaining=raw.get("remaining"),
                reset_at=raw.get("reset_at"),
            )
            windows.append(item)
            exhausted = exhausted or window_exhausted

    if isinstance(subscription, dict):
        suffix = "usd" if subscription_uses_usd else "credits"
        for prefix, label in (("daily", "1d"), ("weekly", "7d"), ("monthly", "30d")):
            raw_used = subscription.get(f"{prefix}_usage_{suffix}")
            raw_limit = subscription.get(f"{prefix}_limit_{suffix}")
            if _decimal(raw_used) is None and _decimal(raw_limit) is None:
                continue
            item, window_exhausted = _window(
                window_id=prefix,
                label=label,
                currency=currency,
                raw_used=raw_used,
                raw_limit=raw_limit,
            )
            windows.append(item)
            exhausted = exhausted or window_exhausted

    usage_value = payload.get("usage")
    usage: dict[str, Any] = {}
    if isinstance(usage_value, dict):
        for key in ("today", "total"):
            if metrics := _usage_metrics(usage_value.get(key)):
                usage[key] = metrics
        for key in ("rpm", "tpm", "average_duration_ms"):
            if (parsed := _number(usage_value.get(key))) is not None:
                usage[key] = parsed
    # The live CloudRouter response places these breakdowns at the top level,
    # while older/alternate responses may nest them under ``usage``. Keep the
    # public shape stable without forwarding arbitrary or non-finite JSON data.
    for key in ("model_stats", "daily_usage"):
        raw = payload.get(key)
        if raw is None and isinstance(usage_value, dict):
            raw = usage_value.get(key)
        if details := _usage_details(raw):
            usage[key] = details
    normalised_usage = usage or None

    balance_decimal = _decimal(payload.get("balance"))
    remaining_decimal = _decimal(payload.get("remaining"))
    def _wallet_depleted(value: Decimal | None) -> bool:
        # CloudRouter uses exactly -1 as the unlimited sentinel. Any other
        # non-positive finite balance is exhausted, including an overdrawn
        # wallet reported as a negative number.
        return value is not None and value != Decimal("-1") and value <= 0

    if mode == "wallet" and (
        _wallet_depleted(balance_decimal)
        or _wallet_depleted(remaining_decimal)
    ):
        exhausted = True

    if invalid:
        state = "error"
    elif expired:
        state = "expired"
    elif exhausted:
        state = "exhausted"
    else:
        state = "active"
    unavailable = state != "active"
    reason = upstream_status if unavailable and upstream_status != "active" else state
    snapshot: dict[str, Any] = {
        "account_id": account_id,
        "fetched_at": _now(),
        "stale": False,
        "state": state,
        "status": state,
        "mode": mode,
        "currency": currency,
        "unit": currency,
        "quota": quota,
        "windows": windows,
        "usage": normalised_usage,
        "available": not unavailable,
        "known": True,
        "reason": reason,
    }
    if mode == "unrestricted":
        # This Key has no independent spend cap or expiry; its effective limit
        # follows the owning account. CloudRouter returns informational zero
        # balance/remaining values here, so never expose them as real money.
        snapshot["unlimited"] = True
    aliases = {
        "balance": ("balance",),
        "remaining": ("remaining",),
        "expires_at": ("expires_at", "expiry", "expiresAt"),
        "days_until_expiry": ("days_until_expiry", "daysUntilExpiry"),
    }
    for key, source_keys in aliases.items():
        if mode == "unrestricted" and key in {"balance", "remaining"}:
            continue
        raw = next(
            (payload.get(source) for source in source_keys if payload.get(source) is not None),
            None,
        )
        if raw is None and isinstance(subscription, dict):
            raw = next(
                (
                    subscription.get(source)
                    for source in source_keys
                    if subscription.get(source) is not None
                ),
                None,
            )
        if key in {"expires_at"} and isinstance(raw, (str, int, float)):
            snapshot[key] = raw
        elif key == "days_until_expiry" and (parsed := _number(raw)) is not None:
            snapshot[key] = parsed
        elif key in {"balance", "remaining"} and (parsed := _number(raw)) is not None:
            snapshot[key] = parsed
    plan_name = payload.get("planName", payload.get("plan_name"))
    if plan_name is None and isinstance(subscription, dict):
        plan_name = subscription.get(
            "planName", subscription.get("plan_name"),
        )
    if isinstance(plan_name, str):
        snapshot["plan_name"] = plan_name
    return snapshot


def _normalise_apex_usage(
    account_id: str,
    payload: Any,
) -> dict[str, Any]:
    """Keep per-Key usage separate from the shared Apex group limits."""

    if not isinstance(payload, dict):
        raise CloudRouterUpstreamError("invalid_usage_response")
    raw_used = payload.get("used")
    raw_remaining = payload.get("remaining")
    raw_limits = payload.get("limits")
    if not all(
        isinstance(value, dict)
        for value in (raw_used, raw_remaining, raw_limits)
    ):
        raise CloudRouterUpstreamError("invalid_usage_response")

    definitions = (
        ("requests_5h", "5h 请求（分组共享）", "requests"),
        ("requests_day", "每日请求（分组共享）", "requests"),
        ("tokens_day", "每日 Tokens（分组共享）", "tokens"),
        ("tokens_month", "每月 Tokens（分组共享）", "tokens"),
    )
    windows: list[dict[str, Any]] = []
    key_usage: dict[str, float | int] = {}
    exhausted = False
    for window_id, label, unit in definitions:
        key_used = _decimal(raw_used.get(window_id))
        has_remaining = window_id in raw_remaining
        has_limit = window_id in raw_limits
        raw_window_remaining = raw_remaining.get(window_id)
        raw_window_limit = raw_limits.get(window_id)
        unlimited = (
            has_remaining
            and has_limit
            and raw_window_remaining is None
            and raw_window_limit is None
        )
        # Availability is governed by the shared group, so a partial response
        # must never become a known-healthy snapshot based only on this Key's
        # own usage. Apex documents every fixed window, using explicit null
        # limit/remaining pairs for windows without a quota.
        if (
            key_used is None
            or key_used < 0
            or not has_remaining
            or not has_limit
        ):
            raise CloudRouterUpstreamError("invalid_usage_response")
        if unlimited:
            parsed_key_used = _json_number(key_used)
            item = {
                "id": window_id,
                "label": label,
                "currency": unit,
                "scope": "group",
                "unlimited": True,
                "key_used": parsed_key_used,
            }
            windows.append(item)
            if parsed_key_used is not None:
                key_usage[window_id] = parsed_key_used
            continue

        remaining = _decimal(raw_window_remaining)
        limit = _decimal(raw_window_limit)
        if (
            remaining is None
            or limit is None
            or remaining < 0
            or limit < 0
            or remaining > limit
        ):
            raise CloudRouterUpstreamError("invalid_usage_response")
        group_used = limit - remaining
        item, window_exhausted = _window(
            window_id=window_id,
            label=label,
            currency=unit,
            raw_used=group_used,
            raw_limit=limit,
            raw_remaining=remaining,
        )
        item["scope"] = "group"
        if (parsed_key_used := _json_number(key_used)) is not None:
            item["key_used"] = parsed_key_used
            key_usage[window_id] = parsed_key_used
        windows.append(item)
        exhausted = (
            exhausted
            or window_exhausted
            or remaining <= 0
        )

    concurrency_decimal = _decimal(raw_limits.get("concurrency"))
    if concurrency_decimal is None or concurrency_decimal < 0:
        raise CloudRouterUpstreamError("invalid_usage_response")
    concurrency = _json_number(concurrency_decimal)
    exhausted = exhausted or concurrency_decimal <= 0
    key_name = payload.get("key_name")
    group_name = payload.get("group_name")
    state = "exhausted" if exhausted else "active"
    snapshot: dict[str, Any] = {
        "account_id": account_id,
        "fetched_at": _now(),
        "stale": False,
        "state": state,
        "status": state,
        "mode": "shared_group",
        "currency": None,
        "unit": None,
        "quota": None,
        "windows": windows,
        "usage": {"key": key_usage},
        "key_usage": key_usage,
        "available": not exhausted,
        "known": True,
        "reason": state,
    }
    if isinstance(key_name, str) and key_name.strip():
        snapshot["key_name"] = key_name.strip()
    if isinstance(group_name, str) and group_name.strip():
        snapshot["group_name"] = group_name.strip()
    if concurrency is not None:
        snapshot["concurrency"] = concurrency
    return snapshot


def _unknown_snapshot(
    account_id: str,
    reason: str,
    *,
    previous: dict[str, Any] | None = None,
) -> dict[str, Any]:
    result = dict(previous or {})
    checked_at = _now()
    if previous:
        result["last_known_available"] = previous.get(
            "last_known_available", previous.get("available"),
        )
        result["last_known_reason"] = previous.get(
            "last_known_reason", previous.get("reason"),
        )
        # Keep the timestamp attached to the retained quota/usage payload.
        # A failed refresh must not make old data look freshly fetched.
        result["refresh_failed_at"] = checked_at
    else:
        result["fetched_at"] = checked_at
    result.update({
        "account_id": account_id,
        "stale": bool(previous),
        "state": "unknown",
        "status": "unknown",
        "available": True,
        "known": False,
        "reason": reason,
    })
    return result


def _unavailable_snapshot(account_id: str, reason: str) -> dict[str, Any]:
    return {
        "account_id": account_id,
        "fetched_at": _now(),
        "stale": False,
        "state": "error",
        "status": "unavailable",
        "mode": None,
        "currency": None,
        "unit": None,
        "quota": None,
        "windows": [],
        "usage": None,
        "available": False,
        "known": True,
        "reason": reason,
    }


def _cleanup_diagnostics_from_metadata(
    data: dict[str, Any],
    account_id: str,
) -> tuple[str | None, str | None, float | None, float | None]:
    """Validate the bounded cleanup diagnostic fields on one tombstone."""

    code = data.get("cleanup_code")
    reason = data.get("cleanup_reason")
    last_attempt_at = data.get("cleanup_last_attempt_at")
    last_error_at = data.get("cleanup_last_error_at")
    if code is not None and (
        not isinstance(code, str)
        or _normalise_cleanup_code(code) != code
        or len(code.encode("utf-8")) > MAX_CLEANUP_CODE_BYTES
    ):
        raise CloudRouterUnsafePathError(
            f"Invalid cleanup metadata: {account_id}",
        )
    if reason is not None and (
        not isinstance(reason, str)
        or not reason
        or _sanitise_cleanup_reason(reason) != reason
        or len(reason.encode("utf-8")) > MAX_CLEANUP_REASON_BYTES
    ):
        raise CloudRouterUnsafePathError(
            f"Invalid cleanup metadata: {account_id}",
        )
    for value in (last_attempt_at, last_error_at):
        if value is not None and (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
            or float(value) < 0
        ):
            raise CloudRouterUnsafePathError(
                f"Invalid cleanup metadata: {account_id}",
            )
    has_error = (code is not None, reason is not None, last_error_at is not None)
    if len(set(has_error)) != 1 or (any(has_error) and last_attempt_at is None):
        raise CloudRouterUnsafePathError(
            f"Inconsistent cleanup metadata: {account_id}",
        )
    cleanup_pending = bool(data.get("cleanup_pending", False))
    if not cleanup_pending and (
        any(has_error) or last_attempt_at is not None
    ):
        raise CloudRouterUnsafePathError(
            f"Cleanup metadata on completed account: {account_id}",
        )
    return (
        code,
        reason,
        float(last_attempt_at) if last_attempt_at is not None else None,
        float(last_error_at) if last_error_at is not None else None,
    )


@dataclass(frozen=True, slots=True)
class CloudRouterAccount:
    id: str
    name: str
    api_provider: str
    enabled: bool
    retired: bool
    cleanup_pending: bool
    cleanup_code: str | None
    cleanup_reason: str | None
    cleanup_last_attempt_at: float | None
    cleanup_last_error_at: float | None
    models: dict[str, list[str]]
    service_tiers: dict[str, list[str]]
    service_tiers_explicit: bool
    key_hint: str
    root: Path

    @property
    def claude_config_dir(self) -> str:
        return str(self.root / "claude")

    @property
    def codex_home(self) -> str:
        return str(self.root / "codex")

    @property
    def providers(self) -> list[str]:
        return [provider for provider in ("claude", "codex") if self.models.get(provider)]

    @property
    def auth_kind(self) -> str:
        return api_auth_kind(self.api_provider)

    @property
    def provider_label(self) -> str:
        return API_PROVIDER_SPECS[self.api_provider].label

    def supports_model(self, provider: str, model: str | None) -> bool:
        provider = str(provider or "").lower()
        if provider not in {"claude", "codex"}:
            return False
        requested = _normalise_model(model)
        if not requested or requested == "default":
            return bool(self.models.get(provider))
        available = {
            _normalise_model(item) for item in self.models.get(provider, [])
        }
        if requested in available:
            return True
        # Anthropic publishes immutable dated model IDs while CCM exposes the
        # corresponding stable short alias.  Accept only the exact alias plus
        # one YYYYMMDD suffix; a generic prefix match would incorrectly route
        # similarly named but distinct models.
        if provider == "claude":
            dated = re.compile(rf"^{re.escape(requested)}-[0-9]{{8}}$")
            return any(dated.fullmatch(candidate) for candidate in available)
        return False

    def supports_service_tier(
        self,
        provider: str,
        model: str | None,
        service_tier: str | None,
    ) -> bool:
        """Return exact advertised support; unknown Fast capability is false."""

        requested_tier = str(service_tier or "default").strip().lower()
        if requested_tier == "default":
            return self.supports_model(provider, model)
        if requested_tier != "priority" or str(provider or "").lower() != "codex":
            return False
        requested_model = _normalise_model(model)
        if not requested_model or requested_model == "default":
            # Apex's catalog currently does not mark its default model.  Do
            # not guess and accidentally route a Fast task to an unsupported
            # default such as a mini/Spark model.
            return False
        return requested_tier in self.service_tiers.get(requested_model, [])

    def public_dict(self) -> dict[str, Any]:
        supported_models = sorted({
            model for values in self.models.values() for model in values
        })
        return {
            "id": self.id,
            "name": self.name,
            "api_provider": self.api_provider,
            "auth_kind": self.auth_kind,
            "enabled": self.enabled,
            "retired": self.retired,
            "cleanup_pending": self.cleanup_pending,
            "cleanup_code": self.cleanup_code,
            "cleanup_reason": self.cleanup_reason,
            "cleanup_last_attempt_at": self.cleanup_last_attempt_at,
            "cleanup_last_error_at": self.cleanup_last_error_at,
            "models": self.models,
            "service_tiers": self.service_tiers,
            "providers": self.providers,
            "key_hint": self.key_hint,
            "account_dir": str(self.root),
            "claude_config_dir": self.claude_config_dir,
            "codex_home": self.codex_home,
            "supported_models": supported_models,
            "endpoints": dict(
                API_PROVIDER_SPECS[self.api_provider].endpoints
            ),
        }


KEY_HELPER = r"""#!/usr/bin/env python3
import os
import stat
import sys
from pathlib import Path

path = Path(__file__).with_name("api.key")
flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
try:
    descriptor = os.open(path, flags)
    metadata = os.fstat(descriptor)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or stat.S_IMODE(metadata.st_mode) != 0o600
    ):
        raise RuntimeError("API key file must be a private regular file")
    payload = os.read(descriptor, 16385)
finally:
    if "descriptor" in locals():
        os.close(descriptor)
if not payload or len(payload) > 16384 or b"\n" in payload or b"\r" in payload:
    raise RuntimeError("Invalid API key file")
sys.stdout.write(payload.decode("utf-8"))
"""


class CloudRouterAccountStore:
    """Manage API-gateway accounts under one caller-selected directory."""

    def __init__(
        self,
        root: str | os.PathLike[str],
        *,
        quota_cache_ttl: float = DEFAULT_QUOTA_CACHE_TTL,
        http_timeout: httpx.Timeout | float = DEFAULT_HTTP_TIMEOUT,
    ):
        raw_root = Path(os.path.expandvars(os.path.expanduser(os.fspath(root))))
        self.root = raw_root.absolute()
        _ensure_private_directory(self.root)
        root_fd = _open_directory_chain_nofollow(self.root)
        try:
            root_metadata = os.fstat(root_fd)
            self._root_identity = (
                root_metadata.st_dev,
                root_metadata.st_ino,
            )
        finally:
            os.close(root_fd)
        self._quota_cache_ttl = max(0.0, float(quota_cache_ttl))
        self._http_timeout = http_timeout
        self._accounts: dict[str, CloudRouterAccount] = {}
        self._quota_cache: dict[str, dict[str, Any]] = {}
        self._quota_cached_at: dict[str, float] = {}
        self._mutation_lock = asyncio.Lock()
        # DELETE is intentionally split into durable disable, runtime
        # quiescence, and cleanup phases. Keep those phases serialized per
        # account without holding the store mutation lock across lifecycle
        # locks (normal launch uses lifecycle -> store ordering).
        self._retirement_locks: dict[str, asyncio.Lock] = {}
        # Upstream usage reads retain the API key in memory after the local
        # file has been opened. Track that full request lifetime so retirement
        # never removes a credential while it is still being used.
        self._credential_users: dict[str, int] = {}
        self.reload()

    def _open_store_root_fd(self) -> int:
        try:
            descriptor = _open_directory_chain_nofollow(self.root)
        except (CloudRouterAccountError, OSError) as exc:
            raise CloudRouterUnsafePathError(
                "API account store root is unsafe",
            ) from exc
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or stat.S_IMODE(metadata.st_mode) != 0o700
            or (metadata.st_dev, metadata.st_ino) != self._root_identity
        ):
            os.close(descriptor)
            raise CloudRouterUnsafePathError(
                "API account store root changed identity",
            )
        return descriptor

    @contextmanager
    def _open_account_fd(self, account_id: str):
        """Anchor one account below the exact store-root inode."""

        valid_id = _validate_account_id(account_id)
        root_fd = self._open_store_root_fd()
        account_fd = -1
        try:
            try:
                account_fd = os.open(
                    valid_id,
                    _directory_open_flags(),
                    dir_fd=root_fd,
                )
            except OSError as exc:
                raise CloudRouterUnsafePathError(
                    f"Unsafe API account directory: {valid_id}",
                ) from exc
            metadata = os.fstat(account_fd)
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or metadata.st_uid != os.getuid()
                or stat.S_IMODE(metadata.st_mode) != 0o700
            ):
                raise CloudRouterUnsafePathError(
                    f"Unsafe API account directory: {valid_id}",
                )
            yield root_fd, account_fd
        finally:
            if account_fd >= 0:
                os.close(account_fd)
            os.close(root_fd)

    @staticmethod
    def _assert_account_fd_current(
        root_fd: int,
        account_fd: int,
        account_id: str,
    ) -> None:
        """Require the account name to still reference this exact directory."""

        try:
            current = os.stat(
                account_id,
                dir_fd=root_fd,
                follow_symlinks=False,
            )
        except OSError as exc:
            raise CloudRouterUnsafePathError(
                f"API account directory changed: {account_id}",
            ) from exc
        opened = os.fstat(account_fd)
        if (
            not stat.S_ISDIR(current.st_mode)
            or stat.S_ISLNK(current.st_mode)
            or current.st_dev != opened.st_dev
            or current.st_ino != opened.st_ino
        ):
            raise CloudRouterUnsafePathError(
                f"API account directory changed: {account_id}",
            )

    def _account_root(self, account_id: str) -> Path:
        valid = _validate_account_id(account_id)
        candidate = self.root / valid
        if candidate.parent != self.root:
            raise CloudRouterUnsafePathError("Account path escaped its store root")
        return candidate

    def _load_account(self, path: Path) -> CloudRouterAccount:
        _ensure_private_directory(path, create=False)
        account_id = _validate_account_id(path.name)
        metadata_path = path / "account.json"
        _require_owned_regular(metadata_path, 0o600)
        try:
            data = json.loads(
                _open_regular_nofollow(
                    metadata_path, maximum=MAX_METADATA_BYTES,
                ).decode("utf-8"),
            )
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CloudRouterUnsafePathError(
                f"Invalid account metadata: {account_id}",
            ) from exc
        if not isinstance(data, dict) or data.get("id") != account_id:
            raise CloudRouterUnsafePathError(f"Mismatched account metadata: {account_id}")
        try:
            api_provider = normalize_api_provider(data.get("api_provider"))
        except ValueError as exc:
            raise CloudRouterUnsafePathError(
                f"Invalid API provider metadata: {account_id}"
            ) from exc
        match = ACCOUNT_ID_RE.fullmatch(account_id)
        spec = API_PROVIDER_SPECS[api_provider]
        if match is None or match.group("provider") != spec.account_prefix:
            raise CloudRouterUnsafePathError(
                f"Mismatched API provider metadata: {account_id}"
            )
        migrate_legacy_apex_endpoints = (
            api_provider == API_PROVIDER_APEX
            and data.get("endpoints") == LEGACY_APEX_ENDPOINTS
        )
        migrate_apex_claude_runtime = (
            api_provider == API_PROVIDER_APEX
            and data.get("endpoints") in (
                LEGACY_APEX_ENDPOINTS,
                LEGACY_APEX_CODEX_ONLY_ENDPOINTS,
            )
        )
        if (
            data.get("endpoints") != spec.endpoints
            and not migrate_legacy_apex_endpoints
            and not migrate_apex_claude_runtime
        ):
            raise CloudRouterUnsafePathError(f"Modified fixed endpoints: {account_id}")
        name = data.get("name")
        models = data.get("models")
        if not isinstance(name, str) or not name.strip() or not isinstance(models, dict):
            raise CloudRouterUnsafePathError(f"Invalid account metadata: {account_id}")
        normalised_models = {
            provider: sorted({
                str(model) for model in models.get(provider, [])
                if isinstance(model, str) and _provider_for_model(model) == provider
            })
            for provider in ("claude", "codex")
        }
        service_tiers = _normalise_service_tiers(
            data.get("service_tiers"),
            normalised_models["codex"],
            unsafe_metadata=True,
        )
        service_tiers_explicit = "service_tiers" in data
        if (
            api_provider == API_PROVIDER_APEX
            and not service_tiers_explicit
        ):
            # Older ApexRouter metadata predates capability persistence. Use
            # only its own exact, bounded local catalog as candidate evidence;
            # an explicit (even empty) metadata field remains authoritative.
            service_tiers = codex_models_cache_service_tiers(
                path / "codex",
                normalised_models["codex"],
            )
        elif api_provider != API_PROVIDER_APEX:
            service_tier_source = data.get("service_tiers_source")
            if service_tier_source is None:
                # Generic API metadata predating source provenance could only
                # contain an empty map in released CCM builds. Ignore any
                # non-empty value left by an intermediate development build;
                # it never proved account-specific Fast support.
                service_tiers = {}
            elif service_tier_source == SERVICE_TIER_SOURCE_NONE:
                if service_tiers:
                    raise CloudRouterUnsafePathError(
                        f"Inconsistent service tier metadata: {account_id}"
                    )
            elif service_tier_source != SERVICE_TIER_SOURCE_UPSTREAM:
                raise CloudRouterUnsafePathError(
                    f"Invalid service tier source metadata: {account_id}"
                )
        (
            cleanup_code,
            cleanup_reason,
            cleanup_last_attempt_at,
            cleanup_last_error_at,
        ) = _cleanup_diagnostics_from_metadata(data, account_id)
        account = CloudRouterAccount(
            id=account_id,
            name=name,
            api_provider=api_provider,
            enabled=bool(data.get("enabled", True)) and not bool(data.get("retired", False)),
            retired=bool(data.get("retired", False)),
            cleanup_pending=bool(data.get("cleanup_pending", False)),
            cleanup_code=cleanup_code,
            cleanup_reason=cleanup_reason,
            cleanup_last_attempt_at=cleanup_last_attempt_at,
            cleanup_last_error_at=cleanup_last_error_at,
            models=normalised_models,
            service_tiers=service_tiers,
            service_tiers_explicit=service_tiers_explicit,
            key_hint=str(data.get("key_hint") or ""),
            root=path,
        )
        for directory in (path / "claude", path / "codex"):
            _ensure_private_directory(directory, create=False)
        if account.retired:
            for preserved in (
                path / "claude" / "projects",
                path / "codex" / "sessions",
            ):
                if preserved.exists() or preserved.is_symlink():
                    _ensure_private_directory(preserved, create=False)
        else:
            for file_name, expected_mode in (
                ("account.json", 0o600), ("api.key", 0o600), ("key-helper", 0o700),
            ):
                _require_owned_regular(path / file_name, expected_mode)
            _require_owned_regular(path / "codex" / "config.toml", 0o600)
            if migrate_apex_claude_runtime:
                # A failed legacy migration must not leave a partially
                # materialized Claude home. Validate every existing Claude
                # artifact and the Codex runtime before the first write.
                self._migrate_apex_claude_runtime(
                    account,
                    write_missing=False,
                )
                self._validate_runtime_configuration(
                    account,
                    validate_claude=False,
                    apply_migrations=False,
                )
                self._migrate_apex_claude_runtime(account)
            if spec.claude_base_url is not None:
                _require_owned_regular(
                    path / "claude" / "settings.json", 0o600
                )
                self._converge_claude_runtime_settings(account)
                _converge_cli_mutable_private_file(
                    path / "claude" / ".claude.json",
                )
            self._validate_runtime_configuration(account)
        if migrate_legacy_apex_endpoints or migrate_apex_claude_runtime:
            data["endpoints"] = dict(spec.endpoints)
            try:
                _atomic_private_json(
                    metadata_path,
                    data,
                    maximum=MAX_METADATA_BYTES,
                )
            except CloudRouterAccountError:
                raise
            except OSError as exc:
                raise CloudRouterUnsafePathError(
                    f"Could not migrate fixed endpoints: {account_id}",
                ) from exc
        return account

    @staticmethod
    def _migrate_apex_claude_runtime(
        account: CloudRouterAccount,
        *,
        write_missing: bool = True,
    ) -> None:
        """Materialize the newly supported Claude runtime for legacy Apex.

        The migration is admitted only from an exact CCM-owned endpoint
        snapshot. Existing files must already match the expected safe payload;
        arbitrary or redirected Claude configuration is never overwritten.
        """

        if account.api_provider != API_PROVIDER_APEX:
            raise CloudRouterUnsafePathError(
                f"Invalid Apex Claude migration: {account.id}",
            )
        expected_files = {
            account.root / "claude" / "settings.json": {
                "env": {"ANTHROPIC_BASE_URL": APEX_CLAUDE_BASE_URL},
                "apiKeyHelper": _claude_helper_command(account.root),
                CLAUDE_SKIP_DANGEROUS_PROMPT: True,
            },
            account.root / "claude" / ".claude.json": {
                "hasCompletedOnboarding": True,
            },
        }
        for path, expected in expected_files.items():
            if path.exists() or path.is_symlink():
                _require_owned_regular(path, 0o600)
                try:
                    current = json.loads(_open_regular_nofollow(
                        path,
                        maximum=MAX_METADATA_BYTES,
                    ).decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise CloudRouterUnsafePathError(
                        f"Invalid legacy Apex Claude config: {account.id}",
                    ) from exc
                if current != expected:
                    raise CloudRouterUnsafePathError(
                        f"Modified legacy Apex Claude config: {account.id}",
                    )
                continue
            if write_missing:
                _atomic_private_json(path, expected)

    @staticmethod
    def _converge_claude_runtime_settings(
        account: CloudRouterAccount,
    ) -> None:
        """Upgrade managed Claude settings needed for unattended launches.

        Claude Code exits when ``--dangerously-skip-permissions`` is used for
        the first time unless this acknowledgement is already present.  API
        accounts are intentionally non-interactive, so migrate older account
        folders while preserving CCM-owned hooks and other harmless CLI state.
        Routing and credential-helper fields are verified before any rewrite.
        """

        settings_path = account.root / "claude" / "settings.json"
        try:
            settings = json.loads(_open_regular_nofollow(
                settings_path,
                maximum=MAX_METADATA_BYTES,
            ).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CloudRouterUnsafePathError(
                f"Invalid Claude settings: {account.id}",
            ) from exc
        expected_base_url = API_PROVIDER_SPECS[account.api_provider].claude_base_url
        if (
            not isinstance(settings, dict)
            or settings.get("env")
            != {"ANTHROPIC_BASE_URL": expected_base_url}
            or settings.get("apiKeyHelper")
            != _claude_helper_command(account.root)
        ):
            raise CloudRouterUnsafePathError(
                f"Modified Claude API routing: {account.id}",
            )
        if settings.get(CLAUDE_SKIP_DANGEROUS_PROMPT) is not True:
            settings[CLAUDE_SKIP_DANGEROUS_PROMPT] = True
            _atomic_private_json(settings_path, settings)

    @staticmethod
    def _validate_runtime_configuration(
        account: CloudRouterAccount,
        *,
        validate_claude: bool = True,
        apply_migrations: bool = True,
    ) -> None:
        """Fail closed if a CLI config could redirect or replace API auth."""

        helper_path = account.root / "key-helper"
        try:
            helper_payload = _open_regular_nofollow(
                helper_path, maximum=len(KEY_HELPER.encode("utf-8")),
            )
        except CloudRouterUnsafePathError as exc:
            raise CloudRouterUnsafePathError(
                f"Modified API credential helper: {account.id}",
            ) from exc
        if helper_payload != KEY_HELPER.encode("utf-8"):
            raise CloudRouterUnsafePathError(
                f"Modified API credential helper: {account.id}",
            )

        spec = API_PROVIDER_SPECS[account.api_provider]
        if spec.claude_base_url is not None and validate_claude:
            try:
                settings = json.loads(_open_regular_nofollow(
                    account.root / "claude" / "settings.json",
                    maximum=MAX_METADATA_BYTES,
                ).decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise CloudRouterUnsafePathError(
                    f"Invalid Claude settings: {account.id}",
                ) from exc
            if (
                not isinstance(settings, dict)
                or settings.get("env")
                != {"ANTHROPIC_BASE_URL": spec.claude_base_url}
                or settings.get("apiKeyHelper")
                != _claude_helper_command(account.root)
                or settings.get(CLAUDE_SKIP_DANGEROUS_PROMPT) is not True
            ):
                raise CloudRouterUnsafePathError(
                    f"Modified Claude API routing: {account.id}",
                )

            try:
                onboarding = json.loads(_open_regular_nofollow(
                    account.root / "claude" / ".claude.json",
                    maximum=MAX_METADATA_BYTES,
                ).decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise CloudRouterUnsafePathError(
                    f"Invalid Claude onboarding state: {account.id}",
                ) from exc
            if (
                not isinstance(onboarding, dict)
                or onboarding.get("hasCompletedOnboarding") is not True
            ):
                raise CloudRouterUnsafePathError(
                    f"Invalid Claude onboarding state: {account.id}",
                )

        try:
            codex = tomllib.loads(_open_regular_nofollow(
                account.root / "codex" / "config.toml",
                maximum=MAX_METADATA_BYTES,
            ).decode("utf-8"))
        except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
            raise CloudRouterUnsafePathError(
                f"Invalid Codex configuration: {account.id}",
            ) from exc
        expected_provider = {
            "name": spec.label,
            "base_url": spec.codex_base_url,
            "wire_api": "responses",
            "supports_websockets": False,
            "auth": {
                "command": str(account.root / "key-helper"),
                "timeout_ms": 5000,
                "refresh_interval_ms": 0,
            },
        }
        expected_providers = {
            spec.codex_provider: expected_provider,
        }
        if account.api_provider == API_PROVIDER_APEX:
            expected_providers[LEGACY_APEX_CODEX_PROVIDER] = {
                **expected_provider,
            }
        expected_codex = {
            "model_provider": spec.codex_provider,
            "model_providers": expected_providers,
        }
        legacy_apex_codex = None
        legacy_apex_endpoint_codex = None
        legacy_apex_endpoint_legacy_codex = None
        if account.api_provider == API_PROVIDER_APEX:
            legacy_apex_codex = {
                "model_provider": LEGACY_APEX_CODEX_PROVIDER,
                "model_providers": {
                    LEGACY_APEX_CODEX_PROVIDER: {
                        **expected_provider,
                        "name": LEGACY_APEX_LABEL,
                    },
                },
            }
            legacy_endpoint_provider = {
                **expected_provider,
                "base_url": LEGACY_APEX_CODEX_BASE_URL,
            }
            legacy_apex_endpoint_codex = {
                "model_provider": spec.codex_provider,
                "model_providers": {
                    spec.codex_provider: legacy_endpoint_provider,
                    LEGACY_APEX_CODEX_PROVIDER: {
                        **legacy_endpoint_provider,
                    },
                },
            }
            legacy_apex_endpoint_legacy_codex = {
                "model_provider": LEGACY_APEX_CODEX_PROVIDER,
                "model_providers": {
                    LEGACY_APEX_CODEX_PROVIDER: {
                        **legacy_endpoint_provider,
                        "name": LEGACY_APEX_LABEL,
                    },
                },
            }
        # Codex 0.144.6 mutates config.toml during normal use:
        #
        # * its one-time personality migration persists ``pragmatic``;
        # * thread/start with workspace-write/danger-full-access persists the
        #   canonical Git root as a trusted project, then immediately reloads
        #   (an explicit official ``untrusted`` value is strictly safer and
        #   uses the same one-field ProjectConfig schema).
        #
        # Treat project trust only as recoverable official state.  A trusted
        # project can load project-local MCP/hooks even though Codex strips
        # provider/auth redirection from that layer, so it must not remain in
        # an API account's persistent user config.  Runtime launches inject an
        # explicit session-level ``untrusted`` entry for their cwd.
        personality = codex.pop("personality", None)
        if personality not in {None, "pragmatic"}:
            raise CloudRouterUnsafePathError(
                f"Modified Codex API routing: {account.id}",
            )
        projects = codex.pop("projects", None)
        if not _is_codex_managed_projects_state(projects):
            raise CloudRouterUnsafePathError(
                f"Modified Codex API routing: {account.id}",
            )
        migrate_legacy_apex = (
            legacy_apex_codex is not None
            and codex == legacy_apex_codex
        )
        migrate_legacy_apex_endpoint = (
            (
                legacy_apex_endpoint_codex is not None
                and codex == legacy_apex_endpoint_codex
            )
            or (
                legacy_apex_endpoint_legacy_codex is not None
                and codex == legacy_apex_endpoint_legacy_codex
            )
        )
        if (
            codex != expected_codex
            and not migrate_legacy_apex
            and not migrate_legacy_apex_endpoint
        ):
            raise CloudRouterUnsafePathError(
                f"Modified Codex API routing: {account.id}",
            )
        if apply_migrations and (
            projects is not None
            or migrate_legacy_apex
            or migrate_legacy_apex_endpoint
        ):
            try:
                _atomic_private_write(
                    account.root / "codex" / "config.toml",
                    _codex_config_payload(
                        spec,
                        account.root / "key-helper",
                        personality=personality,
                    ),
                )
            except CloudRouterAccountError:
                raise
            except OSError as exc:
                raise CloudRouterUnsafePathError(
                    f"Could not secure Codex project state: {account.id}",
                ) from exc

    def reload(self) -> list[CloudRouterAccount]:
        root_fd = self._open_store_root_fd()
        os.close(root_fd)
        loaded: dict[str, CloudRouterAccount] = {}
        for child in self.root.iterdir():
            if not ACCOUNT_ID_RE.fullmatch(child.name):
                continue
            account = self._load_account(child)
            loaded[account.id] = account
        self._accounts = loaded
        self._quota_cache = {
            key: value for key, value in self._quota_cache.items() if key in loaded
        }
        self._quota_cached_at = {
            key: value for key, value in self._quota_cached_at.items() if key in loaded
        }
        return self.all_accounts(include_retired=True)

    def all_accounts(self, include_retired: bool = False) -> list[CloudRouterAccount]:
        accounts = sorted(
            self._accounts.values(),
            key=lambda account: (
                list(API_PROVIDER_SPECS).index(account.api_provider),
                int(
                    ACCOUNT_ID_RE.fullmatch(account.id).group("number")  # type: ignore[union-attr]
                ),
            ),
        )
        if not include_retired:
            accounts = [account for account in accounts if not account.retired]
        return accounts

    def visible_accounts(self) -> list[CloudRouterAccount]:
        """Return active accounts plus resumable cleanup tombstones."""

        return [
            account
            for account in self.all_accounts(include_retired=True)
            if not account.retired or account.cleanup_pending
        ]

    def account(self, account_id: str) -> CloudRouterAccount | None:
        _validate_account_id(account_id)
        return self._accounts.get(account_id)

    @asynccontextmanager
    async def account_retirement_guard(self, account_id: str):
        """Serialize the complete staged retirement workflow for one id."""

        valid_id = _validate_account_id(account_id)
        lock = self._retirement_locks.setdefault(valid_id, asyncio.Lock())
        async with lock:
            yield

    def active_credential_users(self, account_id: str) -> int:
        """Number of admitted upstream requests still using this key."""

        return max(0, int(self._credential_users.get(
            _validate_account_id(account_id), 0,
        )))

    async def _release_credential_user(self, account_id: str) -> None:
        async with self._mutation_lock:
            remaining = self._credential_users.get(account_id, 0) - 1
            if remaining > 0:
                self._credential_users[account_id] = remaining
            else:
                self._credential_users.pop(account_id, None)

    @asynccontextmanager
    async def credential_admission(self, account_id: str):
        """Lease one enabled key for the full lifetime of an upstream call."""

        valid_id = _validate_account_id(account_id)
        async with self._mutation_lock:
            self.reload()
            account = self._require_account(valid_id)
            self._credential_users[valid_id] = (
                self._credential_users.get(valid_id, 0) + 1
            )
        try:
            yield account
        finally:
            await finish_awaitable(
                self._release_credential_user(valid_id)
            )

    @staticmethod
    def _canonical_runtime_path(path: str | os.PathLike[str]) -> str:
        raw = os.path.expandvars(os.path.expanduser(os.fspath(path)))
        if not raw:
            return ""
        return os.path.realpath(os.path.abspath(raw))

    def account_for_claude_config_dir(
        self, path: str | os.PathLike[str],
    ) -> CloudRouterAccount | None:
        """Find an active or retired API account by exact runtime directory."""

        candidate = self._canonical_runtime_path(path)
        return next((
            account for account in self._accounts.values()
            if self._canonical_runtime_path(account.claude_config_dir) == candidate
        ), None)

    def account_for_codex_home(
        self, path: str | os.PathLike[str],
    ) -> CloudRouterAccount | None:
        """Find an active or retired API account by exact CODEX_HOME."""

        candidate = self._canonical_runtime_path(path)
        return next((
            account for account in self._accounts.values()
            if self._canonical_runtime_path(account.codex_home) == candidate
        ), None)

    def account_for_runtime_home(
        self, path: str | os.PathLike[str],
    ) -> CloudRouterAccount | None:
        return (
            self.account_for_claude_config_dir(path)
            or self.account_for_codex_home(path)
        )

    def _reload_runtime_account(
        self,
        provider: str,
        runtime_home: str | os.PathLike[str],
    ) -> CloudRouterAccount:
        """Reload and resolve one enabled account while mutation is fenced."""

        try:
            self.reload()
            finder = (
                self.account_for_codex_home
                if provider == "codex"
                else self.account_for_claude_config_dir
            )
            account = finder(runtime_home)
        except CloudRouterAccountError:
            raise
        except OSError as exc:
            # Filesystem races/read-only mounts are permanent for this
            # admission attempt. Convert them to the same sanitized,
            # non-requeued safety failure as an invalid managed path.
            raise CloudRouterUnsafePathError(
                "API account storage is unavailable"
            ) from exc
        if account is None or account.retired or not account.enabled:
            raise CloudRouterAccountError(
                "API account is disabled or missing"
            )
        return account

    @asynccontextmanager
    async def configuration_admission(
        self,
        provider: str,
        runtime_home: str | os.PathLike[str],
    ):
        """Validate storage/routing without applying model or quota gates."""

        provider = str(provider or "").lower()
        if provider not in {"claude", "codex"}:
            raise CloudRouterAccountError("Unknown provider")
        async with self._mutation_lock:
            yield self._reload_runtime_account(provider, runtime_home)

    @asynccontextmanager
    async def runtime_admission(
        self,
        provider: str,
        runtime_home: str | os.PathLike[str],
        model: str | None,
        *,
        service_tier: str = "default",
    ):
        """Serialize model/quota revalidation with metadata mutation.

        The lock is intentionally held only until the caller has spawned and
        registered its process. Refresh can then update future routing without
        invalidating the admission decision between selection and spawn.
        """

        provider = str(provider or "").lower()
        if provider not in {"claude", "codex"}:
            raise CloudRouterAccountError("Unknown provider")
        async with self._mutation_lock:
            account = self._reload_runtime_account(provider, runtime_home)
            if not account.supports_model(provider, model):
                raise CloudRouterAccountError(
                    f"API account does not support model {model!r}"
                )
            requested_tier = str(
                service_tier or "default"
            ).strip().lower()
            if requested_tier not in {"default", "priority"}:
                raise CloudRouterAccountError(
                    f"Unsupported Codex service tier {service_tier!r}"
                )
            if (
                provider == "codex"
                and not account.supports_service_tier(
                    provider,
                    model,
                    requested_tier,
                )
            ):
                raise CloudRouterAccountError(
                    "API account does not advertise service tier "
                    f"{requested_tier!r} for model {model!r}"
                )
            decision = self.cached_quota_decision(account.id)
            if (
                bool(decision.get("known"))
                and decision.get("available") is False
            ):
                raise CloudRouterAccountError(
                    "API account is unavailable: "
                    f"{decision.get('reason') or 'quota'}"
                )
            yield account

    def _require_account(
        self, account_id: str, *, allow_retired: bool = False,
    ) -> CloudRouterAccount:
        account = self.account(account_id)
        if account is None or (account.retired and not allow_retired):
            raise CloudRouterAccountNotFound("Unknown API account")
        return account

    def _next_account_id(self, api_provider: str) -> str:
        spec = API_PROVIDER_SPECS[normalize_api_provider(api_provider)]
        used = {
            int(match.group("number"))
            for child in self.root.iterdir()
            if (match := ACCOUNT_ID_RE.fullmatch(child.name))
            and match.group("provider") == spec.account_prefix
        }
        number = 1
        while number in used:
            number += 1
        return f"{spec.account_prefix}-{number}"

    async def _request_json(self, url: str, api_key: str) -> Any:
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
        }
        try:
            async with httpx.AsyncClient(
                timeout=self._http_timeout,
                follow_redirects=False,
            ) as client:
                async with client.stream("GET", url, headers=headers) as response:
                    status_code = response.status_code
                    if 300 <= status_code < 400:
                        raise CloudRouterUpstreamError(
                            "unexpected_redirect", status_code=status_code,
                        )
                    chunks: list[bytes] = []
                    size = 0
                    async for chunk in response.aiter_bytes():
                        size += len(chunk)
                        if size > MAX_API_RESPONSE_BYTES:
                            raise CloudRouterUpstreamError("response_too_large")
                        chunks.append(chunk)
        except CloudRouterUpstreamError:
            raise
        except (httpx.TimeoutException, TimeoutError) as exc:
            raise CloudRouterUpstreamError("timeout") from exc
        except (httpx.RequestError, OSError) as exc:
            raise CloudRouterUpstreamError("network_error") from exc

        if status_code == 401:
            raise CloudRouterUpstreamError("invalid_api_key", status_code=401)
        if status_code == 403:
            raise CloudRouterUpstreamError("forbidden", status_code=403)
        if status_code == 429:
            raise CloudRouterUpstreamError("rate_limited", status_code=429)
        if status_code >= 500:
            raise CloudRouterUpstreamError("upstream_unavailable", status_code=status_code)
        if not 200 <= status_code < 300:
            raise CloudRouterUpstreamError("upstream_rejected", status_code=status_code)
        try:
            return json.loads(b"".join(chunks))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CloudRouterUpstreamError("invalid_json") from exc

    async def probe_models(
        self,
        api_key: str,
        *,
        api_provider: str = API_PROVIDER_CLOUDROUTER,
    ) -> dict[str, Any]:
        provider = normalize_api_provider(api_provider)
        spec = API_PROVIDER_SPECS[provider]
        if provider == API_PROVIDER_APEX:
            models_url = str(
                httpx.URL(spec.models_url).copy_set_param(
                    "client_version", APEX_CODEX_CLIENT_VERSION,
                )
            )
            return _normalise_apex_models(
                await self._request_json(models_url, api_key)
            )
        payload = await self._request_json(spec.models_url, api_key)
        if provider == API_PROVIDER_APIBEST:
            authenticated = (
                _normalise_models(payload)
                if isinstance(payload, dict) and payload.get("data")
                else None
            )
            if authenticated is not None:
                return authenticated
            return _normalise_apibest_pricing(
                await self._request_json(APIBEST_PRICING_URL, api_key)
            )
        return _normalise_models(payload)

    def _read_api_key(self, account: CloudRouterAccount) -> str:
        path = account.root / "api.key"
        try:
            payload = _open_regular_nofollow(path, maximum=MAX_API_KEY_BYTES)
            metadata = path.lstat()
        except OSError as exc:
            raise CloudRouterUnsafePathError("API key is unavailable") from exc
        if stat.S_IMODE(metadata.st_mode) != 0o600:
            raise CloudRouterUnsafePathError("API key permissions are unsafe")
        try:
            value = payload.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise CloudRouterUnsafePathError("API key is invalid") from exc
        if not value or value.strip() != value or "\r" in value or "\n" in value:
            raise CloudRouterUnsafePathError("API key is invalid")
        return value

    @staticmethod
    def _metadata(
        account_id: str,
        name: str,
        models: dict[str, Any],
        key_hint: str,
        *,
        api_provider: str = API_PROVIDER_CLOUDROUTER,
        enabled: bool = True,
        retired: bool = False,
        created_at: float | None = None,
    ) -> dict[str, Any]:
        current = _now()
        provider = normalize_api_provider(api_provider)
        service_tiers_source = (
            SERVICE_TIER_SOURCE_UPSTREAM
            if "service_tiers" in models
            else SERVICE_TIER_SOURCE_NONE
        )
        provider_models, service_tiers = _split_model_probe(models)
        return {
            "version": 2,
            "id": account_id,
            "name": name,
            "api_provider": provider,
            "enabled": enabled,
            "retired": retired,
            "cleanup_pending": False,
            "cleanup_code": None,
            "cleanup_reason": None,
            "cleanup_last_attempt_at": None,
            "cleanup_last_error_at": None,
            "models": provider_models,
            "service_tiers": service_tiers,
            "service_tiers_source": service_tiers_source,
            "key_hint": key_hint,
            "endpoints": dict(API_PROVIDER_SPECS[provider].endpoints),
            "created_at": created_at or current,
            "updated_at": current,
        }

    def _write_account_files(
        self,
        root: Path,
        *,
        runtime_root: Path | None = None,
        account_id: str,
        name: str,
        api_key: str,
        models: dict[str, Any],
        api_provider: str = API_PROVIDER_CLOUDROUTER,
    ) -> None:
        provider = normalize_api_provider(api_provider)
        spec = API_PROVIDER_SPECS[provider]
        _ensure_private_directory(root)
        claude_dir = root / "claude"
        codex_dir = root / "codex"
        _ensure_private_directory(claude_dir)
        _ensure_private_directory(codex_dir)

        helper = root / "key-helper"
        runtime_helper = (runtime_root or root) / "key-helper"
        _atomic_private_write(helper, KEY_HELPER.encode("utf-8"), mode=0o700)
        _atomic_private_write(root / "api.key", api_key.encode("utf-8"))

        if spec.claude_base_url is not None:
            settings = {
                "env": {"ANTHROPIC_BASE_URL": spec.claude_base_url},
                "apiKeyHelper": _claude_helper_command(runtime_root or root),
                CLAUDE_SKIP_DANGEROUS_PROMPT: True,
            }
            _atomic_private_json(claude_dir / "settings.json", settings)
            _atomic_private_json(
                claude_dir / ".claude.json",
                {"hasCompletedOnboarding": True},
            )

        _atomic_private_write(
            codex_dir / "config.toml",
            _codex_config_payload(spec, runtime_helper),
        )
        _atomic_private_json(
            root / "account.json",
            self._metadata(
                account_id,
                name,
                models,
                _key_hint(api_key),
                api_provider=provider,
            ),
            maximum=MAX_METADATA_BYTES,
        )

    async def add_account(
        self,
        name: str,
        api_key: str,
        *,
        api_provider: str = API_PROVIDER_CLOUDROUTER,
    ) -> CloudRouterAccount:
        provider = normalize_api_provider(api_provider)
        clean_name = str(name or "").strip()
        if not clean_name or len(clean_name) > 100 or any(
            ord(character) < 32 for character in clean_name
        ):
            raise ValueError("Account name must be 1-100 printable characters")
        if (
            not isinstance(api_key, str)
            or not api_key
            or api_key.strip() != api_key
            or "\r" in api_key
            or "\n" in api_key
            or len(api_key.encode("utf-8")) > MAX_API_KEY_BYTES
        ):
            raise ValueError("Invalid API key")

        models = await self.probe_models(
            api_key,
            api_provider=provider,
        )
        async with self._mutation_lock:
            self.reload()
            account_id = self._next_account_id(provider)
            target = self._account_root(account_id)
            temporary = Path(tempfile.mkdtemp(
                prefix=f".{account_id}.", suffix=".tmp", dir=self.root,
            ))
            os.chmod(temporary, 0o700)
            try:
                self._write_account_files(
                    temporary,
                    runtime_root=target,
                    account_id=account_id,
                    name=clean_name,
                    api_key=api_key,
                    models=models,
                    api_provider=provider,
                )
                if target.exists() or target.is_symlink():
                    raise CloudRouterUnsafePathError("Account destination already exists")
                os.rename(temporary, target)
                _fsync_directory(self.root)
            finally:
                if temporary.exists() and not temporary.is_symlink():
                    shutil.rmtree(temporary)
            self.reload()
            return self._require_account(account_id)

    async def refresh_account(self, account_id: str) -> CloudRouterAccount:
        async with self._mutation_lock:
            self.reload()
            account = self._require_account(account_id)
            api_key = self._read_api_key(account)
            models = await self.probe_models(
                api_key,
                api_provider=account.api_provider,
            )
            metadata_path = account.root / "account.json"
            data = json.loads(
                _open_regular_nofollow(
                    metadata_path, maximum=MAX_METADATA_BYTES,
                ).decode("utf-8"),
            )
            service_tiers_source = (
                SERVICE_TIER_SOURCE_UPSTREAM
                if "service_tiers" in models
                else SERVICE_TIER_SOURCE_NONE
            )
            provider_models, service_tiers = _split_model_probe(models)
            data["models"] = provider_models
            data["service_tiers"] = service_tiers
            # Development builds briefly persisted response-based denials.
            # Response metadata is informational and must not override the
            # account's current catalog capability.
            data.pop("service_tier_denials", None)
            data["service_tiers_source"] = service_tiers_source
            data["updated_at"] = _now()
            _atomic_private_json(
                metadata_path,
                data,
                maximum=MAX_METADATA_BYTES,
            )
            self.reload()
            return self._require_account(account_id)

    async def fetch_usage(
        self, account_id: str, force: bool = False,
    ) -> dict[str, Any]:
        self._require_account(account_id)
        current = _now()
        cached = self._quota_cache.get(account_id)
        if (
            not force
            and cached is not None
            and current - self._quota_cached_at.get(account_id, 0.0)
            < self._quota_cache_ttl
        ):
            return dict(cached)
        async with self.credential_admission(account_id) as account:
            spec = API_PROVIDER_SPECS[account.api_provider]
            if spec.usage_url is None:
                snapshot = _unknown_snapshot(
                    account_id,
                    "usage_not_supported",
                    previous=cached,
                )
            else:
                try:
                    payload = await self._request_json(
                        spec.usage_url, self._read_api_key(account),
                    )
                    snapshot = (
                        _normalise_apex_usage(account_id, payload)
                        if account.api_provider == API_PROVIDER_APEX
                        else _normalise_usage(account_id, payload)
                    )
                except CloudRouterUpstreamError as exc:
                    if exc.status_code in {401, 403}:
                        snapshot = _unavailable_snapshot(account_id, exc.code)
                    else:
                        snapshot = _unknown_snapshot(
                            account_id, exc.code, previous=cached,
                        )
                except CloudRouterUnsafePathError:
                    snapshot = _unavailable_snapshot(
                        account_id, "invalid_local_credentials",
                    )
            # Retirement may have staged while the HTTP request was in flight.
            # Publish only while this exact id is still active; stage clears
            # any snapshot that won an earlier publication race.
            async with self._mutation_lock:
                self.reload()
                current_account = self.account(account_id)
                if (
                    current_account is not None
                    and current_account.enabled
                    and not current_account.retired
                ):
                    self._quota_cache[account_id] = snapshot
                    self._quota_cached_at[account_id] = current
        return dict(snapshot)

    def cached_quota_decision(self, account_id: str) -> dict[str, Any]:
        account = self.account(account_id)
        if account is None or account.retired or not account.enabled:
            return {"available": False, "known": True, "reason": "disabled"}
        snapshot = self._quota_cache.get(account_id)
        if not snapshot:
            return {"available": True, "known": False, "reason": "not_fetched"}
        if (
            not bool(snapshot.get("known"))
            and snapshot.get("last_known_available") is False
        ):
            return {
                "available": False,
                "known": True,
                "reason": str(
                    snapshot.get("last_known_reason")
                    or snapshot.get("reason")
                    or "last_known_unavailable"
                ),
            }
        if (
            bool(snapshot.get("known"))
            and snapshot.get("available") is False
        ):
            return {
                "available": False,
                "known": True,
                "reason": str(snapshot.get("reason") or "unavailable"),
            }
        return {
            "available": bool(snapshot.get("available", True)),
            "known": bool(snapshot.get("known", False)),
            "reason": str(snapshot.get("reason") or "unknown"),
        }

    def _remove_except(
        self,
        account_fd: int,
        runtime_name: str,
        preserved_name: str,
    ) -> None:
        """Remove only direct children of one proven managed runtime dir.

        ``shutil.rmtree``'s fd-based implementation refuses symlink swaps. The
        account/runtime descriptor chain and relative child names ensure
        cleanup cannot be redirected if a same-uid process replaces an
        ancestor or descendant between inspection and removal.
        """

        if (
            runtime_name not in {"claude", "codex"}
            or (runtime_name, preserved_name)
            not in {("claude", "projects"), ("codex", "sessions")}
            or not shutil.rmtree.avoids_symlink_attacks
        ):
            raise CloudRouterUnsafePathError(
                f"Refusing unmanaged account cleanup: {runtime_name}",
            )
        try:
            descriptor = os.open(
                runtime_name,
                _directory_open_flags(),
                dir_fd=account_fd,
            )
        except OSError as exc:
            raise CloudRouterUnsafePathError(
                f"Unsafe managed runtime directory: {runtime_name}",
            ) from exc
        try:
            opened = os.fstat(descriptor)
            current = os.stat(
                runtime_name,
                dir_fd=account_fd,
                follow_symlinks=False,
            )
            if (
                not stat.S_ISDIR(opened.st_mode)
                or opened.st_uid != os.getuid()
                or stat.S_IMODE(opened.st_mode) != 0o700
                or current.st_dev != opened.st_dev
                or current.st_ino != opened.st_ino
            ):
                raise CloudRouterUnsafePathError(
                    f"Unsafe managed runtime directory: {runtime_name}",
                )
            for name in os.listdir(descriptor):
                metadata = os.stat(
                    name, dir_fd=descriptor, follow_symlinks=False,
                )
                if name == preserved_name:
                    if (
                        not stat.S_ISDIR(metadata.st_mode)
                        or stat.S_ISLNK(metadata.st_mode)
                        or metadata.st_uid != os.getuid()
                    ):
                        raise CloudRouterUnsafePathError(
                            "Unsafe preserved account directory",
                        )
                    continue
                if stat.S_ISDIR(metadata.st_mode):
                    shutil.rmtree(name, dir_fd=descriptor)
                else:
                    os.unlink(name, dir_fd=descriptor)
            os.fsync(descriptor)
            current = os.stat(
                runtime_name,
                dir_fd=account_fd,
                follow_symlinks=False,
            )
            if (
                current.st_dev != opened.st_dev
                or current.st_ino != opened.st_ino
            ):
                raise CloudRouterUnsafePathError(
                    f"Managed runtime directory changed: {runtime_name}",
                )
        finally:
            os.close(descriptor)

    def _unlink_account_credentials(
        self, account_fd: int,
    ) -> None:
        """Unlink only verified regular credential files via account dirfd."""

        for name in ("api.key", "key-helper"):
            try:
                initial = os.stat(
                    name, dir_fd=account_fd, follow_symlinks=False,
                )
            except FileNotFoundError:
                continue
            if (
                not stat.S_ISREG(initial.st_mode)
                or initial.st_uid != os.getuid()
            ):
                raise CloudRouterUnsafePathError(
                    f"Unsafe account credential entry: {name}",
                )
            file_flags = (
                os.O_RDONLY
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0)
            )
            try:
                child_fd = os.open(name, file_flags, dir_fd=account_fd)
            except OSError as exc:
                raise CloudRouterUnsafePathError(
                    f"Unsafe account credential entry: {name}",
                ) from exc
            try:
                opened = os.fstat(child_fd)
                current = os.stat(
                    name, dir_fd=account_fd, follow_symlinks=False,
                )
                if (
                    not stat.S_ISREG(opened.st_mode)
                    or opened.st_uid != os.getuid()
                    or current.st_dev != opened.st_dev
                    or current.st_ino != opened.st_ino
                    or initial.st_dev != opened.st_dev
                    or initial.st_ino != opened.st_ino
                ):
                    raise CloudRouterUnsafePathError(
                        f"Account credential changed during cleanup: {name}",
                    )
            finally:
                os.close(child_fd)
            os.unlink(name, dir_fd=account_fd)
        os.fsync(account_fd)

    @staticmethod
    def _retirement_metadata_at(
        account_fd: int,
        account_id: str,
    ) -> dict[str, Any]:
        try:
            data = json.loads(_read_regular_at(
                account_fd,
                "account.json",
                maximum=MAX_METADATA_BYTES,
            ).decode("utf-8"))
        except CloudRouterAccountError:
            raise
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CloudRouterUnsafePathError(
                f"Invalid account metadata: {account_id}",
            ) from exc
        if not isinstance(data, dict) or data.get("id") != account_id:
            raise CloudRouterUnsafePathError(
                f"Mismatched account metadata: {account_id}",
            )
        _cleanup_diagnostics_from_metadata(data, account_id)
        return data

    async def stage_retirement(self, account_id: str) -> CloudRouterAccount:
        """Durably disable an account before any runtime lifecycle fencing."""

        async with self._mutation_lock:
            account = self._require_account(account_id, allow_retired=True)
            with self._open_account_fd(account.id) as (
                root_fd,
                account_fd,
            ):
                self._assert_account_fd_current(
                    root_fd, account_fd, account.id,
                )
                data = self._retirement_metadata_at(account_fd, account.id)
                retired = bool(data.get("retired", False))
                cleanup_pending = bool(
                    data.get("cleanup_pending", False)
                )
                if retired and not cleanup_pending:
                    self.reload()
                    return self._require_account(
                        account_id,
                        allow_retired=True,
                    )
                if not retired:
                    data.update({
                        "enabled": False,
                        "retired": True,
                        "cleanup_pending": True,
                        "cleanup_code": None,
                        "cleanup_reason": None,
                        "cleanup_last_attempt_at": None,
                        "cleanup_last_error_at": None,
                        "updated_at": _now(),
                    })
                    _atomic_private_json_at(
                        account_fd,
                        "account.json",
                        data,
                        maximum=MAX_METADATA_BYTES,
                    )
                    self._assert_account_fd_current(
                        root_fd, account_fd, account.id,
                    )
                self._quota_cache.pop(account_id, None)
                self._quota_cached_at.pop(account_id, None)
            self.reload()
            return self._require_account(account_id, allow_retired=True)

    async def mark_cleanup_attempt(
        self,
        account_id: str,
    ) -> CloudRouterAccount:
        """Persist one retry timestamp without discarding the last failure."""

        async with self._mutation_lock:
            account = self._require_account(account_id, allow_retired=True)
            if account.retired and not account.cleanup_pending:
                return account
            with self._open_account_fd(account.id) as (root_fd, account_fd):
                self._assert_account_fd_current(root_fd, account_fd, account.id)
                data = self._retirement_metadata_at(account_fd, account.id)
                if (
                    not bool(data.get("retired", False))
                    or not bool(data.get("cleanup_pending", False))
                ):
                    raise CloudRouterAccountError(
                        "API account retirement was not durably staged",
                    )
                attempted_at = _now()
                data.update({
                    "cleanup_last_attempt_at": attempted_at,
                    "updated_at": attempted_at,
                })
                _atomic_private_json_at(
                    account_fd,
                    "account.json",
                    data,
                    maximum=MAX_METADATA_BYTES,
                )
                self._assert_account_fd_current(root_fd, account_fd, account.id)
            self.reload()
            return self._require_account(account_id, allow_retired=True)

    async def record_cleanup_failure(
        self,
        account_id: str,
        *,
        code: str,
        reason: str,
    ) -> CloudRouterAccount:
        """Persist a bounded failure only on a proven pending tombstone."""

        safe_code = _normalise_cleanup_code(code)
        safe_reason = _sanitise_cleanup_reason(reason)
        async with self._mutation_lock:
            account = self._require_account(account_id, allow_retired=True)
            if account.retired and not account.cleanup_pending:
                # A late duplicate failure must never resurrect diagnostics on
                # an already finalized tombstone.
                return account
            with self._open_account_fd(account.id) as (root_fd, account_fd):
                self._assert_account_fd_current(root_fd, account_fd, account.id)
                data = self._retirement_metadata_at(account_fd, account.id)
                if (
                    not bool(data.get("retired", False))
                    or not bool(data.get("cleanup_pending", False))
                ):
                    raise CloudRouterAccountError(
                        "API account retirement was not durably staged",
                    )
                failed_at = _now()
                data.update({
                    "cleanup_code": safe_code,
                    "cleanup_reason": safe_reason,
                    "cleanup_last_attempt_at": (
                        data.get("cleanup_last_attempt_at") or failed_at
                    ),
                    "cleanup_last_error_at": failed_at,
                    "updated_at": failed_at,
                })
                _atomic_private_json_at(
                    account_fd,
                    "account.json",
                    data,
                    maximum=MAX_METADATA_BYTES,
                )
                self._assert_account_fd_current(root_fd, account_fd, account.id)
            self.reload()
            return self._require_account(account_id, allow_retired=True)

    async def finalize_retirement(
        self, account_id: str,
    ) -> CloudRouterAccount:
        """Remove credentials/config from an already-disabled tombstone."""

        async with self._mutation_lock:
            account = self._require_account(account_id, allow_retired=True)
            if self._credential_users.get(account.id, 0) > 0:
                raise CloudRouterAccountBusyError(
                    "API account still has an active credential request",
                    code="credential_busy",
                )
            with self._open_account_fd(account.id) as (
                root_fd,
                account_fd,
            ):
                self._assert_account_fd_current(
                    root_fd, account_fd, account.id,
                )
                data = self._retirement_metadata_at(account_fd, account.id)
                retired = bool(data.get("retired", False))
                cleanup_pending = bool(
                    data.get("cleanup_pending", False)
                )
                if retired and not cleanup_pending:
                    self.reload()
                    return self._require_account(
                        account_id,
                        allow_retired=True,
                    )
                if not retired or not cleanup_pending:
                    raise CloudRouterAccountError(
                        "API account retirement was not durably staged",
                    )
                self._remove_except(
                    account_fd,
                    "claude",
                    "projects",
                )
                self._assert_account_fd_current(
                    root_fd, account_fd, account.id,
                )
                self._remove_except(
                    account_fd,
                    "codex",
                    "sessions",
                )
                self._assert_account_fd_current(
                    root_fd, account_fd, account.id,
                )
                self._unlink_account_credentials(account_fd)
                self._assert_account_fd_current(
                    root_fd, account_fd, account.id,
                )
                data.update({
                    "enabled": False,
                    "retired": True,
                    "cleanup_pending": False,
                    "cleanup_code": None,
                    "cleanup_reason": None,
                    "cleanup_last_attempt_at": None,
                    "cleanup_last_error_at": None,
                    "key_hint": "",
                    "updated_at": _now(),
                })
                _atomic_private_json_at(
                    account_fd,
                    "account.json",
                    data,
                    maximum=MAX_METADATA_BYTES,
                )
                self._assert_account_fd_current(
                    root_fd, account_fd, account.id,
                )
            self._quota_cache.pop(account_id, None)
            self._quota_cached_at.pop(account_id, None)
            self.reload()
            return self._require_account(account_id, allow_retired=True)

    async def retire_account(self, account_id: str) -> CloudRouterAccount:
        """Offline/test convenience wrapper for staged, resumable cleanup."""

        async with self.account_retirement_guard(account_id):
            staged = await self.stage_retirement(account_id)
            if staged.retired and not staged.cleanup_pending:
                return staged
            await self.mark_cleanup_attempt(account_id)
            try:
                return await self.finalize_retirement(account_id)
            except CloudRouterAccountBusyError as exc:
                await self.record_cleanup_failure(
                    account_id,
                    code=exc.code,
                    reason=exc.reason,
                )
                raise
