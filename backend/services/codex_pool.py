"""Codex account pool — multi-account rotation and quota tracking.

Parallel to claude_pool.py but for OpenAI Codex CLI accounts.
Config: ~/.codex-pool/accounts.json
Each account has its own CODEX_HOME directory with auth.json.

Manual quota refresh uses Codex app-server's account/rateLimits/read RPC for
the account's own CODEX_HOME. Session rollout rate_limits payloads remain a
cached/background source, but never substitute for a failed live account read:
migrated sessions can carry another account's historical quota snapshot.
"""

import asyncio
import json
import logging
import math
import os
import re
import stat
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Awaitable, BinaryIO, Callable, Iterator

from backend.config import settings
from backend.services.claude_pool import (
    is_codex_auth_failure,
    is_codex_transient,
    is_codex_usage_limited,
)
from backend.services.cloudrouter_accounts import (
    is_api_auth_kind as _is_api_auth_kind,
)
from backend.services.codex_session_migration import (
    CodexRolloutMigrationMetadataError,
    read_rollout_migration_marker,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Rate-limit / auth-failure detection
# ---------------------------------------------------------------------------

def is_rate_limited(text: str) -> bool:
    """Backward-compatible alias for the shared Codex usage-limit detector."""
    return is_codex_usage_limited(text)


def is_auth_failure(text: str) -> bool:
    """Backward-compatible alias for the shared Codex auth detector."""
    return is_codex_auth_failure(text)


def is_transient(text: str) -> bool:
    """Backward-compatible alias for the shared Codex transient detector."""
    return is_codex_transient(text)


def is_pool_rotatable(text: str) -> bool:
    return is_rate_limited(text) or is_auth_failure(text)


# ---------------------------------------------------------------------------
# Account configuration
# ---------------------------------------------------------------------------

DEFAULT_CONFIG_PATH = Path.home() / ".codex-pool" / "accounts.json"
DEFAULT_COOLDOWN_SECONDS = 300
QUOTA_CACHE_TTL = 120  # seconds
QUOTA_SWITCH_THRESHOLD_PERCENT = 90.0
PROACTIVE_QUOTA_MAX_COOLDOWN_SECONDS = 8 * 24 * 60 * 60
MIN_COOLDOWN_SECONDS = 1
MAX_COOLDOWN_SECONDS = 8 * 24 * 60 * 60
MIN_QUOTA_SWITCH_THRESHOLD_PERCENT = 1.0
MAX_QUOTA_SWITCH_THRESHOLD_PERCENT = 100.0
ROUTING_POLICIES = frozenset({"api_first", "native_first"})
_SESSION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_MODELS_CACHE_MAX_BYTES = 4 * 1024 * 1024
_MODELS_CACHE_MAX_MODELS = 2048
_CODEX_SERVICE_TIERS = frozenset({"default", "priority"})


def _normalize_service_tier(service_tier: str | None) -> str:
    value = str(service_tier or "default").strip().lower()
    if value not in _CODEX_SERVICE_TIERS:
        raise ValueError(f"Unsupported Codex service tier: {service_tier!r}")
    return value


def _service_tier_model(model: str | None) -> str:
    """Resolve Task.model NULL/default exactly like request validation."""

    value = str(model or "").strip()
    if not value or value.lower() == "default":
        return settings.default_codex_model
    return value


def _models_cache_model_state(
    codex_home: str,
    model: str | None,
) -> tuple[bool | None, dict | None]:
    """Return native model-catalog evidence.

    ``True`` means a valid catalog contains the requested model, ``False``
    means a valid catalog was read and does not contain it, and ``None`` means
    the catalog cannot be trusted (missing, malformed, or unreadable).  The
    distinction is important for standard turns: an unknown catalog remains
    a usable compatibility fallback, while a verified model mismatch must not
    be routed and allowed to fail after the provider boundary.
    """

    requested_model = _service_tier_model(model)
    path = Path(codex_home) / "models_cache.json"
    descriptor = -1
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_size > _MODELS_CACHE_MAX_BYTES
        ):
            return None, None
        chunks: list[bytes] = []
        remaining = _MODELS_CACHE_MAX_BYTES + 1
        while remaining:
            chunk = os.read(descriptor, min(65536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
        if len(raw) > _MODELS_CACHE_MAX_BYTES:
            return None, None
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None, None
    finally:
        if descriptor >= 0:
            os.close(descriptor)

    models = payload.get("models") if isinstance(payload, dict) else None
    if not isinstance(models, list) or len(models) > _MODELS_CACHE_MAX_MODELS:
        return None, None
    for item in models:
        if not isinstance(item, dict):
            return None, None
        slug = item.get("slug")
        if not isinstance(slug, str) or not slug:
            return None, None
        if slug == requested_model:
            return True, item
    return False, None


def _models_cache_supports_service_tier(
    codex_home: str,
    model: str | None,
    service_tier: str,
) -> bool:
    """Read one native account's bounded catalog without guessing support.

    Priority turns remain fail-closed when catalog evidence is unavailable.
    Standard turns retain the historical unknown-catalog fallback, but reject
    a model mismatch once a valid catalog proves it.
    """

    state, item = _models_cache_model_state(codex_home, model)
    if service_tier == "default":
        return state is not False
    if state is not True or item is None:
        return False
    tiers = item.get("service_tiers")
    if not isinstance(tiers, list):
        return False
    return any(
        isinstance(tier, dict) and tier.get("id") == service_tier
        for tier in tiers
    )


def quota_at_or_above(
    quota: dict | None,
    *,
    threshold: float = QUOTA_SWITCH_THRESHOLD_PERCENT,
    now: float | None = None,
) -> bool:
    """Whether Codex's 5-hour or weekly window reached ``threshold`` percent."""

    if not isinstance(quota, dict):
        return False
    current = time.time() if now is None else now
    if quota.get("usage_limit_exceeded") is True:
        try:
            reset_at = float(quota.get("usage_limit_resets_at"))
        except (TypeError, ValueError):
            return True
        return reset_at > current

    high_window_seen = False
    for used_key, reset_key in (
        ("primary_used_percent", "primary_resets_at"),
        ("secondary_used_percent", "secondary_resets_at"),
    ):
        try:
            if float(quota.get(used_key)) < threshold:
                continue
        except (TypeError, ValueError):
            continue
        high_window_seen = True
        try:
            reset_at = float(quota.get(reset_key))
            if reset_at > 10_000_000_000:
                reset_at /= 1000
        except (TypeError, ValueError):
            return True
        if reset_at > current:
            return True
    if high_window_seen:
        return False

    if quota.get("is_rate_limited") is True:
        resets = []
        for key in ("primary_resets_at", "secondary_resets_at"):
            try:
                reset_at = float(quota.get(key))
                if reset_at > 10_000_000_000:
                    reset_at /= 1000
                resets.append(reset_at)
            except (TypeError, ValueError):
                continue
        return not resets or any(reset_at > current for reset_at in resets)
    return False


def api_quota_at_or_above(
    snapshot: dict | None,
    *,
    threshold: float = QUOTA_SWITCH_THRESHOLD_PERCENT,
) -> bool:
    """Whether a generic API quota snapshot reached ``threshold``.

    The adapter deliberately keeps provider-specific wallet/credit/quota data in
    ``api_quota`` rather than pretending it is Codex's native 5-hour/weekly
    subscription shape.  Ratios are evaluated only when the upstream supplied
    a real limit.
    """

    if not isinstance(snapshot, dict):
        return False
    if (
        bool(snapshot.get("known"))
        and snapshot.get("available") is False
    ):
        return True
    candidates: list[dict] = []
    quota = snapshot.get("quota")
    if isinstance(quota, dict):
        candidates.append(quota)
    windows = snapshot.get("windows")
    if isinstance(windows, list):
        candidates.extend(window for window in windows if isinstance(window, dict))
    for window in candidates:
        if window.get("unlimited") is True:
            continue
        try:
            limit = float(window.get("limit"))
            used = float(window.get("used"))
        except (TypeError, ValueError):
            continue
        if limit > 0 and (used / limit) * 100 >= threshold:
            return True
    return False


def quota_cooldown_seconds(
    quota: dict | None,
    *,
    threshold: float = QUOTA_SWITCH_THRESHOLD_PERCENT,
    now: float | None = None,
    fallback: int = DEFAULT_COOLDOWN_SECONDS,
    maximum: int = PROACTIVE_QUOTA_MAX_COOLDOWN_SECONDS,
) -> int:
    """Return cooldown through the latest reset of every high quota window.

    Codex reports Unix timestamps for the primary (5-hour) and secondary
    (weekly) windows. Only windows whose usage reached ``threshold`` count; if
    both are high, the later reset keeps the old account out of selection until
    both windows are usable again. Millisecond timestamps are accepted for
    defensive compatibility with upstream payload changes.
    """

    if not isinstance(quota, dict):
        return max(1, int(fallback))

    current = time.time() if now is None else now
    if quota.get("usage_limit_exceeded") is True:
        try:
            reset_at = float(quota.get("usage_limit_resets_at"))
            if reset_at > 10_000_000_000:
                reset_at /= 1000
        except (TypeError, ValueError):
            return max(1, int(fallback))
        if reset_at <= current:
            return max(1, int(fallback))
        return min(
            max(1, int(reset_at - current)),
            max(1, int(maximum)),
        )

    reset_timestamps: list[float] = []
    for used_key, reset_key in (
        ("primary_used_percent", "primary_resets_at"),
        ("secondary_used_percent", "secondary_resets_at"),
    ):
        try:
            if float(quota.get(used_key)) < threshold:
                continue
            reset_at = float(quota.get(reset_key))
            if reset_at > 10_000_000_000:  # milliseconds, not seconds
                reset_at /= 1000
            reset_timestamps.append(reset_at)
        except (TypeError, ValueError):
            continue

    future_resets = [
        reset_at for reset_at in reset_timestamps if reset_at > current
    ]
    if not future_resets:
        return max(1, int(fallback))
    remaining = int(max(future_resets) - current)
    return min(max(1, remaining), max(1, int(maximum)))


class AmbiguousCodexSessionHomeError(RuntimeError):
    """A Codex thread rollout exists under more than one account home."""

    def __init__(self, session_id: str, homes: list[str]):
        self.session_id = session_id
        self.homes = homes
        super().__init__(
            f"Codex session {session_id!r} exists in multiple homes: "
            + ", ".join(homes)
        )


def canonical_codex_home(codex_home: str | os.PathLike[str]) -> str:
    """Return the stable absolute identity for a CODEX_HOME directory.

    Account lookup, app-server routing, cooldown state, and session ownership
    must all compare the same value.  Resolving existing symlinks also prevents
    one credential directory from being registered under two spellings.
    """

    raw = os.path.expandvars(os.path.expanduser(os.fspath(codex_home)))
    if not raw:
        raise ValueError("CODEX_HOME cannot be empty")
    return str(Path(raw).resolve(strict=False))


class CodexPoolAccount:
    __slots__ = (
        "id", "codex_home", "email", "enabled", "retired", "cleanup_pending",
        "login_recovery_failed", "quota_valid_after", "quota_cutoff_invalid",
        "auth_kind", "api_provider", "display_name", "api_account_id",
        "supported_models", "service_tiers",
        "_api_account",
    )

    def __init__(self, data: dict):
        self.id: str = data.get("id") or data.get("name") or ""
        if not self.id:
            raise ValueError("Codex pool account requires 'id'")
        self.codex_home: str = canonical_codex_home(data["codex_home"])
        self.email: str = str(data.get("email") or "")
        self.retired: bool = bool(data.get("retired", False))
        self.cleanup_pending: bool = bool(data.get("cleanup_pending", False))
        self.login_recovery_failed: bool = bool(
            data.get("login_recovery_failed", False)
        )
        has_quota_cutoff = "quota_valid_after" in data
        raw_quota_cutoff = data.get("quota_valid_after")
        parsed_quota_cutoff = 0.0
        valid_quota_cutoff = False
        if isinstance(raw_quota_cutoff, (int, float)) and not isinstance(
            raw_quota_cutoff, bool,
        ):
            try:
                candidate_cutoff = float(raw_quota_cutoff)
            except (OverflowError, TypeError, ValueError):
                pass
            else:
                if math.isfinite(candidate_cutoff) and candidate_cutoff > 0:
                    parsed_quota_cutoff = candidate_cutoff
                    valid_quota_cutoff = True
        self.quota_valid_after = parsed_quota_cutoff
        self.quota_cutoff_invalid: bool = has_quota_cutoff and not valid_quota_cutoff
        self.enabled: bool = bool(data.get("enabled", True)) and not self.retired
        self.auth_kind: str = str(data.get("auth_kind") or "oauth")
        self.api_provider: str | None = data.get("api_provider")
        if self.api_provider is None and _is_api_auth_kind(self.auth_kind):
            self.api_provider = self.auth_kind.removesuffix("_api")
        self.display_name: str = str(
            data.get("display_name") or self.email or self.id
        )
        self.api_account_id: str | None = data.get("api_account_id")
        self.supported_models: list[str] | None = data.get("supported_models")
        self.service_tiers: dict[str, list[str]] = {
            str(model): [
                str(tier) for tier in tiers if isinstance(tier, str)
            ]
            for model, tiers in (data.get("service_tiers") or {}).items()
            if isinstance(model, str) and isinstance(tiers, list)
        }
        self._api_account = data.get("_api_account")

    @classmethod
    def from_cloudrouter(cls, account) -> "CodexPoolAccount":
        auth_kind = str(getattr(account, "auth_kind", "") or "cloudrouter_api")
        retired = bool(getattr(account, "retired", False))
        cleanup_pending = bool(
            getattr(account, "cleanup_pending", False)
        )
        return cls({
            "id": account.id,
            "codex_home": str(account.codex_home),
            "email": "",
            # Keep the home as a disabled projection when refreshed models no
            # longer include Codex. Old rollouts remain migration evidence.
            "enabled": (
                bool(getattr(account, "enabled", True))
                and not retired
                and bool((account.models or {}).get("codex"))
            ),
            "retired": retired,
            "cleanup_pending": cleanup_pending,
            "auth_kind": auth_kind,
            "api_provider": (
                getattr(account, "api_provider", None)
                or auth_kind.removesuffix("_api")
            ),
            "display_name": account.name,
            "api_account_id": account.id,
            "supported_models": list((account.models or {}).get("codex", [])),
            "service_tiers": dict(getattr(account, "service_tiers", {}) or {}),
            "_api_account": account,
        })

    def supports_model(self, model: str | None) -> bool:
        requested_model = _service_tier_model(model)
        if not _is_api_auth_kind(self.auth_kind):
            state, _ = _models_cache_model_state(
                self.codex_home,
                requested_model,
            )
            # A missing/unreadable catalog is an unknown capability and keeps
            # native accounts eligible as a compatibility fallback.  A valid
            # catalog that omits the model is authoritative evidence that the
            # account cannot serve this turn.
            return state is not False
        try:
            return bool(
                self._api_account.supports_model("codex", requested_model)
            )
        except Exception:
            logger.exception(
                "Could not evaluate API account model support for %s", self.id
            )
            return False

    def supports_service_tier(
        self,
        model: str | None,
        service_tier: str | None,
    ) -> bool:
        requested_tier = _normalize_service_tier(service_tier)
        requested_model = _service_tier_model(model)
        if not _is_api_auth_kind(self.auth_kind):
            return _models_cache_supports_service_tier(
                self.codex_home,
                requested_model,
                requested_tier,
            )
        if not self.supports_model(requested_model):
            return False
        if requested_tier == "default":
            return True
        try:
            return bool(
                self._api_account.supports_service_tier(
                    "codex",
                    requested_model,
                    requested_tier,
                )
            )
        except Exception:
            logger.exception(
                "Could not evaluate API account service-tier support for %s",
                self.id,
            )
            return False


class CodexPool:
    """In-process Codex account pool with cooldown and quota tracking."""

    def __init__(
        self,
        config_path: str | Path | None = None,
        cooldown_seconds: int = DEFAULT_COOLDOWN_SECONDS,
        *,
        quota_reader: Callable[[str], Awaitable[dict]] | None = None,
        cloudrouter_store=None,
        bootstrap_default: bool = True,
        include_native: bool = True,
    ):
        if config_path:
            self._config_path = Path(os.path.expandvars(os.path.expanduser(str(config_path))))
        else:
            self._config_path = DEFAULT_CONFIG_PATH
        self._default_cooldown_seconds = cooldown_seconds
        self._cooldown_seconds = cooldown_seconds
        self._enabled = True
        self._quota_switch_threshold_percent = QUOTA_SWITCH_THRESHOLD_PERCENT
        self._routing_policy = "api_first"
        self._accounts: list[CodexPoolAccount] = []
        self._cooldowns: dict[str, float] = {}
        self._terminal_failures: set[str] = set()
        self._preferred_account_id: str | None = None
        self._last_selected_id: str | None = None
        self._last_selected_at: float = 0.0
        # Round-robin proposals advance independently from the UI's
        # last-successfully-routed marker.
        self._selection_cursor_id: str | None = None
        self._quota_cache: dict[str, dict] | None = None
        self._quota_cache_at: float = 0.0
        self._quota_cache_live_until: float = 0.0
        self._selection_quota_cache: dict[str, dict] | None = None
        self._selection_quota_cache_at: float = 0.0
        self._selection_quota_refresh_lock = asyncio.Lock()
        self._quota_reader = quota_reader
        self._cloudrouter_store = cloudrouter_store
        self._bootstrap_native = bool(bootstrap_default)
        self._include_native = bool(include_native)
        self._config_generation = 0
        self._load()

    @property
    def enabled(self) -> bool:
        return self._enabled

    def settings(self) -> dict:
        """Return the effective, non-secret runtime pool policy."""

        return {
            "enabled": self._enabled,
            "cooldown_seconds": self._cooldown_seconds,
            "quota_switch_threshold_percent": self._quota_switch_threshold_percent,
            "routing_policy": self._routing_policy,
            "preferred_account_id": self._preferred_account_id,
        }

    def _load_settings(self, data: dict) -> None:
        raw = data.get("pool_settings") or {}
        if not isinstance(raw, dict):
            raise ValueError("Codex pool_settings must be an object")

        enabled = raw.get("enabled", True)
        if not isinstance(enabled, bool):
            raise ValueError("Codex pool enabled must be a boolean")

        cooldown = raw.get("cooldown_seconds", self._default_cooldown_seconds)
        if isinstance(cooldown, bool) or not isinstance(cooldown, int):
            raise ValueError("Codex pool cooldown_seconds must be an integer")
        if not MIN_COOLDOWN_SECONDS <= cooldown <= MAX_COOLDOWN_SECONDS:
            raise ValueError("Codex pool cooldown_seconds is outside the supported range")

        threshold = raw.get(
            "quota_switch_threshold_percent",
            QUOTA_SWITCH_THRESHOLD_PERCENT,
        )
        if isinstance(threshold, bool) or not isinstance(threshold, (int, float)):
            raise ValueError("Codex pool quota_switch_threshold_percent must be numeric")
        threshold = float(threshold)
        if not math.isfinite(threshold) or not (
            MIN_QUOTA_SWITCH_THRESHOLD_PERCENT
            <= threshold
            <= MAX_QUOTA_SWITCH_THRESHOLD_PERCENT
        ):
            raise ValueError(
                "Codex pool quota_switch_threshold_percent is outside the supported range"
            )

        routing_policy = raw.get("routing_policy", "api_first")
        if routing_policy not in ROUTING_POLICIES:
            raise ValueError(f"Unsupported Codex pool routing policy: {routing_policy!r}")

        preferred = raw.get("preferred_account_id", self._preferred_account_id)
        if preferred is not None and not isinstance(preferred, str):
            raise ValueError("Codex pool preferred_account_id must be a string or null")

        self._enabled = enabled
        self._cooldown_seconds = cooldown
        self._quota_switch_threshold_percent = threshold
        self._routing_policy = routing_policy
        self._preferred_account_id = preferred or None

    def _load(self):
        if self._include_native and not self._config_path.exists():
            if self._bootstrap_native:
                self._bootstrap_default()
            if not self._config_path.exists():
                logger.info(
                    "Native Codex pool config not found at %s; loading API accounts only",
                    self._config_path,
                )
        try:
            data = (
                json.loads(self._config_path.read_text(encoding="utf-8"))
                if self._config_path.exists()
                else {"accounts": []}
            )
            self._load_settings(data)
            native_records = data.get("accounts", []) if self._include_native else []
            accounts = [CodexPoolAccount(a) for a in native_records]
            if self._cloudrouter_store is not None:
                known_ids = {account.id for account in accounts}
                known_homes = {account.codex_home for account in accounts}
                for api_account in self._cloudrouter_store.all_accounts(
                    include_retired=True
                ):
                    projection = CodexPoolAccount.from_cloudrouter(api_account)
                    if (
                        projection.id in known_ids
                        or projection.codex_home in known_homes
                    ):
                        logger.error(
                            "Skipping duplicate API Codex projection "
                            "%s (%s)",
                            projection.id,
                            projection.codex_home,
                        )
                        continue
                    accounts.append(projection)
                    known_ids.add(projection.id)
                    known_homes.add(projection.codex_home)
            account_ids = [account.id for account in accounts]
            account_homes = [account.codex_home for account in accounts]
            if len(account_ids) != len(set(account_ids)):
                raise ValueError("Codex pool account ids must be unique")
            if len(account_homes) != len(set(account_homes)):
                raise ValueError(
                    "Each Codex pool account must use a distinct CODEX_HOME"
                )
            self._accounts = accounts
            logger.info("Codex pool loaded %d accounts from %s", len(self._accounts), self._config_path)
        except Exception:
            logger.exception("Failed to load codex pool config")

    def _bootstrap_default(self):
        """If no pool config exists but ~/.codex/auth.json does, bootstrap it."""
        default_auth = Path.home() / ".codex" / "auth.json"
        if not default_auth.exists():
            return
        try:
            auth = json.loads(default_auth.read_text())
            tokens = auth.get("tokens") or {}
            if not tokens.get("access_token"):
                return
        except Exception:
            return
        # Try to get email from id_token JWT
        email = _extract_email_from_jwt(tokens.get("id_token", ""))
        data = {"accounts": [{
            "id": "codex-1",
            "codex_home": str(Path.home() / ".codex"),
            "email": email or "default",
            "enabled": True,
        }]}
        self._config_path.parent.mkdir(parents=True, exist_ok=True)
        self._config_path.write_text(json.dumps(data, indent=2, ensure_ascii=False))
        logger.info("Bootstrapped default codex account (%s) into pool", email)

    def reload(self):
        # In-flight quota reads must not publish data for an identity that was
        # replaced under the same account id/home.
        self._config_generation += 1
        self._accounts.clear()
        self._load()

        valid_ids = {
            account.id for account in self._accounts if not account.retired
        }
        self._cooldowns = {
            account_id: until
            for account_id, until in self._cooldowns.items()
            if account_id in valid_ids
        }
        self._terminal_failures.intersection_update(valid_ids)
        if self._preferred_account_id not in valid_ids:
            self._preferred_account_id = None
        if self._last_selected_id not in valid_ids:
            self._last_selected_id = None
            self._last_selected_at = 0.0
        if self._selection_cursor_id not in valid_ids:
            self._selection_cursor_id = None
        # Account membership/home changes invalidate every quota entry, even
        # when the same account id remains in the reloaded file.
        self._quota_cache = None
        self._quota_cache_at = 0.0
        self._quota_cache_live_until = 0.0
        self._selection_quota_cache = None
        self._selection_quota_cache_at = 0.0

    def account(self, account_id: str) -> CodexPoolAccount | None:
        return next((a for a in self._accounts if a.id == account_id), None)

    @staticmethod
    def canonical_home(codex_home: str | os.PathLike[str]) -> str:
        return canonical_codex_home(codex_home)

    def account_for_home(
        self, codex_home: str | os.PathLike[str]
    ) -> CodexPoolAccount | None:
        """Return the registered account owning ``codex_home``."""

        try:
            target = canonical_codex_home(codex_home)
        except (OSError, RuntimeError, TypeError, ValueError):
            return None
        return next((a for a in self._accounts if a.codex_home == target), None)

    def account_id_for_home(self, codex_home: str | os.PathLike[str]) -> str | None:
        account = self.account_for_home(codex_home)
        return account.id if account else None

    # Explicit spelling for call sites where several provider pools coexist.
    account_id_from_codex_home = account_id_for_home

    def home_for_account(self, account_id: str) -> str | None:
        account = self.account(account_id)
        return account.codex_home if account else None

    def account_status(self, account_id: str) -> dict | None:
        """Return current enabled/cooldown state for one account id."""

        account = self.account(account_id)
        if not account:
            return None
        now = time.time()
        cooldown_until = self._cooldowns.get(account.id, 0)
        quota_decision = self._api_quota_decision(account)
        available = (
            account.enabled
            and now >= cooldown_until
            and (
                _is_api_auth_kind(account.auth_kind)
                or not self._cached_native_quota_is_unavailable(account.id)
            )
            and not (
                bool(quota_decision.get("known"))
                and quota_decision.get("available") is False
            )
        )
        return {
            "id": account.id,
            "codex_home": account.codex_home,
            "email": account.email,
            "enabled": account.enabled,
            "retired": account.retired,
            "cleanup_pending": account.cleanup_pending,
            "login_recovery_failed": account.login_recovery_failed,
            "quota_valid_after": account.quota_valid_after or None,
            "quota_cutoff_invalid": account.quota_cutoff_invalid,
            "available": available,
            "cooldown_until": cooldown_until if cooldown_until > now else None,
            "cooldown_remaining": (
                max(0, cooldown_until - now) if cooldown_until > now else 0
            ),
            "auth_kind": account.auth_kind,
            "api_provider": account.api_provider,
            "display_name": account.display_name,
            "api_account_id": account.api_account_id,
            "supported_models": account.supported_models,
            "service_tiers": account.service_tiers,
            "api_quota": self._cached_api_quota(account.id),
        }

    def _api_quota_decision(self, account: CodexPoolAccount) -> dict:
        if (
            not _is_api_auth_kind(account.auth_kind)
            or self._cloudrouter_store is None
            or not account.api_account_id
        ):
            return {"available": True, "known": False, "reason": ""}
        try:
            decision = self._cloudrouter_store.cached_quota_decision(
                account.api_account_id
            )
            return decision if isinstance(decision, dict) else {
                "available": True,
                "known": False,
                "reason": "",
            }
        except Exception:
            logger.exception(
                "Could not read cached API quota for %s", account.id
            )
            return {"available": True, "known": False, "reason": ""}

    def _cached_api_quota(self, account_id: str) -> dict | None:
        for cache in (self._selection_quota_cache, self._quota_cache):
            if not isinstance(cache, dict):
                continue
            row = cache.get(account_id)
            snapshot = row.get("api_quota") if isinstance(row, dict) else None
            if isinstance(snapshot, dict):
                return snapshot
        return None

    def _cached_native_quota_state(
        self,
        account_id: str,
        *,
        now: float | None = None,
    ) -> tuple[str, dict | None]:
        """Return ``available``, ``unavailable``, or ``unknown`` quota state.

        A structured usage-limit terminal is stronger than the app-server's
        sometimes stale 0% snapshot until the terminal's stated reset. Ordinary
        percentage snapshots, however, follow cache recency so a later healthy
        live read can reopen an account before an older rollout cache expires.
        Unknown reads never erase the last known terminal evidence.
        """

        current = time.time() if now is None else now
        evidence: list[tuple[float, dict]] = []
        for cache, observed_at in (
            (self._selection_quota_cache, self._selection_quota_cache_at),
            (self._quota_cache, self._quota_cache_at),
        ):
            if not isinstance(cache, dict):
                continue
            row = cache.get(account_id)
            quota = row.get("quota") if isinstance(row, dict) else None
            if isinstance(quota, dict):
                evidence.append((observed_at, quota))
        if not evidence:
            return "unknown", None

        active_terminals = [
            quota
            for _, quota in evidence
            if quota.get("usage_limit_exceeded") is True
            and quota_at_or_above(
                quota,
                threshold=self._quota_switch_threshold_percent,
                now=current,
            )
        ]
        if active_terminals:
            return "unavailable", active_terminals[0]

        newest_at = max(float(observed_at or 0) for observed_at, _ in evidence)
        newest = [quota for observed_at, quota in evidence if observed_at == newest_at]
        unavailable = next(
            (
                quota
                for quota in newest
                if quota_at_or_above(
                    quota,
                    threshold=self._quota_switch_threshold_percent,
                    now=current,
                )
            ),
            None,
        )
        if unavailable is not None:
            return "unavailable", unavailable
        return "available", newest[-1]

    def _cached_native_quota_is_unavailable(self, account_id: str) -> bool:
        state, _ = self._cached_native_quota_state(account_id)
        return state == "unavailable"

    async def refresh_selection_quota(self, *, force: bool = False) -> None:
        """Refresh native rollout evidence before assigning a fresh turn.

        This intentionally stays rollout-only: admission must not start every
        account's app-server, and filesystem scanning runs off the event loop.
        A single-flight lock prevents concurrent fresh tasks from duplicating a
        potentially large account-history scan.
        """

        now = time.time()
        if (
            not force
            and self._selection_quota_cache is not None
            and now - self._selection_quota_cache_at < QUOTA_CACHE_TTL
        ):
            return
        async with self._selection_quota_refresh_lock:
            now = time.time()
            if (
                not force
                and self._selection_quota_cache is not None
                and now - self._selection_quota_cache_at < QUOTA_CACHE_TTL
            ):
                return
            while True:
                generation = self._config_generation
                accounts = [
                    account
                    for account in self._accounts
                    if account.enabled
                    and not account.retired
                    and not _is_api_auth_kind(account.auth_kind)
                ]
                quotas = await asyncio.gather(
                    *(
                        asyncio.to_thread(
                            _read_quota_from_rollout,
                            account.codex_home,
                            min_event_timestamp=(
                                account.quota_valid_after or None
                            ),
                        )
                        if not account.quota_cutoff_invalid
                        else asyncio.sleep(0, result=None)
                        for account in accounts
                    )
                )
                if generation == self._config_generation:
                    break
                logger.info(
                    "Discarding Codex selection quota read across pool reload "
                    "(%s -> %s)",
                    generation,
                    self._config_generation,
                )
            self._selection_quota_cache = {
                account.id: {
                    "id": account.id,
                    "quota": quota,
                    "error": (
                        "invalid_quota_cutoff"
                        if account.quota_cutoff_invalid
                        else None if quota else "no_rollout_data"
                    ),
                }
                for account, quota in zip(accounts, quotas)
            }
            self._selection_quota_cache_at = time.time()

    def home_status(self, codex_home: str | os.PathLike[str]) -> dict | None:
        account = self.account_for_home(codex_home)
        return self.account_status(account.id) if account else None

    def is_home_enabled(self, codex_home: str | os.PathLike[str]) -> bool:
        account = self.account_for_home(codex_home)
        return bool(account and account.enabled)

    def is_home_available(self, codex_home: str | os.PathLike[str]) -> bool:
        state = self.home_status(codex_home)
        return bool(state and state["available"])

    def supports_model_for_home(
        self,
        codex_home: str | os.PathLike[str],
        model: str | None,
        *,
        service_tier: str = "default",
    ) -> bool:
        account = self.account_for_home(codex_home)
        requested_tier = _normalize_service_tier(service_tier)
        if account is None:
            return _models_cache_supports_service_tier(
                canonical_codex_home(codex_home),
                model,
                requested_tier,
            )
        return account.supports_service_tier(model, requested_tier)

    def has_compatible_enabled_account(
        self,
        model: str | None,
        *,
        service_tier: str = "default",
    ) -> bool:
        """Whether model routing is possible independent of cooldown/quota."""

        requested_tier = _normalize_service_tier(service_tier)
        return any(
            account.enabled
            and not account.retired
            and account.supports_service_tier(model, requested_tier)
            for account in self._accounts
        )

    def has_retryable_compatible_account(
        self,
        model: str | None,
        *,
        service_tier: str = "default",
    ) -> bool:
        """Whether unavailable compatible accounts can recover automatically."""

        requested_tier = _normalize_service_tier(service_tier)
        for account in self._accounts:
            if (
                not account.enabled
                or account.retired
                or not account.supports_service_tier(
                    model,
                    requested_tier,
                )
            ):
                continue
            if account.id in self._terminal_failures:
                continue
            if not _is_api_auth_kind(account.auth_kind):
                return True
            decision = self._api_quota_decision(account)
            if not (
                bool(decision.get("known"))
                and decision.get("available") is False
            ):
                return True
        return False

    def has_native_enabled_account(self) -> bool:
        """Whether service-default fallback remains backed by a native account."""

        return any(
            account.enabled
            and not account.retired
            and not _is_api_auth_kind(account.auth_kind)
            for account in self._accounts
        )

    def is_disabled(self, codex_home: str | os.PathLike[str]) -> bool:
        """Whether a known account home is explicitly disabled."""

        account = self.account_for_home(codex_home)
        return account is not None and not account.enabled

    def is_known_account(self, codex_home: str | os.PathLike[str]) -> bool:
        return self.account_for_home(codex_home) is not None

    def list_accounts(self) -> list[dict]:
        result: list[dict] = []
        for account in self._accounts:
            # Retired tombstones remain internally addressable so historical
            # task bindings can migrate their rollout. A pending API cleanup
            # remains visible so an administrator can retry DELETE after the
            # active runtime user exits; finalized tombstones stay hidden.
            pending_api_cleanup = bool(
                account.retired
                and account.cleanup_pending
                and _is_api_auth_kind(account.auth_kind)
            )
            if (account.retired and not pending_api_cleanup) or (
                not pending_api_cleanup
                and
                _is_api_auth_kind(account.auth_kind)
                and not account.supported_models
            ):
                continue
            state = self.account_status(account.id)
            if state is not None:
                result.append(state)
        return result

    def status(self) -> dict:
        accounts = self.list_accounts()
        return {
            "enabled": self.enabled,
            "total": len(accounts),
            "available": sum(1 for a in accounts if a["available"]),
            "cooldown": sum(1 for a in accounts if not a["available"] and a["enabled"]),
            "disabled": sum(1 for a in accounts if not a["enabled"]),
            "preferred": self._preferred_account_id,
            "last_selected": self._last_selected_id,
            "last_selected_at": self._last_selected_at or None,
            "settings": self.settings(),
            "accounts": accounts,
        }

    @property
    def preferred_account_id(self) -> str | None:
        return self._preferred_account_id

    def set_preferred(self, account_id: str | None) -> bool:
        if account_id is None:
            self._preferred_account_id = None
            return True
        if not any(
            a.id == account_id and not a.retired for a in self._accounts
        ):
            return False
        self._preferred_account_id = account_id
        return True

    def select(
        self,
        exclude: set[str] | None = None,
        *,
        model: str | None = None,
        service_tier: str = "default",
    ) -> str | None:
        """Pick an available CODEX_HOME. Returns None if all exhausted."""
        if not self.enabled:
            return None
        now = time.time()
        excluded = exclude or set()
        requested_tier = _normalize_service_tier(service_tier)
        candidates = []
        for account in self._accounts:
            decision = self._api_quota_decision(account)
            if (
                account.enabled
                and account.id not in excluded
                and now >= self._cooldowns.get(account.id, 0)
                and account.supports_service_tier(model, requested_tier)
                and (
                    _is_api_auth_kind(account.auth_kind)
                    or not self._cached_native_quota_is_unavailable(account.id)
                )
                and not (
                    bool(decision.get("known"))
                    and decision.get("available") is False
                )
            ):
                candidates.append(account)
        if not candidates:
            return None

        # Prefer the pinned account if available
        if self._preferred_account_id:
            preferred = next((a for a in candidates if a.id == self._preferred_account_id), None)
            if preferred:
                return self._record_selection(preferred, now)

        # Fresh launches prefer a compatible API account only after its cached
        # health has been proven.  At process startup the API cache can still
        # be unknown; prefer a usable native account in that window so a stale
        # or invalid API key cannot consume a turn before its first probe.  An
        # unknown API account remains the final fallback for API-only setups.
        # True round-robin is preserved within every group.
        api_candidates = [
            account
            for account in candidates
            if _is_api_auth_kind(account.auth_kind)
        ]
        api_decisions = {
            account.id: self._api_quota_decision(account)
            for account in api_candidates
        }
        verified_api_candidates = [
            account
            for account in api_candidates
            if (
                bool(api_decisions[account.id].get("known"))
                and api_decisions[account.id].get("available") is True
            )
        ]
        verified_api_ids = {
            account.id for account in verified_api_candidates
        }
        unknown_api_candidates = [
            account
            for account in api_candidates
            if account.id not in verified_api_ids
        ]
        known_native_candidates = [
            account
            for account in candidates
            if not _is_api_auth_kind(account.auth_kind)
            and self._cached_native_quota_state(account.id)[0] == "available"
        ]
        unknown_native_candidates = [
            account
            for account in candidates
            if not _is_api_auth_kind(account.auth_kind)
            and self._cached_native_quota_state(account.id)[0] == "unknown"
        ]

        # A valid native catalog is stronger than an unknown one for an
        # explicit (or configured-default) model.  Keep unknown catalogs as a
        # final compatibility fallback, but do not let them win round-robin
        # selection over an account whose model support was verified.
        def split_model_catalog_candidates(
            native_candidates: list[CodexPoolAccount],
        ) -> tuple[list[CodexPoolAccount], list[CodexPoolAccount]]:
            known: list[CodexPoolAccount] = []
            unknown: list[CodexPoolAccount] = []
            for account in native_candidates:
                state, _ = _models_cache_model_state(account.codex_home, model)
                if state is True:
                    known.append(account)
                elif state is None:
                    unknown.append(account)
            return known, unknown

        known_quota_known_model, known_quota_unknown_model = (
            split_model_catalog_candidates(known_native_candidates)
        )
        unknown_quota_known_model, unknown_quota_unknown_model = (
            split_model_catalog_candidates(unknown_native_candidates)
        )
        groups = (
            (
                known_quota_known_model,
                known_quota_unknown_model,
                unknown_quota_known_model,
                unknown_quota_unknown_model,
                verified_api_candidates,
                unknown_api_candidates,
            )
            if self._routing_policy == "native_first"
            else (
                verified_api_candidates,
                known_quota_known_model,
                known_quota_unknown_model,
                unknown_quota_known_model,
                unknown_quota_unknown_model,
                unknown_api_candidates,
            )
        )
        for group in groups:
            chosen = self._round_robin_candidate(group)
            if chosen is not None:
                return self._record_selection(chosen, now)
        return None

    def _round_robin_candidate(
        self, candidates: list[CodexPoolAccount]
    ) -> CodexPoolAccount | None:
        """Return the next config-order candidate without recording usage."""

        if not candidates:
            return None
        candidate_ids = {account.id for account in candidates}
        start = 0
        if self._selection_cursor_id:
            previous = next(
                (
                    index
                    for index, account in enumerate(self._accounts)
                    if account.id == self._selection_cursor_id
                ),
                None,
            )
            if previous is not None:
                start = (previous + 1) % len(self._accounts)
        for offset in range(len(self._accounts)):
            chosen = self._accounts[(start + offset) % len(self._accounts)]
            if chosen.id in candidate_ids:
                return chosen
        return None

    def _record_selection(self, account: CodexPoolAccount, now: float) -> str:
        self._selection_cursor_id = account.id
        logger.info("Codex pool selected account %s (%s)", account.id, account.codex_home)
        return account.codex_home

    def _record_routed_account(
        self, account: CodexPoolAccount, now: float
    ) -> None:
        self._last_selected_id = account.id
        self._last_selected_at = now

    def record_routed_account(self, codex_home: str) -> bool:
        """Record the account chosen as the final route for a Codex launch.

        A selected home can differ from the final route after resident-thread
        discovery or a migration fallback. Selection therefore advances only
        an internal round-robin cursor; callers record here after the final
        route is committed or an auxiliary process has spawned. Unknown homes
        do not alter the current marker.
        """

        account = self.account_for_home(codex_home)
        if account is None:
            logger.warning(
                "Cannot record routed Codex account for unknown CODEX_HOME: %s",
                codex_home,
            )
            return False
        self._record_routed_account(account, time.time())
        logger.info(
            "Codex pool recorded routed account %s (%s)",
            account.id,
            account.codex_home,
        )
        return True

    def mark_rate_limited(self, codex_home: str, duration: int | None = None):
        acc = self._find_by_home(codex_home)
        if acc:
            d = duration if duration is not None else self._cooldown_seconds
            self._terminal_failures.discard(acc.id)
            self._cooldowns[acc.id] = time.time() + d
            logger.info("Codex pool: marked %s rate-limited for %ds", acc.id, d)

    def quota_retry_after(self) -> float | None:
        """Return the earliest bounded recovery delay for cached native quota."""

        delays: list[float] = []
        now = time.time()
        for account in self._accounts:
            if (
                not account.enabled
                or account.retired
                or _is_api_auth_kind(account.auth_kind)
            ):
                continue
            state, quota = self._cached_native_quota_state(account.id, now=now)
            if state != "unavailable" or quota is None:
                continue
            delays.append(
                float(
                    quota_cooldown_seconds(
                        quota,
                        threshold=self._quota_switch_threshold_percent,
                        now=now,
                        fallback=self._cooldown_seconds,
                    )
                )
            )
        return max(1.0, min(delays)) if delays else None

    def mark_auth_failure(self, codex_home: str):
        acc = self._find_by_home(codex_home)
        if acc:
            self._terminal_failures.add(acc.id)
            self._cooldowns[acc.id] = time.time() + 365 * 86400
            logger.info("Codex pool: marked %s auth-failed (indefinite)", acc.id)

    def is_in_cooldown(self, codex_home: str) -> bool:
        acc = self._find_by_home(codex_home)
        if not acc:
            return False
        return time.time() < self._cooldowns.get(acc.id, 0)

    def clear_cooldown(self, account_id: str):
        self._cooldowns.pop(account_id, None)
        self._terminal_failures.discard(account_id)

    def _find_by_home(self, codex_home: str) -> CodexPoolAccount | None:
        return self.account_for_home(codex_home)

    def _session_search_homes(self, extra_homes: list[str] | None = None) -> list[str]:
        candidates: list[str | os.PathLike[str]] = [
            account.codex_home for account in self._accounts
        ]
        env_home = os.environ.get("CODEX_HOME")
        if env_home:
            candidates.append(env_home)
        candidates.append(Path.home() / ".codex")
        if extra_homes:
            candidates.extend(extra_homes)

        # Include orphaned homes left by accounts removed from accounts.json.
        # Their rollout may be the only copy of a task's native thread.
        try:
            candidates.extend(
                path
                for path in sorted(Path.home().iterdir())
                if path.is_dir()
                and (path.name == ".codex" or path.name.startswith(".codex-"))
            )
        except OSError:
            pass

        homes: list[str] = []
        seen: set[str] = set()
        for candidate in candidates:
            try:
                canonical = canonical_codex_home(candidate)
            except (OSError, RuntimeError, TypeError, ValueError):
                continue
            if canonical in seen:
                continue
            seen.add(canonical)
            homes.append(canonical)
        return homes

    def locate_session_homes(
        self,
        session_id: str,
        extra_homes: list[str] | None = None,
    ) -> list[str]:
        """Return every CODEX_HOME containing a rollout for ``session_id``.

        Multiple copies are expected after account migration.  Returning all
        homes lets the dispatcher use the task's account affinity to choose;
        this method never silently makes that ownership decision.
        """

        if not isinstance(session_id, str) or not _SESSION_ID_RE.fullmatch(session_id):
            raise ValueError(
                "Invalid Codex session id; expected letters, digits, '.', '_' or '-'"
            )
        matches: list[str] = []
        pattern = f"*/*/*/rollout-*-{session_id}.jsonl"
        for home in self._session_search_homes(extra_homes):
            try:
                found = any(
                    rollout.is_file()
                    for rollout in (Path(home) / "sessions").glob(pattern)
                )
            except OSError:
                continue
            if found:
                matches.append(home)
        return matches

    def locate_session_home(
        self,
        session_id: str,
        extra_homes: list[str] | None = None,
    ) -> str | None:
        """Return the unique home holding a session, or raise on ambiguity."""

        homes = self.locate_session_homes(session_id, extra_homes=extra_homes)
        if len(homes) > 1:
            raise AmbiguousCodexSessionHomeError(session_id, homes)
        return homes[0] if homes else None

    # --- Quota tracking (live account RPC and rollout-backed rotation) ---

    async def fetch_quota(
        self, force: bool = False, *, live: bool = False,
    ) -> list[dict]:
        """Read per-account quota, optionally querying app-server live.

        ``force`` bypasses the short cache. ``live`` is reserved for an
        explicit user refresh: background quota checks continue using rollout
        files so they do not start every account's app-server after each turn.
        """
        if live:
            force = True
        now = time.time()
        if not force and self._quota_cache is not None and (now - self._quota_cache_at) < QUOTA_CACHE_TTL:
            return list(self._quota_cache.values())

        async def _read_account_quota(
            acc: CodexPoolAccount,
        ) -> tuple[dict | None, dict | None, str | None]:
            if _is_api_auth_kind(acc.auth_kind):
                if self._cloudrouter_store is None or not acc.api_account_id:
                    return None, None, "api_store_unavailable"
                try:
                    snapshot = await self._cloudrouter_store.fetch_usage(
                        acc.api_account_id,
                        force=force,
                    )
                except Exception as exc:
                    logger.warning(
                        "API usage read failed for %s: %s", acc.id, exc
                    )
                    return None, None, f"request_failed: {exc}"[:200]
                if not isinstance(snapshot, dict):
                    return None, None, "invalid_api_quota"
                error = None
                if (
                    bool(snapshot.get("known"))
                    and snapshot.get("available") is False
                ):
                    error = str(snapshot.get("reason") or "unavailable")
                return None, snapshot, error
            if live:
                if self._quota_reader is None:
                    logger.warning(
                        "Codex live quota reader is unavailable for %s",
                        acc.id,
                    )
                    return None, None, "live_unavailable"
                try:
                    response = await self._quota_reader(acc.codex_home)
                    quota = _quota_from_app_server_response(response)
                    if quota is not None:
                        return quota, None, None
                    logger.warning(
                        "Codex live quota response had no rate-limit snapshot: %s",
                        acc.id,
                    )
                except Exception as exc:
                    logger.warning(
                        "Codex live quota read failed for %s: %s",
                        acc.id,
                        exc,
                    )
                # A rollout belongs to a session, not to credentials. Session
                # migration preserves its old rate_limits events, so it cannot
                # safely substitute for an unavailable live account response.
                return None, None, "live_unavailable"

            if acc.quota_cutoff_invalid:
                return None, None, "invalid_quota_cutoff"
            quota = await asyncio.to_thread(
                _read_quota_from_rollout,
                acc.codex_home,
                min_event_timestamp=acc.quota_valid_after or None,
            )
            return quota, None, None

        while True:
            generation = self._config_generation
            enabled_accounts = [acc for acc in self._accounts if acc.enabled]
            quota_results = await asyncio.gather(*(
                _read_account_quota(acc) for acc in enabled_accounts
            ))
            if generation == self._config_generation:
                break
            logger.info(
                "Discarding Codex quota read across pool reload (%s -> %s)",
                generation,
                self._config_generation,
            )

        results = {}
        for acc, (quota, api_quota, quota_error) in zip(
            enabled_accounts, quota_results
        ):
            results[acc.id] = {
                "id": acc.id,
                "email": acc.email,
                "codex_home": acc.codex_home,
                "plan_type": quota.get("plan_type") if quota else None,
                "quota": quota,
                "api_quota": api_quota,
                "auth_kind": acc.auth_kind,
                "api_provider": acc.api_provider,
                "display_name": acc.display_name,
                "api_account_id": acc.api_account_id,
                "supported_models": acc.supported_models,
                "error": (
                    quota_error
                    if quota_error
                    else None
                    if quota or api_quota
                    else "no_rollout_data"
                ),
            }

        completed_at = time.time()
        if live:
            self._quota_cache = results
            self._quota_cache_at = completed_at
            self._quota_cache_live_until = completed_at + QUOTA_CACHE_TTL
        else:
            # Quota-aware rotation needs the exact rollout snapshot it just
            # selected on, even while a live UI result is protected by TTL.
            self._selection_quota_cache = results
            self._selection_quota_cache_at = completed_at
            if completed_at >= self._quota_cache_live_until:
                # A background rollout scan may overlap a manual live refresh.
                # Return its fresh data to quota-aware switching, but do not let
                # it replace a newer authoritative UI snapshot during the TTL.
                self._quota_cache = results
                self._quota_cache_at = completed_at
        return list(results.values())

    async def verify_account_live(self, account_id: str) -> dict:
        """Classify one account with an authenticated app-server RPC.

        Reading auth.json alone cannot detect a revoked refresh token.  A live
        rate-limit RPC proves the credential is accepted; transient transport
        failures remain ``logged_in=None`` so callers fail closed instead of
        unnecessarily launching a destructive relogin.
        """
        account = self.account(account_id)
        if account is None or account.retired:
            return {"logged_in": False, "detail": "account missing"}
        local = await asyncio.to_thread(
            verify_login,
            account.codex_home,
            auth_kind=account.auth_kind,
        )
        if local.get("logged_in") is not True:
            return local
        if _is_api_auth_kind(account.auth_kind):
            if self._cloudrouter_store is None or not account.api_account_id:
                return {
                    **local,
                    "logged_in": None,
                    "live_verified": False,
                    "detail": "API account store unavailable",
                }
            try:
                snapshot = await self._cloudrouter_store.fetch_usage(
                    account.api_account_id,
                    force=True,
                )
            except Exception:
                return {
                    **local,
                    "logged_in": None,
                    "live_verified": False,
                    "detail": "live account verification temporarily unavailable",
                }
            reason = str(
                snapshot.get("reason") if isinstance(snapshot, dict) else ""
            )
            if reason in {"invalid_api_key", "forbidden"}:
                return {
                    **local,
                    "logged_in": False,
                    "live_verified": True,
                    "detail": "live account authentication was rejected",
                }
            return {
                **local,
                "logged_in": True,
                "live_verified": True,
                "detail": "ok",
                "api_quota": snapshot,
            }
        if self._quota_reader is None:
            return {
                **local,
                "logged_in": None,
                "live_verified": False,
                "detail": "live account verification unavailable",
            }
        try:
            await self._quota_reader(account.codex_home)
        except Exception as exc:
            detail = str(exc)
            if is_auth_failure(detail):
                return {
                    **local,
                    "logged_in": False,
                    "live_verified": True,
                    "detail": "live account authentication was rejected",
                }
            return {
                **local,
                "logged_in": None,
                "live_verified": False,
                "detail": "live account verification temporarily unavailable",
            }
        return {
            **local,
            "logged_in": True,
            "live_verified": True,
            "detail": "ok",
        }

    async def select_quota_alternative(
        self,
        current_home: str,
        *,
        threshold: float | None = None,
        model: str | None = None,
        service_tier: str = "default",
    ) -> str | None:
        """Return a below-threshold alternative when the current home is high.

        Quota is refreshed from each account's latest rollout after a completed
        turn. Unknown quota remains eligible, while known-high, disabled, and
        cooled accounts cannot be chosen. No cooldown is written here, so a pool
        with no usable alternative simply continues on the current account.
        """

        if not self.enabled:
            return None
        if threshold is None:
            threshold = self._quota_switch_threshold_percent
        current_id = self.account_id_for_home(current_home)
        if not current_id:
            return None
        quota_by_id = {
            row["id"]: row for row in await self.fetch_quota(force=True)
        }
        current = quota_by_id.get(current_id)
        if not current or not (
            quota_at_or_above(current.get("quota"), threshold=threshold)
            or api_quota_at_or_above(
                current.get("api_quota"), threshold=threshold
            )
        ):
            return None

        excluded = {current_id}
        alternatives = [
            account
            for account in self._accounts
            if account.enabled and account.id != current_id
        ]
        login_states = await asyncio.gather(
            *(
                asyncio.to_thread(
                    verify_login,
                    account.codex_home,
                    auth_kind=account.auth_kind,
                )
                for account in alternatives
            ),
            return_exceptions=True,
        )
        for account, login_state in zip(alternatives, login_states):
            if (
                isinstance(login_state, dict)
                and login_state.get("logged_in") is False
            ):
                excluded.add(account.id)

        for account in alternatives:
            row = quota_by_id.get(account.id)
            if row and (
                quota_at_or_above(row.get("quota"), threshold=threshold)
                or api_quota_at_or_above(
                    row.get("api_quota"), threshold=threshold
                )
            ):
                excluded.add(account.id)
        return self.select(
            exclude=excluded,
            model=model,
            service_tier=service_tier,
        )

    def cached_quota_for_home(self, codex_home: str) -> dict | None:
        """Return the latest selection snapshot for one account home."""

        account_id = self.account_id_for_home(codex_home)
        if not account_id:
            return None
        _, quota = self._cached_native_quota_state(account_id)
        return quota if isinstance(quota, dict) else None


# ---------------------------------------------------------------------------
# Quota helpers
# ---------------------------------------------------------------------------

def _read_quota_from_rollout(
    codex_home: str,
    *,
    min_event_timestamp: float | None = None,
) -> dict | None:
    """Parse the newest rate_limits event across an account's rollouts.

    Session migration changes the destination file's mtime without changing
    the embedded events. Read only the last valid quota event in each JSONL,
    then compare those events by their own timestamps. Missing or malformed
    event timestamps fall back to mtime for compatibility with older files.
    When a login activation cutoff is present, events without a valid embedded
    timestamp are rejected: a migrated old session's fresh mtime must never
    make its previous identity's quota look current.
    """
    if min_event_timestamp is not None and (
        not isinstance(min_event_timestamp, (int, float))
        or isinstance(min_event_timestamp, bool)
        or not math.isfinite(float(min_event_timestamp))
    ):
        return None
    sessions_dir = Path(codex_home) / "sessions"
    if not sessions_dir.is_dir():
        return None

    latest_key: tuple[float, str] | None = None
    latest_quota: dict | None = None
    for path in sessions_dir.glob("*/*/*/rollout-*.jsonl"):
        try:
            with path.open("rb") as stream:
                rollout_stat = os.fstat(stream.fileno())
                if not stat.S_ISREG(rollout_stat.st_mode):
                    continue
                foreign_prefix_bytes = read_rollout_migration_marker(
                    path,
                    rollout_stat=rollout_stat,
                )
                candidate = _latest_quota_event_in_rollout(
                    path,
                    rollout_stat.st_mtime,
                    min_event_timestamp=min_event_timestamp,
                    foreign_prefix_bytes=foreign_prefix_bytes,
                    stream=stream,
                )
                if read_rollout_migration_marker(
                    path,
                    rollout_stat=rollout_stat,
                ) != foreign_prefix_bytes:
                    continue
        except (CodexRolloutMigrationMetadataError, OSError) as exc:
            if isinstance(exc, CodexRolloutMigrationMetadataError):
                logger.warning(
                    "Ignoring Codex rollout with invalid migration metadata %s: %s",
                    path,
                    exc,
                )
            continue
        if candidate is None:
            continue
        event_timestamp, quota = candidate
        candidate_key = (event_timestamp, str(path))
        if latest_key is None or candidate_key > latest_key:
            latest_key = candidate_key
            latest_quota = quota
    return latest_quota


def _latest_quota_event_in_rollout(
    path: Path,
    fallback_mtime: float,
    *,
    min_event_timestamp: float | None = None,
    foreign_prefix_bytes: int | None = None,
    stream: BinaryIO | None = None,
) -> tuple[float, dict] | None:
    """Read one rollout tail and return its latest usable quota evidence.

    A native ``task_complete`` usage-limit error is durable account evidence
    even when the preceding ``token_count`` has null percentage windows. Treat
    that terminal event as a quota candidate so an older 0% rollout cannot win
    the account-wide timestamp comparison.
    """

    if stream is not None:
        lines = _iter_rollout_lines_reverse_stream(
            stream,
            minimum_offset=foreign_prefix_bytes or 0,
        )
    else:
        lines = (
            _iter_rollout_lines_reverse_after(path, foreign_prefix_bytes)
            if foreign_prefix_bytes
            else _iter_rollout_lines_reverse(path)
        )
    for raw_line in lines:
        if not raw_line:
            continue
        if (
            b'"rate_limits"' not in raw_line
            and b'"usage_limit_exceeded"' not in raw_line
            and b'"usageLimitExceeded"' not in raw_line
            and b'"codex_error_info"' not in raw_line
            and b'"codexErrorInfo"' not in raw_line
        ):
            continue
        try:
            event = json.loads(raw_line)
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue
        if not isinstance(event, dict):
            continue
        event_timestamp = _rollout_event_timestamp(event)
        if min_event_timestamp is not None and (
            event_timestamp is None
            or event_timestamp <= min_event_timestamp
        ):
            continue
        candidate_quota: dict | None = None
        if _rollout_usage_limit_evidence(event):
            candidate_quota = _terminal_usage_limit_quota(
                event,
                observed_at=(
                    fallback_mtime if event_timestamp is None else event_timestamp
                ),
            )
        else:
            payload = event.get("payload") or {}
            if isinstance(payload, dict) and payload.get("type") == "token_count":
                candidate_quota = _normalize_rate_limits(payload.get("rate_limits"))
        if candidate_quota is None:
            continue
        return (
            fallback_mtime if event_timestamp is None else event_timestamp,
            candidate_quota,
        )
    return None


_USAGE_LIMIT_RESET_RE = re.compile(
    r"\b(?:try\s+again|retry|available)\s+(?:at|after)\s+"
    r"([A-Za-z]{3,9}\s+\d{1,2}(?:st|nd|rd|th)?,\s+\d{4}\s+"
    r"\d{1,2}:\d{2}\s+[AP]M)\b",
    re.IGNORECASE,
)


def _usage_limit_reset_at(event: dict) -> float | None:
    """Parse the local-time reset printed in a structured Codex terminal."""

    payload = event.get("payload")
    error = payload.get("error") if isinstance(payload, dict) else None
    if not isinstance(error, dict):
        error = event.get("error")
    if not isinstance(error, dict):
        return None
    for key in ("resets_at", "resetsAt", "reset_at", "resetAt"):
        value = error.get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            reset_at = float(value)
            return reset_at / 1000 if reset_at > 10_000_000_000 else reset_at
    message = error.get("message")
    if not isinstance(message, str):
        return None
    match = _USAGE_LIMIT_RESET_RE.search(message)
    if not match:
        return None
    normalized = re.sub(r"(?<=\d)(?:st|nd|rd|th)\b", "", match.group(1), flags=re.I)
    for date_format in ("%b %d, %Y %I:%M %p", "%B %d, %Y %I:%M %p"):
        try:
            # Codex renders this timestamp in the CLI's local timezone. Calling
            # ``astimezone`` on a naive datetime applies the host timezone (and
            # its DST rules) for that date before converting to Unix seconds.
            return datetime.strptime(normalized, date_format).astimezone().timestamp()
        except ValueError:
            continue
    return None


def _terminal_usage_limit_quota(event: dict, *, observed_at: float) -> dict:
    """Build a normalized quota snapshot for a structured terminal error."""

    reset_at = _usage_limit_reset_at(event)
    if reset_at is None or reset_at <= observed_at:
        reset_at = observed_at + DEFAULT_COOLDOWN_SECONDS
    return {
        "primary_used_percent": None,
        "primary_window_minutes": None,
        "primary_resets_at": None,
        "secondary_used_percent": None,
        "secondary_window_minutes": None,
        "secondary_resets_at": None,
        "plan_type": None,
        "is_rate_limited": True,
        "has_credits": None,
        "usage_limit_exceeded": True,
        "usage_limit_observed_at": observed_at,
        "usage_limit_resets_at": reset_at,
    }


def _rollout_usage_limit_evidence(event: dict) -> bool:
    """Recognize the exact structured Codex usage-limit terminal code."""

    payload = event.get("payload")
    if not isinstance(payload, dict) or payload.get("type") != "task_complete":
        return False
    error = payload.get("error")
    if not isinstance(error, dict):
        return False
    code = error.get("codex_error_info")
    if not isinstance(code, str):
        code = error.get("codexErrorInfo")
    if not isinstance(code, str):
        return False
    normalized = re.sub(r"[^a-z]", "", code.lower())
    return normalized == "usagelimitexceeded"


def _iter_rollout_lines_reverse(
    path: Path, *, chunk_size: int = 64 * 1024,
) -> Iterator[bytes]:
    """Yield JSONL lines newest-first without loading a rollout into memory."""

    with path.open("rb") as stream:
        yield from _iter_rollout_lines_reverse_stream(
            stream,
            chunk_size=chunk_size,
        )


def _iter_rollout_lines_reverse_after(
    path: Path,
    minimum_offset: int,
    *,
    chunk_size: int = 64 * 1024,
) -> Iterator[bytes]:
    """Yield complete JSONL lines newest-first after a byte offset.

    Migration markers describe a foreign byte prefix. A destination account
    may append native events to that same rollout, so only complete lines that
    begin at or after the marker are eligible quota evidence. If the boundary
    cuts through a line, that mixed line is discarded.
    """

    if (
        not isinstance(minimum_offset, int)
        or isinstance(minimum_offset, bool)
        or minimum_offset < 0
    ):
        raise ValueError("minimum_offset must be a non-negative integer")
    with path.open("rb") as stream:
        yield from _iter_rollout_lines_reverse_stream(
            stream,
            minimum_offset=minimum_offset,
            chunk_size=chunk_size,
        )


def _iter_rollout_lines_reverse_stream(
    stream: BinaryIO,
    *,
    minimum_offset: int = 0,
    chunk_size: int = 64 * 1024,
) -> Iterator[bytes]:
    """Yield reverse JSONL lines from an already-open rollout identity."""

    if (
        not isinstance(minimum_offset, int)
        or isinstance(minimum_offset, bool)
        or minimum_offset < 0
    ):
        raise ValueError("minimum_offset must be a non-negative integer")
    stream.seek(0, os.SEEK_END)
    end = stream.tell()
    if minimum_offset >= end:
        return

    scan_start = minimum_offset
    if minimum_offset > 0:
        stream.seek(minimum_offset - 1)
        if stream.read(1) != b"\n":
            stream.seek(minimum_offset)
            while True:
                chunk = stream.read(chunk_size)
                if not chunk:
                    return
                newline = chunk.find(b"\n")
                if newline >= 0:
                    scan_start = stream.tell() - len(chunk) + newline + 1
                    break

    position = end
    remainder = b""
    while position > scan_start:
        read_size = min(chunk_size, position - scan_start)
        position -= read_size
        stream.seek(position)
        parts = (stream.read(read_size) + remainder).split(b"\n")
        remainder = parts[0]
        yield from reversed(parts[1:])
    if remainder:
        yield remainder


def _rollout_event_timestamp(event: dict) -> float | None:
    """Convert a native rollout event timestamp to Unix seconds."""

    value = event.get("timestamp")
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        timestamp = float(value)
        if not math.isfinite(timestamp):
            return None
        return timestamp / 1000 if timestamp > 10_000_000_000 else timestamp
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def _value(data: dict, camel: str, snake: str):
    """Read a v2 app-server camelCase field or rollout snake_case field."""

    return data.get(camel) if camel in data else data.get(snake)


def _normalize_rate_limits(snapshot: dict | None) -> dict | None:
    """Map app-server or rollout rate-limit fields to CCM's quota shape."""

    if not isinstance(snapshot, dict) or not snapshot:
        return None
    primary = snapshot.get("primary")
    secondary = snapshot.get("secondary")
    primary = primary if isinstance(primary, dict) else {}
    secondary = secondary if isinstance(secondary, dict) else {}
    credits = snapshot.get("credits")
    credits = credits if isinstance(credits, dict) else {}
    plan_type = _value(snapshot, "planType", "plan_type")
    reached_type = _value(
        snapshot, "rateLimitReachedType", "rate_limit_reached_type"
    )
    spend_control_reached = _value(
        snapshot,
        "spendControlReached",
        "spend_control_reached",
    ) is True
    primary_used = _value(primary, "usedPercent", "used_percent")
    secondary_used = _value(secondary, "usedPercent", "used_percent")
    if (
        primary_used is None
        and secondary_used is None
        and reached_type is None
        and not spend_control_reached
    ):
        return None
    return {
        "primary_used_percent": primary_used,
        "primary_window_minutes": _value(
            primary, "windowDurationMins", "window_minutes"
        ),
        "primary_resets_at": _value(primary, "resetsAt", "resets_at"),
        "secondary_used_percent": secondary_used,
        "secondary_window_minutes": _value(
            secondary, "windowDurationMins", "window_minutes"
        ),
        "secondary_resets_at": _value(secondary, "resetsAt", "resets_at"),
        "plan_type": plan_type,
        "is_rate_limited": reached_type is not None or spend_control_reached,
        "has_credits": bool(_value(credits, "hasCredits", "has_credits")),
    }


def _quota_from_app_server_response(response: dict | None) -> dict | None:
    """Extract the Codex bucket from account/rateLimits/read."""

    if not isinstance(response, dict):
        return None
    snapshot = response.get("rateLimits")
    if not isinstance(snapshot, dict) or not snapshot:
        buckets = response.get("rateLimitsByLimitId")
        if isinstance(buckets, dict):
            snapshot = buckets.get("codex")
    return _normalize_rate_limits(snapshot)


def _extract_email_from_jwt(id_token: str) -> str:
    """Extract email from JWT id_token payload (no verification)."""
    if not id_token:
        return ""
    try:
        import base64
        parts = id_token.split(".")
        if len(parts) < 2:
            return ""
        payload = parts[1]
        # Add padding
        payload += "=" * (4 - len(payload) % 4)
        decoded = base64.urlsafe_b64decode(payload)
        data = json.loads(decoded)
        return data.get("email", "")
    except Exception:
        return ""


def verify_login(
    codex_home: str,
    *,
    auth_kind: str = "oauth",
) -> dict:
    """Check whether a CODEX_HOME has the expected local auth projection.

    Managed API accounts use command-backed custom-provider credentials rather
    than OAuth ``auth.json``. Their live key validity is checked asynchronously by
    :meth:`CodexPool.verify_account_live`; this local branch only identifies
    the deliberately configured API projection and never reads OAuth files.
    """

    if _is_api_auth_kind(auth_kind):
        return {
            "logged_in": True,
            "email": "",
            "plan_type": None,
            "subscription_until": None,
            "auth_mode": "api",
            "detail": "configured",
        }
    auth_path = Path(codex_home) / "auth.json"
    if not auth_path.exists():
        return {"logged_in": False, "detail": "auth.json missing"}
    try:
        data = json.loads(auth_path.read_text())
    except Exception:
        return {"logged_in": False, "detail": "auth.json unreadable"}

    tokens = data.get("tokens") or {}
    has_access = bool(tokens.get("access_token") or data.get("OPENAI_API_KEY"))
    email = _extract_email_from_jwt(tokens.get("id_token", ""))

    # Check subscription info from id_token
    plan_type = None
    subscription_until = None
    try:
        import base64
        parts = (tokens.get("id_token") or "").split(".")
        if len(parts) >= 2:
            payload = parts[1] + "=" * (4 - len(parts[1]) % 4)
            claims = json.loads(base64.urlsafe_b64decode(payload))
            auth_info = claims.get("https://api.openai.com/auth", {})
            plan_type = auth_info.get("chatgpt_plan_type")
            subscription_until = auth_info.get("chatgpt_subscription_active_until")
    except Exception:
        pass

    return {
        "logged_in": has_access,
        "email": email,
        "plan_type": plan_type,
        "subscription_until": subscription_until,
        "auth_mode": data.get("auth_mode"),
        "detail": "ok" if has_access else "no access token",
    }
