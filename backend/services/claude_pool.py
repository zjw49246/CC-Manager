"""Claude account pool — automatic rotation on rate limit / auth failure.

Reads account configuration from ``~/.claude-pool/accounts.json`` (compatible
with the agent-ml-research pool format). Each account has its own
``CLAUDE_CONFIG_DIR`` so Claude Code sees independent OAuth credentials.

When a subprocess hits a rate limit or auth failure, the dispatcher calls
:func:`select` to pick the next available account and :func:`migrate_session`
to hardlink the session JSONL so ``--resume`` works transparently.
"""

import json
import logging
import os
import random
import re
import stat
import subprocess
import time
from pathlib import Path

from backend.services.cloudrouter_accounts import (
    is_api_auth_kind as _is_api_auth_kind,
)
from backend.services.cancellation import await_task_completion

logger = logging.getLogger(__name__)
_SAFE_SESSION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")

# ---------------------------------------------------------------------------
# Rate-limit / auth-failure detection (narrow patterns to avoid false positives)
# ---------------------------------------------------------------------------

_RATE_LIMIT_RE = re.compile(
    # "hit your limit" / "hit your session limit" / "hit your weekly limit"...
    r"hit your (?:\w+ )?limit"
    r"|usage limit reached"
    r"|session limit reached"
    # "resets 5pm (America/...)" / "resets 5:50pm (UTC)" — 任意时区、可带分钟
    r"|resets \d{1,2}(?::\d{2})?\s*[ap]m"
    r"|organization has been disabled"
    r"|organization has disabled"
    r"|account has been disabled"
    r"|当前限速",
    re.IGNORECASE,
)

_AUTH_FAIL_RE = re.compile(
    r"not logged in"
    r"|please run /login"
    r"|not authenticated"
    r"|please log in"
    r"|failed to authenticate",
    re.IGNORECASE,
)


def is_rate_limited(text: str) -> bool:
    if not text:
        return False
    return bool(_RATE_LIMIT_RE.search(text))


def is_auth_failure(text: str) -> bool:
    if not text:
        return False
    return bool(_AUTH_FAIL_RE.search(text))


def is_pool_rotatable(text: str) -> bool:
    """Return True if the output warrants trying another pool account."""
    return is_rate_limited(text) or is_auth_failure(text)


def rate_limit_event_is_actionable(
    rate_limit_info: dict | None, *, warn_threshold: float = 0.9
) -> bool:
    """Whether a CLI ``rate_limit_event`` warrants evaluating a pool switch.

    The Claude CLI emits a ``rate_limit_event`` on almost every turn as a
    routine quota-status ping; its ``status`` field is the real signal:

    - ``allowed``          → account fully healthy. **Never** actionable. Cooling
                             it down here benches a perfectly usable account for
                             ``pool_cooldown_seconds`` every turn, so the pool
                             starves and resumes hit "no available accounts"
                             (prod #734/#740 — a 37%-of-7-day *warning* was
                             benching accounts for 5 min).
    - ``allowed_warning``  → approaching a threshold. Actionable for either the
                             5-hour or 7-day window only when utilization is
                             genuinely high (``>= warn_threshold``).
    - anything else (``rejected``/``blocked``/…) → actionable.

    Note the reactive rotation path (on an actual failure with a usage-limit
    banner) is separate and unaffected; this only gates the *proactive* switch.
    """
    if not isinstance(rate_limit_info, dict):
        return False
    status = str(rate_limit_info.get("status") or "").lower()
    if status == "allowed":
        return False
    if status == "allowed_warning":
        if rate_limit_info.get("rateLimitType") not in {"five_hour", "seven_day"}:
            return False
        util = rate_limit_info.get("utilization")
        if util is None:
            util = rate_limit_info.get("surpassedThreshold")
        try:
            return float(util) >= warn_threshold
        except (TypeError, ValueError):
            return False
    # rejected / blocked / unknown non-"allowed" status → be safe, rotate.
    return True


QUOTA_SWITCH_THRESHOLD_PERCENT = 90.0
PROACTIVE_QUOTA_MAX_COOLDOWN_SECONDS = 8 * 24 * 60 * 60
_DEFINITIVE_QUOTA_AUTH_ERRORS = {
    "no_credentials",
    "token_expired",
    "http_401",
    "http_403",
}


def quota_usage_at_or_above(
    usage: dict | None, *, threshold: float = QUOTA_SWITCH_THRESHOLD_PERCENT
) -> bool:
    """Whether either user-visible Claude quota window reached ``threshold``.

    The OAuth usage endpoint reports percentages on a 0..100 scale. Missing
    data is deliberately not treated as exhausted: an account whose quota could
    not be read remains an eligible fallback rather than starving the pool.
    """

    if not isinstance(usage, dict):
        return False
    for name in ("five_hour", "seven_day"):
        window = usage.get(name)
        if not isinstance(window, dict):
            continue
        try:
            if float(window.get("utilization")) >= threshold:
                return True
        except (TypeError, ValueError):
            continue
    return False


def api_quota_at_or_above(
    snapshot: dict | None,
    *,
    threshold: float = QUOTA_SWITCH_THRESHOLD_PERCENT,
) -> bool:
    """Whether a generic API quota/window reached ``threshold``.

    CloudRouter exposes wallet, subscription, and quota-limited shapes through
    one normalized snapshot.  Unknown/missing limits stay eligible; only a
    proven unavailable state or a real used/limit ratio can trigger rotation.
    """

    if not isinstance(snapshot, dict):
        return False
    if bool(snapshot.get("known")) and snapshot.get("available") is False:
        return True
    candidates: list[dict] = []
    quota = snapshot.get("quota")
    if isinstance(quota, dict):
        candidates.append(quota)
    windows = snapshot.get("windows")
    if isinstance(windows, list):
        candidates.extend(
            window for window in windows if isinstance(window, dict)
        )
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
    rate_limit_info: dict | None,
    *,
    now: float | None = None,
    fallback: int = 300,
    maximum: int = PROACTIVE_QUOTA_MAX_COOLDOWN_SECONDS,
) -> int:
    """Convert a Claude ``resetsAt`` event timestamp into a safe cooldown.

    Both seconds and millisecond Unix timestamps are accepted. Expired or
    malformed values fall back to the normal short cooldown; corrupt far-future
    values are capped so one event cannot quarantine an account forever.
    """

    reset_at = rate_limit_info.get("resetsAt") if isinstance(rate_limit_info, dict) else None
    try:
        reset_at = float(reset_at)
        if reset_at > 10_000_000_000:  # milliseconds, not seconds
            reset_at /= 1000
        remaining = int(reset_at - (time.time() if now is None else now))
    except (TypeError, ValueError):
        return max(1, int(fallback))
    if remaining <= 0:
        return max(1, int(fallback))
    return min(remaining, max(1, int(maximum)))


# ---------------------------------------------------------------------------
# Transient server-side 429 / overload (NOT an account usage limit)
# ---------------------------------------------------------------------------
# Anthropic 官方 CLI 对 HTTP 429(rate_limit) / 529(overloaded) 的人类可读文案：
#   "API Error: Server is temporarily limiting requests (not your usage
#    limit) · Rate limited"  /  "API overloaded — wait and retry"
# 这是 Anthropic 基础设施侧的临时限流/过载，换账号无济于事——应当退避后用
# 同一账号 --resume 重试。必须与「账号额度用尽 / 认证失败」(那些要轮换号)
# 互斥：故先排除 is_rate_limited / is_auth_failure，让额度横幅永远走轮换、
# 绝不误入同号重试死循环。
_TRANSIENT_OVERLOAD_RE = re.compile(
    r"temporarily limiting requests"
    r"|not your usage limit"
    r"|overloaded_error"
    r"|api overloaded",
    re.IGNORECASE,
)


def is_transient_overload(text: str) -> bool:
    """Server-side transient 429/overload — wait-and-retry the SAME account.

    Distinct from account usage-limit / auth-failure (which rotate accounts);
    those take precedence so a usage-limit banner never triggers a same-account
    retry loop.
    """
    if not text:
        return False
    if is_rate_limited(text) or is_auth_failure(text):
        return False
    return bool(_TRANSIENT_OVERLOAD_RE.search(text))


# ---------------------------------------------------------------------------
# Codex (OpenAI) counterparts — exact texts from codex-rs protocol/src/error.rs
# (rust-v0.144.6, 与本机 CLI 同版实证)。usage-limit / auth 失败由独立
# CodexPool 做账号轮换，并与 transient 同号重试保持互斥；transient 对应
# CLI 自身 is_retryable=true 的错误（外加 ServerOverloaded——CLI 只是想让
# 用户换模型，对 CCM 而言退避重试同样有效）。
# ---------------------------------------------------------------------------

_CODEX_USAGE_LIMIT_RE = re.compile(
    r"hit your usage limit"          # UsageLimitReached
    r"|usage_limit_exceeded"          # structured Codex error code
    r"|usage limit exceeded"          # structured/human error wording
    r"|quota exceeded"               # QuotaExceeded
    r"|out of credits"               # workspace credits depleted
    r"|spend cap"                    # workspace spend cap
    r"|upgrade to plus",             # UsageNotIncluded
    re.IGNORECASE,
)

_CODEX_AUTH_FAIL_RE = re.compile(
    r"token has been invalidated"    # 401 identity_authorization_error（实测）
    r"|refresh token was revoked"    # RefreshTokenFailed（实测）
    r"|log out and sign in again"
    r"|try signing in again"
    r"|401 unauthorized",
    re.IGNORECASE,
)

_CODEX_TRANSIENT_RE = re.compile(
    r"stream disconnected before completion"      # CodexErr::Stream
    r"|request timed out"                         # RequestTimeout
    r"|connection failed:"                        # ConnectionFailed（带冒号防误报）
    r"|error while reading the server response"   # ResponseStreamFailed
    r"|currently experiencing high demand"        # InternalServerError
    r"|selected model is at capacity"             # ServerOverloaded
    r"|too many requests"                         # 429 状态行文案
    r"|unexpected status (?:429|5\d\d)"           # UnexpectedStatus 429/5xx
    r"|exceeded retry limit, last status: (?:429|5\d\d)",  # RetryLimit 429/5xx
    re.IGNORECASE,
)


def is_codex_usage_limited(text: str) -> bool:
    if not text:
        return False
    return bool(_CODEX_USAGE_LIMIT_RE.search(text))


def is_codex_auth_failure(text: str) -> bool:
    if not text:
        return False
    return bool(_CODEX_AUTH_FAIL_RE.search(text))


def is_codex_transient(text: str) -> bool:
    """Codex-side transient failure — wait-and-retry the SAME account.

    Usage-limit / auth failures take precedence (they are not retryable within
    the backoff budget; codex has no account pool to rotate to, so they fall
    through to the normal fail path with the CLI's message intact).
    """
    if not text:
        return False
    if is_codex_usage_limited(text) or is_codex_auth_failure(text):
        return False
    return bool(_CODEX_TRANSIENT_RE.search(text))


def is_transient_for(provider: str | None, text: str) -> bool:
    """Provider-aware transient detection (claude → is_transient_overload,
    codex → is_codex_transient). All retry paths should go through this."""
    if (provider or "claude").lower() == "codex":
        return is_codex_transient(text)
    return is_transient_overload(text)


def transient_retry_delay(attempt: int, base: float, cap: float) -> float:
    """Exponential backoff (with jitter) for transient-overload retries.

    ``attempt`` is 1-based: delay = min(base * 2**(attempt-1), cap), then ±20%
    jitter so concurrent tasks don't retry in lockstep against the same
    overloaded backend. Always >= 1s.
    """
    attempt = max(1, attempt)
    raw = base * (2 ** (attempt - 1))
    delay = min(raw, cap)
    jitter = delay * 0.2
    return max(1.0, delay + random.uniform(-jitter, jitter))


# ---------------------------------------------------------------------------
# Account configuration
# ---------------------------------------------------------------------------

DEFAULT_CONFIG_PATH = Path.home() / ".claude-pool" / "accounts.json"
DEFAULT_COOLDOWN_SECONDS = 300  # 5 minutes
USAGE_API_URL = "https://api.anthropic.com/api/oauth/usage"
USAGE_CACHE_TTL = 60  # seconds
OAUTH_TOKEN_URL = "https://platform.claude.com/v1/oauth/token"
OAUTH_CLIENT_ID = "9d1c250a-e61b-44d9-88ed-5944d1962f5e"  # Claude Code 公开 client_id
# Cloudflare 拦默认 python UA（403 error 1010），必须用 CLI 形态的 UA
OAUTH_USER_AGENT = "claude-cli/2.1.0 (external, cli)"


class PoolAccount:
    __slots__ = (
        "id",
        "config_dir",
        "email",
        "role",
        "enabled",
        "retired",
        "cleanup_pending",
        "auth_kind",
        "api_provider",
        "display_name",
        "api_account_id",
        "supported_models",
        "_api_account",
    )

    def __init__(self, data: dict):
        account_id = data.get("id") or data.get("name")
        if not account_id:
            raise ValueError("Pool account requires 'id' or 'name'")
        self.id: str = account_id
        self.config_dir: str = os.path.expandvars(os.path.expanduser(data["config_dir"]))
        self.email: str = data.get("email", "")
        self.role: str = data.get("role", "automation")
        self.enabled: bool = data.get("enabled", True)
        self.retired: bool = bool(data.get("retired", False))
        self.cleanup_pending: bool = bool(data.get("cleanup_pending", False))
        self.auth_kind: str = str(data.get("auth_kind") or "subscription")
        self.api_provider: str | None = data.get("api_provider")
        if self.api_provider is None and _is_api_auth_kind(self.auth_kind):
            self.api_provider = self.auth_kind.removesuffix("_api")
        self.display_name: str = str(
            data.get("display_name") or self.email or self.id
        )
        self.api_account_id: str | None = data.get("api_account_id")
        self.supported_models: list[str] | None = data.get("supported_models")
        self._api_account = data.get("_api_account")

    @classmethod
    def from_cloudrouter(cls, account) -> "PoolAccount":
        auth_kind = str(getattr(account, "auth_kind", "") or "cloudrouter_api")
        retired = bool(getattr(account, "retired", False))
        cleanup_pending = bool(
            getattr(account, "cleanup_pending", False)
        )
        return cls({
            "id": account.id,
            "config_dir": str(account.claude_config_dir),
            "email": "",
            "role": "automation",
            # Keep a disabled projection if a later model refresh removes the
            # Claude family. Historical JSONL sessions in this directory must
            # remain discoverable for safe migration.
            "enabled": (
                bool(getattr(account, "enabled", True))
                and not retired
                and bool((account.models or {}).get("claude"))
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
            "supported_models": list((account.models or {}).get("claude", [])),
            "_api_account": account,
        })

    def supports_model(self, model: str | None) -> bool:
        if not _is_api_auth_kind(self.auth_kind):
            return True
        try:
            return bool(self._api_account.supports_model("claude", model))
        except Exception:
            logger.exception(
                "Could not evaluate API account model support for %s", self.id
            )
            return False


class ClaudePool:
    """In-process account pool with cooldown tracking."""

    def __init__(
        self,
        config_path: str | Path | None = None,
        cooldown_seconds: int = DEFAULT_COOLDOWN_SECONDS,
        *,
        cloudrouter_store=None,
        bootstrap_default: bool = True,
        include_native: bool = True,
    ):
        if config_path:
            expanded = os.path.expandvars(os.path.expanduser(str(config_path)))
            self._config_path = Path(expanded)
        else:
            self._config_path = DEFAULT_CONFIG_PATH
        self._cooldown_seconds = cooldown_seconds
        self._accounts: list[PoolAccount] = []
        # account_id -> timestamp when cooldown expires
        self._cooldowns: dict[str, float] = {}
        # Auth failures require explicit credential repair/manual clear; unlike
        # rate-limit cooldowns they must not drive an infinite routing retry.
        self._terminal_failures: set[str] = set()
        # Manual switch: preferred account is tried first by select(); if it's
        # cooled down / excluded / fails the probe, selection falls back to
        # the normal rotation order (auto rotation stays the safety net).
        self._preferred_account_id: str | None = None
        # Most recently committed route (display only — selection proposals do
        # not update it because migration/binding may still fail).
        self._last_selected_id: str | None = None
        self._last_selected_at: float = 0.0
        self._usage_cache: list[dict] | None = None
        self._usage_cache_at: float = 0.0
        self._api_quota_cache: dict[str, dict] = {}
        self._cloudrouter_store = cloudrouter_store
        self._bootstrap_native = bool(bootstrap_default)
        self._include_native = bool(include_native)
        # account_id -> asyncio.Lock，防并发重复 refresh（refresh token 会轮换）
        self._refresh_locks: dict[str, object] = {}
        self._load()

    @property
    def enabled(self) -> bool:
        return True

    def _load(self):
        if self._include_native and not self._config_path.exists():
            if self._bootstrap_native:
                self._bootstrap_default_account()
            if not self._config_path.exists():
                logger.info(
                    "Native pool config not found at %s; loading API accounts only",
                    self._config_path,
                )
        try:
            data = (
                json.loads(self._config_path.read_text(encoding="utf-8"))
                if self._include_native and self._config_path.exists()
                else {"accounts": []}
            )
            accounts = [PoolAccount(a) for a in data.get("accounts", [])]
            if self._cloudrouter_store is not None:
                known_ids = {account.id for account in accounts}
                known_dirs = {account.config_dir for account in accounts}
                for api_account in self._cloudrouter_store.all_accounts(
                    include_retired=True
                ):
                    projection = PoolAccount.from_cloudrouter(api_account)
                    if (
                        projection.id in known_ids
                        or projection.config_dir in known_dirs
                    ):
                        logger.error(
                            "Skipping duplicate API Claude projection "
                            "%s (%s)",
                            projection.id,
                            projection.config_dir,
                        )
                        continue
                    accounts.append(projection)
                    known_ids.add(projection.id)
                    known_dirs.add(projection.config_dir)
            self._accounts = accounts
            logger.info("Pool loaded %d accounts from %s", len(self._accounts), self._config_path)
        except Exception:
            logger.exception("Failed to load pool config from %s", self._config_path)

    def _bootstrap_default_account(self):
        """accounts.json 不存在时，检测默认账号自动加入号池。"""
        default_cred = Path.home() / ".claude" / ".credentials.json"
        if not default_cred.exists():
            return
        try:
            creds = json.loads(default_cred.read_text(encoding="utf-8"))
            oauth = creds.get("claudeAiOauth", {})
            if not oauth.get("accessToken"):
                return
        except Exception:
            return
        email = "default"
        try:
            import httpx
            r = httpx.get("https://api.claude.ai/api/auth/user",
                          headers={"Authorization": f"Bearer {oauth['accessToken']}"},
                          timeout=10)
            if r.status_code == 200:
                email = r.json().get("email_address", "default")
        except Exception:
            pass
        data = {"accounts": [{
            "id": "account-1",
            "config_dir": str(Path.home() / ".claude"),
            "email": email,
            "role": "automation",
            "enabled": True,
        }]}
        self._config_path.parent.mkdir(parents=True, exist_ok=True)
        self._config_path.write_text(json.dumps(data, indent=2, ensure_ascii=False))
        logger.info("Bootstrapped default account (%s) into pool", email)

    def reload(self):
        self._accounts.clear()
        self._usage_cache = None
        self._usage_cache_at = 0.0
        self._api_quota_cache.clear()
        self._load()
        valid_ids = {
            account.id for account in self._accounts if not account.retired
        }
        self._terminal_failures.intersection_update(valid_ids)
        if self._preferred_account_id not in valid_ids:
            self._preferred_account_id = None
        if self._last_selected_id not in valid_ids:
            self._last_selected_id = None
            self._last_selected_at = 0.0

    def _api_quota_decision(self, account: PoolAccount) -> dict:
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

    def _account_available(self, account: PoolAccount, now: float) -> bool:
        if not account.enabled or account.retired:
            return False
        if now < self._cooldowns.get(account.id, 0):
            return False
        decision = self._api_quota_decision(account)
        if (
            bool(decision.get("known"))
            and decision.get("available") is False
        ):
            return False
        return True

    def list_accounts(self) -> list[dict]:
        now = time.time()
        result = []
        for a in self._accounts:
            pending_api_cleanup = bool(
                a.retired
                and a.cleanup_pending
                and _is_api_auth_kind(a.auth_kind)
            )
            if (a.retired and not pending_api_cleanup) or (
                not pending_api_cleanup
                and
                _is_api_auth_kind(a.auth_kind)
                and not a.supported_models
            ):
                continue
            cd_until = self._cooldowns.get(a.id, 0)
            available = self._account_available(a, now)
            result.append({
                "id": a.id,
                "config_dir": a.config_dir,
                "email": a.email,
                "role": a.role,
                "enabled": a.enabled,
                "retired": a.retired,
                "cleanup_pending": a.cleanup_pending,
                "available": available,
                "cooldown_until": cd_until if cd_until > now else None,
                "cooldown_remaining": max(0, cd_until - now) if cd_until > now else 0,
                "auth_kind": a.auth_kind,
                "api_provider": a.api_provider,
                "display_name": a.display_name,
                "api_account_id": a.api_account_id,
                "supported_models": a.supported_models,
                "api_quota": self._api_quota_cache.get(a.id),
            })
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
            "accounts": accounts,
        }

    @property
    def preferred_account_id(self) -> str | None:
        return self._preferred_account_id

    def set_preferred(self, account_id: str | None) -> bool:
        """Pin an account as the preferred choice for subsequent launches.

        None clears the pin (back to pure auto rotation). Returns False if
        the account id is unknown.
        """
        if account_id is None:
            self._preferred_account_id = None
            logger.info("Pool preferred account cleared (auto rotation)")
            return True
        if not any(
            a.id == account_id and not a.retired for a in self._accounts
        ):
            return False
        self._preferred_account_id = account_id
        logger.info("Pool preferred account set to %s", account_id)
        return True

    def select(
        self,
        *,
        exclude: set[str] | None = None,
        validate: bool = False,
        model: str | None = None,
    ) -> str | None:
        """Pick the best available account config_dir, excluding specified IDs.

        Returns the config_dir path, or None if no account is available.
        """
        now = time.time()
        candidates = []
        for a in self._accounts:
            if exclude and a.id in exclude:
                continue
            if not self._account_available(a, now):
                continue
            if not a.supports_model(model):
                continue
            candidates.append(a)

        if not candidates:
            logger.warning("Pool has no available accounts (exclude=%s)", exclude)
            return None

        # Fresh launches prefer a compatible API account only after cached
        # health has been proven.  At process startup an API account can still
        # be unknown, so keep it behind a usable native account until the first
        # probe settles.  Unknown API accounts remain a final fallback for
        # API-only installations.
        def automatic_rank(account: PoolAccount) -> int:
            if not _is_api_auth_kind(account.auth_kind):
                return 1
            decision = self._api_quota_decision(account)
            if (
                bool(decision.get("known"))
                and decision.get("available") is True
            ):
                return 0
            return 2

        candidates.sort(key=lambda a: (
            automatic_rank(a),
            self._cooldowns.get(a.id, 0),
        ))
        # Manual switch: preferred account jumps the queue; if it fails the
        # probe below the normal order takes over (auto-rotation fallback)
        if self._preferred_account_id:
            preferred = next(
                (a for a in candidates if a.id == self._preferred_account_id), None
            )
            if preferred:
                candidates.remove(preferred)
                candidates.insert(0, preferred)
        for chosen in candidates:
            if validate and not self._probe_account(chosen):
                continue
            logger.info("Pool selected account %s (%s)", chosen.id, chosen.config_dir)
            return chosen.config_dir

        logger.warning("Pool has no healthy accounts after validation (exclude=%s)", exclude)
        return None

    def record_routed_account(self, config_dir: str) -> bool:
        """Record the account chosen as the final route for a Claude launch.

        Selection is only a routing proposal and deliberately does not update
        this marker: resume discovery or a safe migration fallback can
        ultimately launch from another account. Callers record only after the
        final route is committed or an auxiliary process has spawned. Unknown
        directories are left untouched and reported to the caller.
        """

        account_id = self.account_id_from_config_dir(config_dir)
        if account_id is None:
            logger.warning(
                "Cannot record routed Claude account for unknown config_dir: %s",
                config_dir,
            )
            return False
        self._last_selected_id = account_id
        self._last_selected_at = time.time()
        logger.info("Pool recorded routed account %s (%s)", account_id, config_dir)
        return True

    async def select_async(
        self,
        *,
        exclude: set[str] | None = None,
        validate: bool = False,
        model: str | None = None,
    ) -> str | None:
        """Async wrapper for :meth:`select` — runs probe subprocesses in a thread
        so validation doesn't block the event loop (up to 30s per account)."""
        import asyncio
        return await asyncio.to_thread(
            self.select,
            exclude=exclude,
            validate=validate,
            model=model,
        )

    def _probe_account(self, account: PoolAccount) -> bool:
        """Run a small Claude CLI probe before assigning work to an account."""
        if _is_api_auth_kind(account.auth_kind):
            # API credentials are validated by the Store at create/refresh and
            # again under runtime_admission around the real process spawn.
            # A synchronous probe cannot participate in that async retirement
            # fence and would create an untracked key consumer.
            return True
        # Same nested-session cleanup as InstanceManager.launch
        env = {k: v for k, v in os.environ.items() if k.upper() not in ("CLAUDECODE", "CLAUDE_CODE")}
        env["CLAUDE_CONFIG_DIR"] = account.config_dir
        try:
            proc = subprocess.run(
                ["claude", "-p", "reply ok only"],
                env=env,
                text=True,
                capture_output=True,
                timeout=30,
            )
        except subprocess.TimeoutExpired:
            logger.warning("Pool account %s probe timed out", account.id)
            self.mark_rate_limited(account.config_dir, duration=60)
            return False

        combined = "\n".join([proc.stdout or "", proc.stderr or ""]).strip()
        if proc.returncode == 0:
            return True
        if is_auth_failure(combined):
            self.mark_auth_failure(account.config_dir)
            return False
        if is_rate_limited(combined):
            self.mark_rate_limited(account.config_dir)
            return False
        logger.warning(
            "Pool account %s probe failed with non-rotatable output: %s",
            account.id,
            combined[:300],
        )
        return False

    def account_id_from_config_dir(self, config_dir: str) -> str | None:
        for a in self._accounts:
            if a.config_dir == config_dir:
                return a.id
        return None

    def mark_rate_limited(self, config_dir: str, duration: int | None = None):
        """Mark an account as rate-limited with a cooldown period."""
        account_id = self.account_id_from_config_dir(config_dir)
        if not account_id:
            logger.warning("Cannot mark unknown config_dir as rate-limited: %s", config_dir)
            return
        cd = duration or self._cooldown_seconds
        self._terminal_failures.discard(account_id)
        self._cooldowns[account_id] = time.time() + cd
        logger.info("Pool account %s rate-limited for %ds", account_id, cd)

    def mark_auth_failure(self, config_dir: str):
        """Mark an account with auth failure — indefinite cooldown until manual clear."""
        account_id = self.account_id_from_config_dir(config_dir)
        if not account_id:
            return
        # Far future = effectively permanent until cleared
        self._terminal_failures.add(account_id)
        self._cooldowns[account_id] = time.time() + 86400 * 365
        logger.warning("Pool account %s marked auth-failure (indefinite cooldown)", account_id)

    def clear_cooldown(self, account_id: str):
        self._cooldowns.pop(account_id, None)
        self._terminal_failures.discard(account_id)
        logger.info("Pool cooldown cleared for account %s", account_id)

    def is_in_cooldown(self, config_dir: str) -> bool:
        """Cheap (no subprocess) check: is this account currently cooled down?

        Used on the resume hot path to decide whether the session's resident
        account is healthy enough to reuse as-is, instead of spawning a
        ``claude -p`` validation probe. Rate-limit / auth-failure cooldowns are
        already tracked in :attr:`_cooldowns`, so this is enough to avoid
        handing work to a known-bad account. A config_dir that isn't a pool
        account (e.g. a leftover ``~/.claude*`` dir) is treated as not cooled
        down — we can't know better and ``--resume`` should still find it.
        """
        account_id = self.account_id_from_config_dir(config_dir)
        if not account_id:
            return False
        return time.time() < self._cooldowns.get(account_id, 0)

    def is_disabled(self, config_dir: str) -> bool:
        """Whether this config_dir belongs to a pool account that is disabled.

        Used on the resume hot path so a session that physically lives on a
        ``enabled=false`` account is migrated off it rather than reused — that
        is the only way ``enabled=false`` becomes a hard "never call this
        account" guarantee (``select`` already skips disabled accounts for fresh
        launches, but resume anchors to wherever the session JSONL sits). A
        config_dir that isn't a pool account (e.g. a leftover ``~/.claude*``
        dir) is treated as not disabled — we can't know better.
        """
        account = next(
            (a for a in self._accounts if a.config_dir == config_dir), None
        )
        return account is not None and not account.enabled

    def is_config_dir_available(self, config_dir: str) -> bool:
        """Whether a registered account is selectable without a live probe.

        Besides the native enabled/cooldown checks this also honors a cached
        API quota decision.  Resume routing uses this instead of
        separately checking cooldown/disabled so a key with a proven exhausted
        quota cannot keep receiving turns merely because its session is
        resident in that account directory.
        """

        account = next(
            (a for a in self._accounts if a.config_dir == config_dir), None
        )
        return bool(account and self._account_available(account, time.time()))

    def supports_model_for_config_dir(
        self, config_dir: str, model: str | None
    ) -> bool:
        """Whether a known account can execute ``model``.

        Subscription accounts retain their existing unrestricted behavior.
        API projections are restricted to the models reported for the
        key's bound model group.
        """

        account = next(
            (a for a in self._accounts if a.config_dir == config_dir), None
        )
        return True if account is None else account.supports_model(model)

    def has_compatible_enabled_account(self, model: str | None) -> bool:
        """Whether any non-retired account is configured for ``model``.

        Cooldown/quota state is intentionally ignored so callers can
        distinguish a temporary exhausted pool from a permanent model-group
        mismatch.
        """

        return any(
            account.enabled
            and not account.retired
            and account.supports_model(model)
            for account in self._accounts
        )

    def has_retryable_compatible_account(self, model: str | None) -> bool:
        """Whether pool exhaustion can recover without account intervention.

        Native cooldowns and API accounts with unknown/otherwise-available
        quota are retryable. A compatible API key with a known unavailable
        quota/auth state is not: repeatedly requeueing cannot refresh that
        cached fact, so callers surface a visible permanent routing refusal.
        """

        for account in self._accounts:
            if (
                not account.enabled
                or account.retired
                or not account.supports_model(model)
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
        return any(
            account.enabled
            and not account.retired
            and not _is_api_auth_kind(account.auth_kind)
            for account in self._accounts
        )

    def is_cloudrouter_account(self, config_dir: str) -> bool:
        """Backward-compatible API-account predicate used by routing callers."""

        account = next(
            (a for a in self._accounts if a.config_dir == config_dir), None
        )
        return bool(account and _is_api_auth_kind(account.auth_kind))

    def is_known_account(self, config_dir: str) -> bool:
        """Whether this config_dir is a registered pool account.

        A session JSONL might land in a stray ``~/.claude`` dir (e.g. after
        migration from a Worker) that has no credentials on this machine.
        Callers use this to avoid blindly trusting such dirs as healthy.
        """
        return any(a.config_dir == config_dir for a in self._accounts)

    def locate_session_config_dirs(
        self,
        session_id: str,
        extra_dirs: list[str] | None = None,
    ) -> list[str]:
        """Find every config dir that currently holds the session JSONL.

        Searches all pool account dirs plus the env CLAUDE_CONFIG_DIR and the
        default ``~/.claude``, so session migration doesn't depend on callers
        knowing which account a session was created under. Results preserve
        the historical search order and are de-duplicated by path spelling.
        """
        if (
            not isinstance(session_id, str)
            or not _SAFE_SESSION_ID_RE.fullmatch(session_id)
        ):
            return []
        candidates = [a.config_dir for a in self._accounts]
        env_dir = os.environ.get("CLAUDE_CONFIG_DIR")
        if env_dir:
            candidates.append(env_dir)
        candidates.append(str(Path.home() / ".claude"))
        if extra_dirs:
            candidates.extend(extra_dirs)
        # Also scan all ~/.claude* dirs on disk — covers accounts removed
        # from the pool whose config dirs still exist (e.g. expired accounts).
        home = Path.home()
        try:
            for d in sorted(home.iterdir()):
                if d.name.startswith(".claude") and d.is_dir() and str(d) not in candidates:
                    candidates.append(str(d))
        except OSError:
            pass
        seen: set[str] = set()
        matches: list[str] = []
        for d in candidates:
            d = os.path.expanduser(d)
            if d in seen:
                continue
            seen.add(d)
            try:
                projects = Path(d) / "projects"
                projects_stat = projects.lstat()
                if not stat.S_ISDIR(projects_stat.st_mode):
                    continue
                safe_jsonl = any(
                    stat.S_ISREG(candidate.lstat().st_mode)
                    and stat.S_ISDIR(candidate.parent.lstat().st_mode)
                    for candidate in projects.glob(
                        f"*/{session_id}.jsonl"
                    )
                )
                if safe_jsonl:
                    matches.append(d)
            except (FileNotFoundError, OSError):
                continue
        return matches

    def locate_session_config_dir(
        self,
        session_id: str,
        extra_dirs: list[str] | None = None,
        *,
        resident_config_dir: str | None = None,
    ) -> str | None:
        """Return the resident copy when known, otherwise the first match.

        A migrated session can remain hardlinked in several accounts. The
        account that just executed the turn may contain newer sidecar files
        even though an older source copy appears first in pool order, so
        rotation callers pass ``resident_config_dir`` as the authoritative
        migration source.
        """

        matches = self.locate_session_config_dirs(session_id, extra_dirs)
        if resident_config_dir:
            resident = os.path.expanduser(resident_config_dir)
            if resident in matches:
                return resident
        return matches[0] if matches else None

    def authoritative_session_config_dir(
        self,
        session_id: str,
        config_dirs: list[str],
    ) -> str | None:
        """Find the unique/equivalent copy that covers every session sidecar.

        Legacy migrations hardlinked only the main JSONL, so same-inode JSONLs
        do not prove that their sibling tool-result/sub-agent trees are equally
        complete. This returns a copy only when its regular-file tree is a
        provable superset of every other copy; callers otherwise fail closed or
        require an explicit user-selected owner.
        """

        return _authoritative_session_config_dir(session_id, config_dirs)

    def account(self, account_id: str) -> "PoolAccount | None":
        return next((a for a in self._accounts if a.id == account_id), None)

    async def refresh_oauth_token(self, account_id: str) -> bool:
        """手动触发某账号的 OAuth refresh（重新登录按钮的第一步）。

        成功后清掉 usage 缓存，让前端下次拉取立即看到恢复。
        """
        acc = self.account(account_id)
        if acc is None:
            raise KeyError(account_id)
        if _is_api_auth_kind(acc.auth_kind):
            return False
        creds = await self._refresh_oauth(acc, Path(acc.config_dir) / ".credentials.json")
        if creds is None:
            return False
        self._usage_cache = None
        return True

    async def ensure_oauth_access_token(
        self,
        config_dir: str | os.PathLike[str],
        *,
        minimum_remaining_seconds: float = 300.0,
    ) -> bool:
        """Refresh a managed native account before access-token-only projection.

        Auxiliary untrusted workloads receive only ``accessToken`` and cannot
        rotate a refresh token themselves.  Resolve the exact pool account and
        refresh under the same per-account lock used by the pool before such a
        workload snapshots its bounded access token.
        """

        if minimum_remaining_seconds < 0:
            raise ValueError("minimum_remaining_seconds must be non-negative")
        canonical = os.path.realpath(os.path.expanduser(os.fspath(config_dir)))
        account = next(
            (
                candidate
                for candidate in self._accounts
                if os.path.realpath(candidate.config_dir) == canonical
            ),
            None,
        )
        if (
            account is None
            or not account.enabled
            or account.retired
            or _is_api_auth_kind(account.auth_kind)
        ):
            return False
        creds = await self._refresh_oauth(
            account,
            Path(account.config_dir) / ".credentials.json",
            minimum_remaining_seconds=minimum_remaining_seconds,
        )
        return creds is not None

    async def _refresh_oauth(
        self,
        account: "PoolAccount",
        cred_path: Path,
        *,
        minimum_remaining_seconds: float = 60.0,
    ) -> dict | None:
        """accessToken 过期时用 refreshToken 换新（与 Claude CLI 自动刷新行为一致）。

        过期 ≠ 需要重新登录：CLI 平时跑着就会自己刷，闲置账号才会看到过期。
        成功：原子写回 .credentials.json（refresh token 会轮换，必须立即持久化）
        并返回新 creds；失败/无 refreshToken：返回 None——此时才真正需要重新登录。
        """
        import asyncio
        import tempfile

        import httpx

        lock = self._refresh_locks.setdefault(account.id, asyncio.Lock())
        async with lock:  # type: ignore[attr-defined]
            try:
                full = json.loads(cred_path.read_text(encoding="utf-8"))
                creds = full["claudeAiOauth"]
            except (OSError, ValueError, KeyError):
                return None
            # 等锁期间可能已被并发请求（或 CLI 进程自己）刷新过
            if (
                creds.get("expiresAt", 0) / 1000
                > time.time() + minimum_remaining_seconds
            ):
                return creds
            refresh_token = creds.get("refreshToken")
            if not refresh_token:
                return None
            try:
                async with httpx.AsyncClient(timeout=20) as client:
                    resp = await client.post(OAUTH_TOKEN_URL, json={
                        "grant_type": "refresh_token",
                        "refresh_token": refresh_token,
                        "client_id": OAUTH_CLIENT_ID,
                    }, headers={"User-Agent": OAUTH_USER_AGENT})
            except httpx.HTTPError as exc:
                logger.warning("pool %s: token refresh request failed: %s", account.id, exc)
                return None
            if resp.status_code != 200:
                logger.warning("pool %s: token refresh got HTTP %s", account.id, resp.status_code)
                return None
            data = resp.json()
            creds["accessToken"] = data["access_token"]
            if data.get("refresh_token"):
                creds["refreshToken"] = data["refresh_token"]
            creds["expiresAt"] = int((time.time() + data.get("expires_in", 28800)) * 1000)
            try:
                fd, tmp = tempfile.mkstemp(dir=str(cred_path.parent), prefix=".credentials.")
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(full, f)
                os.chmod(tmp, 0o600)
                os.replace(tmp, cred_path)
            except OSError as exc:
                logger.error("pool %s: refreshed token write-back failed: %s", account.id, exc)
            logger.info("pool %s: OAuth token refreshed", account.id)
            return creds

    async def fetch_usage(self, *, force: bool = False) -> list[dict]:
        """Per-account quota utilization from the Anthropic OAuth usage API.

        Reads each account's OAuth access token from
        ``<config_dir>/.credentials.json`` and queries the usage endpoint.
        Results are cached for USAGE_CACHE_TTL seconds.  Pass ``force=True``
        to bypass the cache (e.g. after a manual token refresh).
        """
        import asyncio

        import httpx

        if force:
            self._usage_cache = None

        now = time.time()
        if self._usage_cache is not None and now - self._usage_cache_at < USAGE_CACHE_TTL:
            return self._usage_cache

        async def fetch_one(account: PoolAccount) -> dict:
            base = {"id": account.id, "email": account.email, "enabled": account.enabled,
                    "subscription_type": None, "error": None, "usage": None,
                    "auth_kind": account.auth_kind,
                    "api_provider": account.api_provider,
                    "display_name": account.display_name,
                    "api_account_id": account.api_account_id,
                    "supported_models": account.supported_models,
                    "api_quota": None}
            # Disabled (retired) accounts make zero outbound requests: don't read
            # their credentials, don't refresh their OAuth token, don't hit the
            # usage API. Combined with select() skipping them and resume migrating
            # off them, a disabled account is fully untouched by this process.
            if not account.enabled:
                base["error"] = "disabled"
                return base
            if _is_api_auth_kind(account.auth_kind):
                if self._cloudrouter_store is None or not account.api_account_id:
                    base["error"] = "api_store_unavailable"
                    return base
                try:
                    snapshot = await self._cloudrouter_store.fetch_usage(
                        account.api_account_id,
                        force=force,
                    )
                except Exception as exc:
                    logger.warning(
                        "API usage read failed for %s: %s",
                        account.id,
                        exc,
                    )
                    base["error"] = f"request_failed: {exc}"[:200]
                    return base
                if isinstance(snapshot, dict):
                    base["api_quota"] = snapshot
                    self._api_quota_cache[account.id] = snapshot
                    if (
                        bool(snapshot.get("known"))
                        and snapshot.get("available") is False
                    ):
                        base["error"] = snapshot.get("reason") or "unavailable"
                else:
                    base["error"] = "invalid_api_quota"
                return base
            cred_path = Path(account.config_dir) / ".credentials.json"
            try:
                creds = json.loads(cred_path.read_text(encoding="utf-8"))["claudeAiOauth"]
            except (OSError, ValueError, KeyError):
                base["error"] = "no_credentials"
                return base
            base["subscription_type"] = creds.get("subscriptionType")
            if creds.get("expiresAt", 0) / 1000 < now:
                # 先尝试 refresh——过期不等于要重新登录，刷不动才是
                creds = await self._refresh_oauth(account, cred_path)
                if creds is None:
                    base["error"] = "token_expired"
                    return base
                base["subscription_type"] = creds.get("subscriptionType") or base["subscription_type"]
            try:
                async with httpx.AsyncClient(timeout=15) as client:
                    resp = await client.get(USAGE_API_URL, headers={
                        "Authorization": f"Bearer {creds['accessToken']}",
                        "anthropic-beta": "oauth-2025-04-20",
                    })
            except httpx.HTTPError as exc:
                base["error"] = f"request_failed: {exc}"[:200]
                return base
            if resp.status_code != 200:
                base["error"] = f"http_{resp.status_code}"
                return base
            data = resp.json()

            def window(w: dict | None) -> dict | None:
                if not w:
                    return None
                return {"utilization": w.get("utilization"), "resets_at": w.get("resets_at")}

            base["usage"] = {
                "five_hour": window(data.get("five_hour")),
                "seven_day": window(data.get("seven_day")),
                "seven_day_opus": window(data.get("seven_day_opus")),
                "seven_day_sonnet": window(data.get("seven_day_sonnet")),
            }
            return base

        public_accounts = [
            account
            for account in self._accounts
            if not account.retired
            and not (
                _is_api_auth_kind(account.auth_kind)
                and not account.supported_models
            )
        ]
        results = await asyncio.gather(
            *(fetch_one(account) for account in public_accounts)
        )
        self._usage_cache = list(results)
        self._usage_cache_at = now
        return self._usage_cache

    async def select_quota_alternative(
        self,
        current_config_dir: str,
        *,
        threshold: float = QUOTA_SWITCH_THRESHOLD_PERCENT,
        model: str | None = None,
    ) -> str | None:
        """Pick an available alternative whose known quota is below threshold.

        A missing/failed usage snapshot is allowed as a candidate, per the pool
        fallback contract. The current account and alternatives known to have
        either their 5-hour or 7-day window at/above the threshold are excluded.
        This method never cools the current account; callers do that only after
        a session migration has succeeded.
        """

        current_id = self.account_id_from_config_dir(current_config_dir)
        if not current_id:
            return None
        usage_by_id = {
            row["id"]: row for row in await self.fetch_usage(force=True)
        }
        current_account = self.account(current_id)
        if current_account and _is_api_auth_kind(current_account.auth_kind):
            current_row = usage_by_id.get(current_id)
            current_snapshot = (
                current_row.get("api_quota")
                if isinstance(current_row, dict)
                else None
            )
            if not api_quota_at_or_above(
                current_snapshot,
                threshold=threshold,
            ):
                return None
        excluded = {current_id}
        for account in self._accounts:
            if account.id == current_id:
                continue
            row = usage_by_id.get(account.id)
            if _is_api_auth_kind(account.auth_kind):
                snapshot = (
                    row.get("api_quota") if isinstance(row, dict) else None
                )
                if api_quota_at_or_above(
                    snapshot,
                    threshold=threshold,
                ):
                    excluded.add(account.id)
                continue
            if row and (
                str(row.get("error") or "").lower()
                in _DEFINITIVE_QUOTA_AUTH_ERRORS
                or quota_usage_at_or_above(
                    row.get("usage"), threshold=threshold
                )
            ):
                excluded.add(account.id)
        return self.select(exclude=excluded, validate=False, model=model)


# ---------------------------------------------------------------------------
# Session migration (hardlink JSONL for --resume across accounts)
# ---------------------------------------------------------------------------

class _UnsafeSessionTreeError(RuntimeError):
    """A Claude session tree cannot be copied without following unsafe nodes."""


def _scan_regular_tree(
    root: Path,
) -> tuple[dict[Path, os.stat_result], dict[Path, os.stat_result]] | None:
    """Snapshot a directory tree containing only directories/regular files.

    Relative paths are returned separately for directories and files.  The
    root directory itself is represented by ``Path()`` in ``directories``.
    Symlinks (including links to otherwise-regular files), sockets, devices,
    and FIFOs are rejected rather than followed.
    """

    try:
        root_stat = root.lstat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise _UnsafeSessionTreeError(
            f"could not inspect session sidecar {root}: {exc}"
        ) from exc
    if not stat.S_ISDIR(root_stat.st_mode):
        raise _UnsafeSessionTreeError(
            f"session sidecar is not a real directory: {root}"
        )

    directories: dict[Path, os.stat_result] = {Path(): root_stat}
    files: dict[Path, os.stat_result] = {}
    pending: list[tuple[Path, Path]] = [(root, Path())]
    while pending:
        directory, relative_directory = pending.pop()
        try:
            with os.scandir(directory) as iterator:
                entries = sorted(iterator, key=lambda item: item.name)
        except OSError as exc:
            raise _UnsafeSessionTreeError(
                f"could not read session sidecar directory {directory}: {exc}"
            ) from exc
        for entry in entries:
            relative = relative_directory / entry.name
            try:
                metadata = entry.stat(follow_symlinks=False)
            except OSError as exc:
                raise _UnsafeSessionTreeError(
                    f"could not inspect session sidecar entry {entry.path}: {exc}"
                ) from exc
            if stat.S_ISDIR(metadata.st_mode):
                directories[relative] = metadata
                pending.append((Path(entry.path), relative))
            elif stat.S_ISREG(metadata.st_mode):
                files[relative] = metadata
            else:
                raise _UnsafeSessionTreeError(
                    f"unsafe session sidecar entry: {entry.path}"
                )
    return directories, files


def _same_file_identity(left: os.stat_result, right: os.stat_result) -> bool:
    return left.st_dev == right.st_dev and left.st_ino == right.st_ino


def _same_tree_identity(
    before: tuple[dict[Path, os.stat_result], dict[Path, os.stat_result]] | None,
    after: tuple[dict[Path, os.stat_result], dict[Path, os.stat_result]] | None,
) -> bool:
    """Compare tree membership and inode identity while allowing file growth."""

    if before is None or after is None:
        return before is after
    before_directories, before_files = before
    after_directories, after_files = after
    if (
        before_directories.keys() != after_directories.keys()
        or before_files.keys() != after_files.keys()
    ):
        return False
    return all(
        _same_file_identity(metadata, after_directories[relative])
        for relative, metadata in before_directories.items()
    ) and all(
        _same_file_identity(metadata, after_files[relative])
        for relative, metadata in before_files.items()
    )


def _authoritative_session_config_dir(
    session_id: str,
    config_dirs: list[str],
) -> str | None:
    """Return a provable sidecar superset among same-inode session copies."""

    if (
        not isinstance(session_id, str)
        or not _SAFE_SESSION_ID_RE.fullmatch(session_id)
        or not config_dirs
    ):
        return None

    snapshots: list[
        tuple[
            str,
            set[Path],
            dict[Path, os.stat_result],
        ]
    ] = []
    main_identity: tuple[int, int] | None = None
    try:
        for raw_config_dir in config_dirs:
            config_dir = os.path.expanduser(raw_config_dir)
            root = Path(config_dir)
            projects = root / "projects"
            projects_stat = projects.lstat()
            if not stat.S_ISDIR(projects_stat.st_mode):
                raise _UnsafeSessionTreeError(
                    f"session projects path is not a real directory: {projects}"
                )
            candidates = list(
                projects.glob(f"*/{session_id}.jsonl")
            )
            if len(candidates) != 1:
                return None
            jsonl = candidates[0]
            project_stat = jsonl.parent.lstat()
            jsonl_stat = jsonl.lstat()
            if (
                not stat.S_ISDIR(project_stat.st_mode)
                or not stat.S_ISREG(jsonl_stat.st_mode)
            ):
                raise _UnsafeSessionTreeError(
                    f"unsafe session JSONL path: {jsonl}"
                )
            current_identity = (jsonl_stat.st_dev, jsonl_stat.st_ino)
            if main_identity is None:
                main_identity = current_identity
            elif current_identity != main_identity:
                return None

            tree = _scan_regular_tree(jsonl.parent / session_id)
            if tree is None:
                directories: set[Path] = set()
                files: dict[Path, os.stat_result] = {}
            else:
                tree_directories, files = tree
                directories = set(tree_directories)
            snapshots.append((config_dir, directories, files))
    except (OSError, _UnsafeSessionTreeError) as exc:
        logger.warning(
            "Could not prove authoritative Claude session copy for %s: %s",
            session_id,
            exc,
        )
        return None

    def covers(
        candidate: tuple[str, set[Path], dict[Path, os.stat_result]],
        other: tuple[str, set[Path], dict[Path, os.stat_result]],
    ) -> bool:
        _, candidate_directories, candidate_files = candidate
        _, other_directories, other_files = other
        if not other_directories.issubset(candidate_directories):
            return False
        for relative, other_stat in other_files.items():
            candidate_stat = candidate_files.get(relative)
            if (
                candidate_stat is None
                or not _same_file_identity(candidate_stat, other_stat)
            ):
                return False
        return True

    authoritative = [
        candidate
        for candidate in snapshots
        if all(covers(candidate, other) for other in snapshots)
    ]
    return authoritative[0][0] if authoritative else None


def _rollback_session_migration(
    created_files: list[tuple[Path, os.stat_result]],
    created_directories: list[tuple[Path, os.stat_result]],
) -> None:
    """Remove only filesystem entries created by the current migration."""

    for path, created_stat in reversed(created_files):
        try:
            current = path.lstat()
            if not _same_file_identity(current, created_stat):
                logger.warning(
                    "Refusing to roll back replaced session file %s", path
                )
                continue
            path.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            logger.exception("Could not roll back migrated session file %s", path)
    for path, created_stat in sorted(
        created_directories,
        key=lambda item: len(item[0].parts),
        reverse=True,
    ):
        try:
            current = path.lstat()
            if not _same_file_identity(current, created_stat):
                logger.warning(
                    "Refusing to roll back replaced session directory %s", path
                )
                continue
            path.rmdir()
        except FileNotFoundError:
            pass
        except OSError:
            # Never recurse here: a concurrent writer may now own the
            # directory, and rollback must not delete anything it did not
            # create.
            logger.warning(
                "Could not remove migrated session directory during rollback: %s",
                path,
            )


def migrate_session(
    *,
    old_config_dir: str,
    new_config_dir: str,
    session_id: str,
) -> bool:
    """Hardlink a complete Claude session tree into another config directory.

    Claude stores session history at
    ``<CLAUDE_CONFIG_DIR>/projects/<encoded_cwd>/<session_id>.jsonl``.
    It may also persist large tool results and native sub-agent state below the
    sibling ``<session_id>/`` directory. ``--resume`` only searches the current
    ``CLAUDE_CONFIG_DIR``, so regular files from both locations are hardlinked
    and the sidecar directory structure is recreated.

    Existing targets are accepted only when every overlapping file is already
    the same inode. Symlinks, special files, and type conflicts are rejected.
    Any entry created by a failed call is rolled back; pre-existing entries are
    never removed.
    """
    old_root = Path(old_config_dir)
    new_root = Path(new_config_dir)
    created_files: list[tuple[Path, os.stat_result]] = []
    created_directories: list[tuple[Path, os.stat_result]] = []

    def ensure_directory(directory: Path) -> None:
        try:
            metadata = directory.lstat()
        except FileNotFoundError:
            parent = directory.parent
            if parent == directory:
                raise _UnsafeSessionTreeError(
                    f"could not create session directory {directory}"
                )
            ensure_directory(parent)
            try:
                os.mkdir(directory, mode=0o700)
            except FileExistsError:
                metadata = directory.lstat()
                if not stat.S_ISDIR(metadata.st_mode):
                    raise _UnsafeSessionTreeError(
                        f"session target path is not a directory: {directory}"
                    )
            else:
                metadata = directory.lstat()
                created_directories.append((directory, metadata))
            return
        except OSError as exc:
            raise _UnsafeSessionTreeError(
                f"could not inspect session target directory {directory}: {exc}"
            ) from exc
        if not stat.S_ISDIR(metadata.st_mode):
            raise _UnsafeSessionTreeError(
                f"session target path is not a real directory: {directory}"
            )

    def link_regular_file(
        source: Path,
        target: Path,
        expected_source: os.stat_result,
    ) -> None:
        try:
            source_now = source.lstat()
        except OSError as exc:
            raise _UnsafeSessionTreeError(
                f"could not revalidate session source {source}: {exc}"
            ) from exc
        if (
            not stat.S_ISREG(source_now.st_mode)
            or not _same_file_identity(source_now, expected_source)
        ):
            raise _UnsafeSessionTreeError(
                f"session source changed during migration: {source}"
            )

        try:
            target_stat = target.lstat()
        except FileNotFoundError:
            ensure_directory(target.parent)
            try:
                os.link(source, target, follow_symlinks=False)
            except FileExistsError:
                target_stat = target.lstat()
            else:
                # Record the inode identity immediately. If the following
                # inspection fails, rollback still knows about the link.
                created_files.append((target, expected_source))
                target_stat = target.lstat()
        except OSError as exc:
            raise _UnsafeSessionTreeError(
                f"could not inspect session target {target}: {exc}"
            ) from exc

        if (
            not stat.S_ISREG(target_stat.st_mode)
            or not _same_file_identity(target_stat, expected_source)
        ):
            raise _UnsafeSessionTreeError(
                f"conflicting session target file: {target}"
            )

    try:
        if (
            not isinstance(session_id, str)
            or not _SAFE_SESSION_ID_RE.fullmatch(session_id)
        ):
            logger.warning("migrate_session: invalid session id %r", session_id)
            return False

        candidates = list(old_root.glob(f"projects/*/{session_id}.jsonl"))
        if len(candidates) != 1:
            logger.warning(
                "migrate_session: expected one jsonl for sid=%s under %s, found %d",
                session_id,
                old_root,
                len(candidates),
            )
            return False

        old_jsonl = candidates[0]
        old_jsonl_stat = old_jsonl.lstat()
        if not stat.S_ISREG(old_jsonl_stat.st_mode):
            raise _UnsafeSessionTreeError(
                f"session JSONL is not a regular file: {old_jsonl}"
            )
        old_project_dir_stat = old_jsonl.parent.lstat()
        if not stat.S_ISDIR(old_project_dir_stat.st_mode):
            raise _UnsafeSessionTreeError(
                f"session project path is not a real directory: {old_jsonl.parent}"
            )

        encoded_cwd = old_jsonl.parent.name
        new_jsonl = new_root / "projects" / encoded_cwd / f"{session_id}.jsonl"
        old_sidecar = old_jsonl.parent / session_id
        new_sidecar = new_jsonl.parent / session_id
        source_tree = _scan_regular_tree(old_sidecar)
        target_tree = _scan_regular_tree(new_sidecar)

        try:
            existing_jsonl_stat = new_jsonl.lstat()
        except FileNotFoundError:
            existing_jsonl_stat = None
        if existing_jsonl_stat is not None and (
            not stat.S_ISREG(existing_jsonl_stat.st_mode)
            or not _same_file_identity(existing_jsonl_stat, old_jsonl_stat)
        ):
            raise _UnsafeSessionTreeError(
                f"conflicting session JSONL target: {new_jsonl}"
            )

        # A sidecar without the matching JSONL cannot be proven to belong to
        # this migration. Once the JSONL is already the same inode, additional
        # regular target-only sidecar files are allowed: they may have been
        # created by a later turn executed in the target account.
        if existing_jsonl_stat is None and target_tree is not None:
            raise _UnsafeSessionTreeError(
                f"orphan session sidecar conflicts with migration: {new_sidecar}"
            )

        if source_tree is not None:
            source_directories, source_files = source_tree
            target_directories, target_files = target_tree or ({}, {})
            for relative in source_directories:
                if relative in target_files:
                    raise _UnsafeSessionTreeError(
                        f"session sidecar directory conflicts with file: "
                        f"{new_sidecar / relative}"
                    )
            for relative, source_stat in source_files.items():
                if relative in target_directories:
                    raise _UnsafeSessionTreeError(
                        f"session sidecar file conflicts with directory: "
                        f"{new_sidecar / relative}"
                    )
                target_stat = target_files.get(relative)
                if target_stat is not None and not _same_file_identity(
                    source_stat, target_stat
                ):
                    raise _UnsafeSessionTreeError(
                        f"conflicting session sidecar file: "
                        f"{new_sidecar / relative}"
                    )

            for relative in sorted(
                source_directories,
                key=lambda item: (len(item.parts), str(item)),
            ):
                ensure_directory(new_sidecar / relative)
            for relative in sorted(source_files, key=str):
                link_regular_file(
                    old_sidecar / relative,
                    new_sidecar / relative,
                    source_files[relative],
                )

        # Link the JSONL last: it is the resume-discovery commit point.
        link_regular_file(old_jsonl, new_jsonl, old_jsonl_stat)

        # Refuse a partial snapshot if the source tree changed while files were
        # linked. File growth is safe because hardlinks share the same inode;
        # entry additions/removals/replacements are not.
        source_tree_after = _scan_regular_tree(old_sidecar)
        if not _same_tree_identity(source_tree, source_tree_after):
            raise _UnsafeSessionTreeError(
                f"session sidecar changed during migration: {old_sidecar}"
            )
        old_jsonl_after = old_jsonl.lstat()
        if not _same_file_identity(old_jsonl_after, old_jsonl_stat):
            raise _UnsafeSessionTreeError(
                f"session JSONL changed during migration: {old_jsonl}"
            )

        logger.info(
            "migrate_session: hardlinked complete session %s → %s",
            old_jsonl,
            new_jsonl,
        )
        return True
    except (OSError, _UnsafeSessionTreeError) as exc:
        _rollback_session_migration(created_files, created_directories)
        logger.warning(
            "migrate_session(%s → %s, sid=%s): %s",
            old_config_dir,
            new_config_dir,
            session_id,
            exc,
        )
        return False


async def migrate_session_async(
    *,
    old_config_dir: str,
    new_config_dir: str,
    session_id: str,
) -> bool:
    """Run complete session migration off-loop and settle cancellation."""

    import asyncio

    operation = asyncio.create_task(
        asyncio.to_thread(
            migrate_session,
            old_config_dir=old_config_dir,
            new_config_dir=new_config_dir,
            session_id=session_id,
        )
    )
    cancellation = await await_task_completion(operation)
    try:
        migrated = operation.result()
    except BaseException as exc:
        if cancellation is not None:
            raise cancellation from exc
        raise
    if cancellation is not None:
        raise cancellation
    return migrated


# ---------------------------------------------------------------------------
# Collect rate-limit text from stderr + last N log entries
# ---------------------------------------------------------------------------

def collect_process_output_for_detection(stderr: str, last_log_contents: list[str]) -> str:
    """Combine stderr and recent log entry contents for rate-limit detection."""
    parts = []
    if stderr:
        parts.append(stderr)
    parts.extend(c for c in last_log_contents if c)
    return "\n".join(parts)
