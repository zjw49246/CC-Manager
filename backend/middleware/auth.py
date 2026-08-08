import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from backend.config import settings

JWT_REFRESH_THRESHOLD_DAYS = 7


class TokenAuthMiddleware(BaseHTTPMiddleware):
    """Bearer token + JWT authentication middleware with role enforcement."""

    PUBLIC_PATHS = {
        "/api/system/health", "/api/auth/login", "/api/auth/register", "/api/auth/send-code",
        "/api/github/webhook",
        "/api/feishu/callback",
        "/api/shared/receive", "/api/shared/revoke",
    }

    # Instance state contains process metadata and cross-task output. Unlike
    # ordinary task resources it has no per-user owner, so reads and writes are
    # both administrator-only.
    ADMIN_ONLY_ALL_METHOD_PREFIXES = (
        "/api/instances",
        "/api/dispatcher",
        "/api/system/update",
    )

    # Other system-level paths only restrict mutations.
    ADMIN_ONLY_PREFIXES = (
        "/api/pool",
        "/api/settings",
        "/api/system/skills/curator",
        "/api/system/skills/distill",
    )

    _admin_user_id: int | None = None
    _admin_resolved: bool = False

    @staticmethod
    def _is_deployment_maintenance_path(path: str) -> bool:
        return (
            path == "/api/system/health"
            or path == "/api/system/update"
            or path.startswith("/api/system/update/")
            or path == "/api/system/restart"
            or path == "/api/auth/me"
            or path == "/api/auth/login"
        )

    @staticmethod
    def _maintenance_identity_response(request: Request) -> JSONResponse:
        role = getattr(request.state, "user_role", "member")
        auth_type = getattr(request.state, "auth_type", "")
        user_id = getattr(request.state, "user_id", None)
        if user_id:
            return JSONResponse(
                {
                    "ok": True,
                    "auth_type": auth_type,
                    "user": {
                        "id": user_id,
                        "email": "",
                        "name": "Maintenance Admin",
                        "role": role,
                        "avatar_url": "",
                        "feishu_open_id": "",
                        "feishu_name": "",
                    },
                    "deployment_maintenance_only": True,
                }
            )
        return JSONResponse(
            {
                "ok": True,
                "auth_type": auth_type,
                "role": role,
                "deployment_maintenance_only": True,
            }
        )

    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        maintenance_only = bool(
            getattr(
                request.app.state,
                "deployment_maintenance_only",
                False,
            )
        )
        maintenance_path = self._is_deployment_maintenance_path(path)
        if (
            maintenance_only
            and path.startswith("/api")
            and not maintenance_path
        ):
            return JSONResponse(
                status_code=503,
                content={
                    "detail": (
                        "CCM is in deployment maintenance mode; only health, "
                        "update status, repair, rollback, and restart are "
                        "available"
                    ),
                    "deployment_maintenance_only": True,
                },
            )

        if not settings.auth_token:
            # 无鉴权模式（AUTH_TOKEN 为空）：历史语义是完全开放。RBAC 守卫
            # （require_task_access / require_admin）需要请求身份，若不设置则
            # user_id=None + role=member → 全线 403，无鉴权部署整个不可用。
            # 故此模式下所有请求视为 super_admin（与「无鉴权 = 全开放」一致）。
            request.state.user_id = None
            request.state.user_role = "super_admin"
            request.state.auth_type = "none"
            if maintenance_only and path == "/api/auth/me":
                return self._maintenance_identity_response(request)
            return await call_next(request)

        if path in self.PUBLIC_PATHS or not path.startswith("/api") or path.startswith("/api/uploads/"):
            return await call_next(request)

        if path.startswith("/api/shared-access/"):
            return await call_next(request)

        if path in ("/ws", "/ws/shared"):
            return await call_next(request)

        auth_header = request.headers.get("Authorization", "")
        header_token = (
            auth_header.removeprefix("Bearer ")
            if auth_header.startswith("Bearer ")
            else ""
        )
        token = header_token

        if not token:
            token = request.query_params.get("token", "")

        if not token:
            return JSONResponse(status_code=401, content={"detail": "Unauthorized"})

        from backend.services.internal_service_auth import (
            InternalServiceTokenError,
            authenticate_internal_service_token,
            is_internal_service_token,
        )

        if is_internal_service_token(token):
            # Query parameters are routinely logged by proxies. Scoped child
            # credentials are accepted only through the Bearer header.
            if token != header_token:
                return JSONResponse(
                    status_code=401,
                    content={"detail": "Internal service bearer header required"},
                )
            try:
                claims = authenticate_internal_service_token(
                    token,
                    method=request.method,
                    path=path,
                )
            except InternalServiceTokenError as exc:
                return JSONResponse(
                    status_code=exc.status_code,
                    content={"detail": exc.detail},
                )
            request.state.user_id = None
            request.state.user_role = "internal_service"
            request.state.auth_type = "internal_service"
            request.state.internal_service_claims = claims
        # The deployment token remains a login/recovery credential, but is no
        # longer copied into Task-launched MCP configuration.
        elif token == settings.auth_token:
            if maintenance_only and maintenance_path:
                # Do not touch a potentially incompatible database merely to
                # authorize the narrowly scoped deployment recovery surface.
                request.state.user_id = None
            else:
                if not self._admin_resolved or self._admin_user_id is None:
                    await self._resolve_admin_id()
                request.state.user_id = self._admin_user_id
            request.state.user_role = "super_admin"
            request.state.auth_type = "token"
        else:
            # JWT token
            from backend.api.auth import decode_jwt
            payload = decode_jwt(token)
            if payload:
                user_id = payload.get("user_id")
                if (
                    not isinstance(user_id, int)
                    or isinstance(user_id, bool)
                    or user_id <= 0
                ):
                    return JSONResponse(
                        status_code=401,
                        content={"detail": "Unauthorized"},
                    )
                if maintenance_only and maintenance_path:
                    # Normal mode revalidates every JWT against the User row.
                    # A partial schema migration may make that query unsafe, so
                    # maintenance mode accepts only an unexpired signed token
                    # whose snapshot role was already administrative, and only
                    # for the recovery endpoints above.
                    role = payload.get("role")
                    if role not in ("admin", "super_admin"):
                        return JSONResponse(
                            status_code=403,
                            content={"detail": "Admin only"},
                        )
                    request.state.user_id = user_id
                    request.state.user_role = role
                    request.state.auth_type = "jwt-maintenance"
                else:
                    # A JWT role is only a login-time snapshot. Resolve the
                    # current database row on every normal request so account
                    # deletion, disabling, and demotion revoke HTTP access.
                    from backend.database import async_session
                    from backend.models.user import User
                    async with async_session() as db:
                        user = await db.get(User, user_id)
                    if user is None or not user.is_active:
                        return JSONResponse(
                            status_code=401,
                            content={"detail": "Unauthorized"},
                        )
                    request.state.user_id = user.id
                    request.state.user_role = user.role
                    request.state.auth_type = "jwt"
                    # Sliding refresh: if token expires within threshold, flag
                    # for refresh.
                    exp = payload.get("exp")
                    if exp:
                        remaining = (
                            datetime.fromtimestamp(exp, tz=timezone.utc)
                            - datetime.now(timezone.utc)
                        )
                        if (
                            remaining.total_seconds()
                            < JWT_REFRESH_THRESHOLD_DAYS * 86400
                        ):
                            request.state._refresh_jwt = True
            else:
                return JSONResponse(status_code=401, content={"detail": "Unauthorized"})

        # Enforce admin-only access to process-wide Instance/Dispatcher state,
        # and admin-only mutations for the remaining system settings.
        role = getattr(request.state, "user_role", "member")
        if role not in ("admin", "super_admin", "internal_service"):
            if any(
                path.startswith(prefix)
                for prefix in self.ADMIN_ONLY_ALL_METHOD_PREFIXES
            ):
                return JSONResponse(status_code=403, content={"detail": "Admin only"})
            if request.method != "GET":
                for prefix in self.ADMIN_ONLY_PREFIXES:
                    if path.startswith(prefix):
                        return JSONResponse(status_code=403, content={"detail": "Admin only"})

        if maintenance_only and path == "/api/auth/me":
            return self._maintenance_identity_response(request)

        response = await call_next(request)

        if getattr(request.state, "_refresh_jwt", False):
            try:
                from backend.api.auth import create_jwt
                from backend.database import async_session
                from backend.models.user import User
                async with async_session() as db:
                    user = await db.get(User, request.state.user_id)
                    if user and getattr(user, "is_active", True):
                        response.headers["X-Refreshed-Token"] = create_jwt(user)
            except Exception:
                logger.debug("JWT refresh failed for user %s", getattr(request.state, "user_id", "?"))

        return response

    @classmethod
    async def _resolve_admin_id(cls):
        from backend.database import async_session
        from backend.models.user import User
        from sqlalchemy import select
        try:
            async with async_session() as db:
                result = await db.execute(
                    select(User.id)
                    .where(
                        User.role.in_(["admin", "super_admin"]),
                        User.is_active.is_(True),
                    )
                    .order_by(User.id)
                    .limit(1)
                )
                cls._admin_user_id = result.scalar_one_or_none()
        except Exception:
            cls._admin_user_id = None
        cls._admin_resolved = True
