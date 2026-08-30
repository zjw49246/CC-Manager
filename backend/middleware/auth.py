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
    }

    # A Worker is an execution node, not a second CCM website.  Its deployment
    # credential may reach only the protocol surface used by the authoritative
    # Manager.  Task-scoped ``internal_service`` tokens remain independently
    # constrained by their signed method/path claims below.
    WORKER_CONTROL_PLANE_PREFIXES = (
        "/api/system/update",
    )
    WORKER_CONTROL_PLANE_EXACT = {
        ("GET", "/api/system/health"),
        ("GET", "/api/system/stats"),
        ("GET", "/api/system/config"),
        ("POST", "/api/system/worker-drain/begin"),
        ("POST", "/api/system/worker-drain/seal"),
        ("POST", "/api/system/worker-drain-proof"),
        ("GET", "/api/settings/runtime"),
        ("PUT", "/api/settings/runtime"),
        ("POST", "/api/system/restart"),
    }
    WORKER_FORBIDDEN_PREFIXES = (
        "/api/auth",
        "/api/github/webhook",
        "/api/feishu",
        "/api/shared",
        "/api/shared-access",
    )

    # These host-wide resources have no member-scoped ownership boundary. Keep
    # their authorization here as well as on the routers so multipart requests
    # are rejected before FastAPI parses and spools their bodies.
    ADMIN_ONLY_ALL_METHOD_PREFIXES = (
        "/api/instances",
        "/api/dispatcher",
        "/api/system/update",
        "/api/files",
        "/api/ssh-profiles",
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
    def _path_matches_prefix(path: str, prefix: str) -> bool:
        return path == prefix or path.startswith(f"{prefix}/")

    @classmethod
    def _worker_control_plane_path_allowed(
        cls,
        method: str,
        path: str,
    ) -> bool:
        method = method.upper()
        if cls._worker_human_task_mutation(method, path):
            return False
        return (
            (method, path) in cls.WORKER_CONTROL_PLANE_EXACT
            or cls._worker_task_protocol_path_allowed(method, path)
            or cls._worker_project_protocol_path_allowed(method, path)
            or cls._worker_plan_protocol_path_allowed(method, path)
            or cls._worker_pool_protocol_path_allowed(method, path)
            or any(
                cls._path_matches_prefix(path, prefix)
                for prefix in cls.WORKER_CONTROL_PLANE_PREFIXES
            )
        )

    @staticmethod
    def _worker_task_protocol_path_allowed(method: str, path: str) -> bool:
        """Match the concrete Manager→Worker Task execution protocol."""

        parts = path.strip("/").split("/")
        if parts[:2] != ["api", "tasks"]:
            return False

        # Task migration uses an operation-bound reservation and is distinct
        # from the ordinary Manager mirror creation endpoint.
        if parts == ["api", "tasks", "migration-import"]:
            return method == "POST"
        if parts in (
            ["api", "tasks", "migration-import", "commit"],
            ["api", "tasks", "migration-import", "rollback"],
        ):
            return method == "POST"
        if parts == ["api", "tasks"]:
            return method == "POST"
        if len(parts) < 3 or not parts[2].isdigit():
            return False
        if len(parts) == 3:
            return method in {"GET", "PUT", "DELETE"}

        suffix = parts[3:]
        if suffix == ["chat"]:
            return method == "POST"
        if suffix == ["chat", "history"]:
            return method == "GET"
        if suffix in (
            ["plan", "staleness"],
            ["plan", "runs"],
            ["legacy-plan-execution-carrier-proof"],
            ["plan-delete-audit"],
        ):
            return method == "GET"
        if suffix == ["artifacts", "download"]:
            return method == "GET"
        if suffix == ["routing-config", "status"]:
            return method == "GET"
        if (
            len(suffix) == 2
            and suffix[0] == "routing-config"
            and suffix[1] in {"stage", "ack", "reconcile"}
        ):
            return method == "POST"
        if suffix == ["internal", "worker-retry"]:
            return method == "POST"
        if (
            len(suffix) == 3
            and suffix[:2] == ["internal", "worker-retry-receipts"]
        ):
            return method == "GET"
        if (
            len(suffix) == 3
            and suffix[:2] == ["internal", "worker-plan-decisions"]
        ):
            return method in {"GET", "PUT"}
        if suffix == ["monitor-sessions"]:
            return method == "POST"
        if (
            len(suffix) == 2
            and suffix[0] == "monitor-sessions"
            and suffix[1].isdigit()
        ):
            return method == "DELETE"
        if suffix == ["sub-agent-sessions"]:
            return method == "POST"
        if (
            len(suffix) == 2
            and suffix[0] == "sub-agent-sessions"
            and suffix[1].isdigit()
        ):
            return method == "DELETE"
        if (
            len(suffix) in {2, 3}
            and suffix[0] == "worker-turn-handoffs"
        ):
            if len(suffix) == 2:
                return method == "GET"
            return suffix[2] == "resume" and method == "POST"
        if (
            len(suffix) in {2, 3}
            and suffix[0] == "termination-receipts"
        ):
            if len(suffix) == 2:
                return method in {"GET", "PUT"}
            return suffix[2] == "ack" and method == "POST"
        return False

    @staticmethod
    def _worker_plain_task_path(method: str, path: str) -> bool:
        """Identify the broad Task row routes that require a second identity."""

        parts = path.strip("/").split("/")
        return bool(
            method.upper() in {"GET", "PUT", "DELETE"}
            and len(parts) == 3
            and parts[:2] == ["api", "tasks"]
            and parts[2].isdigit()
        )

    @staticmethod
    def _valid_worker_task_incarnation_header(value: object) -> bool:
        return bool(
            isinstance(value, str)
            and len(value) == 32
            and all(char in "0123456789abcdef" for char in value)
        )

    @staticmethod
    def _worker_project_protocol_path_allowed(method: str, path: str) -> bool:
        """Allow only remote checkout discovery/materialization."""

        parts = path.strip("/").split("/")
        if parts == ["api", "projects"]:
            return method in {"GET", "POST"}
        return bool(
            len(parts) == 3
            and parts[:2] == ["api", "projects"]
            and parts[2].isdigit()
            and method == "GET"
        )

    @staticmethod
    def _worker_pool_protocol_path_allowed(method: str, path: str) -> bool:
        """Match Manager-operated Worker account bootstrap/status calls."""

        parts = path.strip("/").split("/")
        if parts[:2] == ["api", "pool"]:
            if len(parts) == 3 and parts[2] in {"status", "usage"}:
                return method == "GET"
            if len(parts) == 4 and parts[2] == "accounts":
                return method == "DELETE"
            return bool(
                len(parts) == 5
                and parts[2] == "accounts"
                and parts[4] == "relogin"
                and method == "POST"
            )
        if parts[:2] != ["api", "codex-pool"]:
            return False
        if len(parts) == 3 and parts[2] in {"status", "usage"}:
            return method == "GET"
        if parts == ["api", "codex-pool", "add"]:
            return method == "POST"
        if len(parts) == 4 and parts[2] == "add":
            return method == "GET"
        if len(parts) == 4 and parts[2] == "login-attempts":
            return method in {"GET", "DELETE"}
        if (
            len(parts) == 5
            and parts[2] == "login-attempts"
            and parts[4] == "otp"
        ):
            return method == "POST"
        if len(parts) == 4 and parts[2] == "accounts":
            return method == "DELETE"
        return bool(
            len(parts) == 5
            and parts[2] == "accounts"
            and parts[4] in {"verify", "relogin"}
            and (
                method == "GET"
                or (parts[4] == "relogin" and method == "POST")
            )
        )

    @staticmethod
    def _worker_plan_protocol_path_allowed(method: str, path: str) -> bool:
        """Allow only Manager↔Worker Plan protocol routes.

        Human Plan creation/edit/approve/reject/cancel belongs to the
        authoritative Manager database.  A deployment bearer proves the
        Manager node, but it is not a human ACL and must not gain those routes
        merely because their URL shares a broad ``/api/plans`` prefix.
        """

        parts = path.strip("/").split("/")
        if parts[:2] == ["api", "plans"]:
            if method == "POST" and parts == [
                "api", "plans", "worker-repo-revision"
            ]:
                return True
            if method == "POST" and parts == [
                "api", "plans", "worker-import"
            ]:
                return True
            if method == "POST" and parts == [
                "api", "plans", "worker-materialize-version"
            ]:
                return True
            if (
                len(parts) == 4
                and parts[2] == "worker-application-receipts"
                and method == "GET"
            ):
                return True
            if (
                len(parts) == 5
                and parts[2] == "worker-application-receipts"
                and parts[4] == "resolve"
                and method == "POST"
            ):
                return True
            if len(parts) == 3 and parts[2].isdigit() and method == "GET":
                return True
            if (
                len(parts) == 4
                and parts[2].isdigit()
                and parts[3] == "versions"
                and method == "GET"
            ):
                return True
            return False
        if parts[:2] == ["api", "plan-runs"]:
            if len(parts) == 3 and parts[2].isdigit() and method == "GET":
                return True
            if (
                len(parts) == 4
                and parts[2].isdigit()
                and parts[3] == "worker-import-audit"
                and method == "GET"
            ):
                return True
            if (
                len(parts) == 4
                and parts[2].isdigit()
                and parts[3] == "worker-import-cancel"
                and method == "POST"
            ):
                return True
            if (
                len(parts) == 6
                and parts[2].isdigit()
                and parts[3] == "input-requests"
                and parts[4].isdigit()
                and parts[5] == "answer"
                and method == "POST"
            ):
                return True
            return False
        return bool(
            parts[:2] == ["api", "plan-versions"]
            and len(parts) == 3
            and parts[2].isdigit()
            and method == "GET"
        )

    @staticmethod
    def _worker_human_task_mutation(method: str, path: str) -> bool:
        """Reject Task UI mutations which have internal Worker protocols."""

        parts = path.strip("/").split("/")
        if parts[:2] != ["api", "tasks"]:
            return False
        if method == "GET" and parts in (
            ["api", "tasks"],
            ["api", "tasks", "count"],
        ):
            return True
        if len(parts) < 4 or not parts[2].isdigit() or method != "POST":
            return False
        suffix = parts[3:]
        return suffix in (
            ["star"],
            ["read"],
            ["unread"],
            ["archive"],
            ["retry"],
            ["plans"],
            ["plan", "approve"],
            ["plan", "reject"],
            ["plan", "revise"],
            ["plan", "create-execution-task"],
        )

    @classmethod
    def _worker_path_forbidden(cls, path: str) -> bool:
        return any(
            cls._path_matches_prefix(path, prefix)
            for prefix in cls.WORKER_FORBIDDEN_PREFIXES
        )

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
        worker_node = settings.ccm_node_role == "worker"
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

        if worker_node:
            if self._path_matches_prefix(path, "/api/workers"):
                # Keep the router's stable operational contract: this is a
                # Manager-only producer and is unavailable (not merely an ACL
                # denial) on an execution node.
                return JSONResponse(
                    status_code=503,
                    content={
                        "detail": (
                            "Worker control plane requires CCM_NODE_ROLE="
                            "manager and a non-empty AUTH_TOKEN"
                        )
                    },
                )
            if self._worker_path_forbidden(path):
                return JSONResponse(
                    status_code=403,
                    content={
                        "detail": (
                            "This endpoint is unavailable on a headless CCM "
                            "Worker"
                        )
                    },
                )
            if path == "/api/system/health":
                return await call_next(request)
            if not path.startswith("/api"):
                return JSONResponse(
                    status_code=404,
                    content={"detail": "CCM Worker has no human-facing HTTP UI"},
                )
            # Settings validation rejects this configuration at process start.
            # Keep the request boundary fail-closed as defence in depth for
            # tests or accidental in-process mutation of global settings.
            if not settings.auth_token.strip():
                return JSONResponse(
                    status_code=503,
                    content={
                        "detail": (
                            "CCM Worker requires a configured control-plane "
                            "AUTH_TOKEN"
                        )
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

        if (
            not worker_node
            and (
                path in self.PUBLIC_PATHS
                or not path.startswith("/api")
                or path.startswith("/api/uploads/")
            )
        ):
            return await call_next(request)

        # Legacy cross-CCM ``share_token`` routers are intentionally not
        # mounted.  Do not retain an authentication bypass for them: an
        # accidental future mount must fail closed behind normal CCM auth.
        if path == "/ws":
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
            if (
                claims.task_id is not None
                and claims.task_incarnation_id is None
            ):
                return JSONResponse(
                    status_code=401,
                    content={
                        "detail": (
                            "Internal service credential lacks an exact "
                            "Task incarnation"
                        )
                    },
                )
            if (
                claims.task_id is not None
                and claims.task_incarnation_id is not None
            ):
                # Revocation is only a fast in-process signal. This durable
                # lookup is authoritative on every request and survives a
                # Manager restart or integer Task-id reuse/import.
                from sqlalchemy import select

                from backend.database import async_session
                from backend.models.task import Task

                task_identity_predicates = [
                    Task.id == claims.task_id,
                    Task.incarnation_id == claims.task_incarnation_id,
                ]
                if claims.task_retry_count is not None:
                    task_identity_predicates.extend((
                        Task.retry_count == claims.task_retry_count,
                        Task.turn_generation
                        == claims.task_turn_generation,
                        Task.status == claims.task_status,
                    ))
                async with async_session() as db:
                    bound_task = await db.scalar(
                        select(Task.id)
                        .where(*task_identity_predicates)
                        .limit(1)
                    )
                if bound_task is None:
                    stale_detail = (
                        "Internal service SSH Task generation is stale"
                        if claims.audience == "ccm_ssh"
                        else (
                            "Internal service Task generation is stale"
                            if claims.task_retry_count is not None
                            else "Internal service Task incarnation is stale"
                        )
                    )
                    return JSONResponse(
                        status_code=401,
                        content={
                            "detail": stale_detail
                        },
                    )
            request.state.user_id = None
            request.state.user_role = "internal_service"
            request.state.auth_type = "internal_service"
            request.state.internal_service_claims = claims
        # The deployment token remains a login/recovery credential, but is no
        # longer copied into Task-launched MCP configuration.
        elif token == settings.auth_token:
            if worker_node:
                if not self._worker_control_plane_path_allowed(
                    request.method,
                    path,
                ):
                    return JSONResponse(
                        status_code=403,
                        content={
                            "detail": (
                                "Endpoint is outside the CCM Worker control-plane "
                                "protocol"
                            )
                        },
                    )
                if (
                    self._worker_plain_task_path(request.method, path)
                    and not self._valid_worker_task_incarnation_header(
                        request.headers.get("x-ccm-task-incarnation")
                    )
                ):
                    # Reject before FastAPI parses a PUT body.  The route then
                    # revalidates the syntactically valid value against the
                    # exact Task row in its own read/write transaction.
                    return JSONResponse(
                        status_code=409,
                        content={
                            "detail": (
                                "Worker Task incarnation header is required"
                            )
                        },
                    )
                # Keep the compatibility role expected by the existing remote
                # Task/Project/Plan routers, but do not bind it to a local User
                # row and label the authentication source explicitly.  The
                # path fence above, not this role string, is the authority.
                request.state.user_id = None
                request.state.user_role = "super_admin"
                request.state.auth_type = "worker_control_plane"
            elif maintenance_only and maintenance_path:
                # Do not touch a potentially incompatible database merely to
                # authorize the narrowly scoped deployment recovery surface.
                request.state.user_id = None
            else:
                if not self._admin_resolved or self._admin_user_id is None:
                    await self._resolve_admin_id()
                request.state.user_id = self._admin_user_id
            if not worker_node:
                request.state.user_role = "super_admin"
                request.state.auth_type = "token"
        else:
            if worker_node:
                # JWTs are Manager-side human sessions.  A Worker never owns
                # that user/ACL control plane even if both nodes share a JWT
                # signing secret.
                return JSONResponse(
                    status_code=401,
                    content={
                        "detail": (
                            "CCM Worker accepts only its deployment or "
                            "task-scoped internal credential"
                        )
                    },
                )
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
                self._path_matches_prefix(path, prefix)
                for prefix in self.ADMIN_ONLY_ALL_METHOD_PREFIXES
            ):
                return JSONResponse(status_code=403, content={"detail": "Admin only"})
            if request.method != "GET":
                for prefix in self.ADMIN_ONLY_PREFIXES:
                    if self._path_matches_prefix(path, prefix):
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
