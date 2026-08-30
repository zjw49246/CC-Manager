from datetime import datetime
import posixpath

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.schemas.task_ssh_grant import SSHCapability


def _unique_capabilities(value: list[SSHCapability]) -> list[SSHCapability]:
    return list(dict.fromkeys(value))


def _validate_task_policy(
    enabled: bool,
    capabilities: list[SSHCapability],
) -> None:
    if enabled and not capabilities:
        raise ValueError("select at least one Task capability when Task access is enabled")
    if not enabled and capabilities:
        raise ValueError("Task capabilities require Task access to be enabled")


def normalize_allowed_roots(value: list[str]) -> list[str]:
    """Validate and collapse absolute POSIX roots."""

    normalized: list[str] = []
    for candidate in value:
        if not isinstance(candidate, str):
            raise ValueError("SSH allowed roots must be strings")
        candidate = candidate.strip()
        if not candidate or "\x00" in candidate or not candidate.startswith("/"):
            raise ValueError("SSH allowed roots must be absolute POSIX paths")
        root = "/" + posixpath.normpath(candidate).lstrip("/")
        if len(root) > 4096:
            raise ValueError("SSH allowed roots must be no longer than 4096 characters")
        if any(
            parent == "/"
            or root == parent
            or root.startswith(parent.rstrip("/") + "/")
            for parent in normalized
        ):
            continue
        normalized = [
            existing
            for existing in normalized
            if not existing.startswith(root.rstrip("/") + "/")
        ]
        normalized.append(root)
        normalized.sort(key=lambda item: (len(item), item))
    if not normalized:
        raise ValueError("select at least one SSH allowed root")
    return normalized


class _SSHProfileConnectionFields(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    host: str = Field(min_length=1, max_length=253)
    port: int = Field(default=22, ge=1, le=65535)
    username: str = Field(min_length=1, max_length=255)

    @field_validator("name", "host", "username")
    @classmethod
    def strip_required_text(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("value must not be blank")
        return normalized


class SSHProfileCreate(_SSHProfileConnectionFields):
    model_config = ConfigDict(extra="forbid")

    key_upload_token: str = Field(min_length=1, max_length=128)
    host_key_value: str = Field(min_length=1, max_length=16384)
    enabled: bool = True
    task_access_enabled: bool = False
    task_capabilities: list[SSHCapability] = Field(default_factory=list, max_length=3)
    allowed_roots: list[str] = Field(default_factory=lambda: ["/"], max_length=32)

    @field_validator("task_capabilities")
    @classmethod
    def unique_task_capabilities(
        cls,
        value: list[SSHCapability],
    ) -> list[SSHCapability]:
        return _unique_capabilities(value)

    @field_validator("allowed_roots")
    @classmethod
    def valid_allowed_roots(cls, value: list[str]) -> list[str]:
        return normalize_allowed_roots(value)

    @model_validator(mode="after")
    def validate_task_policy(self):
        _validate_task_policy(self.task_access_enabled, self.task_capabilities)
        return self


class SSHProfileUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expected_revision: int = Field(ge=1)
    name: str | None = Field(default=None, min_length=1, max_length=100)
    host: str | None = Field(default=None, min_length=1, max_length=253)
    port: int | None = Field(default=None, ge=1, le=65535)
    username: str | None = Field(default=None, min_length=1, max_length=255)
    key_upload_token: str | None = Field(default=None, min_length=1, max_length=128)
    host_key_value: str | None = Field(default=None, min_length=1, max_length=16384)
    enabled: bool | None = None
    task_access_enabled: bool | None = None
    task_capabilities: list[SSHCapability] | None = Field(
        default=None,
        max_length=3,
    )
    allowed_roots: list[str] | None = Field(default=None, max_length=32)

    @field_validator("task_capabilities")
    @classmethod
    def unique_optional_task_capabilities(
        cls,
        value: list[SSHCapability] | None,
    ) -> list[SSHCapability] | None:
        if value is None:
            raise ValueError("task_capabilities must not be null")
        return _unique_capabilities(value)

    @field_validator("allowed_roots")
    @classmethod
    def valid_optional_allowed_roots(
        cls,
        value: list[str] | None,
    ) -> list[str]:
        if value is None:
            raise ValueError("allowed_roots must not be null")
        return normalize_allowed_roots(value)

    @field_validator("task_access_enabled")
    @classmethod
    def task_access_switch_must_not_be_null(
        cls,
        value: bool | None,
    ) -> bool | None:
        if value is None:
            raise ValueError("task_access_enabled must not be null")
        return value

    @field_validator("name", "host", "username")
    @classmethod
    def strip_optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("value must not be blank")
        return normalized

class SSHHostKeyProbeRequest(BaseModel):
    host: str = Field(min_length=1, max_length=253)
    port: int = Field(default=22, ge=1, le=65535)
    timeout_seconds: float = Field(default=10, gt=0, le=30)

    @field_validator("host")
    @classmethod
    def strip_host(cls, value: str) -> str:
        return value.strip()


class SSHHostKeyProbeResponse(BaseModel):
    key_type: str
    host_key_value: str
    fingerprint: str


class SSHProfileResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    host: str
    port: int
    username: str
    key_path_hint: str
    public_key_fingerprint: str
    host_key_type: str
    host_key_fingerprint: str
    revision: int
    enabled: bool
    task_access_enabled: bool
    task_capabilities: list[SSHCapability]
    allowed_roots: list[str]
    created_by: int | None
    last_tested_at: datetime | None
    last_test_ok: bool | None
    last_error_code: str | None
    last_error_detail: str | None
    created_at: datetime
    updated_at: datetime


class SSHProfileTestResponse(BaseModel):
    ok: bool
    error_code: str | None = None
    detail: str | None = None


class SSHPrivateKeyUploadResponse(BaseModel):
    upload_token: str
    filename: str
    public_key_fingerprint: str
