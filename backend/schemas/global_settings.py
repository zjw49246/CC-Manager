from pydantic import BaseModel, ConfigDict, Field


class GlobalSettingsUpdate(BaseModel):
    git_author_name: str | None = None
    git_author_email: str | None = None
    git_credential_type: str | None = None  # "ssh" | "https" | None
    git_ssh_key_path: str | None = None
    git_https_username: str | None = None
    git_https_token: str | None = None


class GlobalSettingsResponse(BaseModel):
    git_author_name: str | None
    git_author_email: str | None
    git_credential_type: str | None
    git_ssh_key_path: str | None
    git_https_username: str | None
    git_https_token: str | None

    model_config = {"from_attributes": True}


class RuntimeSettingsResponse(BaseModel):
    use_pty_mode: bool
    pty_available: bool
    codex_app_server_enabled: bool
    codex_main_mcp_enabled: bool
    # Versioned capability signal. Exact Task scope is still enforced by the
    # Task/API gates; Worker and shared Codex Tasks remain unsupported.
    codex_monitor_enabled: bool
    auto_sort_on_access: bool
    # Effective value (DB override, else env default)
    context_compact_threshold: float


class RuntimeSettingsUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    use_pty_mode: bool | None = None
    auto_sort_on_access: bool | None = None
    context_compact_threshold: float | None = Field(default=None, ge=0.3, le=0.95)


class CapacitySettingsResponse(BaseModel):
    max_concurrent_instances: int
    configured_override: int | None
    env_default: int
    min_idle_instances: int
    active_instances: int
    live_instances: int
    pending_tasks: int


class CapacitySettingsUpdate(BaseModel):
    # ``None`` deliberately clears the DB override and restores the env default.
    max_concurrent_instances: int | None = Field(default=None, ge=1, le=64)


class UpdateChannelResponse(BaseModel):
    update_channel: str


class UpdateChannelUpdate(BaseModel):
    update_channel: str = Field(pattern="^(stable|main)$")
