from pydantic import BaseModel, Field


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
    auto_sort_on_access: bool
    # Effective value (DB override, else env default)
    context_compact_threshold: float


class RuntimeSettingsUpdate(BaseModel):
    use_pty_mode: bool | None = None
    auto_sort_on_access: bool | None = None
    context_compact_threshold: float | None = Field(default=None, ge=0.3, le=0.95)
