from datetime import datetime

from pydantic import BaseModel


class ProjectCreate(BaseModel):
    name: str
    worker_id: int | None = None  # NULL = local
    git_url: str | None = None
    default_branch: str = "main"
    sort_order: int = 0
    tags: list[str] = []
    env_files: list[str] = []
    badge_color: str | None = None
    git_author_name: str | None = None
    git_author_email: str | None = None
    git_credential_type: str | None = None  # "ssh" | "https" | None
    git_ssh_key_path: str | None = None
    git_https_username: str | None = None
    git_https_token: str | None = None


class ProjectUpdate(BaseModel):
    name: str | None = None
    git_url: str | None = None
    has_remote: bool | None = None
    default_branch: str | None = None
    show_in_selector: bool | None = None
    sort_order: int | None = None
    tags: list[str] | None = None
    env_files: list[str] | None = None
    badge_color: str | None = None
    git_author_name: str | None = None
    git_author_email: str | None = None
    git_credential_type: str | None = None
    git_ssh_key_path: str | None = None
    git_https_username: str | None = None
    git_https_token: str | None = None
    preview_config: dict | None = None


class ProjectReorderItem(BaseModel):
    id: int
    sort_order: int


class ProjectResponse(BaseModel):
    id: int
    name: str
    worker_id: int | None = None
    git_url: str | None
    has_remote: bool
    local_path: str | None
    default_branch: str
    status: str
    error_message: str | None
    show_in_selector: bool
    sort_order: int
    tags: list[str]
    env_files: list[str]
    preview_config: dict | None = None
    git_author_name: str | None
    git_author_email: str | None
    git_credential_type: str | None
    git_ssh_key_path: str | None
    git_https_username: str | None
    git_https_token: str | None  # returned as-is; frontend should treat as sensitive
    badge_color: str | None
    created_at: datetime

    model_config = {"from_attributes": True}
