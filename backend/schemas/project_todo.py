from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from backend.config import settings

ProjectTodoStatus = Literal["open", "done", "archived"]


class ProjectTodoCreate(BaseModel):
    title: str = Field(min_length=1, max_length=200)
    prompt: str = Field(min_length=1)


class ProjectTodoUpdate(BaseModel):
    """User-editable Todo fields.

    ``created_task_id`` and the matching request hash are server-owned
    provenance.  Accepting either through PATCH would let a client erase the
    idempotency anchor and create a second executable Task from one Todo.
    """

    model_config = ConfigDict(extra="forbid")

    title: str | None = Field(default=None, min_length=1, max_length=200)
    prompt: str | None = Field(default=None, min_length=1)
    status: ProjectTodoStatus | None = None
    sort_order: int | None = None


class ProjectTodoTaskCreate(BaseModel):
    """Narrow, atomic Todo -> ordinary Task admission contract."""

    title: str = Field(min_length=1, max_length=200)
    prompt: str = Field(min_length=1, max_length=200_000)
    provider: Literal["claude", "codex"] = Field(
        default_factory=lambda: settings.default_provider,
    )
    model: str | None = Field(default=None, max_length=100)
    codex_service_tier: Literal["default", "priority"] = "default"
    effort_level: str | None = Field(default=None, max_length=20)
    timeout_hours: float | None = Field(default=None, ge=0, le=168)

    model_config = ConfigDict(extra="forbid")

    @field_validator("title", "prompt", "model", "effort_level")
    @classmethod
    def strip_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("field must not be blank")
        return normalized


class ProjectTodoResponse(BaseModel):
    id: int
    project_id: int
    title: str
    prompt: str
    status: ProjectTodoStatus
    sort_order: int
    created_task_id: int | None = None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}
