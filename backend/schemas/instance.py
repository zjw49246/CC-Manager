from datetime import datetime

from pydantic import BaseModel, Field, PositiveInt, field_validator


class InstanceCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    config: dict | None = None

    @field_validator("name")
    @classmethod
    def normalize_name(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("name cannot be blank")
        return value


class InstanceStopRequest(BaseModel):
    expected_task_id: PositiveInt
    expected_task_turn_generation: int = Field(ge=0)
    # Required even when null so callers explicitly acknowledge the complete
    # generation they observed. Omission would silently degrade back to a
    # task-id-only ABA check.
    expected_pid: PositiveInt | None
    expected_started_at: datetime | None


class InstanceResponse(BaseModel):
    id: int
    name: str
    pid: int | None
    status: str
    current_task_id: int | None
    current_task_retry_count: int | None = None
    current_task_turn_generation: int | None = None
    current_plan_run_id: int | None
    worktree_path: str | None
    worktree_branch: str | None
    provider: str
    model: str
    effort_level: str | None
    thinking_budget: int | None
    total_tasks_completed: int
    total_cost_usd: float
    config: dict | None
    started_at: datetime | None
    last_heartbeat: datetime | None

    model_config = {"from_attributes": True}
