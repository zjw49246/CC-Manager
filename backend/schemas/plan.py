"""Typed configuration for the two-stage independent Plan pipeline."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

PlanEffort = Literal["low", "medium", "high", "xhigh", "max", "ultra"]


class PlanModelRoute(BaseModel):
    """One concrete provider/model route for a Plan Agent step."""

    model_config = ConfigDict(extra="forbid")

    provider: Literal["claude", "codex"]
    model: str = Field(min_length=1, max_length=100)
    effort: PlanEffort | None = None

    @field_validator("provider", mode="before")
    @classmethod
    def normalize_provider(cls, value: object) -> object:
        return value.strip().lower() if isinstance(value, str) else value

    @field_validator("model", mode="before")
    @classmethod
    def normalize_model(cls, value: object) -> object:
        return value.strip() if isinstance(value, str) else value

    @field_validator("effort", mode="before")
    @classmethod
    def normalize_effort(cls, value: object) -> object:
        if value is None:
            return None
        if isinstance(value, str):
            normalized = value.strip().lower()
            return normalized or None
        return value


class PlanStageRoutes(BaseModel):
    """Primary and fallback routes for one pipeline stage."""

    model_config = ConfigDict(extra="forbid")

    primary: PlanModelRoute
    fallback: PlanModelRoute


class PlanReviewerRoutes(PlanStageRoutes):
    enabled: bool = True


class PlanPipelineConfig(BaseModel):
    """Immutable, versioned routing snapshot stored on every Plan Task."""

    model_config = ConfigDict(extra="forbid")

    version: Literal[1] = 1
    planner: PlanStageRoutes
    reviewer: PlanReviewerRoutes
    max_revision_cycles: int = Field(default=3, ge=0, le=10)
    # Independent from reviewer revision cycles: this bounds how many times a
    # single Run may pause for user input.  It is frozen with every Run.
    max_interactions: int = Field(default=3, ge=0, le=5)


def default_plan_pipeline_config() -> PlanPipelineConfig:
    """Resolve deployment defaults to a concrete per-Plan snapshot."""

    from backend.config import settings

    return PlanPipelineConfig(
        planner=PlanStageRoutes(
            primary=PlanModelRoute(
                provider=settings.plan_planner_provider,
                model=settings.plan_planner_model,
                effort=settings.plan_planner_effort,
            ),
            fallback=PlanModelRoute(
                provider=settings.plan_planner_fallback_provider,
                model=settings.plan_planner_fallback_model,
                effort=settings.plan_planner_fallback_effort,
            ),
        ),
        reviewer=PlanReviewerRoutes(
            enabled=settings.plan_reviewer_enabled,
            primary=PlanModelRoute(
                provider=settings.plan_reviewer_provider,
                model=settings.plan_reviewer_model,
                effort=settings.plan_reviewer_effort,
            ),
            fallback=PlanModelRoute(
                provider=settings.plan_reviewer_fallback_provider,
                model=settings.plan_reviewer_fallback_model,
                effort=settings.plan_reviewer_fallback_effort,
            ),
        ),
        max_revision_cycles=settings.plan_max_revision_cycles,
        max_interactions=max(0, min(5, settings.plan_max_interactions)),
    )


def resolve_plan_pipeline_config(
    value: PlanPipelineConfig | dict | None,
    *,
    base_config: PlanPipelineConfig | None = None,
    legacy_provider: str | None = None,
    legacy_model: str | None = None,
    legacy_effort: str | None = None,
) -> PlanPipelineConfig:
    """Return a concrete config while preserving legacy Planner overrides."""

    if value is not None:
        if isinstance(value, PlanPipelineConfig):
            return value
        return PlanPipelineConfig.model_validate(value)

    config = base_config or default_plan_pipeline_config()
    if legacy_provider or legacy_model or legacy_effort:
        payload = config.model_dump(mode="json")
        payload["planner"]["primary"] = {
            "provider": legacy_provider or config.planner.primary.provider,
            "model": legacy_model or config.planner.primary.model,
            "effort": (
                legacy_effort
                if legacy_effort is not None
                else config.planner.primary.effort
            ),
        }
        config = PlanPipelineConfig.model_validate(payload)
    return config
