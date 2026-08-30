"""Tests for TaskResponse datetime serialization — UTC timestamps must include timezone info."""
from datetime import datetime, timezone

import pytest

from backend.config import settings
from backend.schemas.task import (
    TaskCreate,
    TaskResponse,
    TaskUpdate,
    public_task_metadata,
)


def _make_task_response(**overrides) -> TaskResponse:
    defaults = dict(
        id=1, incarnation_id="a" * 32,
        title="t", description="d", status="pending", priority=0,
        project_id=None, target_repo=None, target_branch="main",
        result_branch=None, merge_status="pending", instance_id=None,
        retry_count=0, turn_generation=0, max_retries=2, mode="auto",
        todo_file_path=None,
        loop_progress=None, max_iterations=50, must_complete=False,
        goal_condition=None, goal_evaluator_model=None, goal_max_turns=30,
        goal_turns_used=0, goal_last_reason=None, plan_content=None,
        pr_loop_url=None, pr_loop_number=None, pr_loop_repo=None,
        pr_loop_state=None, pr_loop_max_turns=10, pr_loop_turns_used=0,
        pr_loop_poll_interval=60,
        plan_approved=None, session_id=None, provider="claude", model=None,
        codex_service_tier="default", effort_level=None, thinking_budget=None,
        enable_workflows=False,
        enabled_skills=None, starred=False, archived=False, has_unread=False,
        error_message=None, tags=None, metadata_=None, active_sub_agents=0,
        context_window_usage=None,
        created_at=datetime(2026, 6, 10, 14, 30, 0),
        started_at=None, completed_at=None,
    )
    defaults.update(overrides)
    return TaskResponse(**defaults)


class TestTaskResponseDatetimeSerialization:
    def test_naive_created_at_serialized_with_utc_suffix(self):
        resp = _make_task_response(created_at=datetime(2026, 6, 10, 14, 30, 0))
        data = resp.model_dump(mode="json")
        assert data["created_at"].endswith("+00:00")

    def test_aware_created_at_preserved(self):
        aware_dt = datetime(2026, 6, 10, 14, 30, 0, tzinfo=timezone.utc)
        resp = _make_task_response(created_at=aware_dt)
        data = resp.model_dump(mode="json")
        assert data["created_at"].endswith("+00:00")

    def test_started_at_none_serialized_as_none(self):
        resp = _make_task_response(started_at=None)
        data = resp.model_dump(mode="json")
        assert data["started_at"] is None

    def test_started_at_naive_gets_utc_suffix(self):
        resp = _make_task_response(started_at=datetime(2026, 6, 10, 15, 0, 0))
        data = resp.model_dump(mode="json")
        assert data["started_at"].endswith("+00:00")

    def test_completed_at_naive_gets_utc_suffix(self):
        resp = _make_task_response(completed_at=datetime(2026, 6, 10, 16, 0, 0))
        data = resp.model_dump(mode="json")
        assert data["completed_at"].endswith("+00:00")

    def test_all_three_timestamps_have_utc(self):
        resp = _make_task_response(
            created_at=datetime(2026, 1, 1),
            started_at=datetime(2026, 1, 2),
            completed_at=datetime(2026, 1, 3),
        )
        data = resp.model_dump(mode="json")
        for field in ("created_at", "started_at", "completed_at"):
            assert "+00:00" in data[field], f"{field} missing UTC offset"


def test_explicit_empty_provider_is_rejected_even_when_default_is_codex(
    monkeypatch,
):
    monkeypatch.setattr(settings, "default_provider", "codex")

    with pytest.raises(ValueError, match="provider must be"):
        TaskCreate(
            description="d",
            provider="",
            model="gpt-5.6-sol",
            codex_service_tier="priority",
        )
    with pytest.raises(ValueError, match="provider must be"):
        TaskUpdate(provider="")
    with pytest.raises(ValueError, match="provider must be"):
        TaskUpdate(provider=None)


def test_task_provider_is_canonicalized():
    created = TaskCreate(description="d", provider=" CODEX ")
    updated = TaskUpdate(provider=" Claude ")

    assert created.provider == "codex"
    assert updated.provider == "claude"


def test_public_pr_monitor_task_metadata_exposes_only_result_identity():
    metadata = public_task_metadata({
        "pr_monitor_display": True,
        "pr_monitor_run_id": 17,
        "pr_monitor_review_id": 19,
        "pr_head_sha": "a" * 40,
        "pr_action_nonce": "secret-runtime-nonce",
    })

    assert metadata == {
        "pr_monitor_display": True,
        "pr_monitor_run_id": 17,
        "pr_monitor_review_id": 19,
    }


def test_public_metadata_does_not_expose_pr_monitor_ids_without_display_marker():
    assert public_task_metadata({
        "pr_monitor_run_id": 17,
        "pr_monitor_review_id": 19,
    }) is None


@pytest.mark.parametrize(
    "upload_id",
    [
        "11111111-1111-4111-8111-111111111111",
        "fork-seed-0",
        "fork-seed-19",
    ],
)
def test_public_fork_seed_upload_accepts_only_ccm_generated_ids(upload_id):
    metadata = public_task_metadata({
        "fork_seed_uploads": [{
            "id": upload_id,
            "filename": "evidence.txt",
            "path": "/private/host/path/evidence.txt",
            "url": (
                "/api/uploads/"
                "22222222-2222-4222-8222-222222222222.txt"
            ),
            "is_image": False,
        }],
    })

    assert metadata == {
        "fork_seed_uploads": [{
            "id": upload_id,
            "filename": "evidence.txt",
            "path": (
                "/api/uploads/"
                "22222222-2222-4222-8222-222222222222.txt"
            ),
            "url": (
                "/api/uploads/"
                "22222222-2222-4222-8222-222222222222.txt"
            ),
            "is_image": False,
        }],
    }


@pytest.mark.parametrize(
    "upload_id",
    [
        "old",
        "11111111-1111-1111-8111-111111111111",
        "11111111-1111-4111-7111-111111111111",
        "11111111-1111-4111-8111-111111111111.txt",
        "11111111-1111-4111-8111-11111111111A",
        "fork-seed--1",
        "fork-seed-01",
        "fork-seed-1000000",
        "fork-seed-0<script>",
    ],
)
def test_public_fork_seed_upload_rejects_unmanaged_ids(upload_id):
    metadata = public_task_metadata({
        "fork_seed_uploads": [{
            "id": upload_id,
            "filename": "evidence.txt",
            "path": "/private/host/path/evidence.txt",
            "url": (
                "/api/uploads/"
                "22222222-2222-4222-8222-222222222222.txt"
            ),
            "is_image": False,
        }],
    })

    assert metadata is None
