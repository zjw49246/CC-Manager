from __future__ import annotations

import asyncio
import base64
from pathlib import Path

import pytest

from backend.services.browser_review import BrowserReviewOptions, BrowserReviewResult
from backend.services.browser_review_jobs import (
    BrowserReviewBusyError,
    BrowserReviewJobManager,
    _safe_tool_arguments,
)
from backend.services.test_harness_artifacts import TestHarnessArtifactStore as ArtifactStore


def _artifact_store(tmp_path: Path) -> ArtifactStore:
    return ArtifactStore(tmp_path / "artifacts")


@pytest.mark.asyncio
async def test_job_manager_tracks_progress_and_artifacts(monkeypatch, tmp_path):
    async def runner(options, *, progress_callback, **_kwargs):
        assert options.output_dir is not None
        options.output_dir.joinpath("initial.png").write_bytes(b"initial")
        progress_callback(
            {
                "stage": "browser_ready",
                "steps": 0,
                "actions": 0,
                "latest_screenshot": "initial.png",
                "telemetry": {"console": []},
                "action_batch": None,
            }
        )
        progress_callback(
            {
                "stage": "executing_actions",
                "steps": 0,
                "actions": 0,
                "latest_screenshot": "initial.png",
                "telemetry": {"console": []},
                "action_batch": [{"type": "scroll", "scroll_y": 500}],
            }
        )
        screenshot = options.output_dir / "final.png"
        report = options.output_dir / "report.md"
        telemetry = options.output_dir / "telemetry.json"
        screenshot.write_bytes(b"final")
        report.write_text("# Demo report", encoding="utf-8")
        telemetry.write_text("{}", encoding="utf-8")
        return BrowserReviewResult(
            output_dir=options.output_dir,
            report_path=report,
            screenshot_path=screenshot,
            telemetry_path=telemetry,
            response_id="resp_demo",
            steps=1,
            actions=1,
        )

    manager = BrowserReviewJobManager(
        runner=runner,
        artifact_store=_artifact_store(tmp_path),
    )
    job = await manager.start(
        BrowserReviewOptions(
            url="http://127.0.0.1:5173",
            network_policy="managed_preview",
        ),
        capture_only=False,
        api_key="test-key",
    )
    assert job.task is not None
    await asyncio.wait_for(job.task, timeout=1)

    payload = job.as_dict()
    assert payload["status"] == "completed"
    assert payload["response_id"] == "resp_demo"
    assert payload["latest_screenshot"] == "final.png"
    assert payload["report"] == "# Demo report"
    assert payload["action_batches"] == [
        {
            "step": 1,
            "actions": [{"type": "scroll", "scroll_y": 500}],
        }
    ]
    assert payload["artifacts"] == [
        "initial.png",
        "final.png",
        "report.md",
        "telemetry.json",
    ]
    assert await manager.resolve_artifact(job.id, "final.png") is not None
    assert await manager.resolve_artifact(job.id, "../final.png") is None


@pytest.mark.asyncio
async def test_job_manager_allows_only_one_job_and_can_cancel(monkeypatch, tmp_path):
    started = asyncio.Event()
    release = asyncio.Event()

    async def runner(options, **_kwargs):
        started.set()
        await release.wait()
        raise AssertionError("cancelled runner must not complete")

    manager = BrowserReviewJobManager(
        runner=runner,
        artifact_store=_artifact_store(tmp_path),
    )
    options = BrowserReviewOptions(
        url="http://127.0.0.1:5173",
        network_policy="managed_preview",
    )
    job = await manager.start(options, capture_only=True, api_key=None)
    await asyncio.wait_for(started.wait(), timeout=1)

    with pytest.raises(BrowserReviewBusyError):
        await manager.start(options, capture_only=True, api_key=None)

    cancelled = await manager.cancel(job.id)
    assert cancelled is job
    assert job.status == "cancelled"
    assert job.completed_at is not None


@pytest.mark.asyncio
async def test_agent_job_tracks_task_and_browser_events(monkeypatch, tmp_path):
    task_state = {
        "status": "in_progress",
        "error": None,
        "assistant_report": None,
        "trace_events": [
            {
                "id": 11,
                "kind": "tool",
                "title": "滚动查看页面",
                "detail": '{"delta_y": 500}',
                "tool_name": "browser_scroll",
                "timestamp": "2026-08-04T00:00:01Z",
            },
            {
                "id": 10,
                "kind": "decision",
                "title": "模型观察与决策",
                "detail": "首屏已检查，下一步滚动查看错误面板。",
                "timestamp": "2026-08-04T00:00:00Z",
            }
        ],
    }

    async def read_task(_task_id: int):
        return dict(task_state)

    manager = BrowserReviewJobManager(
        task_reader=read_task,
        poll_interval=0.01,
        artifact_store=_artifact_store(tmp_path),
    )
    job = await manager.prepare_agent(
        BrowserReviewOptions(
            url="http://127.0.0.1:5173",
            network_policy="managed_preview",
        ),
        provider="codex",
        codex_service_tier="default",
    )
    await manager.attach_task(job.id, 73)

    png = b"\x89PNG\r\n\x1a\nexample"
    await manager.record_event(
        job.id,
        {
            "stage": "browser_ready",
            "steps": 0,
            "actions": 0,
            "screenshot_base64": base64.b64encode(png).decode(),
            "telemetry": {"console": []},
        },
    )
    await manager.record_event(
        job.id,
        {
            "stage": "agent_reported",
            "steps": 1,
            "actions": 1,
            "screenshot_base64": base64.b64encode(png).decode(),
            "telemetry": {"http_errors": [{"status": 404}]},
            "action_batch": [{"type": "scroll", "scroll_y": 500}],
            "report": "# Agent report",
        },
    )
    task_state["status"] = "completed"
    assert job.task is not None
    await asyncio.wait_for(job.task, timeout=1)

    payload = job.as_dict()
    assert payload["task_id"] == 73
    assert payload["provider"] == "codex"
    assert payload["status"] == "completed"
    assert payload["latest_screenshot"] == "final.png"
    assert payload["report"] == "# Agent report"
    assert [event["id"] for event in payload["trace"]] == [10, 11]
    assert payload["action_batches"] == [
        {
            "step": 1,
            "actions": [{"type": "scroll", "scroll_y": 500}],
        }
    ]
    assert payload["artifacts"] == [
        "initial.png",
        "final.png",
        "actions.jsonl",
        "report.md",
        "telemetry.json",
    ]


@pytest.mark.asyncio
async def test_inline_task_tool_completes_and_allows_a_later_run(
    monkeypatch,
    tmp_path,
):

    task_state = {"status": "in_progress"}

    async def read_task(_task_id: int):
        return {
            "status": task_state["status"],
            "trace_events": [
                {
                    "id": 31,
                    "kind": "decision",
                    "title": "模型观察与决策",
                    "detail": "首屏存在布局溢出，继续检查网络错误。",
                    "timestamp": "9999-01-01T00:00:00Z",
                }
            ],
        }

    manager = BrowserReviewJobManager(
        task_reader=read_task,
        artifact_store=_artifact_store(tmp_path),
    )
    options = BrowserReviewOptions(
        url="http://127.0.0.1:5173",
        network_policy="managed_preview",
    )
    first = await manager.prepare_task_tool(
        options,
        task_id=73,
        provider="claude",
        codex_service_tier="default",
    )
    assert first.inline_tool is True
    assert first.task is not None

    await manager.record_event(
        first.id,
        {
            "stage": "agent_reported",
            "steps": 2,
            "actions": 1,
            "report": "# Inline report",
        },
    )
    assert first.status == "completed"
    assert first.completed_at is not None

    second = await manager.prepare_task_tool(
        options,
        task_id=73,
        provider="claude",
        codex_service_tier="default",
    )
    jobs = await manager.list_for_task(73)
    assert [job.id for job in jobs] == [second.id, first.id]
    assert [event["id"] for event in second.trace_events] == [31]
    task_state["status"] = "failed"
    assert second.task is not None
    await asyncio.wait_for(second.task, timeout=1)
    assert second.status == "failed"
    assert "finish_review" in (second.error or "")
    await manager.shutdown()


@pytest.mark.asyncio
async def test_terminal_job_history_is_bounded_without_deleting_staging(
    tmp_path,
):
    store = _artifact_store(tmp_path)
    manager = BrowserReviewJobManager(
        artifact_store=store,
        history_limit=2,
    )
    options = BrowserReviewOptions(url="https://example.com")

    first = await manager.prepare_agent(
        options,
        provider="codex",
        codex_service_tier="default",
    )
    first_dir = first.options.output_dir
    assert first_dir is not None and first_dir.exists()
    await manager.fail_start(first.id, RuntimeError("done"))

    second = await manager.prepare_agent(
        options,
        provider="codex",
        codex_service_tier="default",
    )
    await manager.fail_start(second.id, RuntimeError("done"))
    third = await manager.prepare_agent(
        options,
        provider="codex",
        codex_service_tier="default",
    )

    assert await manager.get(first.id) is None
    assert first_dir.exists()
    assert [job.id for job in await manager.list()] == [third.id, second.id]
    await manager.fail_start(third.id, RuntimeError("done"))
    await manager.shutdown()
    assert first_dir.exists()


def test_trace_tool_arguments_redact_typed_text_and_hide_report_body():
    typed = _safe_tool_arguments(
        "browser_type_text",
        '{"text":"sensitive test value"}',
    )
    report = _safe_tool_arguments(
        "finish_review",
        '{"report":"private report body"}',
    )

    assert typed == '{"text": "<20 chars redacted>"}'
    assert "sensitive" not in typed
    assert report is None
