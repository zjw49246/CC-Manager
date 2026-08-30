from __future__ import annotations

from inspect import signature

import pytest

from backend.api.browser_reviews import (
    BrowserReviewCreate as BrowserReviewCreateModel,
    TaskBrowserReviewStart as TaskBrowserReviewStartModel,
)
from backend.api.test_harness import TestHarnessRunStart as HarnessRunStartModel
from backend.api.workspace_reviews import (
    WorkspaceReviewStart as WorkspaceReviewStartModel,
)
from backend.mcp.ccm_browser_review_server import start_review
from backend.mcp.ccm_workspace_review_server import (
    test_current_changes as mcp_test_current_changes,
    test_git_target as mcp_test_git_target,
)
from backend.services.test_harness_contracts import (
    DEFAULT_BROWSER_CHANNEL,
    TestHarnessSpec as HarnessSpec,
)
from backend.services.workspace_review import WorkspaceReviewManager


@pytest.mark.parametrize(
    "model",
    [
        HarnessRunStartModel,
        WorkspaceReviewStartModel,
        BrowserReviewCreateModel,
        TaskBrowserReviewStartModel,
    ],
)
def test_browser_review_request_models_default_to_playwright_chromium(model):
    assert DEFAULT_BROWSER_CHANNEL == "chromium"
    assert (
        model.model_fields["browser_channel"].default
        == DEFAULT_BROWSER_CHANNEL
    )


@pytest.mark.parametrize(
    "entrypoint",
    [
        WorkspaceReviewManager.start,
        mcp_test_current_changes,
        mcp_test_git_target,
        start_review,
    ],
)
def test_browser_review_callable_entrypoints_default_to_playwright_chromium(
    entrypoint,
):
    assert (
        signature(entrypoint).parameters["browser_channel"].default
        == DEFAULT_BROWSER_CHANNEL
    )


def test_explicit_system_chrome_remains_supported():
    assert (
        HarnessSpec(
            target_kind="fixed_url",
            target={"url": "https://example.com"},
            goal="Verify explicit Chrome compatibility",
            browser_channel="chrome",
        ).normalized().browser_channel
        == "chrome"
    )
