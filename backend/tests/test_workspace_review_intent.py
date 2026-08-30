from backend.services.workspace_review_intent import (
    is_workspace_browser_review_intent,
    workspace_browser_review_routing_prompt,
)


def test_routes_named_pr_frontend_acceptance_to_fresh_browser_review():
    message = "审查一下pr99分支的前端内容是否实现"

    assert is_workspace_browser_review_intent(message) is True
    prompt = workspace_browser_review_routing_prompt(message)
    assert prompt is not None
    assert "test_git_target" in prompt
    assert "PR #99" in prompt
    assert "target_kind=pull_request" in prompt
    assert "check_current_changes_review" in prompt
    assert "resolved Git SHA" in prompt
    assert "不得改测当前工作区" in prompt
    assert "不得直接复用旧结论" in prompt


def test_routes_github_pull_url_to_the_exact_pr_target():
    prompt = workspace_browser_review_routing_prompt(
        "请用浏览器测试 https://github.com/acme/ui/pull/123 的前端",
    )

    assert prompt is not None
    assert "PR #123" in prompt
    assert "check_current_changes_review" in prompt
    assert "不得改测当前工作区" in prompt


def test_routes_explicit_browser_review_without_named_branch():
    assert is_workspace_browser_review_intent(
        "拉起浏览器审查一下刚开发的设置页",
    ) is True


def test_static_code_review_opt_out_wins():
    message = "只看代码，静态审查 PR99 的前端实现，不要用浏览器"

    assert is_workspace_browser_review_intent(message) is False
    assert workspace_browser_review_routing_prompt(message) is None


def test_ordinary_frontend_implementation_request_is_not_forced():
    assert is_workspace_browser_review_intent(
        "请实现设置页面并补充单元测试",
    ) is False
