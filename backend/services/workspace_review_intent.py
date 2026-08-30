"""Deterministic chat routing for current-workspace browser reviews."""

from __future__ import annotations

import re


_BROWSER_OPT_OUT = (
    "不要用浏览器",
    "不用浏览器",
    "不要调用浏览器",
    "无需浏览器",
    "只看代码",
    "只做代码审查",
    "静态审查",
    "without browser",
    "no browser",
    "code review only",
)

_EXPLICIT_BROWSER_REVIEW = (
    "浏览器审查",
    "浏览器测试",
    "拉起浏览器",
    "打开浏览器",
    "黑盒审查",
    "黑盒测试",
    "运行验收",
    "browser review",
    "browser test",
    "browser testing",
    "black-box test",
    "black box test",
    "computer use",
)

_FRONTEND_SUBJECT = (
    "前端",
    "网页",
    "页面",
    "界面",
    "交互",
    "frontend",
    "front-end",
    "web page",
)

_REVIEW_INTENT = (
    "审查",
    "测试",
    "检查",
    "验证",
    "验收",
    "看看效果",
    "是否实现",
    "是否生效",
    "review",
    "test",
    "verify",
    "validate",
    "acceptance",
)

_WORKSPACE_TARGET = (
    "分支",
    "当前改动",
    "当前修改",
    "当前实现",
    "这次改动",
    "刚开发",
    "工作区",
    "worktree",
    "branch",
    "pull request",
)

_STRONG_ACCEPTANCE = (
    "是否实现",
    "是否生效",
    "验收",
    "看看效果",
    "功能实现",
    "works in the browser",
)

_PR_REFERENCE_RE = re.compile(
    r"(?:(?<![A-Za-z0-9_])pr\s*#?\s*(?P<label>\d+)(?![A-Za-z0-9_])|"
    r"/pull/(?P<url>\d+)(?:\b|/))",
    re.IGNORECASE,
)
_UI_WORD_RE = re.compile(r"\bui\b", re.IGNORECASE)


def is_workspace_browser_review_intent(message: str) -> bool:
    """Return whether a human follow-up asks for fresh runtime UI evidence."""

    normalized = " ".join(message.strip().lower().split())
    if not normalized or any(marker in normalized for marker in _BROWSER_OPT_OUT):
        return False
    if any(marker in normalized for marker in _EXPLICIT_BROWSER_REVIEW):
        return True

    frontend_subject = (
        any(marker in normalized for marker in _FRONTEND_SUBJECT)
        or _UI_WORD_RE.search(normalized) is not None
    )
    review_intent = any(marker in normalized for marker in _REVIEW_INTENT)
    workspace_target = (
        any(marker in normalized for marker in _WORKSPACE_TARGET)
        or _PR_REFERENCE_RE.search(normalized) is not None
    )
    strong_acceptance = any(
        marker in normalized for marker in _STRONG_ACCEPTANCE
    )
    return frontend_subject and review_intent and (
        workspace_target or strong_acceptance
    )


def workspace_browser_review_routing_prompt(message: str) -> str | None:
    """Build the hidden per-turn protocol for a detected review request."""

    if not is_workspace_browser_review_intent(message):
        return None
    pr_match = _PR_REFERENCE_RE.search(message)
    pr_number = None
    if pr_match is not None:
        pr_number = int(pr_match.group("label") or pr_match.group("url"))
    if pr_number is not None:
        target_instruction = (
            f"用户点名 PR #{pr_number}。调用 `ccm_workspace_review.test_git_target`，"
            f"使用 target_kind=pull_request、pr_number={pr_number} 创建精确 SHA 的"
            "隔离黑盒验收；不得改测当前工作区，也不得以代码分析冒充该 PR 的浏览器验收。"
        )
        completion_instruction = (
            "成功创建 Test Harness run 后，用 `check_current_changes_review` 轮询到终态。"
        )
        report_instruction = (
            "报告必须记录 Harness run、resolved Git SHA、覆盖范围、Sandbox cleanup 与限制。\n"
        )
    else:
        target_instruction = (
            "当前分支/未提交修改调用 `ccm_workspace_review.test_current_changes`；"
            "若用户明确点名另一个 Git ref，则调用 `ccm_workspace_review.test_git_target` "
            "创建 exact-SHA Sandbox run；不得静默测试不同目标。"
        )
        completion_instruction = (
            "成功创建当前工作区 Test Harness run 后，用 "
            "`check_current_changes_review` 轮询到终态。"
        )
        report_instruction = (
            "报告必须记录 Harness 返回的 resolved Git SHA、覆盖范围和限制。\n"
        )
    return (
        "<ccm_workspace_browser_review_request>\n"
        "本轮用户明确要求对本地 PR、分支或当前前端改动做运行验收。"
        "在给出实现/通过结论前，你必须调用 "
        "`ccm_workspace_review.workspace_review_capabilities`。"
        f"{target_instruction}{completion_instruction}\n"
        "这是一次新的验收请求：之前的浏览器报告、代码阅读、构建或单元测试"
        "都不能替代本次 run。除非用户明确要求解读旧报告，否则不得直接复用旧结论。\n"
        f"{report_instruction}"
        "若 Preview 配置尚未由用户确认，明确要求用户通过一次性审查入口确认；"
        "不要退化成代码分析后声称已经完成浏览器审查。\n"
        "浏览器审查实际完成前不要声称功能已实现、已通过或已验收。\n"
        "</ccm_workspace_browser_review_request>"
    )
