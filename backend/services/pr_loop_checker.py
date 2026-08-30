"""PR Loop Checker — polls GitHub API for PR status and review feedback.

Uses `gh api` CLI (already authenticated on the host) to avoid managing
tokens directly. Designed for the pr_loop task mode where the agent creates
a PR and iterates on review feedback until approved.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

PR_URL_RE = re.compile(
    r"https?://github\.com/([^/]+/[^/]+)/pull/(\d+)"
)


@dataclass
class ReviewComment:
    reviewer: str
    body: str
    path: str | None = None
    line: int | None = None
    state: str = ""  # APPROVED, CHANGES_REQUESTED, COMMENTED
    submitted_at: str = ""
    review_id: int = 0


@dataclass
class PRStatus:
    state: str  # open, closed, merged
    mergeable: bool = False
    ci_state: str = "pending"  # pending, success, failure
    ci_checks: list[dict] = field(default_factory=list)
    reviews_state: str = "pending"  # pending, approved, changes_requested
    latest_reviews: list[ReviewComment] = field(default_factory=list)
    head_sha: str = ""
    base_branch: str = ""
    head_branch: str = ""


class PRLoopChecker:
    """Lightweight GitHub PR poller using `gh api`."""

    async def _run_gh_api(self, endpoint: str) -> dict | list | None:
        cmd = ["gh", "api", endpoint, "--cache", "0s"]
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=30
            )
        except asyncio.TimeoutError:
            logger.warning("gh api timed out for %s", endpoint)
            return None
        except OSError as exc:
            logger.error("gh api exec failed: %s", exc)
            return None

        if proc.returncode != 0:
            err_text = stderr.decode(errors="replace").strip()
            logger.warning("gh api %s failed (rc=%s): %s", endpoint, proc.returncode, err_text)
            return None

        try:
            return json.loads(stdout)
        except json.JSONDecodeError:
            logger.warning("gh api %s returned non-JSON", endpoint)
            return None

    async def check_pr_status(self, repo: str, pr_number: int) -> PRStatus | None:
        pr_data = await self._run_gh_api(f"/repos/{repo}/pulls/{pr_number}")
        if not pr_data or not isinstance(pr_data, dict):
            return None

        state = pr_data.get("state", "open")
        if pr_data.get("merged"):
            state = "merged"

        status = PRStatus(
            state=state,
            mergeable=bool(pr_data.get("mergeable")),
            head_sha=pr_data.get("head", {}).get("sha", ""),
            base_branch=pr_data.get("base", {}).get("ref", ""),
            head_branch=pr_data.get("head", {}).get("ref", ""),
        )

        # CI checks
        if status.head_sha:
            checks = await self._run_gh_api(
                f"/repos/{repo}/commits/{status.head_sha}/check-runs"
            )
            if checks and isinstance(checks, dict):
                check_runs = checks.get("check_runs", [])
                status.ci_checks = [
                    {"name": c.get("name"), "status": c.get("status"), "conclusion": c.get("conclusion")}
                    for c in check_runs
                ]
                conclusions = [c.get("conclusion") for c in check_runs if c.get("status") == "completed"]
                if not check_runs:
                    status.ci_state = "pending"
                elif all(c == "success" or c == "skipped" for c in conclusions) and len(conclusions) == len(check_runs):
                    status.ci_state = "success"
                elif any(c == "failure" or c == "cancelled" for c in conclusions):
                    status.ci_state = "failure"
                else:
                    status.ci_state = "pending"

        # Reviews
        reviews_data = await self._run_gh_api(f"/repos/{repo}/pulls/{pr_number}/reviews")
        if reviews_data and isinstance(reviews_data, list):
            latest_by_user: dict[str, ReviewComment] = {}
            for r in reviews_data:
                user = r.get("user", {}).get("login", "unknown")
                r_state = r.get("state", "")
                if r_state in ("APPROVED", "CHANGES_REQUESTED", "COMMENTED"):
                    rc = ReviewComment(
                        reviewer=user,
                        body=r.get("body", ""),
                        state=r_state,
                        submitted_at=r.get("submitted_at", ""),
                        review_id=r.get("id", 0),
                    )
                    latest_by_user[user] = rc

            status.latest_reviews = list(latest_by_user.values())
            states = [r.state for r in status.latest_reviews]
            if any(s == "CHANGES_REQUESTED" for s in states):
                status.reviews_state = "changes_requested"
            elif any(s == "APPROVED" for s in states):
                status.reviews_state = "approved"
            else:
                status.reviews_state = "pending"

        return status

    async def get_review_feedback(
        self, repo: str, pr_number: int, since_review_id: int | None = None
    ) -> list[ReviewComment]:
        """Get review comments (both review-level and inline) since a given review ID."""
        feedback: list[ReviewComment] = []

        # Review-level comments
        reviews_data = await self._run_gh_api(f"/repos/{repo}/pulls/{pr_number}/reviews")
        if reviews_data and isinstance(reviews_data, list):
            for r in reviews_data:
                rid = r.get("id", 0)
                if since_review_id and rid <= since_review_id:
                    continue
                state = r.get("state", "")
                body = r.get("body", "")
                if state == "CHANGES_REQUESTED" or (state == "COMMENTED" and body):
                    feedback.append(ReviewComment(
                        reviewer=r.get("user", {}).get("login", "unknown"),
                        body=body,
                        state=state,
                        submitted_at=r.get("submitted_at", ""),
                        review_id=rid,
                    ))

        # Inline comments
        comments_data = await self._run_gh_api(f"/repos/{repo}/pulls/{pr_number}/comments")
        if comments_data and isinstance(comments_data, list):
            for c in comments_data:
                cid = c.get("pull_request_review_id", 0)
                if since_review_id and cid and cid <= since_review_id:
                    continue
                feedback.append(ReviewComment(
                    reviewer=c.get("user", {}).get("login", "unknown"),
                    body=c.get("body", ""),
                    path=c.get("path"),
                    line=c.get("line") or c.get("original_line"),
                    review_id=cid or 0,
                ))

        return feedback

    async def detect_pr_from_branch(self, repo: str, branch: str) -> tuple[int, str] | None:
        """Find an open PR for a given head branch. Returns (pr_number, url) or None."""
        owner = repo.split("/")[0] if "/" in repo else ""
        head_ref = f"{owner}:{branch}" if owner else branch
        data = await self._run_gh_api(
            f"/repos/{repo}/pulls?head={head_ref}&state=open"
        )
        if data and isinstance(data, list) and len(data) > 0:
            pr = data[0]
            return pr.get("number"), pr.get("html_url", "")
        return None


def extract_pr_from_logs(log_text: str) -> tuple[str, int, str] | None:
    """Extract repo, PR number, and URL from agent output text."""
    match = PR_URL_RE.search(log_text)
    if match:
        repo = match.group(1)
        number = int(match.group(2))
        url = match.group(0)
        return repo, number, url
    return None
