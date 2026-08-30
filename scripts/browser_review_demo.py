"""Run an isolated real-browser frontend review with OpenAI Computer Use.

Example:

    OPENAI_API_KEY=... uv run python scripts/browser_review_demo.py \
      http://127.0.0.1:5173 --allow-actions

Use --capture-only to verify the browser harness without making an API call.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from backend.services.browser_review import (  # noqa: E402
    BrowserReviewError,
    BrowserReviewOptions,
    DEFAULT_REVIEW_GOAL,
    run_browser_review,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url", help="http(s) URL to review")
    parser.add_argument("--goal", default=DEFAULT_REVIEW_GOAL)
    parser.add_argument("--model", default="gpt-5.6-terra")
    parser.add_argument(
        "--reasoning-effort",
        choices=("none", "low", "medium", "high", "xhigh", "max"),
        default="medium",
    )
    parser.add_argument(
        "--allow-actions",
        action="store_true",
        help="allow clicks, typing, keypresses, and drags on the isolated target",
    )
    parser.add_argument("--headed", action="store_true", help="show the browser window")
    parser.add_argument(
        "--browser-channel",
        help="Playwright browser channel, for example 'chrome' for system Chrome",
    )
    parser.add_argument("--max-steps", type=int, default=20)
    parser.add_argument("--max-actions", type=int, default=60)
    parser.add_argument("--viewport-width", type=int, default=1440)
    parser.add_argument("--viewport-height", type=int, default=900)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument(
        "--capture-only",
        action="store_true",
        help="capture screenshot/telemetry without calling OpenAI",
    )
    parser.add_argument(
        "--managed-preview",
        action="store_true",
        help="allow an explicitly trusted loopback Preview URL",
    )
    return parser


async def _run(args: argparse.Namespace) -> int:
    options = BrowserReviewOptions(
        url=args.url,
        goal=args.goal,
        model=args.model,
        reasoning_effort=args.reasoning_effort,
        headless=not args.headed,
        allow_actions=args.allow_actions,
        browser_channel=args.browser_channel,
        max_steps=args.max_steps,
        max_actions=args.max_actions,
        viewport_width=args.viewport_width,
        viewport_height=args.viewport_height,
        output_dir=args.output_dir,
        network_policy=("managed_preview" if args.managed_preview else "external_public"),
    )
    try:
        result = await run_browser_review(
            options,
            api_key=os.getenv("OPENAI_API_KEY"),
            capture_only=args.capture_only,
        )
    except (BrowserReviewError, ValueError) as exc:
        print(f"browser review failed: {exc}", file=sys.stderr)
        return 1

    print(f"report: {result.report_path}")
    print(f"screenshot: {result.screenshot_path}")
    print(f"telemetry: {result.telemetry_path}")
    if result.response_id:
        print(f"response: {result.response_id}")
    return 0


def main() -> int:
    return asyncio.run(_run(build_parser().parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
