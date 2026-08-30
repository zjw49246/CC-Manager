"""Shared validation for public PR review admission evidence."""

from __future__ import annotations


PR_REVIEW_INPUT_ERROR_CATEGORY = "unsupported_input_size"
PR_REVIEW_INPUT_ERROR_UNITS = frozenset({"characters", "UTF-8 bytes"})
# JavaScript numbers are exact only through this inclusive bound.  PR Monitor
# exposes these values as JSON and stores them in signed BIGINT columns, so the
# narrower cross-client boundary is authoritative everywhere.
PR_REVIEW_INPUT_ERROR_MAX_SAFE_INTEGER = (1 << 53) - 1


def valid_pr_review_input_error_evidence(
    *,
    category: object,
    measured: object,
    limit: object,
    unit: object,
) -> bool:
    """Return whether all four fields form one canonical public receipt."""

    return bool(
        category == PR_REVIEW_INPUT_ERROR_CATEGORY
        and type(measured) is int
        and type(limit) is int
        and 0 < limit < measured <= PR_REVIEW_INPUT_ERROR_MAX_SAFE_INTEGER
        and unit in PR_REVIEW_INPUT_ERROR_UNITS
    )


def pr_review_input_error_detail(
    *,
    measured: int,
    limit: int,
    unit: str,
) -> str:
    """Build the only public message allowed for structured size evidence."""

    if not valid_pr_review_input_error_evidence(
        category=PR_REVIEW_INPUT_ERROR_CATEGORY,
        measured=measured,
        limit=limit,
        unit=unit,
    ):
        raise ValueError("invalid PR review input-size evidence")
    return (
        "unsupported_input_size: PR review input is "
        f"{measured} {unit}; the safe limit is {limit} {unit}. "
        "No executable Reviewer Task was created."
    )
