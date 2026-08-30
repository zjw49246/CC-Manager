#!/usr/bin/env python3
"""Deny Claude tools that bypass CCM's Task-scoped SSH broker.

The hook is installed in Claude's account settings, but is active only when
``CCM_TASK_SSH_GUARD=1`` is present in the launched Task environment.  It is
deliberately dependency-free because account homes can be used outside the
project virtualenv.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import sys
from pathlib import Path
from typing import Any


_FILE_TOOLS = {"Read", "Write", "Edit", "MultiEdit", "Glob", "Grep"}
_DIRECT_SSH_COMMAND = re.compile(
    r"(?:^|[;&|()]\s*|\b(?:command|exec|env|nohup|sudo|timeout|xargs)\s+)"
    r"(?:[A-Za-z_][A-Za-z0-9_]*=[^\s]+\s+)*"
    r"(?:/[^\s;|&]+/)?"
    r"(?:ssh|scp|sftp|ssh-add|ssh-keyscan|ssh-copy-id)"
    r"(?=\s|$)",
    re.IGNORECASE,
)
_REMOTE_RSYNC = re.compile(
    r"(?:^|[;&|()]\s*)(?:/[^\s;|&]+/)?rsync\b[^\n]*(?:\s-e\s+ssh|"
    r"--rsh(?:=|\s+)ssh|\S+@[^\s:]+:|ssh://)",
    re.IGNORECASE,
)


def _deny(reason: str) -> None:
    print(json.dumps({
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "deny",
            "permissionDecisionReason": reason,
        }
    }))
    raise SystemExit(0)


def _allow() -> None:
    raise SystemExit(0)


def _canonical(value: str, *, cwd: str | None = None) -> str | None:
    try:
        path = Path(os.path.expandvars(value)).expanduser()
        if not path.is_absolute():
            if not cwd:
                return None
            path = Path(cwd) / path
        return os.path.normcase(os.path.realpath(os.fspath(path)))
    except (OSError, TypeError, ValueError):
        return None


def _under(path: str, root: str) -> bool:
    try:
        return os.path.commonpath((path, root)) == root
    except ValueError:
        return False


def _string_values(value: Any):
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for child in value.values():
            yield from _string_values(child)
    elif isinstance(value, list):
        for child in value:
            yield from _string_values(child)


def _references_protected_path(
    value: str,
    roots: tuple[str, ...],
    *,
    cwd: str | None = None,
) -> bool:
    # File tools normally provide an absolute path.  Also recognize the shell
    # aliases used in the incident that motivated this guard.
    expanded = value.replace("${HOME}", str(Path.home())).replace(
        "$HOME", str(Path.home())
    )
    candidate = _canonical(expanded, cwd=cwd)
    if candidate is not None and any(_under(candidate, root) for root in roots):
        return True

    normalized = expanded.replace("\\", "/")
    if re.search(r"(?:^|[\s'\"=:/])~?/?\.ssh(?:/|[\s'\";|&]|$)", normalized):
        return True
    for root in roots:
        if root and root in expanded:
            return True
    return False


def _bash_references_protected_path(
    command: str,
    roots: tuple[str, ...],
    *,
    cwd: str | None,
) -> bool:
    if _references_protected_path(command, roots):
        return True
    try:
        lexer = shlex.shlex(command, posix=True, punctuation_chars=";&|()<>")
        lexer.whitespace_split = True
        tokens = list(lexer)
    except ValueError:
        # A malformed shell command must not turn a known literal protected
        # path into a fail-open result; the raw-string check above still ran.
        return False
    for token in tokens:
        candidate = token.split("=", 1)[-1] if "=" in token else token
        if _references_protected_path(candidate, roots, cwd=cwd):
            return True
    return False


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--protected-path", action="append", default=[])
    args = parser.parse_args()

    if os.environ.get("CCM_TASK_SSH_GUARD") != "1":
        _allow()

    try:
        payload = json.load(sys.stdin)
    except Exception:
        _deny("CCM blocked this tool because its Task SSH policy could not be validated.")
        return

    roots = tuple(
        sorted({
            canonical
            for value in args.protected_path
            if (canonical := _canonical(value)) is not None
        })
    )
    tool_name = str(payload.get("tool_name") or "")
    tool_input = payload.get("tool_input") or {}
    cwd = str(payload.get("cwd") or "") or None

    if tool_name in _FILE_TOOLS:
        if any(
            _references_protected_path(value, roots, cwd=cwd)
            for value in _string_values(tool_input)
        ):
            _deny(
                "Ambient SSH files are outside this Task's authorization. "
                "Use ccm_ssh.list_connections and the capability-specific "
                "ccm_ssh tools instead."
            )
        _allow()

    if tool_name != "Bash":
        _allow()
    command = str(tool_input.get("command") or "")
    if (
        _DIRECT_SSH_COMMAND.search(command)
        or _REMOTE_RSYNC.search(command)
        or _bash_references_protected_path(command, roots, cwd=cwd)
    ):
        _deny(
            "Direct SSH and ambient SSH credentials are disabled for this "
            "Task. Call ccm_ssh.list_connections first, then use only the "
            "authorized ccm_ssh tools."
        )
    _allow()


if __name__ == "__main__":
    main()
