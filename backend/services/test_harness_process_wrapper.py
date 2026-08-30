"""Small PID/log/status wrapper baked into the untrusted Harness image.

The manager invokes this file with an argv vector through ``docker exec -d``.
It never parses a shell command and writes only to fixed files beneath /run.
"""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys


def _run_path(value: str, suffix: str) -> Path:
    path = Path(value)
    if (
        not path.is_absolute()
        or path.parent != Path("/run")
        or not path.name.startswith("ccm-preview-")
        or not path.name.endswith(suffix)
    ):
        raise ValueError("preview process path is outside /run")
    return path


def main() -> int:
    if len(sys.argv) < 6 or sys.argv[4] != "--":
        raise ValueError("preview process wrapper arguments are invalid")
    pid_path = _run_path(sys.argv[1], ".pid")
    status_path = _run_path(sys.argv[2], ".status")
    log_path = _run_path(sys.argv[3], ".log")
    argv = sys.argv[5:]
    if not argv or any(not item or "\x00" in item for item in argv):
        raise ValueError("preview process argv is invalid")
    with log_path.open("ab", buffering=0) as log_file:
        process = subprocess.Popen(
            argv,
            stdin=subprocess.DEVNULL,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            close_fds=True,
            start_new_session=True,
        )
        pid_path.write_text(f"{process.pid}\n", encoding="ascii")
        code = process.wait()
    status_path.write_text(f"{code}\n", encoding="ascii")
    return code


if __name__ == "__main__":
    os.umask(0o077)
    raise SystemExit(main())
