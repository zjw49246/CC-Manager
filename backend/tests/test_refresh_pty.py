from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
REFRESH_SCRIPT = PROJECT_ROOT / "scripts" / "refresh_pty.sh"
OLD_COMMIT = "a" * 40
NEW_COMMIT = "b" * 40


def _write_executable(path: Path, body: str) -> None:
    path.write_text("#!/bin/sh\nset -eu\n" + body)
    path.chmod(0o700)


def _run_refresh(
    tmp_path: Path,
    *,
    python_location: str = "/tmp/site-packages/claude_pty/__init__.py",
    initial_commit: str = OLD_COMMIT,
    after_install_commit: str = NEW_COMMIT,
    git_exit: int = 0,
    check_only: bool = False,
):
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    shutil.copy2(REFRESH_SCRIPT, scripts / "refresh_pty.sh")
    (tmp_path / "pyproject.toml").write_text(
        '[project]\ndependencies = [\n'
        '    "claude-pty @ git+https://example.invalid/Claude-Code-PTY",\n'
        ']\n'
    )

    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    venv_python = tmp_path / ".venv" / "bin" / "python3"
    venv_python.parent.mkdir(parents=True)
    state_file = tmp_path / "python-state"
    uv_calls = tmp_path / "uv-calls"
    state_file.write_text("0")

    _write_executable(
        venv_python,
        f"""
if [ "${{1:-}}" = "-c" ]; then
    case "${{2:-}}" in
        *'print(claude_pty.__file__)') printf '%s\\n' '{python_location}' ;;
        *) exit 0 ;;
    esac
elif [ "${{1:-}}" = "-" ]; then
    count=$(sed -n '1p' '{state_file}')
    if [ "$count" = "0" ]; then
        printf '%s\\n' '{initial_commit}'
        printf '1\\n' > '{state_file}'
    else
        printf '%s\\n' '{after_install_commit}'
    fi
fi
""",
    )
    _write_executable(
        fake_bin / "git",
        f"""
if [ '{git_exit}' -ne 0 ]; then exit '{git_exit}'; fi
printf '%s\\trefs/heads/main\\n' '{NEW_COMMIT}'
""",
    )
    _write_executable(
        fake_bin / "uv",
        f"printf '%s\\n' \"$*\" >> '{uv_calls}'\n",
    )

    env = {
        **os.environ,
        "PATH": f"{fake_bin}:/usr/bin:/bin",
        "UV": str(fake_bin / "uv"),
        "HOME": str(tmp_path),
    }
    command = ["/bin/bash", str(scripts / "refresh_pty.sh")]
    if check_only:
        command.append("--check")
    result = subprocess.run(
        command,
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )
    return result, uv_calls


def test_refresh_pty_fails_when_remote_probe_fails(tmp_path):
    result, uv_calls = _run_refresh(tmp_path, git_exit=1)

    assert result.returncode != 0
    assert "拒绝完成更新" in result.stderr
    assert not uv_calls.exists()


def test_refresh_pty_fails_when_package_is_missing(tmp_path):
    result, uv_calls = _run_refresh(tmp_path, python_location="")

    assert result.returncode != 0
    assert "未安装或无法导入" in result.stderr
    assert not uv_calls.exists()


def test_refresh_pty_verifies_installed_commit_after_reinstall(tmp_path):
    result, uv_calls = _run_refresh(tmp_path)

    assert result.returncode == 0
    assert "CCM_PTY_REFRESH_CHANGED=1" in result.stdout
    assert uv_calls.read_text().splitlines() == [
        "pip install --python .venv/bin/python3 "
        "--force-reinstall --no-deps "
        "claude-pty @ git+https://example.invalid/Claude-Code-PTY@"
        + NEW_COMMIT
    ]


def test_refresh_pty_rejects_install_that_left_old_commit(tmp_path):
    result, _ = _run_refresh(tmp_path, after_install_commit=OLD_COMMIT)

    assert result.returncode != 0
    assert "安装后 commit 校验失败" in result.stderr


def test_refresh_pty_check_mode_does_not_install(tmp_path):
    result, uv_calls = _run_refresh(tmp_path, check_only=True)

    assert result.returncode == 0
    assert "CCM_PTY_REFRESH_CHANGED=1" in result.stdout
    assert not uv_calls.exists()
