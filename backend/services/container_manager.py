"""Docker container manager for shared Project isolation.

Each shared Project gets its own Docker container with:
- Project directory mounted as /workspace
- Project-specific git credentials (Deploy Key or HTTPS token)
- Restricted capabilities (cap-drop ALL, read-only root, no-new-privileges)
- Resource limits (memory, CPU, pids)
- No access to host filesystem, SSH keys, or other projects
"""

import asyncio
import logging
import os
import re
import secrets
import shlex
import shutil
import signal
import tempfile
from dataclasses import dataclass
from pathlib import Path

from backend.config import settings
from backend.services.cancellation import await_task_completion
from backend.services.process_safety import require_safe_process_group_id

logger = logging.getLogger(__name__)

SANDBOX_IMAGE = "ccm-sandbox:latest"
CONTAINER_PREFIX = "ccm-project-"
_EXEC_TOKEN_ENV = "CCM_CONTAINER_EXEC_TOKEN"
_EXEC_ROLE_ENV = "CCM_CONTAINER_EXEC_ROLE"
_API_ACCOUNT_CONTAINER_ROOT = "/home/sandbox/.ccm-api-account"
_TMP_RUNTIME_DIR = "/home/sandbox/.ccm-runtime"
_TMP_LEASE_PATH = f"{_TMP_RUNTIME_DIR}/tmp-pressure.lock"
_TMP_ROOT = "/tmp"


class ContainerTmpPressureError(RuntimeError):
    """A shared-container Agent cannot safely use its isolated temporary FS."""


# The lease inode is created by root under a root-owned directory and exposed
# read-only to the sandbox uid.  Agent code can contend on the inode (causing a
# safe denial of service) but cannot unlink/replace it and split the lock.
_TMP_LEASE_INIT = r"""
import os
import stat
import sys

runtime_dir, lease_path = sys.argv[1:3]
runtime_parent = os.path.dirname(runtime_dir)
parent_stat = os.lstat(runtime_parent)
parent_is_protected = (
    stat.S_ISDIR(parent_stat.st_mode)
    and not stat.S_ISLNK(parent_stat.st_mode)
    and parent_stat.st_uid == 0
    and (
        (parent_stat.st_mode & 0o022) == 0
        or (parent_stat.st_mode & stat.S_ISVTX) != 0
    )
)
if not parent_is_protected:
    raise SystemExit("unsafe CCM runtime parent directory")
os.makedirs(runtime_dir, mode=0o755, exist_ok=True)
runtime_stat = os.lstat(runtime_dir)
if (
    not stat.S_ISDIR(runtime_stat.st_mode)
    or stat.S_ISLNK(runtime_stat.st_mode)
    or runtime_stat.st_uid != 0
    or runtime_stat.st_mode & 0o022
):
    raise SystemExit("unsafe CCM runtime directory")

flags = os.O_RDONLY | os.O_CREAT
flags |= getattr(os, "O_CLOEXEC", 0)
flags |= getattr(os, "O_NOFOLLOW", 0)
fd = os.open(lease_path, flags, 0o444)
try:
    lease_stat = os.fstat(fd)
    if (
        not stat.S_ISREG(lease_stat.st_mode)
        or lease_stat.st_uid != 0
        or lease_stat.st_nlink != 1
        or lease_stat.st_mode & 0o222
    ):
        raise SystemExit("unsafe CCM tmp-pressure lease")
    os.fchmod(fd, 0o444)
finally:
    os.close(fd)
"""


# This library is sent to the sandbox rather than imported there: the image
# intentionally does not mount CCM's backend source.  Its safety boundary is
# the root-owned flock plus an idle /proc proof.  /tmp is a private tmpfs for
# this one Project container, so after that proof the whole filesystem is a
# disposable sandbox (unlike the host /tmp, which uses a narrow allow-list).
_TMP_PRESSURE_LIB = r"""
import fcntl
import os
import shutil
import stat

TMP_BUSY_EXIT = 75
TMP_UNSAFE_EXIT = 76

class TmpPressureGateError(RuntimeError):
    def __init__(self, message, exit_code=TMP_UNSAFE_EXIT):
        super().__init__(message)
        self.exit_code = exit_code

def _usage_ratios(tmp_root):
    values = os.statvfs(tmp_root)
    if values.f_blocks <= 0:
        bytes_ratio = 1.0
    else:
        bytes_ratio = 1.0 - (values.f_bavail / values.f_blocks)
    if values.f_files <= 0:
        inode_ratio = None
    else:
        inode_ratio = 1.0 - (values.f_favail / values.f_files)
    return bytes_ratio, inode_ratio

def _under_ratio(ratios, ratio):
    return all(value is None or value < ratio for value in ratios)

def _open_lease(lease_path, expected_owner):
    flags = os.O_RDONLY
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(lease_path, flags)
    lease_stat = os.fstat(fd)
    if (
        not stat.S_ISREG(lease_stat.st_mode)
        or lease_stat.st_uid != expected_owner
        or lease_stat.st_nlink != 1
        or lease_stat.st_mode & 0o222
    ):
        os.close(fd)
        raise TmpPressureGateError("unsafe CCM tmp-pressure lease inode")
    return fd

def _read_process_command(pid):
    with open("/proc/%d/cmdline" % pid, "rb") as stream:
        return [value for value in stream.read().split(b"\0") if value]

def _read_process_parent(pid):
    with open("/proc/%d/stat" % pid, "rb") as stream:
        raw = stream.read()
    close = raw.rfind(b")")
    fields = raw[close + 2:].split() if close >= 0 else []
    if len(fields) < 2:
        raise ProcessLookupError(pid)
    return int(fields[1])

def _is_idle_tail(command):
    return (
        len(command) == 3
        and os.path.basename(command[0]) == b"tail"
        and command[1:] == [b"-f", b"/dev/null"]
    )

def _unexpected_processes():
    current_pid = os.getpid()
    unexpected = []
    try:
        process_entries = [
            int(entry) for entry in os.listdir("/proc") if entry.isdigit()
        ]
        pid_one_command = _read_process_command(1)
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        return [1]

    allowed = set()
    if _is_idle_tail(pid_one_command):
        # Compatibility for containers created before CCM added --init.
        pass
    elif (
        len(pid_one_command) == 5
        and os.path.basename(pid_one_command[0]) in (b"docker-init", b"tini")
        and pid_one_command[1] == b"--"
        and _is_idle_tail(pid_one_command[2:])
    ):
        idle_children = []
        for pid in process_entries:
            if pid in (1, current_pid):
                continue
            try:
                command = _read_process_command(pid)
                parent = _read_process_parent(pid)
            except (FileNotFoundError, PermissionError, ProcessLookupError):
                continue
            if parent == 1 and _is_idle_tail(command):
                idle_children.append(pid)
        if len(idle_children) == 1:
            allowed.add(idle_children[0])
        else:
            unexpected.append(1)
    else:
        unexpected.append(1)

    for pid in process_entries:
        if pid in (1, current_pid) or pid in allowed:
            continue
        try:
            with open("/proc/%d/environ" % pid, "rb") as stream:
                stream.read()
        except (FileNotFoundError, ProcessLookupError):
            # It exited between /proc enumeration and inspection.
            continue
        except PermissionError:
            # A live but unverifiable process is not an idle proof.
            unexpected.append(pid)
            continue
        # Do not trust a role environment variable to exempt a process here:
        # Agent-controlled Bash can forge its environment. Concurrent probes
        # may therefore cause a conservative busy result, but can never make
        # an active process invisible to cleanup.
        unexpected.append(pid)
    return unexpected

def _validate_tree(path, root_device):
    metadata = os.lstat(path)
    if metadata.st_dev != root_device:
        raise TmpPressureGateError("refusing nested filesystem under /tmp")
    if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        return
    for current_root, directories, files in os.walk(
        path, topdown=True, followlinks=False
    ):
        current_stat = os.lstat(current_root)
        if current_stat.st_dev != root_device:
            raise TmpPressureGateError("refusing nested filesystem under /tmp")
        for name in directories + files:
            child_stat = os.lstat(os.path.join(current_root, name))
            if child_stat.st_dev != root_device:
                raise TmpPressureGateError(
                    "refusing nested filesystem under /tmp"
                )

def _clear_private_tmp(tmp_root):
    root_stat = os.lstat(tmp_root)
    if not stat.S_ISDIR(root_stat.st_mode) or stat.S_ISLNK(root_stat.st_mode):
        raise TmpPressureGateError("container /tmp is not a real directory")
    if not shutil.rmtree.avoids_symlink_attacks:
        raise TmpPressureGateError("safe recursive deletion is unavailable")
    for entry in os.scandir(tmp_root):
        path = entry.path
        _validate_tree(path, root_stat.st_dev)
        metadata = os.lstat(path)
        if stat.S_ISDIR(metadata.st_mode) and not stat.S_ISLNK(metadata.st_mode):
            shutil.rmtree(path)
        else:
            os.unlink(path)

def acquire_agent_tmp_lease(
    tmp_root,
    lease_path,
    expected_owner,
    trigger_ratio,
):
    if not 0 < trigger_ratio <= 1:
        raise TmpPressureGateError("invalid tmp pressure threshold")

    lease_fd = _open_lease(lease_path, expected_owner)
    try:
        # Every Agent holds SH from before it creates a pid file/child until
        # the supervisor has killed and reaped the complete inner group.
        fcntl.flock(lease_fd, fcntl.LOCK_SH)
        before = _usage_ratios(tmp_root)
        if _under_ratio(before, trigger_ratio):
            return lease_fd

        fcntl.flock(lease_fd, fcntl.LOCK_UN)
        try:
            fcntl.flock(lease_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            # If another cleaner owns EX, this blocks until its result is
            # visible. If Agents own SH, SH succeeds immediately and pressure
            # remains, so this launch fails without interrupting them.
            fcntl.flock(lease_fd, fcntl.LOCK_SH)
            after_wait = _usage_ratios(tmp_root)
            if _under_ratio(after_wait, trigger_ratio):
                return lease_fd
            raise TmpPressureGateError(
                "container /tmp is pressured while an Agent is active",
                TMP_BUSY_EXIT,
            )

        # Re-read under EX in case a sibling cleaner won the race first.
        under_exclusive = _usage_ratios(tmp_root)
        if not _under_ratio(under_exclusive, trigger_ratio):
            unexpected = _unexpected_processes()
            if unexpected:
                raise TmpPressureGateError(
                    "container is not idle; unexpected pids: "
                    + ",".join(str(pid) for pid in unexpected),
                    TMP_BUSY_EXIT,
                )
            _clear_private_tmp(tmp_root)
            after_cleanup = _usage_ratios(tmp_root)
            if not _under_ratio(after_cleanup, trigger_ratio):
                raise TmpPressureGateError(
                    "container /tmp remains at the pressure threshold"
                )

        # Conversion may briefly queue behind another waiter, but no Agent
        # child exists yet; the returned SH is held before child creation.
        fcntl.flock(lease_fd, fcntl.LOCK_SH)
        final_usage = _usage_ratios(tmp_root)
        if not _under_ratio(final_usage, trigger_ratio):
            raise TmpPressureGateError(
                "container /tmp returned to pressure before Agent launch"
            )
        return lease_fd
    except BaseException:
        os.close(lease_fd)
        raise
"""


_TMP_PRESSURE_PREFLIGHT = _TMP_PRESSURE_LIB + r"""
import sys

tmp_root = sys.argv[1]
lease_path = sys.argv[2]
expected_owner = int(sys.argv[3])
trigger_ratio = float(sys.argv[4])
try:
    lease_fd = acquire_agent_tmp_lease(
        tmp_root,
        lease_path,
        expected_owner,
        trigger_ratio,
    )
except TmpPressureGateError as exc:
    print(str(exc), file=sys.stderr)
    raise SystemExit(exc.exit_code)
os.close(lease_fd)
"""


class ContainerExecSpawnCleanupError(RuntimeError):
    """A cancelled docker-exec spawn whose exact cleanup was not proven."""

    def __init__(
        self,
        process: asyncio.subprocess.Process,
        spec: "ContainerExecSpec",
    ):
        super().__init__(
            f"Cancelled container exec {spec.token} could not be reaped"
        )
        self.process = process
        self.spec = spec


@dataclass(frozen=True)
class ContainerExecSpec:
    """Stable token and paths for one exact in-container generation."""

    container_name: str
    token: str
    pid_file: str
    wrapper_path: str | None = None


@dataclass(frozen=True)
class _ContainerExec:
    """Host process identity paired with its in-container generation."""

    process: asyncio.subprocess.Process
    spec: ContainerExecSpec


# ``docker exec`` is only a client connection.  Killing that host process does
# not guarantee that the command in the container stopped.  This supervisor
# gives the inner command its own session, publishes its group identity, and
# reaps/kills any group members which outlive the command leader.  Arguments
# are passed positionally, never interpolated into shell source.
_EXEC_SUPERVISOR = _TMP_PRESSURE_LIB + r"""
import os
import signal
import sys
import time

tmp_enabled = sys.argv[1] == "1"
tmp_root = sys.argv[2]
tmp_lease_path = sys.argv[3]
tmp_lease_owner = int(sys.argv[4])
tmp_trigger_ratio = float(sys.argv[5])
pid_file = sys.argv[6]
command = sys.argv[7:]
if not command:
    raise SystemExit(127)

tmp_lease_fd = None
if tmp_enabled:
    try:
        tmp_lease_fd = acquire_agent_tmp_lease(
            tmp_root,
            tmp_lease_path,
            tmp_lease_owner,
            tmp_trigger_ratio,
        )
    except TmpPressureGateError as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(exc.exit_code)

child = os.fork()
if child == 0:
    os.setsid()
    os.environ["CCM_CONTAINER_EXEC_ROLE"] = "agent"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    fd = os.open(pid_file, flags, 0o600)
    try:
        os.write(fd, str(os.getpid()).encode("ascii"))
        os.fsync(fd)
    finally:
        os.close(fd)
    os.execvpe(command[0], command, os.environ)

if child <= 1:
    # ``killpg(1, sig)`` becomes the special broadcast ``kill(-1, sig)``.
    # fork() must never return such an identity to this parent.
    raise SystemExit(125)

def forward(signum, _frame):
    try:
        os.killpg(child, signum)
    except ProcessLookupError:
        pass

for forwarded in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
    signal.signal(forwarded, forward)

while True:
    try:
        _, status = os.waitpid(child, 0)
        break
    except InterruptedError:
        continue

def group_alive():
    try:
        os.killpg(child, 0)
        return True
    except ProcessLookupError:
        return False

# Tool processes can retain inherited descriptors after the agent leader exits.
# They must not survive into the next task which reuses this project container.
for cleanup_signal, deadline_seconds in (
    (signal.SIGTERM, 1.0),
    (signal.SIGKILL, 1.0),
):
    if not group_alive():
        break
    try:
        os.killpg(child, cleanup_signal)
    except ProcessLookupError:
        break
    deadline = time.monotonic() + deadline_seconds
    while group_alive() and time.monotonic() < deadline:
        time.sleep(0.02)

try:
    os.unlink(pid_file)
except FileNotFoundError:
    pass

# Release the shared lease only after the complete inner process group is
# proven gone and its pid evidence is removed.
if tmp_lease_fd is not None:
    os.close(tmp_lease_fd)
    tmp_lease_fd = None

if os.WIFEXITED(status):
    raise SystemExit(os.WEXITSTATUS(status))
if os.WIFSIGNALED(status):
    raise SystemExit(128 + os.WTERMSIG(status))
raise SystemExit(1)
"""


# Locate only processes carrying the unguessable per-exec token.  The pid file
# is an optimization, not the trust boundary: an agent can remove files in
# /tmp, so /proc is also scanned before signalling a process group.
_EXEC_CONTROL = r"""
import os
import signal
import sys
import time

token, pid_file, action = sys.argv[1:4]
requested_signal = int(sys.argv[4]) if action == "signal" else 0
wait_seconds = float(sys.argv[5])
token_entry = ("CCM_CONTAINER_EXEC_TOKEN=" + token).encode()
agent_role = b"CCM_CONTAINER_EXEC_ROLE=agent"
supervisor_role = b"CCM_CONTAINER_EXEC_ROLE=supervisor"

def tagged_processes():
    agents = []
    supervisors = []
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        pid = int(entry)
        try:
            with open("/proc/%d/environ" % pid, "rb") as stream:
                values = stream.read().split(b"\0")
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            continue
        if token_entry not in values:
            continue
        if agent_role in values:
            agents.append(pid)
        elif supervisor_role in values:
            supervisors.append(pid)
    return agents, supervisors

deadline = time.monotonic() + wait_seconds
unsafe_group = False
while True:
    agents, supervisors = tagged_processes()
    groups = set()
    for pid in agents:
        try:
            group = os.getpgid(pid)
            if group <= 1:
                unsafe_group = True
            else:
                groups.add(group)
        except ProcessLookupError:
            pass
    if unsafe_group:
        # Never turn a targeted container cleanup into kill(-1, sig).
        raise SystemExit(4)
    if groups or time.monotonic() >= deadline:
        break
    # A check can return immediately when no tagged process exists.  A signal
    # intentionally waits out the short docker-exec startup window: killing
    # the host client does not prove the daemon will not start an already
    # accepted tokenized command a moment later.
    if action == "check" and not supervisors:
        break
    time.sleep(0.02)

alive_groups = []
for group in groups:
    try:
        os.killpg(group, 0)
        alive_groups.append(group)
    except ProcessLookupError:
        pass

if action == "check":
    if alive_groups:
        raise SystemExit(0)
    # A live supervisor with no visible child is a short startup/cleanup
    # transition.  Treat it as alive so callers fail closed.
    raise SystemExit(2 if supervisors else 3)

signalled = False
if alive_groups:
    for group in alive_groups:
        try:
            os.killpg(group, requested_signal)
            signalled = True
        except ProcessLookupError:
            pass
# With no published agent group, include the exact tokenized supervisor (and a
# child in the tiny pre-agent role transition) so cancellation cannot leave a
# command which has not completed setsid yet.  Once an agent group is visible,
# leave its supervisor alive long enough to reap the leader and report the
# command's conventional exit status.
if not alive_groups:
    for pid in supervisors:
        try:
            os.kill(pid, requested_signal)
            signalled = True
        except ProcessLookupError:
            pass
if signalled:
    raise SystemExit(0)
raise SystemExit(3)
"""


class ContainerManager:
    """Manages Docker containers for shared project isolation."""

    def __init__(self):
        self._containers: dict[int, str] = {}
        self._locks: dict[int, asyncio.Lock] = {}
        self._git_dirs: dict[int, str] = {}  # project_id -> temp dir with git credentials
        self._execs: dict[int, _ContainerExec] = {}

    def _lock(self, project_id: int) -> asyncio.Lock:
        if project_id not in self._locks:
            self._locks[project_id] = asyncio.Lock()
        return self._locks[project_id]

    @staticmethod
    def _tmp_pressure_args() -> list[str]:
        return [
            _TMP_ROOT,
            _TMP_LEASE_PATH,
            "0",
            str(settings.tmp_cleanup_usage_threshold),
        ]

    @staticmethod
    async def _run(cmd: list[str], timeout: int = 60) -> tuple[int, str]:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        cancellation: asyncio.CancelledError | None = None

        spawn = asyncio.create_task(
            asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
        )
        cancellation = await await_task_completion(spawn)
        try:
            proc = spawn.result()
        except BaseException:
            if cancellation is not None:
                raise cancellation
            raise

        communication = asyncio.create_task(proc.communicate())
        timed_out = False
        remaining = deadline - loop.time()
        if remaining <= 0:
            timed_out = True
        elif not communication.done():
            # Create this deadline waiter once. Recreating wait_for(shield())
            # inside a cancelled AnyIO scope immediately re-cancels every new
            # wrapper and can busy-spin without allowing communicate() to run.
            deadline_wait = asyncio.create_task(
                asyncio.wait({communication}, timeout=remaining)
            )
            wait_cancellation = await await_task_completion(deadline_wait)
            cancellation = cancellation or wait_cancellation
            done, _pending = deadline_wait.result()
            timed_out = not done

        if timed_out and proc.returncode is None:
            try:
                proc.kill()
            except ProcessLookupError:
                pass

        settle_cancellation = await await_task_completion(communication)
        cancellation = cancellation or settle_cancellation

        try:
            out, _ = communication.result()
        except BaseException:
            if proc.returncode is None:
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
                await proc.wait()
            if cancellation is not None:
                raise cancellation
            raise

        if cancellation is not None:
            raise cancellation
        if timed_out:
            return 1, "timeout"
        return proc.returncode or 0, (out or b"").decode("utf-8", errors="replace")

    @staticmethod
    def is_docker_available() -> bool:
        return shutil.which("docker") is not None

    async def _initialize_tmp_pressure_lease(self, name: str) -> None:
        """Create the immutable container-internal lease as root."""

        code, output = await self._run(
            [
                "docker",
                "exec",
                "-u",
                "0",
                name,
                "python3",
                "-c",
                _TMP_LEASE_INIT,
                _TMP_RUNTIME_DIR,
                _TMP_LEASE_PATH,
            ]
        )
        if code != 0:
            raise ContainerTmpPressureError(
                "Could not establish the shared-container /tmp safety lease: "
                f"{output[:500]}"
            )

    async def ensure_tmp_capacity(self, project_id: int) -> None:
        """Require the private container /tmp to be ready for a new Agent."""

        if not settings.tmp_cleanup_enabled:
            return
        name = self._containers.get(
            project_id, f"{CONTAINER_PREFIX}{project_id}"
        )
        code, output = await self._run(
            [
                "docker",
                "exec",
                "-e",
                f"{_EXEC_ROLE_ENV}=tmp-gate",
                name,
                "python3",
                "-c",
                _TMP_PRESSURE_PREFLIGHT,
                *self._tmp_pressure_args(),
            ]
        )
        if code != 0:
            detail = output.strip() or f"container preflight exited {code}"
            raise ContainerTmpPressureError(
                "Shared-container /tmp is not safe for a new Agent: "
                f"{detail[:500]}"
            )

    def _prepare_git_credentials(self, project_id: int, git_credential_type: str | None,
                                  git_ssh_key_path: str | None,
                                  git_https_username: str | None,
                                  git_https_token: str | None) -> str | None:
        """Create a temp directory with project-specific git credentials.

        Returns the temp dir path to mount into the container, or None.
        """
        if not git_credential_type:
            return None

        git_dir = tempfile.mkdtemp(prefix=f"ccm-git-{project_id}-")
        self._git_dirs[project_id] = git_dir

        if git_credential_type == "ssh" and git_ssh_key_path:
            # Copy the Deploy Key into the temp dir
            ssh_dir = os.path.join(git_dir, ".ssh")
            os.makedirs(ssh_dir, mode=0o700)
            key_dest = os.path.join(ssh_dir, "id_rsa")
            try:
                shutil.copy2(git_ssh_key_path, key_dest)
                os.chmod(key_dest, 0o600)
            except Exception:
                logger.warning("Failed to copy SSH key %s for project %d", git_ssh_key_path, project_id)
                return None

            # SSH config: skip host key checking for git
            with open(os.path.join(ssh_dir, "config"), "w") as f:
                f.write("Host *\n  StrictHostKeyChecking no\n  UserKnownHostsFile /dev/null\n  IdentityFile ~/.ssh/id_rsa\n")
            os.chmod(os.path.join(ssh_dir, "config"), 0o600)

        elif git_credential_type == "https" and git_https_token:
            # Git credential helper that returns the token
            cred_script = os.path.join(git_dir, "git-credential-helper.sh")
            username = git_https_username or "oauth2"
            with open(cred_script, "w") as f:
                f.write(f"#!/bin/sh\necho username={username}\necho password={git_https_token}\n")
            os.chmod(cred_script, 0o755)

            # .gitconfig that uses the credential helper
            with open(os.path.join(git_dir, ".gitconfig"), "w") as f:
                f.write(f"[credential]\n  helper = {cred_script}\n")

        return git_dir

    async def ensure_container(self, project_id: int, project_path: str,
                                config_dir: str | None = None,
                                *,
                                api_account_root: str | None = None,
                                git_credential_type: str | None = None,
                                git_ssh_key_path: str | None = None,
                                git_https_username: str | None = None,
                                git_https_token: str | None = None) -> str:
        """Ensure a running container for this project with isolated git credentials."""
        async with self._lock(project_id):
            name = f"{CONTAINER_PREFIX}{project_id}"
            desired_api_root = (
                str(Path(api_account_root).resolve(strict=False))
                if api_account_root
                else ""
            )

            code, out = await self._run(["docker", "inspect", "-f", "{{.State.Running}}", name])
            if code == 0 and "true" in out.lower():
                mount_template = (
                    "{{range .Mounts}}{{if eq .Destination "
                    f"\"{_API_ACCOUNT_CONTAINER_ROOT}\""
                    "}}{{.Source}}{{end}}{{end}}"
                )
                mount_code, mounted_root = await self._run(
                    ["docker", "inspect", "-f", mount_template, name]
                )
                current_api_root = (
                    str(Path(mounted_root.strip()).resolve(strict=False))
                    if mount_code == 0 and mounted_root.strip()
                    else ""
                )
                if current_api_root == desired_api_root:
                    self._containers[project_id] = name
                    if settings.tmp_cleanup_enabled:
                        await self._initialize_tmp_pressure_lease(name)
                        await self.ensure_tmp_capacity(project_id)
                    return name
                logger.info(
                    "Recreating container %s because its API account mount "
                    "changed (%s -> %s)",
                    name,
                    current_api_root or "none",
                    desired_api_root or "none",
                )

            await self._run(["docker", "rm", "-f", name])
            os.makedirs(project_path, exist_ok=True)

            # Prepare project-specific git credentials
            git_dir = self._prepare_git_credentials(
                project_id, git_credential_type,
                git_ssh_key_path, git_https_username, git_https_token
            )

            cmd = [
                "docker", "run", "-d",
                "--name", name,
                "--init",
                "--security-opt", "no-new-privileges",
                "--cap-drop", "ALL",
                "--read-only",
                "--tmpfs", "/tmp:size=2g",
                "--tmpfs", "/home/sandbox:size=1g",
                "--pids-limit", "200",
                "-v", f"{project_path}:/workspace",
            ]

            # Mount Claude config (read-only, for auth)
            if config_dir:
                cmd.extend(["-v", f"{config_dir}:/home/sandbox/.claude:ro"])
            if desired_api_root:
                cmd.extend([
                    "-v",
                    f"{desired_api_root}:{_API_ACCOUNT_CONTAINER_ROOT}:ro",
                ])

            # Mount project-specific git credentials
            if git_dir:
                ssh_dir = os.path.join(git_dir, ".ssh")
                if os.path.isdir(ssh_dir):
                    cmd.extend(["-v", f"{ssh_dir}:/home/sandbox/.ssh:ro"])
                gitconfig = os.path.join(git_dir, ".gitconfig")
                if os.path.isfile(gitconfig):
                    cmd.extend(["-v", f"{gitconfig}:/home/sandbox/.gitconfig:ro"])
                cred_script = os.path.join(git_dir, "git-credential-helper.sh")
                if os.path.isfile(cred_script):
                    cmd.extend(["-v", f"{cred_script}:/home/sandbox/git-credential-helper.sh:ro"])

            cmd.extend(["--entrypoint", "tail", SANDBOX_IMAGE, "-f", "/dev/null"])

            code, out = await self._run(cmd, timeout=120)
            if code != 0:
                logger.error("Failed to start container %s: %s", name, out)
                raise RuntimeError(f"Docker container start failed: {out[:500]}")

            self._containers[project_id] = name
            if settings.tmp_cleanup_enabled:
                await self._initialize_tmp_pressure_lease(name)
                await self.ensure_tmp_capacity(project_id)
            logger.info("Container %s started for project %d (git: %s)",
                        name, project_id, git_credential_type or "none")
            return name

    async def exec_command(self, project_id: int, cmd: list[str],
                           env: dict[str, str] | None = None,
                           cwd: str = "/workspace") -> asyncio.subprocess.Process:
        """Execute a command in an exact, externally controllable inner group."""
        name = self._containers.get(project_id, f"{CONTAINER_PREFIX}{project_id}")
        token = secrets.token_hex(24)
        pid_file = f"/tmp/ccm-exec-{token}.pid"
        spec = ContainerExecSpec(name, token, pid_file)

        docker_cmd = ["docker", "exec", "-i", "-w", cwd]
        if env:
            for k, v in env.items():
                docker_cmd.extend(["-e", f"{k}={v}"])
        docker_cmd.extend([
            "-e", f"{_EXEC_TOKEN_ENV}={token}",
            "-e", f"{_EXEC_ROLE_ENV}=supervisor",
            name,
            "python3",
            "-c",
            _EXEC_SUPERVISOR,
            "1" if settings.tmp_cleanup_enabled else "0",
            *self._tmp_pressure_args(),
            pid_file,
            *cmd,
        ])

        spawn_kwargs = {
            "stdout": asyncio.subprocess.PIPE,
            "stderr": asyncio.subprocess.PIPE,
            "limit": 10 * 1024 * 1024,
        }
        if os.name == "posix":
            spawn_kwargs["start_new_session"] = True
        spawn = asyncio.create_task(
            asyncio.create_subprocess_exec(*docker_cmd, **spawn_kwargs)
        )
        cancellation = await await_task_completion(spawn)

        process = spawn.result()
        self.register_exec(process, spec)
        if cancellation is not None:
            cleanup = asyncio.create_task(
                self._cleanup_cancelled_exec_spawn(process, spec)
            )
            later_cancellation = await await_task_completion(cleanup)
            cancellation = cancellation or later_cancellation
            # On failure keep the exact record in ``_execs`` as fail-closed
            # evidence.  The cleanup exception is logged, but cancellation
            # remains the public outcome expected by the caller.
            cleanup_error: BaseException | None = None
            try:
                cleanup.result()
            except BaseException as exc:
                cleanup_error = exc
                logger.exception(
                    "Could not prove cancelled container exec %s terminal",
                    spec.token,
                    exc_info=(type(exc), exc, exc.__traceback__),
                )
            if cleanup_error is not None:
                raise ContainerExecSpawnCleanupError(
                    process, spec
                ) from cleanup_error
            raise cancellation
        return process

    async def _cleanup_cancelled_exec_spawn(
        self,
        process: asyncio.subprocess.Process,
        spec: ContainerExecSpec,
    ) -> None:
        """Stop both sides of a docker-exec spawn cancelled before return."""

        host_group_alive = False
        host_process_group_id: int | None = None
        if os.name == "posix":
            host_process_group_id = require_safe_process_group_id(
                getattr(process, "pid", None),
                context=f"container exec {spec.token}",
            )
            try:
                os.killpg(host_process_group_id, 0)
                host_group_alive = True
            except ProcessLookupError:
                pass
            except PermissionError:
                host_group_alive = True

        if process.returncode is None or host_group_alive:
            try:
                if host_process_group_id is not None:
                    os.killpg(host_process_group_id, signal.SIGKILL)
                else:
                    process.kill()
            except ProcessLookupError:
                pass
        if process.returncode is None:
            await asyncio.wait_for(
                asyncio.shield(process.wait()), timeout=5.0
            )

        # Killing docker(1) only closes a client connection.  Scan by the
        # unguessable token after host termination, kill startup transitions
        # and agent groups, then require a subsequent empty scan.
        code = await self._control_spec(
            spec,
            action="signal",
            sig=signal.SIGKILL,
            wait_seconds=2.0,
        )
        if code not in (0, 3):
            raise RuntimeError(
                f"Could not signal cancelled container exec {spec.token}"
            )
        deadline = asyncio.get_running_loop().time() + 5.0
        while True:
            code = await self._control_spec(spec, action="check")
            if code == 3:
                break
            if asyncio.get_running_loop().time() >= deadline:
                raise RuntimeError(
                    f"Cancelled container exec {spec.token} survived SIGKILL"
                )
            await asyncio.sleep(0.05)
        self.forget_exec(process)

    def create_pty_wrapper(
        self,
        project_id: int,
        instance_id: int,
    ) -> tuple[str, ContainerExecSpec]:
        """Create a PTY binary wrapper using the same supervised exec protocol."""

        name = self._containers.get(project_id, f"{CONTAINER_PREFIX}{project_id}")
        token = secrets.token_hex(24)
        pid_file = f"/tmp/ccm-exec-{token}.pid"
        # Respect the host's configured temporary directory.  Some deployments
        # put /tmp on a small per-user quota even when the application volume
        # still has ample space; hard-coding /tmp can then make PTY launch fail
        # before Docker is involved.
        wrapper_path = os.path.join(
            tempfile.gettempdir(),
            f"ccm-docker-claude-{instance_id}-{token}.sh",
        )
        spec = ContainerExecSpec(name, token, pid_file, wrapper_path)
        command = [
            "docker",
            "exec",
            "-i",
            "-e",
            f"{_EXEC_TOKEN_ENV}={token}",
            "-e",
            f"{_EXEC_ROLE_ENV}=supervisor",
            name,
            "python3",
            "-c",
            _EXEC_SUPERVISOR,
            "1" if settings.tmp_cleanup_enabled else "0",
            *self._tmp_pressure_args(),
            pid_file,
            "claude",
        ]
        quoted = " ".join(shlex.quote(part) for part in command)
        with open(wrapper_path, "w", encoding="utf-8") as wrapper:
            wrapper.write(
                "#!/bin/sh\n"
                f"exec {quoted} \"$@\"\n"
            )
        os.chmod(wrapper_path, 0o700)
        return wrapper_path, spec

    def register_exec(
        self,
        process: asyncio.subprocess.Process,
        spec: ContainerExecSpec,
    ) -> None:
        self._execs[id(process)] = _ContainerExec(process=process, spec=spec)

    def owns_exec(self, process: asyncio.subprocess.Process) -> bool:
        record = self._execs.get(id(process))
        return record is not None and record.process is process

    async def _control_exec(
        self,
        process: asyncio.subprocess.Process,
        *,
        action: str,
        sig: signal.Signals | None = None,
        wait_seconds: float = 0.0,
    ) -> int | None:
        record = self._execs.get(id(process))
        if record is None or record.process is not process:
            return None
        return await self._control_spec(
            record.spec,
            action=action,
            sig=sig,
            wait_seconds=wait_seconds,
        )

    async def _control_spec(
        self,
        spec: ContainerExecSpec,
        *,
        action: str,
        sig: signal.Signals | None = None,
        wait_seconds: float = 0.0,
    ) -> int:
        code, output = await self._run(
            [
                "docker",
                "exec",
                spec.container_name,
                "python3",
                "-c",
                _EXEC_CONTROL,
                spec.token,
                spec.pid_file,
                action,
                str(int(sig or 0)),
                str(wait_seconds),
            ],
            timeout=max(5, int(wait_seconds) + 3),
        )
        if code not in (0, 2, 3):
            raise RuntimeError(
                f"Could not {action} container exec in "
                f"{spec.container_name}: {output[:500]}"
            )
        return code

    async def signal_exec(
        self,
        process: asyncio.subprocess.Process,
        sig: signal.Signals,
    ) -> bool:
        """Signal the exact in-container process group for ``process``."""

        code = await self._control_exec(
            process,
            action="signal",
            sig=sig,
            wait_seconds=1.0,
        )
        if code is None:
            return False
        if code == 2 and process.returncode is None:
            raise RuntimeError(
                "Container exec supervisor is live but its agent group "
                "could not be identified"
            )
        return code == 0

    async def exec_is_alive(
        self,
        process: asyncio.subprocess.Process,
    ) -> bool:
        """Return whether the exact tokenized inner group/supervisor survives."""

        code = await self._control_exec(process, action="check")
        if code is None:
            return False
        return code in (0, 2)

    def forget_exec(self, process: asyncio.subprocess.Process) -> None:
        record = self._execs.get(id(process))
        if record is not None and record.process is process:
            self._execs.pop(id(process), None)
            self.discard_spec(record.spec)

    @staticmethod
    def discard_spec(spec: ContainerExecSpec) -> None:
        if spec.wrapper_path:
            try:
                os.unlink(spec.wrapper_path)
            except FileNotFoundError:
                pass

    async def stop_container(self, project_id: int):
        name = self._containers.pop(project_id, f"{CONTAINER_PREFIX}{project_id}")
        await self._run(["docker", "stop", "-t", "10", name])
        await self._run(["docker", "rm", "-f", name])
        # Clean up git credentials temp dir
        git_dir = self._git_dirs.pop(project_id, None)
        if git_dir and os.path.isdir(git_dir):
            shutil.rmtree(git_dir, ignore_errors=True)
        logger.info("Container %s stopped", name)

    async def retire_api_account_mounts(
        self, api_account_root: str | os.PathLike[str],
    ) -> int:
        """Stop CCM-owned containers bind-mounting one exact API account.

        Account retirement calls this only after task/process admission has
        been durably disabled and all known active turns were rejected. The
        scan includes containers surviving a CCM restart, not just this
        object's in-memory cache. Exact canonical source matching prevents an
        arbitrary path from becoming a Docker removal selector.
        """

        source = str(Path(api_account_root).resolve(strict=True))
        if not self.is_docker_available():
            # A CCM container created before a CLI/package failure can retain
            # an open read-only bind mount (and therefore the key inode).
            # Absence of the client is not proof that no such container exists.
            raise RuntimeError(
                "Docker is unavailable; API account mounts cannot be verified",
            )
        code, output = await self._run([
            "docker",
            "ps",
            "-a",
            "--format",
            "{{.Names}}",
            "--filter",
            f"name=^{CONTAINER_PREFIX}",
        ])
        if code != 0:
            raise RuntimeError(
                "Could not verify API account container mounts",
            )
        names: dict[int, str] = dict(self._containers)
        for raw_name in output.splitlines():
            name = raw_name.strip()
            match = re.fullmatch(rf"{re.escape(CONTAINER_PREFIX)}([0-9]+)", name)
            if match:
                names.setdefault(int(match.group(1)), name)

        stopped = 0
        mount_template = (
            "{{range .Mounts}}{{if eq .Destination "
            f"\"{_API_ACCOUNT_CONTAINER_ROOT}\""
            "}}{{.Source}}{{end}}{{end}}"
        )
        for project_id, name in sorted(names.items()):
            async with self._lock(project_id):
                expected_name = f"{CONTAINER_PREFIX}{project_id}"
                if name != expected_name:
                    raise RuntimeError(
                        "Could not verify CCM container identity",
                    )
                mount_code, mounted_root = await self._run([
                    "docker", "inspect", "-f", mount_template, name,
                ])
                if mount_code != 0:
                    # Distinguish an ordinary list/inspect disappearance race
                    # from daemon, permission, or malformed-state failures.
                    # The project lock prevents CCM from recreating this exact
                    # name between the proof and cleanup.
                    proof_code, proof_output = await self._run([
                        "docker",
                        "ps",
                        "-a",
                        "--format",
                        "{{.Names}}",
                        "--filter",
                        f"name=^{re.escape(name)}$",
                    ])
                    if proof_code != 0:
                        raise RuntimeError(
                            "Could not verify an API account container",
                        )
                    exact_names = {
                        value.strip()
                        for value in proof_output.splitlines()
                        if value.strip()
                    }
                    if name in exact_names:
                        raise RuntimeError(
                            "Could not inspect an API account container",
                        )
                    self._containers.pop(project_id, None)
                    continue
                mounted = (
                    str(Path(mounted_root.strip()).resolve(strict=False))
                    if mounted_root.strip()
                    else ""
                )
                if mounted != source:
                    continue
                stop_code, _ = await self._run([
                    "docker", "stop", "-t", "10", name,
                ])
                remove_code, _ = await self._run([
                    "docker", "rm", "-f", name,
                ])
                if stop_code != 0 or remove_code != 0:
                    raise RuntimeError(
                        "Could not detach an API account container mount",
                    )
                self._containers.pop(project_id, None)
                git_dir = self._git_dirs.pop(project_id, None)
                if git_dir and os.path.isdir(git_dir):
                    shutil.rmtree(git_dir, ignore_errors=True)
                stopped += 1
        return stopped

    async def cleanup_all(self):
        for pid in list(self._containers.keys()):
            try:
                await self.stop_container(pid)
            except Exception:
                pass


async def is_shared_project(project_id: int | None, db_factory) -> bool:
    """Check legacy cross-CCM Project sharing behind its writer fence.

    TeamProjectShare is an in-process ACL and deliberately does not select the
    legacy container/shadow trust boundary.
    """
    if not project_id:
        return False
    from backend.services.project_share_admission import (
        lock_project_share_authority,
        project_has_active_share,
    )

    async with db_factory() as db:
        # The lock pairs with the 0 -> >0 share transition fence. If sharing
        # won first, launch observes the committed grant; if launch already
        # published its reservation, the share path returns 409 instead.
        await lock_project_share_authority(db, project_id)
        return await project_has_active_share(db, project_id)


async def build_sandbox_image():
    """Build the ccm-sandbox Docker image if it doesn't exist."""
    proc = await asyncio.create_subprocess_exec(
        "docker", "images", "-q", SANDBOX_IMAGE,
        stdout=asyncio.subprocess.PIPE,
    )
    out, _ = await proc.communicate()
    if out.strip():
        return

    logger.info("Building sandbox image %s ...", SANDBOX_IMAGE)
    dockerfile = """\
FROM node:22-slim
RUN apt-get update && apt-get install -y --no-install-recommends \\
    curl git ssh-client ca-certificates python3 \\
    && rm -rf /var/lib/apt/lists/*
RUN npm install -g @anthropic-ai/claude-code
RUN groupadd -g 1000 sandbox 2>/dev/null; useradd -m -u 1000 -g 1000 sandbox 2>/dev/null; exit 0
USER 1000
WORKDIR /workspace
"""
    build_dir = "/tmp/ccm-docker-build"
    os.makedirs(build_dir, exist_ok=True)
    dockerfile_path = os.path.join(build_dir, "Dockerfile")
    with open(dockerfile_path, "w") as f:
        f.write(dockerfile)

    proc = await asyncio.create_subprocess_exec(
        "docker", "build", "-t", SANDBOX_IMAGE, build_dir,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    out, _ = await proc.communicate()
    if proc.returncode != 0:
        logger.error("Failed to build sandbox image: %s", out.decode()[:1000])
        raise RuntimeError("Failed to build ccm-sandbox image")
    logger.info("Sandbox image built successfully")
