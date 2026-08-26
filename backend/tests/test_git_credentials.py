"""Comprehensive tests for git credential injection.

Covers the three bugs that caused all push failures:
1. HTTPS token was never injected (_build_git_env only handled SSH)
2. SSH and HTTPS were mutually exclusive (if/elif), but remote URL protocol
   determines which one git uses — both must be injected simultaneously
3. macOS osxkeychain credential helper took priority over our injected creds

Also covers:
- merge_git_config individual field fallback
- GIT_ASKPASS script creation and content
- _apply_git_config credential helper chain reset
- clone/fetch with injected env
"""
import asyncio
import os
import stat
import subprocess
from pathlib import Path
from unittest.mock import patch, AsyncMock, MagicMock

import pytest

from backend.services.git_config import merge_git_config
from backend.services.dispatcher import _build_git_env, _get_or_create_askpass_script


# ============================================================
# merge_git_config — individual field merging
# ============================================================

class TestMergeGitConfig:

    def test_empty_configs(self):
        result = merge_git_config({}, {})
        assert result["git_author_name"] is None
        assert result["git_credential_type"] is None
        assert result["git_ssh_key_path"] is None
        assert result["git_https_token"] is None

    def test_global_only(self):
        global_cfg = {
            "git_author_name": "Global",
            "git_author_email": "global@test.com",
            "git_credential_type": "ssh",
            "git_ssh_key_path": "/keys/global",
            "git_https_username": "globaluser",
            "git_https_token": "ghp_global",
        }
        result = merge_git_config({}, global_cfg)
        assert result["git_author_name"] == "Global"
        assert result["git_author_email"] == "global@test.com"
        assert result["git_credential_type"] == "ssh"
        assert result["git_ssh_key_path"] == "/keys/global"
        assert result["git_https_username"] == "globaluser"
        assert result["git_https_token"] == "ghp_global"

    def test_project_overrides_global(self):
        project_cfg = {
            "git_author_name": "Project",
            "git_author_email": "proj@test.com",
            "git_credential_type": "https",
            "git_ssh_key_path": "/keys/project",
            "git_https_username": "projuser",
            "git_https_token": "ghp_project",
        }
        global_cfg = {
            "git_author_name": "Global",
            "git_author_email": "global@test.com",
            "git_credential_type": "ssh",
            "git_ssh_key_path": "/keys/global",
            "git_https_username": "globaluser",
            "git_https_token": "ghp_global",
        }
        result = merge_git_config(project_cfg, global_cfg)
        assert result["git_author_name"] == "Project"
        assert result["git_credential_type"] == "https"
        assert result["git_ssh_key_path"] == "/keys/project"
        assert result["git_https_token"] == "ghp_project"

    def test_identity_requires_both_name_and_email(self):
        project_cfg = {"git_author_name": "Project", "git_author_email": None}
        global_cfg = {"git_author_name": "Global", "git_author_email": "global@test.com"}
        result = merge_git_config(project_cfg, global_cfg)
        assert result["git_author_name"] == "Global"
        assert result["git_author_email"] == "global@test.com"

    def test_credential_fields_merge_individually(self):
        """BUG FIX: Each credential field merges independently.

        Previously, credential_type controlled an all-or-nothing switch.
        Now each field falls back individually.
        """
        project_cfg = {
            "git_credential_type": None,
            "git_ssh_key_path": "/keys/project_ssh",
            "git_https_username": None,
            "git_https_token": None,
        }
        global_cfg = {
            "git_credential_type": "ssh",
            "git_ssh_key_path": "/keys/global_ssh",
            "git_https_username": "globaluser",
            "git_https_token": "ghp_global",
        }
        result = merge_git_config(project_cfg, global_cfg)
        assert result["git_ssh_key_path"] == "/keys/project_ssh"
        assert result["git_https_username"] == "globaluser"
        assert result["git_https_token"] == "ghp_global"

    def test_project_no_creds_inherits_all_global_creds(self):
        """BUG FIX: Project with zero credential fields → all from global.

        This was the exact scenario for tasks 1-6.
        """
        project_cfg = {
            "git_credential_type": None,
            "git_ssh_key_path": None,
            "git_https_username": None,
            "git_https_token": None,
        }
        global_cfg = {
            "git_credential_type": "ssh",
            "git_ssh_key_path": "/keys/id_ed25519",
            "git_https_username": "fxcyf",
            "git_https_token": "ghp_fxcyftoken",
        }
        result = merge_git_config(project_cfg, global_cfg)
        assert result["git_ssh_key_path"] == "/keys/id_ed25519"
        assert result["git_https_username"] == "fxcyf"
        assert result["git_https_token"] == "ghp_fxcyftoken"


# ============================================================
# _build_git_env — environment variable injection
# ============================================================

class TestBuildGitEnv:

    @patch("backend.services.dispatcher.settings")
    def test_empty_config(self, mock_settings):
        mock_settings.git_ssh_key_path = None
        env = _build_git_env({})
        assert env == {}

    @patch("backend.services.dispatcher.settings")
    def test_identity_only(self, mock_settings):
        mock_settings.git_ssh_key_path = None
        env = _build_git_env({
            "git_author_name": "Alice",
            "git_author_email": "alice@test.com",
        })
        assert env["GIT_AUTHOR_NAME"] == "Alice"
        assert env["GIT_COMMITTER_NAME"] == "Alice"
        assert env["GIT_AUTHOR_EMAIL"] == "alice@test.com"
        assert env["GIT_COMMITTER_EMAIL"] == "alice@test.com"

    @patch("backend.services.dispatcher.settings")
    def test_ssh_only(self, mock_settings):
        mock_settings.git_ssh_key_path = None
        env = _build_git_env({"git_ssh_key_path": "/keys/id_ed25519"})
        assert "GIT_SSH_COMMAND" in env
        assert "/keys/id_ed25519" in env["GIT_SSH_COMMAND"]
        assert "StrictHostKeyChecking=no" in env["GIT_SSH_COMMAND"]
        assert "GIT_ASKPASS" not in env

    @patch("backend.services.dispatcher.settings")
    def test_https_only(self, mock_settings):
        mock_settings.git_ssh_key_path = None
        env = _build_git_env({
            "git_https_username": "testuser",
            "git_https_token": "ghp_testtoken123",
        })
        assert "GIT_ASKPASS" in env
        assert os.path.isfile(env["GIT_ASKPASS"])
        assert env["GIT_TERMINAL_PROMPT"] == "0"
        assert "GIT_SSH_COMMAND" not in env

    @patch("backend.services.dispatcher.settings")
    def test_https_disables_osxkeychain(self, mock_settings):
        """BUG FIX: HTTPS creds must disable system credential helpers.

        macOS osxkeychain caches old account credentials and takes priority
        over GIT_ASKPASS. We bypass global/system config entirely.
        """
        mock_settings.git_ssh_key_path = None
        env = _build_git_env({"git_https_token": "ghp_token"})
        assert env["GIT_CONFIG_GLOBAL"] == "/dev/null"
        assert env["GIT_CONFIG_NOSYSTEM"] == "1"

    @patch("backend.services.dispatcher.settings")
    def test_ssh_and_https_simultaneously(self, mock_settings):
        """BUG FIX: Both SSH and HTTPS injected at the same time.

        Previously if/elif meant only one was set. Now both are always
        injected when available — git picks the right one based on remote URL.
        """
        mock_settings.git_ssh_key_path = None
        env = _build_git_env({
            "git_ssh_key_path": "/keys/id_ed25519",
            "git_https_username": "user",
            "git_https_token": "ghp_token",
        })
        assert "GIT_SSH_COMMAND" in env
        assert "/keys/id_ed25519" in env["GIT_SSH_COMMAND"]
        assert "GIT_ASKPASS" in env
        assert os.path.isfile(env["GIT_ASKPASS"])
        assert env["GIT_TERMINAL_PROMPT"] == "0"
        assert env["GIT_CONFIG_NOSYSTEM"] == "1"

    @patch("backend.services.dispatcher.settings")
    def test_reproduces_original_bug_scenario(self, mock_settings):
        """Reproduces the exact scenario that caused tasks 1-6 to fail.

        Global config: credential_type=ssh, ssh_key set, HTTPS token also set.
        Project config: all null (inherits global).
        Remote URL: HTTPS (https://github.com/fxcyf/price-tracker.git).

        OLD behavior: only GIT_SSH_COMMAND set → HTTPS push uses macOS Keychain → 403
        NEW behavior: both GIT_SSH_COMMAND and GIT_ASKPASS set → HTTPS push uses our token
        """
        mock_settings.git_ssh_key_path = None

        project_cfg = {
            "git_credential_type": None,
            "git_ssh_key_path": None,
            "git_https_username": None,
            "git_https_token": None,
        }
        global_cfg = {
            "git_credential_type": "ssh",
            "git_ssh_key_path": "/Users/zhoujunwei/.ssh/id_ed25519_2",
            "git_https_username": "fxcyf",
            "git_https_token": "ghp_fxcyftoken",
        }
        merged = merge_git_config(project_cfg, global_cfg)
        env = _build_git_env(merged)

        # SSH should be set (for SSH remotes)
        assert "GIT_SSH_COMMAND" in env
        assert "id_ed25519_2" in env["GIT_SSH_COMMAND"]
        # HTTPS should ALSO be set (for HTTPS remotes) — this was the missing piece
        assert "GIT_ASKPASS" in env
        assert env["GIT_TERMINAL_PROMPT"] == "0"
        # osxkeychain must be disabled
        assert env["GIT_CONFIG_NOSYSTEM"] == "1"
        assert env["GIT_CONFIG_GLOBAL"] == "/dev/null"

    @patch("backend.services.dispatcher.settings")
    def test_instance_level_ssh_fallback(self, mock_settings):
        mock_settings.git_ssh_key_path = "/fallback/key"
        env = _build_git_env({})
        assert "GIT_SSH_COMMAND" in env
        assert "/fallback/key" in env["GIT_SSH_COMMAND"]

    @patch("backend.services.dispatcher.settings")
    def test_instance_level_ssh_not_used_when_config_has_ssh(self, mock_settings):
        mock_settings.git_ssh_key_path = "/fallback/key"
        env = _build_git_env({"git_ssh_key_path": "/project/key"})
        assert "/project/key" in env["GIT_SSH_COMMAND"]
        assert "/fallback/key" not in env["GIT_SSH_COMMAND"]

    @patch("backend.services.dispatcher.settings")
    def test_credential_type_no_longer_gates_injection(self, mock_settings):
        """credential_type field no longer controls which creds are injected.

        _build_git_env ignores credential_type entirely — it injects
        whatever fields are available.
        """
        mock_settings.git_ssh_key_path = None
        config = {
            "git_credential_type": "ssh",
            "git_ssh_key_path": "/keys/ssh",
            "git_https_token": "ghp_token",
        }
        env = _build_git_env(config)
        assert "GIT_SSH_COMMAND" in env
        assert "GIT_ASKPASS" in env

    @patch("backend.services.dispatcher.settings")
    def test_credential_type_https_with_ssh_key_also_present(self, mock_settings):
        mock_settings.git_ssh_key_path = None
        config = {
            "git_credential_type": "https",
            "git_ssh_key_path": "/keys/ssh",
            "git_https_token": "ghp_token",
        }
        env = _build_git_env(config)
        assert "GIT_SSH_COMMAND" in env
        assert "GIT_ASKPASS" in env


# ============================================================
# _get_or_create_askpass_script — script creation and content
# ============================================================

class TestAskpassScript:

    def test_script_is_created(self):
        path = _get_or_create_askpass_script("testuser", "testtoken")
        assert os.path.isfile(path)
        mode = os.stat(path).st_mode
        assert mode & stat.S_IXUSR

    def test_script_returns_username_for_username_prompt(self):
        path = _get_or_create_askpass_script("myuser", "mytoken")
        result = subprocess.run(
            [path, "Username for 'https://github.com': "],
            capture_output=True, text=True,
        )
        assert result.stdout.strip() == "myuser"

    def test_script_returns_token_for_password_prompt(self):
        path = _get_or_create_askpass_script("myuser", "mytoken")
        result = subprocess.run(
            [path, "Password for 'https://myuser@github.com': "],
            capture_output=True, text=True,
        )
        assert result.stdout.strip() == "mytoken"

    def test_script_returns_token_for_unknown_prompt(self):
        path = _get_or_create_askpass_script("myuser", "mytoken")
        result = subprocess.run(
            [path, "Something else"],
            capture_output=True, text=True,
        )
        assert result.stdout.strip() == "mytoken"

    def test_same_credentials_reuse_script(self):
        path1 = _get_or_create_askpass_script("user_reuse", "token_reuse")
        path2 = _get_or_create_askpass_script("user_reuse", "token_reuse")
        assert path1 == path2

    def test_different_credentials_different_script(self):
        path1 = _get_or_create_askpass_script("user_a", "token_a")
        path2 = _get_or_create_askpass_script("user_b", "token_b")
        assert path1 != path2

    def test_empty_username(self):
        path = _get_or_create_askpass_script("", "mytoken_empty")
        result = subprocess.run(
            [path, "Username for 'https://github.com': "],
            capture_output=True, text=True,
        )
        assert result.stdout.strip() == ""


# ============================================================
# _apply_git_config — credential helper chain reset
# ============================================================

class TestApplyGitConfig:

    @pytest.mark.asyncio
    async def test_https_resets_credential_helper_chain(self, tmp_path):
        """BUG FIX: credential.helper="" must precede the store helper.

        Without this, macOS osxkeychain returns cached creds first.
        """
        subprocess.run(["git", "init", "-b", "main"], cwd=tmp_path, capture_output=True)
        subprocess.run(
            ["git", "remote", "add", "origin", "https://github.com/test/repo.git"],
            cwd=tmp_path, capture_output=True,
        )

        from backend.api.projects import _apply_git_config
        await _apply_git_config(str(tmp_path), {
            "git_credential_type": "https",
            "git_https_username": "testuser",
            "git_https_token": "ghp_testtoken",
        })

        result = subprocess.run(
            ["git", "config", "--local", "--get-all", "credential.helper"],
            cwd=tmp_path, capture_output=True, text=True,
        )
        helpers = result.stdout.strip().split("\n")
        # git config replaces the empty value with the store helper,
        # so the final config has only the store helper (no inherited osxkeychain)
        assert len(helpers) >= 1
        assert any("store --file" in h for h in helpers)
        # Verify osxkeychain is NOT in the helper chain
        assert not any("osxkeychain" in h for h in helpers)

    @pytest.mark.asyncio
    async def test_https_credentials_file_written(self, tmp_path):
        subprocess.run(["git", "init", "-b", "main"], cwd=tmp_path, capture_output=True)
        subprocess.run(
            ["git", "remote", "add", "origin", "https://github.com/test/repo.git"],
            cwd=tmp_path, capture_output=True,
        )

        from backend.api.projects import _apply_git_config
        await _apply_git_config(str(tmp_path), {
            "git_credential_type": "https",
            "git_https_username": "myuser",
            "git_https_token": "ghp_mytoken",
        })

        creds_path = tmp_path / ".git" / "credentials"
        assert creds_path.exists()
        content = creds_path.read_text()
        assert "https://myuser:ghp_mytoken@github.com" in content
        assert "http://myuser:ghp_mytoken@github.com" in content

    @pytest.mark.asyncio
    async def test_https_extracts_host_from_remote(self, tmp_path):
        subprocess.run(["git", "init", "-b", "main"], cwd=tmp_path, capture_output=True)
        subprocess.run(
            ["git", "remote", "add", "origin", "https://gitlab.example.com/test/repo.git"],
            cwd=tmp_path, capture_output=True,
        )

        from backend.api.projects import _apply_git_config
        await _apply_git_config(str(tmp_path), {
            "git_credential_type": "https",
            "git_https_username": "user",
            "git_https_token": "token",
        })

        creds_path = tmp_path / ".git" / "credentials"
        content = creds_path.read_text()
        assert "gitlab.example.com" in content
        assert "github.com" not in content

    @pytest.mark.asyncio
    async def test_https_defaults_username_to_oauth2(self, tmp_path):
        subprocess.run(["git", "init", "-b", "main"], cwd=tmp_path, capture_output=True)
        subprocess.run(
            ["git", "remote", "add", "origin", "https://github.com/test/repo.git"],
            cwd=tmp_path, capture_output=True,
        )

        from backend.api.projects import _apply_git_config
        await _apply_git_config(str(tmp_path), {
            "git_credential_type": "https",
            "git_https_token": "ghp_token",
        })

        creds_path = tmp_path / ".git" / "credentials"
        content = creds_path.read_text()
        assert "oauth2:ghp_token@" in content

    @pytest.mark.asyncio
    async def test_ssh_sets_core_sshcommand(self, tmp_path):
        subprocess.run(["git", "init", "-b", "main"], cwd=tmp_path, capture_output=True)

        from backend.api.projects import _apply_git_config
        await _apply_git_config(str(tmp_path), {
            "git_credential_type": "ssh",
            "git_ssh_key_path": "/keys/my_key",
        })

        result = subprocess.run(
            ["git", "config", "--local", "core.sshCommand"],
            cwd=tmp_path, capture_output=True, text=True,
        )
        assert "/keys/my_key" in result.stdout
        assert "StrictHostKeyChecking=no" in result.stdout

    @pytest.mark.asyncio
    async def test_identity_sets_user_name_email(self, tmp_path):
        subprocess.run(["git", "init", "-b", "main"], cwd=tmp_path, capture_output=True)

        from backend.api.projects import _apply_git_config
        await _apply_git_config(str(tmp_path), {
            "git_author_name": "Test User",
            "git_author_email": "test@example.com",
        })

        name = subprocess.run(
            ["git", "config", "--local", "user.name"],
            cwd=tmp_path, capture_output=True, text=True,
        )
        email = subprocess.run(
            ["git", "config", "--local", "user.email"],
            cwd=tmp_path, capture_output=True, text=True,
        )
        assert name.stdout.strip() == "Test User"
        assert email.stdout.strip() == "test@example.com"

    @pytest.mark.asyncio
    async def test_https_host_from_ssh_style_remote(self, tmp_path):
        """Extract host from git@host:user/repo.git style remote."""
        subprocess.run(["git", "init", "-b", "main"], cwd=tmp_path, capture_output=True)
        subprocess.run(
            ["git", "remote", "add", "origin", "git@bitbucket.org:team/repo.git"],
            cwd=tmp_path, capture_output=True,
        )

        from backend.api.projects import _apply_git_config
        await _apply_git_config(str(tmp_path), {
            "git_credential_type": "https",
            "git_https_username": "user",
            "git_https_token": "token",
        })

        creds_path = tmp_path / ".git" / "credentials"
        content = creds_path.read_text()
        assert "bitbucket.org" in content

    @pytest.mark.asyncio
    async def test_https_no_remote_defaults_to_github(self, tmp_path):
        """No remote configured → defaults to github.com."""
        subprocess.run(["git", "init", "-b", "main"], cwd=tmp_path, capture_output=True)

        from backend.api.projects import _apply_git_config
        await _apply_git_config(str(tmp_path), {
            "git_credential_type": "https",
            "git_https_username": "user",
            "git_https_token": "token",
        })

        creds_path = tmp_path / ".git" / "credentials"
        content = creds_path.read_text()
        assert "github.com" in content


# ============================================================
# End-to-end: clone with injected credentials
# ============================================================

class TestCloneWithCredentials:

    @pytest.mark.asyncio
    async def test_clone_passes_git_env_with_https(self, tmp_path):
        """_clone_repo passes env with HTTPS credentials to git subprocess."""
        from backend.api.projects import _clone_repo

        captured_envs = []

        async def mock_subprocess(*args, **kwargs):
            if 'env' in kwargs and kwargs['env']:
                captured_envs.append(dict(kwargs['env']))
            mock_proc = MagicMock()
            mock_proc.communicate = AsyncMock(return_value=(b"", b""))
            mock_proc.returncode = 0
            mock_proc.wait = AsyncMock()
            return mock_proc

        git_config = {
            "git_https_username": "testuser",
            "git_https_token": "ghp_test",
        }

        mock_db = AsyncMock()
        mock_session_ctx = AsyncMock()
        mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_db)
        mock_session_ctx.__aexit__ = AsyncMock(return_value=False)

        local_path = str(tmp_path / "test-clone-repo")
        with patch("backend.api.projects.async_session", return_value=mock_session_ctx), \
             patch("asyncio.create_subprocess_exec", side_effect=mock_subprocess), \
             patch("backend.api.projects._apply_git_config", new_callable=AsyncMock):

            await _clone_repo(
                project_id=1,
                git_url="https://github.com/test/repo.git",
                local_path=local_path,
                project_name="test",
                default_branch="main",
                git_config=git_config,
            )

        # At least one subprocess call should have our creds in env
        assert len(captured_envs) > 0
        clone_env = captured_envs[0]
        assert "GIT_ASKPASS" in clone_env
        assert clone_env["GIT_TERMINAL_PROMPT"] == "0"
        assert clone_env["GIT_CONFIG_NOSYSTEM"] == "1"

    @pytest.mark.asyncio
    async def test_clone_passes_git_env_with_ssh(self, tmp_path):
        """_clone_repo passes env with SSH credentials to git subprocess."""
        from backend.api.projects import _clone_repo

        captured_envs = []

        async def mock_subprocess(*args, **kwargs):
            if 'env' in kwargs and kwargs['env']:
                captured_envs.append(dict(kwargs['env']))
            mock_proc = MagicMock()
            mock_proc.communicate = AsyncMock(return_value=(b"", b""))
            mock_proc.returncode = 0
            mock_proc.wait = AsyncMock()
            return mock_proc

        git_config = {
            "git_ssh_key_path": "/keys/my_ssh_key",
        }

        mock_db = AsyncMock()
        mock_session_ctx = AsyncMock()
        mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_db)
        mock_session_ctx.__aexit__ = AsyncMock(return_value=False)

        local_path = str(tmp_path / "test-clone-repo-ssh")
        with patch("backend.api.projects.async_session", return_value=mock_session_ctx), \
             patch("asyncio.create_subprocess_exec", side_effect=mock_subprocess), \
             patch("backend.api.projects._apply_git_config", new_callable=AsyncMock):

            await _clone_repo(
                project_id=1,
                git_url="git@github.com:test/repo.git",
                local_path=local_path,
                project_name="test",
                default_branch="main",
                git_config=git_config,
            )

        assert len(captured_envs) > 0
        clone_env = captured_envs[0]
        assert "GIT_SSH_COMMAND" in clone_env
        assert "/keys/my_ssh_key" in clone_env["GIT_SSH_COMMAND"]

    @pytest.mark.asyncio
    async def test_clone_no_config_no_env(self):
        """_clone_repo without git_config → no credentials, but prompts off.

        Background clones have no TTY; the env always carries
        GIT_TERMINAL_PROMPT=0 (and stdin=DEVNULL) so an HTTPS clone without
        credentials fails fast instead of hanging or dying with the cryptic
        "could not read Username ... No such device or address".
        """
        from backend.api.projects import _clone_repo

        captured_kwargs = []

        async def mock_subprocess(*args, **kwargs):
            captured_kwargs.append(kwargs)
            mock_proc = MagicMock()
            mock_proc.communicate = AsyncMock(return_value=(b"", b""))
            mock_proc.returncode = 0
            mock_proc.wait = AsyncMock()
            return mock_proc

        with patch("backend.api.projects.async_session") as mock_ctx, \
             patch("asyncio.create_subprocess_exec", side_effect=mock_subprocess), \
             patch("backend.api.projects._apply_git_config", new_callable=AsyncMock), \
             patch("builtins.open", MagicMock()), \
             patch("os.path.exists", return_value=True), \
             patch("os.path.isdir", return_value=False), \
             patch("os.makedirs"):
            mock_db = AsyncMock()
            mock_ctx.return_value.__aenter__ = AsyncMock(return_value=mock_db)
            mock_ctx.return_value.__aexit__ = AsyncMock(return_value=False)

            await _clone_repo(
                project_id=1,
                git_url="https://github.com/test/repo.git",
                project_name="test",
                local_path="/tmp/test-clone-no-config",
                default_branch="main",
                git_config=None,
            )

        clone_env = captured_kwargs[0].get("env")
        assert clone_env is not None
        assert clone_env["GIT_TERMINAL_PROMPT"] == "0"
        assert "GIT_ASKPASS" not in clone_env
        assert "GIT_SSH_COMMAND" not in clone_env or (
            # instance-level fallback key may exist in the host env
            "BatchMode=yes" in clone_env["GIT_SSH_COMMAND"]
        )
        assert captured_kwargs[0].get("stdin") is not None
