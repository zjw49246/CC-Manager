"""Tests for MCP config generation and cleanup."""
import json
import tempfile
import tomllib
from pathlib import Path

import pytest

from backend.config import settings
from backend.mcp import (
    ccm_monitor_agent_server,
    ccm_skills_server,
    ccm_sub_agent_server,
    ccm_workspace_review_server,
)
from backend.services import mcp_config
from backend.services import internal_api_endpoint
from backend.services.mcp_config import (
    CCM_BROWSER_REVIEW_TOOLS,
    CCM_FRONTEND_REVIEW_TOOLS,
    CCM_MONITOR_AGENT_TOOLS,
    CCM_SKILLS_TOOLS,
    CCM_SUB_AGENT_CONTROLLER_TOOLS,
    CCM_SUB_AGENT_TOOLS,
    CCM_WORKSPACE_REVIEW_TOOLS,
    McpServerSpec,
    build_mcp_server_specs,
    build_browser_review_mcp_server_specs,
    build_frontend_review_mcp_server_specs,
    build_monitor_agent_mcp_server_specs,
    build_sub_agent_controller_mcp_server_specs,
    build_sub_agent_mcp_server_specs,
    build_workspace_review_mcp_server_specs,
    cleanup_mcp_config,
    cleanup_monitor_agent_mcp_config,
    cleanup_sub_agent_mcp_config,
    generate_mcp_config,
    generate_monitor_agent_mcp_config,
    generate_sub_agent_mcp_config,
    render_claude_mcp_config,
    render_codex_exec_config_args,
    render_codex_mcp_config,
)


EXPECTED_MAIN_TOOLS = (
    "ccm_command_help",
    "ccm_read_skill",
    "ccm_read_user_skill",
    "ccm_create_skill",
    "ccm_distill",
    "ccm_enable_skill",
    "ccm_disable_skill",
    "create_monitor",
    "check_monitors",
    "stop_monitor",
    "create_sub_agent",
    "check_sub_agents",
    "stop_sub_agent",
)
EXPECTED_MONITOR_TOOLS = (
    "report_status",
    "mark_complete",
    "get_context",
)
EXPECTED_SUB_AGENT_TOOLS = (
    "report_progress",
    "submit_result",
    "get_context",
)


def _assert_ccm_skills_config(path, task_id: int, api_base: str):
    """ccm_skills server 始终包含，参数齐全。"""
    assert path is not None
    assert path.exists()

    config = json.loads(path.read_text())
    assert "mcpServers" in config
    assert "ccm_skills" in config["mcpServers"]

    server = config["mcpServers"]["ccm_skills"]
    assert Path(server["command"]).is_file()
    assert "--task-id" in server["args"]
    assert str(task_id) in server["args"]
    assert "--api-base" in server["args"]
    assert api_base in server["args"]
    assert "-m" in server["args"]
    assert "backend.mcp.ccm_skills_server" in server["args"]


def test_generate_mcp_config_none_skills_still_includes_ccm_skills():
    """skills=None 时也返回配置：ccm_skills 提供 $help 等默认命令。"""
    path = generate_mcp_config(1, None, api_base="http://localhost:8000")
    _assert_ccm_skills_config(path, 1, "http://localhost:8000")
    path.unlink(missing_ok=True)


def test_generate_mcp_config_empty_skills_still_includes_ccm_skills():
    """skills={} 时也返回配置（ccm_skills 始终包含）。"""
    path = generate_mcp_config(1, {}, api_base="http://localhost:8000")
    _assert_ccm_skills_config(path, 1, "http://localhost:8000")
    path.unlink(missing_ok=True)


def test_generate_mcp_config_skills_do_not_add_extra_servers():
    """普通 skill 不增加服务；固定的 Task 工具服务保持存在。"""
    path = generate_mcp_config(1, {"worker": True, "monitor": True}, api_base="http://localhost:8000")
    config = json.loads(path.read_text())
    assert set(config["mcpServers"].keys()) == {
        "ccm_skills",
        "ccm_frontend_review",
        "ccm_workspace_review",
    }
    path.unlink(missing_ok=True)


def test_browser_review_adds_required_task_scoped_server():
    specs = build_mcp_server_specs(
        73,
        {"browser-review": "job-abc"},
        api_base="http://127.0.0.1:8795",
        provider="codex",
    )

    assert [spec.name for spec in specs] == ["ccm_browser_review"]
    browser_spec = specs[0]
    assert browser_spec == build_browser_review_mcp_server_specs(
        "job-abc",
        api_base="http://127.0.0.1:8795",
    )[0]
    assert browser_spec.required is True
    assert browser_spec.enabled_tools == CCM_BROWSER_REVIEW_TOOLS
    assert "backend.mcp.ccm_browser_review_server" in browser_spec.args
    assert "--job-id" in browser_spec.args
    assert "job-abc" in browser_spec.args


def test_ordinary_task_adds_repeatable_frontend_review_server():
    specs = build_mcp_server_specs(
        73,
        api_base="http://127.0.0.1:8795",
        provider="codex",
    )

    assert [spec.name for spec in specs] == [
        "ccm_skills",
        "ccm_frontend_review",
        "ccm_workspace_review",
    ]
    frontend_spec = specs[1]
    assert frontend_spec == build_frontend_review_mcp_server_specs(
        73,
        api_base="http://127.0.0.1:8795",
    )[0]
    assert frontend_spec.enabled_tools == CCM_FRONTEND_REVIEW_TOOLS
    assert frontend_spec.enabled_tools == ("start_review", "check_review", "stop_review")
    assert "browser_open" not in frontend_spec.enabled_tools
    assert "--task-id" in frontend_spec.args
    assert "73" in frontend_spec.args
    workspace_spec = specs[2]
    assert workspace_spec == build_workspace_review_mcp_server_specs(
        73,
        api_base="http://127.0.0.1:8795",
    )[0]
    assert workspace_spec.enabled_tools == CCM_WORKSPACE_REVIEW_TOOLS
    assert workspace_spec.enabled_tools == (
        "workspace_review_capabilities",
        "test_current_changes",
        "check_current_changes_review",
        "stop_current_changes_review",
        "test_git_target",
        "compare_test_runs",
    )


def test_generate_mcp_config_monitor_enabled():
    path = generate_mcp_config(99, {"monitor": True}, api_base="http://test:8000")
    _assert_ccm_skills_config(path, 99, "http://test:8000")
    path.unlink(missing_ok=True)


def test_generate_mcp_config_file_path():
    path = generate_mcp_config(42, {"monitor": True}, api_base="http://localhost:8000")
    expected = Path(tempfile.gettempdir()) / "ccm_mcp_42.json"
    assert path == expected
    path.unlink(missing_ok=True)


def test_cleanup_mcp_config():
    path = generate_mcp_config(77, {"monitor": True}, api_base="http://localhost:8000")
    assert path.exists()
    cleanup_mcp_config(77)
    assert not path.exists()


def test_cleanup_mcp_config_missing_file():
    cleanup_mcp_config(999999)


def _set_spec_snapshot_runtime(monkeypatch):
    monkeypatch.setattr(mcp_config, "_CCM_ROOT", "/srv/ccm")
    monkeypatch.setattr(
        mcp_config,
        "_VENV_PYTHON",
        "/srv/ccm/.venv/bin/python3",
    )
    monkeypatch.setattr(settings, "auth_token", "secret-token")


def test_main_mcp_server_spec_snapshot(monkeypatch):
    _set_spec_snapshot_runtime(monkeypatch)

    specs = build_mcp_server_specs(
        42,
        {"monitor": True},
        api_base="http://manager:8321",
    )
    assert specs[0] == (
        McpServerSpec(
            name="ccm_skills",
            command="/srv/ccm/.venv/bin/python3",
            args=(
                "-P",
                "-m",
                "backend.mcp.ccm_skills_server",
                "--task-id",
                "42",
                "--api-base",
                "http://manager:8321",
                "--auth-token",
                "secret-token",
            ),
            cwd="/srv/ccm",
            env={"PYTHONPATH": "/srv/ccm"},
            required=True,
            enabled_tools=EXPECTED_MAIN_TOOLS,
            default_tools_approval_mode="approve",
            startup_timeout_sec=10.0,
            tool_timeout_sec=60.0,
        )
    )
    assert specs[1] == build_frontend_review_mcp_server_specs(
        42,
        api_base="http://manager:8321",
    )[0]
    assert CCM_SKILLS_TOOLS == EXPECTED_MAIN_TOOLS


def test_monitor_agent_mcp_server_spec_snapshot(monkeypatch):
    _set_spec_snapshot_runtime(monkeypatch)

    assert build_monitor_agent_mcp_server_specs(
        7,
        42,
        api_base="http://manager:8321",
    ) == (
        McpServerSpec(
            name="ccm_monitor_agent",
            command="/srv/ccm/.venv/bin/python3",
            args=(
                "-P",
                "-m",
                "backend.mcp.ccm_monitor_agent_server",
                "--monitor-session-id",
                "7",
                "--task-id",
                "42",
                "--api-base",
                "http://manager:8321",
                "--auth-token",
                "secret-token",
            ),
            cwd="/srv/ccm",
            env={"PYTHONPATH": "/srv/ccm"},
            required=True,
            enabled_tools=EXPECTED_MONITOR_TOOLS,
            default_tools_approval_mode="approve",
            startup_timeout_sec=10.0,
            tool_timeout_sec=60.0,
        ),
    )
    assert CCM_MONITOR_AGENT_TOOLS == EXPECTED_MONITOR_TOOLS


def test_monitor_agent_mcp_spec_carries_exact_turn_generation(monkeypatch):
    _set_spec_snapshot_runtime(monkeypatch)

    spec = build_monitor_agent_mcp_server_specs(
        7,
        42,
        api_base="http://manager:8321",
        turn_generation=9,
    )[0]

    assert spec.args[3:9] == (
        "--monitor-session-id",
        "7",
        "--task-id",
        "42",
        "--turn-generation",
        "9",
    )


def test_sub_agent_mcp_server_spec_snapshot(monkeypatch):
    _set_spec_snapshot_runtime(monkeypatch)

    assert build_sub_agent_mcp_server_specs(
        9,
        42,
        api_base="http://manager:8321",
    ) == (
        McpServerSpec(
            name="ccm_sub_agent",
            command="/srv/ccm/.venv/bin/python3",
            args=(
                "-P",
                "-m",
                "backend.mcp.ccm_sub_agent_server",
                "--sub-agent-session-id",
                "9",
                "--task-id",
                "42",
                "--api-base",
                "http://manager:8321",
                "--auth-token",
                "secret-token",
            ),
            cwd="/srv/ccm",
            env={"PYTHONPATH": "/srv/ccm"},
            required=True,
            enabled_tools=EXPECTED_SUB_AGENT_TOOLS,
            default_tools_approval_mode="approve",
            startup_timeout_sec=10.0,
            tool_timeout_sec=60.0,
        ),
    )
    assert CCM_SUB_AGENT_TOOLS == EXPECTED_SUB_AGENT_TOOLS


def test_sub_agent_controller_spec_is_narrow_and_required(monkeypatch):
    _set_spec_snapshot_runtime(monkeypatch)

    (spec,) = build_sub_agent_controller_mcp_server_specs(
        42,
        api_base="http://manager:8321",
    )

    assert spec.name == "ccm_skills"
    assert spec.required is True
    assert spec.enabled_tools == CCM_SUB_AGENT_CONTROLLER_TOOLS
    assert spec.enabled_tools == (
        "ccm_read_skill",
        "create_sub_agent",
        "check_sub_agents",
        "stop_sub_agent",
    )
    assert "create_monitor" not in spec.enabled_tools


@pytest.mark.parametrize(
    ("server_module", "enabled_tools"),
    [
        (ccm_skills_server, CCM_SKILLS_TOOLS),
        (ccm_monitor_agent_server, CCM_MONITOR_AGENT_TOOLS),
        (ccm_sub_agent_server, CCM_SUB_AGENT_TOOLS),
        (ccm_workspace_review_server, CCM_WORKSPACE_REVIEW_TOOLS),
    ],
)
def test_spec_enabled_tools_match_registered_server_tools(
    server_module,
    enabled_tools,
):
    assert set(enabled_tools) == set(server_module.mcp._tool_manager._tools)


@pytest.mark.parametrize(
    ("generator", "generator_args", "cleanup", "expected_name", "expected_args"),
    [
        (
            generate_mcp_config,
            (42, {"monitor": True}),
            lambda: cleanup_mcp_config(42),
            "ccm_skills",
            [
                "-P",
                "-m",
                "backend.mcp.ccm_skills_server",
                "--task-id",
                "42",
            ],
        ),
        (
            generate_monitor_agent_mcp_config,
            (7, 42),
            lambda: cleanup_monitor_agent_mcp_config(7),
            "ccm_monitor_agent",
            [
                "-P",
                "-m",
                "backend.mcp.ccm_monitor_agent_server",
                "--monitor-session-id",
                "7",
                "--task-id",
                "42",
            ],
        ),
        (
            generate_sub_agent_mcp_config,
            (9, 42),
            lambda: cleanup_sub_agent_mcp_config(9),
            "ccm_sub_agent",
            [
                "-P",
                "-m",
                "backend.mcp.ccm_sub_agent_server",
                "--sub-agent-session-id",
                "9",
                "--task-id",
                "42",
            ],
        ),
    ],
)
def test_claude_json_output_remains_compatible(
    monkeypatch,
    generator,
    generator_args,
    cleanup,
    expected_name,
    expected_args,
):
    _set_spec_snapshot_runtime(monkeypatch)
    api_base = "http://manager:8321"

    path = generator(*generator_args, api_base=api_base)
    try:
        servers = json.loads(path.read_text())["mcpServers"]
        assert servers[expected_name] == {
            "command": "/srv/ccm/.venv/bin/python3",
            "args": [
                *expected_args,
                "--api-base",
                api_base,
                "--auth-token",
                "secret-token",
            ],
            "cwd": "/srv/ccm",
            "env": {"PYTHONPATH": "/srv/ccm"},
        }
        expected_names = {expected_name}
        if expected_name == "ccm_skills":
            expected_names.add("ccm_frontend_review")
            expected_names.add("ccm_workspace_review")
            assert "--task-id" in servers["ccm_frontend_review"]["args"]
            assert "--task-id" in servers["ccm_workspace_review"]["args"]
        assert set(servers) == expected_names
    finally:
        cleanup()


def test_default_api_base_and_empty_auth_token(monkeypatch):
    monkeypatch.setattr(internal_api_endpoint, "_observed_api_base", None)
    monkeypatch.setattr(settings, "host", "0.0.0.0")
    monkeypatch.setattr(settings, "port", 8321)
    monkeypatch.setattr(settings, "internal_api_base_url", "")
    monkeypatch.setattr(settings, "auth_token", "")

    spec = build_mcp_server_specs(42)[0]

    assert spec.args[-2:] == ("--api-base", "http://127.0.0.1:8321")
    assert "--auth-token" not in spec.args


def test_observed_asgi_port_overrides_cli_stale_settings(monkeypatch):
    monkeypatch.setattr(internal_api_endpoint, "_observed_api_base", None)
    monkeypatch.setattr(settings, "host", "0.0.0.0")
    monkeypatch.setattr(settings, "port", 8000)
    monkeypatch.setattr(settings, "internal_api_base_url", "")

    internal_api_endpoint.observe_asgi_server(("127.0.0.1", 8803))
    spec = build_mcp_server_specs(42)[0]

    assert spec.args[-2:] == ("--api-base", "http://127.0.0.1:8803")


def test_codex_main_server_advertises_monitor_only_for_confirmed_local_scope():
    claude_spec = build_mcp_server_specs(42, provider="claude")[0]
    closed_codex_spec = build_mcp_server_specs(42, provider="codex")[0]
    local_codex_spec = build_mcp_server_specs(
        42,
        provider="codex",
        codex_monitor_enabled=True,
    )[0]

    monitor_tools = {"create_monitor", "check_monitors", "stop_monitor"}
    assert monitor_tools.issubset(claude_spec.enabled_tools)
    assert monitor_tools.isdisjoint(closed_codex_spec.enabled_tools)
    assert monitor_tools.issubset(local_codex_spec.enabled_tools)
    assert "ccm_read_skill" in closed_codex_spec.enabled_tools
    assert "ccm_read_user_skill" in closed_codex_spec.enabled_tools


@pytest.mark.parametrize(
    ("root", "python"),
    [
        ("/opt/Claude Code Manager", "/opt/Claude Code Manager/.venv/bin/python3"),
        (
            r"C:\CCM 工作区",
            r"C:\CCM 工作区\.venv\Scripts\python.exe",
        ),
    ],
)
def test_platform_paths_are_preserved(monkeypatch, root, python):
    monkeypatch.setattr(mcp_config, "_CCM_ROOT", root)
    monkeypatch.setattr(mcp_config, "_VENV_PYTHON", python)
    monkeypatch.setattr(settings, "auth_token", "")

    spec = build_mcp_server_specs(
        42,
        api_base="http://127.0.0.1:8000",
    )[0]
    rendered = render_claude_mcp_config((spec,))

    assert spec.command == python
    assert spec.cwd == root
    assert spec.args[:3] == ("-P", "-m", "backend.mcp.ccm_skills_server")
    assert dict(spec.env) == {"PYTHONPATH": root}
    assert rendered["mcpServers"]["ccm_skills"]["command"] == python
    assert rendered["mcpServers"]["ccm_skills"]["cwd"] == root
    assert rendered["mcpServers"]["ccm_skills"]["env"] == {
        "PYTHONPATH": root,
    }


def test_claude_renderer_includes_env_but_not_provider_metadata():
    spec = McpServerSpec(
        name="example",
        command="python",
        args=("-m", "example"),
        cwd="/workspace",
        env={"LANG": "zh_CN.UTF-8"},
        required=True,
        enabled_tools=("example_tool",),
        startup_timeout_sec=15,
        tool_timeout_sec=90,
    )

    assert render_claude_mcp_config((spec,)) == {
        "mcpServers": {
            "example": {
                "command": "python",
                "args": ["-m", "example"],
                "cwd": "/workspace",
                "env": {"LANG": "zh_CN.UTF-8"},
            }
        }
    }


def test_mcp_server_spec_collections_are_immutable():
    args = ["-m", "example"]
    env = {"TOKEN": "secret"}
    tools = ["example_tool"]

    spec = McpServerSpec(
        name="example",
        command="python",
        args=args,
        env=env,
        enabled_tools=tools,
    )
    args.append("--changed")
    env["TOKEN"] = "changed"
    tools.append("changed_tool")

    assert spec.args == ("-m", "example")
    assert dict(spec.env) == {"TOKEN": "secret"}
    assert spec.enabled_tools == ("example_tool",)
    with pytest.raises(TypeError):
        spec.env["TOKEN"] = "cannot-change"


def test_claude_renderer_rejects_duplicate_server_names():
    spec = McpServerSpec(name="duplicate", command="python")

    with pytest.raises(ValueError, match="Duplicate MCP server name: duplicate"):
        render_claude_mcp_config((spec, spec))


def _parse_codex_exec_config_args(args: list[str]) -> dict[str, object]:
    assert len(args) % 2 == 0
    servers: dict[str, object] = {}
    for flag, override in zip(args[::2], args[1::2], strict=True):
        assert flag == "-c"
        parsed = tomllib.loads(override)
        assert set(parsed) == {"mcp_servers"}
        servers.update(parsed["mcp_servers"])
    return {"mcp_servers": servers}


def test_codex_app_server_renderer_includes_supported_stdio_fields():
    spec = McpServerSpec(
        name="ccm_server_zh",
        command=r"C:\Program Files\CCM\python.exe",
        args=(
            "-m",
            "测试.mcp",
            "--label",
            '他说 "你好"',
            "--path",
            r"C:\工具\server.py",
        ),
        cwd=r"C:\CCM 工作区",
        env={
            "API.TOKEN": 'a\\b"c',
            "中文键": "值",
        },
        required=True,
        enabled_tools=("工具一", "tool_two"),
        startup_timeout_sec=10.0,
        tool_timeout_sec=60.0,
    )

    assert render_codex_mcp_config((spec,)) == {
        "mcp_servers": {
            "ccm_server_zh": {
                "command": r"C:\Program Files\CCM\python.exe",
                "args": [
                    "-m",
                    "测试.mcp",
                    "--label",
                    '他说 "你好"',
                    "--path",
                    r"C:\工具\server.py",
                ],
                "cwd": r"C:\CCM 工作区",
                "env": {
                    "API.TOKEN": 'a\\b"c',
                    "中文键": "值",
                },
                "required": True,
                "enabled_tools": ["工具一", "tool_two"],
                "startup_timeout_sec": 10.0,
                "tool_timeout_sec": 60.0,
            }
        }
    }


def test_codex_exec_renderer_serializes_the_same_config_as_toml():
    spec = McpServerSpec(
        name="ccm_server_zh",
        command=r"C:\Program Files\CCM\python.exe",
        args=("-m", "测试.mcp", "--label", '他说 "你好"'),
        cwd=r"C:\CCM 工作区",
        env={"API.TOKEN": 'a\\b"c'},
        required=True,
        enabled_tools=("工具一", "tool_two"),
        startup_timeout_sec=10.0,
        tool_timeout_sec=60.0,
    )

    app_server_config = render_codex_mcp_config((spec,))
    exec_args = render_codex_exec_config_args((spec,))

    assert len(exec_args) == 2
    assert exec_args[0] == "-c"
    assert exec_args[1].startswith("mcp_servers={ ccm_server_zh = { ")
    assert 'args = ["-m", "测试.mcp", "--label", "他说 \\"你好\\""]' in exec_args[1]
    assert 'cwd = "C:\\\\CCM 工作区"' in exec_args[1]
    assert _parse_codex_exec_config_args(exec_args) == app_server_config


@pytest.mark.parametrize(
    ("builder", "builder_args", "expected_name"),
    [
        (build_mcp_server_specs, (42,), "ccm_skills"),
        (
            build_monitor_agent_mcp_server_specs,
            (7, 42),
            "ccm_monitor_agent",
        ),
        (
            build_sub_agent_mcp_server_specs,
            (9, 42),
            "ccm_sub_agent",
        ),
    ],
)
def test_codex_renderers_share_each_role_spec(
    monkeypatch,
    builder,
    builder_args,
    expected_name,
):
    _set_spec_snapshot_runtime(monkeypatch)
    specs = builder(*builder_args, api_base="http://manager:8321")

    app_server_config = render_codex_mcp_config(specs)

    expected_names = {expected_name}
    if expected_name == "ccm_skills":
        expected_names.add("ccm_frontend_review")
        expected_names.add("ccm_workspace_review")
    assert set(app_server_config["mcp_servers"]) == expected_names
    assert (
        app_server_config["mcp_servers"][expected_name][
            "default_tools_approval_mode"
        ]
        == "approve"
    )
    assert _parse_codex_exec_config_args(
        render_codex_exec_config_args(specs)
    ) == app_server_config


def test_codex_renderer_omits_unset_optional_fields():
    spec = McpServerSpec(name="minimal", command="python")

    assert render_codex_mcp_config((spec,)) == {
        "mcp_servers": {
            "minimal": {
                "command": "python",
                "args": [],
                "required": False,
            }
        }
    }
    assert _parse_codex_exec_config_args(
        render_codex_exec_config_args((spec,))
    ) == render_codex_mcp_config((spec,))


def test_codex_renderers_support_empty_specs():
    assert render_codex_mcp_config(()) == {"mcp_servers": {}}
    assert render_codex_exec_config_args(()) == []


def test_codex_exec_renderer_emits_one_merged_server_table_override():
    specs = (
        McpServerSpec(name="first", command="python"),
        McpServerSpec(name="second", command="node"),
    )

    exec_args = render_codex_exec_config_args(specs)

    assert len(exec_args) == 2
    assert exec_args[0] == "-c"
    assert exec_args[1].startswith("mcp_servers={ first = { ")
    assert "second = { " in exec_args[1]
    assert _parse_codex_exec_config_args(exec_args) == render_codex_mcp_config(specs)


@pytest.mark.parametrize(
    "renderer",
    [render_codex_mcp_config, render_codex_exec_config_args],
)
def test_codex_renderers_reject_duplicate_server_names(renderer):
    spec = McpServerSpec(name="duplicate", command="python")

    with pytest.raises(ValueError, match="Duplicate MCP server name: duplicate"):
        renderer((spec, spec))


@pytest.mark.parametrize(
    "invalid_name",
    ["", "contains.dot", "contains space", "中文"],
)
@pytest.mark.parametrize(
    "renderer",
    [render_codex_mcp_config, render_codex_exec_config_args],
)
def test_codex_renderers_reject_names_the_cli_cannot_initialize(
    renderer,
    invalid_name,
):
    spec = McpServerSpec(name=invalid_name, command="python")

    with pytest.raises(
        ValueError,
        match=r"must match \^\[A-Za-z0-9_-\]\+\$",
    ):
        renderer((spec,))


@pytest.mark.parametrize("existing_config", [False, True])
def test_codex_renderers_do_not_write_codex_home(
    monkeypatch,
    tmp_path,
    existing_config,
):
    codex_home = tmp_path / "codex-home"
    codex_home.mkdir()
    config_path = codex_home / "config.toml"
    original = b'model = "existing-user-choice"\n'
    if existing_config:
        config_path.write_bytes(original)
    monkeypatch.setenv("CODEX_HOME", str(codex_home))
    spec = McpServerSpec(name="example", command="python")

    render_codex_mcp_config((spec,))
    render_codex_exec_config_args((spec,))

    if existing_config:
        assert config_path.read_bytes() == original
        assert list(codex_home.iterdir()) == [config_path]
    else:
        assert not config_path.exists()
        assert list(codex_home.iterdir()) == []


@pytest.mark.parametrize(
    "renderer",
    [render_codex_mcp_config, render_codex_exec_config_args],
)
@pytest.mark.parametrize(
    ("field_name", "invalid_timeout"),
    [
        ("startup_timeout_sec", float("nan")),
        ("startup_timeout_sec", float("inf")),
        ("startup_timeout_sec", -0.1),
        ("startup_timeout_sec", True),
        ("tool_timeout_sec", float("-inf")),
        ("tool_timeout_sec", -1),
        ("tool_timeout_sec", False),
    ],
)
def test_codex_renderers_reject_invalid_timeouts(
    renderer,
    field_name,
    invalid_timeout,
):
    spec = McpServerSpec(
        name="invalid",
        command="python",
        **{field_name: invalid_timeout},
    )

    with pytest.raises(
        ValueError,
        match=rf"Invalid Codex {field_name}.*finite non-negative number",
    ):
        renderer((spec,))


@pytest.mark.parametrize(
    "renderer",
    [render_codex_mcp_config, render_codex_exec_config_args],
)
def test_codex_renderers_reject_invalid_default_tools_approval_mode(renderer):
    spec = McpServerSpec(
        name="invalid",
        command="python",
        default_tools_approval_mode="always",
    )

    with pytest.raises(
        ValueError,
        match=r"Invalid Codex default_tools_approval_mode.*'invalid'",
    ):
        renderer((spec,))
