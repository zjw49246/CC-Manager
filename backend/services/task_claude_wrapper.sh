#!/bin/sh

# claude-pty intentionally exposes only a small fixed CLI surface.  CCM uses
# this wrapper to apply the exact per-Task settings that the direct `-p` path
# receives, without putting any credential value in argv.
permission_profile=${CCM_TASK_CLAUDE_PROFILE:-isolated}
claude_binary=${CCM_TASK_CLAUDE_BINARY:-claude}
builtin_tools=${CCM_TASK_CLAUDE_TOOLS:-default}
allowed_rules=${CCM_TASK_CLAUDE_ALLOWED_RULES:?missing Task Claude allowed rules}

if [ "$permission_profile" = "unrestricted" ]; then
  settings_path=${CCM_TASK_CLAUDE_SETTINGS:-}
  unset CCM_TASK_CLAUDE_PROFILE
  unset CCM_TASK_CLAUDE_SETTINGS
  unset CCM_TASK_CLAUDE_BINARY
  unset CCM_TASK_CLAUDE_TOOLS
  unset CCM_TASK_CLAUDE_ALLOWED_RULES

  # The PTY-provided bypass flag remains in its original arguments ("$@").
  # Ordinary administrator Tasks provide exact settings to survive Claude's
  # effective-mode fallback. Legacy unrestricted Delivery intentionally has
  # no Task settings/MCP scope and retains its fixed built-in inventory.
  if [ -n "$settings_path" ]; then
    exec "$claude_binary" \
      --settings "$settings_path" \
      --setting-sources "" \
      --strict-mcp-config \
      --disable-slash-commands \
      --no-chrome \
      --tools "$builtin_tools" \
      --allowedTools "$allowed_rules" \
      "$@"
  fi

  exec "$claude_binary" \
    --setting-sources "" \
    --strict-mcp-config \
    --disable-slash-commands \
    --no-chrome \
    --tools "$builtin_tools" \
    --allowedTools "$allowed_rules" \
    "$@"
fi

settings_path=${CCM_TASK_CLAUDE_SETTINGS:?missing Task Claude settings}
unset CCM_TASK_CLAUDE_PROFILE
unset CCM_TASK_CLAUDE_SETTINGS
unset CCM_TASK_CLAUDE_BINARY
unset CCM_TASK_CLAUDE_TOOLS
unset CCM_TASK_CLAUDE_ALLOWED_RULES

exec "$claude_binary" \
  --permission-mode acceptEdits \
  --settings "$settings_path" \
  --setting-sources "" \
  --strict-mcp-config \
  --disable-slash-commands \
  --no-chrome \
  --tools "$builtin_tools" \
  --allowedTools "$allowed_rules" \
  "$@"
