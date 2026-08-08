#!/bin/sh

# claude-pty intentionally exposes only a small fixed CLI surface.  CCM uses
# this wrapper to apply the exact per-Task settings that the direct `-p` path
# receives, without putting any credential value in argv.
settings_path=${CCM_TASK_CLAUDE_SETTINGS:?missing Task Claude settings}
claude_binary=${CCM_TASK_CLAUDE_BINARY:-claude}
tool_allowlist=${CCM_TASK_CLAUDE_TOOLS:-AskUserQuestion,Bash,Edit,Glob,Grep,MultiEdit,NotebookEdit,Read,Write}

unset CCM_TASK_CLAUDE_SETTINGS
unset CCM_TASK_CLAUDE_BINARY
unset CCM_TASK_CLAUDE_TOOLS

exec "$claude_binary" \
  --permission-mode acceptEdits \
  --settings "$settings_path" \
  --setting-sources "" \
  --strict-mcp-config \
  --disable-slash-commands \
  --no-chrome \
  --tools "$tool_allowlist" \
  "$@"
