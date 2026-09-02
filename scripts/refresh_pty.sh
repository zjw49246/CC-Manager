#!/usr/bin/env bash
# 把 claude-pty 依赖刷新到 PTY 仓库 main 的最新 commit。
#
# 背景：pyproject 里的 git 依赖（claude-pty @ git+https://...）是**安装时快照**，
# `git pull` CCM 不会更新它——生产同步流程必须跑本脚本：
#   git pull → ./scripts/refresh_pty.sh → alembic upgrade head → npm build → restart
#
# 行为：
# - editable/本地安装（开发环境指向 /home/ubuntu/Projects/PTY）→ 跳过，天然最新
# - 已安装 commit == PTY 远端 main HEAD → 跳过
# - 否则用 uv 重装到最新 commit（--force-reinstall：URL/版本号不变时 pip/uv
#   会认为"已安装"直接跳过，必须强制）
set -euo pipefail
cd "$(dirname "$0")/.."

PY=.venv/bin/python3
UV="${UV:-$HOME/.local/bin/uv}"
# 必须锚定 claude-pty 那一行——pyproject 里还有其他 git 依赖（如 auto-backup）
PTY_URL=$(grep -E '"claude-pty @ git\+' pyproject.toml | grep -oE 'git\+https://[^"@]+' | head -1 | sed 's/^git+//')
[ -n "$PTY_URL" ] || { echo "pyproject.toml 里找不到 claude-pty git 依赖"; exit 1; }

# editable 安装：代码就是本地 PTY 仓库，无需刷新
if ! "$PY" -c "import claude_pty, sys; sys.exit(0 if 'site-packages' in claude_pty.__file__ else 1)" 2>/dev/null; then
    echo "claude-pty 是 editable/本地安装（$("$PY" -c 'import claude_pty; print(claude_pty.__file__)' 2>/dev/null || echo 未安装)），跳过刷新"
    echo "CCM_PTY_REFRESH_CHANGED=0"
    exit 0
fi

installed=$("$PY" - <<'EOF'
import json, importlib.metadata as m
try:
    raw = m.distribution("claude-pty").read_text("direct_url.json") or "{}"
    print(json.loads(raw).get("vcs_info", {}).get("commit_id", ""))
except Exception:
    print("")
EOF
)

latest=$(git ls-remote "$PTY_URL" refs/heads/main | cut -f1)
[ -n "$latest" ] || {
    echo "无法获取 PTY 远端 main HEAD（网络/权限？），跳过"
    echo "CCM_PTY_REFRESH_CHANGED=0"
    exit 0
}

if [ "$installed" = "$latest" ]; then
    echo "claude-pty 已是最新（${latest:0:12}）"
    echo "CCM_PTY_REFRESH_CHANGED=0"
    exit 0
fi

echo "claude-pty: ${installed:0:12} -> ${latest:0:12}，重新安装…"
"$UV" pip install --python "$PY" --force-reinstall --no-deps "claude-pty @ git+${PTY_URL}@${latest}"
echo "完成。验证："
"$PY" -c "import claude_pty; print(' import OK:', claude_pty.__file__)"
echo "CCM_PTY_REFRESH_CHANGED=1"
