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

CHECK_ONLY=0
if [ "${1:-}" = "--check" ]; then
    CHECK_ONLY=1
fi

PY=.venv/bin/python3
UV="${UV:-$HOME/.local/bin/uv}"
[ -x "$PY" ] || { echo "claude-pty 刷新失败：Python 环境不存在（$PY）" >&2; exit 1; }
[ -x "$UV" ] || { echo "claude-pty 刷新失败：uv 不可执行（$UV）" >&2; exit 1; }

# 必须锚定 claude-pty 那一行——pyproject 里还有其他 git 依赖（如 auto-backup）。
# 把解析失败视为失败，而不是让更新流程继续并误报“已完成”。
if ! PTY_URL="$(grep -E '"claude-pty @ git\+' pyproject.toml \
    | grep -oE 'git\+https://[^"@]+' \
    | head -1 | sed 's/^git+//')"; then
    PTY_URL=""
fi
[ -n "$PTY_URL" ] || {
    echo "pyproject.toml 里找不到 claude-pty git 依赖" >&2
    exit 1
}

# editable 安装：代码就是本地 PTY 仓库，无需刷新。导入失败不能当作
# editable，否则生产缺包时会被错误地当成“PTY 已同步”。
PTY_LOCATION=$("$PY" -c 'import claude_pty; print(claude_pty.__file__)' 2>/dev/null || true)
if [ -z "$PTY_LOCATION" ]; then
    echo "claude-pty 未安装或无法导入，拒绝继续更新" >&2
    exit 1
fi
case "$PTY_LOCATION" in
*/site-packages/*)
    ;;
*)
    echo "claude-pty 是 editable/本地安装（$PTY_LOCATION），跳过刷新"
    echo "CCM_PTY_REFRESH_CHANGED=0"
    exit 0
    ;;
esac

installed_commit() {
    "$PY" - <<'EOF'
import json, importlib.metadata as m
try:
    raw = m.distribution("claude-pty").read_text("direct_url.json") or "{}"
    print(json.loads(raw).get("vcs_info", {}).get("commit_id", ""))
except Exception:
    print("")
EOF
}

installed="$(installed_commit)"
[ "$installed" != "" ] || {
    echo "无法读取已安装 claude-pty 的 commit，拒绝继续更新" >&2
    exit 1
}

if ! latest="$(git ls-remote "$PTY_URL" refs/heads/main | awk 'NR == 1 {print $1}')"; then
    echo "无法获取 PTY 远端 main HEAD（网络/权限？），拒绝完成更新" >&2
    exit 1
fi
if ! [[ "$latest" =~ ^[0-9a-fA-F]{40,64}$ ]]; then
    echo "PTY 远端 main HEAD 无效（${latest:-空}），拒绝完成更新" >&2
    exit 1
fi

if [ "$installed" = "$latest" ]; then
    echo "claude-pty 已是最新（${latest:0:12}）"
    echo "CCM_PTY_REFRESH_CHANGED=0"
    exit 0
fi

if [ "$CHECK_ONLY" = "1" ]; then
    echo "claude-pty 可更新（${installed:0:12} -> ${latest:0:12}）"
    echo "CCM_PTY_REFRESH_CHANGED=1"
    exit 0
fi

echo "claude-pty: ${installed:0:12} -> ${latest:0:12}，重新安装…"
"$UV" pip install --python "$PY" --force-reinstall --no-deps "claude-pty @ git+${PTY_URL}@${latest}"
installed_after="$(installed_commit)"
if [ "$installed_after" != "$latest" ]; then
    echo "claude-pty 安装后 commit 校验失败（期望=${latest} 实际=${installed_after:-空}），拒绝完成更新" >&2
    exit 1
fi
echo "完成。验证："
"$PY" -c "import claude_pty; print(' import OK:', claude_pty.__file__)"
echo "CCM_PTY_REFRESH_CHANGED=1"
