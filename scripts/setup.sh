#!/bin/bash
# CCM 新机器一键部署脚本
# 用法：git clone 后执行 ./scripts/setup.sh
set -euo pipefail
cd "$(dirname "$0")/.."

echo "=== CCM 环境初始化 ==="

# ── 1. 系统依赖（登录 UI + Claude Task fail-closed sandbox preflight）──
echo "[1/5] 安装系统依赖..."
PACKAGES=(xvfb xauth xdotool bubblewrap socat)
MISSING=()
for pkg in "${PACKAGES[@]}"; do
    if ! dpkg -s "$pkg" &>/dev/null; then
        MISSING+=("$pkg")
    fi
done
if [ ${#MISSING[@]} -gt 0 ]; then
    sudo apt-get update -qq
    sudo apt-get install -y "${MISSING[@]}"
fi

# ── 2. Google Chrome（CDP 登录使用系统 Chrome）─────────────────────
# 固定 Chrome 149：150+ 在 Xvfb 上 renderer crash（导航 claude.ai 时崩溃）
CHROME_VERSION="149.0.7827.53-1"
CHROME_DEB_URL="https://dl.google.com/linux/chrome/deb/pool/main/g/google-chrome-stable/google-chrome-stable_${CHROME_VERSION}_amd64.deb"
echo "[2/5] 安装 Google Chrome ${CHROME_VERSION}..."
INSTALLED_CHROME=$(google-chrome --version 2>/dev/null | grep -oP '[\d.]+' || echo "none")
if [ "$INSTALLED_CHROME" != "149.0.7827.53" ]; then
    wget -q -O /tmp/google-chrome.deb "$CHROME_DEB_URL"
    sudo dpkg -i /tmp/google-chrome.deb 2>/dev/null || sudo apt-get install -f -y
    rm -f /tmp/google-chrome.deb
    # 阻止 apt 自动升级 Chrome
    sudo apt-mark hold google-chrome-stable 2>/dev/null || true
fi
echo "  Chrome: $(google-chrome --version)"

# ── 3. Node.js + Agent CLIs ────────────────────────────────────────
echo "[3/5] 安装 Node.js、Codex CLI 和 Claude CLI..."
if ! command -v node &>/dev/null; then
    curl -fsSL https://deb.nodesource.com/setup_22.x | sudo -E bash -
    sudo apt-get install -y nodejs
fi
if ! command -v claude &>/dev/null; then
    sudo npm install -g @anthropic-ai/claude-code
fi
# Claude treats the Linux seccomp helper as optional and otherwise leaves Unix
# sockets open. CCM's Task sandbox is fail-closed, so install and verify the
# architecture-matched helper explicitly.
SANDBOX_RUNTIME_VERSION="0.0.71"
case "$(uname -m)" in
    x86_64|amd64) SANDBOX_RUNTIME_ARCH="x64" ;;
    aarch64|arm64) SANDBOX_RUNTIME_ARCH="arm64" ;;
    *) echo "Unsupported Claude sandbox architecture: $(uname -m)" >&2; exit 1 ;;
esac
SANDBOX_NPM_ROOT="$(sudo npm root -g)"
APPLY_SECCOMP="${SANDBOX_NPM_ROOT}/@anthropic-ai/sandbox-runtime/vendor/seccomp/${SANDBOX_RUNTIME_ARCH}/apply-seccomp"
if [ ! -f "$APPLY_SECCOMP" ] || [ -L "$APPLY_SECCOMP" ] || [ ! -x "$APPLY_SECCOMP" ]; then
    sudo npm install -g "@anthropic-ai/sandbox-runtime@${SANDBOX_RUNTIME_VERSION}"
fi
if [ ! -f "$APPLY_SECCOMP" ] || [ -L "$APPLY_SECCOMP" ] || [ ! -x "$APPLY_SECCOMP" ]; then
    echo "Claude apply-seccomp helper installation could not be verified" >&2
    exit 1
fi
CODEX_CLI_VERSION="0.147.0"
if [ "$(codex --version 2>/dev/null | head -1)" != "codex-cli ${CODEX_CLI_VERSION}" ]; then
    sudo npm install -g "@openai/codex@${CODEX_CLI_VERSION}"
fi
echo "  Node: $(node --version)  Codex CLI: $(codex --version 2>&1 | head -1)  Claude CLI: $(claude --version 2>&1 | head -1)"

# ── 4. Python 后端依赖 ─────────────────────────────────────────────
echo "[4/5] 安装 Python 依赖..."
if ! command -v uv &>/dev/null; then
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.local/bin:$PATH"
fi
uv sync

# ── 5. 前端构建 ────────────────────────────────────────────────────
echo "[5/5] 构建前端..."
cd frontend
npm install --no-fund --no-audit
npm run build
cd ..

# ── 6. SSH 密钥（Worker 功能需要，用于 Manager→Worker SSH 连接）────────
echo "[6/8] 配置 SSH 密钥..."
mkdir -p "$HOME/.ssh"
chmod 700 "$HOME/.ssh"
CCM_KEY="$HOME/.ssh/ccm_worker_key"
if [ ! -f "$CCM_KEY" ]; then
    ssh-keygen -t ed25519 -f "$CCM_KEY" -N "" -C "ccm-worker-$(hostname)" >/dev/null 2>&1
    echo "  生成密钥: $CCM_KEY"
else
    echo "  密钥已存在: $CCM_KEY"
fi
chmod 600 "$CCM_KEY"
# WorkerProvisioner 会在创建 EC2 时通过 cloud-init 把对应公钥注入 Worker。
# Manager 不接受 Worker 反向 SSH，因此不要把这把公钥写进 Manager 自己的
# authorized_keys（旧逻辑既无助于 Manager→Worker，也扩大了入口）。
echo "  Worker 创建时将自动注入对应公钥"

# ── 7. Claude CLI warmup（完成 onboarding 对话框，否则 PTY 模式 MCP 不初始化）
echo "[7/8] Claude CLI warmup..."
if command -v claude &>/dev/null; then
    # Phase 1: -p 模式填充 GrowthBook cache + 验证凭证
    timeout 30 claude -p 'reply: ok' --dangerously-skip-permissions 2>/dev/null || true
    # Phase 2: PTY 交互模式完成 theme picker 等 onboarding 对话框
    .venv/bin/python3 -c '
import asyncio

async def warmup():
    from claude_pty.session import Session
    from claude_pty.config import PTYConfig
    from claude_pty.bridge import BridgeHub

    bridge = BridgeHub()
    bridge.start()
    try:
        cfg = PTYConfig(default_model="claude-opus-4-6", dangerously_skip_permissions=True)
        s = Session(cwd="'"$(pwd)"'", config=cfg, bridge=bridge)
        await s.start()
        count = 0
        async for ev in s.send_prompt("reply: ok"):
            if ev.content:
                count += 1
                if count >= 2:
                    break
        await s.stop()
        print("warmup ok")
    except Exception as e:
        print(f"warmup failed: {e}")
    finally:
        bridge.stop()

asyncio.run(warmup())
' 2>&1 | tail -1
fi

# ── 8. 自动生成 .env（如不存在）───────────────────────────────────────
echo "[8/8] 配置 .env..."
if [ ! -f .env ]; then
    TOKEN=$(python3 -c "import secrets; print(secrets.token_urlsafe(24))")
    cat > .env <<ENVEOF
AUTH_TOKEN=${TOKEN}
DATABASE_URL=sqlite+aiosqlite:///./claude_manager.db
WORKSPACE_DIR=~/Projects
CCM_NODE_ROLE=manager
AUTO_START_DISPATCHER=true
PORT=8002
POOL_ENABLED=true
CODEX_POOL_ENABLED=true
WORKER_SSH_KEY_PATH=${CCM_KEY}
ENVEOF
    echo "  .env 已生成（AUTH_TOKEN=${TOKEN}）"
else
    # 确保 WORKER_SSH_KEY_PATH 存在
    if ! grep -q "WORKER_SSH_KEY_PATH" .env; then
        echo "WORKER_SSH_KEY_PATH=${CCM_KEY}" >> .env
        echo "  已追加 WORKER_SSH_KEY_PATH 到 .env"
    fi
    if ! grep -q "^CCM_NODE_ROLE=" .env; then
        echo "CCM_NODE_ROLE=manager" >> .env
        echo "  已追加 CCM_NODE_ROLE=manager 到 .env"
    fi
    echo "  .env 已存在，跳过生成"
fi

echo ""
echo "=== 初始化完成 ==="
echo "启动服务："
echo "  uv run uvicorn backend.main:app --host 127.0.0.1 --port 8002"
