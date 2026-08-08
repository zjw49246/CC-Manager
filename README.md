# Claude Code Manager

**简体中文** | [English](README.en-US.md)

Web 端调度和管理多个 Claude Code 实例并行工作。灵感来自胡渊鸣的文章「我给 10 个 Claude Code 打工」。

> **⚠️ 重要安全提示：** 本项目会以 `--dangerously-skip-permissions` 模式运行 Claude Code，这意味着 Claude Code 将拥有**不受限制的文件读写、命令执行和网络访问权限**，并且会自动执行 `git push` 等操作。**强烈建议在一台单独的、没有重要文件的电脑或虚拟机上部署**，避免对你的个人数据或工作环境造成意外影响。

## 功能

### 核心调度
- **全局调度器** — 启动时自动创建 worker、自动分配任务，无需手动操作
- **Claude Code 完全自主** — Claude Code 自主完成 worktree 创建、commit、fetch、merge、push、冲突解决和清理，Dispatcher 只负责分配任务和判断成败
- **9 步任务生命周期** — 领取 → 创建工作区 → 实现 → 提交 → merge + 测试 → 合并到 main → 标记完成 → 清理 → 经验沉淀
- **项目管理** — 支持 clone 已有仓库（有 remote）和本地 git init（无 remote），创建任务时可直接新建项目
- **项目 Todo 清单** — 每个项目维护一个可折叠的待办清单（prompt 模板），一键「▶ Run」直接创建 Task 并跳转 Chat；创建后 Todo 自动标记完成并记录派生的 task。支持归档/恢复/永久删除
- **任务队列** — 按优先级自动调度（数字越小优先级越高）
- **多实例并行** — 同时运行多个 Claude Code 实例，各自处理不同任务
- **Git Worktree** — 每个实例在独立的 worktree 中工作，互不干扰

### 执行模式
- **多 Provider（Claude / Codex）** — Task 级选择执行引擎：OpenAI Codex CLI（默认，`gpt-5.6-sol`）或 Claude Code。Codex 任务支持完整生命周期、多轮对话、Goal 模式评估、Plan 审批、上下文自动压缩、瞬时错误退避重试、账号池与跨 Worker rollout 迁移；指令文件读 `AGENTS.md`（自动注入）。PTY 热会话、ask_user 和 Claude 原生子 Agent 仍为 Claude 专属（Codex 下显式隐藏/拒绝，不静默降级）
- **PTY 持久会话模式** — 默认模式，Claude Code 以常驻交互会话运行，多轮免冷启动（热 session 复用），首次启动有 Cold Start 指示器
- **Goal 模式** — `mode="goal"` 使用自然语言完成条件（`goal_condition`），每 turn 后由轻量评估器（默认 Haiku）自动判断是否达成目标
- **交互式版本化 Plan** — Plan 是独立于 Task 的一等制品；Planner/Reviewer 可暂停同一个 Run 请求任意数量的必要输入，回答后继续并保留不可变 Version 历史。审批不自动执行，关联 Version 由用户显式附到下一条真实消息，standalone Version 可一键创建执行 Task
- **Effort Level** — 支持 `low` / `medium` / `high` / `xhigh` / `max` 五档，优先级链：Task → Instance → 全局默认
- **Model 配置** — 支持全称模型 ID（包括 `claude-opus-5`）；Opus 5 固定为 1M context 并支持 `low/medium/high/xhigh/max` effort，其他兼容模型可用 `[1m]` 后缀开启 1M context
- **Codex Fast** — Codex Task 可选择 Standard 或 Fast；Fast 使用同一模型的 `priority` service tier，不会换模型或降低 effort。当前支持 GPT-5.6 Sol/Terra/Luna、GPT-5.5、GPT-5.4；账号或模型无法确认 `priority` 时会在执行前明确失败，不会挂着 Fast 徽标偷偷按 Standard 运行
- **Thinking Budget** — Instance 级别设置 `thinking_budget`，通过 `MAX_THINKING_TOKENS` 传递给 CLI
- **Workflows 开关** — Task 级别控制是否启用 Workflow 工具，关闭时节省 token

### 智能能力
- **Skills 系统** — 创建 Task 时可为 Claude/Codex 勾选普通 Skills 与 User Skills；统一 task context 由 Claude system-prompt 文件、Codex app-server 的 schema-backed turn text 或等价 `codex exec` adapter 注入。本地、非共享、非 Worker 管理的 Codex Task 在主 MCP capability 已确认时也可使用 Monitor；Worker/Shared Task 继续 fail closed。`CODEX_MAIN_MCP_ENABLED=false` 会关闭 Codex 普通/User/Monitor Skills，但不影响独立的 Sub-Agent。开头的 `$command` 会在本地、Worker 和 Shared chat 写入消息前按任务的精确运行范围统一校验。远程 Worker 首次领取任务时会在同一任务锁内固定最新 Skill 配置；领取后的活跃执行不允许中途改写 Skills
- **Monitor Sub-Agent** — Claude 及 capability 已确认的本地 Codex Task 可自主创建持久监控子 Agent；子 Agent 拥有独立 MCP 工具（report_status / mark_complete / get_context），按数据库调度的短回合自主检查并向系统汇报
- **原生子 Agent 镜像（PTY 模式）** — 模型用内置 Agent/Task/Monitor 工具开的子 agent 会被 PTY 层观测并自动注册进子 agent 体系（类别 native-agent / native-monitor），统一展示和管理

### 交互与对话
- **多轮对话** — 任务完成后可通过 Chat 界面继续追问，自动 `--resume` 同一 session
- **Session 关注标签** — 每个 Task 可维护一个自定义短标签，在任务列表和 Chat 顶栏醒目展示并随时编辑，便于记录“何时再看/下一步做什么”；该字段与系统内部 `tags` 独立，复制、Fork 和 Worker 迁移时会保留
- **Task 产物下载** — Claude/Codex 会把明确交付给用户的文件保存到当前 Project 的 `.claude-manager/artifacts/task-<id>/`，聊天中的显式产物链接可直接下载；普通源码和文档引用不会误显示为下载文件
- **SSH 工作台与 Task 授权** — Files 页面可管理固定主机指纹的 SSH Profile、浏览/下载远程文件；管理员可在新建 Task 或 Chat 中按 `exec/read/write` 最小权限授权，Claude/Codex 通过 task-scoped `ccm_ssh` MCP 使用，私钥始终留在 Manager。使用和安全边界见 [SSH access](docs/ssh-access.md)
- **数学公式渲染** — 聊天和 Discussion 中的 Markdown 支持 KaTeX；兼容 Codex 常用的 `\\(...\\)` / 整段 `\\[...\\]` 以及 `$$...$$`，单美元符号内容按普通文本显示，链接、HTML、代码和货币内容保持不变
- **交互式提问（ask_user）** — 模型调用内置 `AskUserQuestion` 时，聊天里弹出可选卡片（单选/多选/自定义文本），用户选完即把答案喂回模型继续。超时默认 1800s，支持跨页面全局通知（右下角弹窗 + 未读标记），可用 `ASK_USER_ENABLED=false` 关闭
- **权限透传（PTY 模式）** — CC 请求工具权限时聊天里出现卡片（工具名/描述/输入预览），点允许/拒绝实时回包；120s 超时默认拒绝
- **语音输入** — 通过 OpenAI Whisper API 语音转文字创建任务

### 可靠性
- **Claude / Codex 统一账号路由** — 原生账号与 CloudRouter API Key 共用账号池、模型/Service Tier 兼容性检查和 session 迁移。Codex Fast 只选择真实广告 `priority` 的账号；ApexRouter 的模型目录能力也会参与选择。手动「优先账号」最高；自动模式下已有对话保持绑定账号，新会话优先兼容且可用的 API、再回退原生额度选择。两池都显示真正提交后的「最近使用」，API 候选失败不会误改徽标
- **API 账号安全删除** — CloudRouter/ApexRouter 账号先停用新任务，再等待活跃任务和会话释放后删除 Key 与运行配置；忙碌时保留“待清理”状态供重试，不会强杀任务，并保留 Claude projects 与 Codex sessions
- **无缝账号轮换** — Claude 递归硬链接 session JSONL 及 sidecar，Codex 独立复制 rollout 并原子完成 app-server rebind + Task binding；撞限、认证失败或主动额度阈值换号时保留原对话上下文，不支持的模型不会静默降级
- **瞬时 429/过载自动重试** — 基础设施侧的临时限流/过载（非账号额度用尽），指数退避+jitter 用同一账号自动 `--resume` 重试，最多 5 次；检测按 provider 分流（Claude / Codex 各自的 CLI 错误文案）
- **`/tmp` 空间保护** — 服务启动时及后台每 3 小时检查容量和 inode；任一达到 80% 时，清理全部超过 6 小时的 CCM 白名单临时产物
- **进程超时保护** — 单任务最长执行时间可配置，超时后自动 kill

### 分布式
- **分布式 Worker** — 将任务分发到远程 EC2 实例执行，突破单机并发瓶颈。Phase 1（创建/部署/管理）+ Phase 2（任务转发+事件中继）+ Phase 3（任务实时迁移）全部可用。详见 [Worker 部署指南](docs/worker-deployment-guide.md)
- **安全的一键更新重启** — 后台定时检查并弹窗提醒；更新时暂停领取新工作，运行中 task、无 Task 的手动实例或待续跑消息未清零则拒绝重启；支持识别手动拉取但尚未加载的代码，再完成依赖、迁移、前端构建和智能重启

### 项目与协作
- **项目管理** — 支持 clone 已有仓库（有 remote）和本地 git init（无 remote），创建任务时可直接新建项目
- **PR Monitor** — 以 exact-head CI 和隔离 Reviewer Panel 审核 GitHub PR；每条 Finding 可审计记录忽略/人工建议，或由 tool-free Task 生成限定范围的候选 diff。AI 候选必须先经后端下载回执绑定用户、Action 与 patch hash，再由用户明确确认，后端才会对仍匹配的 PR 源分支执行 exact-old compare-and-swap push；任何 Finding 操作都不能绕过 Panel Gate
- **PWA** — 手机浏览器 Add to Home Screen，原生 App 体验
- **Android App** — 通过 Capacitor 打包原生 APK，App 内可配置远程服务器地址
- **主题切换** — v2 主题系统：现代深色（默认，Multica 风格）/ 现代浅色（tonal zinc 灰调分层）/ 飞书（官方色板 + 真实 App 截图取色实证：白底为主 + 经典飞书蓝 #3370FF + N 系中性色 + 低边框风，与浅色主题以「白 vs 灰」区分，飞书客户端式窄图标 rail + IconPark 双色图标集）/ 苹果（apple-design skill 驱动：iOS systemGray 中性色 + apple.com CTA 蓝 #0071E3 + 系统字体优先 + 毛玻璃顶栏 + 按压反馈 + macOS Settings 式侧栏与 Ionicons 图标集，尊重 reduced-motion/transparency），v1 的经典深色、海蓝、森林、莓红完整保留为 Legacy 组，偏好持久化
- **Token 认证** — Bearer Token 保护所有 API，安全远程访问
- **远程访问** — 通过 Cloudflare Tunnel 隧道暴露到公网

## 任务生命周期

Dispatcher 只负责分配任务和判断成败，Claude Code 自主完成整个工作流：

1. **领取任务** — Dispatcher dequeue，status=in_progress
2. **创建工作区** — Claude Code 自主创建 git worktree，status=executing
3. **实现功能** — Claude Code 在 worktree 中编写代码
4. **提交代码** — Claude Code 自主 `git add` + `git commit`
5. **Merge + 测试** — Claude Code 自主 `git fetch origin && git merge origin/main` + 运行测试
6. **合并到 main** — Claude Code 自主 rebase + merge + push（有冲突自行解决）
7. **标记完成** — Claude Code 更新文档
8. **清理** — Claude Code 自主清理 worktree 和 task 分支
9. **经验沉淀** — Claude Code 在 PROGRESS.md 记录经验

**状态流转：**
```
pending → in_progress → executing → completed
                           ↓
                        (fail)
                           ↓
                        pending (retry)
```

## 技术栈

| 层 | 技术 |
|---|------|
| Backend | Python 3.11+, FastAPI, SQLAlchemy (async), Alembic |
| Database | SQLite (默认) / PostgreSQL / MySQL |
| Frontend | React 19, Vite, Tailwind CSS v4, TypeScript, Lucide icons |
| PTY | claude-pty（Claude Code 持久会话框架） |
| 实时通信 | WebSocket (原生, channel-based pub/sub) |
| MCP | FastMCP server (Skills / Monitor Agent) |
| 语音 | OpenAI Whisper API |
| 远程 | Cloudflare Tunnel / ngrok |
| Worker | AWS EC2, boto3, rsync, SSH |

## 项目结构

```
claude-manager/
├── backend/
│   ├── main.py                  # FastAPI 入口, 全局单例, 静态文件服务
│   ├── config.py                # Pydantic BaseSettings (.env)
│   ├── database.py              # SQLAlchemy async engine + session
│   ├── api/                     # REST + WebSocket 路由
│   │   ├── tasks.py             # 任务 CRUD + plan 审批 + conflict 解决
│   │   ├── plans.py             # 关联 Plan 历史、stale、revision、执行 Task
│   │   ├── plan_resources.py    # 一等 Plan/Version/Run/Input/Application API
│   │   ├── chat.py              # 多轮对话 (基于 task, --resume)
│   │   ├── instances.py         # 实例 CRUD + Ralph Loop + Dispatcher 端点
│   │   ├── projects.py          # Project CRUD + git clone
│   │   ├── monitor.py           # Monitor Session CRUD + 子 agent endpoints
│   │   ├── pool.py              # Claude 账号池 status/usage/reload/clear-cooldown
│   │   ├── pr_monitor.py        # PR Monitor CRUD + GitHub webhook
│   │   ├── workers.py           # 分布式 Worker CRUD + stop/start/destroy/retry
│   │   ├── sub_agents.py        # 子 Agent summary API
│   │   ├── ask_user.py          # ask_user 拦截 + 答案回流
│   │   ├── settings.py          # 运行时设置 API
│   │   ├── system.py            # 健康检查 + 统计 + 一键更新
│   │   ├── ws.py                # WebSocket 端点
│   │   ├── voice.py             # Whisper 语音转文字
│   │   └── auth.py              # Token 登录
│   ├── middleware/auth.py       # Bearer token 认证中间件
│   ├── hooks/
│   │   └── ask_user_hook.py     # AskUserQuestion PreToolUse hook 脚本
│   ├── models/                  # SQLAlchemy ORM 模型
│   │   ├── task.py              # Task (session_id, last_cwd, project_id, enabled_skills, effort_level...)
│   │   ├── plan_agent.py        # Planner/Reviewer Run + Step 审计
│   │   ├── plan.py              # Plan/Version/Input/Application 聚合模型
│   │   ├── instance.py          # Claude Code 实例
│   │   ├── project.py           # Project (name, git_url, local_path)
│   │   ├── sub_agent.py         # SubAgentSession + SubAgentReport (通用子 agent)
│   │   ├── pr_monitor.py        # MonitoredRepo + PRReview
│   │   ├── worker.py            # 分布式 Worker (EC2 实例 + bootstrap 状态机)
│   │   ├── log_entry.py         # 执行日志
│   │   └── worktree.py          # Git worktree 跟踪
│   ├── schemas/                 # Pydantic 请求/响应模型
│   ├── mcp/                     # MCP Servers
│   │   ├── ccm_skills_server.py         # 主 Agent MCP: create_monitor / check_monitors / stop_monitor
│   │   └── ccm_monitor_agent_server.py  # 子 Agent MCP: report_status / mark_complete / get_context
│   └── services/                # 核心业务逻辑
│       ├── dispatcher.py        # 全局调度器 (9 步任务生命周期 + goal + monitor)
│       ├── instance_manager.py  # 子进程生命周期 (launch/stop/consume, MCP 注入)
│       ├── claude_pool.py       # 多账号池 (限速检测/自动切换/session 迁移/额度查询)
│       ├── goal_evaluator.py    # Goal 条件评估器 (claude -p 子进程)
│       ├── plan_agent_runner.py # 严格只读 Planner/Reviewer pipeline
│       ├── plan_tasks.py        # Plan 上下文、repo 指纹、stale 与附件校验
│       ├── plan_service.py      # 版本状态机、输入、审批、Worker outcome 导入
│       ├── mcp_config.py        # MCP config 动态生成
│       ├── tmp_space_manager.py # /tmp 容量/inode 看门狗与白名单安全清理
│       ├── cloud_provider.py    # AWS EC2 Provider (Worker 实例创建/启停/销毁)
│       ├── worker_provisioner.py # Worker 全生命周期 (创建→bootstrap→ready)
│       ├── worker_proxy.py      # 任务转发到 Worker
│       ├── worker_relay.py      # Manager↔Worker WebSocket 事件中继
│       ├── task_migrator.py     # 任务在本机↔Worker 之间迁移
│       ├── update_service.py    # 更新/修复/回滚事务 + 智能重启
│       ├── deployment_start_guard.py # 部署 lease、启动守卫与跨进程 task fence
│       ├── stream_parser.py     # NDJSON stream-json 解析
│       ├── task_queue.py        # 优先级任务队列
│       ├── worktree_manager.py  # Git worktree 管理 + rebase + push
│       ├── pr_review_service.py # PR 审核 prompt 构建 + 状态回查
│       ├── ask_user.py          # ask_user 注册表 + Future 管理
│       ├── ask_user_settings.py # ask_user hook 注入/移除
│       ├── ws_broadcaster.py    # WebSocket channel 广播
│       ├── whisper_client.py    # 语音转文字
│       └── backup_service.py    # 数据库备份 (可选)
├── frontend/
│   ├── public/                  # PWA manifest, service worker, icons
│   └── src/
│       ├── api/client.ts        # API 客户端 + 类型 (401 自动登出, 动态 base URL)
│       ├── api/ws.ts            # WebSocket 客户端 (指数退避重连)
│       ├── config/server.ts     # 远程服务器 URL 配置 (Capacitor/Android)
│       ├── config/theme.ts      # 主题注册表 (现代深/浅 + Legacy 组, meta theme-color 同步)
│       ├── pages/               # Dashboard, TasksPage, PlansPage, WorkersPage, PRMonitorPage, LoginPage...
│       ├── components/
│       │   ├── AskUserNotifications.tsx   # 全局 ask_user 弹窗通知
│       │   ├── Chat/ChatView.tsx          # 多轮对话 UI
│       │   ├── Chat/MonitorPanel.tsx      # Monitor 面板
│       │   ├── Chat/SubSessionIndicator.tsx
│       │   ├── Instances/                 # InstanceGrid, InstanceLog
│       │   ├── Tasks/                     # TaskForm, TaskList, TaskConfigBadge
│       │   ├── Layout/PoolDrawer.tsx      # Pool 额度抽屉
│       │   ├── PlanReview/                 # 一等 Plan action/history/detail/input UI
│       │   ├── System/                    # UpdatePanel
│       │   └── Voice/VoiceButton.tsx      # 语音录入
│       └── hooks/useWebSocket.ts
├── scripts/
│   ├── dev.sh                   # 一键启动开发环境
│   ├── setup.sh                 # Worker SSH Key + 环境初始化
│   ├── refresh_pty.sh           # 刷新 claude-pty 依赖
│   ├── start_all.sh             # 生产环境启动脚本
│   └── tunnel.sh                # ngrok/cloudflare 隧道
├── docs/
│   └── worker-deployment-guide.md  # Worker 部署指南
├── pyproject.toml
└── .env
```

## 快速开始

### 前置条件

- macOS / Linux（推荐 Ubuntu 22.04+，支持 EC2 部署）
- Python 3.11+
- Node.js 18+
- [uv](https://docs.astral.sh/uv/) — Python 包管理器
- [Claude Code CLI](https://docs.anthropic.com/en/docs/claude-code) 已安装并登录（`claude auth login`）
- Google Chrome + Xvfb（号池自动登录需要，服务器部署时安装）

### 安装

```bash
git clone https://github.com/zjw49246/Claude-Code-Manager.git && cd Claude-Code-Manager

# 后端依赖（使用 uv）
uv sync

# 如需 PostgreSQL 支持
uv sync --extra postgres

# 如需 MySQL 支持
uv sync --extra mysql

# 前端依赖
cd frontend && npm install && cd ..

# 配置
cp .env.example .env
# 编辑 .env，设置：
#   AUTH_TOKEN=你的访问密码
#   OPENAI_API_KEY=sk-...（语音功能需要）
#   WORKSPACE_DIR=~/Projects（项目工作区根目录）
```

### 启动

```bash
# 一键启动
./scripts/dev.sh

# 或分别启动
uv run uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload &
cd frontend && npx vite --host &
```

访问 http://localhost:5173，输入 `AUTH_TOKEN` 登录。

启动后 Dispatcher 会自动创建 worker 实例并开始调度。

### Android App 打包

```bash
cd frontend

# 安装 Capacitor（已在 package.json 中）
npm install

# 构建 Web 资源
npm run build

# 首次 clone：生成未纳入 Git 的原生 Android 工程；后续打包跳过此步
npx cap add android

# 同步 Web 资源与原生依赖，然后打包 APK
npx cap sync android
JAVA_HOME="/Applications/Android Studio.app/Contents/jbr/Contents/Home" \
  android/gradlew -p android assembleDebug

# APK 位于 android/app/build/outputs/apk/debug/app-debug.apk
```

首次打开 App 在登录页展开 "Server URL" 输入服务器地址（如 Cloudflare Tunnel URL），然后输入 Token 登录。

## 数据库

默认使用 SQLite，也支持 PostgreSQL 和 MySQL。通过 `.env` 中的 `DATABASE_URL` 切换：

```bash
# SQLite（默认）
DATABASE_URL=sqlite+aiosqlite:///./claude_manager.db

# PostgreSQL（需安装: uv sync --extra postgres）
DATABASE_URL=postgresql+asyncpg://user:pass@host:5432/claude_manager

# MySQL（需安装: uv sync --extra mysql）
DATABASE_URL=mysql+aiomysql://user:pass@host:3306/claude_manager
```

### Schema 迁移（Alembic）

使用 Alembic 管理 schema 版本。**启动时自动执行 `alembic upgrade head`**，无需手动操作。

```bash
uv run alembic upgrade head    # 手动升级（通常不需要）
uv run alembic current         # 查看当前版本
uv run alembic history         # 查看历史
```

### 数据迁移

本仓库不提供跨 SQLite、PostgreSQL 和 MySQL 的数据搬运脚本。跨数据库迁移时，请先停止写入并完整备份，再使用源数据库和目标数据库官方提供的导出、导入或复制工具（或经过独立验证的 ETL）。迁移后应运行 Alembic、核对行数与关键关联，并在切换 `DATABASE_URL` 前完成恢复演练。

## 更新已部署的实例

### 方式一：一键更新（推荐）

通过 API 或前端 System 面板触发：

```bash
curl -X POST http://localhost:8000/api/system/update \
  -H "Authorization: Bearer $AUTH_TOKEN"
```

每次新开、刷新或重新登录 CCM 页面后约 1 秒会自动检查一次，之后每小时检查；后台检查只执行 `fetch/dry_run`，**不会自动拉取或重启**。发现远端新提交，或检测到有人手动拉取了代码但当前服务仍运行旧版本时，会在页面顶部显示非阻塞通知，不遮挡页面，也不影响正常操作；只有点击“查看详情”才打开更新弹窗。同一页面生命周期内同一版本只提醒一次，下次重新打开页面仍会再次提醒。远端检查失败通常保持静默，但不会掩盖本地已经拉取、仍需重启加载的代码。

真正更新时自动执行：git pull → 刷新 PTY 依赖 → 数据库迁移 → 重建前端 → 智能重启（自动检测 systemd 服务名 `SERVICE_NAME`）。更新源优先使用目标分支配置的 tracking remote（例如本仓库的 `upstream/main`），没有 tracking remote 时回退 `origin`。

状态检查会分别显示当前进程实际加载的 commit、磁盘 `HEAD`、数据库 Alembic current/head。三者含义不同：代码已经拉取成功，只说明磁盘是新版；依赖、前端产物或迁移失败时，旧服务仍可能继续运行，所以“远端与本地代码一致”不能作为部署完成的判断。此时页面会提供「修复并重新部署」，重新执行当前磁盘版本的依赖同步、PTY 刷新、前端安装/构建、数据库确认/迁移和受控重启；只有代码与数据库都已确认一致时才开放轻量重启。即使一切一致，详情页仍保留「手动重启」按钮。

更新、修复和受控重启还要求 Git 工作树干净，包括 staged、unstaged 和未被 `.gitignore` 排除的 untracked 文件；否则新进程可能加载无法由 commit 证明的代码。数据库、日志、备份、构建产物等运行时文件应通过 `.gitignore` 明确排除。

```bash
# 对当前磁盘版本补齐完整部署
curl -X POST http://localhost:8000/api/system/update/repair \
  -H "Authorization: Bearer $AUTH_TOKEN" -H "Content-Type: application/json" \
  -d '{}'

# 已证明代码/数据库一致时，仅重启服务
curl -X POST http://localhost:8000/api/system/restart \
  -H "Authorization: Bearer $AUTH_TOKEN"
```

更新事务使用仓库内的持久 deployment lease 记录 token、worker PID 身份、期望 commit 和迁移结果。服务启动前会先检查该 lease；若上一次迁移或回滚没有完整结束，CCM 只启动一个不访问业务数据库的维护界面，Dispatcher、Worker 和普通 API 不会启动，管理员仍可查看状态并执行修复/回滚。迁移脚本会在服务完全停止后重新生成 SQLite 快照；数据库恢复、代码回退、依赖恢复或前端恢复任一步失败，服务保持停止，避免启动代码、依赖和 schema 混合的版本。

一键更新和自动修复仅支持文件型 SQLite，因为 CCM 必须能在停服后制作并验证快照，才能承诺自动回滚。PostgreSQL/MySQL 等外部数据库仍可在版本完全一致时使用「重启」，但更新和修复必须由管理员先完成数据库备份，再按数据库自己的迁移/恢复流程部署。迁移失败通常来自数据库 schema 漂移、迁移脚本本身报错、数据库文件被其他进程占用、权限/磁盘空间问题或新服务未在健康检查期限内启动；页面和 deployment status 会保留失败步骤与日志，不应通过反复点击更新绕过。

为避免中断任务，更新开始前会关闭统一的任务启动门禁：普通 Dispatcher、Worker 转发、聊天/Monitor 续跑、RalphLoop 和手动 Instance 运行都不能越过维护窗口。`in_progress/executing` task、无 Task 关联但仍为 `running` 的 prompt-only 实例，或已经进入队列、尚未启动的续跑消息都会阻止停服；stop-session 清空消息时会在同一门禁内同步移除已经失效的队列 blocker，避免之后出现幽灵阻塞。若更新期间收到新的续跑消息，本次重启会取消并恢复调度，消息不会因进程重启而从内存队列丢失。更新与回滚请求还共用同一个操作准入锁：一次只能放行一个操作，回滚使用的 commit 和备份会在锁内固定，不能被并发更新替换。所有更新、迁移、手动拉取后的快速重启和回滚都在同一门禁内完成最后一次 blocker 查询，并在查询成功后不再经过异步等待，直接提交停服操作。任务完成后点击「重新检查」即可继续。手动 `git pull` 后触发更新时，系统会以服务实际加载的旧 commit 为基线补齐部署步骤，而不是只做一次盲目重启。

如果任务列表里已经找不到任务、更新弹窗却仍显示运行阻断项，可点击「重新核对运行状态」。系统会暂停新领取并由 Dispatcher 对照内存中的真实 lifecycle/process 与数据库 Task↔Instance 所有权：明确死亡且关系一致的残留会安全收敛；多 owner、关系不一致或 PID 仍可能存活时不会猜测重放，而会终止损坏状态或继续保留阻断证据。正在准备 launch 的任务、当前进程拥有的任务、Monitor/子 Agent 不会被误清；远端共享任务镜像不参与本机 stale recovery。

同一 checkout 被多个 CCM 进程使用时，任务领取还会持有仓库级共享文件锁，部署 claim 使用排他锁；部署拿到 lease 后会在任何 checkout、备份或依赖修改之前再次查询活动任务。这样即使另一个 CCM 恰好在第一次检查后提交了 task，也只会取消本次部署，不会一边运行任务一边改它脚下的环境。

stop-session 清理 per-task 消息时还会推进该队列的 cancellation generation。已经被 consumer 从队列取走、但尚未登记为 in-flight 的旧代次消息会被明确取消，不会在清理成功后再次启动；已经登记的真实 in-flight 工作仍保留为更新 blocker，直到其生命周期结束。

### 方式二：手动更新

```bash
git pull                      # 1. 拉取 CCM 最新代码
./scripts/refresh_pty.sh      # 2. 刷新 claude-pty 依赖（见下）
.venv/bin/alembic upgrade head  # 3. 数据库迁移
cd frontend && npm run build && cd ..  # 4. 重建前端
# 5. 重启服务（systemd / 手动）
```

> **为什么需要第 2 步**：`pyproject.toml` 里的 `claude-pty @ git+https://...` 是
> **安装时快照**——`git pull` 只更新 CCM 自己的代码，不会带来新的 PTY 框架代码。
> `scripts/refresh_pty.sh` 会对比已安装的 PTY commit 与远端 main HEAD，不一致时
> 自动重装（editable/本地开发安装会自动跳过）。

## 使用流程

### 基本流程

1. **Tasks** — 创建任务，选择已有项目或新建项目，填写 Prompt、优先级、Effort Level。可勾选 **Monitor** skill 赋予 Agent 后台监控能力
2. **Dispatcher** 自动分配任务到空闲 worker → Claude Code 自主完成所有工作（含 worktree 创建和清理） → 取下一个
3. 点击任务的 **Chat** 按钮，可以对已完成的任务继续追问
4. 启用 Monitor 的任务中，Agent 可自主创建持久监控子 Agent，Task 列表显示活跃子 Agent 数量
5. 可在任务卡片菜单添加一个关注标签，或直接点击任务卡片/Chat 顶栏中的标签修改；清空并保存即可移除

### Interactive Plans

Plan 是独立于 Task 的一等、版本化制品：

1. 在独立的 **Plans** 页面创建 standalone Plan，或在已有 Chat 的 **Plans** 面板创建多个互相独立的 related Plan。**Tasks** 页面只创建和展示真正的 Task。
2. Planner/Reviewer 路由只在全局 Settings 配置；每个新 Plan 冻结当时的 primary/fallback provider、model、effort 和轮数设置。
3. Planner 和 Reviewer 都可暂停同一个 Run 请求必要输入；单轮问题数量没有业务上限，每 Run 可暂停次数是独立的 `0–5` 全局设置。
4. 每次完整方案写入不可变 Version。Revise 在同一 Plan 下创建新 Run/Version，只有 Fork 才创建新 Plan。
5. Approve/Reject 绑定用户看到的 exact Version，不唤醒或改变原 Task/session。关联 Version 只有显式附到下一条真实消息时才应用一次；standalone Version 可显式创建普通 execution Task。

**Plans** 页面用 **Plans requiring action** 汇总待输入、待审批和待执行动作；下方目录支持
standalone/related、状态、Project、搜索和 Archived only 筛选。Archive 是可恢复的软归档，不会
删除 Plan、Version、Run 或问答历史。Plans 详情保留 Version
切换/比较、完整 Q&A/Run/route/repository 审计，以及旧 Version 已应用而新 Version 待审的双状态。
若对话、仓库或目标发生变化，操作会要求 stale 确认或因 hard conflict 明确阻断。

Planner/Reviewer 使用严格只读 transport。Codex 步骤复用账号的常驻 App Server，但使用终态即
删除的一次性只读 thread。Plan 创建请求、标题、Revise/Fork 请求和回答都会持久化，因此
API key、access token 和 private key 等高置信凭据必须存入 Settings → Secrets，Plan 文本中
只写引用名称。

### Goal Mode

创建任务时选择 Mode = `goal`，填写自然语言完成条件：
1. Claude Code 执行任务
2. 每 turn 结束后，轻量评估器（默认 `claude-haiku-4-5`）判断条件是否满足
3. 未满足则自动 `--resume` 继续执行，保持同一 session 的连续上下文
4. 达成目标后自动标记完成

### 语音输入

任务创建表单的标题和描述字段旁有 🎙 按钮，点击后录音，松开自动转文字填入。

### PR Monitor 前置条件

PR Monitor 的审核流程会在后端 shell out 调用 `gh pr view` / `gh pr review` / `gh pr merge`，使用前需满足：

1. **gh CLI 已认证**：运行后端的系统用户必须先执行 `gh auth login` 完成 GitHub 认证
2. **账号权限**：该 GitHub 账号需要对被监控仓库有 push / review 权限（auto-merge 还需要 merge 权限）
3. **PUBLIC_BASE_URL**：在 `.env` 中设置 `PUBLIC_BASE_URL`（如 `https://ccm.example.com`），PR Monitor 页面才能显示正确的 Webhook Payload URL

Panel 中的 Finding 操作是独立的审计流程：**Ignore** 和 **Human advice** 只保存决策或供下一次候选生成参考，不会把阻断项改成通过；**Generate AI fix** 会在与 Reviewer 相同的 tool-free 沙箱中读取后端冻结的 exact-head 单文件输入，只输出有界 unified diff，不会直接访问仓库或 GitHub。候选完成后必须从 CCM 后端下载 diff；后端签发并保存与当前用户、Action 和 SHA-256 绑定的下载回执，只有回传同一回执并再次确认 exact base/head/repo/ref 后才会创建 commit。push 以 captured head 为 expected-old 做原子 compare-and-swap，分支被删除、漂移或远端结果无法证明时会拒绝或进入可对账恢复，不覆盖他人提交。

同一 Finding 同时只有一个 active AI fix。自动 fix Task 的编辑、聊天、注入、重试、取消、停止和删除入口均被冻结；本机和 Worker 都由 Manager 按 exact generation 收口结果，Worker 成功前先补齐完整日志，失败或 Manager/Worker 崩溃后则从持久 Action/lease 恢复，不能靠内存状态重复生成或重复 push。详细状态机与安全边界见 [PR Monitor 权威设计](docs/pr-monitor-design.md)。

## 分布式 Worker

Worker 系统支持将任务分发到远程 EC2 实例执行，适合需要更多并行能力的场景。每个 Worker 是一台运行完整 CCM 的 EC2，拥有独立的 Claude 账号池。

**核心能力：**
- 水平扩展并发能力，每个 Worker 可配多个 Claude 账号
- 任务执行位置可实时切换（本机 / 任意 Worker），session 无缝衔接
- Worker 销毁时自动迁回全部任务和数据，不丢失上下文
- 前端零感知差异 — 远程任务与本地任务 UI/操作完全一致

**使用流程：**
1. **创建 Worker**：Workers 页面点 **+**，输入名称，系统自动创建 EC2 → 安装依赖 → 部署代码 → 启动服务
2. **分配账号**：Worker 详情页的号池面板添加 Claude 账号
3. **分配任务**：任务的 Config 面板 → "Run on" 下拉选择 Worker
4. **任务迁移**：运行中的任务可随时在本机和 Worker 之间迁移，session 自动同步
5. **关机/销毁**：Stop 保留实例数据（可重新 Start），Destroy 会先将所有任务迁回本机再终止实例

> 详细部署指南、前置条件、配置参数和故障排除见 **[docs/worker-deployment-guide.md](docs/worker-deployment-guide.md)**

## API

| 模块 | 端点 | 说明 |
|------|------|------|
| Projects | `GET/POST /api/projects` | 项目列表/创建 |
| | `GET/PUT/DELETE /api/projects/{id}` | 项目详情/更新/删除 |
| | `POST /api/projects/{id}/reclone` | 重新 clone |
| Project Todos | `GET/POST /api/projects/{id}/todos` | 项目待办列表/创建 |
| | `PATCH /api/projects/{id}/todos/{todo_id}` | 更新（含归档/恢复/排序） |
| | `DELETE /api/projects/{id}/todos/{todo_id}` | 永久删除 |
| Tasks | `GET/POST /api/tasks` | 任务列表/创建 |
| | `GET/PUT/DELETE /api/tasks/{id}` | 任务详情/更新/删除 |
| | `POST /api/tasks/{id}/cancel` | 取消任务 |
| | `POST /api/tasks/{id}/retry` | 重试任务 |
| | `POST /api/tasks/{id}/chat` | 发送追问消息 |
| | `GET /api/tasks/{id}/chat/history` | 获取对话历史 |
| | `POST /api/tasks/{id}/permissions/{rid}` | 回复权限请求 |
| | `POST /api/tasks/{id}/ask-user/{rid}` | 回复 ask_user 提问 |
| | `GET /api/tasks/{id}/ask-user/pending` | 获取待回复提问 |
| Plans | `GET/POST /api/plans` | Plan 目录/创建 |
| | `GET/PATCH /api/plans/{id}` | 详情、重命名、归档或恢复 |
| | `POST /api/plans/{id}/runs` | 创建 Revise/Refresh/Retry Run |
| | `POST /api/plans/{id}/fork` | Fork 为新 Plan |
| | `GET /api/plans/{id}/versions` | 获取不可变 Version 历史 |
| | `GET /api/plan-versions/{id}` | 获取 exact Version |
| | `GET /api/plan-versions/{id}/staleness` | 检查 Version 上下文是否过期 |
| | `POST /api/plan-versions/{id}/approve` | 批准 exact Version |
| | `POST /api/plan-versions/{id}/reject` | 拒绝 exact Version |
| | `POST /api/plan-runs/{run_id}/input-requests/{request_id}/answer` | 回答必要输入并恢复同一 Run |
| | `POST /api/plan-versions/{id}/create-execution-task` | 从 standalone Version 创建执行 Task |
| Instances | `GET/POST /api/instances` | 实例列表/创建 |
| | `DELETE /api/instances/{id}` | 删除实例 |
| | `POST /api/instances/{id}/stop` | 停止实例 |
| | `POST /api/instances/{id}/run` | 手动执行 |
| | `GET /api/instances/{id}/logs` | 获取日志 |
| Monitor | `POST /api/tasks/{id}/monitor-sessions` | 创建 monitor 子 session |
| | `GET /api/tasks/{id}/monitor-sessions` | 列出 monitor sessions |
| | `DELETE /api/tasks/{id}/monitor-sessions/{sid}` | 停止 monitor session |
| | `POST /api/tasks/{id}/monitor-sessions/{sid}/checks` | 子 agent 报告状态 |
| | `POST /api/tasks/{id}/monitor-sessions/{sid}/complete` | 子 agent 标记完成 |
| Sub-Agents | `GET /api/tasks/{id}/sub-agents/summary` | 子 agent 按类型汇总 |
| Workers | `GET/POST /api/workers` | Worker 列表/创建 |
| | `GET /api/workers/{id}` | Worker 详情 |
| | `GET /api/workers/{id}/logs` | Bootstrap 日志 |
| | `POST /api/workers/{id}/stop` | 关机 |
| | `POST /api/workers/{id}/start` | 开机 |
| | `POST /api/workers/{id}/destroy` | 销毁（自动迁回任务） |
| | `POST /api/workers/{id}/retry` | 重试 Bootstrap |
| | `GET/POST /api/workers/{id}/pool/*` | Worker 号池管理 |
| Dispatcher | `GET /api/dispatcher/status` | 调度器状态 |
| | `POST /api/dispatcher/start` | 启动调度器 |
| | `POST /api/dispatcher/stop` | 停止调度器 |
| Pool | `GET /api/pool/status` | 账号池状态（可用/冷却/禁用） |
| | `GET /api/pool/usage` | 账号池状态 + 每账号额度利用率（5h/7d） |
| | `POST /api/pool/reload` | 重新加载账号配置 |
| | `POST /api/pool/accounts/{id}/clear-cooldown` | 清除账号冷却 |
| Voice | `POST /api/voice/transcribe` | 语音转文字 |
| System | `GET /api/system/health` | 健康检查 |
| | `GET /api/system/stats` | 统计信息 |
| | `POST /api/system/update` | 一键更新重启 |
| | `POST /api/system/update/reconcile` | 安全核对并收敛幽灵运行状态 |
| | `POST /api/system/update/repair` | 对当前磁盘版本执行完整修复部署 |
| | `POST /api/system/restart` | 代码与数据库一致时受控重启 |
| WebSocket | `ws://host/ws` | 实时推送（subscribe channel） |
| Auth | `POST /api/auth/login` | Token 登录 |

所有 API（除 health、login、github webhook）需要 `Authorization: Bearer <token>` 头。

## 配置

### 基础配置

| 环境变量 | 默认值 | 说明 |
|----------|--------|------|
| `AUTH_TOKEN` | (必填) | API 认证 Token |
| `PORT` | `8000` | 主服务监听端口 |
| `PUBLIC_BASE_URL` | （空） | 部署的公网地址（如 `https://ccm.example.com`） |
| `OPENAI_API_KEY` | (可选) | 语音功能所需 |
| `DATABASE_URL` | `sqlite+aiosqlite:///./claude_manager.db` | 数据库连接（支持 SQLite/PostgreSQL/MySQL） |
| `FEISHU_APP_ID` / `FEISHU_APP_SECRET` | （空） | 飞书 OAuth 应用凭据 |
| `FEISHU_OAUTH_STATE_SECRET` | （空） | 可选独立 OAuth state 签名密钥；留空时安全复用 `FEISHU_APP_SECRET` |
| `FEISHU_OAUTH_STATE_TTL_SECONDS` | `600` | 飞书 OAuth state 有效期（上限 3600 秒） |
| `SMTP_HOST` / `SMTP_PORT` | `smtp.feishu.cn` / `465` | 注册验证码 SMTP 服务 |
| `SMTP_USER` / `SMTP_PASSWORD` | （空） | 注册验证码凭据；必须由部署环境提供 |
| `SMTP_FROM` | （空） | 可选发件地址，留空时使用 `SMTP_USER` |
| `VERIFICATION_CODE_CAPACITY` | `4096` | 单进程内存中待验证验证码及限流事件的容量上限 |
| `VERIFICATION_CODE_RATE_WINDOW_SECONDS` | `600` | 验证码发送限流窗口（秒） |
| `VERIFICATION_CODE_EMAIL_LIMIT` / `VERIFICATION_CODE_IP_LIMIT` | `5` / `30` | 每个窗口内单邮箱/来源 IP 的发送上限 |
| `VERIFICATION_CODE_RESEND_COOLDOWN_SECONDS` | `60` | 同一邮箱两次发送的最短间隔（秒） |
| `VERIFICATION_CODE_SMTP_CONCURRENCY` | `4` | 单进程同时进行的 SMTP 投递上限 |
| `WORKSPACE_DIR` | `~/Projects` | 项目 clone 目标目录 |
| `MAX_CONCURRENT_INSTANCES` | `5` | 最大并发 worker 数 |
| `AUTO_START_DISPATCHER` | `true` | 启动时自动开始调度 |
| `TASK_TIMEOUT_SECONDS` | `1800` | 单个任务最长执行时间（秒） |
| `SERVICE_NAME` | (自动检测) | systemd 服务名，一键更新重启时使用 |

新安装不会生成固定口令管理员：首个完成邮箱验证的注册用户成为
`super_admin`。当配置了 `AUTH_TOKEN` 且尚无 active 用户时，注册表单还必须
在可选的 “Bootstrap Token” 输入框填写该 Token，避免公网访客抢占首个管理员。
升级会自动禁用旧版本曾写入的共享默认管理员；单 Token 部署可继续使用
`AUTH_TOKEN` 进入，团队部署需配置 `SMTP_*` 后注册真实管理员。验证码服务会
按邮箱和 `Request.client.host` 限速；多进程/多副本部署还应在反向代理或网关
配置共享限流，因为应用内状态按进程隔离。

### PTY 模式

| 环境变量 | 默认值 | 说明 |
|----------|--------|------|
| `USE_PTY_MODE` | `true` | 启用 PTY 持久会话模式（false 则用 `claude -p` 一次性进程） |

### 瞬时过载重试

| 环境变量 | 默认值 | 说明 |
|----------|--------|------|
| `TRANSIENT_RETRY_ENABLED` | `true` | 启用瞬时 429/过载自动重试 |
| `TRANSIENT_RETRY_MAX` | `5` | 最大重试次数 |
| `TRANSIENT_RETRY_BASE_DELAY` | `10` | 基础退避延迟（秒） |
| `TRANSIENT_RETRY_MAX_DELAY` | `120` | 最大退避延迟（秒） |

### ask_user 拦截

| 环境变量 | 默认值 | 说明 |
|----------|--------|------|
| `ASK_USER_ENABLED` | `true` | 启用 AskUserQuestion 拦截（false 时自动移除已注入的 hook） |
| `ASK_USER_TIMEOUT` | `1800` | 等待用户回答的超时时间（秒） |

### `/tmp` 临时空间保护

| 环境变量 | 默认值 | 说明 |
|----------|--------|------|
| `TMP_CLEANUP_ENABLED` | `true` | 启用宿主及共享 Docker `/tmp` 压力检查与安全清理 |
| `TMP_CLEANUP_USAGE_THRESHOLD` | `0.80` | 容量或 inode 使用率达到此值即触发 |
| `TMP_CLEANUP_INTERVAL_SECONDS` | `10800` | 后台检查间隔（秒，默认 3 小时） |
| `TMP_CLEANUP_MIN_AGE_SECONDS` | `21600` | 临时产物至少闲置多久才可清理（默认 6 小时） |

服务启动时先检查一次，随后由后台看门狗按间隔检查。容量字节或 inode 任一达到
阈值即触发；低于阈值时不会扫描候选，也不会影响 Agent。一旦触发，本轮会处理
全部符合条件的候选，不设置清理目标线。

清理器不会执行通配式 `rm /tmp/*`。它只处理固定命名白名单中的 Sub-Agent
过期日志、唯一命名的 skill/下载/讨论临时文件，并要求同一服务用户、同一文件
系统、超过最小年龄的普通文件；宿主目录一律不递归删除。无法从清理进程独立
证明空闲的 session/workspace 迁移 staging、未知文件、X11/Xvfb、登录浏览器
资料以及 `/tmp/ccm-update-*` 更新/回滚证据始终保留。安全候选不足时只记录
告警并在下个周期重试，不会扩大删除范围，也不会创建额外的模型任务。

共享 Docker 执行模式中的 `/tmp` 是容器内独立的 2GB tmpfs，也使用相同阈值。
每个容器 Agent 从创建子进程前到完整回收后代都持有共享 lease；达到阈值时，
只有取得独占 lease 且能证明容器除固定 PID 1 与清理进程外完全空闲，才会清空
这个可丢弃的私有 tmpfs。新建或因原有配置变化而重建的容器使用 Docker `--init`
回收 Agent 遗留的孤儿进程；现有容器不会仅为补 `--init` 而被强制重建。活跃
Agent、未知 PID、权限不足或清后仍达到触发线都会阻止新容器 Agent，绝不会退回
宿主裸进程。远程 Worker 升级到同一版本后会运行自己的宿主看门狗和相同容器门禁。

### 号池配置

| 环境变量 | 默认值 | 说明 |
|----------|--------|------|
| `POOL_ENABLED` | `true` | 启用 Claude 账号池 |
| `POOL_CONFIG_PATH` | `~/.claude-pool/accounts.json` | 账号池配置文件路径 |
| `POOL_COOLDOWN_SECONDS` | `300` | 撞限账号的冷却时长（秒） |

号池默认启用。首次启动时，如果 `accounts.json` 不存在但 `~/.claude/.credentials.json` 有有效凭证，系统会自动将默认账号加入号池。

通过 Header 右侧的 **Pro** 按钮打开号池面板，可以：
- 查看每个账号的 5h/7d 额度利用率
- 点击 **+** 添加新账号（需要邮箱 + 接码 token）
- 刷新 OAuth Token / 重新登录
- 手动切换首选账号

多账号时，撞限或认证失败会自动换号，通过硬链接 session 目录实现无缝 `--resume`。

### 账号自动登录浏览器

| 环境变量 | 默认值 | 说明 |
|----------|--------|------|
| `CCM_XVFB_DISPLAY` | `:99` | 自动登录专用 X display；同机多套 CCM 必须不同 |
| `CCM_LOGIN_TMPDIR` | `~/.cache/ccm/login-tmp` | Chrome profile/诊断文件的磁盘目录，避免使用 RAM-backed `/tmp` |
| `CCM_LOGIN_MIN_AVAILABLE_MB` | `512` | 可用内存低于此值时拒绝启动新登录浏览器 |
| `CCM_LOGIN_MIN_TEMP_FREE_MB` | `512` | 登录临时目录可用空间下限 |

Claude/Codex 自动登录共享一个 Xvfb manager 和登录锁。manager 使用私有
Xauthority、跨进程 display 锁和实际 display 就绪探测，并持久记录 Xvfb 的
PID/start time 与 socket inode。只有能证明属于已死亡 manager owner 的残留
socket 才会被清理；未知或已被替换的 socket 继续 fail-closed。Chrome 每次使用
独立 profile 和动态 DevTools 端口，并用该 profile 的 `DevToolsActivePort`
校验 browser websocket identity，不会再连接固定端口上的孤儿 Chrome。

### Worker（分布式执行节点）

| 环境变量 | 默认值 | 说明 |
|----------|--------|------|
| `WORKER_SSH_KEY_PATH` | (必填) | SSH 私钥 `.pem` 文件路径 |
| `WORKER_SSH_USER` | `ubuntu` | Worker EC2 的 SSH 用户名 |
| `WORKER_ENABLED` | `true` | 是否启用 Worker 功能 |
| `WORKER_INSTANCE_TYPE` | (继承 Manager) | 覆盖 Worker 的 EC2 实例类型 |
| `WORKER_IMAGE_ID` | (继承 Manager) | 覆盖 Worker 的 AMI ID |

> 完整 Worker 配置和前置条件见 [Worker 部署指南](docs/worker-deployment-guide.md)

### Git 相关

| 环境变量 | 默认值 | 说明 |
|----------|--------|------|
| `MERGE_PUSH_RETRIES` | `3` | rebase + push 最大重试次数 |
| `AUTO_PUSH_TO_ORIGIN` | `true` | 完成后是否自动 push |

### 数据库自动备份（可选）

集成 [auto-backup](https://github.com/zjw49246/auto-backup)，支持定期备份 SQLite 数据库到本机、AWS S3 或阿里云 OSS。
PostgreSQL/MySQL 请使用对应数据库的原生备份工具；内置调度器会拒绝把外部数据库 URL 当成本地文件。

| 环境变量 | 默认值 | 说明 |
|----------|--------|------|
| `BACKUP_ENABLED` | `false` | 设为 `true` 启用备份 |
| `BACKUP_TYPE` | `local` | 目标类型：`local` / `s3` / `oss` |
| `BACKUP_INTERVAL_SECONDS` | `3600` | 备份间隔（秒） |
| `BACKUP_MAX_COPIES` | `10` | 保留的最大备份份数 |
| `BACKUP_DESTINATION_PATH` | `` | （local）备份目标目录 |
| `BACKUP_S3_BUCKET` | `` | （s3）S3 桶名 |
| `BACKUP_S3_REGION` | `` | （s3）AWS 区域 |
| `BACKUP_S3_ACCESS_KEY` | `` | （s3）AWS Access Key ID |
| `BACKUP_S3_SECRET_KEY` | `` | （s3）AWS Secret Access Key |
| `BACKUP_OSS_ENDPOINT` | `` | （oss）OSS Endpoint |
| `BACKUP_OSS_BUCKET` | `` | （oss）OSS 桶名 |
| `BACKUP_OSS_ACCESS_KEY` | `` | （oss）阿里云 Access Key ID |
| `BACKUP_OSS_SECRET_KEY` | `` | （oss）阿里云 Access Key Secret |

**示例（本地备份）：**
```env
BACKUP_ENABLED=true
BACKUP_TYPE=local
BACKUP_DESTINATION_PATH=/mnt/backup/claude-manager
BACKUP_INTERVAL_SECONDS=3600
BACKUP_MAX_COPIES=10
```

## 同一台机器部署多个实例

可以在同一台机器上部署多个 Claude Code Manager 实例，分别服务不同用户/团队，推送到不同 GitHub 账号的仓库。

### 1. 准备独立的 `.env`

每个实例需要独立的端口、Token 和数据库：

```env
# 实例 A（端口 8000）
AUTH_TOKEN=token-for-user-a
PORT=8000
DATABASE_URL=sqlite+aiosqlite:///./claude_manager_a.db

# 实例 B（端口 8002）
AUTH_TOKEN=token-for-user-b
PORT=8002
DATABASE_URL=sqlite+aiosqlite:///./claude_manager_b.db
```

### 2. 配置 Git 凭据（关键）

每个实例可能需要推送到不同 GitHub 账号的仓库。在前端「全局 Git 设置」（Projects 页面齿轮按钮）中配置：

**推荐同时填写 SSH 和 HTTPS 凭据**，系统会根据 remote URL 协议自动选用：

| 字段 | 说明 |
|------|------|
| Author name / email | git commit 的作者信息 |
| SSH private key path | 如 `/Users/you/.ssh/id_ed25519_account_b` |
| HTTPS Username | GitHub 用户名 |
| HTTPS Token | GitHub Personal Access Token (PAT) |

**注意事项**：
- SSH key 在 GitHub 上是全局唯一的，一个公钥只能绑定一个账号
- macOS Keychain 的 `osxkeychain` 会缓存旧账号凭据，系统已自动绕过（`GIT_CONFIG_GLOBAL=/dev/null`）
- HTTPS Token 必须由目标仓库的**所有者账号**生成，而非本机账号

### 3. 为不同 GitHub 账号生成 SSH Key

```bash
# 为账号 A 生成
ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519_account_a -C "account-a@github" -N ""

# 为账号 B 生成
ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519_account_b -C "account-b@github" -N ""
```

在 `~/.ssh/config` 中配置 Host 别名：

```
Host github-account-a
  HostName github.com
  User git
  IdentityFile ~/.ssh/id_ed25519_account_a
  IdentitiesOnly yes

Host github-account-b
  HostName github.com
  User git
  IdentityFile ~/.ssh/id_ed25519_account_b
  IdentitiesOnly yes
```

将公钥分别添加到对应的 GitHub 账号。

### 4. Cloudflare Tunnel 路由

在 `~/.cloudflared/config.yml` 中为每个实例配置不同的子域名：

```yaml
ingress:
  - hostname: user-a.yourdomain.com
    service: http://localhost:8000
  - hostname: user-b.yourdomain.com
    service: http://localhost:8002
  - service: http_status:404
```

添加 DNS 路由：
```bash
cloudflared tunnel route dns <tunnel-name> user-a.yourdomain.com
cloudflared tunnel route dns <tunnel-name> user-b.yourdomain.com
```

### 5. 启动

```bash
# 构建前端（共用）
cd frontend && npm run build && cd ..

# 启动实例 A
PORT=8000 uv run uvicorn backend.main:app --host 0.0.0.0 --port 8000 &

# 启动实例 B（使用实例 B 的 .env）
PORT=8002 uv run uvicorn backend.main:app --host 0.0.0.0 --port 8002 &

# 启动 Cloudflare Tunnel
cloudflared tunnel run <tunnel-name>
```

## 架构要点

- **GlobalDispatcher**：只负责分配任务、启动 Claude Code、判断成败。所有 git 操作全由 Claude Code 自主完成
- **Claude Code 集成**：默认 PTY 模式（常驻交互会话，多轮免冷启动）；可切换为 `claude -p` 一次性进程模式（`USE_PTY_MODE=false`）
- **进程超时保护**：任务执行超过 `TASK_TIMEOUT_SECONDS`（默认 30 分钟）后自动 kill，防止进程挂死
- **多轮对话**：session_id 绑定在 Task 上，follow-up 时使用 `--resume <session_id>` 续接会话
- **Codex 共享传输停止**：`stop-session` 只停止目标 turn。若精确中断暂时无法确认，且同一账号 app-server 还在服务其他 turn 或已准入请求，API 返回 409 并保留原任务运行证据，可稍后重试；不会为停一个任务杀掉同账号的其他任务
- **Codex Fast**：`Task.codex_service_tier` 持久保存 `default|priority`。Standard 会显式清除会话残留的 Fast tier；Fast 必须同时通过当前账号 `model/list` 能力检查、app-server 显式 priority 准入，以及 CCM loopback Responses 代理对上游 `response.created.response.service_tier=priority` 的实际响应验证。成功 SSE 在验证前不会释放；缺字段、不一致、非 2xx、未知 lineage 或代理不可用都会明确失败，且 Fast 禁止回退 `codex exec`。日志与聊天事件会记录 `actual_service_tier_verified=true` 和上游 response id。Fast Goal evaluator 使用与任务相同的模型并走同一实际 tier 证明链路；当前 Distill 无法提供同等证明，因此 Fast Task 会在 Distill 执行前明确返回 409
- **子 Agent 系统**：统一存 `sub_agent_sessions` 表，`agent_type` 区分类别（monitor / native-agent / native-monitor）。CCM 自有子 agent 拥有独立 MCP server，通过 HTTP API 与系统通信
- **瞬时过载重试**：Anthropic 基础设施侧 429/overloaded 与账号额度用尽严格区分，前者退避重试同一账号，后者走号池轮换
- **进程管理**：`asyncio.create_subprocess_exec` 启动，必须 unset `CLAUDECODE` 环境变量避免嵌套检测
- **停止机制**：SIGTERM → 等待 10s → SIGKILL

## 分布式 Worker

CCM 支持将任务分发到远程 EC2 Worker 节点执行，突破单机并发瓶颈。每个 Worker 运行完整的 CCM 服务并拥有独立的 Claude 账号池，Manager 通过 VPC 内网统一管理。

**核心能力：**
- **一键创建** — Workers 页面点 +，自动创建 EC2、部署代码、安装依赖、启动服务（配置从 Manager 自身继承，无需手动填写 AMI/机型/子网）
- **任务转发** — 创建任务或修改已有任务时选择执行 Worker，所有 Chat/Stop/Retry/Plan 操作自动代理，前端零感知
- **实时迁移** — 任务可随时在本机和 Worker 之间迁移，session 文件和工作目录自动同步，`--resume` 无缝衔接
- **WebSocket 中继** — 每个 Worker 一条 WS 连接，日志实时中继到 Manager 并存储副本，断线自动重连+补全
- **生命周期管理** — 支持关机（保留数据）/开机/销毁（自动迁回全部任务），健康检查每 30s 自动恢复降级 Worker
- **版本锁定** — Worker 通过 rsync 部署与 Manager 完全一致的代码版本，健康检查校验 commit 一致性

**前置条件：** Manager 运行在 EC2 + IAM Role 有 EC2 权限 + `.env` 配置 `WORKER_SSH_KEY_PATH`。

详细部署步骤见 [分布式 Worker 部署指南](docs/worker-deployment-guide.md)。
