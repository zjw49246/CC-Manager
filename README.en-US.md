# Claude Code Manager

[简体中文](README.md) | **English**

Web-based tool for scheduling and managing multiple Claude Code instances to work in parallel. Inspired by Hu Yuanming's article "I Worked for 10 Claude Code Instances".

> **⚠️ Important Security Notice:** Ordinary Tasks initiated by administrators, super administrators, or a single-token deployment run unrestricted (Claude Code uses `--dangerously-skip-permissions`) and may read/write files, execute commands, access the network, and run operations such as `git push`. Member turns and purpose-built Browser/PR Review/Delivery/Planner tasks use their respective restricted protocols. **Strongly recommended to deploy on a separate machine or virtual machine without important files.**

## Features

### Core Scheduling
- **Global Dispatcher** — Automatically creates workers and assigns tasks upon startup, no manual intervention required
- **Fully Autonomous Claude Code** — Claude Code independently handles worktree creation, commit, fetch, merge, push, conflict resolution, and cleanup. The Dispatcher only assigns tasks and evaluates success/failure
- **9-Step Task Lifecycle** — Claim → Create Workspace → Implement → Commit → Merge + Test → Merge to main → Mark Complete → Cleanup → Experience Accumulation
- **Project Management** — Supports cloning existing repositories (with remote) and local git init (without remote). New projects can be created directly when creating tasks. Background clones never hang waiting for credential input and authentication failures produce an actionable message; a project whose clone failed rejects new task creation (422) while already-queued tasks wait and start automatically after a successful re-clone
- **Project Todo List** — Each project maintains a collapsible todo list (prompt template). Click "▶ Run" to directly create a Task and jump to Chat; todos are automatically marked complete upon creation and record the derived task. Supports archive/recover/permanent delete
- **Task Queue** — Automatically schedules by priority (smaller number = higher priority)
- **Multi-Instance Parallelism** — Runs multiple Claude Code instances simultaneously, each handling different tasks
- **Git Worktree** — Each instance works in an independent worktree, avoiding interference

### Execution Modes
- **Multi-Provider (Claude / Codex)** — Task-level execution engine selection: OpenAI Codex CLI (default, `gpt-5.6-sol`) or Claude Code. Model options include `gpt-6-astra`, with `low` through `ultra` reasoning and Fast (`priority`); deployments overriding `CODEX_MODEL_OPTIONS` must add this ID to their list. Codex tasks support full lifecycle, multi-turn dialog, Goal mode evaluation, Plan approval, automatic context compression, instantaneous error backoff retry, account pool, and cross-Worker rollout migration; instruction files read `AGENTS.md` (auto-injected). PTY hot sessions, `ask_user`, and Claude native sub-agents remain Claude-exclusive (explicitly hidden/rejected under Codex, no silent degradation)
- **Persistent PTY Session Mode** — Default mode, Claude Code runs as a persistent interactive session, multi-turn cold-start free (hot session reuse), first launch shows Cold Start indicator
- **Goal Mode** — `mode="goal"` uses natural language completion conditions (`goal_condition`), lightweight evaluator (default Haiku) automatically checks goal achievement after each turn
- **Interactive Versioned Plans** — A Plan is a first-class artifact independent of Tasks. Planner and Reviewer can pause the same Run for any number of required inputs, resume after answers, and preserve immutable Version history. Approval does not execute anything automatically: a related Version is applied only when the user explicitly attaches it to the next real message, while a standalone Version can create an execution Task on demand
- **Effort Level** — Supports `low` / `medium` / `high` / `xhigh` / `max` five levels, priority chain: Task → Instance → Global default
- **Model Configuration** — Supports full model IDs (including `claude-opus-5`); Opus 5 fixed to 1M context and supports `low/medium/high/xhigh/max` effort; other compatible models can enable 1M context with `[1m]` suffix
- **Codex Fast** — Codex Tasks can choose Standard or Fast; Fast uses the same model's `priority` service tier, won't switch models or reduce effort. Currently supports GPT-5.6 Sol/Terra/Luna, GPT-5.5, GPT-5.4; if account/model cannot confirm `priority`, it explicitly fails before execution, won't silently attach Fast badge and run as Standard
- **Thinking Budget** — Instance-level `thinking_budget` setting, passed to CLI via `MAX_THINKING_TOKENS`
- **Workflows Toggle** — Task-level control for Workflow tool activation, saves tokens when disabled

### Intelligent Capabilities
- **Skills System** — When creating a Task, check Standard Skills and User Skills for Claude/Codex; unified task context is injected via Claude system-prompt file, Codex app-server's schema-backed turn text, or equivalent `codex exec` adapter. Local, non-shared, non-Worker-managed Codex Tasks can also use Monitor when primary MCP capability is confirmed; Worker/Shared Tasks continue fail-closed. `CODEX_MAIN_MCP_ENABLED=false` disables Codex Standard/User/Monitor Skills but doesn't affect independent Sub-Agents. Leading `$command` is uniformly validated against the task's exact execution scope before writing messages to local, Worker, and Shared chats. Remote Workers lock the latest Skill configuration within the same task lock upon first claim; active execution after claim doesn't allow mid-flight Skill rewriting
- **Monitor Sub-Agent** — Claude and capability-confirmed local Codex Tasks can autonomously create persistent monitoring sub-agents; sub-agents have independent MCP tools (`report_status` / `mark_complete` / `get_context`), autonomously check and report to the system via database-scheduled short turns
- **Native Sub-Agent Mirroring (PTY Mode)** — Sub-agents opened by the model using built-in Agent/Task/Monitor tools are observed by the PTY layer and automatically registered into the sub-agent system (categories native-agent / native-monitor), unified display and management

### Interaction & Chat
- **Multi-turn Conversations** — Continue asking follow-up questions via Chat interface after task completion, automatically `--resume` the same session
- **Session Focus Tags** — Each Task maintains a custom short tag, prominently displayed in task list and Chat top bar, editable anytime, facilitating notes on "check later/next steps"; this field is independent of internal `tags`, preserved during copy, Fork, and Worker migration
- **Task Artifact Download** — Claude/Codex saves explicitly delivered files to `.claude-manager/artifacts/task-<id>/` in the current Project; explicit artifact links in chat can be directly downloaded; standard source code and doc references won't falsely appear as downloadable files
- **Math Formula Rendering** — Markdown in Chat and Discussion supports KaTeX; compatible with Codex's `\\(...\\)` / block `\\[...\\]` and `$$...$$`, single dollar sign content displays as plain text, links, HTML, code, and currency remain unchanged
- **Interactive Prompts (ask_user)** — When model calls built-in `AskUserQuestion`, selectable cards (single/multi-select/custom text) pop up in chat, user selection feeds answer back to model to continue. Defaults to 1800s timeout, supports cross-page global notifications (bottom-right popup + unread marker), can be disabled with `ASK_USER_ENABLED=false`
- **Permission Passthrough (PTY Mode)** — When CC requests tool permissions, cards (tool name/description/input preview) appear in chat, click allow/deny to instantly return packet; 120s timeout defaults to deny
- **Voice Input** — Speech-to-text via OpenAI Whisper API to create tasks

### Reliability
- **Unified Account Routing for Claude / Codex** — Native accounts and CloudRouter API Keys share account pool, model/Service Tier compatibility checks, and session migration. Codex Fast only selects accounts with real advertising `priority`; ApexRouter's model catalog capability also participates in selection. Manual "Priority Account" ranks highest; automatic mode keeps existing conversations bound to account, new sessions prioritize compatible/available APIs, then fall back to native quota selection. Both pools show actual post-submission "Last Used", API candidate failure won't falsely modify badge
- **Secure Deletion of API Accounts** — CloudRouter/ApexRouter accounts first disable new tasks, then wait for active tasks and sessions to release before deleting Key and running config; retains "pending cleanup" status when busy for retry, won't forcibly kill tasks, and preserves Claude projects and Codex sessions
- **Seamless Account Rotation** — Claude recursively hardlinks session JSONL and sidecar, Codex independently copies rollout and atomically completes app-server rebind + Task binding; keeps original conversation context on hit limit, auth failure, or active quota threshold switch; unsupported models won't silently degrade
- **Automatic Retry for Transient 429/Overload** — Infrastructure-side temporary throttling/overload (not account quota exhaustion), exponential backoff + jitter automatically `--resume` retries with same account, max 5 times; detection split by provider (Claude / Codex respective CLI error texts)
- **`/tmp` Space Protection** — Service checks capacity and inodes on startup and every 3 hours in background; clears all CCM whitelist temp artifacts over 6 hours old when either reaches 80%
- **Process Timeout Protection** — Configurable max execution time per task, auto-kill on timeout

### Distributed
- **Distributed Workers** — Distributes tasks to remote EC2 instances for execution, breaking single-machine concurrency limits. Phase 1 (create/deploy/manage) + Phase 2 (task forwarding + event relay) + Phase 3 (real-time task migration) all available. See [Worker Deployment Guide](docs/worker-deployment-guide.md)
- **Safe One-Click Update & Restart** — Scheduled background checks with popup reminders; pauses new task claims during update, refuses restart if active tasks, manual instances without tasks, or pending resume messages aren't zeroed; detects manually pulled but unloaded code, then completes dependencies, migration, frontend build, and intelligent restart

### Projects & Collaboration
- **Project Management** — Supports cloning existing repositories (with remote) and local git init (without remote), new projects can be created directly when creating tasks. Background clones never hang waiting for credential input and authentication failures produce an actionable message; a project whose clone failed rejects new task creation (422) while already-queued tasks wait and start automatically after a successful re-clone
- **PR Monitor** — Audits GitHub PRs with exact-head CI and isolated Reviewer Panel; each Finding can be audited with ignore/manual advice, or tool-free Task generates scoped candidate diffs. AI candidates must first be downloaded and bound to user/Action/patch hash by backend, then explicitly confirmed by user, backend only performs exact-old compare-and-swap push on matching PR source branches; any Finding operation cannot bypass Panel Gate
- **PWA** — Add to Home Screen in mobile browsers, native App experience
- **Android App** — Native APK packaged via Capacitor, configurable remote server URL in-app
- **Theme Switching** — v2 theme system: Modern Dark (default, Multica style) / Modern Light (tonal zinc gray layered) / Feishu (official color palette + real App screenshot color sampling: white background main + classic Feishu blue #3370FF + N series neutral colors + low border style, distinguished from Light theme by "white vs gray", Feishu client-style narrow icon rail + IconPark dual-color icon set) / Apple (apple-design skill driven: iOS systemGray neutral colors + apple.com CTA blue #0071E3 + system fonts priority + frosted glass top bar + press feedback + macOS Settings-style sidebar and Ionicons icon set, respects reduced-motion/transparency), v1's classic dark, sea blue, forest, berry red fully retained as Legacy group, preferences persisted
- **Token Authentication** — Bearer Token protects all APIs, secure remote access
- **Remote Access** — Exposed to public internet via Cloudflare Tunnel

## Task Lifecycle

Dispatcher only assigns tasks and judges success/failure, Claude Code independently completes the entire workflow:

1. **Claim Task** — Dispatcher dequeues, status=in_progress
2. **Create Workspace** — Claude Code autonomously creates git worktree, status=executing
3. **Implement Features** — Claude Code writes code in worktree
4. **Commit Code** — Claude Code autonomously `git add` + `git commit`
5. **Merge + Test** — Claude Code autonomously `git fetch origin && git merge origin/main` + runs tests
6. **Merge to main** — Claude Code autonomously rebase + merge + push (resolves conflicts independently)
7. **Mark Complete** — Claude Code updates documentation
8. **Cleanup** — Claude Code autonomously cleans worktree and task branch
9. **Experience Accumulation** — Claude Code records experience in PROGRESS.md

**State Transitions:**
```
pending → in_progress → executing → completed
                           ↓
                        (fail)
                           ↓
                        pending (retry)
```

## Tech Stack

| Layer | Technology |
|---|------|
| Backend | Python 3.11+, FastAPI, SQLAlchemy (async), Alembic |
| Database | SQLite (default) / PostgreSQL / MySQL |
| Frontend | React 19, Vite, Tailwind CSS v4, TypeScript, Lucide icons |
| PTY | claude-pty (Claude Code persistent session framework) |
| Real-time Communication | WebSocket (native, channel-based pub/sub) |
| MCP | FastMCP server (Skills / Monitor Agent) |
| Voice | OpenAI Whisper API |
| Remote | Cloudflare Tunnel / ngrok |
| Worker | AWS EC2, boto3, rsync, SSH |

## Project Structure

```
claude-manager/
├── backend/
│   ├── main.py                  # FastAPI entry, global singleton, static file serving
│   ├── config.py                # Pydantic BaseSettings (.env)
│   ├── database.py              # SQLAlchemy async engine + session
│   ├── api/                     # REST + WebSocket routes
│   │   ├── tasks.py             # Task CRUD + plan approval + conflict resolution
│   │   ├── plans.py             # Related Plan history, staleness, revisions, execution Tasks
│   │   ├── plan_resources.py    # First-class Plan/Version/Run/Input/Application API
│   │   ├── chat.py              # Multi-turn conversation (based on task, --resume)
│   │   ├── instances.py         # Instance CRUD + Ralph Loop + Dispatcher endpoints
│   │   ├── projects.py          # Project CRUD + git clone
│   │   ├── monitor.py           # Monitor Session CRUD + sub-agent endpoints
│   │   ├── pool.py              # Claude account pool status/usage/reload/clear-cooldown
│   │   ├── pr_monitor.py        # PR Monitor CRUD + GitHub webhook
│   │   ├── workers.py           # Distributed Worker CRUD + stop/start/destroy/retry
│   │   ├── sub_agents.py        # Sub-Agent summary API
│   │   ├── ask_user.py          # ask_user intercept + answer feedback
│   │   ├── settings.py          # Runtime settings API
│   │   ├── system.py            # Health check + stats + one-click update
│   │   ├── ws.py                # WebSocket endpoint
│   │   ├── voice.py             # Whisper speech-to-text
│   │   └── auth.py              # Token login
│   ├── middleware/auth.py       # Bearer token auth middleware
│   ├── hooks/
│   │   └── ask_user_hook.py     # AskUserQuestion PreToolUse hook script
│   ├── models/                  # SQLAlchemy ORM models
│   │   ├── task.py              # Task (session_id, last_cwd, project_id, enabled_skills, effort_level...)
│   │   ├── plan_agent.py        # Planner/Reviewer Runs and step audit records
│   │   ├── plan.py              # Plan/Version/Input/Application aggregate models
│   │   ├── instance.py          # Claude Code instance
│   │   ├── project.py           # Project (name, git_url, local_path)
│   │   ├── sub_agent.py         # SubAgentSession + SubAgentReport (generic sub-agents)
│   │   ├── pr_monitor.py        # MonitoredRepo + PRReview
│   │   ├── worker.py            # Distributed Worker (EC2 instance + bootstrap state machine)
│   │   ├── log_entry.py         # Execution logs
│   │   └── worktree.py          # Git worktree tracking
│   ├── schemas/                 # Pydantic request/response models
│   ├── mcp/                     # MCP Servers
│   │   ├── ccm_skills_server.py         # Main Agent MCP: create_monitor / check_monitors / stop_monitor
│   │   └── ccm_monitor_agent_server.py  # Sub-Agent MCP: report_status / mark_complete / get_context
│   └── services/                # Core business logic
│       ├── dispatcher.py        # Global dispatcher (9-step task lifecycle + goal + monitor)
│       ├── instance_manager.py  # Child process lifecycle (launch/stop/consume, MCP injection)
│       ├── claude_pool.py       # Multi-account pool (rate limit detection/auto-switch/session migration/quota query)
│       ├── goal_evaluator.py    # Goal condition evaluator (claude -p child process)
│       ├── plan_agent_runner.py # Strictly read-only Planner/Reviewer pipeline
│       ├── plan_tasks.py        # Plan context, repository fingerprints, staleness, attachment validation
│       ├── plan_service.py      # Version state machine, inputs, approval, Worker outcome imports
│       ├── mcp_config.py        # Dynamic MCP config generation
│       ├── tmp_space_manager.py # /tmp capacity/inode watchdog & whitelist safety cleanup
│       ├── cloud_provider.py    # AWS EC2 Provider (Worker instance create/start/destroy)
│       ├── worker_provisioner.py # Worker full lifecycle (create→bootstrap→ready)
│       ├── worker_proxy.py      # Task forwarding to Worker
│       ├── worker_relay.py      # Manager↔Worker WebSocket event relay
│       ├── task_migrator.py     # Task migration between local machine & Worker
│       ├── update_service.py    # Update/fix/rollback transactions + intelligent restart
│       ├── deployment_start_guard.py # Deployment lease, startup guard & cross-process task fence
│       ├── stream_parser.py     # NDJSON stream-json parser
│       ├── task_queue.py        # Priority task queue
│       ├── worktree_manager.py  # Git worktree management + rebase + push
│       ├── pr_review_service.py # PR review prompt building + status callback
│       ├── ask_user.py          # ask_user registry + Future management
│       ├── ask_user_settings.py # ask_user hook injection/removal
│       ├── ws_broadcaster.py    # WebSocket channel broadcast
│       ├── whisper_client.py    # Speech-to-text
│       └── backup_service.py    # Database backup (optional)
├── frontend/
│   ├── public/                  # PWA manifest, service worker, icons
│   └── src/
│       ├── api/client.ts        # API client + types (401 auto-logout, dynamic base URL)
│       ├── api/ws.ts            # WebSocket client (exponential backoff reconnect)
│       ├── config/server.ts     # Remote server URL config (Capacitor/Android)
│       ├── config/theme.ts      # Theme registry (modern dark/light + Legacy group, meta theme-color sync)
│       ├── pages/               # Dashboard, TasksPage, PlansPage, WorkersPage, PRMonitorPage, LoginPage...
│       ├── components/
│       │   ├── AskUserNotifications.tsx   # Global ask_user popup notifications
│       │   ├── Chat/ChatView.tsx          # Multi-turn conversation UI
│       │   ├── Chat/MonitorPanel.tsx      # Monitor panel
│       │   ├── Chat/SubSessionIndicator.tsx
│       │   ├── Instances/                 # InstanceGrid, InstanceLog
│       │   ├── Tasks/                     # TaskForm, TaskList, TaskConfigBadge
│       │   ├── Layout/PoolDrawer.tsx      # Pool quota drawer
│       │   ├── PlanReview/                 # First-class Plan action/history/detail/input UI
│       │   ├── System/                    # UpdatePanel
│       │   └── Voice/VoiceButton.tsx      # Voice input
│       └── hooks/useWebSocket.ts
├── scripts/
│   ├── dev.sh                   # One-click dev environment startup
│   ├── setup.sh                 # Worker SSH Key + environment initialization
│   ├── refresh_pty.sh           # Refresh claude-pty dependencies
│   ├── start_all.sh             # Production startup script
│   └── tunnel.sh                # ngrok/cloudflare tunnel
├── docs/
│   └── worker-deployment-guide.md  # Worker deployment guide
├── pyproject.toml
└── .env
```

## Quick Start

### Prerequisites

- macOS / Linux (Ubuntu 22.04+ recommended, supports EC2 deployment)
- Python 3.11+
- Node.js 18+
- [uv](https://docs.astral.sh/uv/) — Python package manager
- [Claude Code CLI](https://docs.anthropic.com/en/docs/claude-code) installed and logged in (`claude auth login`)
- Google Chrome + Xvfb (required for automatic account pool login, install for server deployment)

### Installation

```bash
git clone https://github.com/zjw49246/Claude-Code-Manager.git && cd Claude-Code-Manager

# Backend dependencies (using uv)
uv sync

# If PostgreSQL support needed
uv sync --extra postgres

# If MySQL support needed
uv sync --extra mysql

# Frontend dependencies
cd frontend && npm install && cd ..

# Configuration
cp .env.example .env
# Edit .env, set:
#   AUTH_TOKEN=your_access_password
#   OPENAI_API_KEY=sk-... (required for voice feature)
#   WORKSPACE_DIR=~/Projects (project workspace root directory)
```

### Startup

```bash
# One-click startup
./scripts/dev.sh

# Or start separately
uv run uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload &
cd frontend && npx vite --host &
```

Visit http://localhost:5173, enter `AUTH_TOKEN` to log in.

After startup, Dispatcher automatically creates worker instances and begins scheduling.

### Android App Packaging

```bash
cd frontend

# Install Capacitor (already in package.json)
npm install

# Build web assets
npm run build

# First clone only: generate the untracked native Android project; skip on later builds
npx cap add android

# Sync web assets and native dependencies, then package the APK
npx cap sync android
JAVA_HOME="/Applications/Android Studio.app/Contents/jbr/Contents/Home" \
  android/gradlew -p android assembleDebug

# APK located at android/app/build/outputs/apk/debug/app-debug.apk
```

First time opening the App, expand "Server URL" on the login page to input server address (e.g., Cloudflare Tunnel URL), then input Token to log in.

## Database

Uses SQLite by default, also supports PostgreSQL and MySQL. Switch via `DATABASE_URL` in `.env`:

```bash
# SQLite (default)
DATABASE_URL=sqlite+aiosqlite:///./claude_manager.db

# PostgreSQL (requires: uv sync --extra postgres)
DATABASE_URL=postgresql+asyncpg://user:pass@host:5432/claude_manager

# MySQL (requires: uv sync --extra mysql)
DATABASE_URL=mysql+aiomysql://user:pass@host:3306/claude_manager
```

### Schema Migration (Alembic)

Uses Alembic to manage schema versions. **Executes `alembic upgrade head` automatically on startup**, no manual action needed.

```bash
uv run alembic upgrade head    # Manual upgrade (usually unnecessary)
uv run alembic current         # View current version
uv run alembic history         # View history
```

### Data Migration

This repository does not provide a data-transfer script for moving between SQLite, PostgreSQL, and MySQL. For a cross-database migration, stop writes and take a complete backup first, then use the source and target databases' official export, import, or replication tools (or an independently validated ETL process). After migration, run Alembic, verify row counts and critical relationships, and complete a restore drill before switching `DATABASE_URL`.

## Updating Deployed Instances

### Method 1: One-Click Update (Recommended)

Trigger via API or Frontend System panel:

```bash
curl -X POST http://localhost:8000/api/system/update \
  -H "Authorization: Bearer $AUTH_TOKEN"
```

Approximately 1 second after opening, refreshing, or relogging into the CCM page, it automatically checks once, then every hour; background checks only execute `fetch/dry_run`, **will not automatically pull or restart**. Detects new remote commits, or detects someone manually pulled code while current service still runs old version, shows non-blocking notification at top of page, doesn't block page or affect normal operations; only clicking "View Details" opens update popup. Same version only reminds once per page lifecycle, will remind again next time page opens. Remote check failure usually stays silent, but won't mask locally pulled code still needing restart.

True update automatically executes: git pull → refresh PTY dependencies → database migration → rebuild frontend → intelligent restart (auto-detects systemd service name `SERVICE_NAME`). Update source prioritizes tracking remote configured for target branch (e.g., `upstream/main` for this repo), falls back to `origin` if no tracking remote.

Status check separately displays actual loaded commit of current process, disk `HEAD`, database Alembic current/head. Three have different meanings: code successfully pulled only means disk is new version; if dependencies, frontend artifacts, or migration fail, old service may still run, so "remote and local code consistent" cannot be used as deployment completion judgment. Page provides "Fix & Redeploy", re-executes dependency sync, PTY refresh, frontend install/build, database confirm/migration, and controlled restart for current disk version; only opens lightweight restart when code and database both confirmed consistent. Detail page still retains "Manual Restart" button.

Update, fix, and controlled restart also require clean Git working tree, including staged, unstaged, and untracked files not excluded by `.gitignore`; otherwise new process may load code unprovable by commit. Runtime files like database, logs, backups, build artifacts should be explicitly excluded via `.gitignore`.

```bash
# Complete full deployment for current disk version
curl -X POST http://localhost:8000/api/system/update/repair \
  -H "Authorization: Bearer $AUTH_TOKEN" -H "Content-Type: application/json" \
  -d '{}'

# Controlled restart only when code/database confirmed consistent
curl -X POST http://localhost:8000/api/system/restart \
  -H "Authorization: Bearer $AUTH_TOKEN"
```

Update transactions use persistent deployment lease records in repo for token, worker PID identity, expected commit, and migration results. Checks lease before service startup; if last migration or rollback didn't fully complete, CCM only starts a maintenance interface not accessing business database, Dispatcher, Worker, and normal APIs won't start, admin can still view status and execute fix/rollback. A database rollback snapshot is created only when the authoritative Alembic revision check finds pending migrations. After the service fully stops, the migration worker creates and validates one authoritative SQLite snapshot instead of first creating an online snapshot that would immediately be replaced. Database restore, code rollback, dependency restore, or frontend restore failure at any step keeps service stopped, avoiding mixed versions of startup code, dependencies, and schema. By default CCM retains only the current and previous database recovery points.

One-click update and auto-fix only support file-type SQLite, because CCM must be able to create and verify snapshot after stopping service to promise auto-rollback. PostgreSQL/MySQL external databases can still use "Restart" when versions fully consistent, but update and fix require admin to first complete database backup, then deploy via database's own migration/restore flow. Migration failure usually stems from database schema drift, migration script errors, database files occupied by other processes, permission/disk space issues, or new service not starting within health check window; page and deployment status retain failed steps and logs, should not bypass by repeatedly clicking update.

To avoid interrupting tasks, unified task launch gate closes before update: normal Dispatcher, Worker forwarding, chat/Monitor resume, RalphLoop, and manual Instance running cannot cross maintenance window. `in_progress/executing` tasks, prompt-only instances still `running` without Task association, or resume messages already queued but not started will prevent service stop; stop-session clearing messages synchronously removes already invalidated queue blockers within same gate, preventing later phantom blocks. If new resume messages received during update, this restart cancels and restores scheduling, messages won't lose from memory queue due to process restart. Update and rollback requests share same operation admission lock: only one operation allowed at a time, rollback commit and backup fixed within lock, cannot be replaced by concurrent update. All updates, migrations, fast restarts after manual pull, and rollbacks complete final blocker query within same gate, and upon query success, immediately submit service stop operation without asynchronous waiting. Click "Re-check" after task completion to continue. When triggering update after manual `git pull`, system uses old commit actually loaded by service as baseline to complete deployment steps, not just a blind restart.

If tasks list already can't find tasks but update popup still shows running blockers, click "Re-verify Running Status". System pauses new claims and Dispatcher compares real lifecycle/process in memory with database Task↔Instance ownership: clearly dead and relationship-consistent residuals safely converge; multi-owner, relationship-inconsistent, or PID possibly still alive won't guess replay, will terminate damaged state or keep blocking evidence. Tasks preparing for launch, tasks owned by current process, Monitor/sub-agents won't be falsely cleared; remote shared task mirrors don't participate in local stale recovery.

When multiple CCM processes use same checkout, task claiming holds repository-level shared file lock, deployment claim uses exclusive lock; after deployment gets lease, queries active tasks again before any checkout, backup, or dependency modification. This way even if another CCM just submitted task after first check, it only cancels this deployment, won't run tasks while modifying its own environment.

stop-session cleanup per-task messages also advances that queue's cancellation generation. Old generation messages already pulled from queue by consumer but not yet registered as in-flight will be explicitly cancelled, won't restart again after cleanup success; already registered real in-flight work remains as update blocker until lifecycle ends.

### Method 2: Manual Update

```bash
git pull                      # 1. Pull latest CCM code
./scripts/refresh_pty.sh      # 2. Refresh claude-pty dependencies (see below)
.venv/bin/alembic upgrade head  # 3. Database migration
cd frontend && npm run build && cd ..  # 4. Rebuild frontend
# 5. Restart service (systemd / manual)
```

> **Why Step 2 is needed:** `claude-pty @ git+https://...` in `pyproject.toml` is an **install-time snapshot** — `git pull` only updates CCM's own code, won't bring new PTY framework code. `scripts/refresh_pty.sh` compares installed PTY commit with remote main HEAD, auto-reinstalls if inconsistent (editable/local dev install automatically skips).

## Usage Workflow

### Basic Flow

1. **Tasks** — Create task, select existing project or new project, fill Prompt, priority, Effort Level. Can check **Monitor** skill to grant Agent background monitoring capability
2. **Dispatcher** automatically assigns task to idle worker → Claude Code independently completes all work (including worktree creation and cleanup) → takes next
3. Click task's **Chat** button to continue asking follow-up questions for completed tasks
4. In tasks with Monitor enabled, Agent can autonomously create persistent monitoring sub-agents, task list shows active sub-agent count
5. Can add focus tag via task card menu, or click task card/chat top bar tag to modify; clear and save to remove

### Interactive Plans

A Plan is a first-class, versioned artifact independent of Tasks:

1. Create a standalone Plan on the dedicated **Plans** page, or create multiple independent related Plans from the **Plans** panel in an existing Chat. The **Tasks** page creates and displays only real Tasks.
2. Planner/Reviewer routing is configured only in global Settings. Each new Plan freezes the current primary/fallback provider, model, effort, and round settings.
3. Both Planner and Reviewer can pause the same Run for required input. There is no business limit on the number of questions in one round; the number of pauses allowed per Run is a separate global `0–5` setting.
4. Each complete proposal is stored as an immutable Version. Revise creates a new Run and Version under the same Plan; only Fork creates a new Plan.
5. Approve/Reject applies to the exact Version the user viewed and does not wake or change the original Task/session. A related Version is applied once only when explicitly attached to the next real message; a standalone Version can explicitly create a normal execution Task.

The **Plans** page groups pending inputs, approvals, and execution actions under **Plans requiring action**. Its catalog supports standalone/related, status, Project, search, and Archived only filters. Archive is a reversible soft archive and does not delete Plan, Version, Run, or Q&A history. Plan details retain Version switching and comparison, complete Q&A/Run/route/repository audit history, and the dual state where an older Version was applied while a newer Version awaits review. If the conversation, repository, or target changes, an action requires stale confirmation or is explicitly blocked by a hard conflict.

Planner and Reviewer use a strictly read-only transport. Codex steps reuse the account's persistent App Server, but use a disposable read-only thread that is deleted at terminal state. Plan creation requests, titles, Revise/Fork requests, and answers are persisted, so high-confidence credentials such as API keys, access tokens, and private keys must be stored in **Settings → Secrets**; Plan text should contain only the reference name.

### Goal Mode

Select Mode = `goal` when creating task, fill natural language completion condition:
1. Claude Code executes task
2. After each turn ends, lightweight evaluator (default `claude-haiku-4-5`) checks if condition met
3. If not met, automatically `--resume` continues execution, keeps continuous context of same session
4. Automatically marks complete upon goal achievement

### Voice Input

🎙 button next to title and description fields in task creation form, click to record, release to auto-transcribe and fill.

### PR Monitor Prerequisites

PR Monitor's review flow shells out backend to call `gh pr view` / `gh pr review` / `gh pr merge`, prerequisites before use:

1. **gh CLI Authenticated**: System user running backend must execute `gh auth login` first to complete GitHub authentication
2. **Account Permissions**: That GitHub account needs push / review permissions for monitored repo (auto-merge also needs merge permission)
3. **PUBLIC_BASE_URL**: Set `PUBLIC_BASE_URL` in `.env` (e.g., `https://ccm.example.com`) for PR Monitor page to display correct Webhook Payload URL

Finding operations in Panel are independent audit flow: **Ignore** and **Human advice** only save decision or reference for next candidate generation, won't change blocker to pass; **Generate AI fix** reads backend frozen exact-head single-file input in tool-free sandbox same as Reviewer, outputs bounded unified diff only, won't directly access repo or GitHub. After candidate complete, must download diff from CCM backend; backend signs and saves download receipt bound to current user, Action, and SHA-256, only upon returning same receipt and re-confirming exact base/head/repo/ref will create commit. Push uses captured head as expected-old for atomic compare-and-swap, refuses or enters auditable recovery if branch deleted, drifts, or remote result unprovable, won't overwrite others' commits.

Only one active AI fix per Finding. Auto fix Task's edit, chat, inject, retry, cancel, stop, and delete entries frozen; local and Worker both collect results by Manager per exact generation, Worker completes full logs first if successful, restores from persistent Action/lease if failed or Manager/Worker crashes, cannot repeatedly generate or push via memory state. Detailed state machine and security boundaries see [PR Monitor Authoritative Design](docs/pr-monitor-design.md).

## Distributed Worker

Worker system supports distributing tasks to remote EC2 instances for execution, suitable for scenarios needing more parallelism. Each Worker is an EC2 running complete CCM, with independent Claude account pool.

**Core Capabilities:**
- Horizontal scalability, each Worker configurable with multiple Claude accounts
- Task execution location switches in real-time (local / any Worker), session seamlessly connected
- Worker destruction automatically migrates back all tasks and data, no context loss
- Frontend zero-perception difference — remote tasks and local tasks UI/operations completely identical

**Usage Flow:**
1. **Create Worker**: Click **+** on Workers page, input name, system auto-creates EC2 → installs dependencies → deploys code → starts service
2. **Assign Accounts**: Add Claude accounts in Pool panel on Worker detail page
3. **Assign Tasks**: Task Config panel → "Run on" dropdown selects Worker
4. **Task Migration**: Running tasks can migrate between local and Worker anytime, session auto-syncs
5. **Shutdown/Destroy**: Stop preserves instance data (can Restart), Destroy migrates all tasks back to local first then terminates instance

> Detailed deployment guide, prerequisites, config parameters, and troubleshooting see **[docs/worker-deployment-guide.md](docs/worker-deployment-guide.md)**

## API

| Module | Endpoint | Description |
|------|------|------|
| Projects | `GET/POST /api/projects` | Project list/create |
| | `GET/PUT/DELETE /api/projects/{id}` | Project detail/update/delete |
| | `POST /api/projects/{id}/reclone` | Re-clone |
| Project Todos | `GET/POST /api/projects/{id}/todos` | Project todo list/create |
| | `PATCH /api/projects/{id}/todos/{todo_id}` | Update (including archive/recover/sort) |
| | `DELETE /api/projects/{id}/todos/{todo_id}` | Permanent delete |
| Tasks | `GET/POST /api/tasks` | Task list/create |
| | `GET/PUT/DELETE /api/tasks/{id}` | Task detail/update/delete |
| | `POST /api/tasks/{id}/cancel` | Cancel task |
| | `POST /api/tasks/{id}/retry` | Retry task |
| | `POST /api/tasks/{id}/chat` | Send follow-up message |
| | `GET /api/tasks/{id}/chat/history` | Get chat history |
| | `POST /api/tasks/{id}/permissions/{rid}` | Reply to permission request |
| | `POST /api/tasks/{id}/ask-user/{rid}` | Reply to ask_user prompt |
| | `GET /api/tasks/{id}/ask-user/pending` | Get pending prompts |
| Plans | `GET/POST /api/plans` | Plan catalog/create |
| | `GET/PATCH /api/plans/{id}` | Detail, rename, archive, or restore |
| | `POST /api/plans/{id}/runs` | Create a Revise/Refresh/Retry Run |
| | `POST /api/plans/{id}/fork` | Fork into a new Plan |
| | `GET /api/plans/{id}/versions` | Get immutable Version history |
| | `GET /api/plan-versions/{id}` | Get an exact Version |
| | `GET /api/plan-versions/{id}/staleness` | Check whether Version context is stale |
| | `POST /api/plan-versions/{id}/approve` | Approve an exact Version |
| | `POST /api/plan-versions/{id}/reject` | Reject an exact Version |
| | `POST /api/plan-runs/{run_id}/input-requests/{request_id}/answer` | Answer required input and resume the same Run |
| | `POST /api/plan-versions/{id}/create-execution-task` | Create an execution Task from a standalone Version |
| Instances | `GET/POST /api/instances` | Instance list/create |
| | `DELETE /api/instances/{id}` | Delete instance |
| | `POST /api/instances/{id}/stop` | Stop instance |
| | `POST /api/instances/{id}/run` | Manual execute |
| | `GET /api/instances/{id}/logs` | Get logs |
| Monitor | `POST /api/tasks/{id}/monitor-sessions` | Create monitor sub-session |
| | `GET /api/tasks/{id}/monitor-sessions` | List monitor sessions |
| | `DELETE /api/tasks/{id}/monitor-sessions/{sid}` | Stop monitor session |
| | `POST /api/tasks/{id}/monitor-sessions/{sid}/checks` | Sub-agent reports status |
| | `POST /api/tasks/{id}/monitor-sessions/{sid}/complete` | Sub-agent marks complete |
| Sub-Agents | `GET /api/tasks/{id}/sub-agents/summary` | Sub-agents summarized by type |
| Workers | `GET/POST /api/workers` | Worker list/create |
| | `GET /api/workers/{id}` | Worker detail |
| | `GET /api/workers/{id}/logs` | Bootstrap logs |
| | `POST /api/workers/{id}/stop` | Shutdown |
| | `POST /api/workers/{id}/start` | Startup |
| | `POST /api/workers/{id}/destroy` | Destroy (auto-migrate tasks back) |
| | `POST /api/workers/{id}/retry` | Retry Bootstrap |
| | `GET/POST /api/workers/{id}/pool/*` | Worker pool management |
| Dispatcher | `GET /api/dispatcher/status` | Dispatcher status |
| | `POST /api/dispatcher/start` | Start dispatcher |
| | `POST /api/dispatcher/stop` | Stop dispatcher |
| Pool | `GET /api/pool/status` | Account pool status (available/cooldown/disabled) |
| | `GET /api/pool/usage` | Account pool status + per-account quota utilization (5h/7d) |
| | `POST /api/pool/reload` | Reload account config |
| | `POST /api/pool/accounts/{id}/clear-cooldown` | Clear account cooldown |
| Voice | `POST /api/voice/transcribe` | Speech-to-text |
| System | `GET /api/system/health` | Health check |
| | `GET /api/system/stats` | Statistics |
| | `POST /api/system/update` | One-click update & restart |
| | `POST /api/system/update/reconcile` | Safety check & converge phantom running status |
| | `POST /api/system/update/repair` | Full fix deployment for current disk version |
| | `POST /api/system/restart` | Controlled restart when code/database consistent |
| WebSocket | `ws://host/ws` | Real-time push (subscribe channel) |
| Auth | `POST /api/auth/login` | Token login |

All APIs (except health, login, github webhook) require `Authorization: Bearer <token>` header.

## Configuration

### Base Configuration

| Environment Variable | Default | Description |
|----------|--------|------|
| `AUTH_TOKEN` | (Required) | API Auth Token |
| `PORT` | `8000` | Main service listen port |
| `PUBLIC_BASE_URL` | (Empty) | Public deployment address (e.g., `https://ccm.example.com`) |
| `OPENAI_API_KEY` | (Optional) | Required for voice feature |
| `DATABASE_URL` | `sqlite+aiosqlite:///./claude_manager.db` | Database connection (supports SQLite/PostgreSQL/MySQL) |
| `FEISHU_APP_ID` / `FEISHU_APP_SECRET` | (Empty) | Feishu OAuth app credentials |
| `FEISHU_OAUTH_STATE_SECRET` | (Empty) | Optional independent OAuth state signature key; if empty securely reuses `FEISHU_APP_SECRET` |
| `FEISHU_OAUTH_STATE_TTL_SECONDS` | `600` | Feishu OAuth state validity period (max 3600 seconds) |
| `SMTP_HOST` / `SMTP_PORT` | `smtp.feishu.cn` / `465` | Registration verification code SMTP service |
| `SMTP_USER` / `SMTP_PASSWORD` | (Empty) | Registration verification code credentials; must be provided by deployment environment |
| `SMTP_FROM` | (Empty) | Optional sender address, uses `SMTP_USER` if empty |
| `VERIFICATION_CODE_CAPACITY` | `4096` | Max capacity in single-process memory for pending verification codes & rate limit events |
| `VERIFICATION_CODE_RATE_WINDOW_SECONDS` | `600` | Verification code sending rate limit window (seconds) |
| `VERIFICATION_CODE_EMAIL_LIMIT` / `VERIFICATION_CODE_IP_LIMIT` | `5` / `30` | Max sends per window per email/source IP |
| `VERIFICATION_CODE_RESEND_COOLDOWN_SECONDS` | `60` | Min interval between two sends for same email (seconds) |
| `VERIFICATION_CODE_SMTP_CONCURRENCY` | `4` | Max concurrent SMTP deliveries per process |
| `WORKSPACE_DIR` | `~/Projects` | Project clone target directory |
| `MAX_CONCURRENT_INSTANCES` | `8` | Default local Task/Plan concurrency limit; administrators can override it live in Settings without restarting |
| `AUTO_START_DISPATCHER` | `true` | Auto-start scheduler on startup |
| `TASK_TIMEOUT_SECONDS` | `1800` | Max execution time per task (seconds) |
| `SERVICE_NAME` | (Auto-detect) | systemd service name, used for one-click update restart |

New installs won't generate a fixed-password administrator: the first registered user to complete email verification becomes `super_admin`. When `AUTH_TOKEN` is configured and no active users exist, the registration form must include that Token in the optional "Bootstrap Token" field to prevent a public visitor from seizing the first administrator account. Upgrades automatically disable the shared default administrator created by older versions; single-token deployments can still enter with `AUTH_TOKEN`, while team deployments should configure `SMTP_*` and then register a real administrator. The verification-code service rate-limits by email and `Request.client.host`; multi-process or multi-replica deployments should also configure shared rate limiting at the reverse proxy or gateway because application-level state is isolated per process.

Execution authority is frozen per turn from the actual caller: ordinary `admin`/`super_admin` and deployment-token turns run unrestricted, while `member` turns run in the fail-closed sandbox. Team Project/Task sharing is only a local ACL and does not change that principal. A Worker is a complete headless CCM compute node: the Manager validates ACLs and roles, then delegates the ordinary turn; the Worker Token authenticates only the control plane and never becomes the model's super-admin identity. Legacy cross-CCM shares and purpose-built Browser, PR Review, Delivery, Planner, and Reviewer tasks retain their own isolated protocols.

Team authorization matrix:

- `super_admin`/`admin` can create, configure, and assign Projects and manage every Project/Task.
- A `member` initially sees no Project or Task and cannot create Tasks. A Project ACL lets the member view that Project and create/manage Tasks inside it.
- A Task creator or administrator can share that Task with a user/group for chat access. A Task share is not a Project ACL and does not allow creation of another Task in that Project.
- Worker ownership grants compute-node administration only, never visibility into Projects/Tasks placed on that node. Workers are headless; the Manager remains authoritative for tasks, logs, and ACLs.

### PTY Mode

| Environment Variable | Default | Description |
|----------|--------|------|
| `USE_PTY_MODE` | `true` | Enable PTY persistent session mode (false uses `claude -p` single-process mode) |

### Transient Overload Retry

| Environment Variable | Default | Description |
|----------|--------|------|
| `TRANSIENT_RETRY_ENABLED` | `true` | Enable automatic retry for transient 429/overload |
| `TRANSIENT_RETRY_MAX` | `5` | Max retry count |
| `TRANSIENT_RETRY_BASE_DELAY` | `10` | Base backoff delay (seconds) |
| `TRANSIENT_RETRY_MAX_DELAY` | `120` | Max backoff delay (seconds) |

### ask_user Intercept

| Environment Variable | Default | Description |
|----------|--------|------|
| `ASK_USER_ENABLED` | `true` | Enable AskUserQuestion intercept (false auto-removes already injected hook) |
| `ASK_USER_TIMEOUT` | `1800` | Timeout waiting for user answer (seconds) |

### `/tmp` Temp Space Protection

| Environment Variable | Default | Description |
|----------|--------|------|
| `TMP_CLEANUP_ENABLED` | `true` | Enable host & shared Docker `/tmp` pressure check & safety cleanup |
| `TMP_CLEANUP_USAGE_THRESHOLD` | `0.80` | Trigger when capacity or inode usage reaches this value |
| `TMP_CLEANUP_INTERVAL_SECONDS` | `10800` | Background check interval (seconds, default 3 hours) |
| `TMP_CLEANUP_MIN_AGE_SECONDS` | `21600` | Temp artifacts must idle at least this long before cleanup (default 6 hours) |

Service checks once on startup, then background watchdog checks by interval. When either capacity bytes or inodes reach threshold, triggers; below threshold won't scan candidates, won't affect Agent. Once triggered, processes all qualified candidates this round, no cleanup target line set.

Cleanup won't execute wildcard `rm /tmp/*`. Only handles fixed-name whitelist Sub-Agent expired logs, uniquely-named skill/download/discussion temp files, requires same service user, same filesystem, ordinary files over min age; host directories never recursively delete. Unverifiable-from-cleanup-process session/workspace migration staging, unknown files, X11/Xvfb, logged-in browser data, and `/tmp/ccm-update-*` update/rollback evidence always retained. When safe candidates insufficient, only logs alert and retries next cycle, won't expand delete scope, won't create extra model tasks.

Shared Docker execution mode's `/tmp` is independent 2GB tmpfs in container, also uses same threshold. Each container Agent holds shared lease from creating child process to full descendant recycling; when threshold reached, only gets exclusive lease and proves container completely idle except fixed PID 1 and cleanup process, will clear this disposable private tmpfs. Containers newly built or rebuilt due to original config changes use Docker `--init` to recycle Agent leftover orphan processes; existing containers won't be forcibly rebuilt just to add `--init`. Active Agents, unknown PIDs, permission insufficient, or post-cleanup still hitting trigger line will block new container Agents, never falls back to host bare processes. Remote Workers upgrading to same version will run their own host watchdog and same container gate.

### Pool Configuration

| Environment Variable | Default | Description |
|----------|--------|------|
| `POOL_ENABLED` | `true` | Enable Claude account pool |
| `POOL_CONFIG_PATH` | `~/.claude-pool/accounts.json` | Account pool config file path |
| `POOL_COOLDOWN_SECONDS` | `300` | Cooldown duration for hit-limit accounts (seconds) |

Pool enabled by default. On first startup, if `accounts.json` doesn't exist but `~/.claude/.credentials.json` has valid credentials, system auto-adds default account to pool.

Open pool panel via **Pro** button at right header to:
- View per-account 5h/7d quota utilization
- Click **+** add new account (requires email + SMS token)
- Refresh OAuth Token / re-login
- Manually switch preferred account

When multi-account, hit-limit or auth failure automatically switches accounts, seamless `--resume` via hardlinking session directory.

### Auto-Login Browser for Accounts

| Environment Variable | Default | Description |
|----------|--------|------|
| `CCM_XVFB_DISPLAY` | `:99` | Auto-login dedicated X display; multiple CCM on same machine must differ |
| `CCM_LOGIN_TMPDIR` | `~/.cache/ccm/login-tmp` | Disk directory for Chrome profile/diagnostic files, avoid RAM-backed `/tmp` |
| `CCM_LOGIN_MIN_AVAILABLE_MB` | `512` | Reject starting new login browser if available memory below this |
| `CCM_LOGIN_MIN_TEMP_FREE_MB` | `512` | Min available space in login temp directory |

Claude/Codex auto-login shares one Xvfb manager and login lock. Manager uses private Xauthority, cross-process display lock, and actual display ready detection, persistently records Xvfb PID/start time and socket inode. Only residual sockets belonging to dead manager owner will be cleaned; unknown or replaced sockets continue fail-closed. Chrome uses independent profile and dynamic DevTools port each time, verifies browser websocket identity via profile's `DevToolsActivePort`, won't connect orphan Chrome on fixed port again.

### Worker (Distributed Execution Node)

| Environment Variable | Default | Description |
|----------|--------|------|
| `WORKER_SSH_KEY_PATH` | (Required) | SSH private key `.pem` file path |
| `WORKER_SSH_USER` | `ubuntu` | Worker EC2 SSH username |
| `WORKER_ENABLED` | `true` | Enable Worker function |
| `CCM_NODE_ROLE` | `manager` | Durable database role; Worker bootstrap writes `worker` and a bound database cannot switch roles in place |
| `WORKER_INSTANCE_TYPE` | (Inherit Manager) | Override Worker EC2 instance type |
| `WORKER_IMAGE_ID` | (Inherit Manager) | Override Worker AMI ID |

> Full Worker config and prerequisites see [Worker Deployment Guide](docs/worker-deployment-guide.md)

### Git Related

| Environment Variable | Default | Description |
|----------|--------|------|
| `MERGE_PUSH_RETRIES` | `3` | Max retry count for rebase + push |
| `AUTO_PUSH_TO_ORIGIN` | `true` | Auto-push after completion |

### Database Auto-Backup (Optional)

Integrates [auto-backup](https://github.com/zjw49246/auto-backup), supports periodic backup of SQLite database to local, AWS S3, or Alibaba Cloud OSS. PostgreSQL/MySQL please use corresponding database's native backup tools; built-in scheduler will reject external database URLs as local files.

| Environment Variable | Default | Description |
|----------|--------|------|
| `BACKUP_ENABLED` | `false` | Set to `true` to enable backup |
| `BACKUP_TYPE` | `local` | Target type: `local` / `s3` / `oss` |
| `BACKUP_INTERVAL_SECONDS` | `3600` | Backup interval (seconds) |
| `BACKUP_MAX_COPIES` | `10` | Max backup copies to retain |
| `BACKUP_DESTINATION_PATH` | `` | (local) Backup target directory |
| `BACKUP_S3_BUCKET` | `` | (s3) S3 bucket name |
| `BACKUP_S3_REGION` | `` | (s3) AWS region |
| `BACKUP_S3_ACCESS_KEY` | `` | (s3) AWS Access Key ID |
| `BACKUP_S3_SECRET_KEY` | `` | (s3) AWS Secret Access Key |
| `BACKUP_OSS_ENDPOINT` | `` | (oss) OSS Endpoint |
| `BACKUP_OSS_BUCKET` | `` | (oss) OSS bucket name |
| `BACKUP_OSS_ACCESS_KEY` | `` | (oss) Alibaba Cloud Access Key ID |
| `BACKUP_OSS_SECRET_KEY` | `` | (oss) Alibaba Cloud Access Key Secret |

**Example (Local Backup):**
```env
BACKUP_ENABLED=true
BACKUP_TYPE=local
BACKUP_DESTINATION_PATH=/mnt/backup/claude-manager
BACKUP_INTERVAL_SECONDS=3600
BACKUP_MAX_COPIES=10
```

## Deploying Multiple Instances on Same Machine

Can deploy multiple Claude Code Manager instances on same machine, serving different users/teams, pushing to different GitHub account repos.

### 1. Prepare Independent `.env`

Each instance needs independent port, Token, and database:

```env
# Instance A (port 8000)
AUTH_TOKEN=token-for-user-a
PORT=8000
DATABASE_URL=sqlite+aiosqlite:///./claude_manager_a.db

# Instance B (port 8002)
AUTH_TOKEN=token-for-user-b
PORT=8002
DATABASE_URL=sqlite+aiosqlite:///./claude_manager_b.db
```

### 2. Configure Git Credentials (Critical)

Each instance may need to push to different GitHub account repos. Configure in Frontend "Global Git Settings" (Projects page gear icon):

**Recommended to fill both SSH and HTTPS credentials**, system auto-selects based on remote URL protocol:

| Field | Description |
|------|------|
| Author name / email | git commit author info |
| SSH private key path | e.g., `/Users/you/.ssh/id_ed25519_account_b` |
| HTTPS Username | GitHub username |
| HTTPS Token | GitHub Personal Access Token (PAT) |

**Notes:**
- SSH key globally unique on GitHub, one public key binds only one account
- macOS Keychain `osxkeychain` caches old account credentials, system auto-bypasses (`GIT_CONFIG_GLOBAL=/dev/null`)
- HTTPS Token must be generated by **owner account** of target repo, not local machine account

### 3. Generate SSH Keys for Different GitHub Accounts

```bash
# Generate for account A
ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519_account_a -C "account-a@github" -N ""

# Generate for account B
ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519_account_b -C "account-b@github" -N ""
```

Configure Host aliases in `~/.ssh/config`:

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

Add public keys to corresponding GitHub accounts.

### 4. Cloudflare Tunnel Routing

Configure different subdomains for each instance in `~/.cloudflared/config.yml`:

```yaml
ingress:
  - hostname: user-a.yourdomain.com
    service: http://localhost:8000
  - hostname: user-b.yourdomain.com
    service: http://localhost:8002
  - service: http_status:404
```

Add DNS routing:
```bash
cloudflared tunnel route dns <tunnel-name> user-a.yourdomain.com
cloudflared tunnel route dns <tunnel-name> user-b.yourdomain.com
```

### 5. Startup

```bash
# Build frontend (shared)
cd frontend && npm run build && cd ..

# Start Instance A
PORT=8000 uv run uvicorn backend.main:app --host 0.0.0.0 --port 8000 &

# Start Instance B (use Instance B's .env)
PORT=8002 uv run uvicorn backend.main:app --host 0.0.0.0 --port 8002 &

# Start Cloudflare Tunnel
cloudflared tunnel run <tunnel-name>
```

## Architecture Notes

- **GlobalDispatcher**: Only assigns tasks, starts Claude Code, judges success/failure. All git operations fully autonomously completed by Claude Code
- **Claude Code Integration**: Default PTY mode (persistent interactive session, multi-turn cold-start free); switchable to `claude -p` single-process mode (`USE_PTY_MODE=false`)
- **Process Timeout Protection**: Auto-kill after `TASK_TIMEOUT_SECONDS` (default 30 mins) exceeds, prevents hung processes
- **Multi-turn Conversations**: `session_id` bound to Task, follow-up uses `--resume <session_id>` to continue session
- **Codex Shared Transport Stop**: `stop-session` only stops target turn. If precise interrupt temporarily unconfirmable, and same account app-server still serving other turns or already admitted requests, API returns 409 and keeps original task running evidence, can retry later; won't kill other tasks on same account to stop one task
- **Codex Fast**: `Task.codex_service_tier` persistently saves `default|priority`. Standard explicitly clears any session-level Fast tier; Fast must pass the account model-catalog capability check, app-server priority admission, and the CCM loopback Responses proxy's verification that the exact thread/turn sent `service_tier=priority`. A successful `response.created` proves that the upstream accepted the request; the response's own `service_tier` is informational telemetry, so `auto`, `default`, or an omitted field does not disable the account. A conflicting request tier, non-2xx response, unknown lineage, or unavailable proxy still fails explicitly, and Fast never falls back to `codex exec`. Logs and chat events record `service_tier_request_verified=true`, the upstream-reported tier, and the response id without claiming that actual Fast execution was proven. The Fast Goal evaluator uses the Task's model and the same request-acceptance chain; current Distill cannot provide equivalent evidence, so Fast Tasks return 409 before Distill executes.
- **Sub-Agent System**: Uniformly stored in `sub_agent_sessions` table, `agent_type` distinguishes categories (monitor / native-agent / native-monitor). CCM's own sub-agents have independent MCP server, communicate with system via HTTP API
- **Transient Overload Retry**: Strictly distinguishes Anthropic infrastructure-side 429/overload from account quota exhaustion, former backoff retries same account, latter goes through pool rotation
- **Process Management**: `asyncio.create_subprocess_exec` start, must unset `CLAUDECODE` env var to avoid nested detection
- **Stop Mechanism**: SIGTERM → wait 10s → SIGKILL

## Distributed Worker

CCM supports distributing tasks to remote EC2 Worker nodes for execution, breaking single-machine concurrency limits. Each Worker runs complete CCM service with independent Claude account pool, Manager uniformly manages via VPC internal network.

**Core Capabilities:**
- **One-Click Creation** — Click + on Workers page, auto-creates EC2, deploys code, installs dependencies, starts service (config inherits from Manager itself, no need to manually fill AMI/machine type/subnet)
- **Task Forwarding** — Select execution Worker when creating task or modifying existing task, all Chat/Stop/Retry/Plan operations auto-proxied, frontend zero-perception
- **Delegated Roles** — The Worker Token authenticates the Manager control plane only; each ordinary turn inherits the Manager-validated user role, and a Worker never resolves a Manager User ID against its own User table
- **Real-time Migration** — Tasks can migrate between local and Worker anytime, session files and working directories auto-sync, `--resume` seamlessly connected
- **WebSocket Relay** — One WS connection per Worker, logs relayed to Manager in real-time and stored copy, auto-reconnects + fills on disconnect
- **Lifecycle Management** — Supports shutdown (preserves data) / startup / destroy (auto-migrates back all tasks), health check auto-recovers degraded Workers every 30s
- **Version Lock** — Worker deploys exactly same code version as Manager via rsync, health check verifies commit consistency

**Prerequisites:** Manager runs on EC2 + IAM Role has EC2 permissions + `.env` configures `WORKER_SSH_KEY_PATH`.

Detailed deployment steps see [Distributed Worker Deployment Guide](docs/worker-deployment-guide.md).
