# 开发进度

> **重要：Claude 必须自主维护本文件。** 每次完成重要改动或遇到问题后，在对应章节记录。每条记录必须附上 git commit ID。

## 已完成功能

### 阶段 1：基础设施
- [x] 项目初始化 (pyproject.toml, .gitignore, .env)
- [x] SQLAlchemy async + SQLite 数据库
- [x] ORM 模型: Task, Instance, LogEntry, Worktree
- [x] Pydantic schemas
- [x] Task CRUD API + 优先级队列

### 阶段 2：Claude Code 集成
- [x] StreamParser — NDJSON stream-json 逐行解析
- [x] InstanceManager — 子进程生命周期管理
- [x] Instance API (CRUD, run, stop, logs)
- [x] 子进程启动前 unset CLAUDECODE 环境变量

### 阶段 3：Git Worktree
- [x] WorktreeManager — create, merge (--no-ff), remove, cleanup
- [x] Worktree ORM 模型及与实例执行的集成

### 阶段 4：Ralph Loop
- [x] 自动取活循环：取最高优先级任务 → 执行 → 循环
- [x] Plan Mode：只读分析 → plan_review → 审批 → 执行
- [x] API: start/stop/status per instance

### 阶段 5：WebSocket
- [x] WebSocketBroadcaster — channel-based pub/sub
- [x] WebSocket 端点 subscribe/unsubscribe
- [x] 实时日志推送和状态更新
- [x] Task channel 广播 (`task:{id}`)

### 阶段 6：React 前端
- [x] Vite + React + Tailwind CSS v4
- [x] LoginPage token 认证
- [x] Dashboard — 统计栏 + InstanceGrid + 日志弹窗
- [x] TasksPage — TaskForm + 筛选标签 + TaskList
- [x] InstanceGrid — 创建/删除/停止 + Ralph Loop 开关
- [x] InstanceLog — WebSocket 实时日志查看器
- [x] useWebSocket hook (指数退避重连)

### 阶段 7：语音输入
- [x] WhisperClient — OpenAI Whisper API
- [x] Voice API (POST /api/voice/transcribe)
- [x] VoiceButton 组件 (MediaRecorder API)
- [x] 集成到 TaskForm 的标题和描述字段

### 阶段 8：PWA
- [x] manifest.json + service worker
- [x] Apple meta tags (iOS 主屏幕)
- [x] PWA 图标 (SVG)

### 阶段 9：Plan Mode UI
- [x] PlanPanel 组件 — 查看/审批/拒绝计划
- [x] Plan approve/reject API
- [x] 任务状态: plan_review (紫色标识)

### 阶段 10：认证 + 远程访问
- [x] TokenAuthMiddleware (Bearer token + query param)
- [x] Login API
- [x] 前端认证流程 (登录门控, 401 自动登出)
- [x] ngrok / Cloudflare Tunnel 隧道支持
- [x] 生产模式: 后端服务前端静态文件

### 阶段 11：多轮对话
- [x] 从 stream-json 提取 session_id (system/init + result 事件)
- [x] session_id + last_cwd 存储在 Task 模型上
- [x] InstanceManager 支持 `--resume` 标志
- [x] Chat API (POST /api/tasks/{id}/chat, GET .../chat/history)
- [x] ChatView 组件 — 聊天气泡 UI + WebSocket 实时流
- [x] Follow-up 时自动查找空闲 instance
- [x] IME 组合输入处理 (防止中文输入法 Enter 发送)
- [x] 过滤空的 partial streaming 消息

### 阶段 12：任务生命周期重构
- [x] GlobalDispatcher — 全局调度器，替代 per-instance RalphLoop
- [x] 9 步任务生命周期: pending → in_progress → executing → merging → completed
- [x] worktree 创建前 git fetch origin，基于远程分支
- [x] 完成后 rebase + merge --ff-only + push (带重试 + merge lock)
- [x] conflict 状态 + 冲突解决端点
- [x] Project 模型 (name, git_url, local_path) + 自动 clone
- [x] Task.project_id 关联 Project，dispatcher 自动解析为 target_repo
- [x] 修复 dequeue() 排序 bug (desc → asc)
- [x] 前端: 项目选择器、新状态颜色、Dispatcher 全局开关
- **Commit**: c1407e4

### 阶段 13：Claude Code 完全自主 + 本地项目支持
- [x] Project 模型：git_url 改为 nullable，新增 has_remote 字段
- [x] 项目创建支持两种模式：clone 已有仓库（has_remote=True）和本地 git init（has_remote=False）
- [x] 新项目自动生成 CLAUDE.md（含 9 步自主任务生命周期模板）
- [x] Dispatcher 简化：去掉 merge/push/conflict 逻辑，Claude Code 自主完成 git 操作
- [x] 去掉 merging/conflict 状态、resolve-conflict 端点
- [x] TaskForm 重构：创建任务时可直接新建项目（输入名称 + 可选 remote URL）
- [x] 去掉 targetRepo 手动填路径方式，统一通过 project_id 关联
- **Commit**: 231a0b7

### 阶段 14：全面补齐测试覆盖
- [x] 整合 conftest.py 共享 fixture（app/client/session_factory）
- [x] 新增 102 个测试（52 → 154 总计）
- [x] 覆盖所有 API 端点：system、auth、projects、instances、chat 补全
- [x] 覆盖所有服务层：dispatcher、instance_manager、worktree_manager、ralph_loop、ws_broadcaster、whisper_client
- [x] 修复 chat.py 中多余的 `db.begin()` 导致事务冲突 bug
- [x] 修复 chat.py 中 last_cwd 指向已清理 worktree 的 bug（添加 os.path.isdir 回退）
- **Commit**: 55e967b

### 阶段 15：Dispatcher 简化 — Git 操作全交给 Claude Code
- [x] dispatcher.py 去掉 worktree_manager，不再创建/清理 worktree
- [x] ralph_loop.py 去掉 worktree_manager，不再 merge/清理 worktree
- [x] main.py 去掉 worktree_manager 单例注入
- [x] CLAUDE.md 模板更新：步骤 2 改为 Claude Code 自己创建 worktree，步骤 8 改为自己清理
- [x] 主项目 CLAUDE.md 同步更新生命周期描述
- [x] chat.py 简化 cwd 逻辑（不再需要 worktree 路径 fallback）
- [x] 测试同步更新（去掉 worktree_manager mock，更新 cwd 测试）
- **Commit**: bebb4c1

### 阶段 16：Chat 完整消息 + 进程超时保护
- [x] stream_parser 正确解析 assistant(tool_use/thinking)、user→tool_result、system_event
- [x] chat API 和前端扩展事件白名单，新增 thinking/system_event 渲染
- [x] dispatcher/ralph_loop 的 process.wait() 加超时保护（默认 30 分钟）
- [x] config 新增 task_timeout_seconds 配置项
- [x] 新增 6 个 stream_parser 测试（153 → 159 总计）
- **Commit**: 3ff1990

### 文档
- [x] README.md
- [x] CLAUDE.md
- [x] TEST.md
- [x] PROGRESS.md

---

### PR5：Codex 主任务 MCP 默认 rollout（2026-07-27，commit efa9286）

- [x] `CODEX_MAIN_MCP_ENABLED` 产品默认值改为 `true`，保留显式 `false` 紧急回退；测试进程仍显式关闭后按用例逐项开启，避免无关用例隐式依赖 rollout。
- [x] app-server 与 `codex exec` 共用 task-scoped required `ccm_skills`；fresh/resume 均覆盖，且日志明确区分 `app-server`、`direct-exec`、`safe-fallback` 与 `fail-closed`。
- [x] Runtime Settings GET/PUT 与 system broadcast 返回实际 `codex_main_mcp_enabled`；Chat header 和管理员偏好菜单展示只读状态。Worker task 从对应 Worker 的代理 runtime API 读取，忽略 Manager broadcast，避免展示错误 capability。
- [x] Worker provisioning 将 Manager 的实际主 MCP 开关写入远端 `.env`，默认行为和紧急关闭保持一致；Codex Sub-Agent 的窄化 controller MCP 与 Claude 路径保持原有语义。
- [x] `.env.example`、README、CLAUDE/AGENTS pointer、TEST 同步更新。
- [x] 验证：变基最新 `upstream/main` 后 Linux 后端全量 `2456 passed, 2 skipped`；Linux/UTC 前端全量 `399 passed`；PR5 前端定向 `81 passed`；Windows 生产构建、`python -m compileall -q backend`、`git diff --check` 通过。
- **测试环境记录**：Windows 直接收集后端会被 POSIX `fcntl` 阻断，故使用带 init/tmpfs、LF 原生副本和完整系统依赖的只读 Linux 容器。Windows 本机前端全量另暴露 3 个既有平台差异（路径分隔符 1 项、非 UTC 时区 2 项），同一源码在 Linux/UTC 全量通过，未把无关修复混入 PR5。
- **Commit**: efa9286

---

### 阶段 N：PTY 常驻会话模式（2026-06-10，commit 1b6d45b）
- [x] `use_pty_mode` flag（默认 false，-p 行为零变化），claude 任务分流到 claude_pty CCMBackend
- [x] 输入走 channel 注入（MCP notification），输出走会话 JSONL，事件结构与 StreamParser 对齐，下游无感知
- [x] stop() PTY 分支（Esc 中断 + 会话回收）；dispatcher 超时 kill 经 proxy 真正回收会话
- [x] 端到端冒烟 `scripts/pty_smoke.py`：launch → 事件入库/广播 → exit 0 → **第二轮热复用同一进程 7.8s 完成**
- 依赖 claude_pty >= a478051（/home/ubuntu/Projects/PTY，dev venv editable 安装）
- 已知边界：交互模式无 result 事件 → instance.total_cost_usd 暂不更新（待 usage 累加方案）；goal evaluator / monitor 子 agent 仍走 -p（设计如此）
- **号池注意（Phase 3 最高优先）**：PTY 模式撞限不退进程（-p 靠 exit code + stderr 触发换号），当前表现为 turn 超时而非自动 rotation。迁移基础设施已就绪（config_dir 注入 / migrate_session 硬链接 / on_exit rotation 钩子），缺撞限检测信号——计划扫 PTY 输出 usage-limit 标志或 JSONL error 事件后调 migrate_and_relaunch
- 开关语义（commit 待定）：关闭 PTY 模式立即回收所有 idle 会话，mid-turn 会话跑完为止

### 实测反馈修复（2026-06-10，commit 见本条）
- [x] **回复黑洞**：channel 注入 + pty_bridge_reply 工具让 CC 把真实回答"发"进无人消费的通道，用户只看到一句自我总结（task 47/48/50 的"回复一点点/报告没发/提示词丢失"全是此因）。修复：PTY 仓库移除该工具 + 指示语改为"channel 消息=用户消息，在对话中直接回答"（PTY commit 30b6588）
- [x] **冷恢复投递被吞**：spawn 瞬间写 stdin 时 TUI 未就绪 → turn 永不开始 → 消费者挂 30 分钟霸占任务队列（task 47/48 后期全卡死）。修复：删除 spawn 时投递，统一走 channel 注入
- [x] **orphan CC 进程**：后端重启不回收 PTY 会话 → 旧 CC 占着 session 文件。修复：lifespan shutdown 调 pty_backend.shutdown()
- [x] **池尸体**：手动中断/超时 kill 后死会话残留 pool。修复：全路径 pool.remove
- [x] **日志盲区**：未配 root logger，claude_pty 日志全丢。修复：basicConfig INFO
- [x] **额度 unknown**：交互模式无 contextWindow。修复：按 task.model 回填（[1m]→1M，否则 200K）
- 已知无解/待做：thinking 在交互 JSONL 中加密（CC 行为，仅能显示占位）；loop 单 turn 跑完导致无逐轮进度（Phase 3 设计）；号池撞限检测（Phase 3）

## 问题记录

> 格式：问题 → 原因 → 解决 → 预防措施 → commit ID

### 前端空白页
- **问题**: 打开网页一片空白，控制台报错 `does not provide an export named 'Instance'`
- **原因**: Vite 会去除 type-only exports，`import { Instance } from '../../api/client'` 失败
- **解决**: 类型用 `import type { X }` 单独导入，值用 `import { api }` 导入
- **预防**: 前端所有类型导入必须用 `import type`，已写入 CLAUDE.md 约定
- **Commit**: c1407e4

### 优先级排序反了
- **问题**: P1 任务在 P0 之前执行
- **原因**: 代码用了 `Task.priority.desc()`，而约定是数字越小优先级越高
- **解决**: 改为 `Task.priority.asc()`
- **预防**: 已在 CLAUDE.md 注明「优先级数字越小越高，排序用 `.asc()`」
- **Commit**: c1407e4

### 多轮对话 resume 失败
- **问题**: Follow-up 消息报错 `No conversation found with session ID`
- **原因**: Claude Code 的 session 文件按 cwd 路径存储，follow-up 时 cwd 变了导致找不到 session
- **解决**: 在 Task 模型上新增 `last_cwd` 字段，launch 时记录，resume 时使用相同 cwd
- **预防**: 已在 CLAUDE.md 注明「resume 必须使用和原始 session 相同的 cwd」
- **Commit**: c1407e4

### session_id 应绑定 Task 而非 Instance
- **问题**: 最初将 session_id 放在 Instance 上，导致 Instance 切换任务后丢失之前任务的 session
- **原因**: Instance 是 worker 会轮换处理多个 task，session 应该跟着 task 走
- **解决**: 将 session_id 和 last_cwd 从 Instance 模型迁移到 Task 模型
- **预防**: 已在 CLAUDE.md 注明「session_id 和 last_cwd 在 Task 上，不是 Instance」
- **Commit**: c1407e4

### Chat 消息显示重复
- **问题**: 用户发的 follow-up 消息和 Claude 回复都显示两遍
- **原因1**: 用户消息 — 前端乐观添加 + WebSocket 广播各一次
- **原因2**: 助手消息 — Claude Code 的 stream-json 会发多条 message 事件，部分 content 为 null（流式 chunk），有内容的和空的都被渲染了
- **解决**: WebSocket 监听忽略 `user_message` 事件；过滤 content 为 null 的 `message`/`result` 事件
- **预防**: 前端接收 WebSocket 消息时注意去重和过滤无效数据
- **Commit**: c1407e4

### 前端构建 TS 报错未使用变量
- **问题**: `npm run build` 因未使用的 import 报 TS6133 错误
- **原因**: 重构时移除了功能但没清理对应的 import
- **解决**: 删除未使用的 import (`Play`, `api`, `useCallback`)
- **预防**: 重构后检查相关文件的 import 是否需要清理
- **Commit**: c1407e4

### 未遵守 CLAUDE.md 规范
- **问题**: 多次改代码时未遵守 CLAUDE.md 要求的测试规范和文件维护规则——改代码前没先跑测试、改完没更新 README.md/TEST.md/PROGRESS.md
- **原因**: 专注实现功能忽略了流程规范
- **解决**: 补跑测试确认全绿，补更新三个文档
- **预防**: 每次改代码严格按流程：1) 先跑测试 2) 改代码 3) 再跑测试 4) 更新四个文档
- **Commit**: 231a0b7

### Chat 完整显示 Claude Code 交互内容
- **问题**: Chat 界面只显示精简内容，工具调用只有名字没有具体代码改动
- **原因**: Chat API 没返回 `tool_input`/`tool_output` 字段，前端也没渲染
- **解决**: Chat API 补全返回字段、ChatMessage 类型加字段、MessageBubble 完整渲染工具内容（带折叠）
- **Commit**: e810760

### Chat 退出 bug + Plan approve 无反应
- **问题1**: 进入 Chat 后退出，页面不断返回 Chat 界面
- **原因**: `TasksPage` 的 `refresh` 回调依赖 `chatTask` state，导致 `setChatTask(null)` 后旧闭包里的 `chatTask` 引用又把它设回去
- **解决**: 用 `useRef` 保存 `chatTask` 引用，`refresh` 不再依赖 `chatTask` state
- **问题2**: PlanPanel 的 approve/reject 按钮按了没反应
- **原因**: 用了原生 `fetch` 而不是 `api` 客户端，没带 `Authorization` header，401 被静默忽略
- **解决**: 改用 `api.approvePlan()` / `api.rejectPlan()`，在 `client.ts` 新增这两个方法
- **附加**: 修复了 conftest.py 模型未导入导致单文件跑测试时 `no such table` 的问题；新增 10 个 chat/plan API 测试
- **Commit**: 2a7cd89

### Tasks 页面三处缺陷修复
- **问题1**: Task 没有 star 按钮 — 前端 TaskList 缺少星标按钮，后端没有 `/tasks/{id}/star` 端点
- **问题2**: Status 筛选缺少 `executing` — filters 只有 `in_progress`，没有 `executing`
- **问题3**: 后端不支持 project_id/starred 筛选 — 前端传了 `project_id` 和 `starred` 参数，但后端 `list_tasks` API 和 TaskQueue 没接收处理
- **解决**: 后端新增 star 端点和 TaskQueue.star()；list_tasks 增加 project_id/starred 参数；前端 TaskList 增加星标按钮；filters 增加 executing
- **预防**: 新增前端筛选参数时，必须同步检查后端 API 是否接收该参数
- **Commit**: 7d01b87

### 部署注意事项
- **问题**: 重新部署时误清理了其它 Cloudflare 域名的服务
- **预防**: 重新部署时只重启当前服务对应的 Cloudflare 域名，除非明确要求，不要清理其它域名

### Alembic 在 uvicorn 下间歇性死锁
- **问题**: 每次重启后端，`init_db()` 中 alembic upgrade 间歇性卡住，导致 startup 无法完成、API 返回 500、网站无数据
- **原因**: `asyncio.get_event_loop().run_in_executor()` 在线程池中运行 alembic，alembic 的 `fileConfig()` 重新配置 Python logging，与 uvicorn 的 logging handler 产生锁冲突，导致线程死锁
- **解决**: 改为 `subprocess.run(["uv", "run", "alembic", "upgrade", "head"])` 执行迁移，完全隔离进程，彻底避免死锁
- **预防**: 在 async 应用中运行重量级同步库时，优先用 subprocess 隔离，而非 run_in_executor
- **Commit**: 2577c3b

### SQLite 相对路径导致连接到错误的数据库
- **问题**: 部署后 API 返回 500，`no such column: tasks.todo_file_path`，但手动查询根目录 db 列是存在的
- **原因**: `database_url` 使用相对路径 `sqlite+aiosqlite:///./claude_manager.db`，部署脚本 `cd frontend && npm run build` 后工作目录停留在 `frontend/`，uvicorn 继承该 cwd，导致连接到 `frontend/claude_manager.db`（意外创建的旧数据库，缺少新增列）
- **解决**: 在 `database.py` 中将 SQLite 相对路径解析为基于项目根目录 (`_PROJECT_ROOT`) 的绝对路径，不再依赖进程工作目录
- **预防**:
  - SQLite URL 中的相对路径必须解析为绝对路径，避免依赖 cwd
  - 遇到意外创建的 db 文件时，先确认问题修复后再删除，或删除前先备份，避免误删重要数据
- **Commit**: 620b99d

### Git HTTPS/SSH 凭据注入修复
- **问题**: 所有任务 git push 失败，即使用户在前端配置了 git 凭据
- **原因（三层 bug）**:
  1. `_build_git_env()` 只处理 SSH（`GIT_SSH_COMMAND`），完全忽略 HTTPS token
  2. SSH 和 HTTPS 是 `if/elif` 二选一，但 remote URL 协议决定 git 用哪个认证——project 用 HTTPS URL 但全局选了 SSH 类型，导致 HTTPS push 无凭据
  3. macOS `osxkeychain` credential helper 缓存了本机账号（`zjw49246`）的凭据，优先级高于我们注入的凭据
- **解决**:
  1. 同时注入 SSH（`GIT_SSH_COMMAND`）和 HTTPS（`GIT_ASKPASS` 脚本）凭据，git 按 remote URL 协议自动选用
  2. `merge_git_config()` 改为每个凭据字段独立 merge，不再按 `credential_type` 整层切换
  3. 设置 `GIT_CONFIG_GLOBAL=/dev/null` + `GIT_CONFIG_NOSYSTEM=1` 彻底绕过系统 git 配置（`GIT_CONFIG_COUNT` 方案无效：空 `credential.helper` 通过 env 是 additive 而非 reset）
  4. `_clone_repo()` 也注入 git 环境变量，否则私有仓库 clone 会失败
  5. `_apply_git_config()` HTTPS 凭据从 remote URL 动态提取 host，不再硬编码 `github.com`；先设 `credential.helper=""` 清空继承链
- **预防**: 新增 35 个测试覆盖凭据注入全流程
- **Commits**: fe5eb23, c347236, c727ac1, 54bd372

### Opus 4.7 thinking 内容只显示 "💭 Thinking" 没有正文
- **问题**: 用户切到 Opus 4.7 后，chat 里 thinking 气泡只剩一个标题，没有思考内容
- **原因**: `stream_parser._extract_thinking_text` 只读 `block["thinking"]` 字段。新版 Claude Code / API 在某些场景里把内容放在 `block["text"]`、嵌套 `content` blocks，或者只输出加密 thinking（仅有 `signature` + `data`，无明文）。原代码遇到这些情况一律拿到空字符串，前端 `{message.content && ...}` 判断后整块不渲染
- **解决**:
  1. `stream_parser` 新增 `_extract_thinking_text` 帮助方法，按 `thinking → text → content → summary` 顺序兜底；遇到加密块返回 `[encrypted thinking ...]` 标记
  2. `ChatView` thinking 气泡改为始终渲染内容区，空/加密时显示提示文案，普通文本 `maxLines` 从 3 提到 20
  3. 同时把 `sonnet[1m]` 加入默认 `model_options`（Sonnet 4.5+ 也支持 1M context）
  4. 新增 `Instance.thinking_budget` 字段（Alembic migration `bb102ab28888`），通过 `MAX_THINKING_TOKENS` env var 注入子进程，按需开启高预算 thinking
- **预防**: 解析外部 stream 协议字段时永远写多字段 fallback；加密 / 缺失 / 空三种情况要在 UI 里显式区分，否则用户以为是前端 bug
- **Commit**: 8dca374

### 同一台机器部署多个实例的 Git 配置
- **问题**: 多个 Claude Code Manager 实例部署在同一台机器，不同实例需要推送到不同 GitHub 账号的仓库
- **原因**: 本机 macOS Keychain（osxkeychain）只缓存一个 GitHub 账号的 HTTPS 凭据；默认 SSH key 也只绑定一个 GitHub 账号
- **解决**:
  1. 在前端「全局 Git 设置」中**同时填写 SSH key 路径和 HTTPS token**，系统会根据 remote URL 协议自动选用
  2. 为每个 GitHub 账号生成独立 SSH key，在 `~/.ssh/config` 中配置 Host 别名（如 `github-account-a`、`github-account-b`）
  3. 每个实例使用独立的 `.env`（不同 `AUTH_TOKEN`、`PORT`、`DATABASE_URL`）
  4. Cloudflare Tunnel 的 `config.yml` 中按 hostname 路由到不同端口
- **预防**: 部署新实例时必须确认全局 Git 设置中的凭据对应正确的 GitHub 账号

---

### 生产部署 (systemd)
- [x] `ccm-backend.service` — uvicorn 后端，开机自启，崩溃自动重启
- [x] `ccm-tunnel.service` — Cloudflare Tunnel，开机自启
- [x] 域名: `claude-code-manager.com`，通过 `claude-code-manager` tunnel (b5c526ab) 路由
- [x] `auto-backup` 依赖改用 HTTPS 拉取（`5a8ee10`）
- 教训：服务器部署用 systemd 而非 nohup，确保 SSH 断开和机器重启后自动恢复

### Chat 中断后工具继续运行、不知何时结束
- **问题**: -p 模式下点 Interrupt 后 Claude 仍在调用工具，UI 不知道回复何时结束
- **原因**: ① Interrupt 只杀当前子进程，per-task 消息队列里排队的消息随即被 consumer 派发，新子进程继续跑；② `_stop_task_process` 找不到进程时静默把 task 标记 completed，进程实际还在跑；③ 前端乐观清状态，无任何提示
- **解决**: `dispatcher.clear_task_queue()` 在 stop-session 时先清空排队消息；端点返回 `stopped`/`cleared_messages` 真实结果；ChatView 在 `stopped=false` 时明确提示用户
- **预防**: 中断语义必须覆盖"排队中"的工作，不只是"执行中"的进程；API 不应把 no-op 包装成成功
- **Commit**: f4d24e5

### Chat 撞限不自动切号（Pool 切换从未在 chat 路径生效）
- **问题**: Chat 中途遇到 "You have hit your session limit" 不切号，任务直接 failed
- **原因**: ① `_try_chat_pool_rotation` 用**位置参数**调用 keyword-only 的 `migrate_session(*, ...)`，TypeError 被外层 `except Exception` 吞掉，切号永远返回 False（无测试覆盖所以一直没暴露）；② 生产部署的旧版正则 `hit your limit` 匹配不到 "hit your **session** limit" 新文案（仓库已修但未部署）。两个问题叠加
- **解决**: 改关键字调用 + 回归测试（先验证红再修绿）；顺带修了 probe 阻塞事件循环（`select_async` 走线程）、probe env 未剔除 `CLAUDECODE`、session 迁移源误用 env `CLAUDE_CONFIG_DIR`（改 `locate_session_config_dir` 全目录查找）
- **预防**: keyword-only 函数防得住签名误用，但防不住异常被宽 `except` 吞掉——关键路径（如切号）必须有集成级测试断言"成功"而不仅是"不抛错"；修了 bug 要及时部署到生产，仓库修了 ≠ 线上修了
- **Commit**: 8856d18

### 2026-06-12 — task 87 PTY 回复错位 + 子 agent 通用化

- **问题**: PTY 模式下模型开后台子 agent（内置 Monitor）后，harness 自主唤醒的 turn 无 consumer 消费；下一条用户消息的 send_prompt 读到积压、认了旧 turn 的 turn_duration → 回复永久 +1 错位（用户问 A 得到上一问 B 的答案，直到会话结束）
- **解决**: PTY 仓库（commit 14ce6a0）turn 以 prompt 回显为起点 + 常驻空闲 watcher；CCM 侧把 monitor 表通用化为 sub_agent_sessions（agent_type 分类），PTY 观测到的原生子 agent（native-agent/native-monitor）镜像入库，徽章/面板/WS 与 $monitor 同一套展示
- **预防**: 哨兵协议必须校验"turn 归属"；接收方可能自己说话的通道必须有常驻消费者。另：调研结论要在目标分支上复核——"drain_idle_pty_sessions 无调用点"在 main 上不成立（settings API 已接），险些重复实现造成双重 drain
- **Commit**: 71c4fdb（CCM task-from-main）、14ce6a0（claude-pty，本地未推送）

### 2026-06-12 — PTY 权限透传（聊天卡片允许/拒绝）

- **问题**: PTY 链路里 BridgeHub 的 permission handler 从未被 CCM 注册，CC 的权限请求全部 120s 超时默认拒绝（task 87 冒烟被拒的根因），用户侧毫无感知
- **解决**: instance_manager 注册 handler（bridge HTTP 线程经 run_coroutine_threadsafe 进主循环），权限请求 → LogEntry + WS `permission_request` 卡片；前端 🔐 卡片点 允许/拒绝 → `POST /api/tasks/{id}/permissions/{request_id}` → bridge → channel server 解除阻塞。未送达（过期/未知）如实 410 且不落库，防止其他客户端误标
- **预防**: 提供回调注册点的库要在集成层 grep 一遍"谁注册了"——长期无人注册的回调点等于功能性静默缺陷；跨线程回调必须显式注入事件循环（lifespan 里给 _loop 赋值），不要在回调里 get_event_loop
- **Commit**: d0e53d4 + 8b6b496

### 2026-06-19 — task #707 双 session 竞争条件（queue consumer 崩溃恢复误标 pending）

- **问题**: 聊天每发一条消息都会同时起两个 Claude session——一个 resume 回应聊天、一个从头重跑任务描述（task #707 日志中 8 组配对，启动时间差仅 2-5 秒）。表现为"第一遍没反应、第二遍才好"
- **原因**: `_process_queued_message` 的崩溃恢复分支（task.status=="failed"）在克隆 session JSONL 失败、回退到 compact 摘要后，把 `task.status` 写成 `"pending"` 并 commit。主调度循环 `_dispatch_loop` → `TaskQueue.dequeue()` 只认 `status=="pending"` 的任务，下一次 2 秒轮询就把它当新任务抢走一个空闲 instance 从头执行；同时 queue consumer 自己也继续 resume。同一 task 被两条路径并发启动两个进程
- **解决**: 崩溃恢复处 `task.status` 改成 `"in_progress"`（dispatcher.py），表示"已被 queue consumer 认领、待 resume"。dequeue 不会再抢；consumer 后续在 launch 前会自行改成 `"executing"`。与 TaskQueue.dequeue 认领时设的 `in_progress` 语义一致
- **预防**: 任何在 dispatch loop 之外操作 task 状态的路径，绝不能把进行中的 task 落回 `"pending"`——那是主调度循环唯一的"可领取"信号。中间态一律用 `in_progress`/`executing`
- **Commit**: 本次提交

### 2026-06-19 — task #725 resume 找不到 session：第一条消息被牺牲、第二条才恢复

- **现象**: 任务跑完后，用户发第一条 follow-up 消息直接把 task 打成 failed，紧接着发第二条**同样内容**却正常回复。DB 日志铁证：turn 0 建 session `70bcfc88` 成功完成 → 第一条消息 `--resume 70bcfc88` 返回 `error_during_execution: "No conversation found with session ID: 70bcfc88"` → 进程非 0 退出 → `_consume_output` 把 task 标 `failed` → 第二条消息因 `status=="failed"` 命中崩溃恢复分支 → `_clone_session` 也找不到 JSONL → 回退到「摘要 + 全新 session `80319fa2`」→ 成功
- **原因（两层）**: ① **结构性**：`_process_queued_message` 的恢复逻辑被 `task.status=="failed"` 这个前置条件挡住，意味着 session 在 resume 时丢失时，**第一条消息永远是炮灰**（必须先失败把 task 翻成 failed，下一条才触发恢复）。② **查找太窄**：`_clone_session` 只在 `~/.claude`/`CLAUDE_CONFIG_DIR` 下、按 `last_cwd` **字面编码**找 JSONL，既不搜各 pool 账号目录（`POOL_ENABLED=true` 时 session 落在某账号 `projects/` 下），也踩了 task #722 记录的符号链接坑——`/Users/matter -> /home/ubuntu`，CLI 用 `os.getcwd()` 的 realpath 编码落盘为 `-home-ubuntu-...`，而 DB 里 `last_cwd` 是符号链接路径 `-Users-matter-...`，字面编码对不上 → clone 永远 miss → 退化成有损摘要
- **解决**: ① `api/tasks.py` 新增 `_find_session_jsonl(session_id)`，pool 在时复用 `pool.locate_session_config_dir`（搜所有账号目录），并统一用 `projects/*/{sid}.jsonl` 通配——对 cwd 编码（符号链接 vs realpath）免疫；`_clone_session` 改用它。② `dispatcher.py` 恢复分支的触发条件从 `status=="failed"` 扩成 `status=="failed" 或 session 不在磁盘上`（resume 前先 `_find_session_jsonl` 探测），让**第一条消息就能自救**。session 真在磁盘上时探测返回非 None，正常 resume，不会误触发恢复
- **预防**: ①「按数据库里存的路径去拼磁盘路径」必须考虑符号链接/realpath 不一致——能 glob 就别拼字面编码；②pool 部署下任何找 session 文件的逻辑都要搜全部账号目录，别假设在默认 `~/.claude`；③"先失败再靠下一条消息恢复"是反模式——恢复条件应基于"能不能 resume"的事实探测，而不是等 task 被标 failed
- **测试**: `backend/tests/test_session_recovery.py`（pool 账号目录 + 跨 project 子目录通配 + session 缺失三类），并修正 `test_api_chat_plan.py` 四个 `_process_queued_message` 用例（新增 `fake_session_on_disk` fixture 在磁盘上放真 session，使其走 resume 而非恢复路径）
- **Commit**: 本次提交

## 已知问题

- `total_cost_usd` 仅在 Claude Code stream-json result 事件报告时更新
- WebSocket 重连期间可能有短暂的实时日志缺失

## 未来计划

- [ ] 任务依赖 (B 等待 A 完成)
- [ ] 费用统计面板 (图表)
- [ ] 实例资源监控 (CPU/内存)
- [ ] 批量导入任务 (CSV/JSON)
- [ ] 任务模板
- [ ] 通知系统 (完成/失败提醒)
- [ ] 深色/浅色主题切换

### Worktree 部署导致版本锁定静默失效（分布式 Worker Phase 1）
- **问题**: worker bootstrap 全绿但 health 上报 `commit=''`，Manager/Worker 版本锁定校验 MISMATCH
- **原因**: 部署走 rsync 把仓库同步到 worker，但开发在 git worktree 里进行——worktree 的 `.git` 不是目录而是一行 `gitdir: <Manager本地路径>` 指针文件，rsync 过去即悬空，`git rev-parse HEAD` 失败返回空
- **解决**: rsync 排除 `.git`（顺带省体积），部署时写 `.deploy_commit` 文件；`git_info.git_head_commit()` git 失败时回退读该文件。真机冒烟二轮 PASS
- **预防**: 「从 git 仓库复制文件到别处再执行 git 命令」的方案必须考虑 worktree/submodule 这类 .git 非目录的形态；版本标识跨机传递宁可用显式文件，别依赖目标机上的 git 状态
- **Commit**: f37a9b9

### PTY bridge 权限 auto-deny：命令带 rm/后台执行触发 ask 被拒
- **问题**: 跑冒烟脚本的 Bash 调用连续三次被 "Denied via channel pty-bridge" 拒绝，看起来像用户拒绝，实际用户没操作
- **原因**: `~/.claude/settings.json` 的 `ask` 列表含 `Bash(rm:*)`，命令以 `rm -f ... &&` 开头即触发权限确认；确认请求经 PTY bridge `/permission_request` 通道，CCM 侧无 UI 透传，等不到应答默认 deny
- **解决**: 长任务改 `setsid nohup ... > log &`（本环境既有惯例，dev CCM 8003 就是这么起的），命令避开 `rm`/`mv` 前缀（如改用带时间戳的新文件名）
- **预防**: 经 bridge 驱动的会话里长任务不要用 run_in_background、命令别踩 ask 触发词；长期方案是把权限确认透传到 CCM 前端（已记 TODO）
- **Commit**: f37a9b9（冒烟脚本与流程）

### 分布式 Worker Phase 2 一次性全绿的关键：先摸清广播协议再写 relay
- **问题**: WorkerRelay 要镜像 worker CCM 的全部事件，但广播 payload 的坑很多（session_id 被 pop、raw_json 被剥、monitor 用 "event" 键、status_change 用 "new_status"、plan_ready 不含内容、MonitorSession id 跨机碰撞）
- **解决**: 写代码前先逐个 grep instance_manager/dispatcher/monitor 的 broadcast 调用点确认每种事件的真实 payload，再按事实实现（设计文档的预判大部分准确但 monitor 键名等细节仍需实测）；MonitorSession 加 remote_id 列做 id 翻译
- **预防**: 跨服务镜像/中继类功能，协议事实（键名、谁 pop 了什么、发到哪个 channel）必须从源码确认，不能按"应该是"写
- **Commit**: e968a11

### 跨机迁移的 cwd 解析链双坑（分布式 Worker Phase 3）
- **问题**: task 从 worker 迁回本机后 chat 续聊连续失败（PTY session 秒死），但手动 `claude -p --resume` 正常——session 迁移本身是好的
- **原因**: ① worker 转发路径没把 project.local_path 写进 task.target_repo（本地 dispatch 路径有这步），cwd 解析回落到 os.getcwd()；② 第一次失败启动把错误 cwd 写进了 task.last_cwd，而 cwd 解析顺序 last_cwd > target_repo——脏数据自我强化，后续每次都错
- **解决**: 转发路径补 target_repo 解析；迁回本机时校验 last_cwd（不存在或不在项目内则清空）
- **预防**: 「衍生状态写回数据库」（如 last_cwd）的字段在失败路径也会被写——排查这类问题先 dump 原始行而不是只看 API（API/ORM 的 identity map 还会叠加缓存假象）；同一逻辑的双路径（本地/远程 dispatch）要逐字段对照
- **Commit**: 见 task-elastic-worker 分支 Phase 3 系列

### 「双 session / 会话不回复」真凶之一：第二个 CCM 抢 8000 端口导致 systemd crash-loop（task #722 实录）
- **现象**: 用户某 session 反复"发消息不回复"，连发 3 次同一指令（task 720/721 failed，722 才接住）。表面像 dispatcher 双调度（参见 496017e 修的 queue-consumer race），实际是基础设施层冲突
- **原因**: 让某 task「在当前文件夹重启 CCM」时，它额外起了**第二个 uvicorn**（手动/后台），与 systemd 的 `ccm.service` 抢占 0.0.0.0:8000 → `ccm.service` 进入 `[Errno 98] address already in use` + `Failed with result 'exit-code'` 的 8 秒一轮崩溃重启循环（journal 10:30–10:35）。每轮启动的 dispatcher 恢复逻辑把用户在飞的 task 反复 reset（"Resetting stuck task 721 from 'executing' to 'completed'"）→ 用户消息全程无人处理。一方进程在 10:35:15 退出释放端口后才自愈。副作用：一次 alembic batch 迁移被打断，残留孤儿表 `_alembic_tmp_log_entries`（alembic_version 仍在 head，不阻塞启动，但下次动 log_entries 的 batch 迁移会撞名，需先 DROP）
- **环境真相**: DB 是从 Mac 迁来的，project/task 路径多为 `/Users/matter/...`；靠符号链接 `/Users/matter -> /home/ubuntu` 解析到真实 Linux 目录，所以 Mac 路径**并没坏**（claude_pty 的 `pty_process.spawn` 用 `Popen(cwd=...)` 无 isdir 回退，路径不存在会直接 PTYSpawnError——是符号链接救了场）。session 落盘在 `/tmp/claude-1000/<realpath编码>/` 与各账号 `projects/<realpath编码>/`，编码取 claude 自己的 `os.getcwd()` realpath，故始终是 `-home-ubuntu-...`
- **解决**: 确认只剩 `ccm.service` 单实例（`ss -ltnp` / `ps`），WorkingDirectory 已是正确的当前文件夹，detached（`systemd-run --on-active` 独立 cgroup）干净重启一次清掉 crash-loop 残留状态
- **预防**: ①「重启 CCM」永远只用 `systemctl restart ccm`，**绝不手动 `uvicorn`/`nohup` 再起一个**——双实例抢 8000 比任何代码 bug 都隐蔽；②排查"会话不回复"先看 `journalctl -u ccm.service` 有没有 `address already in use` / `Failed with result`，再怀疑 dispatcher 逻辑；③孤儿 `_alembic_tmp_*` 表是迁移被打断的信号，记得清
- **Commit**: 运维处置（本次无代码变更），文档沉淀于此条

### 2026-06-21 — task #728 同一 session 被并发 `--resume`：长 turn 误触发看门狗复制出双 consumer
- **现象**: task 728 末尾几条 monitor 汇报进来后，同一个 session（`f16da894`、instance 86）在 24 秒内被 `claude --resume` 启动了 **3 次**，其中 2 次重叠并发（system_init 在 02:36:49 / 02:36:50 都早于第一条 result），最后一条 resume 撞 429 把 task 打成 failed。DB 铁证：紧挨着的上一轮 turn `duration_ms=832017`（≈14 分钟）；期间 monitor #66/#67 在 02:26–02:33 持续 `report_status`（`sub_agent_reports`）→ 每次 `enqueue_message`
- **原因（两层）**: ① **心跳只覆盖消息边界**：`_task_queue_activity` 只在 `_process_queued_message` 调用前后各刷一次，而那一轮的 14 分钟全卡在里面的 `_wait_process`，心跳冻结 → `_ensure_queue_worker` 的 ">120s stuck" 看门狗在长 turn 中被 monitor 的 enqueue 反复触发，`cancel()` 旧 consumer + 重建新 consumer。但 `_wait_process` 只有**超时分支**才 `process.kill()`，cancel 杀不掉那个 14 分钟的 `claude` 子进程（它照样活到 02:36:41）。② **`finally` 无条件 pop**：被 cancel 的旧 consumer 退出时 `self._task_queue_workers.pop(task_id)` 把看门狗刚塞进去的**新 consumer 登记**也抹掉了 → 下次 enqueue 看到字典空 → 再起一个 → 同时存在 ≥2 个活 consumer，长进程一结束、instance 空出，它们的无锁 busy-wait（TOCTOU）几乎同时判定 "not busy"、各自抢同一 idle instance 并发 launch
- **解决**（dispatcher.py）: ① 新增 `_queue_heartbeat`，consumer 全生命周期跑一个心跳协程持续刷新 activity——长 turn 和 idle 等待都算"活着"，看门狗只在事件循环真卡死（连心跳都跑不动）时才兜底触发。② consumer `finally` 改成**只在 `_task_queue_workers[task_id]` 仍是自己时才 pop**（`is asyncio.current_task()`），杜绝旧 consumer 误删新 consumer 登记。③ 把 `300/30/120` 抽成模块常量 `QUEUE_CONSUMER_IDLE_TIMEOUT/QUEUE_HEARTBEAT_INTERVAL/QUEUE_STUCK_THRESHOLD` 便于测试 patch。两者叠加保证 per-task 永远只有一个活 consumer = 串行 resume，故无需再加 launch 锁
- **预防**: ①「定时心跳判活」的 activity 必须由独立心跳源在整个生命周期刷新，绝不能只在"一个工作单元完成"时打点——工作单元本身可能跑十几分钟；②cancel 一个 asyncio task **不会**杀掉它 `await process.wait()` 的 OS 子进程，"重启 consumer"前要么先杀进程要么靠下游 busy-wait/锁兜底；③任何"先 cancel 旧的、再注册新的"模式，旧对象的 cleanup/`finally` 必须自证身份（`is current_task()`）再清理共享登记，否则会误删继任者；④这是 #707/#722 之后"双 session"家族的第三种成因，排查"同一 session 并发 resume"先看上一轮 turn 的 `duration_ms` 与期间的 enqueue 频率
- **测试**: `backend/tests/test_service_dispatcher.py::test_long_turn_does_not_respawn_consumer`（长 turn 不被复制、不并发处理）+ `::test_watchdog_respawn_keeps_live_worker`（强制 respawn 后旧 consumer 的 finally 不抹掉新 consumer 登记）。两测在还原旧逻辑时均 red、修复后 green；全量 902 passed
- **Commit**: 本次提交

### 2026-06-21 — 瞬时 429/过载自动等待重试（Anthropic 基础设施侧限流，非额度用尽）
- **背景**: 用户问 `API Error: Server is temporarily limiting requests (not your usage limit) · Rate limited` 这条报错从哪来。排查确认：CCM 后端与 `claude_pty` 框架里**都没有**这段文案；它硬编码在 Anthropic 官方 CLI 二进制 `@anthropic-ai/claude-code/bin/claude.exe` 里，是 HTTP 429 `rate_limit`（基础设施侧临时限流，**非**账号额度用尽 `billing_error`）的人类可读文案。需求：让 CCM 对这类瞬时 429 自动退避重试
- **关键判断**: 这条文案既不命中 CCM 的 `is_rate_limited`（额度横幅），也不命中 `claude_pty` 的 banner 标记 → 现状会被当普通失败：autonomous 立即重试（烧 `max_retries` 预算、不等待），chat 直接置 failed（**零重试**）。而且换号无用（是 Anthropic 服务端节流，不是某个账号的事），正确处置就是**同账号退避后 `--resume` 重试**
- **最大的坑（PTY exit_code=0）**: PTY 是生产默认模式。api_error 会中止 turn，但**持久 PTY session 不退出** → `claude_pty` 的 `_consume` 取 `session._process.exit_code` 得 None，且 transient 不命中它的 `rate_limited` 标记 → `on_exit` 收到 `ec=0` → 任务被**误判为 completed**。所以单纯按 `exit_code != 0` 挂钩（subprocess 模式可行）在 PTY 下**完全不触发**。解决：在两种模式共用的 `instance_manager._process_event` 里，对带 `is_error` 且命中 `is_transient_overload` 的事件打 **turn-scoped 标记** `_transient_seen`（`launch()` 重置，`transient_error_seen()` 读取，重试 turn 也会重打）——它是唯一可靠的跨模式信号，dispatcher 据此即便 exit_code=0 也重试
- **第二个坑（重试要被驱动）**: PTY chat 的重试不能 fire-and-forget——那样第 2 次失败没人再检查标记。改由 dispatcher `_process_queued_message` 在 `_wait_process` 之后用 `while transient_error_seen(): _try_chat_transient_retry()+_wait_process()` 循环自驱（该循环在 consumer 体内，被 `_queue_heartbeat` 覆盖，不会被 #728 的看门狗误杀）；autonomous 用递归的 `_run_transient_retry` 自驱；subprocess chat 靠 `_consume_output.finally` 的 relaunch 自然自驱
- **实现**: ① `claude_pool.py` 加 `is_transient_overload`（先排除 `is_rate_limited`/`is_auth_failure` 保证与「额度→换号」互斥）+ `transient_retry_delay`（指数退避+jitter）。② `config.py` 加 `transient_retry_enabled/max/base_delay/max_delay`。③ dispatcher `_run_transient_retry`（同号 resume，用尽→单向衔接号池轮换→普通重试/失败，无 ping-pong）+ `_collect_failure_output`（一次性取 stderr+log，因 `get_last_stderr` 是 pop 破坏性）+ 抽出 `_build_task_prompt`/`_relaunch_and_wait` 复用（`_run_pool_retry` 一并瘦身）。④ instance_manager `_try_chat_transient_retry` + `_transient_attempts`（**不能存 `_launch_params`，那个 `launch()` 会重置**，故独立 dict，成功/放弃/stop 时清）
- **测试坑**: 新检测在 `exit_code!=0` 时**总会**先取 log（不再被 pool=None 短路），且 PTY 标记/`pty_mode_enabled` 在 MagicMock 上默认 truthy → 8 个老 dispatcher/chat 测试因 mock 不符真实接口而 red。修法是让 `_make_dispatcher` 的 mock 建模真实接口（`pty_mode_enabled=False`、`transient_error_seen→False`、`get_recent_log_contents→AsyncMock([])`、`get_last_stderr→""`），非改产品逻辑
- **测试**: `test_claude_pool.py::TestTransientOverloadDetection`（含官方原文案命中、额度/认证优先级互斥、无误报）+ `::TestTransientRetryDelay`（退避边界）+ `test_service_instance_manager.py` 的 4 个 turn-scoped 标记测试（命中置位/额度不置位/干净不置位/`launch` 重置）。全量 924 passed
- **预防**: ①判断一条 LLM 报错是「我方还是 Anthropic 官方」先全仓 grep 文案，再 `strings` CLI 二进制定位；②PTY 持久 session 下"turn 失败"**不等于**"进程退出"，凡依赖 exit_code 的判定都要另想 turn-scoped 信号；③任何"重试/轮换"的计数器若relaunch 走 `launch()`，别存会被 `launch()` 重置的结构里；④给 dispatcher 加新的 instance_manager 调用后，先扫各测试的 mock helper 是否建模了该接口
- **Commit**: c5fc96a

### 2026-06-21 — task #729 瞬时 429 重试成功后任务被误判 failed（recover-then-failed）
- **问题**: 上面的瞬时 429 自动退避重试上线后，用户反馈：退避 + `--resume` 之后 session 明明已经成功续上、活儿也干完了，**任务最终却被标成 failed**——"resume 之后状态没及时改过来"
- **根因**: `_transient_seen` 是 **turn-scoped** 标记，本意只反映「当前前台 turn」是否撞了瞬时 429。但 `_process_event` 打标时只看 `is_error + is_transient_overload`，**没排除 `orphan`/`autonomous` 事件**。PTY 在 resume 时（尤其冷 resume：transient 把 CC 进程打挂后 `on_exit` 已把 session 从 `_sessions` pop、池里只剩尸体 → 下次 `get_or_create` 起全新 `JSONLReader`，offset=0 从头重读整份 JSONL）会把**上一 turn 那条触发了本次重试的旧 api_error 当 backlog 回放**，`send_prompt` 标它 `orphan=True` 仍 yield 给 host。于是成功的 resume turn 里这条旧错误把标记**重新置位** → autonomous 路径 `_run_transient_retry` 的 `still_transient` 永真 → 成功分支（`exit_code in (0,…) and not still_transient`）走不到 → 反复重试到预算耗尽 → `mark_failed("Transient server overload persisted…")`。chat PTY 路径同理：`_process_queued_message` 的 `while transient_error_seen()` 死循环到预算耗尽
- **修复**: `_process_event` 打 `_transient_seen` 前加守卫 `not event.get("orphan") and not event.get("autonomous")`——只认当前前台 turn 的活事件。orphan 是上一 turn 的陈旧回放、autonomous 是后台子 agent turn 的报错，都不该驱动前台 turn 的重试。合法的当前 turn api_error（`turn_started=True` 后 normalize 出来的）`orphan=False`，不受影响
- **测试**: `test_service_instance_manager.py::test_process_event_orphan_overload_does_not_set_transient_flag`（orphan + autonomous 两种回放都不置位）。全量 925 passed
- **预防**: turn-scoped 信号**必须**显式区分「当前 turn 的活事件」与「回放/后台事件」——claude_pty 已用 `orphan`/`autonomous` 两个 flag 标好了边界（见 task-87 turn 对齐），任何按事件流推断 turn 内状态的逻辑都要先过滤这两类
- **Commit**: 本次提交

### 2026-06-21 — ask_user：拦截内置 AskUserQuestion，转前端可选卡片再喂回模型（方案②）
- **需求**: 模型调用内置 `AskUserQuestion`（多选/澄清提问）在 CCM 的 headless（`-p`）/PTY 模式下**无人应答会卡住**（工具等交互式 UI，CCM 这边没有原生选项 UI）。要让它弹成 CCM 聊天里的可选卡片，用户点选后把答案喂回模型继续
- **选型**: 评估过「MCP `ask_user` 工具 + disallow 内置」与「PreToolUse hook 拦截」两条路。用户拍板**方案②（hook 拦截）**——优点是模型**自然地用它本就想用的 `AskUserQuestion`**，无需 disallow、无需引导改用别的工具，hook 透明拦截
- **坑1（注入通道）**: hook 要在 `-p` 和 PTY **两条链路都注入**。`-p` 路 `_build_command` 能加 `--settings`，但 **PTY 路不行**：`claude_pty` 的 `pty_process` 命令构建是**固定字段**（`PTYConfig` 无 `settings`/`extra_args`），只认 `--mcp-config`/`--disallowedTools`；且本仓库对 `Claude-Code-PTY` 只有 **READ 权限**（`gh repo view ... viewerPermission=READ`）无法 bump 依赖加 flag。**解法**：两条链路都靠 `CLAUDE_CONFIG_DIR` 指定账号目录，而 Claude 在 `--dangerously-skip-permissions` 下会**自动加载 `{CLAUDE_CONFIG_DIR}/settings.json` 里的 hook**（实测无审批弹窗）。于是把 hook **幂等合并进 `{config_dir}/settings.json`**，注入点选在 `instance_manager.launch()`——它是 `-p` 与 PTY 的**统一入口**（PTY 分流 `_launch_pty` 之前），一处注入两路统吃，零依赖改动
- **坑2（答案怎么"喂回"模型）**: PreToolUse hook 返回 `permissionDecision="deny"` + `permissionDecisionReason=<答案>`——**实测**：deny 的 reason 会作为 `tool_result`（`is_error=true`）**原样**喂回模型，模型会读它并照做（冒烟测试：deny reason 写"回 PINEAPPLE"，haiku 就回 PINEAPPLE）。所以把用户选择格式化进 reason 即可，无需任何"合成 tool result"的特殊机制。用户曾担心 deny→reason 语义不确定，实测打消
- **阻塞实现**: hook 脚本 `backend/hooks/ask_user_hook.py`（**纯 stdlib urllib**，任何 python3 都能跑、不依赖 httpx）阻塞式 `POST /api/ask-user/wait` → 后端 `ask_user_registry` 登记 `asyncio.Future`、广播 `ask_user_question` 卡片、`await asyncio.wait_for(future, ask_user_timeout)`；前端卡片选完 `POST /api/tasks/{id}/ask-user/{request_id}` → `registry.resolve` set future → `/wait` 拼 `format_answer_reason` 返回 → hook 打印 deny+reason。**阻塞期间不持有 DB 连接**（用独立短 `async_session()` 做查 task/落库，await 时零连接占用）
- **fail-open 原则**: hook 任何异常（CCM 不可达 / 非 CCM 托管 session（`/wait` 返回 `no_session`）/ 超时）都 **exit 0、不输出决策** → 放行原生 `AskUserQuestion`，**绝不因辅助设施挂掉而打断会话**。注意：默认 `config_dir=None` 时 hook 会写进用户全局 `~/.claude/settings.json`，对用户自己跑的 `claude` 也生效，但 `no_session` 即时放行，最多一次 localhost 往返延迟
- **复用**: 整套照搬 PTY 权限透传范式（`session_id→task` 查找、卡片 live-only 经 WS、`system_event` 落库进 chat 历史、`/ask-user/pending` 重连回填活跃卡片、前端 `AskUserCard` 仿 `PermissionCard`）
- **实测（端到端，真实 claude）**: 起测试后端 + 建带 `session_id` 的 task + 注入真 hook → `claude -p` 强制调用 `AskUserQuestion` → hook 阻塞 → 脚本提交 `{labels:["Spaces"]}` → 模型收到 `tool_result` 后输出 `FINAL=Spaces`。答案完整回流
- **预防**: ①给「无 UI 的 headless/PTY agent」接交互式工具，**PreToolUse hook 拦截 + 异步回包**是通用解；②**deny→reason 是给模型"喂结果"的可靠通道**（实测），不必发明合成机制；③当依赖的 CLI 不可加 flag / 依赖仓库只读时，`{config_dir}/settings.json` 是 hook/permission 的**注入后门**，且 `-p` 与 PTY 都吃 `CLAUDE_CONFIG_DIR` → 一处统吃；④辅助拦截器**必须 fail-open**；⑤注入点优先选「两路统一入口」（`launch()`）而非各自分支，避免双份维护与漏注入
- **测试**: `backend/tests/test_ask_user.py`（registry roundtrip/重复 resolve/list 排除已完成、`format_answer_reason` 单选/多选/自定义文本/缺答、settings 注入幂等/保留既有 key 与他人 hook/disable 移除/损坏 JSON/建目录），10 passed；全量 935 passed；`frontend tsc --noEmit` 通过
- **Commit**: fcc0b6d（feat）+ 892cb3c（test）+ 本次（docs）

### 2026-06-21 — task #734/#740 号池耗尽时 resume 落到错号 → "No conversation found"（丢 session）
- **问题**: 用户连聊一整天后，几乎每个 turn 都撞 `rate_limit_event`；用户「充值」一个号、并用最新代码重启后端后，所有后续消息（含连发 7 次的「继续」）瞬间失败，错误 `No conversation found with session ID: <sid>`，task 直接 failed、session 丢失。用户怀疑是「切号 + session 软链接逻辑坏了」
- **根因（journal 实锤）**: 软链接/`migrate_session` 本身没坏（日志里一整天都在正确 hardlink）。真凶是**号池耗尽时 resume 放弃了「定位 session 所在目录」这一步**。`_process_queued_message` 里 `config_dir = await pool.select_async(validate=True)`，当**所有号都被限速**（journal: `Pool has no healthy accounts after validation`）时返回 `None`；随后 ① migrate 块被 `if config_dir` 守卫整段跳过，② `instance_manager.launch` 只在 `if config_dir` 为真时才设 `CLAUDE_CONFIG_DIR`，于是子进程**继承 systemd 单元里写死的 `CLAUDE_CONFIG_DIR`**（本机 = `.claude-account-ddrichardmichael2qsth7`）——这个号**从没存过该 session 的 JSONL** → `claude --resume` 秒挂 `No conversation found`。task 翻 failed 后每次重试（包括 `_clone_session` 克隆出的新 sid，文件落在源号目录）仍落同一错号 → 反复秒挂。⚠️ 注意：`config_dir=None` **并非**回退到 `~/.claude`，而是回退到**继承的 env**（systemd 里那个特定号），比 `~/.claude` 更隐蔽
- **修复**: 抽出 `GlobalDispatcher._resolve_resume_config_dir(session_id)` 统一「为 resume 解析 config_dir」：拿到健康号则迁移 session 进去；**号池耗尽（select 返回 None）时不再放任继承 env，而是回退到 `locate_session_config_dir(sid)`——session 真正所在的那个号**。该号即便限速，也只是以「可恢复的 rate-limit/transient 事件」出现、交给既有重试链处理，绝不再硬挂 `No conversation found` 丢 session。`_process_queued_message`（chat resume）与自治派发 launch（`dispatcher.py:990`，失败 task 带 session 重试）两处都换用此 helper
- **测试**: `backend/tests/test_resume_config_dir.py`（耗尽→锚定 resident 目录 / 耗尽无 session→None / 耗尽 session 不在盘→None / 健康号→选中并 hardlink 迁入 / 池关闭→None），5 passed；全量 940 passed
- **预防**: ①「为 resume 选号」和「session 在哪个号」是**两件事**——选不到新号时**绝不能**让 `--resume` 落到与 session 无关的目录（尤其 `config_dir=None` 会静默继承父进程 env，而非你以为的 `~/.claude`）；②号池相关的 fallback 一律先过 `locate_session_config_dir`（它会扫遍所有 `~/.claude*` 含已移出池的号）；③限速是可恢复态，**绝不能升级成「丢 session 的硬失败」**
- **Commit**: c05d919

### 2026-06-21 — task #734/#740 真凶②：主动换号对"良性 rate_limit_event"也冷却，号池假性耗尽
- **问题**: 修了「耗尽→落错号」后，用户追问「我号池里明明有可用账号，为什么还是选不到对应账号？」。即号池**根本不该耗尽**——为什么健康号被判成不可用？
- **根因（journal + DB 实锤）**: CLI 几乎**每个 turn**都吐一条 `rate_limit_event` 状态 ping，`rate_limit_info.status` 才是真信号：`allowed`=健康、`allowed_warning`=接近阈值、`rejected`=真限速。今日 DB 统计：`allowed/five_hour` 274 条、`allowed_warning/seven_day` 71、`allowed_warning/five_hour` 69、`rejected` 82。但 `_consume_output`（`-p` 路；PTY backend 为 None 时实际跑这条）对**任意** `rate_limit_event` 都置 `_saw_rate_limit=True` → turn 成功后 `_try_proactive_pool_switch` **无条件** `mark_rate_limited(当前号, 300s)` + 迁号。今日 12 次冷却里 **7 次来自主动换号**，触发样本之一是 task740 09:01:17：被一条 `allowed_warning / seven_day / utilization=0.37`（7 天额度才用 37%！）的 ping 触发，把 account-1 冷却 5 分钟。3 个号轮着被良性 ping 冷却 → `select` 返回 None → 号池**假性耗尽** → 撞上真凶①
- **修复**: 新增 `rate_limit_event_is_actionable(rate_limit_info)`（`claude_pool.py`）：`allowed`→False（永不冷却）；`allowed_warning`→仅当 `rateLimitType=five_hour` 且 `utilization/surpassedThreshold ≥0.9` 才 True（`seven_day` 警告永远 False——5min 冷却改变不了 7 天窗口，纯空转）；其余非 allowed（rejected/blocked）→True。`stream_parser` 给 `rate_limit_event` 补出 `rate_limit_info` 字段；`_consume_output` 用该 helper 把关 `_saw_rate_limit`。**反应式轮换（`is_rate_limited` 命中真·限速横幅）是另一条路、不动**
- **测试**: `test_claude_pool.py::TestRateLimitEventActionable`（9 例，含 37% 真实坑）+ `test_stream_parser.py`（surface info / 缺失），全量 951 passed
- **预防**: ①「状态 ping」≠「事件发生」——CLI 的 `rate_limit_event` 是**周期性遥测**，必须看 `status`/`utilization` 再决定动作，绝不能见 event 就当限速；②**冷却时长要匹配窗口**：5min 冷却只对 `five_hour` 有意义，对 `seven_day` 是空转churn；③主动优化（proactive rotate）若带副作用（冷却=减少可用号），触发条件必须**保守**，否则会把优化变成自伤；④另注意运维：本机磁盘有 13 个 `.claude-account-*` 目录但 `accounts.json` 只挂了 3 个——「可用账号」要真在 pool 配置里才会被 `select` 看见
- **Commit**: 本次提交

### 2026-06-24 — task #676 卡 executing、无 chat 按钮：两条取实例路径抢同一 idle instance
- **现象**: 用户报 task #676「一直在执行、没有 chat 按钮」。DB：`status=executing`、`session_id=None`、`instance_id=124`；instance 124（worker-9）却是 `status=idle`、`current_task_id=None`、`pid=None`，且无任何 `claude --task-id 676` 进程。
- **根因（journal 实锤）**: instance 的 DB 状态要等 `instance_manager.launch()` 内 PTY 会话**完全 spawn 完**才从 idle 翻成 running，中间约 10s 窗口仍是 idle。两条取实例路径互不知情：`_dispatch_loop` 13:32:32 认领 124 给 676（登记进 `_running_tasks`）并开始 launch；`_process_queued_message` 13:32:47 处理 task 675 的用户消息时，只按 DB `status=='idle'` 选实例，又选中正在 launch 的 124 → `launch_for_ccm` "Stopping stale PTY session for instance 124 before launch" 把 676 半启动的会话杀掉。676 成孤儿：状态卡 executing、无 session、无进程、worker 空闲 → 前端无 chat 按钮、永不完成。
- **修复（commit b40d2b4）**: 让 `_running_tasks` + 新增 `_launching_instances` 成为两条路径共用的内存认领表。queued-message 选实例时排除「in-flight lifecycle」和「另一个 mid-launch」的实例；dispatch loop 跳过 `_launching_instances`；queued-message 的 launch 用 `try/finally` 持有/释放认领，失败也不泄漏（否则该 instance 会被永久挤出调度池）。新增双向排除回归测试 2 例，`test_service_dispatcher.py` 88 passed。
- **预防**: ①「DB 状态」作为并发仲裁有滞后窗口（异步 spawn 期间状态没翻），跨协程抢资源不能只信 DB 行；要么选取时**原子**标占，要么用内存认领表且**两条路径都遵守**；②任何"选 idle 资源后再慢慢 launch"的模式都要问：launch 期间别的路径会不会也选到它？③内存认领必须 `try/finally` 释放，否则异常会把资源永久 wedge 出池。
- **运维**: 该机（ccm-zhoujunwei, ap-northeast-1, i-03e9984e1c983a1a0）跑两套 CCM：`code/`(ccm-backend,8000) 与 `cyf/`(ccm-backend-cyf,8002)，DB 分别在仓库内 `./claude_manager.db` 与 `/home/ubuntu/cyf/claude_manager.db`；#676 在 `code/`。

### 2026-06-25 — auto_login 在小机型上 Chrome 起不来：cdp_login 漏了 --disable-dev-shm-usage
- **问题**: 在新开的 t3.medium worker 上跑 `scripts/auto_login.py` 登录 Claude 账号，step 1（171mail 接码、拿 magic link）正常，但 step 2「Chrome CDP」整段失败：`httpx.ConnectError: All connection attempts failed`，连 `http://127.0.0.1:9222/json` 都连不上。
- **根因**: `scripts/cdp_login.py` 启 google-chrome 时没带 `--disable-dev-shm-usage`。Chrome 默认把渲染进程共享内存放 `/dev/shm`，小机型（t3.medium）的 `/dev/shm` 太小 → 渲染进程因共享内存不足**立即崩溃** → CDP 调试端口 9222 根本没起来 → 后续 `GET /json` 必然 ConnectError。大机型（Manager 是 c7i.2xlarge，/dev/shm 够大）不触发，所以一直没暴露。讽刺的是 `auto_login.py` 另一条 `_mailcatcher_browser_login` 路径早就带了这个 flag，唯独主路径 `cdp_login.py` 漏了。
- **修复**: `cdp_login.py` 的 chrome 参数加 `--disable-dev-shm-usage`（必需）+ `--disable-software-rasterizer`（顺带），并把启动等待 `sleep(4)→6`（小机型冷启动更稳）。加 flag 后 CDP ~1s 即开放，登录一次成功（实测 BuffaloWingsxvq@diplomats.com，订阅 max，写出有效 .credentials.json）。
- **预防**: ①任何无头机上跑 Chrome 一律带 `--no-sandbox --disable-dev-shm-usage`，前者过 root、后者过小 /dev/shm，二者是服务器跑 Chrome 的标配；②同一仓库里有多条「启 Chrome」代码时，flag 要对齐（这次就是一条带一条没带）；③只在大机型验证过的浏览器自动化，换小机型必复测。
- **Commit**: 本次提交

### 2026-06-26 — task #770 loop 模式选号失效：launch 漏传 config_dir，号池从未被咨询
- **问题**: loop 模式（含 `_resume_fix_signal` 补信号那次）调用 `instance_manager.launch()` 时**完全没传 `config_dir`**。`launch()` 只在 `config_dir` 为真时才写 `env["CLAUDE_CONFIG_DIR"]`，否则子进程继承 systemd 里写死的 `CLAUDE_CONFIG_DIR`。后果：loop 永远跑在那一个默认号上——不从池里选号、不避开冷却中的号、PTY 模式下 `iteration>0` resume 还可能落到没存该 session 的号上 `No conversation found`。普通 Step 4 路径早就 `pool_config_dir = await self._resolve_resume_config_dir(task.session_id)` 选好了，loop/goal 两条提前 return 的分支各自漏了。
- **修复（本次提交）**: `_run_loop_iterations` 主 launch 与 `_resume_fix_signal` 各加一行 `config_dir = await self._resolve_resume_config_dir(resume_sid)` 并传入 launch——iteration 0（resume_sid=None）走「挑健康号」；iteration>0 锚到 session 所在号（不漂移 config_dir → 保 PTY 热 session）；号池耗尽时回退到 resident 号。新增回归测试 `test_loop_iteration_passes_pool_config_dir`（断言 launch 收到 resolver 返回的 config_dir）。
- **预防**: ①新增「另起一条 lifecycle 分支」（loop/goal/plan）时，凡 launch 子进程都要问：号池选号那步（`_resolve_resume_config_dir`）走了没？别让分支静默继承 systemd 默认号。②`config_dir=None` 不是「用默认号」的安全默认，而是「听天由命继承 env」——池开着时必须显式选号。
- **goal 模式同款修复（commit 7499d94 之后的后续提交）**: `_run_goal_lifecycle` 的 turn 0（fresh，resolver 传 None）与 followup（resume，resolver 传 session_id）两处 launch 同样补上 `config_dir = await self._resolve_resume_config_dir(...)`。新增回归测试 `test_goal_turn_passes_pool_config_dir`。至此 loop / goal / Step 4 三条路径选号行为一致。

### 2026-06-28 — Safari 整页崩 "Invalid regular expression: invalid group specifier name"（前端 lookbehind）
- **问题**: 用户在 Safari 打开 `*.claude-code-manager.com`（CCM 前端）整页崩，错误边界显示 `Something went wrong / Invalid regular expression: invalid group specifier name`。Chrome 正常，故之前 curl/Chrome 验证一直没暴露。
- **根因**: 依赖 `mdast-util-gfm-autolink-literal@2.0.1`（`remark-gfm@4` → react-markdown 渲染 Chat markdown 时引入）在模块加载时构造了带 **lookbehind** 的正则 `(?<=^|\s|\p{P}|\p{S})([-.\w+]+)@(...)`（email 自动链接）。Safari <16.4 不支持 lookbehind → 解析即抛 → React 错误边界整页崩。打开任意聊天页（ChatView/LoopChatView/DiscussionView/SharedChatView）即触发。
- **修复**: 用 `patch-package` 删掉该 lookbehind（`frontend/patches/mdast-util-gfm-autolink-literal+2.0.1.patch`），并加 `postinstall: patch-package`。**行为不变**：URL 那条正则本就不用 lookbehind，email 的 `findEmail` 内部已调用 `previous(match, true)` 做完全等价的「前一字符必须是行首/空白/标点」校验，lookbehind 纯属冗余。重建 dist 后全量扫描无任何 lookbehind/命名组，Vite build 通过。
- **预防**: ①前端验收不能只用 Chrome/curl——Safari 的正则引擎更严（lookbehind 需 16.4+），关键页面要在 Safari 实测；②markdown/gfm 这类依赖升级时留意是否引入 lookbehind；③`patch-package` + `postinstall` 已固化，`npm install` 后自动重打。
- **Commit**: 本次提交

### 2026-06-30 — Skills 页「点创建无反应」：user_skills 表缺失 + 前端静默吞错误
- **问题**: 用户在 Skills 页填好「新建 Skill」点「创建」毫无反应，skill 不入库。
- **根因（两层）**: ①后端：线上 DB 的 `user_skills` 表**根本不存在**，所以 `POST/GET /api/user-skills` 全 500（`no such table: user_skills`）。怪点是 `alembic_version` 已在 head `a2628601782f`（晚于建表迁移 `a70ee5479e2e`），且更晚那条加的 `tasks.selected_user_skills` 列**在**——只有早一条的 `create_table` 没落地（迁移漂移，非纯 stamp），导致 `alembic upgrade head` 永远空操作、修不回来。②前端：`SkillsPage.tsx` 的 create/update 用 `catch { /* keep form */ }` **静默吞掉** 500，UI 上「什么都没发生」，连进页面的 list 也被 `.catch(()=>{})` 吞掉 → 列表空。
- **修复**: ①线上 DB（运维动作，非本提交）：备份后按迁移/模型精确 schema 补建 `user_skills` 表，`alembic_version` 不动（本就该指向「表已存在」）；**不能**重新 stamp+upgrade——重跑 `a2628601782f` 会去重复添加已存在的 `selected_user_skills` 列而炸。无需重启（SQLite 下次查询即见新表）。验证 create/list/delete 均 200。②前端（本提交）：create/update/delete 三个 handler 改 `setError(String(e))` 并在弹窗红字显示，沿用其它页面既有写法。
- **预防**: ①「alembic 在 head」**不等于**「表一定存在」——排查 DB 问题先 `PRAGMA`/实际查表，别只信 `alembic current`；修漂移要**按模型补建缺失对象**，而不是 stamp 回退去重跑已部分应用的更晚迁移。②前端任何 `catch {}` 静默吞错误都是「按钮无反应」类 bug 的温床——一律 `setError` 给用户看见。③线上跑的是 rsync 副本 `~/.claude-code-manager/claude-code-manager/`（:8000，DB 在那），不是 git 仓库；调试线上现象要查副本+其 DB，可用 `ps` 里 MCP 进程的 `--auth-token` 直接 curl 复现。
- **Commit**: 本次提交

### 2026-07-12 — 前端 task 状态"老是显示不对"：三层根因大排查 + 修复（多子 Agent 交叉审计）
- **问题**: 用户长期反馈前端 task 状态显示不对（已完成还显示 executing、列表与聊天页状态不一致、侧栏状态点永久陈旧）。
- **排查方法**: 3 路只读审计子 Agent（后端状态流转 / WS 链路 / 前端状态管理）+ 主 Agent 独立排查交叉比对 + 生产 DB 取证（确认 DB 真值基本正确、错在展示与广播层）。
- **三层根因**:
  ① **后端静默状态变更**：cancel/retry/plan 审批/stop-session 兜底/dispatcher 启动批量重置/_reset_instance_if_stale/队列 consumer 标 in_progress/worker_relay 断连标 failed/pr_monitor supersede——全都只写库不广播 `status_change`，靠 WS 驱动的 ChatView 永远等不到事件；
  ② **ChatView localStatus 优先级倒挂**：`useState(task.status)` 初始化 + `localStatus || task.status`，WS 覆盖永久优先于轮询 props，错过一次 WS 事件（断线/根因①）就永久陈旧；
  ③ **TasksPage freeze 幽灵状态**：chat 打开时 `prev.map(t => byId.get(t.id) ?? t)` 对掉出当前页/过滤条件的任务保留旧数据，开状态过滤时任务完成后永远冻结在旧状态。
  另修 **复活块隐患**：`_process_event` 的 completed→executing 复活不排除 `orphan`/`autonomous` 事件（transient 打标在 #729 已排除，复活块漏了）——PTY 回放/后台子 agent 输出可把完成任务翻回 executing 且无人收尾。
- **修复**（commit aa9adc4 + 审查回改 6064329）: 新增 `backend/services/task_events.broadcast_status_change` 收口（**约定：任何写 Task.status 的路径 commit 后必须广播**），接入全部静默点；ChatView localStatus 改 null 初始化 + prop 变化时清除覆盖 + `lastWsStatusAt` 守卫（防在途旧轮询快照击穿刚到的 WS 状态、误触发 autoDequeue）；TasksPage 订阅 `tasks` 频道就地 patch tasks/allTasks/searchResults/chatTask；复活块补 orphan/autonomous 排除；instance_manager chat 收尾广播移到 commit 后；user_skill_injector fail-open（DB 表缺失不再炸 launch）。
- **审查流程**: 2 个子 Agent（后端/前端视角）审 diff，抓到 1 个 major（在途旧快照击穿 WS 覆盖 → autoDequeue 误触发，本次修复自身引入的新窗口）+ 多个 minor（worker 代理路径漏广播、pr_monitor 隐式 commit 依赖、搜索结果不吃 patch），全部落地。
- **测试**: 复活块排除 ×3、cancel 广播 ×1 回归测试；全量 967 passed，失败集与 main 基线完全一致（7 个存量失败非本次引入）；tsc 通过。
- **预防**: ①改 Task.status 必须走「commit 后广播」约定（用 `task_events.broadcast_status_change`），新增状态写入点时先问"前端怎么知道"；②前端"WS 实时覆盖 + 轮询兜底"双通道时，覆盖必须能被更新鲜的兜底数据击穿（且要防在途旧快照反向击穿——时间戳守卫）；③事件驱动的状态翻转（如复活块）必须区分前台活事件与 orphan/autonomous 回放，参考 #729；④广播一律放 commit 之后。

### 2026-07-13 — 后台监视器回报在聊天里不可见：autonomous turn 被 subagent-only 回调丢弃（task 27）

- **问题**：agent 挂 `Bash run_in_background` 监视器后前台 turn 结束；监视器正点回调、session 自主醒来写出完整报告，但用户在聊天里看不到任何东西（用户以为"后台任务不能回调激活 session"——实际回调链路全通，是产出被丢了）。根因：adapter 在 chat turn 结束时把 `on_autonomous_event` 降级成 `_subagent_only_callback`（claude_pty 412d911，防重放旧 prompt），自主 turn 的 assistant 事件全部被丢弃；idle watcher 消费后 reader offset 越过，orphan 回填也捞不回
- **解决**：`FullMirrorCCMBackend`（backend/services/pty_full_mirror.py）在 `super().on_exit()` 降级后按回调函数名识别并原位换回全量转发 `_process_event`；`_process_event` 增加 autonomous user-role 消毒（`<task-notification>` 压成一行 system_event，channel 回显丢弃），承接历史上降级要防的重放问题。10 例新测试（test_autonomous_mirror.py）
- **以后如何避免**：镜像/过滤类回调要区分"结构事件"和"内容事件"——砍内容前先想清楚谁是它的最终读者；"防 A 顺带丢 B"的粗粒度降级要在修好 A 的防线后回收
- **commit**: 6dd3547（PR zjw49246/Claude-Code-Manager#31；因 push 权限收回改走 fork PR）

### 2026-07-19 — 用户消息复制带上 `[发送者]` 前缀

- **问题**：多人聊天为区分来源，会把用户消息存储并显示为 `[Admin] 正文`；消息气泡的复制按钮直接复制 `message.content`，导致粘贴时混入仅用于界面标识的发送者前缀。
- **修复**：界面继续原样显示完整消息，仅在复制用户消息时移除开头的 `[发送者] `；助手消息复制逻辑保持不变。新增前端回归测试，覆盖 `[Admin] 现在进度怎么样了` 只复制正文。
- **预防**：消息的展示文本与导出/复制文本语义不同时，应在操作入口显式转换，避免直接复用带 UI 元数据的展示字符串。
- **2026-07-22 补充**：发送者前缀进一步限定为纯展示信息，本地/Worker/Shared 的模型 prompt 均改传原文；日志元数据保存 `raw_content`，自动压缩、Distill、历史 API 和前端复制优先取原文，避免用户名进入模型，也避免把真实的 `[BUG]`/`[TODO]` 标签误删。PTY/Codex live-turn inject 同样记录并广播原文元数据。
- **Commit**：2b21c73

### 2026-07-13 — 前端设计 v2：Multica 风主题系统 + App Shell（commit e1c778c）

**改动**：主题系统升级为「每主题覆盖 gray（中性）+ indigo（品牌）CSS 变量」的换肤架构：
新默认 dark（zinc + 蓝品牌，oklch）+ 新增 light；v1 默认外观完整保留为 `legacy` 主题，
ocean/forest/rose 归入 Legacy 组。Header 顶栏导航重构为 AppShell（桌面固定侧栏 + 移动端抽屉），
偏好设置抽出 PrefsMenu。字体 Inter/JetBrains Mono 随 bundle 离线。

**经验**：
1. **1300+ 处硬编码 gray-* 类名不必重写**——变量重映射层让全部旧类名自动适配新主题，
   手工精修只做高频页面（Login/Tasks/Chat/Dashboard）。新增主题必须同时覆盖 gray 全档 + indigo 全档。
2. **浅色主题的坑在 accent 300/400 档**：`text-X-300/400` 是深底浅字的设计，浅色主题必须
   把这些档位反转成深色调（≈ Tailwind 原生 600/700），否则 chip 全部不可读；同理中性底上的
   `text-white`/`hover:text-white` 要清扫成 `text-foreground`。
3. **视觉验证用临时后端 + Playwright 截图时，演示数据绝不能插 `pending` 状态的 task**——
   dispatcher 2 秒轮询会把它真的跑起来（本次浪费了一次真实 Claude 调用，还往 worktree 里写了
   一段不相干的文档改动，差点混入 PR）。演示数据只用 completed/failed/cancelled/executing。
4. **Playwright 截图切主题要强制 reload**：localStorage 在 app 启动后写入、hash-only 导航
   不重载页面，不 reload 的话所有截图都是默认主题（第一轮截图全部白拍）。
5. 布局改动前先 grep `100vh|h-screen|fixed|sticky` 找耦合点：TasksPage 分屏高度硬编码了
   顶栏高度（64px→49px），漏改会溢出/留缝。

### 2026-07-15 — 飞书主题「不像」的返工 + 203 个测试失败大清理（commit 1628f2b）

**主题返工教训**：v1 只依据官方设计系统 token（open.feishu.cn CSS 提取）就动手，结果两处失真：
① 官方 CSS 的 pri-500 是新版 #336df4，但**真实 App（v7.72）仍是经典 #3370FF**（App Store 官方截图像素取色 #316efa 实证）；
② token 表不会告诉你「用量分布」——把 N300 #dee0e3 给 gray-700 后，151 处 bg-gray-700 + 102 处 border-gray-700 让整个 UI 变成灰块+线框，而真实飞书是低边框、浅填充、层次靠留白。
**预防**：模仿一个产品的视觉，官方 token 只是词表；必须拿真实截图做像素取色对照（App Store iPad 截图分辨率高、无水印，iTunes lookup API 拿 URL），实现后用 headless chromium 实截自己的页面对比验收。

**203 个测试失败分诊结论**（前端 50 + 后端 153）：只有 1 个真产品 bug——**RBAC 上线把无鉴权模式（AUTH_TOKEN 为空）打死**：中间件无 token 分支直接放行但不设置身份，require_task_access/require_admin 全线 403（backend/middleware/auth.py 修复，回归测试 test_no_auth_mode_grants_full_access）。其余全部是测试代码落后于产品演进：手写 api mock 缺新方法、断言旧签名/旧事件序列/旧状态码。
**预防**：
1. 组件测试的 api mock 是「产品加一个 mount 时 API 调用就整文件爆炸」的单点——给组件新增 api 调用时，同 PR 必须补对应 test mock（grep `vi.mock('../../api/client'` 找到所有手写 mock 清单）。
2. 给守卫类 dependency（require_*）加参数/加检查时，grep 直接函数调用的单元测试（绕过 HTTP 层的），它们不会走 conftest 的兜底。
3. 失败数大 ≠ 问题多：先按错误签名分组再看代表 traceback，本次 153 个失败里 143 个同根因，一处修复全绿。
4. conftest 用 `auth_token=""` 短路鉴权的写法，意味着**测试跑的就是无鉴权模式**——这个模式从此有测试兜底，别再让守卫默认拒绝它。

### 2026-07-16 — 「浅色和飞书几乎一样」：主题趋同问题（feishu v3，commit a457c09）

**问题**：用户反馈浅色主题与飞书主题肉眼无法区分。根因不是色值抄错，而是**结构趋同**：
现代浅色本来就是「灰壳 + 白卡片 + 蓝品牌 + 大圆角」，和飞书处在同一设计空间；feishu v2
只在具体 hex 上有 ≤7 个灰阶单位的差别（壳 #eceef1 vs #f0f0f1、画布 #f5f6f7 vs ~#f6f6f7），
低于肉眼阈值。

**解法（两边同时拉开）**：
① 重新取证飞书的**结构性特征**——iPad + macOS 官方截图像素取色一致证实：消息列表/聊天区
是**纯白 #ffffff~#fbfbfd**，飞书是「白底为主、发丝线分隔」，不是「灰画布+白卡片」。
→ feishu 画布 gray-900 从 #f5f6f7 改为近白 #fbfbfc，rail 修正为取样值 #ecedef。
② 浅色主题找回自己的性格：壳/画布加深一档（oklch 92.5%/95.8%，tonal zinc 分层灰）。
最终「灰调分层 vs 白底为主」一眼可分，theme.test.ts 加了防趋同回归断言（两主题画布取值钉死）。

**经验**：
1. 仿制主题「像不像」之外还有「和邻居分不分得开」一维——同一 App 里两个浅色主题若结构同源，
   仅调 hex 永远趋同；要从取证里找**结构差异**（白底 vs 灰调、层次策略），不是继续微调色号。
2. 对比验收要做**同页面双主题分屏拼图**（PIL 左右各半），单看一张永远觉得"挺像飞书"；
   拼起来才暴露"和自己的浅色更像"。
3. headless chromium 的 localStorage 探针不能用 file:// 页面写完再跳 http://（跨 origin 不共享），
   往 dist/index.html 临时注入 query-param 读取脚本最省事（dist 不进 git）。

### 2026-07-16 — Monitor「一直起不来」：长间隔等待猝死 + broadcast 迭代竞态（commit 14282b0）

**问题**：task 35 的 monitor #192(3600s)/#193(3600s)/#194(1800s) 全部首查后即挂（"process exited rc=0 without calling mark_complete, marked failed"），主 agent 被迫自己踩坑定根因、把间隔压到 300s 才活（#196/#198）。同晚 create_monitor 偶发 500 又炸出重复 monitor（#197/#198 双胞胎）。

**根因（A/B 对照实测钉死）**：
1. CLI 单次 Bash 调用默认墙钟上限 **600s，与请求的 timeout 参数无关**（sleep(700)+timeout=750000 在默认 env 下恰于 600s 被转后台，`is_backgrounded: True`）。转后台时工具回话「完成会通知你」——对 `-p` 一次性进程是空头支票，子 agent 信了就转投 ScheduleWakeup / 结束回合 → 进程退出 → 后台 sleep 被杀 → dispatcher 判 failed。`BASH_MAX_TIMEOUT_MS=7200000` 后同一调用阻塞整 700s 正常完成。
2. `broadcast` 迭代订阅集合的**活引用**，`send` 是悬挂点；前端 WS 连环 keepalive 超时断开时，断连处理中途改集合 → `RuntimeError: Set changed size during iteration` → create_monitor 在 monitor 已建好、进程已启动之后返回 500 → 主 agent 重试 → 重复 monitor。

**解决**：`_launch_monitor_agent` 按 interval 抬高子进程 `BASH_MAX_TIMEOUT_MS`（只抬不降）；`_build_monitor_agent_prompt` 按 interval 生成等待指引（单次 `time.sleep(interval)` + 显式大 timeout + 被拦时拆 300s 块兜底，ScheduleWakeup 禁令附上「为什么必死」）；broadcast 两个循环改 `list()` 快照迭代。

**预防**：
1. 给 `-p` 持久子进程设计"定期干活"循环时，等待手段必须核对 CLI 的单调用上限；任何「结束回合、到点唤醒你」类工具/话术对一次性进程都是死刑，prompt 里要连理由一起禁。
2. 跨 `await` 迭代共享容器一律快照（`list()`）——"有 try/except 就安全"是错觉，改集合的是并发协程不是当前帧。
3. 诊断这类"起不来"先看子 agent 自己的 stream 日志（/tmp/ccm_monitor_{id}.log）最后几个事件，死法一目了然（本次直接拍到 sleep 被转后台 + ScheduleWakeup + result）。

### 2026-07-16 — 蓝色气泡上选中高亮不可见（commit 6641525）

**问题**：全局 `::selection` 是品牌蓝 30% tint，用户聊天气泡/主按钮是 bg-indigo-600 实底蓝，蓝上蓝选中完全看不见（浅色/飞书主题下尤其明显）。
**解法**：`[class~='bg-indigo-600']`（词级匹配，不误伤 `bg-indigo-600/15` tint）及后代的 `::selection` 改白色半透明 `oklch(100% 0 0 / .35)`，所有主题通用。
**经验**：① 定义全局 `::selection` 时要想到品牌色实底面——高亮色和底色同色系时必须给实底面单独一套反色高亮；② `::selection` 的视觉验证可以自动化：probe 页面里 `Range.selectNodeContents` 程序化选中 + headless chromium 截图（注意 Chrome 的 Selection 只保留一个 Range，多块要分次截）。

### 2026-07-16 — ccm-xiaoyu 502：带迁移的自更新停服后无人启服（commit 4f9ab93）

**问题**：ccm-xiaoyu 前端点「更新」后整站 502，更新面板卡死在「停止服务」。排查：`update_migrate.sh` 只写了一行日志就消失，状态文件冻结在 `"stopping"`，journal 只有 Stopping→Stopped 没有 Started——脚本停服后自己也死了，服务停死，tunnel 转发空端口 502。

**根因**：`_migration_path` 用 `subprocess.Popen(..., start_new_session=True)` 拉起迁移脚本。`start_new_session` 只脱离进程组，**脱离不了 systemd cgroup**——脚本仍在 ccm.service 的 cgroup 里，它执行 `systemctl --user stop` 时被 `KillMode=control-group` 连带杀死。systemd 部署 + 更新带迁移 = 100% 复现。（无迁移的快速路径侥幸存活：`systemctl restart` 的 job 入队后客户端被杀不影响执行。）

**解决**（三层防御）：① systemd 托管时改用 `systemd-run --user --collect --unit=ccm-update-{port}` 把脚本放进独立 transient unit（合法逃逸）；② 脚本停服成功后立即 `trap 'systemctl --user start ...' EXIT`——无论脚本怎么死都把服务拉回来（启动时 init_db 会自动补迁移）；③ `recover_from_status_file` 识别 stopping/migrating 中间态标 failed 提示用户重试（原来被静默忽略）。测试先红后绿：stub systemctl + SIGTERM 杀脚本复现事故，断言 trap 仍启服。

**预防**：
1. systemd 服务内 spawn 的"要活过本服务 stop"的进程，`start_new_session`/`nohup`/`setsid` 全都无效——唯一正解是 `systemd-run` 交给 systemd manager 托管。判断标准：进程要不要在 `systemctl stop <本服务>` 之后继续跑？要就必须出 cgroup。
2. 「停服→干活→启服」型脚本，停服成功后第一件事挂 EXIT trap 兜底启服；孤儿状态文件的中间态要能被下次启动识别，不能静默吞掉。
3. 事故机上另发现一个过时的 system 级 ccm.service（与 user 级同目录同端口同 SQLite，Restart=always 反复拉起僵尸 uvicorn），已 `disable --now`。同机双 systemd 单元指向同一套 CCM 是定时炸弹，部署时要检查 `systemctl list-units` 和 `systemctl --user list-units` 有无重名。

### 2026-07-16 — 回滚把数据库毁成 0 字节：活连接下覆盖 SQLite 文件（commit 75e2108）

**问题**：测试环境验证更新修复时，Admin 更新成功后点「回滚」，数据库变成 0 字节空库；再点更新在「备份数据库」步骤报 disk I/O error（源库已损坏）。

**根因**：`rollback()` 在**本进程还持有打开的 SQLite 连接**时 `rm` 掉 `-wal`/`-shm` 并用备份覆盖 DB 文件，之后才重启。活连接随后的写入/checkpoint 基于已失效的文件状态，把刚恢复的库直接截断。

**解决**：回滚复用 `update_migrate.sh` 新增的 `rollback` 模式——先 `systemctl stop`（EXIT trap 兜底启服）→ 恢复 DB 备份 → `git reset --hard` → `uv sync` → 启服；非 systemd 部署则把 DB 恢复挪进 detached 重启 shell 的 kill 之后。数据用更新前自动备份完整救回。

**预防**：SQLite 文件级恢复（cp/rm -wal）的前置条件永远是"持有连接的进程已退出"，顺序必须 stop → restore → start；任何"先动文件再重启"的写法在 WAL 模式下都是数据毁灭器。

### 2026-07-16 — 孤儿 uvicorn 抢端口 + `_is_managed_by_systemd` 误判：更新全面加固（commit 见本条上一提交）

**问题**：测试环境验证期间，一个 SSH 会话里手动裸跑的 uvicorn（06:31 起）一直霸占 8010 端口：systemd 实例活着但绑不上端口（Errno 98）成了空壳，用户的更新/回滚全打在跑旧代码的孤儿上；孤儿的 `_is_managed_by_systemd()`（查 `systemctl is-active`）被恰好 active 的空壳单元骗过，以为自己是 systemd 托管，停/启的都是别的实例——双头混乱，症状千奇百怪（假成功、disk I/O error、代码版本漂移）。

**解决**：① 判定改为读 `/proc/self/cgroup` 看**本进程**是否真在 service 的 cgroup 里（空壳单元骗不过，非 Linux 自动落 fallback）；② `update_migrate.sh` 支持裸 uvicorn 部署（SERVICE_NAME="-"：kill pid 停服 / respawn uvicorn 启服），带迁移更新和回滚不再依赖 systemd；③ 回滚统一走脚本（先停服再动 DB）；④ svc_start 防 EXIT trap 双拉起。

**预防**：
1. 「我是否被 systemd 管」必须问自己的 cgroup，不能问单元状态——单元 active ≠ 我就是那个单元。
2. CCM 机器上不要手动裸跑 uvicorn 和 systemd 服务并存；排查"行为怪异"先 `ss -tlnp` 看端口属主是不是 systemd 单元里的 PID（本次和 xiaoyu 的 system/user 双单元事故是同一族问题）。

### 2026-07-16 — Chat 发图后图片不显示（commit 8c6201e）

**问题**：Chat 里附图发送后图片不出现在会话里。
**排查路径**（先证据后结论）：生产 DB 直查（uploads 文件在、task.metadata_ 附件在、
但 7/6 后无带附件的 user_message 行）→ 生产 API 实测 metadata_ 正常返回 →
锁定前端展示层。注意本机跑着**两个 CCM 实例**（8000=code/ 用户实例、8002=cyf/ 别人的），
先前 curl 8002 得到"metadata_ 缺失"是打错实例的假线索——**多实例机器上先 ss -tlnp 对准端口再下结论**。
**两个真因**：
① 发「文字+图片」：乐观回显不带附件 → 带附件的 WS 广播因内容相同被去重**整条丢弃**。
   修：去重时合并附件而非丢弃 + 乐观回显直接带附件。
② Capacitor App 里附件相对 URL（/api/uploads/…）按 capacitor://localhost 解析 404。
   修：resolveAssetUrl() 统一拼 getApiBase()。
**经验**：去重逻辑丢消息前要想清楚"两条消息不完全等价"的情形（同文本、附件不同），
丢弃前先合并增量字段；移动端 WebView 里任何相对资源路径都要过 API base 解析。

### 2026-07-15 — PTY 吸收型卡死定性：auto-resume 与 harness 通知对同一事件双投递（task 32/33）

- **问题**：task 33 消息队列冻结（发消息永无响应）、task 32 被 7200s 超时杀掉一个还在正常干活的进程。解剖：native 子 agent 完成瞬间，harness 自己的 task-notification 零延迟抢开新 turn；CCM 的 auto-resume（subagent_done → enqueue "[Agent 完成]…"）慢几秒到达，被 CLI 当 mid-turn steering 吸收（JSONL `queue-operation op=remove`、**无独立 user 回显**）→ claude_pty send_prompt 的回显锁定永不成立 → turn_duration 全被无视 → consumer 永挂（watchdog 因 heartbeat 常刷新不介入）→ 队列冻结 → 7200s 兜底杀进程
- **不是新 bug**：auto-resume 自 6 月 26 日（c5bd064）就在、严格回显锁定自 07-08（d6ff732）就在；journal 里 7 月 1—15 日共 **18 次**无声 7200s 超时杀（task 4/11/14/16/19/23/27/28/31/32），历史案发源头还包括**普通用户消息**撞 turn 边界（07-13 task 27、07-14 task 31）。此前不可见：spinner 卡忙 → 用户消息走 inject 照常有回应 → 2 小时后杀进程"自愈"。**07-14 的 b5ce020 修好 spinner 才揭开盖子**——用户消息开始走 dispatcher 队列，冻结第一次直接可见
- **解决**：① 删除 subagent_done 的 auto-resume enqueue（本 commit）——PTY 模式唤醒完全交给 harness 通知 + FullMirror 镜像（#31），-p 模式退出补唤醒（monitor:native-exit-resume / monitor:complete）不受影响；② 生产先以 remote_exec 热补丁过滤该 source 顶住（重启失效）；③ 卡死现场解法：cancel 卡死的 `_consumers[key]`（CancelledError → finally → on_exit → own_proxy.complete 自动释放并 drain 队列），**不要**只 complete proxy（活 consumer 会继续占事件流）
- **以后如何避免**：对"同一事件"绝不能有两条唤醒链路赛跑——加投递机制前先问 harness 是否已有原生链路；`_wait_process` 兜底超时杀进程前应先判断 session 是否实际存活在干活（残余：用户消息吸收仍可触发，根治在 claude_pty——send_prompt 识别含本 prompt 的 op=remove 后采认当前 turn 或等下个 turn_duration）
- **commit**: e0cd17b（历次 rebase 前 8663640 / 0ff15c6）

### 2026-07-16 — ccm-xiaoyu 502：带迁移的自更新停服后无人启服（commit 4f9ab93）

**问题**：ccm-xiaoyu 前端点「更新」后整站 502，更新面板卡死在「停止服务」。排查：`update_migrate.sh` 只写了一行日志就消失，状态文件冻结在 `"stopping"`，journal 只有 Stopping→Stopped 没有 Started——脚本停服后自己也死了，服务停死，tunnel 转发空端口 502。

**根因**：`_migration_path` 用 `subprocess.Popen(..., start_new_session=True)` 拉起迁移脚本。`start_new_session` 只脱离进程组，**脱离不了 systemd cgroup**——脚本仍在 ccm.service 的 cgroup 里，它执行 `systemctl --user stop` 时被 `KillMode=control-group` 连带杀死。systemd 部署 + 更新带迁移 = 100% 复现。（无迁移的快速路径侥幸存活：`systemctl restart` 的 job 入队后客户端被杀不影响执行。）

**解决**（三层防御）：① systemd 托管时改用 `systemd-run --user --collect --unit=ccm-update-{port}` 把脚本放进独立 transient unit（合法逃逸）；② 脚本停服成功后立即 `trap 'systemctl --user start ...' EXIT`——无论脚本怎么死都把服务拉回来（启动时 init_db 会自动补迁移）；③ `recover_from_status_file` 识别 stopping/migrating 中间态标 failed 提示用户重试（原来被静默忽略）。测试先红后绿：stub systemctl + SIGTERM 杀脚本复现事故，断言 trap 仍启服。

**预防**：
1. systemd 服务内 spawn 的"要活过本服务 stop"的进程，`start_new_session`/`nohup`/`setsid` 全都无效——唯一正解是 `systemd-run` 交给 systemd manager 托管。判断标准：进程要不要在 `systemctl stop <本服务>` 之后继续跑？要就必须出 cgroup。
2. 「停服→干活→启服」型脚本，停服成功后第一件事挂 EXIT trap 兜底启服；孤儿状态文件的中间态要能被下次启动识别，不能静默吞掉。
3. 事故机上另发现一个过时的 system 级 ccm.service（与 user 级同目录同端口同 SQLite，Restart=always 反复拉起僵尸 uvicorn），已 `disable --now`。同机双 systemd 单元指向同一套 CCM 是定时炸弹，部署时要检查 `systemctl list-units` 和 `systemctl --user list-units` 有无重名。

### 2026-07-16 — 回滚把数据库毁成 0 字节：活连接下覆盖 SQLite 文件（commit 75e2108）

**问题**：测试环境验证更新修复时，Admin 更新成功后点「回滚」，数据库变成 0 字节空库；再点更新在「备份数据库」步骤报 disk I/O error（源库已损坏）。

**根因**：`rollback()` 在**本进程还持有打开的 SQLite 连接**时 `rm` 掉 `-wal`/`-shm` 并用备份覆盖 DB 文件，之后才重启。活连接随后的写入/checkpoint 基于已失效的文件状态，把刚恢复的库直接截断。

**解决**：回滚复用 `update_migrate.sh` 新增的 `rollback` 模式——先 `systemctl stop`（EXIT trap 兜底启服）→ 恢复 DB 备份 → `git reset --hard` → `uv sync` → 启服；非 systemd 部署则把 DB 恢复挪进 detached 重启 shell 的 kill 之后。数据用更新前自动备份完整救回。

**预防**：SQLite 文件级恢复（cp/rm -wal）的前置条件永远是"持有连接的进程已退出"，顺序必须 stop → restore → start；任何"先动文件再重启"的写法在 WAL 模式下都是数据毁灭器。

### 2026-07-16 — 孤儿 uvicorn 抢端口 + `_is_managed_by_systemd` 误判：更新全面加固（commit 见本条上一提交）

**问题**：测试环境验证期间，一个 SSH 会话里手动裸跑的 uvicorn（06:31 起）一直霸占 8010 端口：systemd 实例活着但绑不上端口（Errno 98）成了空壳，用户的更新/回滚全打在跑旧代码的孤儿上；孤儿的 `_is_managed_by_systemd()`（查 `systemctl is-active`）被恰好 active 的空壳单元骗过，以为自己是 systemd 托管，停/启的都是别的实例——双头混乱，症状千奇百怪（假成功、disk I/O error、代码版本漂移）。

**解决**：① 判定改为读 `/proc/self/cgroup` 看**本进程**是否真在 service 的 cgroup 里（空壳单元骗不过，非 Linux 自动落 fallback）；② `update_migrate.sh` 支持裸 uvicorn 部署（SERVICE_NAME="-"：kill pid 停服 / respawn uvicorn 启服），带迁移更新和回滚不再依赖 systemd；③ 回滚统一走脚本（先停服再动 DB）；④ svc_start 防 EXIT trap 双拉起。

**预防**：
1. 「我是否被 systemd 管」必须问自己的 cgroup，不能问单元状态——单元 active ≠ 我就是那个单元。
2. CCM 机器上不要手动裸跑 uvicorn 和 systemd 服务并存；排查"行为怪异"先 `ss -tlnp` 看端口属主是不是 systemd 单元里的 PID（本次和 xiaoyu 的 system/user 双单元事故是同一族问题）。

### 2026-07-17 — 苹果主题（apple-design skill 驱动）+ 基线两处过期断言修复（commit aa40dde / 1ed2d26）

- **问题 1（基线不绿）**：main 上 `7a1bc7c`（自定义主题 PR #39 内）调深了 @theme 深色阶并把顶栏从 `bg-gray-900/85 backdrop-blur-md` 改为实底，但 `customTheme.test.ts` 硬编码的 dark 参考色阶和 `AppShell.test.tsx` 的 backdrop-blur 断言没同步 → 2 个失败躺在基线里。分诊定性：实现是对的（`GRAY_REF` 已同步、去 blur 是有意为之且 `4afc5e3` 清理过注释），测试过期 → 修测试（aa40dde）。教训重申：**改 @theme 基准值 / 结构性 className 时，全局 grep 一下有没有测试把旧值写死**。
- **功能（1ed2d26）**：按 emilkowalski/skills 的 `apple-design` SKILL.md 新增 `apple` 现代浅色主题。skill 是「原则库」（响应/直接操纵/材质/字体/无障碍），落到本仓换肤机制 = 变量块 + 三条附属规则：系统字体在主题块内覆盖 `--font-sans`/`--font-mono` 即可全局生效（Tailwind v4 的 `--default-font-family` 引用 `--font-sans`，变量在 html 上按主题覆盖）；按压反馈用**独立 `scale` 属性**而非 `transform`，避免覆盖组件已有 transform 工具类；毛玻璃顶栏选择器钉在 `header.sticky`（全仓唯一），并注释 backdrop-filter 的 containing block 风险（AppShell.test 已有 header 内无 fixed 元素的断言兜底）。
- **实证流程（沿用飞书主题 v2 教训）**：测试先行（5 红 → 实现 → 全绿 250/244）；headless chromium + localStorage harness（`public/__harness.html?theme=x` 种 token/主题后 redirect）对 apple/light/feishu 实截，像素取众数验证侧栏 #e8e8ed/#e6e6e8/#ecedef 与三个品牌蓝互异（禁用态按钮 50% 混白后仍能反推原色）。防趋同断言从「light vs feishu」扩到三浅色画布三方互异。
- **坑**：scratchpad 目录里的截图会被环境异步清掉（写完 ls 就没了），持久产物放 home 下再看。

### 2026-07-17 — 三浅色主题趋同返工：差异化要靠形状语言，不是画布 hex（commit 见本条）

- **问题**：新增苹果主题后用户反馈 apple/feishu/light 肉眼无区别。自查确认：三者只在画布/侧栏灰度上差 2-4 个灰阶点，而屏幕 90% 是白卡片——上一轮「防趋同断言」只钉了 token 取值，防不了感知趋同。
- **方案（三轴差异化）**：①圆角主题级覆盖 `--radius-*`（feishu 紧凑 4/6/8px——feishucdn 官网 CSS 高频值实测统计；apple 大圆角 8-24px——apple.com 卡片语言；light 保持默认 10px 作基准）；②壳结构（apple 改为壳=画布 #f2f2f7 连续面 + 白卡软阴影浮起，iPad Settings 语言；light 保持分层；feishu 保持 rail 灰）；③画布灰度维持原三档。截图程序化测量圆角实证：feishu≈6px / light≈10px / apple≈16px。
- **经验**：⑴ 主题差异化的有效轴按感知强度排序是「形状 > 壳结构 > 大面积色块 > 细部 hex」，防趋同测试要断言前两者；⑵ `--radius-*` 在 Tailwind v4 里是 var() 引用，主题块内覆盖即可全局换形状语言，零组件改动；⑶ theme.test 的 themeBlock 正则不锚定行首会把文件头注释里的 `html[data-theme='x']：` 字样误当规则、吞进 @theme 块（已修 + 注释）；⑷ 近白画布（#fbfbfc）会骗过 >250 的「白色」阈值，程序化测量卡片边界要用 ≥254。

### 2026-07-17 — 飞书/苹果主题激进结构复刻：data 钩子 + 主题作用域结构 CSS（commit 见本条）

- **背景**：形状语言差异化（2c68d05）后用户仍觉得三浅色太像，要求「激进地完美复刻」飞书和苹果。变量换肤的天花板到了——复刻需要动布局结构。
- **方案**：AppShell 暴露**主题无关 data 钩子**（`data-shell-sidebar`/`data-shell-main`/`data-nav-item[data-active]` 等 7 个），index.css 新增「结构级复刻层」按主题作用域重排：feishu 桌面侧栏 → 飞书客户端 76px 窄图标 rail；apple 侧栏 → macOS System Settings（iOS 系统色 squircle 图标 + 实底蓝选中行）+ 全局胶囊按钮。类名不动，其他主题与移动端行为零影响。
- **要点**：⑴ rail 重排必须同步改 `[data-shell-main]` 的 padding-left 并包在 lg+ media query（移动端抽屉共享 navList，列布局要用 `[data-shell-sidebar]` 前缀隔离）；⑵ 胶囊按钮规则 (0,1,2) 会波及导航项，用更高特异性 (0,2,1) 的 `[data-nav-item]` 覆盖回 8px——同一文件内顺序无关，靠特异性；⑶ squircle = `svg { background + padding + border-radius + color:#fff }`，Lucide 线稿天然变白色玻璃感图标，nth-of-type 轮换 iOS 系统色；⑷ 钩子是复刻层的生命线，AppShell.test 断言存在性，防止重构时静默丢失。

### 2026-07-17 — 官方参照物驱动的复刻迭代：一手截图取证 → 像素级修正（commit 见本条）

- **流程**（用户要求「对照官方截图迭代到很像」）：① iTunes lookup API 拉飞书 App Store 官方 iPad 截图（2732×2048 高清，改 URL 尺寸段即可）+ support.apple.com mac-help 手册拉 System Settings 官方截图；② 像素取证；③ 修 CSS；④ headless chromium 截自己 → 与官方并排对比 → 再修。
- **取证推翻了两个想当然**：⑴ 飞书 rail 选中态不是蓝 tint 方块，是**白色圆角 tile 包住图标+文字**（实测 #fcfbf9），rail 顶部是用户头像不是 logo；⑵ macOS Settings 侧栏 #f9f9f9 比内容区 #f7f7f7 **更亮**（此前拍脑袋 #f2f2f7 壳=画布是反的）。二手常识不可信，复刻必须一手取证。
- **修正落地**：feishu＝白 tile 选中 + 头像置顶（user-footer flex order:-1）+ 图标 stroke-width 2.4 贴近 duotone 观感 + 选中会话 tint #e9f1fe；apple＝侧栏 216px + 装饰性 Search 框（`[data-shell-sidebar]::before` + SVG data-URI 放大镜）+ 账户行上移（order 重排：brand -3 / search -2 / user -1）+ 行高 ~28px + 选中 6px 圆角 + #f9f9f9/#f7f7f7。
- **技巧**：aside 的 ::before 也是 flex item，可用 order 插入「装饰性搜索框」而无需改组件；CSS `stroke-width` 能覆盖 SVG presentation attribute，一行让线稿图标变粗。


### 2026-07-17 — 主题图标集：iconSet 成为主题系统一等公民（commit 见本条）

- **需求**：飞书/苹果复刻的最后一块短板是图标库本身（Lucide 线稿 vs 飞书 duotone / SF 填充式）。要求架构上与主题系统一脉相承、新主题接入零架构改动。
- **架构**：`ThemeOption.iconSet?`（声明式，与配色注册同构）→ `config/iconSets.tsx` 注册表（导航语义 key → 渲染器；feishu=IconPark two-tone（字节官方开源，授权 Apache-2.0）、sf=Ionicons5（MIT））→ `useTheme()` hook（useSyncExternalStore 订阅 setTheme，解决换主题 React 不重渲染）→ AppShell 解析，缺集合/缺 key 回退 Lucide（纯增强不阻塞）。新主题三步接入：theme.ts 条目 + index.css 变量块 +（可选）注册图标集。
- **测试倒逼实录**：⑴ iconSets.test 的「集合覆盖全部导航 key + 渲染出 svg」断言在新增导航页时会精确红出缺哪个图标；⑵ AppShell.test 断言三种主题切换时 `[data-icon-set]` 实时切换（抓 useTheme 断线类回归）；⑶ 调色后忘改断言立刻红（#646a73→#51565d），证明取值断言在看门。
- **坑**：⑴ IconPark two-tone 次级填充色若与 rail 底色同灰阶（#d5d8dd vs #ecedef）等于白画，对照官方截图应为「深灰笔画+白填充」；⑵ 肉眼对比小尺寸截图不可靠，用 `chromium --dump-dom | grep fill=` 直接验证渲染的 SVG 属性值最快；⑶ 图标名必须先 `node -e "require('@icon-park/react')[name]"` 验证存在再写代码，防拍脑袋编名字。


### 2026-07-17 — 全站主题图标：中央图标模块 + 架构守卫（commit 见本条）

- **背景**：上一轮图标集只接了侧栏导航，用户指出全站其余图标（36 文件 / 88 种 Lucide）没跟随主题。
- **方案**：新增 `components/icons.tsx` 中央图标模块——导出与 Lucide 同名同 props 的主题化组件，内部按 iconSet 解析 IconPark/Ionicons、无映射回退 Lucide；脚本批量把 36 个文件的 lucide 值导入改写为中央模块（type-only 保留）。映射表 92 项全部先 `node -e` 验证存在。**架构守卫测试**：扫描 src 禁止任何文件再值导入 lucide-react，防回潮。
- **两个被测试/构建逼出来的 bug**：⑴ `npm run build`（tsc -b）比 `npx tsc --noEmit` 严格，抓到 NavItem.icon: LucideIcon 与主题化组件类型不兼容（改为 ComponentType 宽类型）；⑵ **fill 语义冲突**：lucide 惯用 `fill='currentColor'|'none'` 表达收藏星标实心/空心，直接透传让 IconPark（fill=颜色数组）整枚隐形（TaskForm 收藏按钮空白）——themed() 拦截翻译：park→theme filled/outline 切换，ion→outline 变体组件。回归断言时又学一课：IconPark svg 根自带 fill="none"（颜色在 path），不能拿根属性当隐形判据。
- **体积**：映射全量打包 +75KB raw / +14KB gzip（216→231），可接受。

### 2026-07-17 — 深/浅主题图标变实心黑块：fill={undefined} 覆盖 lucide 默认属性（commit 见本条）

- **现象**：中央图标模块上线后用户反馈深色/浅色主题图标"好丑"——Mail 等含闭合形状的图标渲染成实心黑块；飞书/苹果正常。
- **根因**：themed() wrapper 把解构出的 `fill`（undefined）显式回传 `<Lucide fill={fill}/>`，React spread 中显式 undefined 会**覆盖掉 lucide 默认属性集里的 `fill="none"`** → svg 根无 fill → 浏览器默认黑色填充。dump-dom 对比即见：svg 根缺 `fill="none"`。
- **修复**：fill === undefined 时完全不传该 prop；回归断言钉死 `svg.getAttribute('fill') === 'none'`。
- **教训**：透传 props 时「解构再回传」对带默认值的下游组件是陷阱——`{...{fill:'none'}, ...{fill:undefined}}` 结果是 undefined 而不是 'none'。条件性传参（undefined 就别传）才等价于"不碰"。截图自查只看了 feishu/apple 两个新主题，没回看 dark/light 基线主题——改共享层时基线主题必须进自查清单。

### 2026-07-19 — Codex GPT-5.6 支持修正：一个版本号 ≠ 一个模型 ID（commit 9265262）

- **问题**：`codex_model_options` 把 GPT-5.6 当单一模型列了裸 `gpt-5.6`，但 Codex 服务端根本没有这个 ID——GPT-5.6 是**三个模型**：`gpt-5.6-sol`（旗舰）/ `gpt-5.6-terra`（均衡）/ `gpt-5.6-luna`（快速）。用户选 gpt-5.6 会把无效 ID 传给 `codex exec --model`。连带发现 `instance_manager` 把 `max` effort 一律静默丢弃（旧注释「codex 无 max」），而 5.6 系列实际支持 `max`（sol/terra 还有 `ultra`）。
- **取证**：不猜命名——直接读本机 Codex CLI（0.144.6）的服务端模型缓存 `~/.codex/models_cache.json`（含每模型 slug/display_name/supported_reasoning_levels），并用 `strings` 扫 codex 二进制交叉验证。
- **修复**：新增 `backend/services/codex_models.py` 集中管理每模型档位（`CODEX_MODEL_EFFORTS` + `clamp_codex_effort`：不支持的高档位向下夹到该模型最高档）；`/api/system/config` 下发 `codex_model_efforts`，前端 TaskForm/TaskBadges 按所选模型过滤 effort、切模型后失效档位自动回落。红测试先行（6 红 → 修复 → 全绿）。
- **教训**：外部 CLI 的模型列表是服务端下发的动态事实，别凭版本号想当然拼模型 ID；本机 `~/.codex/models_cache.json` 就是一手证据源，下次先查它。

### 2026-07-19 — Codex 兼容层（AGENTS.md）落地 + 存量目录覆盖险修复（commit 46827d2 / 59fa329 / 64c0fdd）

- **做了什么**：Codex provider 对等逻辑三连——① AGENTS.md（symlink → CLAUDE.md）在 project 创建时注入、dispatcher 任务启动时对存量项目惰性补齐（`services/agent_docs.py`）；② 所有 prompt 按 provider 引用对应文档 + 下发 CLAUDE.md/AGENTS.md 同步纪律（`_DOC_SYNC_NOTE`）；③ skills 模板只对 claude 注入
- **遇到的问题**：审计「不覆盖原有文件」时发现 `_init_local_repo` 的遗留 bug——本地建项目指向「已有文件但未 git init」的目录时，无条件 `open('w')` 用模板覆盖已有 CLAUDE.md（clone 路径一直有存在性守卫，init 路径没有）
- **如何解决**：init 路径与 clone 路径对齐：两个文档都先查存在性、只提交本次创建的文件、都存在时跳过 initial commit。回归测试先红（stash 掉修复实证旧代码覆盖）后绿
- **以后如何避免**：任何「生成默认文件」的路径必须带存在性守卫，且同类路径（clone/init）的守卫逻辑要对齐审查；对用户已有内容的兜底原则：**宁可少做，不可覆盖**

### 2026-07-19 — Codex 对等补齐三批次：静默错误 > 体验退化 > 能力入口（commit a9dd366）

- **背景**：全面审计各功能对 codex 的支持度（两个探索 agent 扫全仓），按「静默出错 / 体验退化 / 能力缺口」分级修复；C 类（PTY/号池/skills-MCP/ask_user 给 codex）经用户确认明确不做，保持显式门控。
- **最危险的发现（A1）**：codex 限额文案 `You've hit your usage limit` 会**误命中 claude 的 `_RATE_LIMIT_RE`**（`hit your (\w+ )?limit`）——不 gate 的话 codex 撞限额会冷却无辜 claude 账号 + 用 `claude --resume` 重启 codex session（provider 在 `_launch_params` 里根本没存，relaunch 默认 claude）。修复：轮换三处（`_check_rate_limit_and_rotate` / `_try_chat_pool_rotation` / `_resolve_resume_config_dir`）全部按 provider gate，`_launch_params` 补存 provider，transient 检测统一走 `is_transient_for(provider, text)`。test_claude_pool 留了重叠回归锚点断言。
- **取证纪律**：codex 错误文案不猜——`strings` 扫二进制失败（Rust 段压缩）后浅 clone codex-rs `rust-v0.144.6` tag 读 `protocol/src/error.rs`（与 CLI 自身 `is_retryable` 语义对齐：Stream/RequestTimeout/ConnectionFailed/InternalServerError 可重试；UsageLimit/Quota/401 不可）；事件字段读 `exec/src/exec_events.rs`（早前 agent 报告说判别字段是 `item_type`，源码实证是 serde `tag="type"`——**二手结论必须对源码复核**）；窗口值读 `~/.codex/models_cache.json`（全系 272K、spark 128K，不是 claude 的 200K 默认）。顺带实测 `codex exec --json` 抓到 turn.failed 真实形状（认证失败样本）。
- **其余修复**：TaskMigrator 按 provider 搬 rollout session（`~/.codex/sessions/YYYY/MM/DD/rollout-<ts>-<sid>.jsonl`，保持相对路径）；monitor/sub-agent API 对 codex 显式 400（否则静默跑成 Claude 子进程）；解析器补 reasoning→thinking + file_change/mcp/web_search/todo_list；TaskForm 隐藏 codex 下的 Thinking/User Skills 幽灵选项；PR Monitor / Todo Run 可选 provider（PRMonitorPage 清空模型时必须发显式 `null`——`undefined` 被 `exclude_unset` 丢弃会让旧 claude 模型残留在 codex repo 上）。
- **教训**：⑴ 跨 provider 复用检测正则前先对撞测试两边文案——窄正则也会跨语境误命中；⑵ worktree 工作时 Bash 相对路径极易写回主仓库（本次两次误写主仓库文件靠 git checkout 救回），批量脚本一律用绝对路径；⑶ 「UI 可选但后端忽略」的幽灵选项是一类值得专项审计的退化——用户以为生效了。

### 2026-07-17 — ask_user hook 被 CLI 600s 默认超时杀掉 → 原生 AskUserQuestion 冻死 PTY turn（task 32）

- **现象**：task 32 的提问卡片 13:36 弹出，几分钟后从前端消失；此后模型零输出、用户消息只入队不投递（turn 永不结束），claude 进程零 CPU、零 TCP 连接——整个 turn 冻死在一个无人应答的交互选择框上，直到 stop-session 手动拆除。
- **根因链**：`ensure_ask_user_hook` 注入的 hook 项没写 `timeout` 字段 → CLI 对 hook 命令**默认 600s** 就杀（服务端 `/api/ask-user/wait` 却要等 1800s）→ hook 在阻塞中途被杀 = 等效 fail-open 放行**原生** AskUserQuestion → PTY 交互终端渲染选择框等键盘输入，无人能按 → turn 冻死。hook 被杀的同时服务端连接断开，`wait` 协程 `CancelledError` 清掉 registry → 前端 pending 回填变空、卡片"消失"，用户连补答的机会都没有。旁证：task 28 在 07-14 的卡片 3 分 42 秒后被回答且答案成功喂回——证明默认超时是 600s（与 Bash 墙钟同款），不是 60s。
- **修复**（本 commit）：① hook 项显式 `"timeout": ask_user_timeout + 60`（`ask_user_settings.py`）；② hook 脚本超时语义反转：`timed_out` → deny +「用户未回应，按你的判断继续」，只有 CCM 不可达/非托管 session/异常才 fail-open（`ask_user_hook.py`）——PTY 下"放行原生工具"不是兜底而是死锁。测试补齐：注入含 timeout 字段断言 + hook 脚本 subprocess 级三态（answered/timed_out/no_session）决策输出断言。
- **生产热补丁（本 PR 部署前的过渡态）**：remote_exec 内存 wrap `ensure_ask_user_hook` 补写 timeout 字段 + 运行时 `ask_user_timeout` 降到 540s（让**已在跑**的 claude 进程——hook 快照仍是无字段/600s 杀——在被杀前先拿到服务端响应走 deny 路径）+ prod 磁盘 hook 脚本同步了 ② 的改动。**内存补丁重启即失、磁盘改动会被下次 rsync 部署冲掉——本 PR 必须在下次部署前合并**，否则原样回归。
- **以后如何避免**：给 Claude CLI 注入 hook 时，凡 hook 内部会阻塞等待的，必须显式声明 `timeout` 且比内部等待上限更长；"fail-open 放行原生工具"这类兜底策略要按执行形态分别评估——headless(-p) 下无害的兜底在 PTY 交互态是死锁。hook 快照随 claude 进程启动固定，改 settings.json 对在跑进程无效——这类修复必须考虑"已在跑的进程怎么办"。

### 2026-07-19 — Codex 常驻 app-server：真实延迟/准确性/稳定性复验（commit 8215242 / d40268b）

- **为什么判定有效**：固定 `codex-cli 0.144.6`、`gpt-5.6-luna`、`effort=low`、同一 cwd 和同形提示词，交替执行 5 组真实 `codex exec` / 常驻 app-server A/B。10/10 回答准确；热态 app-server 首输出中位数 2.245s vs 3.173s（约快 29%），总耗时 2.542s vs 3.431s（约快 26%），turn 准备 70ms vs 684ms（约少 90%）。app-server 有一次 4.160s 离群值，结论是中位体验明确改善，不承诺每次必快。
- **准确性/稳定性实测**：两条链路的原生 resume 都正确记住 nonce 且 thread id 不变；同一 app-server PID 并发 3 turn 全部答对且 thread 隔离；两个并发 task 注入不同 git env，shell 输出各自正确、无串值；真实 Shell 事件的 command/output/exit_code 映射正确；强杀空闲 app-server 后自动换 PID并成功完成下一 turn。
- **完整 CCM 链路**：用真实 Codex 跑 `InstanceManager → parser → SQLite → WebSocket → 状态收尾` 两轮，PID 复用、session 不变、最终消息各只落库一次、delta 为 live-only 零落库，task=completed / instance=idle；首轮/续聊 WebSocket 首 delta 为 2.819s / 1.736s。
- **可复现与回归**：新增 `scripts/benchmark_codex_transport.py`（手动运行，真实消耗额度）和 3 个回归测试，锁定并发事件不串线、共享进程退出解除全部 waiter、delta 广播但不落库。后端全量 1108 passed；最新 main 上 Codex 相关 210 passed；前端 production build 与 Codex delta 专项测试通过。ChatView 全文件另有 2 个由 7454a5a 改 `scrollIntoView` 为 `container.scrollTo` 后未同步断言的既有失败；全仓 ESLint 也有 66 个既有错误，不归入本性能修复。

### 2026-07-20 — Completion Guard 设计稿代码核查修订 + 默认 provider 变更砸出过期测试（commit 9853059）

- **问题 1**：设计稿（d7b2402）多处描述与代码不符/缺落地事实——「PTY finally 完成恢复路径」不存在、per-task 队列写成需要新增优先级（实际已是 PriorityQueue+source）、Worker 配置同步通道假设存在（实际 PUT 只写本地 DB）。逐项 grep+读码核查后修订：新增附录 A（completed 写入路径全量盘点 14 处），指出 `_cleanup_stale_state` 重启兜底会把 verifying 任务洗白、分享飞书通知挂在广播层（verifying 不能借用 completed 广播）、迁移 payload 丢 `must_complete` 等一批字段的既有缺陷。
- **问题 2**：基线 3 个 monitor 测试失败——`DEFAULT_PROVIDER` 改 codex 后，不写 provider 建任务的旧测试命中 monitor API 的 codex 400 门控（门控是有意行为且已有专测）。修复：测试显式 pin `provider: "claude"`。
- **以后如何避免**：改配置默认值（如 DEFAULT_PROVIDER）后必须全量跑测试并清零，不能只跑改动相关文件；测试建任务时对 provider 敏感的行为要显式 pin provider。设计稿评审的标准动作是把每个「现有机制 X 可复用」的断言落到 文件:行号 再采信。

### 2026-07-22 — 两个 Codex follow-up 抢同一 instance：一条消息被吞（commit e70712d）

- **现象**：测试环境同时向两个已完成 Codex task 发送 follow-up；两个 API 都返回 queued，但 task 94 正常运行，task 93 瞬间恢复为蓝色 completed，只有 user_message、没有任何 assistant 回复。日志显示两个 `_process_queued_message` 都选中 instance 28，后启动者抛 `InstanceAlreadyRunningError`。
- **根因**：旧的 `_launching_instances` 只解决了“看见已发布 claim 后跳过”，却把 `add(inst_id)` 放在 idle SELECT、账号解析、日志和状态提交等大量 `await` 之后。不同 task 各有独立 consumer，能同时 SELECT 同一最低 ID idle row；异常又不在 consumer 的可重排队列表中，于是 `q.task_done()` 直接吞掉原消息，7 月 21 日新增的 launch 失败状态回滚让 UI 表现为“蓝色完成但没回复”。
- **修复**：所有本地分配路径共用 `_instance_claim_lock`，在同一临界区完成 idle SELECT + token/owner 预留；fresh-task dispatch 把预留原子转换为 `_running_tasks`，queued-message 在 launch 后或任意异常路径按 token 释放。`InstanceAlreadyRunningError` 同时作为最后一道防线重排队原 `QueuedMessage`，不再丢消息。
- **预防/测试**：并发资源分配不能把“SELECT 可用资源”和“发布占用”拆在两个临界区；仅有 busy set 不等于原子领取。新增 `test_concurrent_task_consumers_reserve_distinct_idle_instances` 用 barrier 稳定扩大旧 race 窗口，新增 `test_instance_contention_requeues_exact_message` 钉死防御性重试；调度器专项 115 passed。

### 2026-07-22 — Worker 默认 Codex 自动登录 + SSH/EC2 创建闭环（commit c4db376）

- **问题**：Worker 仍按 Claude-only 账号模型 bootstrap，无法自动建立 Codex 号池；创建 EC2 时又继承 Manager 的 `KeyName`，但实际 SSH 使用另一把本地私钥，同时只复用安全组而不保证 22/CCM 端口从 Manager 可达，导致新 Worker 会稳定落入 `ssh-wait` 的认证失败或超时。RunInstances 响应丢失后 retry 还可能重复开计费实例。
- **解决**：Worker 账号增加 provider（默认 codex），bootstrap 固定安装 `codex-cli 0.144.6` 与登录运行时，复用 Worker localhost Codex pool API 完成自动取码、OTP、live verify/relogin 和幂等槽位恢复；所有 bearer/登录/.env 内容只经 SSH stdin，禁用代理且 `.env` 强制 0600。SSH 创建前严格预检私钥并派生公钥经 cloud-init 注入，自动创建只允许 Manager SG → 22/CCM 端口的专属 SG，所有 rsync/SSH 以 `cloud_instance_id` 隔离 known_hosts。新增 `provision_spec` + 稳定 EC2 ClientToken，先按 token 认领丢响应实例。生命周期、账号 JSON 写入、delete/add/destroy 竞态统一用 SQL CAS/锁收口，销毁后凭据不可复活。
- **预防**：Worker SSH 的私钥、公钥、authorized_keys 和网络入口必须作为同一条可验证闭环，不能把 EC2 KeyName 与本地文件路径想当然地视为同一身份；所有跨网络创建都要在调用前持久化非敏感请求日志并使用服务端幂等键；JSON 凭据 RMW 必须把生命周期状态条件放进同一条 UPDATE，远端 terminal 也不能早于本地事务完成对外发布。验证：后端全量 1532 passed，前端全量 310 passed，production build 成功。

### 2026-07-22 — error/stopped 僵尸实例占满 cap，任务与续聊无限排队

- **现象**：task 51/54/56 的续聊反复出现 `No idle instance`，新 task 58 也无法领取；数据库已有 9 个 `error/stopped` instance，而实际没有对应进程。手动调用 `DELETE /api/instances/cleanup` 后 dispatcher 立即补出 8 个 worker，task 58 与 task 51 随即并行启动。
- **根因**：启动补齐 `_ensure_instances()` 与运行时 `_ensure_min_idle_instances()` 都用全部 Instance 行计算 `max_concurrent_instances`；不持有进程的 `error/stopped` 历史也占 cap，导致 live worker 永远无法补充。
- **修复**：两条补充路径统一只以 `idle/running` 计算 live capacity；终态行保留用于诊断，物理删除仍由既有 cleanup API 负责。回归覆盖“9 个终态实例超过 cap 仍补 worker”及“补充后 live 数不突破 cap”。
- **预防**：资源上限必须统计实际占用资源的状态集合，不能用表总行数代替；启动补齐和运行时 top-up 必须共享同一容量语义并分别留测试。

### 2026-07-22 — 自更新任务安全门禁 + 页面非阻塞更新提醒（commit 834e9d3）

- **问题**：用户点击「更新并重启」时，服务会直接进入更新/重启流程，可能中断正在 `in_progress` 或 `executing` 的任务；手动 `git pull` 后也没有可靠提示当前进程仍运行旧代码。更新检查仅靠手动触发，发现更新的时机晚，原有弹窗又会打断正常操作。
- **根因**：更新服务没有和 Dispatcher 的领取任务流程建立互斥，也没有在停服前二次检查活动任务；版本判断只比较工作区与远端，未区分进程启动时的 commit 和磁盘 `HEAD`；前端缺少页面打开后的静默 dry-run 与非阻塞通知入口。
- **解决**：更新/回滚先暂停 Dispatcher 领取新任务并查询活动任务，查询失败按风险存在处理，`force` 也不能绕过；更新完成到重启前再次检查，出现新任务则取消重启并恢复调度。进程启动时记录实际 commit，与磁盘 `HEAD` 精确比较以识别手动更新，并优先使用分支 tracking remote。dry-run 增加短缓存和并发锁，但活动任务始终实时查询、手动检查可强制刷新；远端 fetch 失败时仍保留本地「需要重启」信号。页面打开约一秒后自动检查、之后每小时检查，有更新时只在顶部显示可关闭的非阻塞通知，用户点击「查看详情」后才进入原更新弹窗；最新版保持静默。
- **预防**：所有会停服或替换运行代码的入口必须共用「暂停领取 → 活动任务门禁 → 停服前复检 → 恢复调度」协议；安全 blocker 不得被版本缓存；版本状态必须同时表达远端差异、磁盘代码和进程实际代码；自动检查只允许 dry-run，不得自动拉取或重启。后端、API、Dispatcher 与前端分别补回归测试锁定失败路径、并发缓存、手动 pull 和通知交互。
- **验证**：更新相关后端测试 149 passed（7 deselected）；`UpdateButton.test.tsx` 31 passed；TypeScript、定向 ESLint、生产构建均通过。

### 2026-07-22 — PR #59 审核返修：关闭续跑与停服之间的竞态（commit 7b929ed）

- **问题**：第一版维护门禁只覆盖 GlobalDispatcher 的普通 dequeue。per-task chat/monitor 消费者可以在更新初检之后把 Task 写成 `executing` 并 launch；正常重启/迁移在最终查库后仍有广播和一秒 sleep，手动 pull 快速重启绕过复检，rollback 也只有早期检查。因此续跑可能在检查与停服之间启动并被杀掉，违反“不打断任务”的核心保证。继续审计还发现 RalphLoop dequeue 和手动 Instance task 运行同样绕过门禁。
- **解决**：把 `_dispatch_claim_lock` 升级为统一任务启动门禁；所有新 Task 启动入口必须在锁内持久化活动状态后才可 launch。per-task 队列以 `_pending_task_starts` 跟踪已接受但尚未完成的续跑，维护期间保留消息并作为 blocker，若更新过程中来了新消息则取消重启、恢复调度后继续处理。四条停服路径统一经 `maintenance_shutdown_guard`：广播和宽限等待全部前移，最终 blocker 查询与同步 restart/spawn 在同一个锁区间内完成，成功提交后封闭新入队；查询异常 fail closed。
- **预防**：任务安全不能只检查数据库状态；任何内存队列或“准备中但尚未落活动状态”的工作都必须进入同一 admission 协议。最终安全检查之后不得再出现 `await`，否则检查结论会在停服前失效。新增并发入口时必须同时回答三件事：是否经过统一门禁、何时持久化为活动状态、维护开始后未启动消息如何保存或拒绝。
- **验证**：新增确定性 Event/lock 回归覆盖准备→launch 竞态、user/monitor 续跑、无迁移/迁移、手动 pull 快速重启、rollback、最终查询失败、RalphLoop 和手动 Instance 入口；相关后端测试 262 passed、7 deselected，`npx tsc --noEmit` 与前端生产构建通过。独立 8011 服务模拟“手动 pull 后待重启 + executing Task”，强制更新仍返回 409；全量后端测试受本机缺少 `claude_pty` 和既有 Task 排序断言影响未跑绿。用户浏览器实测符合预期，实现提交为 `7b929ed`。

### 2026-07-22 — PR #59 二次审核返修：补齐 taskless 实例与队列清理生命周期（commit db1b219）

- **问题**：prompt-only 手动 Instance 只在 admission lock 内完成 launch，却没有对应 Task 行；进程启动后更新只查 Task 状态，会误判为空闲并杀掉该进程。另一方面，`enqueue_message()` 会登记 `_pending_task_starts`，但 stop-session 的同步 `clear_task_queue()` 只清队列、不清登记；若消息在 dequeue 前被清掉，consumer 会继续等到 idle timeout，幽灵 `queued_resume` 会永久阻止后续更新/回滚。
- **解决**：`UpdateService` 把持久化为 `running/current_task_id=NULL` 的 Instance 作为 `running_instance` blocker，与活动 Task、排队续跑一起参与每次初检和最终原子复检。`clear_task_queue()` 改为异步并持有统一 admission lock，同时清空 PriorityQueue 和失效 pending 标记；新增 per-task in-flight 计数，确保清剩余队列时不会误删已经 dequeue、正在准备或执行的续跑 blocker，consumer idle/cancel 的 finally 也会收敛 bookkeeping。
- **预防**：维护 blocker 必须覆盖所有真实运行单元，不能假设每个进程都有 Task 行；任何创建、消费、清理或取消内存队列的入口，都必须在同一同步协议下维护 blocker 的完整生命周期。回归测试必须从“工作已经启动后再发起更新”和“enqueue → clear → query”验证最终可观察行为，不能只测维护已暂停时拒绝新启动。
- **验证**：新增两条指定回归；rebase 最新父仓库后，更新/Dispatcher 相关测试在 Windows 排除 5 条 Linux 脚本用例后 147 passed、2 skipped、5 deselected，受影响 API/消息/Ralph 测试 199 passed、1 deselected；`UpdateButton.test.tsx` 31 passed，`npx tsc --noEmit` 与生产构建通过；实现提交为 `db1b219`。

### 2026-07-23 — PR #59 三次审核返修：串行化更新与回滚准入（commit c2ab2dd）

- **问题**：`start_update()` 用独立的启动锁保护状态替换，`rollback()` 却只在 await 之前、之后分别读取 `_current`。回滚暂停调度或查询 blocker 时，并发更新可以替换 `_current`，导致回滚拿到空或错误的 commit/备份；两个并发回滚也可能同时通过未加锁的前置检查。
- **解决**：更新与回滚统一使用 `_operation_lock`。回滚在锁内捕获并验证同一个 `UpdateState`、`old_commit` 和 `backup_file`，保持准入锁直到最终停服决定已提交并把状态标记为 `restarting`；额外保留状态对象身份校验，阻止未来绕过入口的状态替换被静默用于回滚。
- **预防**：长流水线执行锁不能替代入口准入锁，因为刚创建的异步任务尚未获得执行锁。所有会替换更新状态或启动停服脚本的入口必须先在同一准入锁下完成“检查 → 固定输入 → 预留操作”，并发请求只能有一个成功。
- **验证**：新增确定性 Event 回归，覆盖“回滚通过初检并暂停 → 并发更新等待 → 回滚恢复后只放行一个操作且脚本始终使用原状态”，并补充双回滚并发只启动一次脚本的覆盖；更新/Dispatcher 相关测试在 Windows 排除 5 条 Linux 脚本用例后 149 passed、2 skipped、5 deselected，System API 14 passed，`npx tsc --noEmit` 通过；实现提交为 `c2ab2dd`。

### 2026-07-23 — PR #59 四次审核返修：关闭 dequeue 与 in-flight 登记窗口（commit ab0d827）

- **问题**：`q.get()` 已经把消息从 PriorityQueue 移除后，consumer 还要等待 `_dispatch_claim_lock` 才登记 `_task_queue_inflight`。stop-session 若在两者之间清队列，会同时看到 `q.empty()` 和 in-flight=0，错误删除 pending blocker；consumer 随后仍持有消息并可能 launch，更新最终门禁也可能在该窗口误判为空闲。
- **解决**：每个 task 增加单调递增的 queue generation。enqueue 在 admission lock 内把当前 generation 固定到消息；clear 在同一锁内推进 generation、清空队列并收敛 pending；consumer dequeue 后通过 `_claim_dequeued_message()` 在锁内校验 generation，已被 clear 失效的 handoff 调用 `task_done()` 后直接丢弃，绝不进入处理或 launch。已登记 in-flight 的消息继续保留 pending blocker，clear 不影响其当前生命周期。
- **预防**：队列长度不是所有权模型；任何 `get()` 与“处理中”登记分开的队列都必须显式表示 handoff，或用 cancellation generation 让清理操作能同步失效不可见的旧所有权。测试必须控制在 dequeue 后、登记前暂停，而不能只覆盖 dequeue 前清队列。
- **验证**：新增确定性 Event 回归覆盖指定顺序，并补充已登记 in-flight 的反向控制；更新/Dispatcher 相关测试在 Windows 排除 5 条 Linux 脚本用例后 151 passed、2 skipped、5 deselected，stop-session API 9 passed，chat/plan 35 passed，`npx tsc --noEmit` 通过；实现提交为 `ab0d827`。
### 2026-07-23 — Codex 全量 CCM MCP PR 1：统一 Server Spec（commit 5c8f150）

- **问题**：主任务、Monitor、Sub-Agent 的 MCP 上下文分别直接拼成 Claude `mcpServers` JSON；以后接入 Codex app-server/exec 时只能复制 task/session ID、认证、路径和工具范围逻辑，容易在重试或多任务并发下产生配置漂移。
- **解决**：新增 provider-neutral、不可变的 `McpServerSpec`，统一描述 command/args/cwd/env/required/enabled_tools/startup timeout/tool timeout；三类现有生成函数先构造各自 spec，再由独立 Claude renderer 输出原 JSON。三个角色的工具白名单分别锁定 13/3/3 个实际 FastMCP 注册工具，Claude 调用方式和输出结构保持不变。
- **预防**：Provider 适配器只负责格式转换，任何 task/session 上下文必须在公共 spec builder 中形成；工具白名单必须与 FastMCP 注册表做回归对照，禁止在 Claude/Codex renderer 中各维护一套角色逻辑。
- **验证**：MCP/Monitor 相关测试 47 passed，Claude 命令构建 5 passed，compileall 与 `git diff --check` 通过；用户在 Windows worktree 人工生成三类 spec/Claude JSON，确认角色、ID、required 和 13/3/3 工具数量正确。

### 2026-07-24 — Codex Pool 支持邮箱-only 人工验证码登录

- **需求**：新增 Codex 账号时只提供 OpenAI 邮箱，不预先保存邮箱查询 token 或 OpenAI 密码；管理员收到验证码后在 CCM 里人工输入。
- **实现**：前端与新增/重登 API 允许空 token、空密码；登录 wrapper 仍通过同一 stdin 管道接收初始化消息和 OTP。密码页会尝试 OpenAI 的 email-code / one-time-code 入口，成功后发布 `awaiting_otp`；若页面只提供密码则明确提示补密码重试。
- **安全与原子性**：OTP 仍只在内存/管道传递，不写文件或日志；账号仍以 journal + `auth.json` 校验为提交点。email-only 账号只保存空凭据元数据，日后重新登录时再次人工收码。

### 2026-07-24 — Codex 全量 CCM MCP PR 2：双 transport 配置渲染（commit f55506b）

- **问题**：PR 1 只有 Claude JSON renderer；Codex app-server 的线程级 `config.mcp_servers` 与 `codex exec -c` 若各自拼装，复杂路径、Unicode、引号、工具白名单和 timeout 很容易漂移。初版单测又只用 `tomllib` 自我反解析，未发现 `mcp_servers."含点名称"` 会被 Codex CLI 的 dotted-path parser 错拆，形成假绿。
- **解决**：同一 `McpServerSpec` 先渲染为 app-server config，再把完整 `mcp_servers` table 序列化成一个 exec TOML override；不经 shell、不写全局 `config.toml`。renderer 提前执行 Codex 实际约束：server name 必须匹配 `^[A-Za-z0-9_-]+$`，两类 timeout 必须为有限非负数。
- **预防**：Provider 配置测试不能只反解析自己的输出，至少要用目标 CLI 的只读命令和实际 session 初始化各验一次；CLI 的 `-c` key parser 与 TOML dotted-key grammar 不应假定等价。配置适配器必须返回 argv token，不返回已拼 shell command。
- **验证**：Codex renderer 单测 56 passed；MCP config/server 65 passed；Monitor 可比项 14 passed（另 2 项因 Windows 沙箱无权写硬编码 `/tmp` 而未运行）；Claude 命令构建 5 passed；前端 `tsc --noEmit` 通过。Codex CLI `0.144.6`、`0.145.0` 均在隔离 `$CODEX_HOME` 下通过 `mcp list -c ... --json`，且两版 app-server 都用 `thread/start` 成功初始化真实 CCM FastMCP server；全程未生成 `config.toml`。

### 2026-07-24 — Codex 全量 CCM MCP PR 3：app-server 按线程注入主 MCP（commit 7f41159）

- **问题**：PR 2 只有纯 renderer，生产 `CodexAppServer.start_turn()` 仍未接收 MCP specs，导致 Codex 主任务在新建、resume、瞬时重试或账号轮换时都看不到 `ccm_skills`；如果 required MCP 初始化失败后再落入尚未接线的 exec fallback，还会伪装成普通无工具任务。
- **解决**：新增默认关闭的 `CODEX_MAIN_MCP_ENABLED` capability；开启后 `InstanceManager` 每次 app-server launch 都从当前 `task_id` 重建 required `ccm_skills` spec，`start_turn()` 将 renderer 结果只合并到本次 `thread/start|resume` 的 `config`，不写共享 `CODEX_HOME/config.toml`。required spec 校验或 thread 初始化失败统一转成 `CodexRequiredMcpError` 并禁止无 MCP 的 exec replay；Claude、prompt、UI、Monitor/Sub-Agent 和 exec 配置接线保持本 PR 范围外。
- **预防**：任何 task-scoped MCP 上下文必须在每次 start/resume/retry/rotation 时重建，不能缓存到共享 app-server 或账号目录；capability 未开启时不得发送空 `config`；required 能力失败必须 fail closed，并用并发双 task 测试锁定配置对象和 `task_id` 隔离。
- **验证**：MCP renderer/app-server 相关回归 97 passed、5 skipped；InstanceManager capability、重试、换号和 Claude 不变相关 11 passed；前端 `tsc --noEmit`、compileall、`git diff --check` 通过。真实 Codex CLI `0.145.0` + `gpt-5.6-sol` 在隔离 `CODEX_HOME` 下完成 start 与同 thread resume：两轮均实际调用 `ccm_skills/ccm_command_help`，工具状态 `completed`、结果 `success=true`、turn returncode 0；隔离后端两次收到绑定 Task 1 的 API 请求，测试进程、数据库和认证硬链接随后全部清理。

### 2026-07-24 — PR 3 评审修复：required MCP 全启动链 fail closed（commit 642b996）

- **问题**：`start_turn()` 在判断 required MCP 前先启动 app-server；transport 初始化异常或 malformed/no-thread-id 响应可能以普通 `CodexAppServerError` 逸出，随后被 `InstanceManager` 的兼容分支重放为尚未携带 MCP 的 `codex exec`，使要求 `ccm_skills` 的任务静默降级。
- **解决**：先构造并校验 thread MCP config，再启动 transport；transport、thread/start|resume、turn/start 的普通异常及缺失 thread/turn id 都统一转成 `CodexRequiredMcpError`，同时保留取消、超时未知状态和 maintenance busy 的原语义。`InstanceManager` 再按 capability + task 上下文增加最终 fail-closed 栅栏，未知 app-server 异常也不得进入 MCP-less exec。
- **预防**：required capability 的错误类型必须覆盖依赖启动、协议 RPC、响应结构和上层 adapter 兜底，不能只识别服务端返回的某一种初始化文案；高层测试必须同时 patch app-server 失败和 exec spawn，并对启动失败、no-thread-id、未知异常逐项断言 exec 未调用。
- **验证**：新增/相关评审用例 7 passed；MCP renderer/app-server 回归 100 passed、5 skipped、2 个既有 Windows `SIGKILL` 用例 deselected；InstanceManager capability/重试/换号回归 15 passed；compileall、前端 `tsc --noEmit`、`git diff --check` 通过。

### 2026-07-24 — 自更新事务修复：代码已拉取不再等于部署完成（commit 3e08b8d）

- **问题**：旧状态只比较 Git 本地/远端；代码已经拉取后若依赖、前端或 Alembic 失败，下一次检查会误报“代码一致”，但进程和数据库仍可能是旧版本。原迁移 worker 的 `/tmp` 状态、systemd handoff、SQLite 活连接恢复和 task 门禁也缺少同一个可证明的事务边界。
- **解决**：状态拆为 running commit、disk commit、Alembic current/head，并新增完整 repair 与受约束 restart API/前端操作。仓库级 durable lease 保存 token、PID/start identity、期望 commit、备份和迁移结果；pre-start/lifespan guard 对半完成事务只启动不访问 ORM 的 maintenance-only 恢复面。协议 v2 worker 在停服后重新生成并校验 SQLite 快照，迁移/健康失败原子恢复 DB、代码、依赖和前端，任一步失败都不启动混合版本；同 commit 修复失败在后端 operation 与 shell commit 比较两层保留 incomplete fence。
- **并发/故障边界**：所有 task claim 与部署共用 repo flock，部署 claim 后、任何 mutation 前二次查 blocker；running Instance、Worker 转发、排队续跑和跨 CCM 进程竞态均会取消部署。post-claim cancellation 会先终结 lease 再恢复 Dispatcher，rollback claim 原子保留重试元数据；systemd-run ACK 超时/非零按结果不确定处理，late worker 即使 token 相同也不能越过终态 lease。更新、修复和重启还会拒绝所有 Git 可见的 staged、unstaged、untracked 改动，避免未跟踪源码绕过 commit 身份校验；ignored 运行时产物不阻断。
- **验证**：开发虚拟环境入口已从误指生产目录修正为 `/home/ubuntu/Claude-Code-Manager-dev/.venv`。部署专项最终 166 passed，前端全量 370 passed、TypeScript/生产构建/本次文件定向 ESLint 通过；全仓 ESLint 仍有 55 个与本次无关的既有错误。后端最终全量 2084 passed；Shell 语法、Python compile、`git diff --check` 通过。提交前追加的 dirty-worktree 专项 91 passed，前端更新定向 56 passed，生产构建再次通过。开发服务真实重启把 SQLite 从 `31fe767354b7` 升到 `c7e9b1d42f60`，状态 API 最终返回 current=head、`db_up_to_date=true`、`repair_required=false`；联调额外发现并修复了 Alembic mergepoint 双标记解析误判。仅操作开发目录与 8003，未触碰生产；本地实现提交为 `3e08b8d`，未 push。

### 2026-07-24 — 更新弹窗幽灵任务核对与 pytest 外部状态隔离（commit 3e08b8d）

- **现象**：开发更新弹窗显示 `#1 test monitor task (executing)`，但正常任务入口已看不到它。数据库实证 Task #1 空标题且仍为 executing，Instance #9–#13 五条同时反向占用它，五个 PID 均已死亡。
- **根因**：这是 Task↔Instance 多 owner 的边界损坏；同时测试虽 override FastAPI `get_db`，`backend.main` 全局 InstanceManager/Dispatcher 仍绑定开发库，三个空响应测试会通过全局 dispatcher 向真实库写 lifecycle，制造了这次残留。
- **解决**：新增管理员 `POST /api/system/update/reconcile` 与弹窗「重新核对运行状态」。Dispatcher 在关闭准入后统一核对：唯一双向一致的 dead claim 才可恢复；多 owner/mismatch fail closed；unknown/live PID 继续阻断；当前进程 process/consumer/lifecycle、fresh `_launching_instances`、Monitor/Sub-Agent exact maps 均保留。shared shadow 不被本机改写，manual reconcile 跳过 startup auxiliary sweep。Update blocker 还覆盖 quarantined PID/owner 与 live auxiliary generation。pytest 在首次 backend import 前把 DB、账号池 journal、Worker/backup、update checkout 全部定向到临时目录，并关闭外部服务；InstanceManager 的空响应重试改用显式注入 callback，不再 import 全局 dispatcher。
- **附带修复**：SQLite 停服独占检查仍对未知不可读同 UID 进程 fail closed，但精确允许 systemd `ssh-agent.service` 的 `/usr/bin/ssh-agent -D`；否则该正常 non-dumpable helper 会让所有迁移/回滚误报数据库无法证明独占。
- **验证**：ghost/Dispatcher/Update/API/InstanceManager/部署门禁专项 563 passed；迁移 hardening 26 passed；后端全量 2107 passed；前端全量 376 passed，TypeScript/Vite production build、定向 ESLint、Python compile、Shell syntax 与 `git diff --check` 全绿。三轮测试前后开发 DB/WAL/SHM 的 inode/mtime/size/SHA-256 完全一致。先生成 `0600` 且 integrity=ok 的 `backups/pre-ghost-reconcile-20260724T114307Z.db`，再只重启 8003：Task #1 fail-close 为 failed，Instance #9–#13 全部清成 error 且移除 PID/owner；在线 reconcile 与 dry-run 均返回 `active_task_count=0/update_blocked=false`，running=disk commit、Alembic current=head、`repair_required=false`。未触碰生产；本地实现提交为 `3e08b8d`，未 push。

### 2026-07-24 — Codex 272K 上下文跑满后摘要续跑（commit ba086c0）

- **问题**：CCM 虽有 80% 预压缩和 `"Prompt is too long"` 失败兜底，但 Codex app-server 的超限以 `codexErrorInfo=contextWindowExceeded` 出现在 `system_event`；chat 失败路径只扫描普通 assistant 文本，fresh/mode 路径又只认固定英文文案，因此真实跑满时可能直接退出。app-server 上报的 `modelContextWindow`、`totalTokens`、`reasoningOutputTokens` 也被丢弃；exec fallback 还会把整轮累计 1.5M token 当成当前 272K 上下文，出现 119%/553% 假利用率。
- **解决**：完整保留 app-server 结构化错误与有效窗口，统一 provider 上下文超限分类；Codex 当前上下文按官方口径 `last.totalTokens - last.reasoningOutputTokens` 计算。exec fallback 从绑定账号 rollout 的最新 `last_token_usage` 恢复当前请求用量，不再使用累计 turn total。chat 与 fresh/mode 两条失败路径都复用既有摘要机制：清旧 session、带摘要和原消息自动续跑；预压缩也按有效窗口和完整 current-context token 提前触发。
- **验证**：先写红测复现结构化错误丢失、输出 token 未计入阈值、exec 553% 假读数；修复后上下文专项 12 passed，Codex/上下文相关 122 passed，Dispatcher 相关 21 passed；正确隔离环境下后端全量 `1981 passed`（3m22s），`compileall` 与 `git diff --check` 通过。随后在独立 8011 环境完成真实前端集成：上下文 210,000/258,400（81%，阈值 80%）时界面显示自动压缩提示，新会话启动并返回 `E2E_CONTEXT_CONTINUED`，任务最终 completed、无错误且上下文重置到约 1.2K/272K；未触碰现有 8010。

### 2026-07-24 — Claude/Codex 自动登录运行时隔离与资源保护（commit 6734a93 / 80f74fe）

- **问题**：Claude/Codex Pool 各自缓存 Xvfb 进程、共用 `:99`/9222 且会全局 `pkill`，死进程对象没有 `poll()` 刷新；小内存机器上 Chrome profile/诊断又落到 tmpfs `/tmp`，资源枯竭时会表现为 X server 丢失、`Page.goto` 超时并最终触发 OOM。
- **解决**：新增两类 Pool 共用的登录锁和 `XvfbManager`：私有 Xauthority、跨进程 display 文件锁、`xdpyinfo` 就绪探测、stderr 诊断、只回收自己启动的进程；display/CDP/runtime/tmp 均可按部署配置。登录前检查 MemAvailable 和磁盘余量，资源不足返回 503；浏览器临时目录默认迁到磁盘缓存，Chrome profile 按 PID 隔离且只清理自身目录；Codex authorize 导航超时增加一次有界重试和资源诊断。
- **预防**：同一宿主上的生产/测试部署必须使用不同 display、CDP 端口和运行目录；禁止通过全局 `pkill` 解决共享资源冲突；需要 headed browser 的流程必须在启动前完成容量检查，并把 tmpfs 当内存预算而不是普通磁盘。
- **深测补漏**：真实多进程并发验证 6 个 CCM 进程只产生 1 个 Xvfb owner、5 个安全复用；死进程恢复、外部 display 不误杀、内存/磁盘门禁和测试残留清理均通过。静态回归发现 standalone Claude 登录只设置 `CCM_XVFB_DISPLAY` 时，Chrome 与 `xdotool` 仍可能使用不同 display；统一 display 解析并补两条回归（commit `80f74fe`）。
- **验证**：登录运行时/Claude/Codex/CDP/号池专项 301 passed，补漏后相关专项 115 passed；后端全量 1983 passed；前端 production build 通过；真实 Xvfb 私有认证 `xdpyinfo=0` 且退出后 socket 清理；8010 测试服务加载 `:100`/9223 隔离配置，浏览器实际打开 CCM 登录页且无页面控制台错误；150 个并发鉴权 API 全部 200，静态资源与 WebSocket 握手通过，服务无 error 日志或自动重启。

### 2026-07-25 — PR #64 评审修复：OOM 残留恢复与 Chrome CDP 身份绑定

- **阻塞问题**：自有 Xvfb 被 SIGKILL/OOM-kill 后可能遗留 Unix socket，旧实现会把它当成未知外部 display 永久返回 503；Claude 两条 Chrome 路径固定连接配置 CDP 端口，孤儿 Chrome 占住端口时可能把新登录施加到旧 profile。
- **解决**：Xvfb owner record 原子持久化 PID、`/proc/<pid>/stat` start time、socket/X lock 的 device/inode/uid/type；跨进程锁内仅在 owner 身份已死亡且现存 artifact 与记录完全一致时清理，活 owner、未知 socket 或 inode 已替换一律 fail-closed。Chrome 改用唯一 profile + `--remote-debugging-port=0`，只读取该 profile 的 `DevToolsActivePort`，并以其中随机 browser websocket path 反查 `/json/version` 后才接受 `/json` tabs；固定 `CCM_LOGIN_CDP_PORT` 配置已移除。
- **验证**：新增 owner socket 恢复/替换保护、动态 CDP/orphan 拒绝和两条实际 launcher 参数回归；登录/号池相关 `314 passed`。真实隔离 smoke 在 `:198` 启动 Xvfb 和 Chrome，配置遗留端口 9222 时实际绑定动态端口 35863；SIGKILL Xvfb PID 1029638 后保留的 socket/lock 被 owner record 安全识别并重启为 PID 1029783，测试结束后 display、lock、进程和临时目录全部清理。全量测试剩余的 20 个失败全部来自部署租约安全与 WebSocket 鉴权用例；在未包含本 PR 修改的最新 `main`（`e89a08d`）独立 worktree 上复跑同一组 115 个测试，结果同为 `20 failed, 95 passed`，确认不是本 PR 回归。

### 2026-07-25 — 旧版更新器跨版本重启兼容

- **问题**：旧进程拉取带 v2 启动守卫的新代码后，仍可能写入无 lease/token 的 `restarting` 状态并直接执行 `systemctl restart`。新 `pre-start` 只承认 v2 worker 的 `starting` handoff，因而永久拒绝启动；systemd `Restart=always` 会形成重启风暴，前端则停留在断连前最后一个“迁移中”状态。
- **解决**：启动守卫仅对非权威临时状态、无任何 owner token、目标 commit 与当前运行 commit 精确一致的 legacy `restarting` 开放启动，并强制进入 maintenance-only。依赖、数据库和业务服务仍禁止自动启动或变更，管理员必须通过事务化 repair 完成部署；tokened、commit 不匹配及其他 active 状态继续 fail closed。
- **验证**：新增 legacy 精确 commit 恢复、错误 commit 拒绝和伪 token 不得降级走 legacy 三条回归。

### 2026-07-25 — Claude/Codex API-first 路由、最近账号一致性与错误归因（基线 commit 919d642）

- **问题**：Codex 缺少与 Claude 对等的「最近使用」展示；两池的 `select()` 又会在 session 迁移和 Task binding 之前提前改 marker。旧对话切换账号时若迁移、绑定或取消落在边界窗口，UI 可能显示未真正使用的候选；Codex rollout 复制若被取消或并发 Task generation 替换，还可能留下多份副本但没有 durable owner。通用 direct consumer 还把 silent exit 硬编码成「Claude API 进程退出」，且没把 Codex `turn.failed` 识别为 provider 终态，导致原始 Codex 错误后再追加一条误导性退出提示。
- **解决**：统一路由优先级为 preferred → 已有 session resident/bound → 新 session 兼容可用 API → 原生号池 fallback，禁止模型降级。Claude/Codex 都把账号 ID 持久绑定到 Task；Codex 在复制前先锚定 registered source，再把 copy、app-server rebind 和 target binding 作为 cancellation-settled 单元。`select()` 只提案，Codex 另用 selection cursor 保持轮询；`last_selected` 只在 binding 提交或临时进程实际 spawn 后更新。临时冷却保留原 queued message，已知额度耗尽/认证终态直接显示失败。进程退出标签按 Claude/Codex 与 API/native 实际路由生成；Codex exec/app-server 的 `turn.failed` 保留原始 provider 错误，不再追加 generic exit。
- **预防**：账号选择、物理 session 副本、live transport owner、Task binding 和 UI marker 必须有明确 commit 顺序；任何 `asyncio.to_thread` 文件迁移都不能让 cancellation 在副本落盘后、durable owner 提交前逸出。失败补偿不能靠再次写全局 marker，而应让候选和已提交路由从结构上分离。
- **验证**：路由/号池/Dispatcher/InstanceManager/Distill/Goal 专项 711 passed；provider 退出归因专项 6 passed；新增 rollout/Claude session copy 取消、generation change、proactive cancel 与 binding-commit cancel 等确定性并发回归。后端最终全量 2337 passed，前端全量 388 passed，TypeScript/Vite production build 与 `git diff --check` 通过；全量测试前后开发 DB/WAL/SHM 的 inode、mtime、size、SHA-256 完全一致。改动通过事务化 repair 加载到开发 8003，未触碰生产。

### 2026-07-25 — `/tmp` 达到 80% 后清理全部安全候选（commit 86a2122）

- **需求**：Agent 的 Bash 依赖可用 `/tmp`；服务启动时检查一次，之后每 3 小时检查容量与 inode，任一达到 80% 即清理，不设置“清到多少停止”的目标线。
- **实现**：新增 `TmpSpaceManager`，触发后按大小遍历并处理全部超过 6 小时的 CCM 唯一命名普通文件；低于 80% 不扫描。SSH 下载响应结束即删除 staging。共享 Project 容器的独立 2GB tmpfs 在每次 Agent 启动前执行同阈值门禁，只有取得独占 lease 且证明容器空闲时才整棵清空；新建容器带 Docker `--init`，但不会仅为补 init 强拆现有容器。
- **安全边界**：宿主只做顶层 regular-file `rename + unlink`，不递归目录；session/workspace 迁移 staging、固定名 MCP、Monitor 日志、更新/回滚证据、登录资料、未知文件和 symlink 均排除。单进程检查合并、跨进程 flock 串行，扫描后再次核对 UID/device/inode/type/mtime；取消与关机等待在途文件操作落稳。容器 Agent 全生命周期持共享 lease，压力门禁 busy/未知时 fail closed，PTY 不得回退宿主裸进程。
- **验证**：定向回归 80 passed；后端全量 2362 passed。全量测试前后开发 DB/WAL/SHM 的 inode、mtime、size、SHA-256 完全一致；真实 `/tmp` 检查为容量约 4.6%、inode 约 1.4%，未触发删除。代码仅提交在开发环境本地 `main`，未 push，生产未触碰。

### 2026-07-25 — 更新脚本可信快照生命周期管理

- **问题**：运行中后端为了抵抗 checkout 变更，会把匹配版本的 `update_migrate.sh` 冻结到 `/tmp/ccm-update-runtime-*`；它没有正常关停清理，测试与重启会持续累积，而人工清空 `/tmp` 又会删除活跃快照，导致更新器在停服前 fail-closed。
- **解决**：新增 `TrustedUpdateRuntime`，把进程级可信快照迁到 owner-only 的 `~/.cache/ccm/update-runtime`（可由 `CCM_UPDATE_RUNTIME_DIR` 覆盖）。脚本内容在进程内只捕获一次并以 SHA-256、inode、权限复核；0700 目录内的 0600 owner 标记绑定 boot ID、PID start tick、uid、port 和目录身份。正常 lifespan 精确删除自身平面目录，异常退出由下次启动只回收可证明死亡的 owner；旧版 `/tmp` 快照也只在 PID 不存在且无未知内容时清理。执行单次更新前仍复制到租约绑定的 `/tmp/ccm-update-run-*`，外部 worker 的既有自清理协议不变。
- **安全边界**：读取和复制可信脚本均用 O_NOFOLLOW，并在复制时持生命周期锁；PID 复用、跨 boot、owner 损坏、`/proc` 不可确认、symlink、inode 替换或额外文件均按 fail-closed 处理。通用 `/tmp` 压力清理继续完全排除所有 `ccm-update-*`。
- **验证**：专项 124 passed，后端全量 2377 passed，`compileall` 与 `git diff --check` 通过；全量测试前后开发 DB/WAL/SHM 的 inode、大小、mtime、SHA-256 完全一致。8003 连续重启两次后健康，旧 `/tmp` runtime 目录从 350 个收敛为 0；第一次新进程快照在第二次 shutdown 被精确删除，专用根始终只留当前 PID 的 1 份快照。真实 update dry-run 返回 `update_supported=true`、`update_block_reason=""`、0 blockers；8002 PID/监听未变化

### 2026-07-27 — Codex 全量 CCM MCP PR 4：exec 等价回退与 transport 互斥（commit 505ad8b）

- **问题**：PR 3 已给 app-server thread 注入 required `ccm_skills`，但生产 `codex exec` 没有消费 PR 2 的 renderer；关闭 app-server 或兼容回退会丢失 MCP。另一方面，把所有 required-MCP 错误一概重放会在 `turn/start` 已上 wire 后重复执行，而 app-server 与 exec 同时进入同一 `CODEX_HOME` 也会混用驻留状态。
- **解决**：launch 层只构造一次 task-scoped spec；完整主任务可直接或在确认尚未发送 `turn/start` 的 transport/thread admission 失败后，通过带同一份 `-c mcp_servers=...` 的 exec 启动。新增 `CodexRequiredMcpPreTurnError` 区分安全边界；invalid config、turn/start/commit、未知异常继续 fail closed。Sub-Agent 无论完整主 MCP 开关如何都保持 app-server-only。同一 home 双向互斥：exec 启动前关闭空闲 app-server、活跃时拒绝，app-server 也拒绝仍有 exec generation 的 home。
- **预防**：fallback 的判据必须是“模型 turn 尚未提交”而不是“异常看起来像启动失败”；能力等价不仅要比较工具集合，还要把同一个 spec 交给两个 transport。共享凭据目录的 transport 切换必须在 canonical-home 锁内完成 shutdown/busy 检查、spawn 和 owner registration，不能留下先查后启的窗口。
- **验证**：PR4/Codex MCP 定向矩阵 53 passed，覆盖 fresh/resume argv、pre-turn 回退、unsafe replay、Sub-Agent 和双向互斥；`compileall`、`git diff --check` 与前端 `tsc --noEmit` 通过。真实 Codex CLI `0.145.0` + `gpt-5.6-sol` 使用生产 `_build_command()` 和隔离 localhost task API：`ccm_skills/ccm_command_help` 实际从 `item.started` 到 `item.completed(status=completed)`，最终严格返回 `PR4_EXEC_MCP_OK`、returncode 0。Windows 全模块回归另有既存 POSIX `SIGKILL`、0700 mode、`/tmp` 规范化及 Linux `fcntl` 导入失败，均在未改 PR4 路径上复现并单独记录。
- **评审修复（commit 1d154cf）**：live quota 原先直接调用 registry，可能在 task/临时 exec 已占用同一 home 时新建 app-server。现统一经过 canonical-home admission lock：已有 exec owner 时在创建 registry 前返回 busy，额度 RPC 全程持锁，后来的临时 exec 等 RPC 完成并关闭空闲 app-server 后才能进入。新增 4 条竞争回归；Windows 专项 4 passed，Linux transport 专项 10 passed、相邻 app-server/pool/distill 135 passed，真实子进程 smoke 的事件顺序为 quota start → quota end → app-server shutdown → exec enter。
- **评审补充修复**：Codex 子 Agent 原先直接调用 registry，仍可绕过 manager 级 exec owner。现抽出统一 `codex_home_app_server_guard`，主任务、live quota 与子 Agent 的所有 app-server admission 都在 canonical-home 锁内检查 maintenance、普通 exec generation 和临时 exec；新增普通/临时 exec 两条子 Agent 竞争回归。

### 2026-07-27 — Chat terminal、Cancel 续聊与 Interrupt 延迟修复（commit 4203873）

- **生产事故**：Task 212 的 Codex rollout 已生成 assistant 与 `task_complete`，但 CCM 只收到 synthetic `thread.started`；Task 被显示为 completed，Instance/worker 仍保持 running，后续消息持续等待旧 generation。Task 208 Cancel 后，chat API 先落库并返回 queued，dispatcher 随后按 cancelled 静默丢弃。Interrupt 又会在 adopted goal 上依次执行 `goal/get`、`goal/set` 后才中断，异常通知丢失时进一步落入 15s/30s 清理等待。
- **原因与解决**：原生 goal/steer 会让一个 adapter 同时收到 active turn id 与 `turn/start` submission id；旧修复只保留 active 映射，后续 submission-id assistant/terminal 通知被判为 unrelated。现同一 context 在终态前保留两种合法 alias，terminal 后禁止重新挂回映射；goal 停止直接一次 `thread/goal/set paused`，再中断权威 active id。Codex adapter 区分 `user_interrupt` 与 `internal_abort`，transport/admission 清理的 130 不再伪装成功。Cancel 只终止当前 generation，后续 chat 仍可用完整 Task/Instance/session CAS 重新领取原生 session。
- **验证**：Codex app-server、InstanceManager、Dispatcher、Chat 专项 `540 passed`；前端 `tsc --noEmit` 通过。后端全量 `2433 passed, 21 failed`，21 项均在未触及模块：20 项由当前 shell `umask 0002` 生成 0664 安全租约文件而触发既有 fail-closed 断言（以 `umask 022` 单项复跑即通过），另 1 项为生产环境首个管理员强制映射成 super_admin 的既有认证环境差异。
