# Claude Code Manager - 项目指南

> **重要：Claude 必须自主维护本文件。** 当项目架构、约定、关键路径发生变化时，只做必要的修改，保持简洁。不要大段重写，只更新变化的部分。

## 概述

Web 端调度管理多个 Claude Code 实例并行工作。Backend (FastAPI) + Frontend (React/Vite) + SQLite/PostgreSQL/MySQL。

GitHub: https://github.com/zjw49246/Claude-Code-Manager.git

## 技术栈

- **后端**: Python 3.11+, FastAPI, SQLAlchemy async, SQLite/PostgreSQL/MySQL
- **前端**: React 19, Vite, Tailwind CSS v4, TypeScript, Lucide icons（默认）+ 主题图标集（IconPark / Ionicons，见「主题图标集」）
- **实时通信**: WebSocket (原生, channel-based pub/sub)
- **语音**: OpenAI Whisper API
- **隧道**: Cloudflare Tunnel / ngrok

## 项目结构

```
claude-manager/
├── backend/
│   ├── main.py                  # FastAPI 入口, 全局单例, 静态文件服务
│   ├── config.py                # Pydantic BaseSettings (.env)
│   ├── database.py              # SQLAlchemy async engine + session
│   ├── api/                     # 路由
│   │   ├── tasks.py             # 任务 CRUD + plan 审批 + conflict 解决
│   │   ├── plans.py             # 关联 Plan 历史/stale/revision/执行 Task
│   │   ├── plan_resources.py    # 一等 Plan/Version/Run/Input/Application API
│   │   ├── chat.py              # 多轮对话 (基于 task, --resume)
│   │   ├── task_artifacts.py    # Task 工作区内安全文件下载 + Worker 分流
│   │   ├── instances.py         # 实例 CRUD + Ralph Loop 控制 + Dispatcher 端点
│   │   ├── projects.py          # Project CRUD + git clone
│   │   ├── project_todos.py     # 项目 Todo 清单 CRUD (prompt 模板 → 一键建 task)
│   │   ├── monitor.py           # Monitor Session CRUD + 子 agent checks/complete endpoints
│   │   ├── pool.py              # Claude 账号池 status/usage/reload/clear-cooldown
│   │   ├── codex_pool.py        # Codex 账号登录/号池/额度/维护 API
│   │   ├── cloudrouter_accounts.py # API 网关账号（CloudRouter/Apex）模型/额度管理 API
│   │   ├── pr_monitor.py       # PR Monitor CRUD + GitHub webhook endpoint
│   │   ├── browser_reviews.py  # 管理员浏览器审查任务/进度/产物 API
│   │   ├── workspace_reviews.py # Task 当前工作区 Preview/黑盒审查 API
│   │   ├── test_harness.py     # Task 统一测试 Run/重试/比较/证据 API
│   │   ├── workers.py           # 分布式 Worker CRUD + stop/start/destroy/retry
│   │   ├── sub_agents.py        # 通用子 Agent summary API (GET /tasks/{id}/sub-agents/summary)
│   │   ├── ws.py                # WebSocket 端点
│   │   ├── voice.py             # Whisper 语音转文字
│   │   ├── auth.py              # Token 登录
│   │   └── system.py            # 健康检查 + 统计
│   ├── middleware/auth.py       # Bearer token 认证中间件
│   ├── models/                  # SQLAlchemy ORM 模型
│   │   ├── task.py              # Task (含 session_id, attention_tag, last_cwd, project_id, enabled_skills)
│   │   ├── plan_agent.py        # Planner/Reviewer run + step 审计
│   │   ├── plan.py              # Plan/Version/Input/Application 聚合模型
│   │   ├── instance.py          # Claude Code 实例
│   │   ├── project.py           # Project (含可信 preview_config)
│   │   ├── workspace_review.py  # 持久化当前工作区审查运行/指纹/报告
│   │   ├── test_harness.py      # Run/Attempt/Event/Evidence/Finding/Lease/ChildBinding 持久记录
│   │   ├── project_todo.py      # ProjectTodo (per-project prompt 模板/清单, status open/done/archived, created_task_id 溯源)
│   │   ├── sub_agent.py         # SubAgentSession + SubAgentReport (通用子 agent 表, agent_type 分类)
│   │   ├── monitor_session.py   # 兼容 shim: MonitorSession/MonitorCheck = sub_agent 别名
│   │   ├── pr_monitor.py       # MonitoredRepo + PRReview (PR 自动审核)
│   │   ├── worker.py            # 分布式 Worker（EC2 实例 + bootstrap 状态机）
│   │   ├── log_entry.py         # 执行日志
│   │   └── worktree.py          # Git worktree 跟踪
│   ├── schemas/                 # Pydantic 请求/响应模型
│   ├── mcp/                     # MCP Server (给 Claude 注入工具能力)
│   │   ├── __init__.py
│   │   ├── ccm_skills_server.py # FastMCP server: create_monitor / check_monitors / stop_monitor
│   │   ├── ccm_browser_review_server.py # 固定 URL 的隔离浏览器审查工具 + 证据回传
│   │   ├── ccm_workspace_review_server.py # 当前分支 Preview + 独立 Browser Agent 高阶工具
│   │   └── ccm_monitor_agent_server.py # 子 Agent MCP server: report_status / mark_complete / get_context
│   └── services/                # 核心业务逻辑
│       ├── instance_manager.py  # 子进程生命周期 (launch/stop/consume output, MCP config 注入)
│       ├── codex_app_server.py  # 按 CODEX_HOME 分片的常驻 app-server registry
│       ├── codex_pool.py        # Codex 多账号选择、冷却与实时/rollout 额度读取
│       ├── cloudrouter_accounts.py # 私有 API Key 目录、运行配置、模型/额度探测
│       ├── codex_session_migration.py # 跨账号安全复制/合并原生 rollout
│       ├── dispatcher.py        # 全局调度器 (9 步任务生命周期, 含 goal 模式 + monitor 子 agent)
│       ├── goal_evaluator.py    # Goal 条件评估器 (Claude/Codex provider 分流)
│       ├── plan_agent_runner.py # 严格只读 Planner/Reviewer pipeline + exact process cleanup
│       ├── plan_tasks.py        # Plan 上下文快照、repo 指纹、stale/附件校验
│       ├── plan_service.py      # Version 状态机、输入/审批、执行 Task 物化与 Worker outcome 导入
│       ├── mcp_config.py        # Provider-neutral MCP specs + Claude/Codex renderers
│       ├── internal_api_endpoint.py # 子进程回调使用的真实 Manager API origin
│       ├── skill_context.py     # Task-scoped 普通/User Skill 目录与 provider adapter
│       ├── tmp_space_manager.py # /tmp 容量/inode 看门狗与白名单安全清理
│       ├── update_runtime.py    # 更新脚本可信快照的专用目录与进程身份回收
│       ├── claude_pool.py       # Claude 账号池 (限速检测/自动切换/session 迁移/额度查询)
│       ├── ralph_loop.py        # 自动取活循环 (legacy, 保留兼容)
│       ├── stream_parser.py     # NDJSON stream-json 解析器
│       ├── task_creation.py     # Task 创建统一边界（默认值/校验/事务内 stage）
│       ├── task_queue.py        # 优先级队列 (asc = 优先级越高)
│       ├── task_artifact_contract.py # Task 产物 namespace/version/项目根契约
│       ├── task_termination.py  # Task→Instance exact-generation 安全终止
│       ├── worktree_manager.py  # Git worktree 创建/合并/删除 + rebase+push
│       ├── pr_review_service.py  # PR 审核 prompt 构建 + task 创建 + 状态回查
│       ├── pr_review_panel.py    # required-identity exact-head CI Gate + 三角色 Reviewer Panel + Finding 聚合
│       ├── pr_monitor_loop.py    # 跨 head PRMonitorRun + Shadow Repair evidence/Wake
│       ├── pr_review_adjudication.py # Finding Rebut + 独立裁决 + Thread 清零
│       ├── pr_merge_queue.py     # durable Merge Queue + merge_group exact-head Gate
│       ├── browser_review.py    # 隔离 Playwright、安全动作与独立 CLI harness
│       ├── browser_network.py   # 浏览器强制代理、逐连接 DNS/IP 公网边界
│       ├── browser_review_jobs.py # 单并发 CCM Task-backed review job/证据管理
│       ├── workspace_review.py # Git 指纹、可信 Preview 生命周期与黑盒审查编排
│       ├── workspace_review_intent.py # 普通对话中的前端运行验收意图路由
│       ├── test_harness.py     # provider-neutral 测试门面与生命周期投影
│       ├── test_harness_artifacts.py # 私有持久证据、配额/保留期与原子归档
│       ├── test_harness_runtime.py # Browser Agent 独立 provider/model/effort 配置解析
│       ├── test_harness_children.py # Browser Agent 子 Task 持久所有权/启动门禁/恢复
│       ├── test_harness_contracts.py # Target/TestPlan/Verdict/Finding 契约
│       ├── test_harness_git_targets.py # 公共 GitHub PR/ref 元数据解析与 exact-SHA 冻结
│       ├── test_harness_sandbox.py # PR/ref 临时 Sandbox Runtime、持久 Lease 与动态能力探测
│       ├── test_harness_egress_proxy.py # Sandbox 内 CONNECT 白名单、DoH 与逐连接公网 IP 门禁
│       ├── test_harness_preview_relay.py # 固定 source:4173 的可信 loopback Preview TCP relay
│       ├── test_harness_process_wrapper.py # Sandbox Preview 的无 shell 进程、PID、日志与退出码包装器
│       ├── test_harness_targets.py # PR/ref exact-SHA 解析、隔离源码与 Preview 编排门禁
│       ├── ws_broadcaster.py    # WebSocket channel 广播
│       ├── whisper_client.py    # OpenAI Whisper 客户端
│       └── backup_service.py    # 数据库备份 (auto-backup SDK 封装, 可选)
├── frontend/
│   └── src/
│       ├── api/client.ts        # API 客户端 + 类型 (401 自动登出, 动态 base URL)
│       ├── api/ws.ts            # WebSocket 客户端 (指数退避重连)
│       ├── config/server.ts     # 远程服务器 URL 配置 (Capacitor/Android 支持)
│       ├── config/theme.ts      # 主题注册表 (现代深/浅 + Legacy 组, localStorage 持久化)
│       ├── pages/               # Dashboard、TasksPage、PlansPage、PRMonitorPage 等一级页面
│       ├── components/
│       │   ├── MarkdownContent.tsx            # Chat/Plan 共用 GFM 渲染（代码复制/链接/表格）
│       │   ├── Chat/ChatView.tsx              # 多轮对话 UI (基于 task, 含 monitor 消息渲染)
│       │   ├── Chat/BrowserReviewPanel.tsx     # Task 右侧/浮窗测试配置、待机页、Harness 证据
│       │   ├── Chat/TaskArtifactLink.tsx       # Markdown 任务文件链接一键下载
│       │   ├── Chat/SubSessionIndicator.tsx   # 子 session 计数指示器
│       │   ├── Chat/MonitorPanel.tsx          # Monitor 面板 (活跃 monitor 列表 + 历史 checks)
│       │   ├── Instances/              # InstanceGrid, InstanceLog
│       │   ├── Tasks/                  # TaskForm、TaskList、独立 attention tag 编辑
│       │   ├── Layout/AppShell.tsx     # App 壳 (桌面侧栏导航 + sticky 顶栏 + 移动端抽屉)
│       │   ├── Layout/PrefsMenu.tsx    # 顶栏齿轮下拉 (时区/主题/PTY/压缩阈值/飞书/密码/退出)
│       │   ├── Layout/PoolDrawer.tsx   # Pool 额度抽屉 (顶栏 "Pro" 徽标 + 账号额度进度条)
│       │   ├── PlanReview/           # Plans 页创建/目录/审批 + 会话内响应式历史/详情/决策 UI
│       │   └── Voice/VoiceButton.tsx   # MediaRecorder → Whisper
│       └── hooks/useWebSocket.ts
├── scripts/
│   ├── dev.sh                   # 一键启动开发环境
│   ├── benchmark_codex_transport.py # 真实 Codex exec/app-server 延迟 A/B（手动、消耗额度）
│   ├── browser_review_demo.py   # 前端浏览器审查 CLI 入口
│   └── tunnel.sh                # ngrok 隧道
├── .env                         # AUTH_TOKEN, OPENAI_API_KEY, DATABASE_URL
└── pyproject.toml
```

## 依赖链（重要）

- 本仓库依赖 **claude-pty**（Claude-Code-PTY 仓库），git rev **pin 在 uv.lock**，不会自动浮动
- PTY 框架更新后必须显式级联：`uv lock --upgrade-package claude-pty && uv sync`，提交 uv.lock
- 生产（8002, ccm-b.service）要使依赖生效：`systemctl --user restart ccm-b`（重启时机需用户确认；启动属主与错库教训见 PROGRESS）
- 领取任务时若涉及 PTY 接口/行为变化，先对比 uv.lock 中 pin 的 rev 与 Claude-Code-PTY main HEAD，落后则先 bump

## 关键约定

- **MCP 内部回调地址**: MCP/AskUser 子进程回调 origin 统一由 `internal_api_endpoint.py` 解析：显式配置优先，否则使用可信 ASGI `scope["server"]` 捕获的真实监听地址，最后才回退 `settings.host/port`；禁止依赖可能与 Uvicorn `--port` 不一致的静态端口。
- **CCM MCP 模块隔离**: Task-scoped CCM stdio MCP 必须使用 `python -P` 并把运行中 Manager checkout 固定为 `PYTHONPATH`；不能依赖 MCP 配置里的非标准 `cwd`，否则 Claude 在审查另一个 CCM checkout 时可能误导入目标分支中的旧 `backend`。
- **Frontend Test Harness**: `TestHarnessService` 是普通对话、一次性按钮、Goal、固定 URL、当前工作区和 PR/ref 的统一门面；`TestHarnessRun/Attempt/Event/Evidence/Finding` 持久化冻结的 Target/TestPlan/runtime、精确 SHA、阶段事件、内容哈希证据、结构化结论和稳定 finding fingerprint。公开 API 为 `/api/tasks/{id}/test-runs`（start/list/get/cancel/repeat/compare/evidence），Agent MCP 走同一 internal start/status/stop；旧 Browser/Workspace API 只保留兼容 adapter，不得另建平行状态源。每个 Task 同时只允许一个 active run，idempotency key 必须绑定完整输入指纹；Browser 子 Task 必须先持久化 `TestHarnessChildBinding` 的 owner/job/child 身份，再由 `reserved → ready` 打开发送门，领取、停止、父 Task cancel/delete 与重启恢复都按同一 binding 收敛，禁止仅靠内存 job 句柄。重启后先恢复并清理持久 Sandbox lease、完成或拒绝受管 job 证据归档，再把剩余未终态 run fail closed；Goal 只接受最新、非 stale、已清理、`evidence_archive_state=complete` 且包含截图/报告的 passed run。PR/ref capability 必须同时通过管理员开关、Docker daemon、固定本地镜像身份、Project 已批准的 `sandbox` Preview profile 与目标流水线探测；任一条件不满足都要从 API、MCP、意图路由和底层 Target manager 一致 fail closed。可用时由 Manager 只经公共 GitHub 元数据冻结 exact SHA/变更清单，源码 fetch、依赖与 Preview 只在隔离容器执行；禁止在 Manager 宿主机 fetch、build 或执行不可信提交，也禁止改测当前工作区。
- **Browser Review**: 每个本机普通 Task 都注入 required `ccm_frontend_review` 与 `ccm_workspace_review` MCP（不受 `CODEX_MAIN_MCP_ENABLED` 影响）；显式 URL、当前工作区和 PR/ref 最终都创建 Harness run。普通 Task 的 `ccm_frontend_review` 只暴露 start/check/stop 高层控制器，浏览器动作一律交给按 Harness runtime 单独路由的 archived 子 Task；不能以内联父模型执行却记录成另一模型。普通人类 follow-up 命中前端运行验收意图时须附加 fresh-run 路由协议：当前修改调用 `test_current_changes`；明确 PR/ref 调用 `test_git_target` 创建 exact-SHA Sandbox run，能力不可用时如实返回门禁原因，不能用旧报告、当前工作区或代码分析替代。固定 `enabled_skills["browser-review"]` 的黑盒子 Task 只暴露绑定的 `ccm_browser_review`，不得继承普通 Task controller/context MCP；Claude 必须关闭内置工具与 ambient settings，Codex 必须使用 app-server MCP-only profile 且不可退回 exec。浏览器默认只读；即使开启交互也阻止跨源顶层导航、弹窗、下载和 Service Worker，页面/DOM/遥测一律视为不可信证据。外部 URL 只能使用公网 HTTP(S)，Chromium 必须强制经过 loopback egress proxy，并在每个主文档、重定向、子资源和 WebSocket 连接上重新解析全部 A/AAAA，任一非公网结果即拒绝；受管 Preview 的 HTTP、CONNECT 与 WebSocket 则只允许配置中的精确 IPv4 loopback origin/端口，必须拒绝同机其他 loopback 端口和一切跨源请求。底层 Browser job 仍是 Manager 本机全局单并发执行槽。
- **Test Harness 证据存储**: Browser job 暂存和 Harness 归档统一位于管理员配置的私有 `test_harness_artifact_root`，数据库只保存相对 storage key、SHA-256 和大小。Attempt 以 `staging/archiving/complete/retryable_error/incomplete`、独立 staging 指针、archive prefix 与 manifest 表达提交协议；只有预期文件全部归档并重新按大小/SHA-256 打开验证后才能清除 staging 指针和进入 `complete`，缺目录、部分复制、篡改或重启恢复失败一律让 Run fail closed。归档必须以 no-follow FD 读取普通文件、校验 PNG magic、原子写入内容寻址版本并 fsync；同摘要目标由保留 staging 原子重写以修复损坏，下载复用已验证 FD 流式返回，禁止校验路径后再次 reopen。按单文件/run/task/全局配额与保留期清理，只允许删除受管根内已证明安全的 terminal/orphan 证据；非 complete Attempt 的 staging、活跃 run 均不得被历史裁剪、shutdown 或配额回收，内存中的 Browser job 历史只移除句柄、不删除 staging。
- **当前工作区黑盒审查**: `Project.preview_config` 是管理员确认后保存的 shell-free argv 契约，自动检测结果只能建议、不得直接执行；确认界面必须明确提示“工作区代码仍以 CCM 系统用户执行”，因此只用于可信本地开发分支。PR/ref 必须使用同一配置中另行显式批准的 `sandbox` profile，不能复用 host 命令或从目标提交读取启动配置；依赖安装仅经固定公网 DoH 逐连接解析并校验全部 A/AAAA 的白名单代理，Preview 启动后必须删除代理和 outbound network 并反向证明源码容器只剩 internal network，再由固定目标、只读的可信 relay 映射到随机 IPv4 loopback 端口。Sandbox 仅允许隔离 `/workspace` tmpfs 执行依赖与构建产物，`/tmp`、HOME 和 `/run` 均保持 `noexec`；失败命令只回传有界、可打印的输出尾部。Preview 使用隔离临时数据库/目录并清除模型与云凭证环境变量。每次运行以 HEAD、tracked binary diff、限量 untracked 文件内容及 Preview config 生成指纹；独立 archived Browser Task 只收到冻结后的 TestPlan、URL、无 diff 内容的目标/变更文件元数据和 browser tools，不收到父会话、源码或仓库上下文。PR Browser 报告必须把每个 frontend candidate 映射到已验证状态或明确标为未覆盖。Task 输入区眼睛按钮启动一次 run，运行中的 Claude/Codex 也可直接调用工具；右侧/浮窗展示 Harness 阶段事件、截图、轨迹、finding、报告、cleanup 与 stale 状态，终态可 repeat 并自动切换。取消必须同时停止精确子 Task、Browser job 与 Preview；当前工作区指纹变化后历史结果标记 stale。
- **Browser Review 轨迹与执行配置**: Task Chat 的前端测试入口常驻；无 run 时右栏显示待机页，当前工作区/固定 URL/PR/ref、视口、浏览器、交互策略和 Browser Agent 的 provider/model/effort/tier 集中在面板配置页，不再提供左侧独立 Browser Review 一级页面。所有新建入口默认使用随 Playwright 安装的 `chromium`，只有用户显式选择时才使用系统 `chrome`；历史 Run 继续按冻结值复现。面板必须使用 capability 返回值在创建前显示 workspace 或 sandbox 的具体门禁，并对 PR/ref 展示 resolved repository/SHA/changed files/cleanup。每个 Task 默认继承父 Task 路由，也可在 `Task.metadata_["test_harness_runtime"]` 保存独立路由；普通对话、一次性按钮、Goal 和所有面板 run 都经 `test_harness_runtime.py` 统一解析并在创建时冻结，Browser 子 Task 不得再次读取父 Task 覆盖它。普通对话命中前端运行验收意图后立即打开右侧等待栏，并以发送前的最新 Harness run ID 为基线轮询、精确切换到本轮新 run；一次性入口则用创建回包立即注入。面板持续展示同 Task 的持久化生命周期事件、冻结的 Browser Agent 路由、截图、遥测、finding、产物和报告；轨迹只汇总公开的 assistant 进度/决策摘要与 browser tool-use，不展示 reasoning/hidden chain-of-thought，也不从动作反推伪造“思考”。`browser_type_text` 只显示字符数，`finish_review` 参数只显示报告已保存；模型仍须把最终报告作为原 Task 的最终回复。
- **Browser Review Goal**: 已有 Task 的 Chat follow-up 工具栏提供“循环审查”按钮；只允许 Manager 本机、已有 session、已安全终态且 Project 已确认 Preview config 的 Task 通过 `POST /api/tasks/{id}/frontend-review-goal` 启动。仓库门禁按真实续接顺序检查 `last_cwd → target_repo → Project.local_path`，要求当前进程可写、无符号链接的本地 Git worktree。API 在同一 Task/session 上原子重置 Goal 进度并交回 Dispatcher；内部协议要求每轮调用 `test_current_changes` 完成“当前指纹审查→必要修改→构建/测试→新指纹复查”，模型可提前完成但受 1–10 轮硬上限约束。评估器判断 achieved 后仍须通过最新 Browser job 的客观门禁，否则继续下一轮。follow-up 启动的 Goal 是临时执行模式，任何终态都必须恢复启动前 Task mode 并清除 activation，普通后续消息还要兼容清理旧版泄漏状态。前端只在真实活动期显示 Goal 轮次/评估反馈，同 Task 新建审查时自动切到最新运行并展开面板或浮窗。
- **Browser Review Goal 证据顺序**: `TestHarnessService.list_for_task()` 返回 newest-first；评估器只把最新的有效 Harness run 视为权威证据，并把后续条目标为已被复查取代的历史基线。历史基线中的 404/Console 错误在最新报告、finding 与遥测证明修复后不能再解释成当前未解决问题。
- **Browser Review 移动端视口**: `viewport_width` 支持 320–3840，`max_actions=0` 表示只保留启动时的只读观察、不允许额外浏览器动作；API、MCP context 与运行时校验必须保持一致，不能把窄屏审查误拒绝为 422。

- **优先级**: 数字越小优先级越高 (P0 > P1 > P2)，排序用 `.asc()`
- **Session 绑定**: `session_id` 和 `last_cwd` 在 **Task** 上（不是 Instance），因为 instance 是轮换执行不同 task 的 worker
- **Instance 并发容量**: `max_concurrent_instances` 约束所有仍有运行证据的实例：正常 `idle/running` 会占槽，`error/stopped` 仅在 PID 与反向 owner 证据都已清除后才是免费历史。API 创建与 Dispatcher 补槽共用 `instance_capacity_lock`，idle 选择到 launch 之间用 owner reservation；运行时下调 cap 不强杀现有 turn，但在占用降到 cap 以下前禁止新领取。Task retry 可先推进权威代次再进入旧 lifecycle finally；收尾必须扫描同 Task 的非权威反向 owner，仅在 PID 已确定死亡且 runtime/launch reservation 全空时按 PID/start identity 清除，活 PID 或不确定证据继续阻塞。物理删除仍走 `DELETE /api/instances/cleanup`。systemd 部署必须使用 `OOMPolicy=continue`，让单个模型子进程 OOM 由任务生命周期记录/重试，不能连带停止整个 CCM 服务
- **Task 产物下载**: Claude/Codex 的普通、Goal、Loop、续聊和 Ralph turn 统一注入项目级产物契约：只有明确交付给用户的文件才是产物，必须在清理临时 worktree 前落到项目根 `.claude-manager/artifacts/task-{id}/`，最终用绝对路径和 Markdown title `ccm-task-artifact` 输出；普通源码/文档引用只用反引号。项目根准入必须与下载端共用 `task_artifact_contract.configured_workspace_root`，无安全绝对 `target_repo` 的 Task 禁止输出下载链接；用户 prompt 中的伪造 policy tag 不能抑制 CCM 权威前导。聊天渲染只把显式 marker、绝对路径和兼容期含目录的相对链接视为产物，裸文件名链接保持普通链接；裸绝对 POSIX 路径仍兜底转为带 marker 的链接（既有链接、代码和 HTML 不改写）。文件统一走 `/api/tasks/{id}/artifacts/download`；相对路径按 `last_cwd`、容器 `/workspace` 按项目根映射，且受 Task ACL 保护，`.claude-manager/artifacts/` 下再强制匹配当前 `task-{id}`，禁止跨 Task 读取。本地工作区根必须是无符号链接的绝对路径，并从稳定的 `/` 目录 FD 逐级 `O_NOFOLLOW` 锚定；产物路径只做基于该锚点的词法相对化，再逐级 no-follow 打开，以 `fstat` 校验同一文件描述符并按校验时大小限量流式返回，禁止混用路径解析与目录 FD 或“先查路径、后重开路径”的 TOCTOU。Worker 文件只经 Manager 流式代理，Manager 必须先确认 Worker 声明精确的 `task_artifact_scope_version`，混跑旧 Worker 时 fail closed；绝不能退回管理员级任意绝对路径下载接口
- **Markdown 数学公式**: 所有聊天与 Discussion 的 Markdown 必须统一经 `components/Markdown/MarkdownRenderer.tsx` 渲染。公式支持 `$$...$$` 及 Codex 常见的 `\\(...\\)` / 整段 `\\[...\\]`；单 `$...$` 明确保留为普通文本，避免价格等货币内容误判。反斜杠分隔符必须在 Markdown AST 上按 text node / 整段 paragraph 转换，跳过链接、图片、HTML、definition 和各类 code，禁止 raw source 全局改写；KaTeX 必须保持 `trust: false`、有界 `maxSize`，直接依赖版本须与 `rehype-katex` 使用的版本一致
- **Claude Code 调用**: `claude -p [prompt] --dangerously-skip-permissions --output-format stream-json --verbose`
- **Resume**: `claude -p [follow-up] --resume [session_id]` — 必须使用和原始 session 相同的 cwd
- **一键更新安全门**: `UpdateService` 启动更新/回滚前先 `dispatcher.pause_dispatching()`，再查 blocker；有活动工作必须拒绝且恢复调度，`force` 也不得绕过。`_dispatch_claim_lock` 是统一任务启动门禁：普通 Dispatcher/Worker dequeue、per-task chat/monitor/sub-agent 续跑、RalphLoop 和手动 Instance task 都必须在门禁内把 Task 持久化为 `in_progress/executing` 后才能 launch；prompt-only Instance 虽无 Task，也必须在 launch 返回前持久化 `Instance.status=running/current_task_id=NULL`，并由 blocker 查询覆盖。已入队但尚未启动的续跑由 `_pending_task_starts` 计入 blocker，维护期间保留消息并取消本次重启；stop-session 的队列清理必须在同一 admission lock 内同步更新 pending/in-flight bookkeeping，绝不能残留幽灵 blocker 或误删正在处理的 blocker。pause 只关闭新启动入口，绝不能 cancel 已运行 lifecycle。正常更新、迁移、手动 pull 后的快速重启和 rollback 统一通过 `maintenance_shutdown_guard`：所有广播/宽限 sleep 在最终检查前完成，最后一次 blocker 查询和同步 spawn/stop 之间不得有 `await`；查询失败 fail closed。更新检查按目标分支的 tracking remote（无则 `origin`）做 fetch-only dry run；进程启动时固定 running commit，与磁盘 HEAD 不同即视为手动更新待完成，绝不能用 commit 时间/systemd 启动时间猜测。前端每次完整打开/刷新/重新登录后检查一次，同页同 commit 只显示一次顶部非阻塞通知，点击“查看详情”才开更新弹窗；后端自动 dry-run 需以 30 秒缓存 + async lock 合并并发 fetch，但 blocker 每次实时查询，手动重新检查用 `force` 绕过缓存。远端 fetch 失败通常静默，但若本地 `needs_restart` 已成立，仍必须提醒用户完成部署重启。
- **Per-task 队列清理代次**: `q.get()` 会先于 consumer 的 in-flight 登记移除消息；stop/cancel 必须持有引用计数的 Task cancellation lease 直到终态 CAS/发布完成，`clear_task_queue()` 则在 admission lock 内推进 per-task generation。消息 enqueue 时固定 generation，consumer 登记 in-flight 前必须在同一锁内校验；旧代次 handoff 视为已经取消，调用 `task_done()` 后不得 launch。已登记的真实 in-flight blocker 则不能被 clear 隐藏。
- **更新/回滚操作准入**: `UpdateService.start_update()` 与 `rollback()` 必须共用 `_operation_lock`；长流水线的 `_lock` 不能替代入口准入，因为新建 pipeline 可能尚未获得它。回滚须在准入锁内捕获并验证同一个 `UpdateState`、`old_commit`、`backup_file`，并保持锁直到操作已预留（当前实现直到最终停服提交并标记 `restarting`），确保并发更新/回滚只有一个被接受。入口在 `pause_dispatching()` 之后到后台 pipeline/停服脚本正式接管之前若被 request cancellation / `CancelledError` 打断，必须恢复调度再重新抛出，绝不能留下永久 paused 的门禁。
- **部署修复与启动守卫**: 更新状态必须同时报告进程实际加载的 `running_commit`、磁盘 `disk_commit` 和 Alembic current/head；磁盘代码相同不等于部署完成。`POST /api/system/update/repair` 用当前磁盘 commit 重新执行依赖同步、PTY 刷新、前端安装/构建、数据库检查/迁移和受控重启；`POST /api/system/restart` 只允许 commit 与数据库均已证明一致时使用，另保留显式手动重启入口。更新/修复/重启都要求 Git 工作树对所有非 ignored 的 staged、unstaged、untracked 路径保持干净；运行时产物必须窄化写入 `.gitignore`，绝不能让未跟踪源码绕过 commit 身份校验。自动更新/修复只支持可停服快照并回滚的文件型 SQLite，外部数据库必须走人工备份/迁移流程。仓库级 `backups/deployment-lease.json`（token + PID/start identity）是权威部署事务，`deployment_start_guard.py` 在 pre-start 与 app lifespan 中阻止不安全的隐式 `uv sync/init_db`；失败或半完成事务只启动 maintenance-only 恢复面，普通 API/Dispatcher/Worker 全部关闭，管理员可用 legacy recovery token 或未过期的已签名 admin JWT 调 status/repair/rollback。`scripts/update_migrate.sh` 协议 v2 必须在停服后重新生成并校验 SQLite 快照，任何恢复步骤失败都保持停服和 incomplete lease，绝不能启动代码/依赖/数据库混合版本。
- **跨进程部署栅栏**: 同一 checkout 的每个 task claim 都须持有 `deployment-lease.lock` 共享锁直到活动状态提交；更新者持排他锁写 active lease。更新/修复/重启/回滚在 lease claim 后、任何 checkout/依赖/备份 mutation 前必须再次查询 blocker，覆盖“首次查询后刚提交的 task”竞态；若取消准入，必须终结本次 lease、恢复 Dispatcher，并保留回滚所需的旧部署元数据。外部 worker 每次写状态或停启服务前都要校验 token、operation、active status 与 handoff，超时后即使 token 相同也不得继续。
- **默认 Provider**: 新任务默认使用 `codex`，Codex 默认模型为 `gpt-5.6-sol`；均可通过 `DEFAULT_PROVIDER` / `DEFAULT_CODEX_MODEL` 覆盖。所有本地可执行/可续聊 Task 创建入口（含 standalone Plan 物化、PR Review、Fork）统一经 `task_creation.stage_task_record`，显式持久化解析后的 provider/model/effort；该函数只 flush 不 commit，便于调用方原子写关联资源，禁止依赖 ORM 中为兼容旧库保留的 Claude 默认值。只读 shared shadow 是唯一例外，不参与本地调度，运行配置由远端权威 Task 同步
- **浏览器前端审查兼容入口**: 固定 URL 的 Web 入口统一位于 Task 右侧测试栏；旧 `/api/browser-reviews` 管理员 API 暂保兼容，`backend/services/browser_review.py` 的 OpenAI Responses `computer` + Playwright 直连仅供独立 CLI 诊断。内存 browser slot 在重启时丢失执行句柄，Harness 会把残留运行标为 interrupted 并保留此前已归档的历史事件/证据；`scripts/browser_review_demo.py` 保留 CLI 入口。
- **Plan Reviewer fallback**: Codex Reviewer 在首个 agent/reasoning delta 后若连续 90 秒无新 delta，记录 Step 的 `last_delta_at/streamed_output_chars/last_event_type`，精确中断并删除 disposable thread；只有清理确认成功后才将 primary Step 标记失败并切换配置中的 fallback。已正常结束但未通过结构化语义校验的 primary Step 也直接进入 fallback，其模型正文不得参与 transient retry 或账号失效/轮换判定；进程/thread 清理不确定时必须 fail closed。首个 delta 前仍使用 Reviewer 总超时，避免误杀正常的 xhigh 首轮推理
- **Model 配置**: 默认 `claude-opus-4-6`，支持全称模型 ID（`claude-opus-5`, `claude-sonnet-5`, `claude-fable-5`, `claude-opus-4-6`, `claude-opus-4-7`, `claude-opus-4-8`, `claude-sonnet-4-6`, `claude-haiku-4-5`）。Opus 5 固定使用 1M context（无 `[1m]` 变体），支持 `low/medium/high/xhigh/max` effort；其余支持的模型用 `[1m]` 后缀开启 1M context
- **Effort Level**: 默认 `medium`，支持 `low/medium/high/xhigh/max`。优先级链：Task.effort_level → Instance.effort_level → settings.default_effort。通过 CLI `--effort` 参数传递；PR Monitor 可用 `MonitoredRepo.review_effort` 覆盖其生成审核任务的 effort
- **PR Monitor 闭环边界**: required CI 只按 exact head 的 `(kind, name, app_slug)` 身份判定；Senior 的完整 changed-file 内容由 Manager 按 base/head blob 注入，Reviewer 始终 tool-free。blocking Finding 由后端以 nonce/fingerprint 发布独立 inline comment，无法锚定时降级为独立 PR comment但绝不解除 blocker；Rebut 必须由独立 tool-free Adjudicator裁决。新 head 全绿后须先进入 `resolving_fixed_threads`，持久化解决旧 subject 的全部已发布 Finding effect，才可通过 zero-thread Gate。自动Repair为repo opt-in，只恢复绑定的原Developer Task/session/cwd；唯一exact project/repo/branch候选可自动绑定，远程Task必须先经`TaskMigrator`权威迁回Manager，失败就暂停，禁止新建替代Agent。Repair push webhook 可能早于Developer turn终态，synchronize必须先持久化Wake成功再停止旧head generation，后到的终态不得回写为失败。Merge Queue与legacy auto-merge互斥，使用durable action/lease、exact-head enqueue和`merge_group` required-CI Gate，只有读取远端PR确认merged才结束。
- **PR Monitor 权威文档**: Reviewer Harness、CI-first、Finding/Rebut、自动Repair、跨Worker、Thread Gate与Merge Queue统一记录在`docs/pr-monitor-design.md`；不要再拆分或生成重复的设计/易读版/HTML文档，架构或验收边界变化时只更新该文件和本指南的必要摘要。
- **API 网关账号（CloudRouter/Apex）**: API Key 与原生账号共享 Claude/Codex 号池选择、模型、Task 和 session 迁移逻辑，不增加 task 级“API 模式”。每把 Key 固定存到 `CLOUDROUTER_ACCOUNTS_DIR/{cloudrouter|apex}-N/` 私有目录（目录 `0700`、Key `0600`），分别生成 Claude config 与 Codex home；CloudRouter 由 OpenAI-compatible `/v1/models` 投影 Claude/Codex，Apex 当前固定为 Codex-only `apexrouter`（Responses API；旧 `apex_gateway` 受管配置会严格校验后原子迁移），其原生 Codex model catalog 请求必须带 pinned CLI `client_version` 并解析 `models[].slug`。`/v1/usage` 展示额度：CloudRouter `mode=unrestricted` 表示当前 Key 没有独立额度上限和时间限制，其有效额度上限/剩余额度等于所属账号的上限/剩余额度；接口返回的零 `balance/remaining` 不是账号额度数值，前端不得显示为 `$0`，而应显示与账号额度的继承关系、真实 Key 用量和时间“不限制”，账号额度具体数值导向控制台。受限 Key 继续显示接口返回的独立额度与到期时间；Apex 的 `used` 是当前 Key 用量，`remaining/limits/concurrency` 是分组共享值，两者绝不能混算；Apex 不返回到期时间时前端显示无法确认。运行进程只经 helper 取 Key并清除继承的官方及对应网关凭据；受管 Codex config 的 provider/base URL/auth helper 及未知持久配置继续严格 fail closed，`personality = "pragmatic"` 可保留，Codex 0.144.6 自动写入的精确 `[projects."<绝对规范路径>"] trust_level = "trusted|untrusted"` 只视为可恢复状态并在校验后原子剥离，额外字段、MCP/命令配置一律拒绝。所有 API Codex 的 app-server/exec、Fork 空线程、Sub-Agent、Goal evaluator 和 Distill 都用 session/CLI whole-map override 把 Codex 规范 trust target 标为 `untrusted`，禁止项目 `.codex` 启动 MCP/hooks；原生 Codex 项目信任行为不变。Claude API 账号跟随全局 PTY 开关：开启时由 wrapper 在最终 exec 前移除官方认证变量并以 `umask 077` 启动，关闭时走直接 `stream-json` 子进程；两路都必须把结构化 API fatal error 覆盖为失败，不能因 CLI/常驻进程 exit 0 误标完成。`.claude.json` 是 CLI 可变状态：仅在同 owner regular/non-symlink 前提下安全收敛回 `0600`，其他受管文件继续严格 fail closed。账号不可用、模型不匹配、存储 I/O 失败或已知额度耗尽时必须脱敏 fail closed，queued message 不得无限重排。PTY transient retry 只由 `FullMirrorCCMBackend.on_exit` 持有；queue 只等待 chained proxy。当前 pinned/remote-main claude-pty 会把所有结构化 `rate_limit_event` 当硬失败，故 FullMirror 必须在 exact `on_event` 中把 `allowed`/`allowed_warning`（及 orphan/autonomous 旧事件）恢复为非终止信号，只有 foreground hard limit 才中止 turn。网络错误不得把此前已知失效额度重新判为可用。
- **API 网关账号安全删除**: DELETE 先持久化 `retired + cleanup_pending` 并从两类号池停止新准入，再证明 Task/Instance、PTY 热 session、Goal/Distill、Monitor/Sub-Agent、迁移、额度请求及 Codex home 全部静止；CloudRouter 还必须 fail-closed 清理精确挂载账号目录的 CCM Docker 容器，Codex-only ApexRouter 跳过该不可能路径。忙碌时只返回 409 并保留可见、可重试的 pending tombstone，不强杀任务；存储完整性异常返回 500，不能误报为已停用。最终清理从固定 store root 逐级 `O_NOFOLLOW + dirfd` 锚定并校验 inode，只删除 Key/helper/运行配置，保留 `claude/projects` 与 `codex/sessions`；完成 tombstone 隐藏且账号编号不复用。
- **API/原生账号路由优先级**: 手动 preferred 最高，并在下一轮安全迁移健康旧 session；未指定时，旧 session 保持 resident/bound，新 session 优先选择兼容且可用的 API 网关账号，API 不可用再回退原生号池。Claude 以 `Task.metadata_["claude_account_id"]` 持久消歧迁移后保留的副本；`migrate_session` 必须递归 hardlink JSONL 同级的 session sidecar（主 JSONL 最后落盘作为 commit point），失败保留健康 resident 或 fail closed，轮换必须优先从当前 resident 而非搜索顺序中的旧副本迁移。Codex 必须先持久锚定已注册 source，再把 rollout 复制、app-server rebind 与 target Task binding 作为一个 cancellation-settled 切换单元；orphan source 在复制前 fail closed。无绑定的分叉副本只能由显式 preferred 消歧，否则永久拒绝；临时路由失败必须保留原 queued message。两池 `select()` 只生成候选（Codex 用独立 selection cursor 保持轮询），`last_selected` 只在最终 binding 提交或临时进程实际 spawn 后更新，前端都显示「最近使用」。
- **Codex provider 对等逻辑**: Task/Instance 的 `provider` 字段（claude/codex）分流所有 CLI 相关行为。Codex 侧的指令文件是 **AGENTS.md**（注入实现集中在 `backend/services/agent_docs.py`）：① project 创建（clone/init）时注入指向 CLAUDE.md 的 symlink（无 symlink 权限的平台回退 pointer 文件）并随 CLAUDE.md 一起 commit（`backend/api/projects.py`）；② **存量项目惰性补齐**——dispatcher 任务启动（Step 2）对 `target_repo` 调 `ensure_agents_md`：有 CLAUDE.md 而无 AGENTS.md 就补 symlink，任何老项目下次跑任务时自动补上（不 commit，由 agent 正常 git 流程带入；幂等、绝不阻断任务）；③ dispatcher 的所有 prompt（task/goal/loop）经 `_agent_doc_preamble`/`_agent_doc_name` 按 provider 引用 AGENTS.md（codex 措辞带 CLAUDE.md 回退兜底）；④ skills 模板只对 claude 注入（Codex prompt 对等留待分阶段开放）。claude-only 的能力（PTY/Claude Pool/thinking budget/ask_user hook）在 instance_manager.launch 已按 provider 门控。**Codex 对等补齐（2026-07-19）**：⑤ transient 检测按 provider 分流——`is_transient_for(provider, text)`（`claude_pool.py`），codex 文案来自 codex-rs 0.144.6 `protocol/src/error.rs` 实证（stream disconnected / request timed out / high demand / at capacity / 429/5xx 等，usage-limit 与 auth 互斥不重试），所有 retry 路径（dispatcher `_run_transient_retry`、chat `_try_chat_transient_retry`）统一走它，`_launch_params` 记录 provider 保证 relaunch 不丢；⑥ Claude 与 Codex 的号池检测、PTY 收尾和迁移链路必须按 provider 分流——codex 限额文案 `hit your usage limit` 会误命中 claude `_RATE_LIMIT_RE`，但应进入独立 `CodexPool`，绝不能触发 Claude PTY 标记、冷却 Claude 账号或用 `claude --resume` 重启 Codex session；⑦ TaskMigrator 按 provider 搬 session——codex 是 rollout 文件 `$CODEX_HOME/sessions/YYYY/MM/DD/rollout-<ts>-<sid>.jsonl`（`_move_codex_session`，保持相对路径落盘）；轮换会留下多个账号副本，跨 Worker 搬运必须选择能证明包含其他副本前缀的最长 rollout，内容分叉时拒绝猜测；⑧ Monitor 创建 API 对 codex 任务仍显式 400；一次性 Sub-Agent 走独立且可持久化的 Codex thread，并把 callback MCP 作为 required thread config 注入：停止时只 interrupt 对应 turn、绝不能杀共享 app-server，turn 终态后必须 `thread/delete` 同时删除 rollout 并回收 helper（ephemeral thread 不支持该删除协议，禁止用于此链路）；删除失败保留运行证据供重试。父 Codex thread 即使在全局 main-MCP 开关关闭时也会按 task 窄化注入 `ccm_read_skill/create/check/stop_sub_agent`，每轮终态后 `thread/unsubscribe` 释放空闲订阅但保留可续聊历史；子线程优先复用父任务绑定的健康账号，否则独立选号且不改父任务绑定；⑨ codex 解析器覆盖 reasoning→thinking、file_change/mcp_tool_call/web_search→tool 事件、todo_list、turn.failed 嵌套 error.message（字段名 exec_events.rs 实证）；⑩ context window 查 `CODEX_CONTEXT_WINDOWS`（models_cache.json 实测：gpt-5.6-*/5.5/5.4 均 272K、spark 128K，非 claude 的 200K 默认），instance_manager 窗口回填与 dispatcher 压缩阈值都按 provider 取；⑪ PR Monitor 的 `MonitoredRepo.provider` 可选 codex（审核 task 透传 provider，未配 review_model 时补 codex 默认模型）、Todo Run 模态框可选 provider；⑫ cost 决策：codex 不折算美元费用（订阅制无单价事实依据），前端只显示 token/额度。**PTY / ask_user 对 codex明确不做；完整主任务 skills-MCP 仅在 `CODEX_MAIN_MCP_ENABLED=true` 时按 thread 注入，Monitor 仍关闭，Sub-Agent 的窄化父/子 thread MCP 不受该全局开关限制**；消息注入由 app-server `turn/steer` 对等支持（`codex exec` fallback 无执行中注入能力）
- **Codex 低延迟链路（2026-07-19）**: 本地 Codex 默认走按 canonical `CODEX_HOME` 分片的常驻 `codex app-server --stdio` registry（`codex_app_server.py`），每账号独立 PID，账号内按 Task 原生 `thread/start|resume` + `turn/start` 复用；普通协议/启动失败只允许回退到**同一 CODEX_HOME** 的 `codex exec`。required 主任务 MCP 仅在确认尚未发送 `turn/start` 的 transport/thread admission 失败时，允许带同一份 MCP spec 安全回退；turn/start 超时/拒绝、未知 required-MCP 异常、账号维护中或 thread/home 不匹配时禁止 fallback，避免重复执行或串号。app-server 与 exec 对同一 home 互斥：exec 启动前关闭空闲 app-server、活跃时返回 busy；app-server 也拒绝仍有 exec generation 的 home。账号切换的 thread rebind 从空闲检查到目标 app-server 重启/owner 更新全程保留 thread 路由，期间 resume、maintenance 一律返回 busy，避免切换窗口内重新跑回旧号。app-server delta 只实时广播（不逐 token 落库），`item/completed` 才持久化；每个事件的日志、heartbeat、session_id、unread 合并为一次事务。Codex session 存在性必须按 provider 查全部账号 `$CODEX_HOME/sessions/*/*/*/rollout-*-{sid}.jsonl`，绝不能用 Claude finder 误判后摘要新开 thread。Codex 会自动加载 AGENTS.md，因此 `_agent_doc_preamble` 只补双文档同步纪律，不再要求重复 Read。新任务 commit 后须 `dispatcher.wake()`，2s poll 仅作旧路径兜底。执行中消息注入由 `/api/tasks/{id}/inject` 按 provider 分流：Codex 用 `turn/steer` + `expectedTurnId` 直达活跃 turn（图片走 `localImage`、文件走 `mention`），Claude 走 PTY `session.inject` 并明确给出已验证上传文件的可读路径；附件路径只接受 CCM upload 根目录下同属主、非 symlink 的 regular file。前端有附件时必须先确认 `attachment_protocol=1`，回包的 `attachment_count` 必须精确匹配才可清空 composer，注入在途时冻结编辑以免清掉未发送的新内容。只有 transport 明确确认完整注入后才落库/广播；turn 已结束、exec fallback 或隔离容器无法访问附件时均须在落库前明确失败，Worker task 在具备持久幂等 receipt 前继续显式 400，禁止界面显示已发送而模型没收到。聊天中的 Codex Fork 只允许选择真实用户消息，注入/模型/tool/system 事件不得作为边界；普通 follow-up 必须把原生 `thread/fork(lastTurnId)` 锚定到所选消息之前的完整已结束 turn，初始 prompt 则用同账号/home 新建无 turn 的空 thread。新 Task 继承绑定账号/home 与 cwd、以 completed/auto 状态直接续聊，并通过 `fork_seed_message` 一次性预填所选消息。数据库落库失败须用 `thread/delete` 补偿，禁止复制或手工裁剪 rollout。原生 goal/同 turn steer 场景下 `turn/start` 返回的 submission id 未必等于真实 active turn id；两者都是同一 adapter generation 的合法通知别名，必须同时保留映射直到 terminal，不能因其中一个 id 出现而丢 assistant/`turn.completed`。active 原生 Goal 的 root `turn/completed` 只释放 turn identity，不得结束 CCM generation；必须保留该 turn 的 terminal snapshot，`thread/goal/get` 不可用时持有 exact owner 并重试而非误报成功，下一原生 turn 原子失效旧 snapshot，turn 间收到非 active/cleared Goal 才按最近 snapshot 收尾。Goal 与 native descendant 重叠时须先释放旧 root identity 以接住 continuation，同时把已知 child（含 detached adoption 前缓存的 lineage）继续绑定到同一 generation，绝不能让旧 descendant guard 丢新 turn 或提前 EOF。停止 adopted goal 直接以一次 `thread/goal/set paused` 后中断真实 id，禁止先 `goal/get` 增加一次完整 RPC；transport/admission 内部 abort 必须标为 failed，只有确认的用户 Interrupt 才可按 completed 收尾。
- **Codex turn 身份与终态恢复**: 每次 `turn/start` 必须发送唯一 `clientUserMessageId`，仅以对应 `userMessage.clientId` 证明 submission/native turn ID 属于同一 adapter generation；`turn/started` 只能作为 provisional 事件，不能单独锁定权威 ID，所有已证明 alias 保留到 terminal 后统一清理。`thread/resume` 必须返回请求的 exact thread ID；返回其他 ID 属于永久、用户可见且不可重放的协议拒绝。`systemError` 只允许对该线程执行一次 archive→unarchive runtime recycle，随后仍须取得显式 `idle` 才能发送 prompt。持续 `systemError`/`notLoaded` 必须停止重排且禁止回退 `codex exec`，同时保留 exact-home owner 并向用户发布可见错误。
- **Codex 原生子 Agent 终态**: app-server 的父 `turn/completed` 不代表 native child thread 已结束。`codex_app_server.py` 必须把 `subAgentActivity`/`collabAgentToolCall`/thread lineage 绑定到 exact root adapter，并等待所有 descendant 的 `turn/completed` 或 `thread/status/changed`（`idle/systemError/notLoaded`）；状态不明用 `thread/read` 对账，绝不能猜成完成。复用 child 的新 `started` 必须覆盖旧 idle。父 completed/failed/interrupted、4h safety deadline 与 abandon 都不得在活跃 descendant 未确认终止时 EOF/detach；只能精确 interrupt child turn 并等权威终态，无法确认则继续持有 adapter，unclaimed turn 由 registry 先 draining、确认 account transport shutdown 后再释放。
- **Codex 共享 app-server 停止与错误归因**: 已有 Task→Instance/consumer 持久 claim 的 turn 停止时，若精确中断无法确认且同 `CODEX_HOME` 还有 peer turn 或已准入请求，不得关闭共享 transport；必须保留 target/peer 的 owner、process 与 consumer，exact-owner stop API 返回 409 供重试。只有证明 target 已隔离时才能升级关闭账号 transport；真正 unclaimed 的 admission cleanup 仍须 draining 并 fail closed 关闭 transport。计划关闭意图必须绑定 exact app-server 进程代次，正常 exit 0 不得报 `unexpected`；账号级 stderr tail 只记服务日志，不得归属或泄漏到单个 Task 错误。
- **Codex Fork 草稿与门禁**: Fork 选中消息的附件必须连同文本进入新 Task 的可编辑 composer，允许移除并且只发送一次；不得把附件只留在不可见 metadata 或 native fork 边界之外。“完整复制”必须以最后一个 `completed` native turn 执行 `thread/fork`，复制全部展示日志且不生成 seed 草稿，禁止复制或改名 rollout 文件。`thread/read/start/fork/delete` 全部通过 `codex_home_app_server_guard`，与普通 exec generation、临时 exec 和账号维护互斥。
- **Codex 模型**: GPT-5.6 是**三个模型**（`gpt-5.6-sol` 旗舰 / `gpt-5.6-terra` 均衡 / `gpt-5.6-luna` 快速），**无裸 `gpt-5.6` ID**（Codex 服务端模型列表 `~/.codex/models_cache.json` 实证）。effort 按模型区分：sol/terra 支持到 `ultra`、luna 到 `max`、gpt-5.5 及更早只到 `xhigh`——集中在 `backend/services/codex_models.py`（`CODEX_MODEL_EFFORTS` + `clamp_codex_effort`，不支持的高档位向下夹而非静默丢弃），经 `/api/system/config` 的 `codex_model_efforts` 下发前端按所选模型过滤档位
- **Codex Fast 模式**: `Task.codex_service_tier` 是非空任务配置，只允许 `default`（Standard，默认）/`priority`（Fast）；Fast 是同一模型的 service tier，**不是换模型，也不改变 effort**。静态能力门控支持 `gpt-5.6-sol/terra/luna`、`gpt-5.5`、`gpt-5.4`，实时执行还必须由当前账号的 `model/list` 广告 `priority`。由于 Codex app-server 0.144.6 不把上游 `response.service_tier` 暴露到 RPC，CCM 为每个 `CODEX_HOME` 启动 loopback-only `codex_tier_proxy.py`，将 app-server 的 Responses HTTP 路径定向经过代理：Fast 请求只有在首个 `response.created.response.service_tier` **精确等于 `priority`** 后才释放成功 SSE、写入 `actual_service_tier_verified` 日志/事件；缺字段、不一致、非 2xx、未知 thread/turn lineage、隐藏 endpoint 或代理不可用都明确失败。Fast 禁止回退 `codex exec`，绝不能以 Standard 静默执行。Standard 显式清除 sticky tier（app-server 传 `null`，exec 传 `service_tier="default"`）；代理可验证请求没有携带 priority，并兼容上游不回 informational tier。已加载 thread 切换档位走 `thread/settings/update` 并等待 `thread/settings/updated`，整个 root lineage 有活跃请求时禁止切换。CodexPool 的选号、重试、换号与 Worker 迁移都携带 tier；ApexRouter 保存并校验模型目录的 `service_tiers`，未广告 `priority` 的 API 账号不参与 Fast 路由。Fast Goal evaluator 必须在主回合前证明与任务使用相同模型，并走同一 priority/实际 tier 证明链路；Standard Goal evaluator 显式使用 Standard。Distill 仍是显式 Standard auxiliary，Fast Task 请求 Distill 时必须在执行前返回 409，不能隐藏降级
- **Fast/路由配置原子性**: 本机 Task 的 provider/model/tier 只允许在 pending 或安全终态/plan_review 通过 Task 写屏障更新，active generation 与运行中子 Agent 明确 409；已有 Codex session 的 tuple 变化还必须用 exact thread reservation 跨 DB commit 证明 Goal 已 complete/不存在且 `thread/read` 明确 idle，多 rollout 无权威绑定或任意读回不确定均拒绝。Manager→Worker 使用 durable stage marker → Manager exact CAS → Worker ack 两阶段协议：stage 只落 candidate、不改 live tuple；marker 存在时 dequeue、retry、chat、plan、fresh/queued/sub-agent launch 全部 fail closed；Manager commit 后 ACK 不可确认仍返回 Manager 权威配置，Worker marker 持续阻断执行并由下一次 readback 收敛。Worker stage 必须先证明 pre-owner launch、Instance reverse owner 与子 Agent generation 均已静止；崩溃恢复不得把带 marker 的 active row 放回 pending，而应保留 marker 并转 safe terminal 供 ack/reconcile
- **Codex Pool 自动登录、额度与运行时轮换**: `scripts/codex_login.py` / `backend/api/codex_pool.py` 支持 `171mail` 与通用 `mailcatcher` 来源（后者由查询 token 定位账号，可用于已在 MailCatcher 配好的 163/mail.com/Onet/Gazeta 等邮箱；旧 `mailcom`/`onet`/`gazeta` provider 值继续兼容）；显式选择来源优先，留空时自动把 `163.com` / `mail.com` / `onet.pl` / `gazeta.pl` 路由到 MailCatcher，其他后缀回退 `171mail`。邮箱 token 与 OpenAI 密码均可选：两者都不填时先尝试 OpenAI 的 email-code 入口并等待管理员人工输入 6 位码；若账号只提供密码登录则明确失败并提示补密码重试。有密码时先走密码登录；若 OpenAI 仍要求 OTP，有可用 token 就自动取码，无 token 或自动取码失败则保持**同一个** `codex login` 进程、OAuth/PKCE 状态和浏览器页面，向前端发布 `awaiting_otp` challenge（10 分钟）供管理员人工填写 6 位码，再通过 stdin 继续原流程，绝不能另起登录进程；OTP 只经内存/管道传递，不写文件、不进日志，最多尝试 3 次，输入框仍可见不等于失败，只有页面明确报错才开启新 challenge。CCM 启动 wrapper 时 password/token 也只作为 stdin 首条初始化消息传入（绝不放 argv），wrapper 再启动 `codex login` 时必须用 `stdin=DEVNULL`，避免子 CLI 抢读人工 OTP 管道；登录浏览器的 Xvfb 必须用私有 `XAUTHORITY` cookie（禁止 `-ac`），watcher 异常/取消必须先杀掉仍存活的登录子进程再释放 maintenance。长期凭据以 `{token, provider, password}` 原子写入账号池配置同目录的 `email_tokens.json`（目录 `0700`、文件从首字节起即 `0600`，前端必须明确告知会持久保存；邮箱-only 模式只保存空凭据元数据，重新登录时再次人工收码）；每个账号在 `accounts.json` 配独立 `codex_home`/`auth.json`，前端可查看目录并设置 preferred。前端打开 Codex 号池或管理员刷新时按账号 `CODEX_HOME` 调 app-server `account/rateLimits/read` 获取实时值；实时查询失败不得把可能来自迁移 session 的 rollout 冒充当前额度，而要明确显示无法确认。后台 turn 收尾仍直接读 rollout，避免每轮拉起全部账号进程；rollout 候选按 `rate_limits` 事件时间而不是文件 mtime 选最新，且其并发结果在实时缓存 TTL 内不得反向覆盖实时值。每次成功 turn 后检查主/副窗口（5 小时/周额度），任一达到 90% 时才尝试迁移到**已登录**且额度未达阈值或额度未知的可用账号，找不到替代号或迁移/重绑/绑定持久化失败就继续当前号且不冷却。切换成功后旧号按所有超阈值窗口中较晚的 reset 时间冷却，避免被新任务立即重新选中；Goal/loop/plan turn 的 consumer 若在成功收尾时主动换号，后续 evaluator/迭代必须读取 InstanceManager 更新后的 home，不能继续使用 launch 时的旧 home。绑定持久化失败要把 app-server owner 回滚，回滚仍失败则清除该 idle thread 的内存 owner，让 DB 旧绑定在下次冷路由时重新成为事实源。新任务由 `CodexPool.select()` 选号，`Task.metadata_["codex_account_id"]` 持久绑定；usage-limit/auth 失败时冷却旧号、选新号并以 `codex_session_migration.py` 独立复制 rollout（不 hardlink、不复制 auth、不删源），随后重绑同一 thread。迁移保留源副本，因此多副本必须以 Task 绑定消歧；无绑定且多副本时明确失败。A→B→A 会保留旧目标备份并原子替换较长前缀；内容分叉拒绝覆盖。新增/重新登录/删除账号会跨整个操作持有全局登录锁与 home maintenance，活跃 turn 或其他账号操作返回 409，防止替换 `auth.json` / 修改账号池时并发丢记录；删除先提交隐藏 disabled tombstone，再清除不再共享的邮箱/OpenAI 凭据，并把服务用户主目录内受管 CODEX_HOME 清到只剩 sessions 与 `0600` tombstone（history/log/state/config/插件等均删除），供旧 task 按绑定迁移恢复；不在服务用户主目录的自定义 home 一律拒绝自动递归清理。用户界面不再列出 retired 账号。后续新增账号只按全局最小编号复用已完成清理的 retired 槽位（无 cleanup/recovery/journal，受管 home 仅含 sessions 与匹配的 `0600` marker）；异常槽位 fail-closed 跳过，现有 active 账号不自动改号。复用保留 sessions，并以 `quota_valid_after` 排除旧身份 rollout 额度。首次登录失败的 orphan 目录只有在顶层仅含普通 `models_cache.json`、`log/`、`tmp/`（后二者内部也只能有普通文件/目录，无 symlink、挂载点或特殊文件）时才可清空并复用原账号编号；auth/session/config/state 等其他内容仍 fail-closed 跳过。
- **Codex 登录/删除故障原子性**: 登录由父 API 在 spawn 前持久化事务 journal：账号池配置同目录的 `login-transactions/` 必须为 `0700`，每个 `0600` journal 以 base64 保存旧 `auth.json`、`accounts.json`（add/relogin）及 `email_tokens.json`（add）的“存在/不存在 + 原始 bytes”快照；journal 写入并 fsync 后 wrapper 才能启动。wrapper `rc=0` 后状态先进入 `finalizing`，父进程必须校验并 fsync 当前 regular/non-symlink `auth.json`（add 还含 `accounts.json`/`email_tokens.json`）及各 parent directory，最后删除并 fsync journal 才可发布 `success`，该删除是唯一 commit point。任何非成功、commit barrier 失败、启动失败或 watcher 取消都先确认整个 wrapper process group 已终止，再按 journal 绑定的**精确 canonical pool path**原子 rollback 后释放 home maintenance/login lock；清理运行在 shield task，外层 cancellation 必须延后到 reap+rollback 完成。服务启动无论 `CODEX_POOL_ENABLED` 是否开启，都必须在任何 Codex home 可用前扫描遗留 journal 幂等 rollback，因此 systemd 同时杀掉 watcher、wrapper 先删临时 auth backup、或 add 在 `email_tokens.json`/`accounts.json` 两次写之间被 SIGKILL 都不会留下半提交。rollback 自身失败时把 regular `auth.json` 移出标准路径；live/dangling symlink 只能 unlink、绝不能 chmod/follow 外部 target；随后写 recovery marker 并禁用对应 pool record。成功隔离后可释放，隔离也失败则保留该 home maintenance 并显式暴露 `recovery_failed`（只释放全局锁让其他账号可用），绝不能让 partial auth 被新 turn 读取。journal/快照/transaction 目录遇到 symlink 或路径不匹配一律 fail closed。Xauthority cookie 只经 xauth stdin 传递，不能出现在 argv。删除先写 `cleanup_pending` retired tombstone（临时保留 email 供清凭据），清理失败允许再次 DELETE 幂等重试；全部清完才清空 email/pending，保留无凭据 tombstone。
- **账号登录浏览器运行时**: Claude/Codex Pool 必须共用 `backend/services/login_runtime.py` 的进程内登录锁与 Xvfb manager，禁止各模块缓存/`pkill` Xvfb 或 Chrome。Xvfb 使用私有 Xauthority、跨进程 display 文件锁与 `xdpyinfo` 就绪探测；只可终止 manager 自己持有的子进程。多套 CCM 同机部署必须分别配置不同的 `CCM_XVFB_DISPLAY`（例如生产 `:99`、测试 `:100`）；Chrome CDP 由每次登录的独立 profile 通过 `DevToolsActivePort` 动态分配和绑定，不再配置或复用固定端口。浏览器 profile 统一写 `CCM_LOGIN_TMPDIR` 的磁盘目录；启动 Chrome 前按 `CCM_LOGIN_MIN_AVAILABLE_MB`/`CCM_LOGIN_MIN_TEMP_FREE_MB` fail-fast，Codex authorize 导航只允许一次诊断性重试。
- **Extended Thinking 预算**: Instance 上的 `thinking_budget` 字段 → 子进程 `MAX_THINKING_TOKENS` env var；NULL = 用 CLI 默认
- **Thinking 解析**: stream_parser 兼容多种字段名（`thinking` / `text` / 嵌套 content blocks）；加密 thinking 显示为 `[encrypted thinking ...]` 标记
- **上下文自动压缩**: 会话 context 利用率达阈值 → dispatcher 自动摘要换新 session，并写入/广播 system_event 在聊天中提示用户。阈值优先级：GlobalSettings.context_compact_threshold（前端 Header 齿轮「压缩阈值」可改，PUT /api/settings/runtime）→ settings.context_compact_threshold（env 默认 0.80）。Codex app-server 的 `thread/tokenUsage/updated` 必须保留 `modelContextWindow` 与最新请求的 `totalTokens/reasoningOutputTokens`，按 `totalTokens - reasoningOutputTokens` 计算当前上下文，禁止用累计 thread/turn token 当上下文（会出现 119%/553% 假利用率）；只有协议没给窗口时才回退 `CODEX_CONTEXT_WINDOWS`。失败恢复必须同时识别人类错误文案和 app-server `codexErrorInfo=contextWindowExceeded`，而且 fresh/mode 与 chat 两条链路都要摘要、清旧 session、携原消息自动续跑；结构化错误码常只在 raw error envelope，失败分类不得只读 assistant 普通消息。压缩后的默认信息优先级为“当前消息 > 近期对话（越靠后越新）> 原始任务背景”；若当前消息执行中有 live inject 补充/纠正，则该更新高于基础当前消息。当前消息必须按 `source_log_id` 从摘要精确排除，近期每阶段保留最后结论而非第一条进度话术，原始描述放在摘要末尾并明确标为最低优先级。压缩、空回复和路由重试必须继续透传不可变 `current_message`、`source_log_id`、原始 `queue_timestamp` 及一次性 model/skills，避免摘要嵌套或后发消息超车后突然重答旧问题。**别设回 0.9**：超大 context 请求在服务端易挂起（2026-07-08 task 22/27 连环 stall 均发生在 ~90% 区间）
- **用户消息发送者前缀**: `[用户名] ` 只属于聊天展示，绝不能进入 Claude/Codex prompt。`backend/api/chat.py` 对本地/Worker 消息分开 `model_message`（原文）与 `display_content`（带前缀），Shared owner 端同样只 enqueue 原文；所有真实用户消息的 `LogEntry.raw_json` 必须保存 `raw_content`（有身份时再带 `sender_name`），供上下文压缩、Distill、历史 API 与前端复制精确取原文（禁止用通用正则处理新消息，否则会误删用户真实的 `[BUG]`/`[TODO]`；正则仅兼容无元数据的旧日志）。PTY/Codex live-turn inject 本来就直传原文，也须持久化/广播 `raw_content`
- **Workflows 开关**: Task.enable_workflows（默认 False）→ CLI `--disallowedTools Workflow`；关闭时 Workflow 工具不可用，节省 token
- **Codex Skill context（2026-07-29）**: 上方 Codex provider 对等逻辑中④“Skill 模板仅 Claude”、⑧“Codex Monitor 创建显式 400”和同段“Monitor 仍关闭”均为旧 staged 状态，现已由统一 `skill_context.py` 与下方 Monitor capability 约定取代；Codex 普通/User Skills 已开放，本地、非共享、非 Worker 管理的 Codex Task 在主 MCP capability 已确认时也开放 Monitor
- **Skills 系统**: `Task.enabled_skills` 控制普通 Skill，`selected_user_skills` 控制 User Skill。启用只表示能力可发现；只有消息开头真实解析到 `$command` 时才注入显式调用指令。initial/queued 两路都必须在 Task 写屏障内以 generation token 临时叠加 required skills，turn 结束只清理仍属于该 token 的 command keys，显式用户保存会清 marker。`skill_context.py` 每轮从同一 Task generation 生成唯一 L0 目录：Claude direct 用一个 append-system-prompt 文件，PTY/Codex exec 用带界限的 prompt context，Codex app-server 必须把同一 bounded context 前缀写入 schema-backed `turn/start.input[].text`；Codex 0.144.6 会静默丢弃未知 `TurnStartParams` 字段，禁止用自创 context 字段假装注入成功。普通 Skill 正文只允许 `ccm_read_skill` 读取 Task 已启用项或 `always` 项，User Skill 正文只由选中 ID 的 `ccm_read_user_skill` 按需读取。Worker 创建/迁移携带 Manager User Skill snapshot，续聊前同步最新选择，不能假设 Worker 有相同的本地 User Skill 表。Codex Monitor 的 Skill 目录与三个主 MCP 工具只对已确认的本地范围开放；Worker、Shared、带 Worker 管理标记/快照的副本及 capability 未知状态一律裁掉。主任务 MCP kill switch 关闭时即使遗留 Task 状态还启用了普通/Monitor Skill，controller 也只能读取/操作已选中的 Sub-Agent。任务级 Distill 仍按 `Task.provider` 分流，Codex 使用绑定/健康账号运行独立只读 `codex exec --ephemeral`，不得复用或改写原 task thread/账号绑定
- **Skill 命令能力门禁**: `$command` 的 `required_skills` 必须与显式勾选 Skill 共用 provider capability / 主 MCP kill switch 校验；新建 Task、更新初始 description 和 follow-up chat 都必须在 Task/LogEntry 持久化及消息入队前拒绝不兼容命令，不能让 `$monitor` 等命令绕过 Codex 能力边界。Worker chat 必须在 task operation lock 内按刷新后的 Manager provider 校验，Shared chat 必须按刷新后的 shadow provider 校验；两者都要早于 Manager 日志、广播、附件同步和远端请求，同时保留远端二次校验并转发原始消息。`PUT task` 同时切换 Worker 与 provider/Skills 时必须先校验完整有效配置，再允许 `TaskMigrator` 产生任何本机或远端变更
- **Worker Skill 执行准入**: 已转发 Task 的 ordinary/User Skill 保存与 Retry/chat 必须共用 task operation lock；Plan Approve 自独立 Plan Task 起是纯控制面完成操作，不再同步执行 Skills 或进入 pending。首次调度时，pending Skill 保存与 Dispatcher claim 也必须共用该锁；claim 胜出后活跃 generation 的 Skill 编辑返回 409，WorkerProxy 在远端创建前重新读取 Manager 权威行并校验 generation，不能转发 claim 前缓存的旧配置。Manager 是最终配置权威；每个真正开始新一轮执行的入口都必须先同步并读回确认 `enabled_skills`、`selected_user_skills`、User Skill snapshots 及当前 Worker generation，确认缺失、陈旧或不一致时 fail closed，绝不能先把远端状态切到 pending/executable。Manager/Worker 的 `instance_id` 属于不同数据库，禁止跨节点按数值比较；远端 generation 由全局 task id、协调后的 inert status 与单调 `retry_count` 证明
- **Monitor Skill**: 后台监控子 session，主 Agent 通过 MCP 工具（create_monitor / check_monitors / stop_monitor）创建和管理。CCM Monitor（`agent_type=monitor, source=ccm`）采用**数据库驱动的定时单轮**：`next_check_at` 到期后 Dispatcher 用 CAS 领取并递增 `turn_generation`，按冻结的 provider 启动一个短生命周期检查回合；该回合必须携带完全相同的 generation，通过 `report_status` 或 `mark_complete` 恰好消费一次。回调成功后才安排下一次 `next_check_at`，重复、过期或漏 token 的回调返回 409，进程退出但未回调按有界指数退避重试，连续 3 次失败后终止。睡眠中的 Monitor 没有常驻子进程，不阻塞更新；只有 `active_turn_generation` 对应的已领取/运行回合是运行证据。重启仅恢复仍为 running、父 Task 存在且没有 active generation 的安全 schedule；遗留 active generation 无法证明是否已产生副作用，必须 fail closed。**PR7B1 内部 Codex 运行时路径**使用独立且持久化的 `thread_id + canonical CODEX_HOME + account_id`，第一次领取时冻结 model/effort/tier/cwd；只走 app-server required callback MCP，不允许 exec fallback，并以 read-only sandbox、禁用项目配置和 autonomous features 启动。新 thread 必须先提交持久身份、再发布精确 turn adapter，最后才允许 `turn/start`。Codex 对已加载 thread 的 `thread/resume` 不会重建 MCP client，因此每个已证明 reaped 且仍为 running 的回合后，必须在 idle generation fence、thread reservation 和 home maintenance guard 下 settle `thread/archive → thread/unarchive`：保持同一 thread/history，同时让下一轮按新 generation 重建 callback MCP；`config/mcpServer/reload`、`thread/unsubscribe` 或杀共享 app-server 都不能替代这一步，回收失败必须立即 terminal fail closed 并清理精确 thread。终态 DB 提交后才 `thread/delete`，删除失败保留 cleanup evidence 供启动重试，同时阻止账号和父 Task 被删除。服务正常关闭只 interrupt 精确 turn、释放 generation 并保留 thread 供下次恢复，绝不能杀共享 app-server。每 task 最多 5 个并发 Monitor。**PR7B2 capability 收口**只在 `CODEX_MAIN_MCP_ENABLED=true`、`worker_id/shared_from_id` 均为空且 metadata 不含 Worker 管理标记或 User Skill snapshot 时开放 Codex Monitor；Task 创建/更新/迁移导入、chat `$monitor`、公开 Monitor API、MCP discovery/enable 与实际 launch 共用该判定并在路由写屏障后复核。Worker、Shared、迁移副本、kill switch 关闭或 capability 未知时均在任何持久化、代理或进程启动前 fail closed。
- **子 Agent 架构**: 子 agent 是分类别的一等概念，统一存 `sub_agent_sessions`（`agent_type` 区分类别，`source` 区分启动方）。Monitor（agent_type=monitor, source=ccm）是第一个类别；PTY 模式下模型原生子 agent 自动镜像进来：`native-agent`（Agent/Task 工具）、`native-monitor`（内置 Monitor 工具），由 claude_pty 从 JSONL + subagents/ 目录观测，经 `_upsert_native_sub_agent` 入库并广播 `sub_agent_*` 事件。CCM Monitor 使用上述可恢复的 scheduled-turn 生命周期；其他一次性 CCM Sub-Agent 启动独立 provider turn，通过 MCP tools → HTTP API → DB + WebSocket 回报后完成/停止并清理。**native 子 agent 完成的唤醒只靠 harness task-notification**（唤醒后产出经 FullMirror 镜像进聊天）；**严禁在 subagent_done 里 enqueue 唤醒 prompt**——它必然和通知 turn 赛跑，输了被 CLI 吸收成 mid-turn steering（无独立回显）→ send_prompt 回显锁定永不成立 → consumer 永挂 → 队列冻结 → 7200s 超时杀掉仍在干活的进程（07-15 task 32/33 事故；journal 里 7 月共 18 次无声超时杀，普通用户消息撞 turn 边界同样能触发，根治在 PTY 上游）。-p 模式的退出补唤醒（`monitor:native-exit-resume`、`monitor:complete`）不在此列，不能动
- **PTY 权限透传**: BridgeHub 的 permission handler 由 instance_manager 注册（`_on_pty_permission_request`，bridge HTTP 线程 → `_loop` 调度）；卡片事件 `permission_request`/`permission_resolved` 走 task WS 频道，回包端点 `POST /api/tasks/{id}/permissions/{request_id}`；CC 侧 channel server 最多阻塞 120s，超时默认 deny
- **PTY turn 对齐**: claude_pty 的 send_prompt 以"本次 prompt 的 user 回显"为 turn 起点，之前的积压事件标 `orphan` 上报；turn 间由 Session 空闲 watcher 持续消费 harness 自主 turn（带 `autonomous` 标记）。修复 task 87 的回复错位事故（详见 PROGRESS.md）
- **Autonomous turn 全量镜像**: 上游 adapter 的 chat on_exit 会把 `on_autonomous_event` 降级成 subagent-only（防重放旧 prompt），导致后台监视器回报的自主 turn 在聊天里不可见（task 27 事故）。`FullMirrorCCMBackend`（`backend/services/pty_full_mirror.py`，set_pty_mode 接线）本地接管 on_exit、保持全量 `_process_event`，并以 consumer identity + Task retry/owner + 每 turn 唯一 `Instance.started_at` 做终态 CAS（PTY 热 session 多轮共用同一 PID，不能拿 PID 当 turn generation）；配套消毒在 `_process_event`：autonomous user-role 事件绝不入库为用户消息（`<task-notification>` 压成一行 system_event，channel 回显等直接丢弃）
- **Claude PTY 后台终态**: foreground `send_prompt` 返回时若 native Agent/Monitor、`Bash(run_in_background=true)` 或 autonomous tool turn 仍活跃，Task 保持 `executing`、Instance 保持 exact owner，`Task.pty_background_generation` 仅作为 Task/session epoch fence（API 暴露布尔 `background_active`）；FullMirror 用结构化 `backgroundTaskId` ↔ `<task-notification>` 精确跟踪 Bash，并等待 exact `turn_duration`、无 running native session 且无 pending tool 后，才一次性写 `completed`/释放 Instance。launch-time callback 在等 transition lock 前同步登记 handoff，begin/on_exit/finish/watchdog 共用同一 Task/session lock，旧 sentinel、双 arm、服务重启和复用 Instance 都不得越代。非 chat turn 在 `process.complete()` 释放普通 consumer maps 前必须保留绑定 record/process/session/retry/Instance.started_at 的 exact post-exit proof，直到 Dispatcher/Ralph 的 marker-aware 终态 CAS 或 callback 接管；stop/replacement 会同步失效 proof 与已登记 handoff。Ralph 只能采纳“其余 generation 字段不变”的 marker-only handoff，终态广播必须携带锁内真实 `background_active`。stop 可按 exact record/proof 立即接管 background waiter/session；删除、重试、迁移、PR supersede 与更新在 marker 清除前必须拒绝或 exact stop-first。仅已完成后才到达的真正 late autonomous turn 使用 detached marker，且绝不写 Instance；graceful shutdown 必须按 Task/session/token 精确停止 ownerless epoch，重启无法续接 marker epoch 时父 Task 与 running native child 一律 fail closed。
- **ask_user（拦截内置 AskUserQuestion）**: 内置 `AskUserQuestion` 在 headless/PTY 下无人应答会卡住。CCM 在 `instance_manager.launch()`（`-p` 与 PTY 的**统一入口**，分流之前）把一个 PreToolUse hook（matcher=`AskUserQuestion`）幂等合并进**本次使用的 `{config_dir}/settings.json`**（`ask_user_settings.ensure_ask_user_hook`，`config_dir` 空则落 `~/.claude`）——Claude 在 `--dangerously-skip-permissions` 下自动加载该文件、无审批弹窗。**为何走 settings 文件而非 CLI flag**：claude_pty 命令构建是固定字段不接受 `--settings`，且本仓库对 PTY 仓库只有 READ 权限无法 bump 依赖；两条链路都用 `CLAUDE_CONFIG_DIR`，故 settings 文件是唯一两路统吃的注入后门。hook 脚本 `backend/hooks/ask_user_hook.py`（纯 stdlib urllib、**fail-open**）阻塞式 `POST /api/ask-user/wait`：后端按 `session_id` 找 task → 登记 `asyncio.Future`（`ask_user_registry`）→ 广播 `ask_user_question` 卡片 → `await` 直到前端 `POST /api/tasks/{id}/ask-user/{request_id}` resolve 或 `ask_user_timeout`(默认 1800s) 超时。**答案回流机制**：hook 拿到答案后以 `permissionDecision=deny` + `permissionDecisionReason=<格式化答案>` 输出——deny 的 reason 会作为 `tool_result`(is_error) 原样喂回模型，模型据此当作"用户的回答"继续（已实测）。**hook 项必须带显式 `timeout` 字段（= ask_user_timeout+60）**：CLI 对 hook 命令默认 600s 就杀，hook 在 /wait 阻塞中途被杀等效 fail-open → 原生 AskUserQuestion 在 PTY 弹无人应答的交互框 → turn 永久冻死、卡片从前端消失（2026-07-17 task 32 事故；任务 28 的卡片 3m42s 被回答成功反证默认值是 600s 不是 60s）。**超时不放行**：timed_out → deny +「用户未回应，按你的判断继续」；只有 CCM 不可达 / 非托管 session / 异常才 fail-open 放行原生工具。整套照搬 PTY 权限透传范式（卡片 live-only + `system_event` 落库 + `GET /api/tasks/{id}/ask-user/pending` 重连回填）。**跨页面全局通知**：内联卡片只走 `task:{id}` 频道，用户不在对应 task 页面时提问会「消失」。故 `/ask-user/wait` 同时 ① 把 task 标 `has_unread=True`（任务列表亮未读点）② 向全局 `tasks` 频道广播 `ask_user_pending`/`ask_user_resolved`（带 `task_id`/`request_id`/`summary`）。前端 `AskUserNotifications`（App 顶层常驻）订阅 `tasks` 频道 + 刷新/重连时拉**全局** `GET /api/ask-user/pending`（`ask_user_registry.list_all()`），右下角弹可点击通知，点击跳 `#/tasks/chat/{id}`，答完/超时由 `ask_user_resolved` 自动消除。开关 `ask_user_enabled`（默认 True，关闭时 `ensure_*` 自动移除已注入的 hook 项）
- **MCP 架构**: `mcp_config.py` 先构造 provider-neutral `McpServerSpec`，再渲染为 Claude `mcpServers`、Codex app-server thread config 或 `codex exec -c` argv（纯内存，不写共享 `config.toml`）。MCP 子进程必须复用后端当前 `sys.executable`，禁止拼接仓库相对的 `.venv/bin/python3`：Windows 宿主挂载进 Linux 容器时该路径不存在，而当前解释器才是已证明装有同一依赖集的跨平台权威。Codex 主任务 MCP 默认启用，`CODEX_MAIN_MCP_ENABLED=false` 是紧急关闭开关。`InstanceManager` 每次 start/resume/重试/换号都从当前 Task generation 重建 required `ccm_skills` spec 与唯一 Skill context；app-server 和 exec 必须消费同一份输入。只有能证明 turn 尚未 admission 的失败才标为 `CodexRequiredMcpPreTurnError` 并回退一次到 MCP/context-equivalent exec，未知状态一律 fail closed。Sub-Agent 需要 live thread control，始终 app-server-only；Codex 主 server 仅为 capability 未确认或非本地范围裁掉 Monitor tools，已确认的本地范围声明完整三个工具。三个 FastMCP server 分别是 `ccm_skills_server.py`、`ccm_monitor_agent_server.py`、`ccm_sub_agent_server.py`
- **`/tmp` 压力保护**: `TmpSpaceManager` 在 lifespan 启动时检查一次，随后每 3 小时检查；容量字节（按服务用户实际可用的 `f_bavail`）或 inode 任一达到 `TMP_CLEANUP_USAGE_THRESHOLD`（默认 80%）才扫描，触发后按大到小清完全部合格候选，不设目标线。宿主删除范围只能是唯一/不可复用的 CCM 普通文件命名白名单，且必须同 uid/device、超过 6 小时；宿主目录一律不递归删除。跨进程 cleaner 用 `~/.cache/ccm/tmp-pressure-cleanup.lock` 串行，rename 前后复核 inode/type。检查只合并同一个在途 single-flight、不缓存已完成低水位，取消/关机必须等待删除线程落稳。无法独立证明空闲的 session/workspace 迁移 staging、未知文件、固定名 MCP 配置、可能合法运行超过 6 小时的 Monitor 日志、symlink/特殊文件、X11/Xvfb、登录资料和 `/tmp/ccm-update-*` 永不因压力扩大清理；安全候选不足只告警并在下个周期重试，不阻塞 Agent。共享 Docker 的独立 2GB `/tmp` 用同一触发线，但因它是单 Project 可丢弃 tmpfs，Agent supervisor 在 fork 前至后代完整回收期间持 root-owned inode 的 SH lease；新建/配置变更后重建的容器用 Docker `--init` 回收孤儿进程，现有容器不为此强制重建；仅取得 EX 且 `/proc` 证明容器无 Agent 进程时才整棵清空，busy/不可核实/清后仍达触发线均 fail closed，PTY 禁止回退宿主。远程 Worker 由自身升级后的 CCM 看门狗与容器门禁负责
- **更新脚本可信快照**: 当前 Python 进程匹配的 `update_migrate.sh` 不再放公共 `/tmp`，而是冻结到 `CCM_UPDATE_RUNTIME_DIR`（默认 `~/.cache/ccm/update-runtime`）下的 0700 独立目录；owner 标记绑定 uid/port/boot ID/PID start tick/目录 inode/脚本 SHA-256，读取与复制必须经 O_NOFOLLOW + inode/hash 复核。正常 lifespan 只删除本进程的精确平面目录，SIGKILL 遗留由下次启动仅在 boot/PID 身份可证明死亡时回收，unknown/标记损坏/额外文件/symlink 一律保留。旧 `/tmp/ccm-update-runtime-*` 只在其 PID 明确不存在且目录内容严格受限时迁移清理；单次 `ccm-update-run-*` 仍归外部 worker/部署租约管理，通用压力 cleaner 不得触碰
- **环境变量清理**: 生成子进程前必须 unset `CLAUDECODE` / `CLAUDE_CODE`，避免嵌套检测
- **实例进程生命周期与领取原子性**: 同一 Instance 的 launch/stop/delete 由 per-instance lifecycle lock 串行；API 的 `is_running` 只作提示，真正准入以锁内 process + output-consumer 代次为准。stop intent 必须引用计数，并由 `launch()` 在同一 lifecycle lock 内检查，避免并发 stale stop 提前拆栅栏或 terminal consumer 在迁移/退避后自启动 replacement。`Task.instance_id` 是当前领取 claim，不是可长期信任的停止目标；所有完成/失败/重试/取消及 stop 都必须同时 CAS task status + instance owner + retry generation，管理端 stop 还须提交其观察到的 `expected_task_id`，防止槽位复用后误杀新任务。spawn 到 reverse owner commit 的窗口必须登记 task-scoped `_launch_reservations`；cancel/stop 在 Task 终态提交后等待同一 lifecycle barrier，未证明 aborted generation 已 reap 就返回 409。输出 consumer 的错误按 `(instance_id, process identity)` 保存，dispatcher/Ralph 必须把 launch 后拿到的 exact process 传给 `wait_for_output_consumer`，旧代错误绝不能被新 turn 消费。launch 在产生文件/进程前先验证 Instance 行仍存在，创建后到 DB commit、consumer 注册之间的任何失败或取消都要 cancellation-shielded reap/abort；Codex `turn/start` 取消或超时且 interrupt 未确认时必须关闭该账号 transport 并在 shutdown 失败时保持 draining fail-closed。重启恢复只把明确 dead/no-PID claim 放回 pending；unknown/live PID 必须保留 PID、started_at 与双向 owner 证据并 fail-closed，cleanup/retry 不得丢证据。所有 Task/Instance 双行事务统一 **Task→Instance** 锁序，终态广播需再锁 exact resulting Task generation，禁止旧事件越过 replacement claim。PTY 的 container binary 临时 build_config 覆盖是 backend 全局可见状态，所有 PTY launch 必须经同一个锁。
- **Claude PTY 终态所有权**: 活跃 PTY 的 stop 与 `FullMirror.on_exit` 必须按 exact `_OutputConsumerRecord.pty_terminal_owner` 在首次 await 前原子交接。stop 赢时 consumer 只能做 identity-guarded 内存映射清理，不得再获取 lifecycle lock；consumer 赢时 stop 持有 stop intent、释放 lifecycle lock 后等待 consumer。启动失败回滚也须在打开 metadata barrier 前让 stop 取得所有权；consumer 异常结束或锁外取消并确认 done 后，stop 才能接管其遗留 claim。禁止 stop 持 lifecycle lock 等待一个会反向获取同锁的 consumer。
- **维护阻断核对**: 更新弹窗的「重新核对运行状态」只调用 Dispatcher 的 ownership reconciliation，绝不能由 UpdateService 自行解释 PID。只有唯一、双向一致且 PID 明确死亡的 claim 可回 pending；多 Instance owner、forward/reverse 不一致一律 fail closed，unknown/live PID 保留为阻断证据。核对必须把 `_launching_instances`、live lifecycle/process/consumer 与 CCM auxiliary process maps 当作当前进程所有权；remote shared shadow 不得被本机改写，人工核对不得执行 startup-only 的 auxiliary stale sweep。
- **Dispatcher instance 预留原子性**: 所有本地分配路径必须经 `_instance_admission_lock` 在同一临界区完成 idle 选择与 owner reservation；`_running_tasks` 构造 Instance SQL 过滤时只取整数本地 ID。底层若仍报 `InstanceAlreadyRunningError`，必须重排同一个 `QueuedMessage`，绝不能丢消息。
- **停止与关机语义**: 普通 CLI 以独立 POSIX process group 启动；停止顺序为整组 SIGINT → 等 10s → SIGTERM → 等 5s → SIGKILL，stderr 必须并发排空且 inherited-fd 收尾有界。Monitor/sub-agent/GoalEvaluator 也必须 shield subprocess spawn，并在父进程正常退出后检查和清理同组 descendants；无法证明整组退出时保留 generation evidence，GoalEvaluator 的全局 exact-handle registry（含 task_id，供删除栅栏）还须由 shutdown 再次回收。Dispatcher/Ralph/queue/aux 的 cancel-await 全部有界，超时不得 pop 精确 task/process/reservation 证据。Dispatcher `stop()` 只暂停新领取，运行中 turn 继续；应用退出必须先 `dispatcher.shutdown()` 关闭 fresh/chat/aux admission、逐项收敛 queue/lifecycle 并继续回收精确 generation，再关闭 PTY/Codex transport；首轮因 shielded cleanup 暂时失败时 transport teardown 后幂等重试 dispatcher shutdown，第二轮仍失败才向 lifespan 显式抛错。DB-fenced stop 失败时必须用已快照的 exact process fallback，不能只记日志。所有对外 stop 的 terminal consumer 等待必须有界，超时后只取消/回收精确 record，失败则保留 DB owner fail-closed 并显式报错。
- **进程组信号安全**: 宿主侧所有 `os.killpg` 调用必须先经 `process_safety.require_safe_process_group_id` 校验为严格 `int` 且 `> 1`；身份非法时必须 fail closed 并保留 generation evidence，绝不能尝试兜底发信号。测试禁止用 `pid=1` 充当子进程，也必须 mock 宿主信号调用；含真实进程生命周期的完整测试只能在带 init/reaper 的隔离容器中运行，不能直接跑在与生产服务共用用户的宿主上。
- **注册与邮件凭据**: 新部署绝不能创建固定用户名/口令的管理员；首个 active 注册用户通过 DB singleton row 锁串行成为唯一 `super_admin`，若配置了 `AUTH_TOKEN` 则必须先提交匹配的 bootstrap token，错误 token 不得消耗邮箱验证码。legacy `AUTH_TOKEN` 只可绑定 active admin；没有 active admin 时保留 `user_id=None` 的 token-super_admin 语义。SMTP 账号、密码和 From 只允许由 `SMTP_*` 环境变量提供，仓库与默认配置不得包含共享凭据；未配置时 fail closed，验证码状态须有线程锁、过期回收、邮箱/IP 窗口限流、总容量与 SMTP 并发上限。
- **Discussion 子进程**: Discussion agent 与 facilitator 都以独立 POSIX process group 启动，stderr 必须从 spawn 起并发排空；同一 agent 的启动由 per-agent lock + DB claim CAS 防双 launch，spawn 失败回滚 claim。consumer 取消和 lifespan shutdown 都必须走有界 SIGINT→SIGTERM→SIGKILL，并在 leader 已退出时继续证明/清理同组 descendants；无法证明整组退出就保留 evidence，不能吞掉 cancellation 后无限 `process.wait()`。
- **Per-task 消息队列**: chat/monitor 的后续消息走 dispatcher 的 per-task 队列（`_task_queues`），由**单个** consumer（`_task_queue_consumer`）串行 `--resume`，保证同一 session 不被并发 resume。busy 判断须同时匹配 Task/Instance owner、exact output-consumer record 与 fresh lifecycle；等待后必须 refresh Task，recovery/launch claim CAS 原 status+retry_count+instance/session generation 且要求 `worker_id/shared_from_id IS NULL`，失败重排同一消息；任务已迁到 Worker/共享节点时禁止本机 launch 并向聊天写入明确未投递提示。Pause→Start 的 stale cleanup 与 queued Phase 1 共用 admission gate，保证 ownership snapshot 后不会出现不可见新进程。`_ensure_queue_worker` 的 ">stuck 阈值 cancel+respawn" 看门狗（`QUEUE_STUCK_THRESHOLD`）只兜底真正卡死的 consumer：consumer 全程跑一个 `_queue_heartbeat`，长 turn（`_wait_process` 等十几分钟）和 idle 等待都持续刷新 activity，故不会被误判。consumer 退出时**只在 `_task_queue_workers[task_id]` 仍指向自己时才 pop**，否则会抹掉 respawn 出来的新 consumer 登记、让下次 enqueue 再起一个 → 双 consumer 并发 resume（task #728 事故，详见 PROGRESS.md）
- **Claude Pool**: 多账号自动切换（`backend/services/claude_pool.py`，`POOL_ENABLED=true` + `~/.claude-pool/accounts.json` 启用）。进程失败后用窄正则检测限速/认证失败 → 标记冷却（限速 5min，认证失败永久直到手动清除）→ `select` 换号（`validate=True` 会用 `claude -p` 探测，必须经 `select_async` 走线程避免阻塞事件循环）→ `migrate_session` 硬链接 session JSONL 到新账号 config_dir 实现 `--resume` 续上下文。**注意 `migrate_session` 参数是 keyword-only，必须用关键字调用**；session 实际所在目录用 `locate_session_config_dir` 查找，不要假设在 env `CLAUDE_CONFIG_DIR` 下。**找 session JSONL 一律用 `projects/*/{sid}.jsonl` 通配，绝不按 DB 里的 `last_cwd` 字面编码拼路径**——符号链接会让落盘编码（CLI 取 `os.getcwd()` realpath）与存库路径不一致（`_find_session_jsonl`/`_clone_session`，task #725）。chat resume 前 dispatcher 先探测 session 在不在磁盘，不在就走恢复（clone→摘要），让第一条消息即可自救而非被牺牲。**resume 选号统一走 `GlobalDispatcher._resolve_resume_config_dir(sid)`，绝不在 resume 热路径做 `claude -p` 探测**：先 `locate_session_config_dir(sid)` 找 session 所在号，**该号没在冷却中（`pool.is_in_cooldown` 查内存 `_cooldowns`，零子进程）就原样复用**——不探测、不迁移、不让 config_dir 漂移，从而保住 PTY 热 session 复用（漂移会逼 PTY 冷重启吃满 8s `startup_wait`）；只有所在号缺失或正在冷却时，才 `select(validate=False)`（冷却感知、便宜）挑健康号并 `migrate_session` 迁入。**砍掉 `validate=True` 探测**：它每条消息起一个 `claude -p "reply ok only"` 完整 API 往返（2–8s，最长 30s）才开始真正 resume，是「回复慢」头号元凶；而限速账号早被 `_cooldowns` 免费排除，真撞限速有反应式轮换 `_check_rate_limit_and_rotate` 兜底。**号池耗尽（select 返回 None）时回退到 `locate_session_config_dir(sid)`——session 真正所在的号，而不是放任 `config_dir=None` 让子进程继承 systemd env 里写死的 `CLAUDE_CONFIG_DIR`**（那个号没存过该 session → `claude --resume` 秒挂 `No conversation found`、丢 session；task #734/#740 事故）。限速是可恢复态，绝不能升级成丢 session 的硬失败。**主动额度换号（`_try_proactive_pool_switch`）只在 `rate_limit_event` 真·临界时评估**：CLI 几乎每个 turn 都吐一条状态 ping，`status="allowed"` 是健康，绝不冷却；`allowed_warning` 仅在 `rateLimitType` 为 `five_hour` 或 `seven_day` 且利用率 ≥0.9 时触发。触发后先通过 OAuth usage API 强制刷新全池 5h/7d 百分比，只选择两窗口均低于 90% 或额度未知且可用的替代号，并排除 `no_credentials` / `token_expired` / 401 / 403 等确定认证坏号（网络查询失败仍视为额度未知的候选，避免瞬时故障饿死号池）；只有 session 迁移成功才按事件 `resetsAt` 冷却旧号，找不到替代号或迁移失败就继续当前号且不冷却。非 PTY 路径只由 output consumer 切换，dispatcher 的 lifecycle 分支必须 gate 在 PTY，严禁同一事件双切。其余非 allowed 状态（rejected/blocked）和实际限额横幅仍走既有硬限额/反应式轮换，不受软阈值规则影响。早期代码曾对任意事件换号，导致 37% 周额度也把健康账号冷却并制造假性耗尽（task #734/#740），禁止恢复这种行为。额度查询走 OAuth usage API（`fetch_usage`，缓存 60s），前端 Header 的 "Pro" 徽标 → PoolDrawer 抽屉展示 5h/7d 利用率
- **瞬时 429/过载自动等待重试**: Anthropic **基础设施侧**的临时限流/过载（CLI 文案 `Server is temporarily limiting requests (not your usage limit)` / overloaded，是 Anthropic 官方报错而非 CCM 内部），换号无用 → 退避后用**同一账号** `--resume` 重试。检测器 `is_transient_overload`（`claude_pool.py`，先排除 `is_rate_limited`/`is_auth_failure` 保证与「额度用尽要换号」互斥），退避 `transient_retry_delay`（指数+jitter）。开关/参数：`transient_retry_enabled`(默认 True)、`transient_retry_max`(5)、`transient_retry_base_delay`(10s)、`transient_retry_max_delay`(120s)。**关键陷阱**：PTY 模式下 api_error 中止 turn 但**持久 session 仍存活 → exit_code 报 0**，单看退出码会误判成功；故 instance_manager 在 `_process_event` 里对带 `is_error` 且命中检测器的事件打 **turn-scoped 标记** `_transient_seen`（`launch()` 重置、`transient_error_seen()` 读取），dispatcher 据此即便 exit_code=0 也触发重试。**标记必须只认当前前台 turn 的活事件**：`_process_event` 打标前要排除 `orphan`（resume 时 PTY 重读 JSONL 回放的上一 turn 旧 api_error）和 `autonomous`（后台子 agent turn 的报错）事件——否则成功 resume 的 turn 会被旧错误重新置标，`still_transient` 永真→烧光重试预算→任务被误判 failed（recover-then-failed bug，task #729）。Autonomous 任务走 dispatcher `_run_transient_retry`（递归自驱）；chat 子进程模式走 instance_manager `_try_chat_transient_retry`（`_consume_output.finally` 自驱循环），PTY 模式由 `_process_queued_message` 在 `_wait_process` 后用 while 循环驱动（heartbeat 覆盖、不会被看门狗误杀）。重试与号池轮换单向衔接（transient 用尽→轮换；轮换不回切 transient），无 ping-pong
- **备份服务**: `BackupService`（`backend/services/backup_service.py`）只接受文件型 SQLite；后台线程每轮先用 SQLite backup API 生成包含已提交 WAL 页的一致性 staging snapshot，再交给 auto-backup SDK 写 local / s3 / oss，并始终清理 staging。PostgreSQL/MySQL 必须用各自原生备份工具，不能把数据库 URL 当文件归档；`BACKUP_ENABLED=false` 时完全不加载。
- **PR Monitor**: GitHub PR 自动审核功能。GitHub Webhook 推送 PR 事件 → Legacy 单 Reviewer 或 Principal/Senior/QA 独立 Panel → Finding Gate → 后端发布 Review/可选 merge。`synchronize` 替换旧审核时必须先经 `task_termination.terminate_authoritative_task_generation` 安全终止旧 generation；Panel 必须终止并锁定该 snapshot 的全部 `PRReviewerRun.task_id`，不能只处理 `PRReview.task_id` 锚点。本地清空/中止 queue，先持久化非终态 superseded admission gate，再以 Task→Instance/auxiliary exact snapshot stop 主进程及 CCM 子进程；终态提交前必须在同一锁序下有界重扫所有 late auxiliary row，DB `completed/failed/stopped` 仅在 Dispatcher exact lifecycle/process/thread/home registry 无证据时才可视为已结束，证据仍在就必须真正 reap。Worker 则持有 migration/operation locks，以仅 service-token 可访问的隐藏 GET/termination endpoint 做 authoritative CAS。两路都要确认无残留并锁定 exact resulting Task generation 直至旧 Review/全部 Reviewer Run superseded + replacement commit；失败一律 409。数据模型：`MonitoredRepo` + aggregate `PRReview` + `PRReviewerRun` + `PRFinding`。Webhook 端点 `/api/github/webhook`（公开，HMAC-SHA256 验签），前端 `PRMonitorPage`。
- **PR Monitor 固定审核上下文与发布边界**: 每次审核以签名 Webhook 捕获的 `(base_sha, head_sha)` 为唯一快照，去重键包含两者；在创建/替换 Task 前，后端先从 exact base 根 tree 读取完整 `CLAUDE.md`、可选 `PROGRESS.md`，并在两次一致 snapshot guard 之间抓取完整 PR metadata/patch，全部经过 OID、类型、UTF-8、NUL 与大小校验后直接注入 prompt。优先级为固定审核 contract → base `CLAUDE.md` → base `PROGRESS.md` → 不可信 PR 内容；绝不读取 PR head、当前分支或 CCM/Worker 本地文档。`pr-review` Task 进入 owner-only 中性 cwd，跳过 agent-doc/MCP/skills/ask-user/PTY：Claude 用 `--tools ""`、空 setting sources、strict MCP 与禁用 slash skills；Codex 仅走 app-server tool-free profile，0.144.6 必须在 thread config 同层定义并选择 profile、以 response audit 确认生效，首个 `turn/start` 只重复空 environment/runtime roots 而不得脱离 profile table 重发 selector，并关闭 shell/browser/web/apps/plugins/hooks、枚举后显式禁用全部 inherited MCP；任何隔离能力无法确认都 fail closed，Worker 还必须声明匹配的 `pr_review_snapshot_context_version`。Agent 只返回严格 body/result block；GitHub review/comment/merge 由后端在 exact completed retry generation 落成 durable `publishing` outbox 后执行，review/merge 固定 captured head，随机 nonce + frozen actor/time 用于崩溃恢复对账（自有 PR 的 approve 或 REQUEST_CHANGES 被 GitHub 拒绝时，均降级为保留原正文的 head-pinned `COMMENT` review，绝非 issue comment）。`opened`/`synchronize`/删除共享 repo 写屏障并在提交前锁定 `MonitoredRepo`、复查 GitHub 当前 snapshot；PR Review Task 在 `pending/reviewing/publishing/superseding` 期间冻结公共 chat/inject，`superseded` 永久冻结；只有 GitHub workflow 已进入 `approved/merged/commented/error` 后才允许普通续聊和 live inject。Worker 路径必须由 Manager 在 operation lock 内确认终态、先握手匹配的 `pr_review_terminal_chat_version`，再携带内部 service-token 断言；旧 Worker 在 Manager 落消息前明确 409。Shared receiver 不具备 owner-only review 状态，因此所有 Shared chat 都必须先由 owner 准入，再在 shadow 落消息。edit/retry/cancel/stop/delete 始终冻结，仅内部 exact-generation 终止可绕过。`publishing` 行不得被 synchronize supersede；新 head 另建 snapshot review。Worker live/backfill 日志必须携带 exact `task_retry_count`，完成回调先同步并重新确认同一远端 generation。
- **PR Monitor Reviewer Panel / Guide Pack**: `review_mode=panel` 为三个互不共享结论的 tool-free Task；Prompt policy v3 给所有角色固定注入七条 Engineering Design Standard（内聚/分层/复用/单元扩展/范式统一/及时删码/简单够用），再按 Principal/Senior/QA 注入独立 persona、证据边界与 litmus，policy hash 必须覆盖共享标准与角色 contract。每个角色输出 strict JSON subject/role/verdict/findings，`critical/high/medium` 为 blocking，任一 required role 缺失、坏 JSON、Task 失败或 subject 不匹配都 fail closed。`wait_for_ci` 由 30 秒 recovery/reconcile loop 查询 exact head check-runs + commit statuses，CI PASS 前不创建 Task、不占 Instance；新 head 全量 supersede 并重审。可选 exact-base `.ccm/review-guides.json` 只声明白名单 regular UTF-8 文档，禁止 symlink/绝对路径/`..`/重复/缺失/截断/超限；Guide 低于固定 contract，高于 PR 内容，PR head 对 Guide 的修改本轮不生效。Panel 完成后 Finding 持久化，纯 Gate 聚合三个终态，再复用 exact-generation publication outbox；Reviewer 自身永远不写 GitHub、不 merge。API/UI detail 展示 role 状态、CI summary 与 Finding 证据。Finding 操作以独立审计记录支持忽略、人工建议与 AI 修复；AI 修复只能在隔离的 tool-free Task 中生成有界 diff，用户下载并确认 base/head/hash 后才允许通过 owner-fenced exact-old CAS push 应用，且不会隐式改变 Panel Gate。
- **PR Monitor Finding Action / 候选补丁边界**: `ignore`/`human_advice` 只是幂等审计输入，不得关闭 Finding 或改变 Panel Gate；`ai_fix` 以 nullable unique `active_fix_finding_id` 占用每 Finding 唯一 active slot，只有 terminal transition 才清槽。`pr-review-fix` 与 Reviewer 共用 tool-free runtime v3，但 Manager 还以 `pr_finding_action_id` 元数据识别、Worker 以保留 tag 识别；其 edit/retry/chat/inject/cancel/stop/delete 全部 409，只有 workflow exact generation 可推进。模型只接收冻结的 exact-head 单文件并输出唯一 canonical diff；后端在私有 checkout 实际 apply/stage 后再次证明 changed path=allowlist 且仅为内容修改。验证后的 patch 先进入持久 candidate outbox（patch 只存后端，API 仅暴露 hash/route/head）；下载必须由后端签发并落库绑定 authenticated user + action + patch hash 的 opaque receipt，confirm 必须回传同一 receipt，禁止信任前端“已下载”布尔值。确认再以数据库时间的 operation lease/CAS 独占 push，创建以 expected head 为唯一 parent且含 nonce 的 commit，并只允许 `--force-with-lease=refs/heads/<captured-ref>:<expected-head>` 这一显式 exact-old CAS；它在 ref 删除或前进时拒绝，禁止无条件 force/`+` refspec，任何 base/head/ref/repo 漂移都 fail closed。push 响应丢失时保持 recoverable running outbox，lease 过期后先按 PR head、唯一 parent 与 nonce 对账，绝不能盲目重推。Worker successful terminal 必须先 backfill exact retry 日志再由 Manager 解析，failed/cancelled/conflict 也必须收口 Action；创建 reservation、active slot 与 push lease 均为 crash recovery 的持久事实，不能用进程内 lock 代替。
- **WebSocket channels 与 ACL**: `instance:{id}`, `task:{id}`, `worker:{id}`, `discussion:{id}[:agent:{id}]` 是资源频道；`tasks/workers/system/system_update/pr-monitor` 是跨 owner 全局频道，仅 admin 可订阅。member 的 task/discussion/worker 订阅复用 HTTP ACL，JWT/资源权限与 legacy share token 每 5s 复核，累计最多 100 个频道。`workers` 事件按 `worker_id` 镜像到 owner-safe `worker:{id}`；broadcast 对订阅快照并发发送，慢客户端最多占一个 send timeout，失败连接自动清理。
- **状态变更必广播**: 任何写 `Task.status` 的路径，`db.commit()` **之后**必须调 `task_events.broadcast_status_change`（tasks 频道，broadcaster 自动镜像到 task:{id}）。此前 cancel/retry/plan 审批/stop-session/stale 兜底/worker 断连等只写库不广播，导致 ChatView（WS 驱动）与列表（轮询驱动）状态分叉（2026-07-12 大排查）。前端侧：ChatView 的 localStatus 是 WS 实时覆盖、task.status prop（轮询）是事实源，prop 变化清覆盖（带 7s `lastWsStatusAt` 守卫防在途旧快照击穿）；`_process_event` 的 completed→executing 复活块排除 orphan/autonomous 事件
- **认证**: 除 `/api/system/health`、`/api/auth/login`、`/api/github/webhook` 与必须接收第三方回调的 Feishu OAuth callback 外，所有 API 需要 `Authorization: Bearer <token>`。JWT 的 role/is_active 必须每个 HTTP 请求以 User 当前数据库行为准，删除/禁用立即 401、降级立即失去 admin 权限；`/api/instances`、`/api/dispatcher` 与 system update/status 的读写，以及 system skill curator/distill 的触发均 admin-only。Feishu callback 的 state 必须用部署 secret 做带过期时间的 HMAC 签名并绑定 exact active user，裸 `uid:`/空 state 不兼容放行；未建立跨 CCM 签名信任协议前，org register/import/registry-changed 只能由本机 admin 调用。`POST /api/ask-user/wait` 是内部 hook 入口，启用 AUTH_TOKEN 时只接受 legacy service token，不能接受用户 JWT。
- **前端 type 导入**: 用 `import type { X }` 导入类型，`import { api }` 导入值（Vite 会去除 type-only exports）
- **pytest 外部状态隔离**: `backend/tests/conftest.py` 必须在首次 `backend.*` import 前覆盖进程级 `DATABASE_URL`、Claude/Codex pool 路径和 update project dir，并关闭 Dispatcher/Worker/pool/backup。FastAPI dependency override 只替换请求 session，不能隔离 `backend.main` 的全局服务；测试不得扫描真实登录 journal、触碰开发库或在真实 checkout 建 deployment lease。
- **Tailwind v4**: 用 `@import "tailwindcss"` + `@tailwindcss/vite` 插件，无 tailwind.config
- **主题（v2）**: 换肤机制 = 每主题覆盖 `--color-gray-*`（中性色）与 `--color-indigo-*`（品牌色）等 CSS 变量（`index.css`），组件类名不变。现代组：`dark`（默认，Multica 风 zinc 中性色 + 蓝品牌色，oklch）、`light`（中性色反转 + accent 300/400 档反转成深色调保对比度；壳/画布取 tonal zinc 分层灰 92.5%/95.8%）、`feishu`（飞书官方色板 + App 截图像素取色实证：**白底为主**——画布近白 #fbfbfc + 纯白卡片发丝线分隔（iPad/macOS 截图实证消息列表与聊天区均纯白）、N 系中性色、经典飞书蓝 #3370FF、hover/pressed 向深走 B600 #245BDB / B700 #1C4CBA、侧栏壳 #ecedef = 飞书 rail 灰、gray-700 取 #e8eaed 弱化线框感）、`apple`（emilkowalski/skills 的 apple-design skill 驱动：iOS systemGray 中性色（分隔线 #e5e5ea / systemGray6 #f2f2f7）+ apple.com 取值（画布 #f7f7f7 / 侧栏 #f9f9f9（官方手册 System Settings 截图实测，侧栏略亮于画布）、主文字 #1d1d1f、CTA 蓝 #0071E3 hover 向亮走 #0077ED）、accent 300/400 取 iOS accessible 色板 light 变体；skill 规则落地：§15 系统字体优先（块内覆盖 --font-sans/--font-mono 为 -apple-system/ui-monospace）、§12 毛玻璃顶栏（`header.sticky` backdrop-blur + color-mix 半透底，@supports 守卫）+ 卡片软阴影浮起（`[class~='bg-gray-800']:not([class*='shadow'])`，不覆盖弹层 shadow 工具类）、§1 按压反馈（button:active 用独立 `scale` 属性 0.97，不覆盖 transform 工具类）、§14 无障碍（按压包在 prefers-reduced-motion 守卫内、顶栏带 prefers-reduced-transparency 实底回退））。**三个现代浅色主题以「形状语言 + 壳结构 + 画布」三轴互相区分**（theme.test.ts 有防趋同回归断言；2026-07-16 用户反馈 light/feishu 趋同、2026-07-17 反馈三浅色全趋同后逐步确立——画布灰度 hex 撑不起辨识度，屏幕 90% 是白卡片）：①圆角 feishu 紧凑 4/6/8px（feishucdn 官网 CSS 高频值实测）/ light 默认 10px / apple 大圆角 8-24px（apple.com 卡片语言），经 `--radius-*` 主题级覆盖实现；②壳结构 light 分层（壳 92.5% 深于画布）/ apple 近连续极浅双灰（侧栏 #f9f9f9 略亮于画布 #f7f7f7，Settings 实测，白卡靠软阴影浮起）/ feishu rail 灰 #ecedef + 近白画布发丝线；③画布 light oklch 95.8% / apple #f7f7f7 / feishu #fbfbfc。**结构级复刻层**（2026-07-17 用户要求激进复刻后新增，index.css「结构级复刻层」段）：AppShell 暴露主题无关 data 钩子（`data-shell-sidebar`/`data-shell-main`/`data-nav-item[data-active]`/`data-shell-brand-row`/`data-shell-brand-text`/`data-shell-user-footer`/`data-shell-user-meta`），feishu 据此把桌面侧栏重排成飞书客户端「76px 窄图标 rail」（图标上小字下、**选中=白色圆角 tile 包住图标+文字**（iPad 官方截图实测 tile≈白）、头像置顶、图标为 IconPark 双色集（见「主题图标集」），仅 lg+，移动端抽屉不变，主列 padding 跟随 rail 宽度），apple 复刻 macOS System Settings 侧栏（216px、顶部装饰性 Search 框 `[data-shell-sidebar]::before` + 账户行上移到搜索框下（flex order 重排）、iOS 系统色 squircle 图标 nth-of-type 轮换、行高压缩 ~28px、选中行实底 #0071e3 白字 6px 圆角）+ 按钮全面胶囊化（导航项以更高特异性覆盖回 8px）+ 输入类控件 10px。**改 AppShell 结构时不得删改这些钩子**（AppShell.test 有断言）。**主题图标集**（2026-07-17，双层）：`ThemeOption.iconSet?` 声明集合名，两层承载——①导航层 `config/iconSets.tsx`（语义 key = AppShell 导航 key，two-tone 双色选中态）；②全站层 `components/icons.tsx`（中央图标模块）：**全站组件一律从它导入图标（与 Lucide 同名同 props），禁止值导入 lucide-react**（icons.test 有架构守卫断言，type-only 豁免），内部按 iconSet 解析 IconPark/Ionicons、无映射回退 Lucide；新增图标 = lucide 导入 + themed() 映射一行（park/ion 可缺省）。**fill 语义陷阱**：lucide 惯用 `fill='currentColor'|'none'` 表达实心/空心（收藏星标），直接透传会让 IconPark（fill=颜色数组）/Ionicons 隐形——themed() 拦截翻译：park 映射为 theme filled/outline，ion 走 outline 变体组件（icons.test 有回归断言）。（语义 key = AppShell 导航 key；`feishu`=IconPark two-tone（字节官方开源图标库，Apache-2.0，选中飞书蓝 #3370ff+淡蓝填充 / 未选中深灰+白填充）、`sf`=Ionicons 5（react-icons/io5，MIT，颜色走 currentColor 由 squircle 结构层控制））；AppShell 经 `useTheme()`（hooks/useTheme.ts，useSyncExternalStore 订阅 theme.ts 的 subscribeTheme，setTheme 即时通知）解析渲染器，包一层 `<span data-icon-set class="contents">`；主题未声明集合 / 集合缺 key 一律回退 Lucide，图标集是纯增强绝不阻塞。**新增导航页**必须同步补 NAV_ICON_KEYS 与各图标集（iconSets.test 覆盖断言会精确红出缺哪个）。**新增主题的丝滑三步**（零架构改动）：① theme.ts 加条目（gray/indigo 全档变量块见下条约定，可选 iconSet）② index.css 加 `html[data-theme='x']` 变量块（可选：用既有 data 钩子写主题作用域结构规则）③（可选）iconSets.tsx 注册图标集——theme.test/iconSets.test 的完整性断言自动把关；Legacy 组：`legacy`（v1 默认外观，Tailwind 原生 gray/indigo）、`ocean`/`forest`/`rose`。**新增主题必须同时覆盖 gray 全档 + indigo 全档**（`frontend/src/config/theme.test.ts` 有自动化断言）；浅色兼容规则：中性底上禁用 `text-white`/`hover:text-white`，一律 `text-foreground`（彩色实底按钮除外）。字体 Inter Variable + JetBrains Mono（@fontsource-variable，随 bundle 离线）。App 壳 `AppShell.tsx`：桌面 lg+ 固定侧栏 + sticky 顶栏（h-12 + 1px 边框，TasksPage 分屏高度按 `100vh-49px` 计算），移动端抽屉导航
- **Android App**: Capacitor 打包，API/WS 地址通过 `config/server.ts` 动态获取，LoginPage 可展开配置 Server URL
- **Goal 模式**: `mode="goal"` 任务使用自然语言完成条件（`goal_condition`），每 turn 后由独立评估器判断是否达成，使用 provider 原生 session resume 保持上下文。评估器跟随 Task provider：Claude 走 `claude -p`，Codex 走绑定账号的 ephemeral `codex exec`；Codex evaluator 的 usage-limit/auth 失败会换号后只重试评估，不重复工作 turn
- **Goal 评估器**: `GoalEvaluator`（`backend/services/goal_evaluator.py`）读取对话日志摘要，发给轻量模型判断条件是否满足。Claude 默认 `claude-haiku-4-5`，Codex 使用 `default_codex_goal_evaluator_model`，均可由 `goal_evaluator_model` 覆盖；进程超时/非零退出属于运行错误，不得误记为“目标未达成”并消耗 turn
- **交互式版本化 Plan（2026-08-04）**: 新 Plan 不再创建 `Task(mode=plan)`；`Plan` 是稳定聚合根，一次规划执行是可暂停/恢复的 `PlanAgentRun`。Planner 中间产物只覆盖 Run-scoped `draft_content`，Reviewer 内部 revise 不增加 Version；完整 Pipeline approve/disabled/exhausted 后才把最终 candidate 与审查结论原子发布为一个不可变 `PlanVersion`。Planner/Reviewer 的必要问题持久化为 `PlanInputRequest`，active Run 立即展示已回答输入，完成后由 `produced_by_run_id` 归入最终 Version 审计。顶级 **Plans** 页面负责 standalone 创建、完整目录和行动队列，行动区固定名为 `Plans requiring action`；**Tasks** 页面及其搜索展示全部真实 Task，迁移后的旧 `Task(mode=plan)` 作为只读历史保留并链接 canonical Plan；New Task 不提供 Plan mode。旧数据迁移只承认 `origin/main` 能产生的单 carrier schema：每个旧 Task 映射一个 Plan 和至多一个 v1，Main approve 映射为指回同一 carrier Task 的 execution Application；本功能分支早期产生的 revision chain、无 Main source Plan 和 Run/Input/Application 测试数据由 reconciliation 清理，但旧 Task 行不删除。Plan 目录支持 kind/status/Project/search/Archived only；Archive 是保留全部 Version/Run/Q&A/Application 的可恢复软归档。Planner/Reviewer 只允许全局配置并在新 Plan 冻结完整快照；同一请求的问题数量没有业务上限，独立 `plan_max_interactions` 只限制一个 Run 的暂停/回答轮数且范围为 0–5。Plan 创建/标题/Revise/Fork/回答都会持久化，高置信 API key/token/private key 必须拒绝并引导使用 Secrets 引用。澄清回答恢复同一 Run，用户 Revise/Refresh 在同一 Plan 下创建新 Run并在 Pipeline 完成后产出下一 Version，Fork 才创建新 Plan；同一 Task 的活跃 Plan 数量必须在 Task 行写锁后的统一服务边界检查，create/Fork 不得绕过。等待用户时必须释放 Instance/process/thread/account 与 update blocker。approve/reject/apply/create-execution-task 都绑定 exact Version；`human_decision` 只保存 pending/approved/rejected 审计事实，UI 由 application/decision/superseded/review 派生 Version 状态，Application 必须蕴含 approved；execution Task 后续缺失只保留历史并禁用跳转。迁移 Version 缺少历史 repo 指纹只算可确认 stale，Reject 直接允许，Approve/Apply/create-execution-task 经用户明确确认可继续；`Refresh contexts and regenerate Plan` 只在 stale 时展示，真正的 target/project/worker/repo 不可用才是 hard conflict。standalone Version 创建 execution Task 必须统一调用 `plan_service.materialize_execution_task`，exact `plan_version_id` 是幂等键，API/Auto 不得复制 ORM 物化逻辑。approve 不自动唤醒主 Task，真实 chat 通过 `plan_version_ids` 应用一次并保存 Version 快照；PlanApplication、用户日志与完整 queue envelope 必须同事务进入 durable outbox，内存队列以 receipt key 幂等准入，未确认 launch 的投递由 Dispatcher 启动恢复。Plan WebSocket 只作 ACL-scoped invalidation，HTTP aggregate snapshot 才是权威；Plan 目录必须先在 SQL 中完成 ACL/display-state/count/limit/offset，再批量聚合关联资源，禁止逐 Plan N+1。Dispatcher 以 `Instance.current_plan_run_id` 和 generation CAS 直接领取本地 Run；Manager 是 Worker Plan/Version/Input/Application 的权威，Worker protocol v3 先握手并同步 candidate/final Version 边界，Manager claim generation 与 Worker-local Run generation 独立，重放只接受排除 Manager claim 后完全相同的 import payload digest，并返回首次持久化的附件 receipt；旧协议必须 fail closed。断线时答案留在 Manager，恢复后继续，跨 Worker Apply 会先 materialize exact Version。通用 `POST /api/tasks mode=plan` 已关闭；旧 `Task.plan_*`、`plan_task_ids` 与 `docs/plans/plan-agent-design.md` 只作 legacy contract 兼容。Auto Mode 交接见 `docs/plans/auto-mode-plan-integration-handoff.md`；完整设计/验收见 `docs/plans/interactive-versioned-plan-design.md`
- **Plan chat outbox 恢复**: queue envelope 使用可跨重启比较的 epoch 排队时间。receipt 的 DB CAS、内存 delivery key/队列登记和 Task queue generation 必须在 `_dispatch_claim_lock` 下统一准入；创建 receipt 前保存 process epoch + queue generation fence，stop/cancel 在同一锁内推进 generation、按 Task 回收尚未进入内存的 durable receipt，CAS/fence 失败不得发布队列项。仅 `pending/queued` receipt 可在启动或暂停恢复后重排；重启遗留的 `launching` 必须携带 exact Task/Instance/generation 证据转为 `uncertain` 并禁止自动重放。管理员对账确认 exact turn 已执行后可审计标记 `launched`；确认未启动则先保存不可变 `PlanApplicationAttempt`，再删除活动 Application、清理日志中的 applied snapshot 并释放 Version 重试，Plan aggregate 必须独立展示 attempt 对应的 resolution/note/evidence；无法确认时保持冲突。只有能证明尚未启动的永久失败或显式取消才能自动释放；实际停服保留 pending，同进程恢复则立即重新准入
- **调度器**: `GlobalDispatcher` 只负责分配任务、启动 Claude Code、判断成败。所有 git 操作（worktree、commit、merge、push）全由 Claude Code 自主完成
- **任务生命周期**: pending → in_progress → executing → completed（失败回 pending 重试）
- **项目**: `Project` 模型管理 git repo，支持 clone 已有仓库（has_remote=True）和本地 git init（has_remote=False）
- **Task.project_id**: 可选关联 Project，dispatcher 自动解析为 target_repo
- **Project Todo（清单）**: 每个 Project 挂一个 prompt 模板清单（`project_todos` 表）。前端 `ProjectTodoList`（Project 卡片内可折叠）「▶ Run」以 `{title, description=prompt, project_id}` 建 task（默认配置，target_repo 由 dispatcher 从 project 补全）→ 跳 chat，并把 todo 标 `done` + 记 `created_task_id`（溯源）。状态 open/done/archived（软归档，DELETE 才是永久删除）。清单语义：建 task 即划掉；非模板库，故只存 prompt 不存 task 配置

- **Codex home runtime 准入**: Dispatcher 的 busy-home 快照只用于选路，direct task exec、app-server、GoalEvaluator 和 Distill 最终都必须在同一 canonical home lock 内互斥准入；direct spawn 取消后只有证明 exact process generation 已回收才可释放 home，GoalEvaluator/Distill cleanup 失败则以各自结构化 retained-process registry 继续 fail closed。若 runtime busy 排空兼容候选，统一短退避重试，不能继承其他账号的长 cooldown，也不能把保留在 disabled/retired source 的 session 判成永久失败。
- **Codex 显式中断兜底**: app-server turn 先按 exact thread/turn/descendant 中断；若原生子线程无法在有界时间内确认终态，用户显式 stop 必须回收该账号 transport。目标 turn 记为 interrupt，同 transport 的同行 turn 明确失败并走正常重试，禁止为保护共享 transport 返回永久 409。

## 任务生命周期（9 步）

你收到任务后，按以下流程自主完成：

1. **领取任务** — 你已被分配任务，阅读 CLAUDE.md 和相关代码
2. **创建工作区**:
   - `git fetch origin`（如有 remote）
   - `git worktree add -b task-<简短描述> .claude-manager/worktrees/task-<简短描述> origin/main`
   - 进入 worktree 目录工作（后续所有操作在 worktree 中）
   - 如果 worktree 创建失败，直接在当前分支工作
3. **实现功能** — 编写代码，确保可运行
4. **提交代码** — `git add` + `git commit`
5. **Merge + 测试**:
   - `git fetch origin && git merge origin/main`
   - `uv run python -m pytest backend/tests/ -v`（后端测试）
   - `cd frontend && npx tsc --noEmit`（前端类型检查）
6. **自动合并到 main**:
   - `git fetch origin main`
   - `git rebase origin/main`，冲突则自行 resolve
   - 成功后: `git checkout main && git merge <task-branch> && git push origin main`
   - 失败则退回步骤 5 重试
7. **标记完成** — 更新文档（在清理之前）
8. **清理** — 回到项目根目录:
   - `git worktree remove .claude-manager/worktrees/<worktree名>`
   - `git branch -D <task-branch>`
   - 如有 remote: `git push origin --delete <task-branch>`
9. **经验沉淀** — 在 PROGRESS.md 记录经验教训（可选）

通过 `git remote -v` 判断是否有 remote，有则执行步骤 5-6-8 的 remote 操作，无则跳过。

**状态流转：**
```
pending → in_progress → executing → completed
                           ↓
                        (fail)
                           ↓
                        pending
                       (retry)
```

## 开发命令

```bash
# 依赖管理（使用 uv）
uv sync              # 安装生产依赖
uv sync --group dev  # 安装生产 + 开发依赖（pytest 等）

# 刷新 claude-pty 到 PTY 仓库 main 最新（git 依赖是安装时快照，
# git pull 不会更新它——部署同步时必须跑；editable 安装自动跳过）
./scripts/refresh_pty.sh

# 一键启动 (后端 + 前端)
./scripts/dev.sh

# 仅后端
uv run uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload

# 仅前端
cd frontend && npx vite --host

# 构建前端
cd frontend && npm run build

# 运行测试
uv run python -m pytest backend/tests/ -v

# 生产模式 (单端口，后端服务前端静态文件)
cd frontend && npm run build && cd ..
uv run uvicorn backend.main:app --host 0.0.0.0 --port 8000

# 公网部署 (Cloudflare Tunnel)
# 首次设置: cloudflared tunnel login → create → route dns → 编写 ~/.cloudflared/config.yml
# 每次部署:
cd frontend && npm run build && cd ..
uv run uvicorn backend.main:app --host 0.0.0.0 --port 8000  # 终端1
cloudflared tunnel run <tunnel-name>                          # 终端2

# 生产后台部署 (systemd, SSH 断开后持续运行)
# 两个 systemd 服务:
#   ccm-backend  — uvicorn 后端
#   ccm-tunnel   — cloudflare tunnel
# 服务文件位于 /etc/systemd/system/ccm-backend.service 和 ccm-tunnel.service
# 常用命令:
sudo systemctl restart ccm-backend   # 重启后端
sudo systemctl restart ccm-tunnel    # 重启 tunnel
sudo systemctl stop ccm-backend      # 停止后端
sudo journalctl -u ccm-backend -f    # 查看后端日志
sudo journalctl -u ccm-tunnel -f     # 查看 tunnel 日志
# 开机自启已通过 systemctl enable 配置

# 自动更新的 systemd 服务配置（.env）:
#   SERVICE_NAME=ccm-backend   # 服务单元名（不含 .service 后缀），默认 ccm
#   SERVICE_SCOPE=auto         # auto | user | system，auto 从 cgroup 自动检测
# 非默认服务名的部署必须显式配置，否则更新机制无法正确 restart
```

## 数据库

默认使用 SQLite（`./claude_manager.db`），也支持 PostgreSQL 和 MySQL 作为外部数据库。通过 `.env` 中的 `DATABASE_URL` 切换：

```bash
# SQLite（默认）
DATABASE_URL=sqlite+aiosqlite:///./claude_manager.db

# PostgreSQL（需安装: uv sync --extra postgres）
DATABASE_URL=postgresql+asyncpg://user:pass@host:5432/claude_manager

# MySQL（需安装: uv sync --extra mysql）
DATABASE_URL=mysql+aiomysql://user:pass@host:3306/claude_manager
```

**数据迁移脚本**：在数据库之间迁移全部数据（注意使用同步 URL）：
```bash
# 先在目标库初始化 schema
DATABASE_URL=postgresql+asyncpg://... uv run alembic upgrade head

# 再迁移数据（使用同步 URL）
uv run python scripts/transfer_db.py \
    "sqlite:///./claude_manager.db" \
    "postgresql://user:pass@host:5432/claude_manager"
```

使用 **Alembic** 管理 schema 版本。`init_db()` 在启动时自动执行 `alembic upgrade head`，无需手动操作。

> **严禁手动修改数据库 schema**（如直接执行 `ALTER TABLE`、`DROP COLUMN` 等）。所有 schema 变更必须且只能通过 Alembic migration 文件管理，否则会导致 migration 状态不一致、其他环境部署失败。
>
> **已发布的 revision 永久保留**：只要 migration 曾进入可部署分支，就不得删除、改 ID 或改写既有 `upgrade`/`downgrade`。功能回滚也必须保留原 migration，并新增一个以它为 `down_revision` 的前向清理 migration；否则已经记录该 revision 的生产数据库会在 `alembic current` 阶段无法启动或更新。若两个已发布 revision 从同一父节点分叉，必须保留两条历史并新增 `down_revision=(head_a, head_b)` 的 merge revision，禁止为线性化而改写任一旧文件。

**Schema 变更流程**（详见 [DATABASE.md](./DATABASE.md)）：
1. 修改 `backend/models/` 中的模型
2. `uv run alembic revision --autogenerate -m "描述"` 生成 migration
3. 测试：upgrade → downgrade → upgrade 全通过后提交
4. migration 文件与模型修改**同一个 commit** 提交

```bash
uv run alembic upgrade head    # 手动升级（通常不需要，启动自动执行）
uv run alembic current         # 查看当前版本
uv run alembic history         # 查看历史
```

## 文件维护规则

> **四个文件都由 Claude Code 自主维护，每次功能变更后必须同步更新。**

- **CLAUDE.md**（本文件）：架构、约定、关键路径变化时更新，只改变化的部分，保持简洁
- **AGENTS.md**（Codex CLI 读取）：**与 CLAUDE.md 必须保持关键内容同步——本仓库和所有被开发项目一律适用**。同步是 CC/Codex 在 coding 时的行为纪律，不做程序化同步：需要往其中一个文件写新内容（约定/规范/教训）时，把相同的意思也写进另一个，不要求逐字一致。本仓库的 AGENTS.md 当前是指向 CLAUDE.md 的 symlink（改一处即两处同步，无需额外操作），不要改成独立文件；若某项目里两者本是独立文件，**不要**用 symlink 覆盖任何一份已有内容，坚持逐次同步意思即可。这条纪律同时经 dispatcher 的 prompt 前导（`_DOC_SYNC_NOTE`）随每个任务下发，覆盖文档里没写这条规则的老项目
- **README.md**：面向用户的文档，功能、API、使用流程变化时同步更新，保持与实际代码一致
- **TEST.md**：测试指南，新增功能时同步添加测试用例和文档
- **PROGRESS.md**：见下方「经验教训沉淀」

## 测试规范

**开发时必须主动使用测试，不是事后补充！**

- **改代码前**：先跑 `uv run python -m pytest backend/tests/ -v`，确认基线全绿
- **改代码后**：再跑一遍确认无回归 + `cd frontend && npx tsc --noEmit` 检查类型
- **新增功能**：同步新增测试用例，更新 [TEST.md](./TEST.md)
- **修 bug**：先写复现 bug 的测试（红），修复后确认变绿
- **提交代码**：改完代码 + 更新文档后，`git commit` + `git push origin main`（默认必须 push）
- 详细测试清单和手动测试项见 [TEST.md](./TEST.md)

## 经验教训沉淀

每次遇到问题或完成重要改动后，要在 [PROGRESS.md](./PROGRESS.md) 中记录：
- 遇到了什么问题
- 如何解决的
- 以后如何避免
- **必须附上 git commit ID**

**同样的问题不要犯两次！**

## 分布式 Worker（Phase 1，设计见 docs/plans/elastic-worker-design.md）

- **形态**：Worker = 一台跑完整 CCM 的 EC2，Manager 全生命周期管理（创建/收养/关机/开机/销毁），前端 Workers 一级页面操作
- **配置自举与 SSH 闭环**：新 EC2 的机型/AMI/子网可从 Manager 实例元数据继承（IMDSv2 + boto3，凭证走 IAM instance profile）；创建前必须对 `WORKER_SSH_KEY_PATH` 做普通文件/属主/0600/未加密格式预检，再从私钥派生公钥用 cloud-init 注入 Worker，不能假设本地私钥与继承的 AWS KeyName 匹配。CCM 创建/校验专属 Worker SG，只允许 Manager SG → TCP 22 + `ccm_port`，绝不开放公网；通信全走 VPC private IP。host key 信任库按 `cloud_instance_id` 隔离（防 AWS 回收 private IP 后旧 known_hosts 误伤新机，同时同一实例换 key 仍 fail closed）；SSH 探针保留 `authentication_failed`/`host_key_mismatch`/`connection_timeout`/`network_unreachable` 等结构化原因，旧实例密钥错配不能靠 retry 修复，需重建或手工修 key。RunInstances 前先持久化非敏感 `Worker.provision_spec`，ClientToken 由 Worker generation 稳定派生；retry 先按 token 查回响应丢失的实例，0 个才用冻结参数重发，外部终止后换 generation/token，绝不能因 rename/配置漂移产生 billable orphan
- **部署 = rsync**（不走 git clone）：Manager 本地仓库 → worker `/home/ubuntu/ccm`，`--filter ':- .gitignore'` + 排除 `.git`（worktree 的 .git 是悬空指针）；版本锁定靠 `.deploy_commit` 文件（`git_info.git_head_commit` 的回退路径），health 端点带 commit 供校验
- **auth 探针**：`/api/system/health` 在 PUBLIC_PATHS 不校验 token，bootstrap 健康检查必须再打需认证端点（`/api/system/stats`）验证 worker 的 AUTH_TOKEN 真可用
- **Worker 账号默认 Codex**：创建表账号 `provider` 默认 codex（历史无 provider 记录按 claude）；Worker 创建/动态添加/重试的无人值守 Codex 登录必须有邮箱 token，OpenAI 密码可选。bootstrap 安装与 Manager 协议实证一致的固定 `@openai/codex@0.144.6`（禁止 `latest` 在 retry 时漂移）+ Chrome/Xvfb/xauth，先启动本机 CCM，再由 Manager 通过 SSH stdin 调 Worker localhost `/api/codex-pool/add`，复用登录事务/目录分配/回滚且凭据不进 argv/VPC 明文；自动取码失败时透传同一登录进程的 OTP challenge 给 Manager 前端，允许提交/取消。retry/开机恢复对已有 `account_id` 必须查 status + live verify，健康就跳过、失效走 `/relogin`，绝不能再次 `/add` 复制槽位；动态添加先持久化 intent，按 Worker/provider/email 幂等，远端成功后 Manager 落库才对外发布 success。只有 Claude 账号才跑兼容登录与 warmup。Worker 号池 status/usage/add/delete 必须携带 provider；删除先清 Manager 重试凭据，远端 404 视为幂等成功，前端默认展示 Codex quota 并保留 Claude 切换
- **error / lifecycle 语义**：`bootstrap_step` 非 None 的 error 是 bootstrap 失败（不自动恢复，UI 给 retry）；为 None 的是健康降级（健康检查恢复后自动回 ready）。stop/start/destroy/retry/rename 与 health 恢复/降级都必须 SQL CAS，异步探针的旧 ORM snapshot 绝不能覆盖 `starting/stopping/destroying`；进程重启把遗留 busy 状态转成可恢复 error，destroy intent 单独保留。账号 JSON 的读改写须串行，登录 terminal 状态必须表示没有晚到 DB write，destroying/terminated 永远拒绝凭据回写
- **开关**：`WORKER_ENABLED=true` + `WORKER_SSH_KEY_PATH`；缺 boto3 时 provisioner 会禁用，创建前必须通过密钥 preflight
- Phase 2（任务转发 + WorkerRelay）、Phase 3（TaskMigrator 实时切换执行位置）见设计文档 §20

### Phase 2（任务转发 + 中继，已实测）

- **执行链路**：Task.worker_id 非空 → Dispatcher 双路径转发（同 ID 在 worker 创建，worker 自 clone 项目）→ WorkerRelay 每 worker 一条 WS 把 chat/status/plan/loop/goal/monitor 事件双写 Manager DB + 镜像广播 → 前端零改动
- **关键陷阱**（实现处有注释）：worker 广播前 pop session_id（靠 chat 响应同步）；广播无 raw_json；monitor 事件用 "event" 键；worker MonitorSession.id 用 remote_id 列翻译；backfill 用非 user_message 条数对比
- **Phase 2 限制**：纯本地项目不能远程执行（Phase 3 播种）；worker task 不支持 secrets 引用；cost 只有 context_usage（token 级）
- `/ws` 已加 token 认证（header 或 ?token=，前端 WsClient 自动带）

### Phase 3（TaskMigrator，已实测双向闭环）

- **执行位置实时切换**：PUT /api/tasks/{id} 带 worker_id（-1=本机）→ TaskMigrator；前端 TaskConfigBadge 的 Run on 下拉。先复制后切指针，失败状态复原可重试
- **搬运内容**：session JSONL（跨账号 glob 定位 → 目标机 ~/.claude 同编码路径）+ 项目目录全量 rsync（含未提交改动）；worker→worker 经 Manager 两跳
- **cwd 链条两个教训**（task 58 实测）：① worker 转发路径必须像本地一样把 project.local_path 写进 target_repo；② 失败启动会把 os.getcwd() 写进 last_cwd 且其优先级高于 target_repo——迁回本机时无效 last_cwd 必须清掉
- 目标 worker 重建必须走 admin-only `POST /api/tasks/migration-import`，首个可见状态就在同一事务内保留 `plan_review/completed/failed/cancelled/conflict` 等不可调度的源状态且不 wake Dispatcher（不能安全保留时回退 `cancelled`）；禁止恢复旧的 `pending create → 第二请求 cancel` 窗口。迁移认领/完成/回滚都以原 status + 原 worker_id 做 CAS，`in_progress`/`executing` 一律先停止再迁移
- `worker_id` 与 provider/Skills/User Skill snapshot 的组合更新必须作为同一次协调迁移处理：目标 Worker 的 inert import 先接收已校验的最终配置，Manager 只在导入成功后把最终配置与 Worker 指针放进同一次 CAS；导入/搬运失败时 Manager 保留原配置。迁移认领与完成 CAS 还必须比较所有待更新字段的原值，不能覆盖并发配置写入
- Worker 销毁 = 批量迁回 + terminate；纯本地项目 = rsync 播种（_init_local_repo 见 .git 跳过）
