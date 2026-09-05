# CCM 产品全景梳理与竞品对照（2026-09）

> 撰写日期：2026-09-05。目的：从**产品角度**（非工程角度）梳理 CCM 的功能全景与设计逻辑，评估其合理性；对照 2026-09-05 当日一手调研的 10 个开源同类产品与 11 个商业云端产品，判断 CCM 的优势与设计缺陷，并给出下一步方向。
>
> **证据口径**：竞品结论全部来自 2026-09-05 当日 WebFetch 直接抓取的官方 GitHub README / 官方文档页（约 60+ 次抓取），未采信二手博客；抓取失败处逐条标注（见文末备忘）。CCM 自身描述以 **origin/main（2026-09-05）** 的 README.md / CLAUDE.md / docs 为准（含 8 月新落地的 Delivery Loop、一等 Plan、Test Harness、隔离体系）。本文与 `docs/coding-agent-frontier-2026H1.md`（2026-07 行业前沿）、`docs/multica-competitive-analysis.md`、`docs/lingtai-comparison-analysis.md` 互补：那三篇聚焦官方能力演进与特定竞品，本文聚焦**产品形态的横向对照与战略取舍**。

---

## TL;DR

1. **CCM 的真实身份**：私有部署的多 agent 交付平台。在"账号池/额度调度、多 provider 深度对等、跨机 Worker 实时迁移、工具调用级人机回路"上领先所有被调研竞品——这些恰是官方云产品**结构性做不了**、开源竞品**普遍空白**的。
2. **2026-08 的 Delivery Loop 是一次重要的自我纠偏**：CCM 起家的"agent 直推 main"模式是全行业孤例（21 个竞品无一默认推主干）；Delivery Loop（Plan → Code → 独立 Review → 浏览器审查 → PR → CI → 合并开关默认关）把 CCM 拉回甚至反超了行业"PR 中心 + 独立验证"的共识水位。**当前的产品问题不再是缺验证，而是双轨并存**：默认的 auto 模式仍是直推 main 的老路径，Delivery Loop 的重管线与之如何分工、谁是推荐默认，缺一个明确的产品答案。
3. **行业 2026H1 三件事**：平台方吞噬工具层（Terragon 关停、Crystal 弃用、Sculptor 退守）；瓶颈从"生成"移到"验证与合并"（review agent 成第二增长曲线）；入口竞争转向 Slack/Linear/事件驱动值守。CCM 在第二件事上已重仓押对，第三件事上几乎空白。
4. **下一步**：P0 收敛交付双轨 + 补出站通知闭环；P1 上游任务源（Linear/飞书/事件触发）与成本可见性；P2 用账号池优势做 best-of-N，并建立显式"不做清单"。总姿态维持 frontier 报告判断：**消费官方原语，不与官方赛跑**。

---

## 一、CCM 产品功能全景（按用户旅程）

### 1.1 任务从哪里来（派发入口）

| 入口 | 形态 |
|---|---|
| Web 任务表单 | 主入口。Project 选择/新建、Mode（Auto/Plan/Loop/Goal/Delivery）、Priority、Model/Effort/Thinking/Codex Fast、Run on（本机/Worker）、Skills 勾选、Copy context from 上游任务 |
| 语音 | Whisper 转文字填表 |
| 项目 Todo 清单 | 每 Project 挂 prompt 模板清单，一键 ▶ Run 建 task 并溯源 |
| Quick Capture | macOS 菜单栏工具 + Chrome MV3 扩展：截图 → 直接建 task 并打开 chat |
| PR Monitor | GitHub webhook 收 PR 事件 → 自动创建审核 run |
| 移动端 | PWA + Capacitor Android APK，全功能 |

### 1.2 任务怎么被执行（调度与执行）

- **GlobalDispatcher + 优先级队列**：自动建 worker 实例、自动领取，9 步生命周期。核心哲学：**Dispatcher 只分配和判成败，git 操作由 agent 自主完成**（auto 模式）。
- **任务模式已形成两档交付强度**：
  - 轻量档：Auto（放手跑、直推 main）、Plan（只读计划→人工审批→执行）、Goal（自然语言完成条件 + 独立评估器）、Loop（Todo 迭代）。Auto 任务可显式冻结 **Capability policy**，让模型在预算内请求 Plan/Code Review 能力调用。
  - 重量档：**Delivery Loop**（2026-08 落地 V1）——持久状态机驱动 Plan → Developer 写码（无凭据、断网、只产生未提交 diff）→ Controller 独占 commit → 独立结构化 Code Review → Test Harness 浏览器审查 → non-force push + PR → Reviewer Panel + exact-head CI → 修复循环 → `ready_to_merge`（auto_merge 默认关）或自动合并。
- **一等 Plan 体系**：Plan 是独立于 Task 的版本化制品；Planner/Reviewer pipeline 严格只读，可暂停请求用户输入，保留不可变 Version 历史，有独立 PlansPage。
- **Test Harness（统一浏览器验证）**：普通对话、一次性测试、Goal 复查、固定 URL、PR/ref 都创建同一种持久 Run；独立黑盒 Browser Agent（不继承父 Task 上下文）收集截图/console/失败请求，证据经 SHA-256 内容寻址归档；PR/ref 测试在临时 Docker Sandbox + 出口白名单代理中执行，绝不在宿主跑不可信提交。
- **双 Provider 对等**：Claude Code 与 Codex CLI（默认 `gpt-5.6-sol`，模型含 `gpt-6-astra`）在生命周期、多轮、Goal、Plan、压缩、重试、账号池、Worker 迁移上全对等；PTY 热会话/ask_user/原生子 agent 为 Claude 专属（显式拒绝而非静默降级）。Codex 有 app-server 常驻低延迟链路与 Fast（priority tier 实证验证）。
- **执行隔离（2026-08 起分层）**：普通管理员任务 unrestricted（Claude 用 `--dangerously-skip-permissions`）；本地 Claude Task 依赖 bubblewrap+socat 的 OS 隔离证明预检（缺失 fail closed）；共享项目的 agent 进程跑在 Docker 容器（cap-drop ALL、read-only、独立 tmpfs）；Delivery Developer 断网、无 git/GitHub 凭据、MCP 全关。

### 1.3 人怎么参与（交互回路）

- **Chat 多轮对话**：`--resume` 续接、运行中注入消息、消息排队、Interrupt、附件、KaTeX、Fork/派生新任务、Distill（对话蒸馏成 Skill）、Session 关注标签。
- **人审卡点**：Plan 审批与交互式输入；PTY 权限透传（工具权限请求变聊天卡片，120s 超时拒绝）；ask_user 拦截（AskUserQuestion 变交互卡片 + 跨页全局通知）；Delivery 的 `ready_to_merge` 终点。
- **可观测性**：实时流式输出、Thinking 展示、Monitor/原生子 agent 统一镜像、实例日志、上下文用量与自动压缩提示、Test Harness 证据（截图/报告/finding 指纹比较）。

### 1.4 平台如何保证"跑得下去"（可靠性层）

- **多账号池（Claude + Codex + CloudRouter/ApexRouter/APIBest API 网关）**：限速/认证失败自动换号、session 硬链接/rollout 复制无缝迁移、5h/7d 额度可视化、Chrome+Xvfb 无人值守自动登录（含 OTP 透传）、API Key 与原生账号统一路由。
- **瞬时 429/过载与额度用尽严格区分**：前者同账号退避重试，后者换号。
- **运维事务**：一键更新/修复/回滚（deployment lease + 启动守卫 + 维护停服门禁）、数据库自动备份、/tmp 看门狗、任务超时保护、Codex 日志库自维护。

### 1.5 横向扩展与协作

- **分布式 Worker**：一键创建 EC2 → rsync 部署 → 独立账号池；任务转发、WS 事件中继、**运行中任务本机↔Worker 实时迁移**、销毁自动迁回。
- **多用户与团队**：邮箱验证码注册、角色体系（super_admin/admin/member）、用户组、任务/项目分享（SharedChat 多人实时共看）、飞书 OAuth 绑定（Team CCM P2P 设计：无中心 Hub、数据不出分享者机器）。
- **周边**：Discussions（多 agent 讨论 + facilitator）、Secrets 管理与引用、Files 页（本地/SSH 远端浏览、diff、.env 编辑）、Skills 页、4+4 主题体系、Cloudflare Tunnel。

---

## 二、设计逻辑与合理性评估

逐条评估 CCM 的关键产品选择，并用竞品事实做对照。

### 2.1 交付模式：从"行业孤例"到"双轨并存"

CCM 起家的默认闭环是 agent 自主 worktree → 实现 → rebase → **merge 进 main → push origin main**。一手证据显示这是**全行业孤例**：11 家商业产品无一默认推主干（GitHub Copilot 甚至硬性禁止——"Copilot cannot push directly to your default branch"，只能推 `copilot/` 分支）；10 个开源产品中最激进的也只是 CCManager 的"人触发的应用内 merge"；Vibe Kanban（28k★）明确 "open PRs… review on GitHub, and merge"。行业把人留在 merge 键上，是对实证结论（agent 自述不可信、91.49% 问题需用户显式纠正，见 frontier 报告）的产品化回应。

2026-08 的 **Delivery Loop 实质上承认并回应了这一点**：它的管线（独立 Review、黑盒浏览器审查、exact-head CI、auto_merge 默认关）在验证强度上已超过多数商业竞品——Jules 的 Critic、Cursor 的 Bugbot Autofix 都没有"Developer 无凭据只产 diff、Controller 独占 commit"这一层权限收缴。

**遗留的产品问题是双轨心智**：默认建出来的仍是直推 main 的 auto task；Delivery Loop 走独立 API/入口、概念负载重（Run/Cycle/Capability/Turn）。"什么时候该用哪条轨、平台推荐哪条"没有产品化答案。合理路径不是废掉直推（单人高信任场景它确实高效），而是：(a) 把交付强度做成任务创建时的**显眼一级选项**并给推荐默认；(b) 从 Delivery 管线里拆出可单独启用的轻量件（如"完成前必跑一轮独立 review"）下放给 auto 任务——Capability policy 已经是这个方向的机制雏形，缺的是默认开启的产品姿态。

### 2.2 执行隔离：短板已大幅补齐，剩余暴露面是刻意选择

早期 CCM 的"全程 skip-permissions + 无沙箱"曾是与主流最大差距（frontier 报告 P2）。当前状态已分层：本地 Claude Task 有 bubblewrap OS 隔离证明（缺依赖 fail closed）；共享项目跑 Docker 容器（防跨用户读号池凭据/其他项目/数据库，`docs/sandbox-audit-and-removal-plan.md` 已逐处审计并论证不可删）；PR/ref 测试进临时 Sandbox + 出口白名单；Delivery Developer 断网无凭据。剩余暴露面——**管理员自己的普通任务 unrestricted**——是效率上的刻意选择，与"owner 即 reviewer"的场景一致，README 首屏也如实披露。对照：商业侧凭证隔离是共同卖点（Anthropic git 凭证代理、Codex 执行期默认断网、OpenHands 8 小时 token），CCM 的差距已从"缺一层"缩小到"信任边界画法不同"。**判断：现状合理；下一步优先级从 P2 上调空间不大，剩余项是 secrets 明文可见性（agent 拿引用不拿明文）**。

### 2.3 「PTY 持久会话 + 权限透传 + ask_user」 — 合理且是真差异化

行业的"中途 steering"全部是消息级（Slack 线程回复、PR 评论）；CCM 做到了**工具调用级**的人机回路（权限卡片实时允许/拒绝、AskUserQuestion 变交互卡片），加上热 session 免冷启动与后台工作不阻塞主会话。没有任何被调研产品做到这一层，且与 Happy（23.6k★，靠"权限请求推送"立身）验证的需求方向一致。**判断：合理，保持投入**；短板在触达（见 4.2）。

### 2.4 「多账号池 + 额度调度 + 自动登录」 — 全行业空白，最强护城河

开源调研明确结论："账号/额度管理是普遍空白……没有产品做多账号池/额度调度"（10 个产品全部 "bring your own subscription"）。商业产品结构上不可能做（它们卖的就是自家额度）。CCM 的号池（限速检测、无缝 session 迁移、额度可视化、无人值守登录、三家 API 网关统一路由）是**唯一深耕此处的产品**，直接兑现"榨干订阅、并行拉满"的核心用户价值。**判断：完全合理，最不可复制的资产**；风险在其依赖对 CLI 报错文案/登录流程的逆向，脆弱面大（frontier 报告 P1"换官方原语"持续适用）。

### 2.5 「分布式 Worker + 运行中任务实时迁移」 — 超出行业水平，但边际投入应收敛

Conductor Cloud、Claude Code cloud 给的是"云上开新会话"；CCM 给的是**运行中的会话连 session 带工作区在机器间搬**。技术含金量最高。但目标用户（单机并发不够、又愿意自管 EC2 的重度个人/小团队）很窄，维护成本极高（CLAUDE.md 中 Worker 约定的篇幅可证）。**判断：当"已完成的天花板能力"维护，不再加码。**

### 2.6 「多 Provider 对等」 — 方向被行业验证，深度优于广度

GitHub mission control 接入 Claude/Codex 第三方 agent、Conductor 聚合四家、Vibe Kanban 支持 10 种 CLI——"选哪个 agent 变成下拉框"是确定趋势。CCM 只有 2 种但做到生命周期/Goal/迁移/账号池全对等、互为限流熔断备份，比竞品"能启动即算支持"深一个量级。**判断：合理；按 multica 分析定下的"深度优先"，暂不扩广度。**

### 2.7 「Goal + Monitor + Test Harness」 — 验证体系已从"自述"升级到"证据"

早期 Goal 评估器只读对话摘要，落在"agent 自述不可信"陷阱里。现在 Test Harness 把 Goal 复查接上了黑盒浏览器验证（独立 Agent、不继承父上下文、证据哈希归档），与 Cursor artifacts（"videos, screenshots, and logs for validation rather than diff-only reviews"）、OpenHands Stop hooks（"block completion until … tests pass"）代表的行业方向一致，且证据链严谨度更高。**判断：方向正确且已领先；剩余缺口是非前端任务的执行性证据**（测试退出码/lint/CI 作为完成门槛在 auto 模式的普及）。

### 2.8 「Web 全功能 + PWA/Android + 截图建任务」 — 合理，形态领先大部分开源竞品

行业移动端在铺（Cursor iOS、Claude 移动监控、Conductor "launching soon"），CCM 的移动全功能 + 截图直达建任务领先。**判断：维持。**

### 2.9 功能发散度 — 单人项目的隐性成本在累积

Discussions、4+4 主题（含飞书像素级取色）、SSH 工作台、KaTeX、Distill、Quick Capture、多数据库、Delivery/Plan/Harness 三大新体系……广度已是团队级产品。代价可见：`SharesPage.tsx` 写了未接路由（死代码）；CLAUDE.md 约定膨胀到 570+ 行超长行；概念数量（Task/Run/Cycle/Capability/Invocation/Turn/Harness Run/Plan Version…）对新用户的学习曲线陡峭。Crystal/Terragon 的死亡说明这个赛道**死于维护不起的广度**多于死于功能不足。**判断：需要显式"不做清单"与收敛机制**（Team CCM 设计"明确不做三件事"是好先例）。

---

## 三、竞品格局速览（2026-09-05 一手证据）

### 3.1 开源/自托管同类

| 产品 | Stars | 一句话定位 | 与 CCM 的关系 |
|---|---|---|---|
| Vibe Kanban (BloopAI) | 28k+ | Kanban 编排 10 种 agent CLI，UI 内 inline comment 回喂 agent，PR 交回 GitHub | 最直接竞品；赢在看板心智 + agent 广度，无账号池/无调度器/无验证管线 |
| Happy (slopus) | 23.6k | 端到端加密的 Claude Code/Codex 移动遥控 + 权限审批推送 | 只做遥控层；验证了移动审批需求 |
| claudecodeui (siteboon) | 13.6k | ~/.claude 会话自动发现的远程 GUI，工具默认全禁用 | 只做 GUI 层，安全姿态与 CCM 相反 |
| Claude Squad (smtg-ai) | 8.4k | tmux+worktree 极简 TUI 并行 | 单机单人，无编排 |
| Crystal→**Nimbalyst** | 3.1k(旧) | **2026-02 弃用重生**：可视化工作区，worktree 改一键可选 | 赛道剧变的证据 |
| Omnara | 2.8k | **已转型**托管 agent 运行时（"open-source alternative to Claude Managed Agents"），唯一有组织/角色/API key 权限体系 | 从遥控层逃向云运行时的证据 |
| CCManager (kbwo) | 1.2k | 全功能 TUI：状态 hook + 应用内 merge worktree | 状态 hook 通知思路可借鉴 |
| parallel-code | 1.0k | "Dispatch agents in parallel… **merge the wins, toss the rest**"（best-of-N 择优） | 质量策略创新方向 |
| claw-orchestrator | 568 | Multi-Agent Council 投票共识 + MCP/ACP 双协议 | 同上 |
| MS Conductor | 422 | YAML/DAG 确定性多 agent 工作流，工作流本身进 PR | CI/CD 化思路 |

**开源侧共性**（原文证据支撑）：worktree 是隔离事实标准（7/10）；review 归人、merge 保守是普遍默契；通知/审批是移动端立身之本；**账号池全行业空白**；团队协作刚起步；赛道剧烈洗牌（Crystal 弃用、Omnara 转型）。

### 3.2 商业/云端

| 产品 | 定位与关键事实 |
|---|---|
| Claude Code on the web | 官方云执行，订阅内含不另收算力费；teleport 云→本地接管；Auto-fix PR 盯 CI/评论自动修；凭证代理不进沙箱 |
| OpenAI Codex cloud | 每任务云沙箱、**执行期默认断网**；先审 diff 再开 PR；`@codex review` 只报 P0/P1 |
| Cursor Cloud Agents + Bugbot | 入口最杂食（Slack/Teams/Linear/Jira/Sentry/PagerDuty/cron 自动化）；artifacts（视频/截图/日志）交付；Bugbot Autofix review→fix 闭环；唯一支持自带 Dockerfile |
| GitHub Copilot cloud agent | Actions 环境、59 分钟上限、只能推 `copilot/` 分支；mission control 接入 Claude/Codex 第三方 agent（平台化） |
| Devin | 编排最强：coordinator 拆解→managed Devins 并行、可给子会话发消息设额度；计费从 ACU 转席位+积分 |
| Google Jules | 消费级：按任务数计费；人审最重（前置 plan 审批 + 内置 Critic 反方 agent）；仅个人账户 |
| Conductor (conductor.build) | Mac 本地 worktree 起家 + 2026-07 加云；免费本地获客、Pro $50 云协作；BYO 订阅 |
| **Terragon** | **已关停**（约 2026-01，开源快照留存）——纯云端第三方编排器被证伪一次 |
| Sculptor (Imbue) | 从"容器隔离踩 worktree"**反转为默认 worktree**，退为免费开源 research preview |
| Factory (Droids) | 企业自主度矩阵（Spec/Mission × Off–High）；Droid Computers 持久云环境；Code Review Droid P0–P3 |
| OpenHands Cloud | 零加价推理；`.openhands` 目录把 Skills/setup/**Stop hooks 质量门禁**编码进仓库 |

**商业侧共性**：PR 中心无一例外；人从"事前审计划"滑向"事后审 PR + 随时插话"；review agent 成独立 SKU（Bugbot、Code Review Droid、@codex review、Critic）；环境收敛到"setup 脚本 + 快照 + AGENTS.md"；计费向美元锚定 credits 简化；**平台方吞噬工具层**（Terragon 关停时点紧邻 Claude Code on the web 上线）。

---

## 四、CCM 的优点与设计缺陷

### 4.1 优点（按护城河深度排序）

1. **多账号池与额度调度** — 21 个被调研产品中唯一。商业产品结构性不做（卖自家额度），开源全部空白。配合无人值守登录与无缝 session 迁移，直接兑现"榨干订阅、并行拉满"的核心价值。
2. **验证管线的严谨度**（Delivery Loop + Test Harness）— Developer 无凭据只产 diff、Controller 独占 commit、黑盒浏览器审查、证据哈希归档：权限收缴与证据链强度超过 Bugbot/Critic 一档。这是 2026-08 之后新增的第二深护城河。
3. **多 provider 深度对等 + 互为熔断** — 竞品的"多 agent 支持"是能启动即算；CCM 做到全生命周期对等。在模型高频跃迁、单家限流频发的 2026，这是可靠性资产。
4. **跨机 Worker 与运行中任务实时迁移** — 官方云做不了（不碰用户机器），开源没人做到。
5. **工具调用级人机回路** — 权限透传卡片、ask_user 卡片、运行中注入、Interrupt；行业只有消息级 steering。
6. **深度可观测** — thinking、原生子 agent 镜像、实时流、上下文用量、Harness 证据；对照 Multica"输出只有评论"的短板。
7. **私有部署 + 数据不出域** — 对照 Claude Code web 的 GitHub 强绑与 ZDR 不可用；与 Happy 的加密叙事同一需求面。
8. **生产级运维事务**（更新 lease/回滚/备份/看门狗）— 自托管产品中罕见的严肃度。

### 4.2 设计缺陷（按风险排序）

1. **交付双轨无产品化答案**：默认 auto 直推 main（行业孤例）与重管线 Delivery Loop 并存，强度选择、推荐默认、轻量件下放均未定义；Capability policy 有机制无姿态（默认不带）。价值已建成，**呈现和默认值没跟上**。
2. **上游任务源缺位**：Slack @提及、Linear/Jira assign、GitHub issue label 已是商业侧第一梯队标配；CCM 的任务全部产生于自家 UI。Linear 调研（`docs/linear-integration-research.md`）已论证可行且成本低（复用 PR Monitor webhook 模式），未实施。
3. **出站通知缺位**：ask_user/plan 待审批/任务完成只有站内弹窗，无飞书/邮件/推送出站通道。对一个主打"放手跑、需要你时叫你"的产品这是关键闭环缺口（Happy 23.6k★ 靠这一件事立身）；飞书 OAuth 绑定已有，只差消息通道。
4. **无事件驱动值守**：Cursor Automations（Sentry/PagerDuty/cron）、Anthropic routines 代表"agent 接进告警流"趋势；CCM 的 Monitor 子 agent 有基础设施，没有外部事件触发入口。
5. **成本/token 不可见**：竞品全面转向美元锚定计量；CCM 只有账号额度条，没有 per-task/项目级 token 与成本统计（multica 分析列为 P0，至今未做）。
6. **广度与概念负载失控风险**：SharesPage 死代码、570+ 行 CLAUDE.md、Task/Run/Cycle/Capability/Harness 概念丛林、单人维护 20+ 子系统。Crystal/Terragon 之死是前车之鉴。
7. **秘密明文可见**：管理员普通任务下 agent 仍可见 secrets 明文；"引用不给明文"的代理层未做（隔离体系其余部分已补齐，见 2.2）。
8. **官方吸收风险持续**：teleport/--cloud、Agent teams、routines、Managed Agents 都在向编排层扩张。护城河（账号池/验证管线/跨机/多 provider/私有部署）之外的功能都可能被官方免费内含。

---

## 五、下一步怎么走

延续 frontier 报告"消费官方原语、锚定官方做不了的事"的总姿态，结合本次横向对照：

### P0 — 把已建成的价值变成默认体验（缺陷 #1/#3）

1. **交付强度一级化**：任务创建表单把"直推 / PR（Delivery）/ 本地 diff"做成显眼选项并给出推荐（如：共享项目与 member 任务默认 Delivery；单人项目默认直推但展示风险徽标）。
2. **Delivery 轻量件下放**：把"完成前必跑一轮独立 Code Review"作为 auto 任务的一键开关（机制上即 Capability policy 预设模板），对标 `@codex review` 的 P0/P1 聚焦策略控噪音。
3. **飞书出站通知**：ask_user/plan 待审批/`ready_to_merge`/失败推飞书（OAuth 已有，只差消息通道）；这同时是 Team CCM 的自然延伸，也是移动端"放手跑"体验的最后一块。
4. **非前端任务的执行性完成证据**：测试退出码/lint/CI 状态进入 auto 任务完成判定与 Goal 评估输入（前端侧 Harness 已解决）。

### P1 — 上游入口与可见性（缺陷 #2/#4/#5）

1. **Linear 集成 Phase 1**（webhook 导入 Issue + 状态回写，调研已完成、预估 2–3 天）；MCP config 注入 Linear 官方 MCP 零成本先行。
2. **事件驱动任务**：从 PR Monitor webhook 框架泛化"事件→模板→建 task"通道（GitHub issue label、CI 失败、定时），对标 Automations/routines 的最小子集。
3. **per-task token/成本统计**：Codex 已有 context_usage，补 Claude 侧并出任务/项目/账号三级汇总页。

### P2 — 杠杆与收敛（缺陷 #6/#7）

1. **best-of-N 实验**（frontier P1 维持）：同题多实例 + 蒸馏摘要后锦标赛。账号池恰好是跑 best-of-N 最便宜的基础设施，Delivery 的独立 Review 恰好是现成的裁判——这是把两条护城河拼成质量优势的杠杆；开源侧 parallel-code/claw-orchestrator 已验证需求方向。
2. **Secrets 代理**：agent 拿引用不拿明文，补齐隔离体系最后一块。
3. **显式"不做清单"**：不做 10 种 runtime 广度、不做 Skill 市场、不做中心化云服务、不再扩 Worker 云厂商、主题体系冻结；清理死代码（SharesPage 接入或删除）。

### 持续有效的战略判断

- **锚定官方结构性做不了的事**：账号池、跨机 Worker、多 provider 互备、私有部署团队面板——现在再加上**自持验证管线**（官方的 review 只服务自家模型生态）。
- **深度优先**（multica 分析结论维持）：2 个 provider 做透 > 10 个能跑。
- **单人维护是真实约束**：每个新面都要回答"谁来养"；Crystal/Terragon/Vibe Kanban 开源版的命运说明这个赛道淘汰的是养不起广度的产品，不是功能少的产品。

---

## 附：证据缺口备忘

竞品调研中未取得一手证据、以替代来源覆盖的条目：openai.com/codex 产品页（403，以 learn.chatgpt.com 官方文档替代）、devin.ai/pricing（429，以 docs.devin.ai 计费页替代）、Terragon 历史定价（文档站证书过期）、Codex iOS 入口（404）、Conductor 通知机制、Copilot capabilities-and-limitations 页（404）、Omnara 早期"移动 mission control"定位（当前官网已不可见，仅现定位有据）。引用本文结论时，上述条目请注意口径。
