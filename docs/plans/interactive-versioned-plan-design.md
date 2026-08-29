# 交互式、版本化 Plan 架构与实施计划

> 状态：已实施，待生产环境迁移与手工验收。
>
> 2026-08-02 决策：Plan 从 `Task(mode="plan")` 中提升为稳定的一等制品；一个主
> Task 可以有多个 Plan，一个 Plan 可以有多个不可变 Version，一次实际规划工作是一个
> 可暂停/恢复的 Pipeline Run。Planner/Reviewer 的必要提问恢复同一个 Run；用户在成品
> 方案上请求 Revise 时创建新 Run 和新 Version，但不创建新 Plan。
>
> 代码已切换到本文的一等 Plan/Version/Run/Input/Application 架构；旧
> `origin/main` 能产生的 `Task(mode="plan")` 数据在迁移后通过 legacy link 解析，前端和所有
> 新产品写入只使用 canonical Plan API。每个 Main carrier 只迁移为一个 Plan、至多一个 v1；
> 本功能分支早期实现产生的 revision chain、独立 Plan/Run/Input/Application 测试数据不属于
> 生产历史，由 cutover reconciliation 清理。通用 `POST /api/tasks` 已拒绝 `mode=plan`，窄化的
> 旧读写 API 仅在 contract 观察期服务历史客户端。生产行为仍取决于是否已部署本提交。
> 2026-08-03 信息架构决策：新增独立顶级 **Plans** 页面；**Tasks** 页面和 New Task
> 表单只承载真正的 Task，不再通过 Task 类型筛选展示 Plan。不会新增“执行 Task / Plan 先行”
> 滑块。

> 2026-08-02 实施复核：全局行动区统一命名为 `Plans requiring action`；Planner/Reviewer
> 只能在全局 Settings 配置，新 Plan 冻结完整路由快照；每 Run 的用户交互轮数是独立的
> `0–5` 全局设置，单轮问题数量仍无业务上限。Plan 创建请求、标题、Revise/Fork 请求和
> 回答因持久化而拒绝高置信 API key/token/private-key 文本，并引导使用 Secrets 引用。

## 0. 决策摘要

当前实现已经有 `PlanAgentRun` 和 `PlanAgentStep` 审计记录，但用户可见 Plan、调度 Task、
审批状态、最终内容和一次 Pipeline 生命周期仍集中在一条 `Task(mode="plan")` 上。结果是：

- Planner/Reviewer 只能一次性返回方案或审查结论，没有持久化的用户输入出口；
- 用户点 Revise 会 supersede 旧 Plan Task 并创建新 Plan Task，稳定的 Plan 身份丢失；
- `plan_content` 只有最新值，早期 Planner 输出只是截断后的 step 日志，不是可审批版本；
- Task 状态、Plan 决策状态和 Pipeline 执行状态共享同一套字段，容易产生 owner、busy、
  Worker 迁移和前端筛选上的耦合；
- approve/apply 绑定 Plan Task，而不是绑定用户实际看到的不可变方案版本。

目标模型为：

```text
Task / Session
├─ Plan A
│  ├─ PlanVersion v1
│  ├─ PlanVersion v2
│  ├─ PlanRun #1 (initial)
│  │  ├─ PlanStep: planner → v1
│  │  ├─ PlanStep: reviewer → revise
│  │  ├─ PlanInputRequest（可选）
│  │  ├─ PlanStep: planner → v2
│  │  └─ PlanStep: reviewer → approve
│  ├─ PlanRun #2 (user_revision) → v3
│  └─ PlanApplication（精确引用某个 version）
├─ Plan B
└─ Plan C

Standalone Plan
└─ target_task_id = NULL，其余 Version/Run/Step 语义相同
```

核心规则：

1. **Task 是主 Session/执行工作，Plan 是规划制品，Run 是后台执行。** 三者不共享状态。
2. **Plan ID 稳定。** 普通 Revise 不再创建新 Plan；只有用户明确新建/分叉规划目标时才创建。
3. **Version 不可变且只代表完整 Pipeline 产物。** Planner 的中间输出先作为 Run-scoped
   candidate draft 保存；Reviewer 的内部对抗轮次只替换 candidate。Pipeline 完成时才原子创建
   一个 Version。
4. **Reviewer 审查精确 candidate，用户操作绑定精确 Version。** Reviewer 最终结论与 candidate
   一起发布；用户审批、应用和执行 Task 都绑定 `plan_version_id`。
5. **澄清不是修订。** Planner/Reviewer 缺少必要输入时暂停当前 Run；回答后恢复同一 Run，
   不创建新 Plan，也不创建新 Run。
6. **成品 Revise 是新 Run。** 用户对 reviewable/approved/rejected Version 提交修改意见时，
   在同一 Plan 下启动新的 Run，产出下一 Version。
7. **逻辑连续不依赖原生 Session。** 每个 Claude/Codex Step 仍是 disposable、严格只读的
   独立调用；恢复时从数据库重建完整上下文。
8. **等待用户不占资源。** `waiting_user` Run 不持有 Instance、进程、账号、Codex thread
   或部署 blocker。
9. **主 Task 独立。** Plan 创建、运行、等待、审批、拒绝和失败都不改变目标 Task 的状态、
   session 或消息队列。
10. **Apply 仍由真实用户消息触发。** Approve 不自动回填主 Session；应用成功只在真实
    user message 已持久化并成功准入时成立。

## 1. 目标与非目标

### 1.1 目标

- 一个主 Task 同时拥有多个互相独立的 Plan；
- 一个 Plan 保留完整、可导航、不可变的版本历史；
- Planner 和 Reviewer 都能以结构化方式请求必要用户输入；
- 用户回答可带文本、选项和最多 10 个现有安全上传附件；
- 服务重启、WebSocket 断线、Worker 短暂离线后仍能恢复等待状态；
- 用户 Revise 不再制造新的用户可见 Plan/Task；
- Reviewer 自动 revision round 留在同一个 Run 内；只有用户主动 revision/refresh Run 才产出
  下一个 Version；
- 审批、拒绝、应用、创建执行 Task 都有精确 Version 审计；
- 保留当前 primary/fallback 选号、只读沙箱、staleness、ACL、Worker 和部署安全约束；
- 精确迁移 `origin/main` 可产生的单 carrier Plan Task、审批和执行状态，不承诺保留本功能分支
  早期 schema 产生的 revision/application 测试历史。

### 1.2 非目标

- 不把 Plan 变成可写代码的 Agent；
- 不 resume 主 Task 的 Claude/Codex 原生 thread 进行规划；
- 不长期保留 Planner/Reviewer 原生 thread；
- 不在 approve 时自动发送 ACK 或模型 turn；
- 不让模型通过 Claude `AskUserQuestion` hook 或 Codex 主线程 steer 直接绕过持久 API；
- 不自动合并多个 Plan 或隐式选择 latest-wins；
- 不在第一阶段提供任意动态 Pipeline DAG；本期仍是 Planner → Reviewer → 可选 revision；
- 不允许用户输入明文 API key、token 或其他 secret；需要凭据时只能提示用户使用现有
  Secret 配置能力。

## 2. 术语与层级

### 2.1 Task

现有主会话和最终执行工作。Task 可以拥有多个关联 Plan，但不保存 Plan 的内容、审批、
应用或 Pipeline 状态。Standalone Plan 没有目标 Task，但仍关联 Project/repo/Worker。

### 2.2 Plan

用户可见的稳定规划主题，是版本、Run、输入请求和应用记录的聚合根。Plan 保存：

- 标题、初始规划请求、创建者；
- `target_task_id`（NULL 表示 standalone）；
- Project、repo、branch、Worker 归属；
- 当前 Version 和当前 active Run 指针；
- 归档/关闭信息；
- 创建、更新时间。

Plan 不保存“planning/reviewing/waiting”这类运行状态；API 根据 active Run、当前 Version 和
决策记录生成 `display_state`。如需列表查询性能，可维护受 CAS 保护的投影字段，但它不是
独立事实来源。

### 2.3 PlanVersion

一次完整 Planner/Reviewer Pipeline 的最终、不可变方案快照。它保存：

- `plan_id`、单调递增的 `version_number`；
- `parent_version_id`；
- `produced_by_run_id`、`produced_by_step_id`；
- 完整 Markdown `content`；
- 此方案实际使用的主 Task 对话截止点、session 快照和 repo 指纹；
- Reviewer 最终 verdict/feedback；
- 人类 decision 及审批审计；
- `superseded_by_version_id`（仅表达“不是当前版本”，不删除历史决策）。

同一 Plan 下 `(plan_id, version_number)` 唯一。Version 正文和上下文一经创建不得更新；
Reviewer 结果与正文在 Version 发布时一并确定；人类 decision 和 superseded 指针只允许各自完成
一次受 CAS 保护的状态转换。Reviewer 在同一 Run 内要求修改只替换 candidate，不创建 Version；
用户显式启动会改变成品正文的新 Run 才创建下一个 Version。

### 2.4 PlanRun / Pipeline Run

一次实际规划请求。Run 类型：

- `initial`：创建 Plan 后首次规划；
- `user_revision`：用户基于某个 Version 提交修改意见；
- `refresh_context`：用户明确要求用最新主 Session/repo 重新规划；
- `retry`：仅用于管理员显式重跑 terminal operational failure，保留来源 Run。

Run 保存冻结的 Pipeline 配置、request/feedback、base Version、上下文快照、Worker 路由、
当前 stage/round、generation、完整 candidate draft/来源 Step/repo 指纹、结果 Version 和错误。
一个 Plan 同时最多一个 active Run。

“Pipeline”专指 Planner/Reviewer 的定义和配置；“PlanRun”才是这套定义的一次执行实例。
修改全局 Pipeline 设置不会漂移已创建 Run。

### 2.5 PlanStep

单次 Planner 或 Reviewer 模型调用，继续沿用当前 route、account、provider/model/effort、
output/error 和起止时间审计。Step 的原始输出可截断用于诊断；当前完整候选保存在 Run，
Pipeline 完成后复制到 PlanVersion，不能依赖 step 日志恢复。

### 2.6 PlanInputRequest

一个持久化的阻塞式用户输入请求，包含当前阶段已知的全部必要问题。每个请求至少一个问题，
但不设置问题数量的业务上限；它绑定 exact Run 和来源 Step，保存：

- `requested_by=planner|reviewer`；
- 问题 schema、请求原因；
- `open|answered|cancelled`；
- 结构化 answers、可选补充文字、附件；
- answer 用户和时间；
- 幂等键。

第一版一个 Run 同时最多一个未终结 InputRequest（prepared/open）。一次请求回答后不可编辑；
需要纠正时由用户提交新的 revision Run，或由模型在同一 Run 创建下一次 InputRequest。

### 2.7 PlanApplication

记录某个精确 Version 被用于真实 user message 或 standalone execution Task：

- `application_type=chat_message|execution_task`；
- `plan_id`、`plan_version_id`；
- 目标 Task/session 快照；
- `user_log_id` 或 `execution_task_id`；
- 操作人和时间。

每个 Version 最多成功应用一次，以保持当前“不会重复自动携带”的产品语义；同一个 Plan 的
后续新 Version 可以再次显式应用。一个 user message 可以携带来自多个 Plan 的多个 Version。

## 3. 用户行为语义

### 3.1 创建关联 Plan

1. 用户在主 Task 的 Plans modal 输入规划请求和附件；
2. 服务端创建 Plan 和 `initial` Run；
3. Run 快照目标 Task 当前对话、session、repo 和 Pipeline 配置；
4. Dispatcher 唤醒，主 Task 状态和消息队列不变；
5. 同一目标 Task 最多存在 3 个 open Plan work items：active Run 或 open InputRequest 计入，
   已完成且等待人工审批的 Version 不占运行并发，但继续出现在 review 区域。

### 3.2 创建 standalone Plan

使用相同 Plan API，只是 `target_task_id=NULL`。Plan 保存 Project/repo/branch/Worker 选择。
批准某个 Version 后可以从该 Version 创建一次 execution Task；Plan 和 Version 保持只读历史。

### 3.3 Planner 请求输入

Planner 必须先读取允许访问的 repo/context。只有无法从代码或已有上下文确定、且会实质改变
方案的用户偏好/约束才允许请求输入。

Planner 返回 `request_input` 后：

1. Step 正常完成；
2. 事务内创建 `prepared` PlanInputRequest，Run 仍保持 running 和 exact owner；
3. 生命周期清理 disposable thread/process；
4. cleanup 被精确确认后，在同一事务发布 `open` request、把 Run 置为 `waiting_user` 并释放
   Instance owner；
5. 前端在 `Plans requiring action` 的 `Input needed` 分组显示请求；
6. 用户回答后 Run 原子地回到 `queued`；
7. 新 Planner Step 使用原请求、冻结上下文、全部既有问答、base Version 和当前 candidate
   继续规划。

选择题的模型选项不是强制穷举集合。若所有选项都不适用，用户可以保持未选择并在补充说明中
给出替代答案；服务端只在补充说明非空时接受 required choice 的空结构化值，恢复后的
Planner/Reviewer 必须把 `null choice + additional response` 作为一个明确答案处理。

如果提问发生在第一版方案之前，不创建空 Version。

### 3.4 Reviewer 请求输入

Reviewer 只能在某个明确 candidate 上提问。回答后回到同一个 Run，由 Planner 把用户决定写进
更新后的完整 candidate，再交 Reviewer 复审。回答在 active Run 详情即时展示；Pipeline 完成后
通过 `produced_by_run_id` 成为最终 Version 的输入审计。这样批准的 Markdown 本身包含所有关键
决定，不会依赖不可见的问答历史才能正确实施。

### 3.5 Reviewer 要求修改

Reviewer 的 `revise` 反馈仍在同一个 Run 内返回 Planner；Planner 下一次成功输出覆盖 Run 的
candidate，不增加 Version number。达到 `max_revision_cycles` 后，最新 candidate 以一个
`review_exhausted` Version 发布并交给用户，不自动失败。

每个 Reviewer Step 都是独立调用；下一轮 Reviewer 必须同时收到上一轮 Reviewer feedback，
逐项验证闭环后再执行完整审查，不能假定新 Reviewer 记得上一轮输出。

### 3.6 用户请求 Revise

用户从当前或历史 Version 发起 Revise 时：

- 必须提交 `base_version_id` 和 `expected_current_version_id`；
- 服务端在 Plan operation lock 内确认没有 active Run；
- 创建 `user_revision` Run，而不是 Plan/Task；
- 新 Run 默认重新快照主 Task 当前对话和 repo；
- 旧 Version、Reviewer 记录、审批和 application 历史不变；
- 新 Run 的 Planner 与 Reviewer prompt 都明确包含 Plan 初始请求、Run 类型、增量 revision
  语义、base Version、base 审查状态和用户 feedback；未被用户撤销的原始要求与 base 中合理
  决策继续属于 scope。Reviewer 必须比较 base 与 candidate，检查未授权删除、回退和扩 scope；
- Plan 初始附件与当前 Run 附件都进入有来源标记的引用清单，Manager 本地与 Worker 路径语义
  一致。

如果当前 Version 已变化，返回 409，让用户确认基于新版本重写反馈，不能静默从旧 base 分叉。
显式 Fork 操作除外：Fork 创建新 Plan，并记录 `forked_from_version_id`。

### 3.7 Approve / Reject

- Approve 和 Reject 只作用于当前、reviewable Version；
- Reviewer 的 `approve` 只是模型审查结论，不等于人类 Approve；
- staleness 首次返回 409，显式确认后才能决定；
- Approve 不启动模型、不唤醒主 Task、不自动 Apply；
- Reject 将当前 Version 的人类 decision 设为 rejected；之后仍可在同一 Plan 上 Revise；
- 历史或 superseded Version 默认只读，管理员审计 API 仍可查看。

### 3.8 Apply

Chat composer 提交 `plan_version_ids`。服务端必须确认：

- Version 当前已由用户批准；
- Plan 属于当前目标 Task；
- Version 尚未应用；
- ACL、routing 和 staleness confirmation 有效；
- 用户消息及附件已通过现有验证。

用户消息、PlanApplication 和 queued message admission 必须保持现有的原子/补偿语义：只有真实
user message 已持久化且成功进入执行队列后才保留 application；准入失败回滚。模型 prompt
使用不可变 Version 快照，UI user message 同样展示该快照。

## 4. 状态机

### 4.1 PlanRun

Run 使用稳定的 terminal/active 状态，stage 单独存储：

```text
queued
  └─► running(stage=planner)
          ├─► waiting_user ──answer──► queued
          ├─► queued(stage=reviewer) ─► running(stage=reviewer)
          │       ├─ revise ─────────► queued(stage=planner, round+1)
          │       ├─ request_input ──► waiting_user
          │       └─ approve/exhausted ─► completed
          ├─► failed
          └─► cancelled
```

允许状态：`queued|running|waiting_user|completed|failed|cancelled`。

关键不变量：

- `running` 必须有精确 generation 和 Instance/process/thread owner 证据；
- `waiting_user` 不得有任何运行 owner；
- `waiting_user` 必须引用一个 `open` InputRequest；
- `completed` 必须引用 `result_version_id`；
- `failed/cancelled` 不发布本 Run 的 candidate Version；Run 审计和此前 Version 仍保留；
- terminal Run 永不复活，重试创建新 Run；
- 一个 Plan 最多一个 `queued|running|waiting_user` Run。

### 4.2 PlanVersion

Version 内容不可变，两个正交维度分别记录：

- Reviewer：`approve|exhausted|disabled`（旧迁移数据可以保留 `unreviewed|revise`）；
- 人类 decision：`pending|approved|rejected`。

新 Version 成为 `plans.current_version_id` 时，旧 current Version 写
`superseded_by_version_id`，但旧 decision/application 不改变。只有 current Version 能进入
全局 `Plans requiring action` 的 review/execute 分组。

API 另行派生 Version 的用户可见 `display_state`，不得把正交字段直接拼成标签。优先级为
`applied → rejected → approved → superseded → awaiting_review → draft`。`draft` 仅用于兼容旧迁移
数据；新 Pipeline 的中间 candidate 不进入 Version 列表。`pending + applied` 属于数据不变量
破坏，必须修复而不能展示。

### 4.3 Plan 展示状态

API 按以下优先级派生 `display_state`：

1. archived；
2. active Run running/queued；
3. active Run waiting_user；
4. current Version 的 Reviewer 状态属于 `approve|exhausted|disabled`，且 human decision
   pending → awaiting_review；
5. current Version approved 且未应用 → approved；
6. current Version applied → applied；
7. current Version rejected → rejected；
8. 没有可决定的 current Version，且 latest Run failed/cancelled → failed/cancelled；
9. draft。

API 另行返回 `latest_run_status` 和 `latest_run_error`。例如已应用 v1 后用户尝试生成 v2 但 Run
失败，主展示仍可保留 v1 的 applied 状态，同时明确展示“latest revision failed”警告。前端
不得从旧 Task status 猜 Plan 状态。

## 5. 结构化模型协议

### 5.1 Planner schema v2

Planner 返回严格 discriminated union：

```json
{
  "action": "propose",
  "plan": "# 完整 Markdown 方案"
}
```

或：

```json
{
  "action": "request_input",
  "reason": "为什么缺少这些信息会阻断可靠规划",
  "questions": [
    {
      "id": "deployment_database",
      "header": "数据库",
      "question": "目标生产数据库是哪一种？",
      "response_type": "single_choice",
      "options": [
        {"value": "sqlite", "label": "SQLite"},
        {"value": "postgresql", "label": "PostgreSQL"}
      ],
      "required": true
    }
  ]
}
```

### 5.2 Reviewer schema v2

Reviewer verdict：

- `approve`：当前 Version 可交用户审批；
- `revise`：提供 Planner 可执行的具体 feedback；
- `request_input`：提供必要问题和 reason，回答后回 Planner；

字段使用严格 schema，`extra=forbid`；非法、缺字段或同时返回 plan/questions 时显式失败。

### 5.3 问题约束

- 每次至少一个问题，不限制问题数量；Planner/Reviewer 应在一次请求中合并当前已经知道的
  全部必要问题，不能为了规避数量而拆成多轮；
- `response_type=text|single_choice|multi_choice`；
- choice 数量 2–5，value 在同一问题内唯一；
- header 最长 20、question 最长 2,000、reason 最长 4,000；
- 仅使用结构化输出总大小、单字段长度和现有模型输出上限保护传输/存储，不以问题条数拒绝；
- 每个 Run 默认最多 3 次用户交互，可在全局 Pipeline 设置中配置 0–5；
- 达到上限仍请求输入时 Run 失败并保留 candidate 审计，但不发布新 Version、不伪装成
  reviewable；
- prompt 明确禁止询问能从 repo 获取的信息、无关偏好、secret 或扩大任务权限的问题。

### 5.4 恢复 Prompt

每次恢复使用有界、结构化上下文：

1. Plan 初始请求及附件清单；
2. Run 类型及其语义、用户 revision feedback；
3. 冻结的主 Task transcript；
4. base Version 及其审查状态、当前 candidate 和 Reviewer feedback；
5. 按时间排序的本 Run 全部 InputRequest/answers；跨 Run 的最终用户决定必须已经收敛进
   自包含的 base Version，旧模型 Step/原始问答不作为新 Run 的隐式会话历史；
6. 当前 repo 指纹及与 Run 开始时的变化提示。

Reviewer 使用相同的原始 scope、Run 语义、base Version 与审查审计，并额外收到 exact
candidate 和同一 Run 的上一轮 Reviewer feedback。`user_revision` 的 request 是增量，不是对
原始 scope 的替换；Reviewer 不得仅因某项未在本轮 revision 中重复就判定原始/base 要求为
out of scope。

Plan 答案不写回主 Task LogEntry；它只属于 Plan。等待期间主 Session 的新消息不会静默进入
当前 Run。用户需要纳入最新对话时显式选择 `Refresh contexts and regenerate Plan`，创建新 Run。

## 6. 数据模型

新表的关联遵循本项目现有数据库兼容策略：可以声明索引/逻辑外键，但业务正确性不能依赖
SQLite FK cascade。聚合服务必须显式校验引用、执行级联归档和完整性检查。Plan/Version/Run/
Application 属于审计数据，默认只允许归档，不提供普通用户 hard delete；目标 Task 被删除时
保留原 `target_task_id` 作为历史引用，并以 `target_missing` 阻止新的 revision/application。

### 6.1 `plans`

| 字段 | 说明 |
|---|---|
| `id` | 稳定 Plan ID |
| `target_task_id` | 关联主 Task；NULL=standalone |
| `project_id/target_repo/target_branch` | 执行位置快照 |
| `worker_id` | standalone 归属；关联 Plan 跟随 target |
| `priority/timeout_hours` | 调度优先级和执行时间限制；关联 Plan 创建时从 target 快照 |
| `title/initial_request` | 用户可见标题和初始意图 |
| `initial_attachments` | 已验证上传引用 |
| `current_version_id` | 当前 Version 软引用 |
| `active_run_id` | 唯一 active Run 软引用/CAS 门禁 |
| `forked_from_version_id` | 显式 Plan Fork 来源 |
| `created_by/created_at/updated_at` | 审计 |
| `archived_at/closed_at` | 用户生命周期；不代替 Run/decision 状态 |
| `lock_version` | 乐观并发版本 |

索引：`target_task_id`、`project_id`、`worker_id`、`created_by`、`updated_at`。

### 6.2 `plan_versions`

| 字段 | 说明 |
|---|---|
| `id/plan_id/version_number` | 主键、归属和单调版本号 |
| `parent_version_id` | 前一 Version |
| `produced_by_run_id/step_id` | 来源审计 |
| `content` | 完整不可变 Markdown |
| `context_session_id/context_log_id/context_snapshot` | 实际使用的主对话快照 |
| `repo_revision` | Planner 输出时 repo 指纹 |
| `review_verdict/review_feedback` | Reviewer 最终结论 |
| `reviewed_by_step_id/reviewed_at` | 审计 |
| `human_decision/decided_at/decided_by` | 用户决定 |
| `superseded_by_version_id` | 后继 Version |
| `created_at` | 创建时间 |

唯一约束：`(plan_id, version_number)`、`produced_by_step_id`。Version 正文及上下文字段不提供
UPDATE API。

### 6.3 扩展 `plan_agent_runs`

第一阶段保留物理表名以降低迁移风险，领域和 API 名称统一为 PlanRun：

- 新增 `plan_id`、`run_type`、`base_version_id`、`result_version_id`；
- 新增 `draft_content/draft_step_id/draft_repo_revision` 保存完整 Run-scoped candidate；
- 新增 `request_text`、附件、context snapshot/repo revision；
- 新增 `current_stage`、`generation`、`worker_id`、`instance_id`；
- 新增 `open_input_request_id`、`interaction_count`；
- 新增 `execution_seconds/last_execution_started_at`，使 timeout 不累计 waiting_user 时间；
- 状态收敛为本设计的六种；
- 保留 Pipeline route/config、round、error、时间审计；
- `plan_task_id` 暂时保留为 legacy 映射，contract 阶段再移除。

### 6.4 扩展 `plan_agent_steps`

- 新增 `plan_id`、`version_id`、`input_request_id`、`generation`；
- `step_type=planner|reviewer`；
- step 状态增加 `cancelled`，不得用 process exit 0 掩盖结构化 fatal error；
- output 仍为有界诊断副本；active Run 的 `draft_content` 是完整候选，completed Run 的 Version
  content 是最终权威数据。

### 6.5 `plan_input_requests`

字段：`id, plan_id, run_id, source_step_id, requested_by, reason, questions,
status, answers, response_text, attachments, answered_by, created_at, answered_at,
cancelled_at, idempotency_key`。

状态为 `prepared|open|answered|cancelled`。`prepared` 表示模型结果已持久化，但原生资源尚未
确认清理；它是内部状态，绝不向用户展示。

`plan_runs.open_input_request_id` 是跨 SQLite/PostgreSQL/MySQL 可移植的“每 Run 最多一个未
终结请求”门禁。先持久化 prepared request，再清理外部资源；cleanup 成功后，发布 open、
转换 Run 状态和释放数据库 owner 必须在同一事务中完成。

### 6.6 `plan_applications`

字段：`id, plan_id, plan_version_id, application_type, target_task_id,
target_session_id, user_log_id, execution_task_id, applied_by, created_at`。

- `plan_version_id` 唯一，保证一个 Version 只应用一次；
- `chat_message` 必须有 `user_log_id` 且无 `execution_task_id`；
- `execution_task` 反之；
- 任何 Application 都要求目标 Version 已 approved；迁移中存在 Application 是 approved 的
  权威历史证据，`rejected + applied` 必须 fail closed；
- execution Task 后续被删除时保留 Application 历史，但 API 标记 target unavailable，UI 不得
  提供会 404 的打开入口；
- 应用内容从 Version 复制到 LogEntry 的 `applied_plans` snapshot，删除/归档 Plan 不影响历史消息。

### 6.7 Legacy 映射

新增 `plan_legacy_task_links(task_id, plan_id, version_id, run_id)`，用于：

- 旧 URL/API 按 Main 历史 Plan Task id 找到新 Plan/Version；
- 数据迁移前后做逐行对账；
- Worker 协议滚动升级期间兼容旧 payload。

旧客户端和旧 Worker 退出后可以停止通过它写入或传输，但该映射继续保留，用于历史深链接
重定向和审计。除非未来有独立、显式批准的数据清理方案，否则不得随 contract migration 删除。

## 7. 并发、幂等与故障恢复

### 7.1 Plan operation lock

所有创建 Run、回答问题、Approve/Reject、Apply、Fork、Archive 操作都先取 `plan_id` 级本机锁，
再用数据库 CAS 作为跨进程最终门禁。不能只依赖 asyncio lock。

### 7.2 唯一 active Run

创建 Run 使用：

1. 插入 queued Run；
2. CAS `plans.active_run_id IS NULL → new_run_id`；
3. CAS 失败则回滚并返回 409；
4. Run terminal 后只在 `active_run_id == run.id` 时清空指针。

这样不依赖 MySQL 不支持的 partial unique index。

### 7.3 回答 InputRequest

- 请求携带 `expected_run_generation` 和 idempotency key；
- CAS `request.status=open` 且 `run.status=waiting_user`；
- answers schema 与附件全部校验后一次提交；
- 重复相同 idempotency key 返回原结果；
- 不同答案竞争时后到者返回 409；
- commit 后 `dispatcher.wake()`，wake 丢失由轮询恢复。

### 7.4 Version exactly-once

Reviewer 最终 approve/disabled/exhausted 后，在同一事务中：

1. 按 Plan counter/CAS 分配下一个 `version_number`；
2. 插入 Version，`produced_by_step_id` 唯一；
3. 链接旧 current 的 `superseded_by_version_id`；
4. 写入最终 Reviewer 结论并更新 `plans.current_version_id`、`run.result_version_id` 和
   planner step.version_id；
5. Run → completed，释放 `active_run_id`。

Planner 每轮 propose 只更新 Run candidate。服务重启重放最终提交时通过 `produced_by_step_id`
找回既有 Version，不能重复创建 vN/vN+1。

### 7.5 恢复规则

- `waiting_user`：原样保留，不自动失败、不占 Instance；
- `running + prepared InputRequest`：先按 exact owner 证据完成/重试 cleanup；只有 cleanup 已
  证明成功才发布 open request 和 waiting_user，证据不确定时 fail closed；
- `queued`：Dispatcher 正常重新领取；
- `running` 且有精确活进程/thread 证据：继续等待；
- `running` 但 owner 已确认死亡：当前 Step 标记 interrupted，Run 回到 queued；由于所有 Step
  严格只读，可以安全重新调用，但必须保留失败尝试与成本审计；
- owner 证据不确定：fail closed，禁止启动重复 Step；
- InputRequest 已 answered 但 wake 丢失：reconciler 把合法 Run 恢复为 queued；
- 最终 Version/terminal commit 是同一数据库事务，不存在用户可见的半完成 Version；提交结果未知
  时按 step/version 唯一键对账，不重新调用模型。

### 7.6 取消

- Cancel Run：精确停止当前 Step，取消 open InputRequest，Run → cancelled，保留 candidate 审计但
  不发布为 Version；
- Archive Plan：有 active Run 时先要求用户显式 Cancel，不能隐式强杀；
- Stop/Interrupt 主 Task 不影响 PlanRun；Cancel PlanRun 不影响主 Task；
- 所有停止都沿用 exact PID/start identity、Codex turn id 和 generation 安全规则。

## 8. Dispatcher、Instance 与部署门禁

最终架构直接调度 PlanRun，不再为每个 Run 创建用户可见或隐藏的 `Task(mode="plan")`。

### 8.1 领取与容量

- Dispatcher 在同一个 `instance_capacity_lock` 下选择普通 Task 和 queued PlanRun；
- 排序继续遵循数字越小优先级越高；PlanRun 继承 Plan/目标 Task priority，同优先级使用
  queued 时间/id 稳定排序，不能设置一套会让普通 Task 或 PlanRun 永久饥饿的独立轮询；
- Instance 新增 `current_plan_run_id`，与 `current_task_id` 必须二选一；
- claim 在 deployment lease shared lock 内把 Run CAS 为 running，并持久化 exact generation、
  instance owner 后才能 launch；
- 所有容量统计、blocker 查询、stop/start/destroy/recovery 识别两类 owner。

### 8.2 等待用户时释放

模型返回 `request_input` 后，Runner 先持久化 Step 和 prepared InputRequest，但 Run 仍保持
running/owner；只有 exact cleanup 成功后，才能在一个事务中发布 open request、切换
waiting_user 并释放 Instance owner。清理无法证明时 Run 保持运行证据并进入
failed/cleanup-error 路径，不能展示成安全等待。

`waiting_user` 不阻塞一键更新；其状态完全持久化，新进程启动后可继续。正在运行或 admission
中的 PlanRun 必须计入更新、修复、回滚、restart blocker。

### 8.3 资源与超时

- Planner/Reviewer 单步 timeout 沿用现有配置；
- Run wall-clock timeout 只累计模型执行时间，不累计等待用户时间；
- waiting_user 默认不自动过期，用户可 Cancel/Archive；
- 每 Run 的 revision round、interaction count、route fallback 都有独立上限；
- `MAX_ACTIVE_PLANS_PER_TASK` 统计 queued/running/waiting_user 的不同 Plan，不统计同一 Run 的
  Step，也不受 TasksPage 类型筛选影响。

## 9. Runner 实现

将 `PlanAgentRunner.run()` 从单个长 for-loop 改为可重入状态机：

```text
advance(run_id, generation)
  读取 Run/Plan/Version/Input 历史
  校验 generation、active_run_id、stage 和无 open 未回答请求
  按 current_stage 启动一个 disposable Step
  解析结构化结果
  原子持久化以下一种 outcome：
    - version_created → reviewer/complete
    - input_requested → waiting_user
    - reviewer_revise → planner(round+1)
    - reviewer_approve/exhausted → completed
    - operational failure → retry/failed
  清理 exact native resources
```

每次 `advance` 至多运行一个模型 Step，随后把下一个状态持久化并重新入队；不要在一个巨大
协程里跨多个模型调用和长期用户等待。Planner 更新待审 candidate 后设置
`queued/current_stage=reviewer`，Reviewer revise 后设置 `queued/current_stage=planner`；两者都在
确认当前 Step 的原生资源已清理后释放 owner。这使 shutdown、Worker relay、重启恢复和测试
更简单。

Provider 约束保持不变：

- Claude：plan permission、no session persistence、只读工具、进程组精确清理；
- Codex：read-only sandbox、whole-map 禁 MCP、untrusted project、disposable thread/delete；
- primary/fallback 和同 route 多账号耗尽语义不变；
- `request_input` 是结构化成功结果，不是 operational failure，不触发 route fallback；
- schema invalid、timeout、auth、usage limit、transport failure 继续走各自现有处理；
- 用户回答不直接 steer 已结束 thread。

## 10. Context、附件与 staleness

### 10.1 Context 所有权

- 初始/refresh/user_revision Run 创建时冻结主 Task 对话和 session 快照；
- 同一 Run 内的用户问答追加到 Plan context，不写主 Task LogEntry；
- 等待期间主 Task 新消息不自动并入；
- 每次 Planner 更新 candidate 时重新记录当时 repo 指纹；
- Reviewer 只审查 candidate 对应内容和实际 repo 状态，并记录审查时指纹变化。

### 10.2 附件

- Plan 初始附件、revision 附件和 input answer 附件分开保存 provenance；
- 复用 `validate_upload_attachments` 的 owner、root、regular file、non-symlink 校验；
- 每个请求最多 10 个，总 prompt 使用现有有界策略；
- Worker 执行前同步 exact 附件清单并校验数量/摘要；
- 删除前端草稿不会删除已持久化、仍被 Plan 引用的上传文件；
- 不允许附件成为越权读取任意服务器路径的入口。

### 10.3 Staleness

Approve、Apply、创建 execution Task 时基于**目标 Version 的** context/repo snapshot 比较：

- 主对话新增 → `conversation_changed`；
- repo HEAD/dirty 指纹变化 → `repository_changed`；
- 迁移 Version 没有历史 repo 指纹 → `captured_repository_state_missing`，属于可确认风险，
  不强制 Refresh/Re-plan；
- 目标 Task/Project/Worker 不可用 → hard conflict；
- 普通过期或迁移快照缺失首次 409，用户显式确认后继续；Reject 不要求确认；
- `Refresh contexts and regenerate Plan` 仅在检测到 stale 时展示；它不修改旧 Version，而是在
  同一 Plan 下创建新 Run，并在 Pipeline 完成后发布新 Version。

## 11. Worker 与分布式协议

### 11.1 归属

- 关联 Plan 的每次 Run 在 admission 时读取目标 Task 的权威 Worker；
- standalone Plan 使用自身 Worker；
- Plan/Version/Input/Application 的 Manager 记录为用户可见权威；Worker 持有执行所需的
  durable mirror，并以 Manager 分配的全局 id 通信；
- Worker 不得自行改写 Pipeline config、Plan current Version 或人类 decision。

### 11.2 Relay

新增 PlanRun relay payload，至少包含：

- Plan/Run/base Version ids；
- generation 和 expected Worker assignment；
- request、冻结 context、附件 manifest；
- Pipeline config；
- 已有 Version/Reviewer/Input history 的有界恢复上下文。

Worker protocol v2 同步 Run candidate 与最终 Version 的边界。Step outcome 回传使用 exact
generation 和 idempotency key。Manager 只有在 Worker 结果与当前 Plan.active_run_id、Run
generation 同时匹配时才提交；protocol v1 Worker 必须 fail closed，避免把中间草稿导入 Version。

### 11.3 Worker 迁移

第一版采用保守规则：

- queued/running/waiting_user Run 阻止目标 Task 单独迁移；
- reviewable/approved/applied 历史不阻止迁移，因为完整 Version 已在 Manager 持久化；
- 后续实现 Plan 组迁移时，必须同步 Run/Input/Version/附件并做两阶段 ACK；
- Worker 断线时 waiting_user 仍可查看和提交回答，但 resume 保持 queued，直到权威 Worker
  恢复或管理员完成安全迁移。

## 12. API 设计

### 12.1 Canonical API

```text
POST   /api/plans
GET    /api/plans?target_task_id=&kind=&display_state=
GET    /api/plans/{plan_id}
PATCH  /api/plans/{plan_id}                    修改标题/归档等非版本内容
POST   /api/plans/{plan_id}/fork
GET    /api/plans/resolve-legacy-task/{task_id} 历史链接重定向

GET    /api/plans/{plan_id}/versions
GET    /api/plan-versions/{version_id}
POST   /api/plan-versions/{version_id}/approve
POST   /api/plan-versions/{version_id}/reject
POST   /api/plan-versions/{version_id}/create-execution-task

GET    /api/plans/{plan_id}/runs
POST   /api/plans/{plan_id}/runs               initial/revise/refresh/retry
GET    /api/plan-runs/{run_id}
POST   /api/plan-runs/{run_id}/cancel
POST   /api/plan-runs/{run_id}/input-requests/{request_id}/answer
```

`POST /api/plans` 同时支持 standalone 和 related，并且始终冻结当时的全局 Pipeline 配置；
前端不再调用旧 `POST /api/tasks/{task_id}/plans`，通用 `POST /api/tasks` 的
`mode=plan` 创建入口明确返回 410。旧 Task 形态的窄化 API 只用于发布 contract 期兼容。

### 12.2 Chat API

请求从：

```json
{"message": "开始实施", "plan_task_ids": [123]}
```

演进为：

```json
{"message": "开始实施", "plan_version_ids": [456]}
```

过渡期允许二者之一，禁止同时传；legacy id 服务端解析为精确迁移 Version。响应和 LogEntry
snapshot 新增 `plan_id/version_id/version_number`，继续兼容旧 `id/title/content` 渲染。

### 12.3 并发参数

所有 mutation 请求携带必要的 expected 值：

- create revision：`base_version_id`、`expected_current_version_id`；
- answer：`expected_run_generation`、`idempotency_key`；
- approve/reject：`expected_current_version_id`；
- apply：由 chat operation lock + Version unique application fence；
- Worker response：`run_id/generation/step_id`。

冲突统一返回 409，并返回当前 Plan/Run/Version 摘要供前端刷新。

## 13. WebSocket 事件

新增小 payload 失效通知：

- `plan_created`；
- `plan_run_created`；
- `plan_run_status_changed`；
- `plan_input_requested` / `plan_input_answered`；
- `plan_version_created`；
- `plan_version_decided`；
- `plan_version_applied`；
- `plan_archived` / `plan_restored`。

事件只带 `plan_id/run_id/version_id/display_state/updated_at` 等摘要，不携带完整 Markdown、
questions answer 或附件。前端收到事件后 refetch canonical API；断线重连同样全量对账，不能把
WebSocket 当权威状态。管理员可订阅全局 `plans`；普通成员只可订阅通过 Plan/Task ACL 的
`plan:{id}` / `task:{id}`，并由 15 秒 HTTP 轮询覆盖新 Plan 尚无 scoped subscription 的窗口。

## 14. 前端交互

### 14.1 Plans modal

- 左侧/列表层级以 Plan 为单位，不再显示 revision Task 链；
- 每个 Plan 显示 `vN`、当前 stage、Reviewer route、stale、decision/application；
- Plan 详情提供 Version selector 和时间线；
- 默认展示 current Version，可切换历史 Version；
- 支持与上一 Version 的 Markdown/text diff；
- 用户 Revise 后留在同一 Plan 页面，显示新的 Run 进度，不产生新卡片；
- 显式 `Fork as new Plan` 才创建另一张 Plan 卡片。

### 14.2 Plans requiring action / Input needed

- Plans 页面以 `Plans requiring action` 统一包裹 `Input needed` 与 review/execute 两类动作；
- Plans modal 使用 `Input` 过滤器和 attention badge；
- InputRequest 使用专用表单渲染 text/single/multi choice；
- 表单按响应中的完整 questions 数组渲染，不截断、不分页丢题，也不因问题数量拒绝提交；
- 支持附件上传、预览、失败重试、移除；
- 提交中冻结输入，成功后清空草稿；409 时保留草稿并刷新状态；
- 刷新页面后从 API 恢复 open questions；
- 回答完成后显示只读 Q&A 审计；
- waiting_user 不显示旋转中的模型或占用 Instance 的假象。

### 14.3 Review 与 Apply

- `Plans requiring action` 的 review/execute 分组只展示 current Version decision=pending，
  或已批准但尚未创建 execution Task 的 standalone Version；
- Plans 目录的 kind/status/Project/search/archive 筛选不影响该区域；
- Approve/Reject 文案带 Version，例如 `Approve v3`；
- composer attachment 显示 `Plan #12 · v3`；
- user message 展开内容展示 exact applied Version snapshot；
- v1 已 applied、v2 新生成时，清晰显示两条状态，不把整个 Plan 永久标成 applied；
- standalone `Create execution task` 绑定批准的 Version，成功后显示目标 Task 链接。

### 14.4 顶级 Plans 页面与 Tasks 边界

canonical Plan 不再依赖普通 Task list response；历史 Task 仍遵守 Task 页面语义：

- Tasks 页面、分页、计数和全局 Task 搜索展示全部真实 Task，包括迁移后只读的
  `Task(mode="plan")` 历史；其卡片通过 legacy link 跳转 canonical Plan；
- New Task 表单不提供 Plan mode；standalone 统一从 Plans 页面创建；
- Plans 页面目录查询 canonical `/api/plans`，支持 standalone/related、display state、Project、
  title/request 搜索和 `archived_only`；
- Archive 是可恢复软归档，Archived only 可发现并打开完整历史；
- 每个 Plan 可用 `#/plans/{plan_id}` 深链接打开，列表选中项必须高亮；
- awaiting review / needs input 使用独立查询，不受 Plan 目录筛选、分页和 count 影响。

## 15. 权限与安全

- Plan ACL 继承 Project/目标 Task，同时保存 created_by；关联 Plan mutation 要求同时有 Plan 和
  target Task 控制权；standalone 按 Project/creator 权限；
- Plan 列表和 WebSocket 事件都执行现有 Team CCM 可见性过滤；
- Version、InputRequest、Run 不能通过猜 id 绕过 Plan ACL；
- 模型问题严禁索取 secret；后端对疑似凭据字段不做自动保存/转发；
- 所有附件复用上传根目录、owner、non-symlink、regular-file 校验；
- Planner/Reviewer 继续禁 Bash 写入、MCP、Apps、多 Agent、网络和项目 trust；
- prompt 中把用户回答标为不可信输入，不能把它解释为扩大工具或文件权限；
- 完整 Plan/answers 不写 WebSocket 或普通结构化日志，避免意外泄露；
- account id 只出现在授权 run audit response，错误消息继续脱敏。

## 16. 数据迁移与兼容

采用 expand → backfill → dual-read → cutover → contract，禁止一次 migration 直接删除 Task 字段。

### 16.1 Expand

1. 创建 `plans/plan_versions/plan_input_requests/plan_applications/
   plan_legacy_task_links`；
2. 扩展 run/step；
3. 为新表添加索引、唯一约束和 portable check；
4. 模型导入加入 Alembic metadata；
5. 保持旧 API/Task 行为不变。

### 16.2 Backfill

迁移事实源固定为当前 `origin/main` 的旧 schema，而不是本功能分支曾经出现过的中间 schema：

1. 在部署 blocker 已证明没有 active Plan 进程/claim 后开始迁移；任何 in_progress/executing
   Task、active Run 或 Instance/process owner 证据都使 migration fail closed；
2. 只选择 `mode=plan` 且处于 Main 生命周期状态，并要求
   `plan_target_task_id/plan_context_*/plan_repo_revision/supersedes_plan_task_id/
   plan_approved_at/by/plan_applied_*/plan_execution_task_id/plan_pipeline_config` 全为空；这些字段和
   `plan_agent_runs/steps` 都由本功能分支引入，不可能是 Main 历史；
3. 每个符合条件的 Task 独立创建一个 Plan；有 `plan_content` 时只创建 v1，没有内容时不伪造
   Version；不读取或重建 revision chain；
4. `plan_review` + content → `review_verdict=disabled, human_decision=pending`，进入
   `Plans requiring action`；Main 当时没有 Reviewer；
5. `plan_approved=True` + content → v1 approved，并创建 execution application 指向同一个
   carrier Task；Main 的 approve 就是把该 Task 重新排队执行，因此它是精确应用事实；
6. `cancelled + plan_approved=False` + content → v1 rejected；
7. 未批准 pending carrier → 唯一 queued canonical Run，并把旧 Task 标为 superseded，避免
   Task 与 Run 双重领取；已批准 pending carrier 仍是原 execution Task，不得 supersede；
8. 有 content 的历史规划 Run 记为 completed；无 content 的 failed/cancelled 状态投影为对应
   terminal Run；每个 Main Task 写一个 legacy link。

首次 backfill 直接忽略带本分支 provenance 的 Task。对于已经运行过早期迁移的数据库，后续
一次性 reconciliation 在同样的 quiescence fence 下：

- 保留每个 Main-compatible carrier 已有的 Plan id 和其链接的 v1 id；从原 Task 重新规范化
  title/content/decision/application/run/link；
- 删除额外 Version、无 Main carrier 的 canonical Plan，以及旧分支创建的 Step/Input/Application
  receipt/Run/Application 审计；旧 Task 行本身不删除；
- 清除分支期间产生的 Archive/Closed/Fork 状态，因为这些状态不属于 Main 生产事实；
- migration 不用日期、部署时间或硬编码 Task id 判断来源。

### 16.3 Dual-read / 校验

- 新 API 读新表，旧 API 通过 legacy links 投影兼容 TaskResponse；
- 临时后台对账 Main-compatible Task 数量、单 v1 内容、审批、carrier application 和 link；
- 新写入只进入新模型；旧字段仅做兼容投影，不允许两个方向同时可写；
- Manager 与 Worker 协议增加 capability/version 握手，旧 Worker 在包含新 PlanRun 时返回 409，
  不静默降级成旧 Plan Task。

### 16.4 Cutover / Contract

- 前端全部切换 Plan/Version API；
- Dispatcher 只 claim PlanRun；
- 旧 Plan Task 标记只读 legacy；规划 mutation 只走 canonical Plan，但历史 Task 继续出现在
  Task list/count/search，并显示 canonical Plan 链接；作为旧 main execution Task 的 pending carrier
  仍可完成既有执行；
- `mode=plan` 通用创建入口已关闭；观察至少一个发布周期后移除剩余窄化旧 mutation
  endpoints、Worker legacy payload 和 `Task.plan_*` 字段；保留只读 legacy resolver/link；
- downgrade 只保证 schema 可回退，不承诺把多 Version/交互 Run 无损压回单 Task 模型；发布
  前必须依赖现有 SQLite 快照/外部数据库人工备份策略。

## 17. 分阶段实施

### Phase 1：领域表与只读投影

- 新模型、schema、Alembic expand migration；
- Main 单 carrier backfill 和分支中间 schema 清理；
- Plan/Version/Run read API；
- 旧行为不变，前端不切换；
- 加 migration 与 dual-read contract tests。

完成门槛：现有数据库迁移后，新 API 与所有 Main-compatible Plan Task 的数量、v1 内容、审批和
carrier 执行 application 一致；本分支中间 schema 数据已清理；restore 路径完成验证。

### Phase 2：可重入 Runner 与用户输入

- Planner/Reviewer schema v2；
- `advance()` 单 Step 状态机；
- InputRequest/answer API；
- waiting_user 恢复、取消、interaction limit；
- 仍不开放前端入口，先用 API 集成测试验证。

完成门槛：Planner/Reviewer 两条 request-input 路径都能跨进程重启恢复；等待期间无进程、
thread、Instance owner 或部署 blocker。

### Phase 3：Plan/Version mutation 与前端切换

- canonical create/revise/fork/approve/reject/application API；
- Plans modal、Needs input、Version history/diff；
- Chat `plan_version_ids` 和 applied snapshot；
- 独立 Plans 页面、standalone 创建、目录筛选和深链接；Tasks 页面继续展示完整 Task 历史；
- 旧 API 保留兼容。

完成门槛：用户 Revise 后 Plan id 和卡片不变，只增加 Run/Version；所有刷新/重连状态一致。

### Phase 4：Dispatcher/Instance/Worker 对等

- PlanRun 直接 claim 和 `Instance.current_plan_run_id`；
- capacity、termination、update blocker、recovery 全覆盖；
- Manager/Worker PlanRun relay、附件同步、generation CAS；
- 停止创建新的 Plan carrier Task。

完成门槛：本机与 Worker 的 create → ask → answer → revise → approve → apply 全链一致；故障注入
无重复 Step、Version 或 application。

### Phase 5：Legacy contract

- 停止旧写 API；
- legacy Plan Task 在普通 Task list/count/filter 中保留只读历史，并链接 canonical Plan；
- 清理 Task Plan 字段、legacy services 和兼容 UI；
- 更新 README、TEST、AGENTS/CLAUDE 文档和运维手册。

完成门槛：仓库无新代码依赖 `Task.mode == "plan"` 或 `Task.plan_content/plan_approved/...`；
迁移后的生产副本完成手工验收后才删除兼容层。

## 18. 文件级实施范围

### 18.1 后端新增

- `backend/models/plan.py`：Plan、PlanVersion、PlanInputRequest、PlanApplication；
- `backend/schemas/plan_resource.py`：canonical resource/mutation response；
- `backend/services/plan_service.py`：聚合根、CAS、版本、决策、application；
- `backend/services/plan_run_queue.py`：PlanRun claim/recovery；
- Alembic expand/backfill/contract migrations；
- 对应单元、API、migration、dispatcher、Worker 测试。

### 18.2 后端修改

- `backend/models/plan_agent.py`；
- `backend/schemas/plan.py`；
- `backend/api/plans.py`、`backend/api/chat.py`、`backend/api/tasks.py`；
- `backend/services/plan_agent_runner.py`、`plan_tasks.py`；
- `dispatcher.py`、`instance_manager.py`、`task_termination.py`、`update_service.py`；
- `worker_proxy.py`、`task_migrator.py` 及 Worker relay/capability；
- `ws_broadcaster.py`、`main.py`、`database.py`/Alembic metadata。

不要求在第一 PR 机械重命名所有旧文件；先建立正确权威边界，再做命名清理。

### 18.3 前端

- `frontend/src/api/client.ts`：Plan/Version/Run/Input/Application 类型和 API；
- `components/PlanReview/`：Plan 列表、Version selector/diff、Run progress、Input form；
- `Chat/ChatView.tsx`：按 Version 创建/选择/应用；
- `pages/PlansPage` / `components/PlanReview`：Plan 创建、目录、筛选、深链接和行动队列；
- `Tasks/TaskList` / `TasksPage`：展示全部 Task；legacy Plan Task 显示 canonical Plan 链接；
- WebSocket invalidation/refetch；
- 对应 Vitest/RTL 测试。

## 19. 测试计划

### 19.1 领域与状态机单元测试

- 一个 Task 创建多个 Plan，Plan id 稳定且互不覆盖；
- 同一 Plan 的 Version number 严格单调且 content 不可更新；
- 同一 Plan 并发创建 Run 只有一个成功；
- Planner propose 更新 Run candidate，不创建 Version；非法 union 不更新 candidate；
- Planner request_input：Run waiting、无 Version、open request；
- Reviewer request_input：answer 后回 Planner并更新包含回答的 candidate，最终只产出一个
  Version；
- Reviewer revise 多轮只覆盖 candidate，达到上限才发布一个 exhausted Version；
- 用户 Revise 新 Run/Version但 Plan 不变；Fork 创建新 Plan；
- answer CAS、idempotency、错误 schema、重复/竞态回答；
- terminal Run 不可复活；cancel 保留旧 Version；
- derived display_state 全组合；
- 单次 1 个、4 个及更多问题均可通过，且只受整体 payload/字段大小保护；
- interaction/revision 轮数上限边界值；轮数限制不得被误实现为单轮问题数量限制。

### 19.2 API/ACL 测试

- related/standalone create；
- 无目标 Task session、target 删除、共享 shadow、跨用户/跨 Project 拒绝；
- Version history、Run/Step/Input audit 权限；
- approve/reject 只允许 current reviewable Version；
- stale 409 与 confirm；
- revision expected current Version 冲突；
- answer 文件验证、最多 10 个、symlink/越界/owner 错误；
- old/new API dual-read、legacy id resolution；
- WebSocket payload 不包含完整 Plan/answers；
- task type filter 不影响 awaiting review/needs input。

### 19.3 Runner/provider 测试

- Claude/Codex Planner 的 propose/request_input schema；
- Reviewer approve/revise/request_input schema；
- primary/fallback、同 route 多账号、usage/auth/transient/timeout；
- request_input 不触发 fallback；
- Claude process group、Codex exact turn/thread delete；
- read-only repo fingerprint 前后相同；
- cleanup 不确定时 fail closed；
- 恢复 prompt 包含全部问答但不包含等待期间未显式刷新的主消息；
- step output 截断不影响完整 Run candidate/最终 Version；
- 重放 step outcome 不重复创建 Version。

### 19.4 Dispatcher/termination/update 测试

- 普通 Task 与 PlanRun 共用 capacity；
- `current_task_id/current_plan_run_id` owner XOR；
- queued→running claim generation CAS；
- waiting_user 释放所有 owner；
- answer 后 wake 丢失由 poll 恢复；
- dead owner 精确回收、不误停复用 Instance；
- Cancel PlanRun 不影响主 Task，Interrupt 主 Task 不影响 PlanRun；
- running/admission PlanRun 阻止 update/restart，waiting_user 不阻止；
- shutdown/cancellation 每个 transaction 边界故障注入；
- 服务重启对 queued/running/waiting/terminal 的恢复矩阵。

### 19.5 Worker 测试

- Manager/Worker capability 握手；
- related Plan 跟随目标 Worker，standalone 使用指定 Worker；
- input answer 在 Worker 离线时持久保存、恢复后只 resume 一次；
- generation 不匹配结果丢弃且保留审计；
- 附件 manifest 数量/摘要不匹配 fail closed；
- active/waiting Run 的迁移 fence；
- Manager authoritative decision/application 不被 Worker 覆盖；
- relay 重试不重复 Step/Version/Application。

### 19.6 Migration 测试

至少构造：

- Main standalone 单 carrier Plan；
- 带本分支 provenance 的 2–5 节点 revision chain，验证不迁移；
- Main plan_review/approved/rejected/applied carrier；
- failed/pending、无 content；
- legacy attachment metadata；
- 已跑过旧分支迁移的数据库：保留 Main Plan/v1 id，删除额外 Version、standalone/related
  分支 Plan、Run/Step/Input/Application/receipt；
- active Task/Run/Instance fence；
- SQLite/PostgreSQL/MySQL upgrade；
- upgrade 后 Main counts/content/ids/carrier application 对账；
- snapshot restore；reconciliation 是不可逆数据清理，downgrade 不重造分支测试数据。

### 19.7 前端测试

- 多 Plan 列表、独立 progress、刷新和 WS 重连；
- Version selector、历史 Markdown、diff；
- Revise 保持 Plan 卡片/id，出现 vN+1；
- Fork 出现新 Plan；
- Needs input text/single/multi、附件、重试、409 保留草稿；
- waiting_user 无运行 spinner；
- approve/reject 标明 Version；
- composer 选择 exact Version，payload 使用 `plan_version_ids`；
- Applied message 展示 exact snapshot；
- v1 applied 后 v2 可独立审批/应用；
- standalone execution Task 幂等；
- task filter、awaiting review、needs input 三个查询互不影响；
- 移动端 modal、键盘操作、焦点和基本无障碍。

## 20. 手工验收计划

### 20.1 基本层级

1. 在同一主 Task 创建 Plan A/B；
2. 确认主 Task 可继续聊天，Plan 独立运行；
3. A 完成 v1，B 等待输入；
4. 刷新页面，A/B 状态和问题不丢失；
5. 回答 B 后确认仍是 Plan B、Run #1，只新增 Version；
6. 对 A 提交 Revise，确认仍是 Plan A，但创建 Run #2 和 v2；
7. 对 A 执行 Fork，确认只有此时才出现新 Plan。

### 20.2 Planner/Reviewer 输入

1. 用确定会缺少部署选择的请求触发 Planner question；
2. 确认问题出现时没有活进程/Instance owner；
3. 回答含选项、文字和附件；
4. 重启服务后继续并完成；
5. 触发 Reviewer question，确认回答回到 Planner，最终 Markdown 包含该决定；
6. 达 interaction limit 时明确失败，不无限追问。

### 20.3 Version/审批/应用

1. 查看 v1/v2 完整内容和 Reviewer feedback；
2. Approve v2，确认没有模型 turn；
3. 发送真实消息并选择 v2，确认 user message 展示完整 snapshot；
4. 让 v3 产生，确认 v2 applied 历史不变、v3 可单独审批；
5. 对 stale Version 验证首次 409 和显式确认；
6. standalone Version 创建 execution Task，重复点击返回同一 Task。

### 20.4 并发/恢复/Worker

1. 同一 Task 同时运行三个 Plan，第四个返回 429；
2. 同一 Plan 双击 Revise，只有一个 Run 成功；
3. 双浏览器回答同一 InputRequest，只有一个答案成功；
4. 在 Planner、Reviewer、waiting_user 各阶段重启服务；
5. 在 Worker 断连、恢复和 generation 变化时重复上述流程；
6. running PlanRun 阻止更新，waiting_user 不阻止；
7. Claude 和 Codex primary/fallback 各覆盖一次并验证 repo 零写入。

### 20.5 旧数据迁移

1. 对停服后的生产 SQLite 在线备份执行 migration，不直接用开发库演练；
2. Main `Task(mode=plan)` 数量与 legacy link/Plan 数量一致，每个 Plan 至多一个 v1；
3. `plan_review` 出现在 `Plans requiring action`，rejected 保持 rejected；
4. Main `plan_approved=True` 的 pending/completed/failed carrier 都显示 Applied，并跳回同一 Task；
5. Main 旧 Task 继续出现在 Tasks list/search，点击 canonical link 可打开对应 Plan；
6. 本分支中间实现产生的多 Version/无 Main source Plan 和交互审计已清除；
7. Archived only、列表选中高亮和旧 Plan 深链接在迁移副本上验证；
8. 校验完成前保留数据库快照，生产迁移/重启仍走独立部署授权。

## 21. 发布门禁与完成定义

以下全部满足才算完成，不能只以 UI 可提问为完成：

- 新建和 Revise 路径不再创建 `Task(mode="plan")`；
- Plan、Version、Run、Step、InputRequest、Application 均有独立持久模型和 ACL；
- Planner/Reviewer request-input 均可跨服务重启恢复；
- waiting_user 无任何进程、thread、Instance owner、capacity 或 update blocker；
- 所有审批和应用绑定 exact Version；
- Main-compatible 旧 Plan Task 的 v1/decision/carrier application 无丢失且 legacy URL 可解析；
- 旧 Plan Task 继续出现在 Tasks list/count/search，且 Plan 决策只允许在 canonical Plan 执行；
- 本机与 Worker 行为对等；
- Worker protocol v2 先握手并同步 Run candidate/final Version 边界，再以 attachment
  size/SHA-256 manifest、generation CAS 和 durable application receipt 验证导入/回答/应用；
  protocol v1 必须拒绝，丢失 HTTP ACK 可按 receipt 查询恢复，不能重复应用；
- 后端全量测试、前端全量测试、生产构建、Ruff、ESLint、Alembic current/head 通过；
- SQLite 自动迁移在备份副本完成，PostgreSQL/MySQL 依项目部署规范人工演练；
- 完成本文 20 节手工验收；
- 更新 `plan-agent-design.md` 状态、README、TEST、AGENTS.md/CLAUDE.md 关键路径；
- 生产部署和重启仍需用户单独确认。

## 22. 风险与控制

| 风险 | 控制 |
|---|---|
| 状态表数量增加、查询复杂 | Plan 聚合服务统一读写；API 返回派生 display_state；禁止前端拼状态 |
| Run 等待时恢复重复调用模型 | durable status + generation + step/version unique idempotency |
| 本分支中间 Plan schema 污染生产事实 | 只认 Main-compatible carrier；reconciliation 删除 canonical 分支数据但保留旧 Task |
| Task/PlanRun 抢 Instance 产生 owner 竞态 | 同一 capacity/admission lock、DB CAS、owner XOR、exact generation |
| 用户回答不进入最终方案 | Reviewer question 回 Planner；最终批准 Version 必须自包含 |
| 等待期间主对话/repo 改变 | 对话不隐式刷新；candidate 更新时重记 repo；审批/application stale 检查 |
| 多版本审批/应用含义不清 | 所有按钮和审计显示 vN；application 对 Version 唯一 |
| Worker 滚动升级协议不一致 | capability handshake，未知版本 409/fail closed |
| 大范围一次改造难回滚 | expand/backfill/dual-read/cutover/contract 分阶段，每阶段独立验收 |

## 23. 最终产品语义

```text
创建 Plan       = 新的规划主题
Planner 提问    = 当前 Run 暂停，回答后继续当前 Run
Reviewer 提问   = 当前 Run 暂停，回答后回 Planner 更新 candidate，Pipeline 结束才发布 Version
Reviewer revise = 当前 Run 内的新 Planner round / 覆盖 candidate，不创建 Version
用户 Revise     = 同一 Plan 下的新 Run / 新 Version
Fork            = 新 Plan
Approve/Reject  = 对 exact Version 做决定
Apply           = 把 exact approved Version 绑定到真实用户消息或执行 Task
```

该语义是后续实现、API 命名、状态机、UI 文案和测试断言的共同权威；任何兼容层都不能改变它。
