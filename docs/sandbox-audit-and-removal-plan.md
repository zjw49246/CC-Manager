# 沙箱代码全量审计：清除计划与合理性判断（2026-09-05）

## 背景

提议：整个服务部署在 AWS 上，主机本身"就是一个沙箱"，不会和个人本地文件交互，因此仓库里的沙箱代码没有存在必要，应全部删除。

本文对代码库里**所有**沙箱相关代码做了逐处审计（一手证据，全部标注 file:line），给出分级清除计划，并评估"全删"是否合理。

**结论先行：全删不合理。** 仓库里的"沙箱"不是一个东西，而是四类互不相关的机制；其中真正的隔离沙箱（Docker 容器）防的不是"个人本地文件"，而是**同一台 AWS 主机上的跨用户/跨项目/凭据边界**——AWS 部署完全不能替代它。另有两类"沙箱"字样实际上是**关闭**沙箱的 flag（删掉反而破坏功能），一类是极小的只读策略参数（防并发写坏工作区）。详细分级见下。

## 一、沙箱相关代码全量清单

### A 类：Docker 容器沙箱（共享项目隔离）——真正的隔离沙箱

| 位置 | 内容 |
|---|---|
| `backend/services/container_manager.py`（整个文件，约 1370 行） | `ccm-sandbox:latest` 镜像、每个共享项目一个容器（`--cap-drop ALL`、`--read-only`、`no-new-privileges`、tmpfs `/tmp`、pids-limit，见 852-886 行）、项目专属 git 凭据准备、API 账号目录只读挂载、容器内 /tmp 压力清理 |
| `backend/services/instance_manager.py:991-1028, 1320-1412` | PTY 与 `-p` 两条启动链路的容器分流：任务所属项目被共享且 Docker 可用时，agent 进程在容器内 `docker exec` 执行 |
| `backend/services/instance_manager.py:2530-2534, 2827-2934, 8885-8956, 9675` | 容器内进程的注册、存活探测与信号收尾（stop/kill 走 `signal_exec`） |
| `backend/main.py:642-648` | 启动时 Docker 可用则后台构建沙箱镜像 |
| `backend/services/worker_provisioner.py:815, 1097-1115` | Worker EC2 bootstrap 的 `docker-sandbox` 步骤（Worker 上同样构建镜像） |
| `backend/services/cloudrouter_accounts.py:588-593` | API 账号 key-helper 的容器内路径分支 |
| `backend/tests/test_container_manager.py` 等 8 个测试文件（21 处） | 上述行为的测试 |

**触发条件**（`container_manager.py:1321-1333`）：项目存在 `TeamProjectShare` 记录 **且** 宿主装了 Docker。两者不满足时整套代码完全休眠，agent 直接跑在宿主上——即**单租户、不用共享功能的部署，本来就没有任何容器开销**。

**它防什么**：共享用户可以对共享项目的 agent 发任意 prompt，而 agent 会执行任意命令。没有容器时，该命令以服务用户身份跑在宿主上，可以直接读取：
- 全部号池凭据：`~/.claude-pool/accounts.json`、各 `CODEX_HOME/auth.json`、`email_tokens.json`、`CLOUDROUTER_ACCOUNTS_DIR` 下的 API Key（CLAUDE.md「Codex Pool」「API 网关账号」节）；
- 其他所有项目的代码与 deploy key；
- CCM 数据库（所有用户的任务、日志）与 `.env`（AUTH_TOKEN、JWT/SMTP secret）；
- CCM 自身源码——配合自更新体系等于可以让服务部署攻击者的代码。

### B 类：Codex CLI 沙箱参数——大部分是"关闭沙箱"，少量是只读策略

1. **关闭沙箱的 flag（与提议的哲学一致，早已是现状）**：主任务、goal evaluator、fork/exec 路径全部传 `--dangerously-bypass-approvals-and-sandbox` / `"sandbox": "danger-full-access"`：
   - `instance_manager.py:5117`、`goal_evaluator.py:1089`、`codex_app_server.py:1550`（默认值）、`codex_app_server.py:2529, 2571`、`scripts/codex_login.py:464, 881`、`scripts/benchmark_codex_transport.py:39`。
   - 这些是**必须传给 Codex CLI 的参数**（否则 Codex 默认开启自己的沙箱并要求审批，headless 下会卡死）。"删除沙箱相关代码"如果把它们删了，效果恰好相反：Codex 的沙箱会被重新打开。
2. **read-only 沙箱策略（仅两处，防写不防谁）**：
   - Monitor 定时检查回合：`dispatcher.py:8850`（`sandbox_mode="read-only"`）+ `codex_app_server.py:1907-1916`（turn 级 `sandboxPolicy: readOnly, networkAccess: false`）。Monitor 是与主 agent **并发**跑在同一工作区的后台观察者，read-only 防它写坏主 agent 正在改的文件、防 resumed thread 的 sticky 设置放宽策略（代码注释与 CLAUDE.md「Monitor Skill」节明确为不变量）。
   - Distill 只读回合：`skill_distill.py:195-201`（`--sandbox read-only --ignore-rules --ephemeral`），同理：蒸馏是纯读分析，不允许改 task 工作区。
3. **`trust_level = "untrusted"` 覆盖**：`codex_app_server.py:278-291, 1609, 2534`、`skill_distill.py:600-607`、`cloudrouter_accounts.py:166-172`。作用是禁止项目目录下的 `.codex` 配置在 API 网关账号进程里启动 MCP/hooks——防的是项目内不可信配置接触 API Key helper，属于凭据隔离，与主机在哪无关（CLAUDE.md「API 网关账号」节列为不变量）。

### C 类：浏览器 `--no-sandbox`——是"关闭 Chrome 沙箱"，不可删

`scripts/auto_login.py:421`、`scripts/cdp_login.py:318`、`scripts/codex_login.py:798`。这些 flag 关闭 Chromium 自带沙箱，使账号登录浏览器能在无特权容器/服务器环境跑起来。删除后 Chrome 在当前部署环境直接启动失败，Claude/Codex 号池自动登录全部瘫痪。

### D 类：文档/设计稿提及

`docs/plans/team-ccm-design.md`、`docs/plans/plan-agent-design.md`、`docs/worker-deployment-guide.md`、`docs/coding-agent-frontier-2026H1.md` 等与 `PROGRESS.md`、`CLAUDE.md` 的描述性文字；另外会话工具里的 `ccm_workspace_review`「untrusted-code sandbox」由外部组件提供，不在本仓库代码内。

## 二、"AWS 即沙箱"论点评估

论点两个前提中，"不和个人本地文件交互"是**真的**——但本仓库没有任何一行沙箱代码是为保护个人本地文件而写的，所以这个前提推不出结论。

"部署在 AWS 上本来就是沙箱"**不成立**，因为它混淆了两个边界：

1. AWS 隔离的是「这台主机 vs 你的个人电脑/其他 AWS 客户」。
2. 仓库里的沙箱隔离的是「同一台主机内部：不可信的 agent 执行 vs 全部账号凭据、其他用户的项目、数据库、服务本体」。

CCM 是多用户系统（注册/角色体系、TeamProjectShare、Shared Task Relay），prompt 是不可信输入，agent 以 `--dangerously-skip-permissions` 执行任意命令。对**项目 owner 自己**的任务，代码从未套沙箱（直接跑宿主）——说明现有设计已经采纳了"自己人的任务不需要沙箱"的立场；容器只在出现**第二个信任主体**（共享用户）时才启用。把主机整体当沙箱，等价于宣布"任何共享用户 = 主机 root + 全部账号凭据持有者"。只要共享功能存在一天，这就不可接受；即使今天只有一个用户在用，删掉的也是一个已上线功能的安全边界，而不是死代码。

B/C 类更直接：约 70% 的 "sandbox" 字样是**关闭**沙箱的参数，删除会把 Codex 沙箱重新打开或让登录浏览器起不来——与提议目标南辕北辙。B2/B3 合计不足 60 行，防的是并发写坏工作区和项目内不可信配置读凭据，收益/成本比极高。

## 三、清除计划（分级）

### 建议执行：不删除任何一类

- A 类在单租户、无共享部署下零开销休眠，没有"清掉换轻快"的收益；
- B/C 类删除会直接破坏功能；
- 维护负担主要在测试（8 个文件 21 处），而这些测试保护的是仍在线的功能。

### 若坚持下线 A 类（唯一可讨论的删除项），前置条件与步骤

**前置条件（必须先满足其一）**：① 先移除/禁用整个项目共享功能（TeamProjectShare、Shared Task Relay），或 ② 书面接受"共享用户可获得主机上全部凭据与数据"的风险。不满足前置条件的删除是安全回退，不应执行。

满足后按依赖序执行：

1. 删 `backend/services/instance_manager.py` 两条启动链路的容器分流（991-1028、1320-1412）及 exec 生命周期辅助（2530-2534、2827-2934、8885-8956、9675 附近），恢复纯宿主 spawn/signal 路径；
2. 删 `backend/main.py:642-648` 镜像构建 hook；
3. 删 `backend/services/worker_provisioner.py` 的 `docker-sandbox` bootstrap 步骤（815、1097-1115）；
4. 处理 `backend/services/cloudrouter_accounts.py` / API 账号删除流程中"fail-closed 清理挂载账号目录的 CCM Docker 容器"分支（CLAUDE.md「API 网关账号安全删除」不变量需同步改写）；
5. 删 `backend/services/container_manager.py` 整文件（含容器内 /tmp 压力管理；`tmp_space_manager` 中共享容器分支同步删）；
6. 删 `backend/tests/test_container_manager.py`，清理其余 7 个测试文件中的容器相关断言；
7. 同步文档：CLAUDE.md（「/tmp 压力保护」「API 网关账号安全删除」「Codex Skill context」等节的容器措辞）与 AGENTS.md 保持同步、README、`docs/plans/team-ccm-design.md`、`docs/worker-deployment-guide.md`；
8. 全量跑 `backend/tests` 清零基线后再合入。

**明确不删**：B 类全部（关沙箱 flag 是 CLI 必需参数；read-only/untrusted 是并发正确性与凭据隔离，共约 60 行）、C 类全部（登录浏览器必需）、D 类随 A 类同步修订即可。

## 四、最终判断

"沙箱全部删除"**不合理**：多数所谓沙箱代码是关闭沙箱的必需参数（删则功能反转），真正的容器沙箱守的是 AWS 无法替代的主机内多用户信任边界，且在不触发共享时零成本。建议维持现状；如确定永久放弃项目共享功能，可按第三节步骤单独下线 A 类。
