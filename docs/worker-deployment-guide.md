# 分布式 Worker 部署指南

> 本文档面向 CCM 管理员，详细说明如何部署和管理分布式 Worker 节点。

## 概述

CCM 的分布式 Worker 系统允许将任务分发到远程 EC2 实例上执行，从而突破单机并发瓶颈。每个 Worker 是一台运行完整 CCM 服务的 EC2 实例，拥有独立的 Codex 账号池（默认）并兼容 Claude 账号池。用户只需在 Manager（主控端）的 UI 上操作，Worker 的创建、部署、监控、销毁全部由 Manager 自动完成。

**核心优势：**
- 水平扩展并发能力，每个 Worker 可配多个 Codex / Claude 账号
- 任务执行位置可实时切换（本机 / 任意 Worker），session 无缝衔接
- Worker 销毁时自动迁回全部任务和数据，不丢失任何上下文
- 前端零感知差异 -- 远程任务与本地任务 UI/操作完全一致

## 架构

```
+------------------------------------------------------------+
|  Manager (用户的主控服务器)                                   |
|  +-------------------------------------------------------+ |
|  |  CCM 服务                                              | |
|  |  UI + API + Dispatcher + WorkerProvisioner             | |
|  |  + WorkerRelay + TaskMigrator                          | |
|  +----+------------------------------------------+-------+ |
|       |                                          |         |
|  本地 Agent 实例                    SSH + WebSocket         |
|  (Manager 自己的账号池)                   |                  |
+------------------------------------------+------------------+
                                           |
            +------------------------------+-------------------+
            |                              |                   |
     +------v------+              +--------v----+      +------v------+
     |  Worker 1   |              |  Worker 2   |      |  Worker 3   |
     |  完整 CCM   |              |  完整 CCM   |      |  完整 CCM   |
     |  独立账号池  |              |  独立账号池  |      |  独立账号池  |
     +-------------+              +-------------+      +-------------+
```

**通信方式：**
- Manager -> Worker：SSH（部署/命令执行）、HTTP API（任务转发/操作代理）、WebSocket（事件中继）
- 所有通信走 VPC 内网 private IP，Worker 不暴露公网
- Worker 安全组仅对 Manager 放行 8000（CCM）和 22（SSH）端口

## 前置条件

### 1. AWS 环境

- Manager 必须运行在 EC2 上（Worker 创建时从 Manager 的实例元数据自举配置）
- Manager 和 Worker 需在同一 VPC，所选子网之间必须可内网路由
- 推荐给 Manager EC2 挂载 IAM Instance Profile，免填 AWS 凭证

### 2. IAM 权限

Manager EC2 的 IAM Role 需要以下 EC2 权限：

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ec2:RunInstances",
        "ec2:DescribeInstances",
        "ec2:StartInstances",
        "ec2:StopInstances",
        "ec2:TerminateInstances",
        "ec2:CreateTags",
        "ec2:DescribeSubnets",
        "ec2:DescribeSecurityGroups",
        "ec2:CreateSecurityGroup",
        "ec2:AuthorizeSecurityGroupIngress"
      ],
      "Resource": "*"
    }
  ]
}
```

### 3. SSH 密钥

Manager 需要一把未加密的 OpenSSH 私钥。创建前会校验它是当前服务用户拥有、权限不宽于 `0600` 的普通文件；系统从私钥派生公钥并通过 EC2 cloud-init 注入新 Worker，因此不要求它与继承的 AWS Key Pair 同名或匹配：

```bash
# 如果还没有密钥，执行 setup.sh 会自动生成
./scripts/setup.sh

# 或手动指定已有的未加密私钥
# 在 .env 中设置：
WORKER_SSH_KEY_PATH=/path/to/your-key.pem
```

### 4. 安全组

创建 Worker 时，CCM 会在同一 VPC 中创建/复用由当前 Manager 管理的专属安全组，并逐条加入：Manager 安全组 → Worker TCP 22 与 `ccm_port`。它不会开放 `0.0.0.0/0`，也不会把“Manager 和 Worker 挂同一个安全组”误当成已经互通。

如通过 `WORKER_SECURITY_GROUP_IDS` 指定安全组，该安全组必须是同 VPC 的 Worker 专用组，且不能已有公网或其他非 CCM 入站规则；CCM 会补齐来自 Manager 安全组的 22/CCM 端口规则。

## Manager 端配置

在 `.env` 中配置以下参数：

### 必填参数

| 环境变量 | 说明 |
|----------|------|
| `WORKER_SSH_KEY_PATH` | 未加密 SSH 私钥的绝对路径；其公钥会自动注入新 Worker |

### 可选参数

| 环境变量 | 默认值 | 说明 |
|----------|--------|------|
| `WORKER_ENABLED` | `true` | 是否启用 Worker 功能 |
| `WORKER_CLOUD_PROVIDER` | `aws` | 云厂商（目前仅支持 `aws`） |
| `WORKER_SSH_USER` | `ubuntu` | Worker EC2 的 SSH 用户名 |
| `WORKER_REMOTE_DIR` | `/home/ubuntu/ccm` | Worker 上 CCM 的部署目录 |
| `WORKER_DEPLOY_SOURCE_DIR` | `.` | rsync 部署源（默认为 Manager 本地仓库根） |
| `WORKER_INSTANCE_TYPE` | (继承 Manager) | 覆盖 Worker 的 EC2 实例类型，如 `c4.xlarge` |
| `WORKER_IMAGE_ID` | (继承 Manager) | 覆盖 Worker 的 AMI ID |
| `WORKER_SUBNET_ID` | (继承 Manager) | 覆盖 Worker 的子网 ID |
| `WORKER_SECURITY_GROUP_IDS` | (自动创建专属组) | 可选 Worker 专用安全组（逗号分隔，必须无公网/额外入站） |
| `WORKER_KEY_NAME` | (不设置) | 可选附加 EC2 Key Pair；SSH 已使用自动注入公钥，通常不要配置第二把钥匙 |

### 配置自举

不填覆盖项时，Worker 创建流程会通过 EC2 IMDSv2 读取 Manager 自身的实例元数据，自动继承：
- 实例类型（instance_type）
- AMI（image_id）
- 子网（subnet_id）

安全组和 SSH 身份不直接继承：CCM 自动创建专属 Worker 安全组并只放行 Manager 来源，且只注入 `WORKER_SSH_KEY_PATH` 派生的公钥。只有显式设置 `WORKER_KEY_NAME` 时，RunInstances 才会附加 AWS Key Pair。

也就是说，**最少只需要配置 `WORKER_SSH_KEY_PATH`，即可创建与 Manager 计算/镜像/子网配置相同、但 SSH 和入站权限隔离的 Worker**。

### 示例 .env

```env
# Worker 基础配置
WORKER_ENABLED=true
WORKER_SSH_KEY_PATH=/home/ubuntu/.ssh/ccm_worker_key

# 可选：使用更大的实例类型
WORKER_INSTANCE_TYPE=c4.xlarge

# 可选：指定 AMI 和安全组
WORKER_IMAGE_ID=ami-0478d64d580a0c8e5
WORKER_SECURITY_GROUP_IDS=sg-056408de7cf971e02
```

## 创建 Worker

### 通过 UI 创建

1. 打开 CCM 前端，进入 **Workers** 页面
2. 点击 **+** 按钮
3. 输入 Worker 名称（必填，也会作为 EC2 Name 标签）
4. 可直接填写一个或多个 Codex 账号（默认；Worker 无人值守登录必须填写邮箱接码 Token，OpenAI 密码可选），也可切换为兼容 Claude 账号
5. 点击 **创建**
6. 系统先校验 SSH 配置，再创建 EC2 并执行 Bootstrap
7. 在 Worker 详情页可实时查看 Bootstrap 进度和日志

### 通过 API 创建

```bash
# 创建 Worker
curl -X POST http://localhost:8000/api/workers \
  -H "Authorization: Bearer $AUTH_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name":"my-worker-1","accounts":[{"provider":"codex","email":"alice@example.com","password":"OPENAI_PASSWORD","token":"MAIL_TOKEN","login_method":"mailcatcher"}]}'

# 响应包含 worker_id，可用于后续操作
```

### 创建后分配账号

账号可随 Worker 一起自动登录，也可在 Worker 就绪后加入 Codex（默认）或 Claude 号池：

1. 进入 Worker 详情页
2. 打开号池面板，点击 **+** 添加账号
3. Codex 填写邮箱接码 Token（OpenAI 密码可选）；自动取码失败时，面板会在同一次登录会话中显示 6 位验证码输入框，可提交或取消
4. 输入邮箱地址，系统会在 Worker 上自动执行登录流程

或通过 API：

```bash
# 添加 Codex 账号到 Worker 号池（provider 省略时默认 codex）
curl -X POST http://localhost:8000/api/workers/{worker_id}/pool/add \
  -H "Authorization: Bearer $AUTH_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"provider":"codex","email":"alice@example.com","password":"OPENAI_PASSWORD","token":"MAIL_TOKEN","login_method":"mailcatcher"}'
```

## Bootstrap 自举流程

Worker 创建后自动执行以下 Bootstrap 步骤。每个步骤的状态会实时广播到前端：

```
ssh-wait        等待 EC2 实例 SSH 可达（最长 3 分钟）
    |
system-init     安装系统依赖，并校验固定 Codex CLI 0.147.0
    |
ccm-deploy      rsync 部署 CCM 代码（从 Manager 本地仓库）
    |
ccm-config      生成 Worker 的 .env 配置文件
    |
docker-sandbox  配置 Docker sandbox 环境（如需要）
    |
ccm-service     创建并启动 systemd 服务
    |
health-check    验证 Worker CCM 服务可访问 + auth_token 校验
    |
account-login   Codex 复用 Worker 本机号池 API；Claude 走兼容登录脚本（如有）
    |
claude-warmup   仅存在 Claude 账号时执行兼容预热
    |
  ready         Worker 就绪，可以接收任务
```

### 部署方式：rsync

Worker 上的 CCM 代码通过 rsync 从 Manager 本地仓库直接同步，不走 git clone：
- 天然实现版本锁定（Manager 和 Worker 代码完全一致）
- Worker 无需 GitHub 凭证
- 排除 `.git`、`.env`、`uploads/` 等目录
- 版本信息通过 `.deploy_commit` 文件记录

### Bootstrap 失败处理

- 任意步骤失败后 Worker 状态变为 `error`，前端显示失败步骤和原因
- 可以点击 **Retry** 按钮从失败步骤继续重试
- 也可以 **Destroy** 后重新创建
- EC2 创建请求会在调用 AWS 前保存非敏感 `provision_spec`，并使用稳定 `ClientToken`；若 RunInstances 已成功但响应丢失，Retry 会先按 token 认领原实例，绝不会盲目再开一台。实例 ID 尚未认领时禁止重命名，避免改变 AWS 幂等请求参数

## 任务转发（Phase 2）

### 在 Worker 上执行任务

1. **创建任务时选择 Worker**：在任务创建表单的 "Run on" 下拉菜单中选择目标 Worker
2. **已有任务切换 Worker**：在任务的 Config 面板中修改 "Run on" 选项

### 工作原理

```
用户创建 Task (worker_id=2)
    |
Dispatcher 识别为 Worker 任务
    |
确保 Worker 上有对应项目（自动创建/clone）
    |
建立 WebSocket 中继连接（WorkerRelay）
    |
调用 Worker API 创建同 ID 的 Task
    |
Worker 本地 Dispatcher 取到任务并执行
    |
执行过程中的所有事件通过 WS 中继回 Manager
    |
Manager 存储日志副本 + 广播到前端
```

### 前端透明

所有代理逻辑在 Manager 后端完成，前端完全不感知任务是本地还是远程：
- API 端点不变
- WebSocket channel 不变
- Chat 操作流程不变
- 历史记录始终查 Manager 本地 DB

### 支持的操作

以下操作会自动代理到 Worker：
- 发送消息（Chat）
- 停止 Session
- 取消/重试任务
- Plan 审批/拒绝
- 创建/删除 Monitor

以下操作直接在 Manager 本地处理：
- 查看对话历史
- 查看任务详情
- 查看 Monitor 状态和检查记录

## 任务迁移（Phase 3 - TaskMigrator）

### 实时切换执行位置

任务的执行位置可以像修改模型一样随时切换：

```bash
# 切到 Worker 2
curl -X PUT http://localhost:8000/api/tasks/{id} \
  -H "Authorization: Bearer $AUTH_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"worker_id": 2}'

# 切回本机
curl -X PUT http://localhost:8000/api/tasks/{id} \
  -H "Authorization: Bearer $AUTH_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"worker_id": -1}'
```

在前端，通过任务 Config 面板的 "Run on" 下拉框操作。

### 迁移流程

```
前置检查：任务不能在 executing 状态（需先 stop）
    |
Step 1: 标记 task.status = "migrating"（前端禁用操作按钮）
    |
Step 2: 从源机取 session 文件（glob 搜索所有账号目录）
    |
Step 3: rsync 工作目录到目标机（含 .git 和未提交改动）
    |
Step 4: session 文件落到目标机第一个账号的 config_dir
    |
Step 5: 确保目标机有 project 记录
    |
Step 6: 在目标机创建同 ID 的 task
    |
Step 7: 切换 WebSocket relay 订阅
    |
Step 8: 更新 task.worker_id，恢复状态
```

**关键特性：**
- 先复制后切指针，失败可重试，不会丢数据
- Session JSONL 文件跨机同步后，`--resume` 可无缝继续对话
- Worker 间迁移经 Manager 两跳（Worker A -> Manager -> Worker B）
- `WORKSPACE_DIR` 路径全机一致，保证 session 路径对得上

### 运行中任务的迁移

正在执行的任务不能直接迁移（PTY/子进程状态搬不走）。前端提供 "停止并切换" 操作：先自动 stop-session，等任务停稳后再发起迁移。

## Worker 生命周期管理

### 状态机

```
creating -> bootstrapping -> ready <-> error
                               |
                            stopping -> stopped -> starting -> ready
                               |
                           destroying -> terminated
```

### 关机（Stop）

保留实例数据（EBS 卷不删除），停止期间只付存储费用。适合 "今天不用了，明天接着干"。

```bash
curl -X POST http://localhost:8000/api/workers/{id}/stop \
  -H "Authorization: Bearer $AUTH_TOKEN"
```

流程：
1. 断开 WebSocket relay 连接
2. SSH 停止 CCM systemd 服务
3. EC2 stop_instances
4. Worker 状态变为 `stopped`

> 钉在 stopped Worker 上的任务：Chat 输入框禁用，提示 "启动 Worker 或切换执行位置"。

### 开机（Start）

```bash
curl -X POST http://localhost:8000/api/workers/{id}/start \
  -H "Authorization: Bearer $AUTH_TOKEN"
```

流程：
1. EC2 start_instances
2. 等待 SSH 可达 + CCM 服务自启
3. 健康检查通过
4. 恢复 relay 连接 + 补全断线期间的日志
5. 版本校验：如果 Worker commit 与 Manager 不一致，UI 显示黄牌

### 销毁（Destroy）

销毁前自动迁回所有任务和数据：

```bash
curl -X POST http://localhost:8000/api/workers/{id}/destroy \
  -H "Authorization: Bearer $AUTH_TOKEN"
```

流程：
1. 停止 Worker 上所有运行中的任务
2. rsync 项目文件回 Manager
3. 归并 session 文件到第一个账号目录
4. rsync session 文件回 Manager
5. 从 Worker API 同步 task 详情（session_id、last_cwd 等）
6. 断开 relay 连接
7. 终止 EC2 实例
8. Worker 状态变为 `terminated`

### 重试 Bootstrap

Bootstrap 失败的 Worker 可以重试：

```bash
curl -X POST http://localhost:8000/api/workers/{id}/retry \
  -H "Authorization: Bearer $AUTH_TOKEN"
```

## 监控和故障排除

### 健康检查

Manager 每 30 秒检查所有 `ready` 和 `error` 状态的 Worker：
- 连续 3 次失败 -> Worker 状态降级为 `error`
- `error` 状态的 Worker 健康检查恢复后自动回到 `ready`（自动恢复 relay 连接 + 补全日志）

### 版本校验

健康检查响应包含 Worker 当前部署的 CCM commit：
- 与 Manager 不一致时，前端 Worker 卡片显示黄牌警告
- 可通过重启 Worker 触发代码同步升级

### WebSocket Relay 断线重连

WorkerRelay 会自动处理断线：
- 指数退避重连（最多 10 次，约 17 分钟）
- 重连成功后自动重新订阅所有活跃 task 的 channel
- 通过对比日志条数补全断线期间缺失的日志
- 10 次重连全部失败 -> 关联 task 标记为 `failed`

### Manager 重启恢复

Manager CCM 重启后会自动：
1. 查找所有在 Worker 上运行中的任务（executing/in_progress/plan_review）
2. 重建 WebSocket relay 连接
3. 补全重启期间缺失的日志

### 查看 Bootstrap 日志

```bash
# API
curl http://localhost:8000/api/workers/{id}/logs \
  -H "Authorization: Bearer $AUTH_TOKEN"

# 或在前端 Worker 详情页查看实时日志
```

### 常见问题

| 问题 | 原因 | 解决方案 |
|------|------|----------|
| 创建前报 `key_*` | 私钥路径为空、权限过宽、软链接、加密或格式不支持 | 使用当前服务用户拥有的未加密普通私钥并设为 `chmod 600` |
| ssh-wait 报 `authentication_failed` | 旧 Worker 没有新公钥，或 AMI 的 SSH 用户与 `WORKER_SSH_USER` 不一致 | 新 Worker 查看 cloud-init；旧 Worker 需销毁重建或手工修正 authorized_keys |
| ssh-wait 报 `host_key_mismatch` | 同一 EC2 实例的 SSH host key 被重置，可能是镜像/主机异常变更 | 核实实例身份后重建；CCM 的 trust store 按 instance ID 隔离，正常的 IP 回收不会误报 |
| ssh-wait 报 `connection_timeout/network_unreachable` | VPC 路由、NACL 或安全组阻断 | 确认同 VPC 可路由；确认专属 Worker SG 有 Manager SG → TCP 22 |
| provision 报创建请求日志/ClientToken 冲突 | RunInstances 结果未知，或创建后 SSH key/config 被修改 | 不要重复手工开机；恢复原私钥后 Retry，让 CCM 按 ClientToken 认领。无法恢复时先在 AWS 核实实例，再销毁记录并重建 |
| health-check 失败 | Worker CCM 服务未启动、端口规则缺失或 auth_token 不匹配 | 查看 Worker 上 `journalctl -u ccm-worker`；确认 Manager SG → CCM 端口 |
| Worker 状态反复 ready/error | 网络抖动 | 检查 VPC 路由、NACL 和安全组；确认子网间可路由 |
| 任务转发后无响应 | Worker 上无空闲 Instance 或无可用账号 | 在 Worker 号池中添加账号 |
| 迁移后 --resume 失败 | session 文件未成功同步 | 检查 WORKSPACE_DIR 两边是否一致；重试迁移 |

## 常用命令参考

### Worker 管理 API

```bash
# 列出所有 Worker
GET  /api/workers

# 创建 Worker
POST /api/workers
Body: {"name": "worker-1"}

# 查看 Worker 详情
GET  /api/workers/{id}

# 查看 Bootstrap 日志
GET  /api/workers/{id}/logs

# 关机
POST /api/workers/{id}/stop

# 开机
POST /api/workers/{id}/start

# 销毁（自动迁回任务）
POST /api/workers/{id}/destroy

# 重试 Bootstrap
POST /api/workers/{id}/retry

# 重命名
PATCH /api/workers/{id}/rename
Body: {"name": "new-name"}
```

### Worker 号池管理 API

```bash
# 查看号池状态（provider 默认 codex，也可传 claude）
GET  /api/workers/{id}/pool?provider=codex

# 添加账号
POST /api/workers/{id}/pool/add
Body: {"provider":"codex","email":"alice@example.com","password":"...","token":"..."}

# 查看号池额度
GET  /api/workers/{id}/pool/usage?provider=codex

# 删除账号
DELETE /api/workers/{id}/pool/{account_id}?provider=codex
```

### Worker 运行时设置 API

```bash
# 查看运行时设置
GET  /api/workers/{id}/settings/runtime

# 更新运行时设置
PUT  /api/workers/{id}/settings/runtime
Body: {"max_concurrent_instances": 3}
```

### 任务分配

```bash
# 创建任务时指定 Worker
POST /api/tasks
Body: {"title": "...", "description": "...", "worker_id": 1}

# 切换已有任务到 Worker
PUT  /api/tasks/{id}
Body: {"worker_id": 1}

# 切回本机
PUT  /api/tasks/{id}
Body: {"worker_id": -1}

# 批量分配到 Worker
PUT  /api/workers/{id}/assign
Body: {"task_ids": [1, 2, 3]}
```

### WebSocket 订阅

```javascript
// 订阅 Worker 状态更新
ws.send(JSON.stringify({
  action: "subscribe",
  channels: ["workers"]
}))

// 事件格式
{
  "event_type": "worker_update",
  "worker_id": 1,
  "status": "bootstrapping",
  "bootstrap_step": "ccm-deploy",
  "log_line": "[14:32:05] rsyncing CCM code..."
}
```
