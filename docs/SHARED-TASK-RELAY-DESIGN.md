# Shared Task Relay 设计方案

> **历史设计，当前已禁用。** 跨 CCM 的 `share_token` HTTP 路由与 `/ws/shared` 不再挂载，本文仅保留早期协议背景；现行 Share 是同一 CCM 内基于真实用户身份的 Team Project/Task ACL，不得按本文重新启用匿名远端执行。

## 概述

共享 task 建立类似 Worker/Manager 的实时中继关系：
- **分享者 CCM** = Worker（拥有 task 数据和 CC 进程）
- **被分享者 CCM** = Manager（实时接收事件、本地存储、前端展示）

被分享者本地创建**影子 task**，通过 WebSocket 实时同步事件，前端直接用现有 ChatView 渲染。

## 核心架构

```
被分享者 CCM                              分享者 CCM
┌──────────────────┐                    ┌──────────────────┐
│  影子 Task (DB)   │  ←── WS relay ──  │  真实 Task (DB)   │
│  log_entries     │  ←── events ────  │  log_entries     │
│  ChatView (前端)  │                    │  CC 进程          │
│                  │  ─── chat msg ──→ │  dispatcher      │
└──────────────────┘                    └──────────────────┘
```

## 数据模型变更

### tasks 表新增字段

```sql
ALTER TABLE tasks ADD COLUMN shared_from_id INTEGER REFERENCES shared_tasks_received(id);
```

`shared_from_id` 非空 = 这是一个影子 task，数据来自远端。

影子 task 的特点：
- `status` 实时从分享者同步
- `session_id` 从分享者同步（用于聊天历史显示）
- `project_id` 为 NULL（不属于本地 project）
- `worker_id` 为 NULL
- `description` 从分享者同步
- 不进入本地调度队列（不会被 dispatcher 执行）

### shared_tasks_received 新增字段

```sql
ALTER TABLE shared_tasks_received ADD COLUMN local_task_id INTEGER REFERENCES tasks(id);
```

关联本地影子 task ID，用于快速查找。

## 后端实现

### 1. SharedRelay 服务（新建 `backend/services/shared_relay.py`）

模仿 `WorkerRelay`，但用 share_token 认证：

```python
class SharedRelay:
    """被分享者侧：连接到分享者 CCM 的 /ws/shared，实时接收事件。"""
    
    def __init__(self, db_factory, broadcaster):
        self.db_factory = db_factory
        self.broadcaster = broadcaster
        self._connections: dict[int, websockets.WebSocketClientProtocol] = {}
        # shared_task_received.id -> ws connection
```

**连接生命周期**：
- 收到 share push（`/api/shared/receive`）或页面加载时，建立 WS 连接
- WS URL: `ws(s)://{owner_ccm_url}/ws/shared?token={share_token}&task_id={remote_task_id}`
- 连接断开自动重连（指数退避，最多 10 次）
- 取消分享或 leave 时断开

**事件处理**（和 WorkerRelay._handle 类似）：
- chat 事件（message/tool_use/tool_result/etc.）→ 写入本地 log_entries（task_id = 影子 task ID）
- status_change → 更新影子 task.status
- context_usage → 更新影子 task.context_window_usage
- user_message → 跳过（本地 chat 代理已存）

### 2. 影子 Task 创建

在 `/api/shared/receive` 收到分享推送时：
1. 创建 `shared_tasks_received` 记录
2. 创建影子 task：
   ```python
   shadow_task = Task(
       title=payload.task_title,
       description=payload.task_description,
       status="pending",
       shared_from_id=shared_record.id,
       # project_id 不设（不属于本地 project）
   )
   ```
3. 更新 `shared_tasks_received.local_task_id = shadow_task.id`
4. 从分享者拉取完整 task 状态（shared-access/config）更新影子 task
5. 启动 SharedRelay 连接
6. Backfill 历史 log_entries（从 shared-access/history 拉取）

### 3. Chat 消息代理

被分享者在 ChatView 发消息时，走现有的 chat API（`POST /api/tasks/{shadow_task_id}/chat`），但检测到 `shared_from_id` 后：
1. 存 user_message 到本地 log_entries
2. 广播到本地前端
3. 代理到分享者 CCM 的 `POST /api/shared-access/{remote_task_id}/chat`
4. 分享者 dispatcher 处理 → CC 回复 → WS relay 推送回来 → 写入本地 DB → 广播到前端

### 4. 取消分享

**分享者撤销**：
- `task_shares.status = revoked`
- 推送 `/api/shared/revoke` 到被分享者
- 被分享者收到后：断开 relay、删除 shared_tasks_received、标记影子 task 为 `cancelled`

**被分享者主动 leave**：
- 断开 relay
- 删除 shared_tasks_received
- 标记影子 task 为 `cancelled`

### 5. 分享者侧 `/ws/shared` 改造

当前 `/ws/shared` 只做了 accept + subscribe。需要确保：
- 被分享者的 WS 连接持久化（不只是页面打开时）
- 后端 SharedRelay 服务端连接，不是浏览器直连

## 前端变更

### 影子 task 在 Tasks 页面显示

影子 task 在本地 tasks 表里，会自动出现在 TasksPage 的 task 列表中。需要：
- 加 `shared_from` 标识徽章（橙色 "from xxx"）
- Config 相关操作禁用（不能修改远端 task 的配置）
- Chat 可用（走代理到分享者）

### ChatView 复用

影子 task 直接用现有 ChatView：
- 历史从本地 log_entries 读（relay 已同步）
- 发消息走本地 chat API（自动代理到分享者）
- 实时更新走本地 WebSocket（relay 写入 DB 后广播）

### Team 页面

Team 页面的 "Shared with me" 改为链接到 Tasks 页面的对应影子 task：
- 点击 → `window.location.hash = '#/tasks/chat/{shadow_task_id}'`
- 不再需要 SharedChatView 组件

### 删除 SharedChatView

不再需要，全部走 ChatView。

## API 变更

### 修改

| 端点 | 变更 |
|------|------|
| `POST /api/shared/receive` | 创建影子 task + 启动 relay |
| `POST /api/shared/revoke` | 断开 relay + 标记影子 task cancelled |
| `DELETE /api/shared/{id}` | 断开 relay + 标记影子 task cancelled |
| `POST /api/tasks/{id}/chat` | 检测 shared_from_id → 代理到分享者 |
| `GET /api/tasks` | 影子 task 自动包含在列表中 |

### 不再需要

| 端点 | 原因 |
|------|------|
| `GET /api/shared/tasks` | 影子 task 在 tasks 列表中 |
| `GET /api/shared/{id}/history` | 本地 log_entries 有 |
| `POST /api/shared/{id}/chat` | 走标准 chat API |
| `GET /api/shared/{id}/config` | 本地 task 有 |

## 数据库迁移

```sql
-- 1. tasks 表加 shared_from_id
ALTER TABLE tasks ADD COLUMN shared_from_id INTEGER REFERENCES shared_tasks_received(id) ON DELETE SET NULL;
CREATE INDEX ix_tasks_shared_from_id ON tasks(shared_from_id);

-- 2. shared_tasks_received 加 local_task_id
ALTER TABLE shared_tasks_received ADD COLUMN local_task_id INTEGER;
```

## 实现步骤

1. Migration: tasks.shared_from_id + shared_tasks_received.local_task_id
2. SharedRelay 服务（WS 连接 + 事件处理 + 重连）
3. `/api/shared/receive` 改造：创建影子 task + 启动 relay + backfill
4. `/api/tasks/{id}/chat` 改造：检测 shared_from_id → 代理
5. 前端 Task 卡片：shared_from_id 非空时显示 "from xxx" 徽章 + 禁用配置
6. Team 页面 "Shared with me" 改为链接到影子 task
7. 删除 SharedChatView、旧的 proxy 端点
8. 测试

## 已知限制

- **分享者离线**：relay 断连，重连失败后影子 task 状态冻结，显示离线提示
- **历史 backfill**：首次连接时拉取全量历史，大 task 可能慢
- **多人同时发消息**：和本地多 tab 发消息一样，先发先执行
- **配置变更**：被分享者不能修改远端 task 的 model/mode 等配置

## 和 Worker Relay 的区别

| 方面 | Worker Relay | Shared Relay |
|------|-------------|-------------|
| 认证 | worker.auth_token | share_token |
| 发起方 | Manager 连到 Worker | 被分享者连到分享者 |
| Task 所有权 | Manager 创建，Worker 执行 | 分享者创建+执行 |
| 配置修改 | Manager 可改 | 被分享者只读 |
| 生命周期 | Worker 存在期间 | share 有效期间 |
| 断连处理 | 标记 task failed | 显示离线提示 |
