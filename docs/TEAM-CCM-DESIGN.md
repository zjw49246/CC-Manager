# Team CCM 设计方案 v2

> **历史设计，P2P 执行通道当前已禁用。** 跨 CCM 的 `share_token` HTTP 路由、代理写入与 `/ws/shared` 不再挂载；现行协作只使用同一 CCM 内基于真实用户身份的 Team Project/Task ACL。本文其余内容仅作早期方案背景。

> P2P 任务分享 + 飞书身份绑定，让多人可以对同一个 task 进行协作对话。
>
> v2 重写：去掉中心 Hub 节点和飞书消息映射，改为轻量的 P2P 任务分享模式。

## 1. 核心思路

每个 CCM 都是平等的个人节点，没有中心 Hub。核心功能：

1. **飞书身份绑定**：CCM 绑定飞书账号，发现同一组织内的其他 CCM 用户
2. **Team 分组**：组织成员可以分成小组，分享时可以直接选组
3. **Task / Project 分享**：将 task 或整个 project 分享给成员或小组
4. **代理模式**：被分享的数据始终在分享者机器上，被分享者的 CCM 代理请求

**不做的事情**：
- 没有中心 Hub 节点
- 飞书不做消息转发/建群，只做身份绑定 + 分享通知
- 不同步数据到被分享者的 DB

## 2. 飞书集成

### 2.1 飞书应用

整个组织共用**一个飞书企业自建应用**（在飞书开放平台创建一次）：
- 所有 CCM 实例共用同一个 `app_id` / `app_secret`
- 每个 CCM 有自己的 `redirect_uri`（飞书应用支持配置多个重定向地址）
- 凭证通过 `.env` 配置：

```bash
FEISHU_APP_ID=cli_xxx
FEISHU_APP_SECRET=your_app_secret_here
```

需要的飞书权限：
- `contact:user.employee_id:readonly`（读取组织成员列表）
- 网页登录能力（OAuth）
- `im:message:send_as_bot`（Bot 发 DM 通知）
- `im:message`（接收群消息，Phase 5 用）
- `im:chat:readonly`（读取群成员列表，Phase 5 用）
- `im:resource`（读取消息中的文件/图片，Phase 5 用）
- 事件订阅 `im.message.receive_v1`（WebSocket 模式，Phase 5 用）

### 2.2 绑定流程

```
1. 用户在 CCM 设置页（PTY 旁边）点"绑定飞书"

2. 前端跳转飞书授权页：
   https://open.feishu.cn/open-apis/authen/v1/authorize
     ?app_id=cli_xxx
     &redirect_uri=https://my-ccm.com/api/feishu/callback
     &state=random_state

3. 用户在飞书页面登录并授权

4. 飞书回调 CCM：
   GET /api/feishu/callback?code=xxx&state=random_state

5. CCM 后端用 code 换 user_access_token：
   POST https://open.feishu.cn/open-apis/authen/v1/oidc/access_token

6. 用 access_token 获取用户信息：
   GET https://open.feishu.cn/open-apis/authen/v1/user_info
   → 拿到 open_id、name、email、avatar_url

7. 写入本地 feishu_user_binding 表

8. 向组织注册表上报：
   POST {ORG_REGISTRY_URL}/api/org/register
   body: { open_id, name, ccm_url, avatar_url }
```

一个飞书账号同时只绑一个 CCM。换绑时先在旧 CCM 解绑（自动从注册表注销），再在新 CCM 绑定。

### 2.3 飞书的作用范围

| 用途 | 说明 |
|------|------|
| 身份绑定 | CCM 用户 ↔ 飞书 open_id |
| 组织发现 | 通过注册表查看组织内其他 CCM 用户 |
| 分享通知 | 分享/取消分享时通过飞书 DM 通知对方 |

**不做**：不建群、不转发消息、不做消息双向同步。不绑定飞书也能手动输入 CCM 地址进行分享。

## 3. 组织注册表

### 3.1 设计

P2P 架构没有中心节点，但成员发现需要一个共同的查询点。方案：**指定一个 CCM 兼任注册表**。

```bash
# 注册表所有者的 .env
ORG_REGISTRY_ENABLED=true

# 其他成员的 .env
ORG_REGISTRY_URL=https://youchengsong.claude-code-manager.com
```

注册表就是额外的 API 端点 + 表，不需要额外部署服务。

只有注册过 CCM（即绑定飞书并上报了 URL）的成员，在分享时才会被显示出来。

### 3.2 注册表 API

```
POST   /api/org/register              注册/更新自己的信息
GET    /api/org/members               获取组织内所有已注册成员
DELETE /api/org/members/{open_id}      注销成员
POST   /api/org/transfer              移交注册表到另一个 CCM
POST   /api/org/import                接收移交过来的注册表数据
POST   /api/org/registry-changed      接收注册表地址变更通知
```

### 3.3 注册表数据

```sql
-- 仅在注册表所有者的 CCM 上有数据
CREATE TABLE org_members (
    id INTEGER PRIMARY KEY,
    feishu_open_id VARCHAR(100) NOT NULL UNIQUE,
    name VARCHAR(100) NOT NULL,
    ccm_url VARCHAR(500) NOT NULL,
    avatar_url VARCHAR(500),
    registered_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen_at DATETIME
);
```

### 3.4 注册表转移

注册表所有者可以将注册表移交给组织内其他成员：

```
1. 当前所有者（A）在设置页点"移交注册表" → 选择新所有者（B）

2. A 的 CCM 推送注册表数据到 B：
   POST B/api/org/import
   body: { members: [...] }

3. B 的 CCM 存下数据，启用 ORG_REGISTRY_ENABLED

4. A 的 CCM 通知所有其他成员（注册表里有每个人的 ccm_url）：
   POST 每个成员/api/org/registry-changed
   body: { new_registry_url: "https://b.xxx.com" }

5. 每个成员的 CCM 自动更新 ORG_REGISTRY_URL

6. A 清除自己的 ORG_REGISTRY_ENABLED，变回普通成员
```

用户感知：点"移交" → 选人 → 完成。全自动。

## 4. Team 分组

### 4.1 设计

在 Workers 旁边新增 **Team** 页面，用于管理组织成员的分组。分组数据存在**注册表所有者的 CCM** 上（和 org_members 一起），所有成员共享同一套分组。

分组的作用：分享 task/project 时可以直接选择一个小组，相当于批量选中该组的所有成员。

### 4.2 Team 页面

```
┌──────────────────────────────────────────────────┐
│  Team                                [+ 新建小组] │
├──────────┬───────────────────────────────────────┤
│ 小组列表  │  小组详情                              │
│          │                                       │
│ > 前端组  │  成员：                                │
│   后端组  │    张三 (zhang3@company.com)           │
│   AI组   │    李四 (li4@company.com)              │
│          │    王五 (wang5@company.com)             │
│          │                                       │
│          │  [添加成员]  [移除成员]                  │
│          │                                       │
│ 未分组    │                                       │
│   赵六   │                                       │
│   钱七   │                                       │
└──────────┴───────────────────────────────────────┘
```

### 4.3 分组数据

```sql
-- 小组（存在注册表所有者的 CCM 上）
CREATE TABLE org_teams (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 小组成员关系（一个成员可以属于多个小组）
CREATE TABLE org_team_members (
    id INTEGER PRIMARY KEY,
    team_id INTEGER NOT NULL REFERENCES org_teams(id) ON DELETE CASCADE,
    feishu_open_id VARCHAR(100) NOT NULL,
    UNIQUE(team_id, feishu_open_id)
);
```

### 4.4 分组 API（注册表所有者端点）

```
POST   /api/org/teams                      创建小组
GET    /api/org/teams                      列出所有小组（含成员）
PUT    /api/org/teams/{id}                 更新小组信息
DELETE /api/org/teams/{id}                 删除小组
POST   /api/org/teams/{id}/members         添加成员到小组
DELETE /api/org/teams/{id}/members/{open_id}  从小组移除成员
```

所有 CCM 都可以通过注册表 API 查询分组信息，但只有注册表所有者可以创建/修改分组。

## 5. 分享功能

### 5.1 分享粒度

两个级别的分享：

| 级别 | 位置 | 效果 |
|------|------|------|
| **Task 分享** | Task 详情页的"分享"按钮 | 分享单个 task |
| **Project 分享** | Projects 界面每个 project 的"分享"按钮 | 分享整个 project，其下所有 task（含未来新建的）自动分享 |

### 5.2 分享目标

分享时可以选择：
- **单个成员**：从组织成员列表中多选
- **小组**：选择一个或多个小组，等于批量选中该组所有成员

### 5.3 Task 分享流程

```
1. 分享者在 task 页面点"分享" → 弹出分享弹窗

2. 弹窗显示：
   - 小组列表（可勾选整组）
   - 组织成员列表（可单独勾选）

3. 确认后，分享者 CCM 为每个被分享者：
   a. 生成 share_token（限定该 task + 只读+对话权限）
   b. 在本地 task_shares 表记录分享关系
   c. 推送分享信息到被分享者 CCM：
      POST 被分享者/api/shared/receive
      body: { 
        owner_ccm_url, task_id, share_token,
        task_title, task_description,
        owner_name, owner_feishu_id
      }
   d. 通过飞书 DM 通知被分享者：
      "张三分享了一个 task 给你：{task_title}
       点击查看：https://被分享者ccm.com/shared/{id}"

4. 被分享者 CCM 收到后：
   a. 存入本地 shared_tasks_received 表
   b. 前端 Shares 区域立即显示
```

### 5.4 Project 分享流程

```
1. 分享者在 Projects 界面，点某个 project 旁的"分享"按钮

2. 弹出分享弹窗（和 task 分享一样，可选小组或成员）

3. 确认后，分享者 CCM：
   a. 在本地 project_shares 表记录分享关系
   b. 为该 project 下所有现有 task 逐个创建 task_shares + 推送
   c. 飞书 DM 通知被分享者

4. 后续该 project 下新建的 task 自动继承分享设置：
   - 新 task 创建时检查 project_shares
   - 自动为每个被分享者生成 share_token 并推送
```

### 5.5 被分享者查看 Task

**代理模式**：被分享者 CCM 不存 task 数据，实时从分享者 CCM 拿。

```
被分享者打开共享 task
  │
  ├─ 前端请求 GET /api/shared/{id}/history
  │
  ├─ 被分享者 CCM 后端代理：
  │   GET {owner_ccm_url}/api/shared-access/{task_id}/history
  │   headers: { Authorization: Bearer {share_token} }
  │
  ├─ 返回聊天历史 → 前端渲染
  │
  └─ 前端直连分享者 CCM 的 WebSocket 订阅实时事件：
     WS {owner_ccm_url}/ws/shared?token={share_token}&task_id={task_id}
     → 实时收到 Claude 回复、工具调用等事件
     → 关闭页面时断开（不是永久连接）
```

实时事件采用**前端直连**分享者 CCM 的 WebSocket（不经过被分享者 CCM 代理），更简单高效。share_token 限定只能访问对应 task，安全可控。

### 5.6 被分享者发消息

```
被分享者在共享 task 的 ChatView 输入消息
  │
  ├─ 前端 POST /api/shared/{id}/chat
  │   body: { message: "用户消息" }
  │
  ├─ 被分享者 CCM 后端代理：
  │   POST {owner_ccm_url}/api/shared-access/{task_id}/chat
  │   headers: { Authorization: Bearer {share_token} }
  │   body: { message: "[李四] 用户消息" }
  │
  └─ 分享者 CCM 处理消息 → Claude 回复
     → 通过 WebSocket 推送到所有在线的被分享者
```

消息前缀 `[xxx]`（飞书账号名）放在消息最前面，让分享者和 Claude 能区分消息来源。分享者自己发的消息不加前缀。

### 5.7 多人同时对话

多个人同时给同一个 task 发消息时，逻辑和单人使用完全一致：
- 先发的先执行
- 后发的进入 pending queue
- task 本身不感知是多人在操作
- 分享者 CCM 的现有 `WebSocketBroadcaster` 按 `task:{id}` channel 广播，所有连着的 shared WS 连接都能收到事件

### 5.8 被分享者权限

| 操作 | 是否允许 |
|------|---------|
| 查看 task 聊天历史 | 可以 |
| 发送消息 | 可以 |
| 查看 config/模式（只读） | 可以 |
| 修改 task 配置/模式 | 不可以 |
| 停止/取消/重试 task | 不可以 |
| 创建新 task | 不可以 |
| 分享给其他人 | 不可以（只有 task 所有者可以） |

### 5.9 管理分享

**分享者可以随时增删被分享者**：

增加成员：
```
分享者点"管理分享" → 添加新成员
  → 生成 share_token → 写入 task_shares
  → 推送到新成员 CCM
  → 飞书 DM 通知新成员
```

删除成员：
```
分享者点"管理分享" → 移除成员
  → task_shares.status = "revoked"
  → 通知被分享者 CCM：POST /api/shared/revoke
  → 飞书 DM 通知被分享者："张三取消了 task 的分享"
  → 被分享者 CCM 删除 shared_tasks_received 记录
  → share_token 失效，后续请求被拒
```

### 5.10 分享者 CCM 离线

被分享者打开共享 task 时，如果分享者 CCM 不可达，显示文字提示："分享者 CCM 不可达，请稍后再试"。不做数据缓存。

## 6. 前端

### 6.1 Team 页面

在 Workers 旁边新增 **Team** 导航项。用于查看组织成员和管理分组。

### 6.2 Shares 按钮

在 Tasks 页面的 Filter / Projects 栏旁边新增 **Shares** 按钮：

```
┌──────────────────────────────────────────────┐
│  🔽 Filter    📁 Projects  ▼    🔍   Shares │
├──────────────────────────────────────────────┤
│  正常 task 列表...                            │
└──────────────────────────────────────────────┘
```

点击 Shares 按钮后，切换到共享 task 视图：

```
┌──────────────────────────────────────────────┐
│  🔽 Filter    📁 Projects  ▼    🔍   Shares │
├──────────────────────────────────────────────┤
│  📎 调研 VibeThinker        来自 张三         │
│  📎 部署 NanoChat 环境      来自 李四         │
│  📎 CI/CD 流水线调试        来自 王五         │
└──────────────────────────────────────────────┘
```

再次点击 Shares 回到正常视图。

### 6.3 共享 Task 列表项

和普通 task 显示一样，额外标注：
- 分享标记图标（📎 或其他）
- "来自 xxx"（分享者飞书账号名）

### 6.4 共享 Task 的 ChatView

和普通 ChatView 布局一致，区别：
- Config 按钮点开后内容**只读**（显示全部配置，但按钮灰掉/禁用）
- 没有停止/取消/重试按钮
- 输入框提示文字："以共享成员身份发送消息"
- 特殊 URL：`/shared/{shared_task_id}`

### 6.5 分享管理弹窗

Task 详情页和 Project 列表都有"分享"按钮：

```
Task 分享按钮：
┌─ Task: 调研 VibeThinker-3B ──────────────────────┐
│  [Chat] [Config] [分享 👥2]                       │
└───────────────────────────────────────────────────┘

Project 分享按钮（Projects 界面）：
┌──────────────────────────────────────────────┐
│  web-app        3 tasks    [分享 👥1]         │
│  api-server     5 tasks    [分享]             │
└──────────────────────────────────────────────┘

点击弹窗：
┌─ 分享管理 ────────────────────────────┐
│  已分享给：                            │
│    ☑ 李四  [移除]                      │
│    ☑ 王五  [移除]                      │
│                                       │
│  小组：                                │
│    ☐ 前端组 (3人)                      │
│    ☐ AI组 (2人)                        │
│                                       │
│  成员：                                │
│    ☐ 张三                              │
│    ☐ 赵六                              │
│                                       │
│  [确认]  [取消]                        │
└───────────────────────────────────────┘
```

### 6.6 设置页面

在 PTY 开关旁边增加飞书绑定：

```
┌─ 设置 ──────────────────────────────────┐
│  PTY 模式    [开启/关闭]                  │
│  飞书绑定    [绑定飞书] / 已绑定: 张三 [解绑] │
└──────────────────────────────────────────┘
```

## 7. 数据模型

### 7.1 所有 CCM 共有

```sql
-- 飞书用户绑定（本 CCM 的所有者）
CREATE TABLE feishu_user_binding (
    id INTEGER PRIMARY KEY,
    feishu_open_id VARCHAR(100) NOT NULL UNIQUE,
    feishu_name VARCHAR(100),
    avatar_url VARCHAR(500),
    access_token TEXT,               -- 飞书 user_access_token（发消息用）
    token_expires_at DATETIME,
    bound_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

### 7.2 注册表所有者的表

```sql
-- 组织成员注册表
CREATE TABLE org_members (
    id INTEGER PRIMARY KEY,
    feishu_open_id VARCHAR(100) NOT NULL UNIQUE,
    name VARCHAR(100) NOT NULL,
    ccm_url VARCHAR(500) NOT NULL,
    avatar_url VARCHAR(500),
    registered_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen_at DATETIME
);

-- 小组
CREATE TABLE org_teams (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 小组成员关系
CREATE TABLE org_team_members (
    id INTEGER PRIMARY KEY,
    team_id INTEGER NOT NULL REFERENCES org_teams(id) ON DELETE CASCADE,
    feishu_open_id VARCHAR(100) NOT NULL,
    UNIQUE(team_id, feishu_open_id)
);
```

### 7.3 分享者侧

```sql
-- Task 级别分享
CREATE TABLE task_shares (
    id INTEGER PRIMARY KEY,
    task_id INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    shared_to_open_id VARCHAR(100) NOT NULL,
    shared_to_name VARCHAR(100),
    shared_to_ccm_url VARCHAR(500) NOT NULL,
    share_token VARCHAR(200) NOT NULL UNIQUE,
    status VARCHAR(20) NOT NULL DEFAULT 'active',  -- active / revoked
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(task_id, shared_to_open_id)
);

-- Project 级别分享（该 project 下所有 task 自动分享）
CREATE TABLE project_shares (
    id INTEGER PRIMARY KEY,
    project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    shared_to_open_id VARCHAR(100) NOT NULL,
    shared_to_name VARCHAR(100),
    shared_to_ccm_url VARCHAR(500) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'active',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(project_id, shared_to_open_id)
);
```

### 7.4 被分享者侧

```sql
-- 别人分享给我的 task
CREATE TABLE shared_tasks_received (
    id INTEGER PRIMARY KEY,
    owner_ccm_url VARCHAR(500) NOT NULL,
    owner_name VARCHAR(100),
    owner_feishu_open_id VARCHAR(100),
    remote_task_id INTEGER NOT NULL,
    share_token VARCHAR(200) NOT NULL,
    task_title VARCHAR(200),
    task_description TEXT,
    project_name VARCHAR(100),          -- 所属 project 名（显示用）
    status VARCHAR(20) NOT NULL DEFAULT 'active',
    received_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(owner_ccm_url, remote_task_id)
);
```

### 7.5 数据关系图

```
分享者 CCM:
  feishu_user_binding (自己的飞书身份)
  tasks ──→ task_shares (单个 task 分享给了谁)
  projects ──→ project_shares (整个 project 分享给了谁)
                 └→ 新 task 创建时自动继承 → task_shares

注册表所有者 CCM（额外）:
  org_members (组织成员列表)
  org_teams ──→ org_team_members (分组)

被分享者 CCM:
  feishu_user_binding (自己的飞书身份)
  shared_tasks_received (别人分享给我的)

请求流:
  被分享者前端 ─HTTP─→ 被分享者 CCM API ─代理─→ 分享者 CCM API (share_token)
  被分享者前端 ─WS──→ 分享者 CCM WebSocket (share_token, 直连)
```

## 8. 认证与安全

### 8.1 share_token 设计

- 分享者为每个 (task_id, 被分享者) 组合生成唯一的 share_token
- token 是随机字符串（`secrets.token_urlsafe(32)`）
- 在分享者 CCM 侧验证：查 `task_shares` 表，检查 token 有效且 status=active
- token 权限限定：只能访问对应的 task，只能查看 + 发消息

### 8.2 分享者 CCM 新增的访问端点

```
# 被分享者通过 share_token 访问（不需要 admin auth_token）
GET    /api/shared-access/{task_id}/history?token={share_token}
POST   /api/shared-access/{task_id}/chat?token={share_token}
GET    /api/shared-access/{task_id}/config?token={share_token}
WS     /ws/shared?token={share_token}&task_id={task_id}
```

## 9. 后端服务

### 9.1 新增服务

| 服务 | 文件 | 说明 |
|------|------|------|
| FeishuAuth | `services/feishu_auth.py` | 飞书 OAuth 绑定：授权跳转、回调处理、token 刷新 |
| OrgRegistry | `services/org_registry.py` | 组织注册表：成员注册/查询/转移 + 分组管理 |
| TaskSharing | `services/task_sharing.py` | Task/Project 分享：创建/撤销、token 生成、推送 |
| SharedProxy | `services/shared_proxy.py` | 共享代理：代理被分享者的请求到分享者 CCM |
| FeishuNotify | `services/feishu_notify.py` | 飞书通知：发送分享/撤销 DM 消息 |

### 9.2 新增 API 端点

```
# 飞书绑定
GET    /api/feishu/auth-url              获取飞书授权 URL
GET    /api/feishu/callback              飞书 OAuth 回调
GET    /api/feishu/status                绑定状态
DELETE /api/feishu/unbind                解除绑定

# 组织注册表
POST   /api/org/register                注册/更新自己
GET    /api/org/members                 获取组织成员列表
DELETE /api/org/members/{open_id}        注销成员
POST   /api/org/transfer                移交注册表
POST   /api/org/import                  接收移交数据
POST   /api/org/registry-changed        接收注册表地址变更通知

# 分组管理（注册表所有者端点）
POST   /api/org/teams                   创建小组
GET    /api/org/teams                   列出所有小组（含成员）
PUT    /api/org/teams/{id}              更新小组
DELETE /api/org/teams/{id}              删除小组
POST   /api/org/teams/{id}/members      添加成员
DELETE /api/org/teams/{id}/members/{open_id}  移除成员

# Task 分享（分享者侧）
POST   /api/tasks/{id}/share            分享 task（可选成员或小组）
DELETE /api/tasks/{id}/share/{open_id}  取消分享
GET    /api/tasks/{id}/shares           查看分享列表

# Project 分享（分享者侧）
POST   /api/projects/{id}/share         分享 project（可选成员或小组）
DELETE /api/projects/{id}/share/{open_id}  取消分享
GET    /api/projects/{id}/shares        查看分享列表

# 分享者为被分享者提供的访问端点（share_token 认证）
GET    /api/shared-access/{task_id}/history  聊天历史
POST   /api/shared-access/{task_id}/chat    发送消息
GET    /api/shared-access/{task_id}/config  查看配置（只读）
WS     /ws/shared?token={share_token}&task_id={task_id}  实时事件

# 共享 task（被分享者侧）
POST   /api/shared/receive              接收分享推送
POST   /api/shared/revoke               接收撤销通知
GET    /api/shared/tasks                列出共享给我的 task
GET    /api/shared/{id}/history         代理获取聊天历史
POST   /api/shared/{id}/chat            代理发送消息
GET    /api/shared/{id}/config          代理获取配置
DELETE /api/shared/{id}                 主动退出共享
```

## 10. 完整数据流

### 10.1 飞书绑定 + 注册

```
用户点"绑定飞书"（设置页，PTY 旁边）
  → 浏览器跳转飞书 OAuth → 授权 → 回调
  → CCM 拿到 open_id + name + avatar
  → 写入 feishu_user_binding
  → POST {ORG_REGISTRY_URL}/api/org/register
  → 注册表记录该用户的 ccm_url
  → 分享功能解锁
```

### 10.2 分享 Task

```
分享者点"分享" → 弹窗显示小组 + 成员列表
  → 可勾选小组（批量选中该组成员）或单独选成员
  → 确认
  → 为每个成员：
     1. 生成 share_token → 写入 task_shares
     2. POST 被分享者ccm/api/shared/receive
     3. 飞书 DM 通知
  → 被分享者 CCM 收到 → 写入 shared_tasks_received
  → 被分享者 Shares 视图出现新 task
```

### 10.3 分享 Project

```
分享者在 Projects 界面点 project 的"分享"
  → 弹窗选小组/成员 → 确认
  → 写入 project_shares
  → 为该 project 下所有现有 task 逐个创建 task_shares + 推送
  → 飞书 DM 通知
  → 后续该 project 新建 task 时自动继承分享
```

### 10.4 被分享者查看和对话

```
被分享者点击共享 task
  → GET /api/shared/{id}/history → 代理到分享者 CCM
  → 前端直连分享者 WS（实时事件）

被分享者发消息
  → POST /api/shared/{id}/chat → 代理到分享者 CCM
  → 消息前缀 "[李四]"（飞书账号名）
  → 分享者 CCM 注入 session → Claude 回复 → WS 推送
```

### 10.5 取消分享

```
分享者移除成员
  → task_shares.status = "revoked"（或 project_shares）
  → POST 被分享者ccm/api/shared/revoke
  → 飞书 DM 通知
  → 被分享者删除 shared_tasks_received
  → share_token 失效
```

## 11. 配置项

```bash
# 飞书应用（整个组织共用）
FEISHU_APP_ID=cli_xxx
FEISHU_APP_SECRET=your_app_secret_here

# 组织注册表
ORG_REGISTRY_URL=https://youchengsong.claude-code-manager.com
ORG_REGISTRY_ENABLED=true  # 仅注册表所有者设 true
```

## 12. 实施计划

### Phase 1：飞书绑定 + 组织注册表
- feishu_user_binding 表 + OAuth 绑定流程
- org_members 表 + 注册/查询 API
- 注册表转移功能
- 设置页面飞书绑定 UI（PTY 旁边）

### Phase 2：Team 分组 + 分享基础
- org_teams / org_team_members 表 + 分组 API
- Team 页面（Workers 旁边）
- task_shares / project_shares / shared_tasks_received 表
- Task 分享 + Project 分享 API
- 分享管理弹窗 UI（task 和 project）
- Shares 按钮 + 共享 task 列表 UI
- 飞书 DM 通知

### Phase 3：共享 Task 交互
- 分享者侧：shared-access API（share_token 认证）
- 分享者侧：shared WebSocket 端点（前端直连）
- 被分享者侧：SharedProxy 代理 HTTP 请求
- 被分享者侧：共享 ChatView（只读 config、可发消息）
- Project 分享自动继承（新 task 自动分享）

### Phase 4：完善
- 分享者 CCM 离线提示
- 分享状态变更通知（task 完成/失败时通知被分享者）
- 注册表心跳（检测成员在线状态）
- 共享 task 搜索/过滤

### Phase 5：飞书群消息创建 Task（后续）
- 在飞书群里添加 CCM Bot
- 用户 @Bot 触发任务创建
- 自动分享给群内已注册成员
- 支持选择 project 归属

## 13. 关键设计决策

| 决策 | 选择 | 理由 |
|------|------|------|
| 架构模式 | P2P 任务分享 | 每个 CCM 平等，无中心节点 |
| 数据存储 | 代理模式，不同步 | 数据始终在分享者机器上，避免多端同步 |
| 飞书用途 | 身份绑定 + 通知 | 不建群不转发，可替换为其他 IM |
| 飞书应用 | 整个组织共用一个 | 一次创建，凭证分发 |
| 组织注册表 | 指定 CCM 兼任，可转移 | 零额外部署 |
| 分享粒度 | Task 级别 + Project 级别 | 单个精确控制 + 批量便捷 |
| 分享目标 | 成员 + 小组 | 小组批量选择，减少重复操作 |
| 被分享者权限 | 查看 + 对话，config 只读 | 所有权不变 |
| 实时事件 | 前端直连分享者 WS | 不代理 WS，更简单 |
| 多人消息 | 先发先执行，后发 pending | 和单人一致，task 不感知多人 |
| 消息标识 | 前缀 `[xxx]`（飞书账号名） | 区分来源 |
| 分享者离线 | 文字提示 | 简单处理 |
| 前端入口 | Team 页面 + Shares 按钮 | 分组在 Team，共享 task 在 Shares |
