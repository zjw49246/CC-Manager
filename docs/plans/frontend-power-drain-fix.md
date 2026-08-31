# 前端耗电问题修复方案

> 2026-08-31 排查结果 + 修复方案 + 自动化测试设计

## 概述

前端存在多个导致电脑持续耗电的问题，核心根因是：**HTTP 轮询在标签页不可见时不暂停** + **WebSocket 连接不共享** + **冗余状态更新触发不必要的 React 渲染**。

---

## 问题清单与修复方案

### P0：HTTP 轮询无 visibility 暂停

**问题**：几乎所有 `setInterval` 轮询在浏览器标签页切到后台后仍然持续运行。**仅 `UpdateButton.tsx:349` 做了 `document.visibilityState` 检查**，其余全部无视标签页可见性。

**受影响组件**：

| 组件 | 文件:行 | 频率 | 生命周期 |
|------|---------|------|----------|
| AskUserNotifications | `AskUserNotifications.tsx:50` | 5s | App 顶层常驻 |
| AppShell (非 admin) | `AppShell.tsx:105` | 30s | 常驻 |
| Dashboard | `Dashboard.tsx:26` | 5s | 页面激活时 |
| TasksPage | `TasksPage.tsx:202` | 5s | 页面激活时 |
| ProjectsPage | `ProjectsPage.tsx:740` | 5s | 页面激活时 |
| WorkersPage | `WorkersPage.tsx:1103` | 30s | 页面激活时 |
| SharedChatView (WS 断开时) | `SharedChatView.tsx:246` | 3s | 聊天激活时 |

**影响估算**：用户切到其他标签页后，每秒仍有 ~1 个 HTTP 请求（AskUserNotifications + 当前页面轮询叠加），CPU 无法降频，网络持续活跃。

**修复方案**：

1. 提取公共 hook `useVisibilityAwareInterval(callback, intervalMs)`：
   - 监听 `document.visibilitychange`
   - `hidden` 时清除 interval
   - `visible` 时立即执行一次回调 + 重建 interval
   - 返回清理函数供 `useEffect` 使用

2. 将上述所有 `setInterval` 替换为该 hook

```typescript
// frontend/src/hooks/useVisibilityAwareInterval.ts
import { useEffect, useRef } from 'react';

export function useVisibilityAwareInterval(
  callback: () => void,
  intervalMs: number,
) {
  const savedCallback = useRef(callback);
  savedCallback.current = callback;

  useEffect(() => {
    let timer: ReturnType<typeof setInterval> | null = null;

    const start = () => {
      savedCallback.current();
      timer = setInterval(() => savedCallback.current(), intervalMs);
    };

    const stop = () => {
      if (timer) { clearInterval(timer); timer = null; }
    };

    const onVisibility = () => {
      if (document.visibilityState === 'visible') start();
      else stop();
    };

    if (document.visibilityState === 'visible') start();
    document.addEventListener('visibilitychange', onVisibility);

    return () => {
      stop();
      document.removeEventListener('visibilitychange', onVisibility);
    };
  }, [intervalMs]);
}
```

**TODO**：
- [ ] 创建 `frontend/src/hooks/useVisibilityAwareInterval.ts`
- [ ] 替换 `AskUserNotifications.tsx:46-52` 的 `setInterval`
- [ ] 替换 `AppShell.tsx:99-107` 的 `setInterval`
- [ ] 替换 `Dashboard.tsx:24-28` 的 `setInterval`
- [ ] 替换 `TasksPage.tsx:200-204` 的 `setInterval`
- [ ] 替换 `ProjectsPage.tsx:738-742` 的 `setInterval`
- [ ] 替换 `WorkersPage.tsx:1100-1105` 的 `setInterval`
- [ ] 替换 `SharedChatView.tsx:244-253` 的 `setInterval`

---

### P0：WebSocket 连接不共享

**问题**：`useWebSocket.ts:44` — 每次调用 hook 都创建独立的 `WsClient` 实例和 WebSocket TCP 连接。当前代码有 12 处调用 `useWebSocket()`。

典型场景同时活跃的连接：

```
AppShell           → ['workers']              ← 独立连接
AskUserNotifications → ['tasks']              ← 独立连接
TasksPage          → ['system', 'tasks']      ← 独立连接
UpdateButton       → ['system_update']        ← 独立连接
ChatView           → ['task:N', 'system', 'tasks'] ← 独立连接
```

**影响**：5-6 个独立 TCP 连接 + 重复订阅（`tasks` 被订阅 3 次，`system` 被订阅 2 次）。每个连接有心跳、重连逻辑、内存开销。

**修复方案**：

1. 将 `WsClient` 改为应用级单例（或 React Context 提供）
2. `useWebSocket` hook 不再创建新连接，而是向单例注册 channel + handler
3. 单例内部只维护一个 WebSocket 连接，动态合并所有组件的 channel 订阅
4. 组件卸载时取消注册，单例自动取消不再有人订阅的 channel

```typescript
// 概念设计：全局 WsClient 单例
const globalClient = new WsClient(getWsUrl());
globalClient.connect();

function useWebSocket(channels, onMessage, onReconnect, onSubscribed) {
  useEffect(() => {
    // 注册 channels + handlers 到单例
    globalClient.addSubscription(id, channels, handlers);
    return () => globalClient.removeSubscription(id);
  }, [channelsKey]);
}
```

**TODO**：
- [ ] 重构 `WsClient` 支持多订阅者注册/注销
- [ ] 创建 `WsClientProvider` 或模块级单例（考虑 token 变化时重建）
- [ ] 重构 `useWebSocket` hook 使用单例而非每次 `new WsClient()`
- [ ] 确保 channel 去重：相同 channel 只发一次 subscribe
- [ ] 确保 reconnect/subscribed 回调正确分发给所有注册者

---

### P1：`useWebSocket` 的 `setLastMessage` 冗余渲染

**问题**：`useWebSocket.ts:50` — 每条 WS 消息都会调用 `setLastMessage(parsed)`，即使调用方已通过 `onMessage` 回调完整处理了消息。

```typescript
client.onMessage((msg) => {
  callbackRef.current?.(parsed);  // ← 回调已处理
  setLastMessage(parsed);          // ← 仍触发父组件重渲染
});
```

**影响**：Chat 流式输出时每秒可能 10+ 条 WS 消息，每条都触发一次额外的组件树重渲染。所有使用 `useWebSocket` 的组件都受影响。

**修复方案**：

只在有组件依赖 `lastMessage` 返回值时才 `setLastMessage`。实践中当前所有调用者都使用 `onMessage` 回调模式，`lastMessage` 返回值无人使用。

方案 A（保守）：新增参数 `skipLastMessage?: boolean`，传 true 时跳过 `setLastMessage`
方案 B（激进）：完全移除 `lastMessage` state 和 `setLastMessage` 调用

推荐方案 B——hook 注释已经明确说明 `lastMessage` 模式会丢消息，所有调用方都使用 callback。

**TODO**：
- [ ] 确认所有 `useWebSocket` 调用方均使用 `onMessage` 回调（已确认）
- [ ] 从 `useWebSocket.ts` 移除 `lastMessage` state 和 `setLastMessage` 调用
- [ ] 移除返回值中的 `lastMessage`
- [ ] 更新所有引用 `lastMessage` 的地方（当前仅 `InstanceLog.tsx` 解构但未使用 `lastMessage`）

---

### P1：轮询与 WebSocket 双重消耗

**问题**：多个组件同时使用 WS 推送 **和** HTTP 轮询，WS 正常连通时轮询完全冗余。

| 组件 | WS 频道 | 轮询频率 |
|------|---------|----------|
| TasksPage | `['system', 'tasks']` | 5s |
| WorkersPage | `['workers']` | 30s |
| AskUserNotifications | `['tasks']` | 5s |

**修复方案**：

WS 连通时降低轮询频率或完全暂停：

```typescript
// 方案：WS 连通时用长轮询兜底，断开时用短轮询补偿
const { isConnected } = useWebSocket(channels, onMessage, onReconnect);
const interval = isConnected ? 60_000 : 5_000; // 连通 60s / 断开 5s
useVisibilityAwareInterval(refresh, interval);
```

注意：不能完全移除轮询——WS 可能丢消息，且 member 用户无法订阅全局频道（`AskUserNotifications.tsx:49` 注释说明）。

**TODO**：
- [ ] `TasksPage` — WS 连通时轮询降至 60s
- [ ] `WorkersPage` — WS 连通时轮询降至 120s
- [ ] `AskUserNotifications` — WS 连通时轮询降至 30s（member 兜底仍需轮询）
- [ ] `Dashboard` — 增加 WS 订阅 `['tasks']`，连通时轮询降至 30s

---

### P1：DiscussionView 的 `useIdleTime` 强制重渲染

**问题**：`DiscussionView.tsx:76-82` — 纯粹为了更新"空闲时间"显示，每 5 秒用 `setTick(t => t+1)` 强制重渲染整个 DiscussionView 组件树。

```typescript
function useIdleTime(events, isRunning) {
  const [, setTick] = useState(0);
  useEffect(() => {
    if (isRunning || events.length === 0) return;
    const interval = setInterval(() => setTick((t) => t + 1), 5000);
    return () => clearInterval(interval);
  }, [isRunning, events.length]);
  // ...计算空闲时间
}
```

**修复方案**：

将 `useIdleTime` 改为只渲染空闲时间文本的独立小组件，用 `memo` 隔离重渲染范围：

```typescript
const IdleTimeBadge = memo(function IdleTimeBadge({
  lastTimestamp, isRunning
}: { lastTimestamp: string | null; isRunning: boolean }) {
  const [, setTick] = useState(0);
  useEffect(() => {
    if (isRunning || !lastTimestamp) return;
    const interval = setInterval(() => setTick(t => t + 1), 5000);
    return () => clearInterval(interval);
  }, [isRunning, lastTimestamp]);
  // ...只渲染时间文本
});
```

这样每 5 秒只重渲染一个小 badge 组件，而非整棵 DiscussionView 树。

**TODO**：
- [ ] 将 `useIdleTime` hook 重构为 `IdleTimeBadge` 独立 memo 组件
- [ ] 在 DiscussionView 中只渲染 `<IdleTimeBadge />`

---

### P2：PoolDrawer 轮询泄漏

**问题**：`PoolDrawer.tsx` 中 Claude AddAccountModal（`:1057-1064`）和 Claude relogin（`:1471-1480`）在事件处理函数中使用递归 `setTimeout(poll, 5000)`，没有清理机制。

```typescript
// AddAccountModal handleSubmit 内部
const poll = async () => {
  const s = await api.poolAddStatus(email.trim());
  if (s.status === 'running') { setTimeout(poll, 5000); return; }
  // ...
};
setTimeout(poll, 5000);  // ← 组件卸载后仍继续
```

对比 `AddCodexAccountModal`（`:1142-1178`）正确使用了 `useEffect` + `cancelled` 标志。

**影响**：用户打开添加账号模态框 → 点添加 → 关闭模态框 → 轮询在后台继续运行，请求无意义的 API 并尝试更新已卸载组件的 state。

**修复方案**：

参考 `AddCodexAccountModal` 的模式，将轮询移入 `useEffect` 并使用 `cancelled` 标志：

```typescript
const [polling, setPolling] = useState(false);
useEffect(() => {
  if (!polling) return;
  let cancelled = false;
  const poll = async () => {
    const s = await api.poolAddStatus(email.trim());
    if (cancelled) return;
    if (s.status === 'running') {
      setTimeout(poll, 5000);
      return;
    }
    // handle terminal states
  };
  setTimeout(poll, 5000);
  return () => { cancelled = true; };
}, [polling, email]);
```

**TODO**：
- [ ] 重构 `AddAccountModal`（`:1057-1064`）的轮询为 `useEffect` + cancelled 模式
- [ ] 重构 `handleClaudeRelogin`（`:1471-1480`）的轮询为 `useEffect` + cancelled 模式

---

### P2：ChatView 每次键入写 localStorage（无 debounce）

**问题**：`ChatView.tsx:400-405` — 每次 `input` 状态变化都同步写入 localStorage。快速打字时（10 字/秒），每个字符触发一次同步 I/O。

```typescript
useEffect(() => {
  if (input) localStorage.setItem(`ccm-chat-draft-${task.id}`, input);
  else localStorage.removeItem(`ccm-chat-draft-${task.id}`);
}, [input, task.id]);
```

**修复方案**：

加 debounce（500ms）：

```typescript
useEffect(() => {
  const timer = setTimeout(() => {
    try {
      if (input) localStorage.setItem(`ccm-chat-draft-${task.id}`, input);
      else localStorage.removeItem(`ccm-chat-draft-${task.id}`);
    } catch {}
  }, 500);
  return () => clearTimeout(timer);
}, [input, task.id]);
```

同样处理 `LoopChatView` 如有类似模式。

**TODO**：
- [ ] `ChatView.tsx:400-405` 的 localStorage 写入加 500ms debounce
- [ ] `ChatView.tsx:406-417` 的 uploadedResults localStorage 写入加 500ms debounce

---

### P3：`animate-pulse` CSS 动画 GPU 开销

**问题**：大量使用 Tailwind `animate-pulse`（opacity 无限循环动画），当有多个活跃任务时，列表页上 10+ 个 DOM 元素同时做动画。

受影响位置：
- `TaskList.tsx:26-27` — 每个 `executing`/`background` 状态任务行的状态点
- `TasksPage.tsx:580-581` — 拖拽指示器状态点
- `InstanceGrid.tsx:14` — 运行中实例状态点
- `SubAgentIndicator.tsx:38` — 活跃子 Agent 指示器
- `TaskBadges.tsx:177` — 子 Agent 徽章
- `DiscussionView.tsx:586` — 运行中 Agent 状态点
- `VoiceButton.tsx:72` — 录音中按钮
- `LoopChatView.tsx:359` — "Claude is working..." 文字

**影响**：每个 `animate-pulse` 元素创建独立的 GPU 合成层，浏览器每帧都需要处理所有动画。10 个以上时对 GPU 和电池有可感知影响。

**修复方案**：

对于小状态点（如 `TaskList` 的 3px 圆点），`animate-pulse` 的 GPU 开销不成比例。改为不使用动画，只用静态的区分色表示状态差异：

- 方案 A：小元素去掉 `animate-pulse`，改用静态亮色 + 更大尺寸区分状态
- 方案 B：保留 `animate-pulse` 但只对**当前可视区域内**的元素启用（IntersectionObserver）
- 方案 C：将 `animate-pulse` 替换为更轻量的 CSS 方案（如用 `box-shadow` 静态发光效果）

推荐方案 A（最简单有效）——小状态点的脉冲动画几乎看不出来，用静态样式已经足够区分。

**TODO**：
- [ ] 评估去掉状态点的 `animate-pulse` 后视觉效果是否可接受
- [ ] 如可接受，移除 `TaskList.tsx`、`TasksPage.tsx`、`InstanceGrid.tsx` 中小元素的 `animate-pulse`
- [ ] 保留有意义的动画（如 `animate-spin` 加载指示器、`VoiceButton` 录音状态）

---

## 修复优先级总览

| 优先级 | 问题 | 预期节电效果 | 改动范围 |
|--------|------|-------------|----------|
| **P0** | HTTP 轮询无 visibility 暂停 | **大** — 标签页后台时 CPU/网络降至零 | 新 hook + 7 处替换 |
| **P0** | WebSocket 连接不共享 | **大** — 5-6 个 TCP 连接降到 1 个 | WsClient 重构 |
| **P1** | `setLastMessage` 冗余渲染 | **中** — 流式推送时减少 50% 渲染 | useWebSocket 改 1 行 |
| **P1** | 轮询与 WS 双重消耗 | **中** — WS 连通时每分钟省 10+ 请求 | 4 处组件改动 |
| **P1** | useIdleTime 强制重渲染 | **小** — 避免整棵 DiscussionView 树重渲染 | 1 处重构 |
| **P2** | PoolDrawer 轮询泄漏 | **小** — 防止组件卸载后继续轮询 | 2 处修复 |
| **P2** | localStorage 无 debounce | **小** — 减少同步 I/O | 2 处修复 |
| **P3** | animate-pulse GPU 开销 | **微** — 减少 GPU 合成层 | 评估后决定 |

---

## 自动化测试设计

### 1. `useVisibilityAwareInterval` 单元测试

文件：`frontend/src/hooks/useVisibilityAwareInterval.test.ts`

```typescript
describe('useVisibilityAwareInterval', () => {
  it('calls callback immediately on mount when visible', () => {
    // 初始 visibilityState = 'visible'
    // 验证 callback 被立即调用一次
  });

  it('calls callback at specified interval', () => {
    // vi.useFakeTimers()
    // advanceTimersByTime(intervalMs * 3)
    // 验证 callback 被调用 4 次 (1 初始 + 3 间隔)
  });

  it('stops interval when tab becomes hidden', () => {
    // 触发 visibilitychange → hidden
    // advanceTimersByTime(intervalMs * 5)
    // 验证 callback 不再被调用
  });

  it('resumes with immediate callback when tab becomes visible again', () => {
    // hidden → visible
    // 验证 callback 立即调用一次
    // 验证新 interval 重新开始
  });

  it('cleans up on unmount', () => {
    // unmount hook
    // 验证 interval 被清除
    // 验证 visibilitychange listener 被移除
  });

  it('does not start interval when mounted while hidden', () => {
    // 初始 visibilityState = 'hidden'
    // 验证 callback 不被调用
  });
});
```

### 2. WebSocket 单例共享测试

文件：扩展 `frontend/src/api/ws.test.ts`

```typescript
describe('WsClient singleton', () => {
  it('reuses a single WebSocket connection across multiple subscriptions', () => {
    // 两次 useWebSocket 注册不同 channels
    // 验证只创建了 1 个 WebSocket 实例
    // 验证 subscribe 消息包含所有 channels 去重后的集合
  });

  it('unsubscribes channels when last subscriber unmounts', () => {
    // 两个组件订阅 ['tasks']，卸载一个
    // 验证 ['tasks'] 仍被订阅
    // 卸载第二个
    // 验证发送 unsubscribe 消息
  });

  it('dispatches messages to correct subscribers', () => {
    // 订阅者 A 订阅 ['tasks']，订阅者 B 订阅 ['workers']
    // 发送 channel='tasks' 的消息
    // 验证只有 A 的 onMessage 被调用
  });

  it('broadcasts reconnect event to all subscribers', () => {
    // WS 断连后重连
    // 验证所有订阅者的 onReconnect 被调用
  });
});
```

### 3. `setLastMessage` 移除的回归测试

文件：扩展 `frontend/src/hooks/useWebSocket.test.tsx`

```typescript
describe('useWebSocket without lastMessage', () => {
  it('only invokes onMessage callback, no state update per message', () => {
    // 发送 WS 消息
    // 验证 onMessage 被调用
    // 验证组件没有因 lastMessage 而重渲染
  });

  it('returns isConnected but not lastMessage', () => {
    // 验证 hook 返回 { isConnected } 而非 { lastMessage, isConnected }
  });
});
```

### 4. 轮询频率与 WS 联动测试

文件：各组件测试中增加用例

```typescript
describe('TasksPage polling adapts to WS connection', () => {
  it('polls every 5s when WS is disconnected', () => {
    // mock WS 为断开状态
    // 验证 setInterval 间隔为 5000ms
  });

  it('polls every 60s when WS is connected', () => {
    // mock WS 为连通状态
    // 验证 setInterval 间隔为 60000ms
  });

  it('switches to fast polling when WS disconnects', () => {
    // WS 连通 → 断开
    // 验证 interval 从 60s 切到 5s
  });
});
```

### 5. PoolDrawer 轮询泄漏测试

文件：扩展 `frontend/src/components/Layout/PoolDrawer.test.tsx`

```typescript
describe('AddAccountModal poll cleanup', () => {
  it('stops polling when modal is unmounted', () => {
    // 渲染 modal → 提交表单 → 触发 poll
    // 卸载 modal
    // advanceTimersByTime(15000)
    // 验证 api.poolAddStatus 不再被调用
  });

  it('stops polling on successful login', () => {
    // mock api.poolAddStatus 第二次返回 success
    // 验证 poll 自行停止
  });
});

describe('Claude relogin poll cleanup', () => {
  it('stops polling when PoolDrawer is unmounted', () => {
    // 同上模式
  });
});
```

### 6. localStorage debounce 测试

文件：扩展 `frontend/src/components/Chat/ChatView.test.tsx`

```typescript
describe('draft persistence debounce', () => {
  it('does not write to localStorage on every keystroke', () => {
    // 快速输入 5 个字符
    // advanceTimersByTime(100)
    // 验证 localStorage.setItem 没有被调用 5 次
  });

  it('writes to localStorage after 500ms debounce', () => {
    // 输入 "hello"
    // advanceTimersByTime(500)
    // 验证 localStorage.setItem 被调用 1 次，值为 "hello"
  });

  it('resets debounce on new input', () => {
    // 输入 "hel" → 300ms 后输入 "lo"
    // advanceTimersByTime(500)
    // 验证最终写入 "hello" 而非中间值
  });
});
```

### 7. DiscussionView IdleTimeBadge 隔离测试

```typescript
describe('IdleTimeBadge render isolation', () => {
  it('does not cause parent DiscussionView to re-render on tick', () => {
    // 渲染 DiscussionView with spy on render count
    // advanceTimersByTime(15000)
    // 验证 DiscussionView 渲染次数不增加
    // 验证 IdleTimeBadge 渲染了 3 次
  });
});
```

---

## 实施顺序建议

1. **第一批（P0，效果最大）**：
   - 创建 `useVisibilityAwareInterval` hook + 测试
   - 替换所有 `setInterval` 为新 hook
   - 移除 `useWebSocket` 的 `setLastMessage`

2. **第二批（P0-P1，架构改动）**：
   - WebSocket 单例重构
   - 轮询频率与 WS 状态联动

3. **第三批（P1-P2，小修复）**：
   - DiscussionView `useIdleTime` 重构
   - PoolDrawer 轮询泄漏修复
   - ChatView localStorage debounce

4. **第四批（P3，可选）**：
   - 评估 `animate-pulse` 替代方案
