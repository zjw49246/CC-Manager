import { useCallback, useEffect, useRef, useState } from 'react';
import { HelpCircle, X } from './icons';
import { api } from '../api/client';
import { useWebSocket } from '../hooks/useWebSocket';
import { useVisibilityAwareInterval } from '../hooks/useVisibilityAwareInterval';

interface PendingAsk {
  task_id: number;
  request_id: string;
  summary: string;
}

/**
 * 全局 ask_user 通知。
 *
 * 内置 AskUserQuestion 被 hook 拦截后会广播两路事件：
 *  - `task:{id}` 频道 → ChatView 渲染内联卡片（仅当用户正看着该 task 时可见）；
 *  - 全局 `tasks` 频道（本组件）→ 无论用户在哪个页面都弹出可点击的通知。
 *
 * 修复：以前只走 task 频道，用户不在对应 task 页面时提问就「消失」了。
 * 现在挂在 App 顶层常驻，订阅 `tasks` 频道 + 刷新/重连时拉 /api/ask-user/pending
 * 回填，点击通知跳转到对应 task 聊天页。
 */
export function AskUserNotifications() {
  const [pending, setPending] = useState<PendingAsk[]>([]);
  const [tasksChannelAccepted, setTasksChannelAccepted] = useState(false);
  const stateVersionRef = useRef(0);
  const refreshGenerationRef = useRef(0);
  const dismissedRef = useRef(new Set<string>());

  const refresh = useCallback(() => {
    const generation = ++refreshGenerationRef.current;
    const stateVersion = stateVersionRef.current;
    api.getAskUserPendingAll()
      .then(({ pending }) => {
        // Do not let an older HTTP snapshot erase a WS event received while
        // the request was in flight.
        if (
          generation === refreshGenerationRef.current
          && stateVersion === stateVersionRef.current
        ) {
          setPending(pending.filter((entry) => !dismissedRef.current.has(entry.request_id)));
        }
      })
      .catch(() => { /* 后端不可达时静默，WS 事件会补 */ });
  }, []);

  const handleWs = useCallback((raw: Record<string, unknown>) => {
    const msg = raw as { channel?: string; data?: Record<string, unknown> };
    if (msg.channel !== 'tasks') return;
    const data = msg.data || {};
    if (data.event === 'ask_user_pending') {
      stateVersionRef.current += 1;
      const entry: PendingAsk = {
        task_id: Number(data.task_id),
        request_id: String(data.request_id),
        summary: String(data.summary || 'A task is asking for your input'),
      };
      if (dismissedRef.current.has(entry.request_id)) return;
      setPending((prev) =>
        prev.some((p) => p.request_id === entry.request_id) ? prev : [...prev, entry]
      );
    } else if (data.event === 'ask_user_resolved') {
      const rid = String(data.request_id);
      stateVersionRef.current += 1;
      dismissedRef.current.delete(rid);
      setPending((prev) => prev.filter((p) => p.request_id !== rid));
    }
  }, []);

  useEffect(() => { refresh(); }, [refresh]);

  // Members cannot subscribe to global channels. This ACL-safe HTTP fallback
  // keeps the App-level notification working for them on other pages.
  const { isConnected: tasksWsConnected } = useWebSocket(
    ['tasks'],
    handleWs,
    () => {
      setTasksChannelAccepted(false);
      refresh();
    },
    (accepted) => setTasksChannelAccepted(accepted.includes('tasks')),
  );
  // Members cannot receive the global tasks channel, so retain the short ACL
  // safe fallback even when the transport itself is connected.
  useVisibilityAwareInterval(
    refresh,
    tasksChannelAccepted && tasksWsConnected ? 30000 : 5000,
    true,
    false,
  );

  const open = useCallback((taskId: number, requestId: string) => {
    // 跳转到该 task 的聊天页；App 监听 hashchange 完成路由
    window.location.hash = `#/tasks/chat/${taskId}`;
    stateVersionRef.current += 1;
    dismissedRef.current.add(requestId);
    setPending((prev) => prev.filter((p) => p.request_id !== requestId));
  }, []);

  const dismiss = useCallback((requestId: string) => {
    stateVersionRef.current += 1;
    dismissedRef.current.add(requestId);
    setPending((prev) => prev.filter((p) => p.request_id !== requestId));
  }, []);

  if (pending.length === 0) return null;

  return (
    <div className="fixed bottom-4 right-4 z-50 flex flex-col gap-2 max-w-sm">
      {pending.map((p) => (
        <div
          key={p.request_id}
          onClick={() => open(p.task_id, p.request_id)}
          className="group flex items-start gap-3 rounded-lg border border-indigo-500/50 bg-gray-900 shadow-xl px-4 py-3 cursor-pointer hover:bg-gray-800 transition-colors"
          role="button"
        >
          <HelpCircle size={18} className="text-indigo-400 shrink-0 mt-0.5" />
          <div className="min-w-0 flex-1">
            <div className="text-xs font-medium text-indigo-300">
              Task #{p.task_id} needs your input
            </div>
            <div className="text-xs text-gray-400 truncate mt-0.5">{p.summary}</div>
            <div className="text-[10px] text-gray-500 mt-1">Click to answer</div>
          </div>
          <button
            onClick={(e) => { e.stopPropagation(); dismiss(p.request_id); }}
            className="text-gray-600 hover:text-gray-300 shrink-0"
            title="Dismiss"
          >
            <X size={14} />
          </button>
        </div>
      ))}
    </div>
  );
}
