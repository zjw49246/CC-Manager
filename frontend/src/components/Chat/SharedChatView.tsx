import { useState, useEffect, useRef, useCallback } from 'react';
import { api } from '../../api/client';
import type { SharedTaskReceived } from '../../api/client';
import { ArrowLeft, Send, RefreshCw, Wifi, WifiOff, Loader2 } from '../icons';
import { MarkdownRenderer } from '../Markdown/MarkdownRenderer';
import { useVisibilityAwareInterval } from '../../hooks/useVisibilityAwareInterval';

interface SharedChatViewProps {
  shared: SharedTaskReceived;
  onBack: () => void;
}

interface ChatMsg {
  id: number;
  role: string;
  event_type: string;
  content: string | null;
  tool_name?: string;
  tool_input?: string;
  tool_output?: string;
  is_error?: boolean;
  timestamp?: string;
  raw_content?: string;
  optimistic?: boolean;
  persisted?: boolean;
}

function sharedMessageFingerprint(message: ChatMsg): string {
  return JSON.stringify([
    message.event_type,
    message.role,
    message.raw_content ?? message.content,
    message.tool_name ?? null,
    message.tool_input ?? null,
    message.tool_output ?? null,
  ]);
}

function consumeFingerprint(counts: Map<string, number>, key: string): boolean {
  const count = counts.get(key) || 0;
  if (count <= 0) return false;
  if (count === 1) counts.delete(key);
  else counts.set(key, count - 1);
  return true;
}

/**
 * Shared history is fetched through a remote relay and can be older than WS
 * events already rendered locally. Reconcile matching occurrences while
 * preserving unconfirmed optimistic bubbles and newer persisted WS rows.
 */
export function mergeSharedHistory(
  history: ChatMsg[],
  current: ChatMsg[],
  confirmedOptimisticIds: ReadonlySet<number> = new Set(),
): ChatMsg[] {
  const snapshot = history.map((message) => ({ ...message, persisted: true }));
  const snapshotIds = new Set(snapshot.map((message) => message.id));
  const snapshotCounts = new Map<string, number>();
  for (const message of snapshot) {
    const key = sharedMessageFingerprint(message);
    snapshotCounts.set(key, (snapshotCounts.get(key) || 0) + 1);
  }

  const extras: ChatMsg[] = [];
  for (const message of current) {
    const key = sharedMessageFingerprint(message);
    if (message.persisted && snapshotIds.has(message.id)) {
      consumeFingerprint(snapshotCounts, key);
      continue;
    }
    if (
      !message.persisted
      && (!message.optimistic || confirmedOptimisticIds.has(message.id))
      && consumeFingerprint(snapshotCounts, key)
    ) {
      continue;
    }
    extras.push(message);
  }

  const persistedById = new Map<number, ChatMsg>();
  for (const message of extras) {
    if (message.persisted) persistedById.set(message.id, message);
  }
  for (const message of snapshot) persistedById.set(message.id, message);

  return [
    ...[...persistedById.values()].sort((a, b) => a.id - b.id),
    ...extras.filter((message) => !message.persisted),
  ];
}

export function SharedChatView({ shared, onBack }: SharedChatViewProps) {
  const [messages, setMessages] = useState<ChatMsg[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [input, setInput] = useState('');
  const [sending, setSending] = useState(false);
  const [wsConnected, setWsConnected] = useState(false);
  const [config, setConfig] = useState<any>(null);
  const bottomRef = useRef<HTMLDivElement>(null);
  const wsRef = useRef<WebSocket | null>(null);
  const nextOptimisticIdRef = useRef(-1);
  const inFlightOptimisticRef = useRef(new Map<number, string>());
  const confirmedOptimisticRef = useRef(new Set<number>());

  const scrollToBottom = useCallback(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, []);

  // Load history and config
  useEffect(() => {
    let active = true;
    (async () => {
      try {
        const [history, cfg] = await Promise.all([
          api.getSharedHistory(shared.id),
          api.getSharedConfig(shared.id),
        ]);
        if (!active) return;
        setMessages((current) => mergeSharedHistory(history, current));
        setConfig(cfg);
        setError(null);
      } catch (e) {
        if (active) setError(String(e));
      } finally {
        if (active) setLoading(false);
      }
    })();
    return () => { active = false; };
  }, [shared.id]);

  // Connect WebSocket directly to the sharer's CCM
  useEffect(() => {
    if (!shared.owner_ccm_url) return;

    const wsUrl = shared.owner_ccm_url
      .replace(/^http/, 'ws')
      + `/ws/shared?token=${encodeURIComponent(shared.share_token)}&task_id=${shared.remote_task_id}`;

    const ws = new WebSocket(wsUrl);
    wsRef.current = ws;

    ws.onopen = () => setWsConnected(true);
    ws.onclose = () => setWsConnected(false);
    ws.onerror = () => setWsConnected(false);

    ws.onmessage = (ev) => {
      try {
        const raw = JSON.parse(ev.data);
        if (raw.action === 'subscribed') return;
        const data = raw.data || raw;
        const eventType = data.event_type || data.event;
        if (!eventType) return;

        if (eventType === 'user_message') {
          setSending(true);
          const rawContent = typeof data.raw_content === 'string'
            ? data.raw_content
            : String(data.content || '');
          const optimisticId = [...inFlightOptimisticRef.current.entries()]
            .find(([, content]) => content === rawContent)?.[0];
          if (optimisticId !== undefined) {
            confirmedOptimisticRef.current.add(optimisticId);
          }
          const persistedId = Number(data.id);
          const isPersisted = Number.isFinite(persistedId) && persistedId > 0;
          setMessages((prev) => {
            const optimisticIndex = prev.findIndex((message) =>
              message.role === 'user'
              && message.event_type === 'user_message'
              && message.optimistic
              && (message.raw_content ?? message.content) === rawContent
            );
            if (optimisticIndex >= 0) {
              const next = [...prev];
              next[optimisticIndex] = {
                ...next[optimisticIndex],
                id: isPersisted ? persistedId : next[optimisticIndex].id,
                content: data.content || null,
                raw_content: rawContent,
                optimistic: false,
                persisted: isPersisted,
                timestamp: typeof data.timestamp === 'string'
                  ? data.timestamp
                  : next[optimisticIndex].timestamp,
              };
              return next;
            }
            return [...prev, {
              id: isPersisted ? persistedId : Date.now(),
              role: 'user',
              event_type: 'user_message',
              content: data.content || null,
              raw_content: rawContent,
              persisted: isPersisted,
              timestamp: typeof data.timestamp === 'string' ? data.timestamp : undefined,
            }];
          });
          return;
        }

        if (eventType === 'process_exit') {
          setTimeout(() => setSending(false), 500);
          return;
        }

        if (eventType === 'status_change') return;
        if (['thinking', 'system_init'].includes(eventType)) return;
        const skipSystem = ['task_progress', 'thinking_tokens', 'token_usage', 'api_request', 'api_response'];
        if (eventType === 'system_event' && skipSystem.includes(data.content)) return;

        if (['message', 'result'].includes(eventType) && data.role === 'assistant') {
          setSending(false);
        }

        if (data.content || data.tool_name) {
          const persistedId = Number(data.id);
          const isPersisted = Number.isFinite(persistedId) && persistedId > 0;
          setMessages(prev => [...prev, {
            id: isPersisted ? persistedId : Date.now() + Math.random(),
            role: data.role || 'assistant',
            event_type: eventType,
            content: data.content || null,
            tool_name: data.tool_name,
            tool_input: data.tool_input,
            tool_output: data.tool_output,
            is_error: data.is_error,
            persisted: isPersisted,
            timestamp: typeof data.timestamp === 'string' ? data.timestamp : undefined,
          }]);
        }
      } catch { /* ignore */ }
    };

    return () => {
      ws.close();
      wsRef.current = null;
    };
  }, [shared.owner_ccm_url, shared.remote_task_id, shared.share_token]);

  // Fallback polling when WS is not connected
  useVisibilityAwareInterval(async () => {
      try {
        const history = await api.getSharedHistory(shared.id);
        setMessages((current) => mergeSharedHistory(history, current));
      } catch { /* ignore */ }
  }, 3000, !wsConnected, false);

  useEffect(() => {
    scrollToBottom();
  }, [messages, scrollToBottom]);

  const handleSend = async () => {
    const text = input.trim();
    if (!text || sending) return;

    setSending(true);
    setInput('');
    const optimisticId = nextOptimisticIdRef.current--;
    inFlightOptimisticRef.current.set(optimisticId, text);

    // Optimistic local message
    setMessages(prev => [...prev, {
      id: optimisticId,
      role: 'user',
      event_type: 'user_message',
      content: text,
      raw_content: text,
      optimistic: true,
    }]);

    try {
      await api.sendSharedChat(shared.id, text);
      // This request starts only after the owner has committed the message.
      // It may therefore reconcile this optimistic bubble. Older in-flight
      // history requests are deliberately not allowed to do so.
      void api.getSharedHistory(shared.id).then((history) => {
        setMessages((current) => mergeSharedHistory(
          history,
          current,
          new Set([optimisticId]),
        ));
      }).catch(() => {
        // The WS echo or a later page load can still confirm the message.
      });
    } catch (e) {
      setError(String(e));
      setSending(false);
      if (!confirmedOptimisticRef.current.has(optimisticId)) {
        setMessages((prev) => prev.filter((message) => message.id !== optimisticId));
        setInput(text);
      }
    } finally {
      inFlightOptimisticRef.current.delete(optimisticId);
      confirmedOptimisticRef.current.delete(optimisticId);
    }
  };

  const refresh = async () => {
    setLoading(true);
    try {
      const history = await api.getSharedHistory(shared.id);
      setMessages((current) => mergeSharedHistory(history, current));
      setError(null);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  };

  const status = config?.status || 'unknown';
  const statusColor = status === 'executing' ? 'text-blue-400' : status === 'completed' ? 'text-green-400' : 'text-gray-400';

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="flex items-center gap-3 px-4 py-3 border-b border-gray-700 bg-gray-800/50">
        <button onClick={onBack} className="text-gray-400 hover:text-gray-200">
          <ArrowLeft size={20} />
        </button>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <h2 className="text-foreground font-medium truncate">
              {shared.task_title || `Task #${shared.remote_task_id}`}
            </h2>
            <span className={`text-xs ${statusColor}`}>{status}</span>
          </div>
          <p className="text-xs text-gray-500 truncate">
            Shared by {shared.owner_name || 'Unknown'}
            {shared.project_name && ` · ${shared.project_name}`}
          </p>
        </div>
        <div className="flex items-center gap-2">
          {wsConnected ? (
            <Wifi size={14} className="text-green-400" />
          ) : (
            <WifiOff size={14} className="text-gray-500" />
          )}
          <button onClick={refresh} disabled={loading} className="text-gray-400 hover:text-gray-200">
            <RefreshCw size={16} className={loading ? 'animate-spin' : ''} />
          </button>
        </div>
      </div>

      {error && <p className="px-4 py-2 text-red-400 text-sm bg-red-900/20">{error}</p>}

      {/* Messages */}
      <div className="flex-1 overflow-y-auto px-4 py-3 space-y-3">
        {loading && messages.length === 0 && (
          <p className="text-gray-500 text-sm text-center py-8">Loading history...</p>
        )}
        {messages.map((msg) => (
          <MessageRow key={msg.id} msg={msg} />
        ))}
        {sending && (
          <div className="flex gap-2 items-center text-gray-500 text-sm px-3">
            <Loader2 size={14} className="animate-spin" />
            <span>Claude is thinking...</span>
          </div>
        )}
        <div ref={bottomRef} />
      </div>

      {/* Input */}
      <div className="px-4 py-3 border-t border-gray-700 bg-gray-800/50">
        <div className="flex gap-2">
          <input
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && !e.shiftKey && handleSend()}
            placeholder="Send a message..."
            className="flex-1 bg-gray-700 text-foreground rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-1 focus:ring-blue-500"
            disabled={sending}
          />
          <button
            onClick={handleSend}
            disabled={sending || !input.trim()}
            className="px-3 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-500 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            <Send size={16} />
          </button>
        </div>
      </div>
    </div>
  );
}

function MessageRow({ msg }: { msg: ChatMsg }) {
  if (msg.event_type === 'user_message') {
    return (
      <div className="flex justify-end">
        <div className="max-w-[80%] bg-blue-600/20 border border-blue-500/20 rounded-xl px-4 py-2">
          <p className="text-sm text-foreground whitespace-pre-wrap">{msg.content}</p>
        </div>
      </div>
    );
  }

  if (msg.event_type === 'tool_use' || msg.event_type === 'tool_result') {
    return (
      <div className="text-xs text-gray-500 px-2 py-1 bg-gray-800/50 rounded font-mono">
        {msg.event_type === 'tool_use' ? `🔧 ${msg.tool_name || 'tool'}` : '📋 result'}
        {msg.tool_output && <span className="ml-2 text-gray-600">{msg.tool_output.slice(0, 100)}</span>}
      </div>
    );
  }

  if (msg.event_type === 'system_event' || msg.event_type === 'system_init') {
    return (
      <div className="text-xs text-gray-600 text-center py-1">
        {msg.content}
      </div>
    );
  }

  // Assistant message
  if (msg.role === 'assistant' && msg.content) {
    return (
      <div className="max-w-[90%]">
        <div className="bg-gray-800 border border-gray-700 rounded-xl px-4 py-2">
          <div className="markdown-body text-sm">
            <MarkdownRenderer content={msg.content} />
          </div>
        </div>
      </div>
    );
  }

  if (msg.content) {
    return (
      <div className="text-sm text-gray-300 px-2 py-1">
        {msg.content}
      </div>
    );
  }

  return null;
}
