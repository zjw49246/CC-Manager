import { useState, useEffect, useRef, useMemo, useCallback, memo } from 'react';
import type { Components } from 'react-markdown';
import { api } from '../../api/client';
import type { BackgroundLifecycle, ChatMessage, Task } from '../../api/client';
import { useWebSocket } from '../../hooks/useWebSocket';
import { ArrowLeft, ChevronDown, ChevronRight, Copy, Check, XCircle, ArrowDown } from '../icons';
import {
  isLegacyCodexCollabCompleted,
  mergeChatHistory,
} from './messageMerge';
import { TaskArtifactLink } from './TaskArtifactLink';
import { remarkTaskArtifactPaths } from './taskArtifactMarkdown';
import { MarkdownRenderer } from '../Markdown/MarkdownRenderer';

interface LoopChatViewProps {
  task: Task;
  onBack: () => void;
  inline?: boolean;
}

// Iteration metadata received from loop_iteration_end WebSocket events
interface IterationMeta {
  action: 'continue' | 'done' | 'abort';
  reason: string;
  progress: string | null;
}

function messageMatchesTaskTurn(message: ChatMessage, task: Task): boolean {
  return (
    message.task_retry_count === task.retry_count
    && message.task_turn_generation === task.turn_generation
  );
}

function latestBackgroundLifecycle(
  messages: ChatMessage[],
  task: Task,
): BackgroundLifecycle | null {
  let lifecycleIndex = -1;
  let lifecycleMessage: ChatMessage | null = null;
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const message = messages[index];
    if (!messageMatchesTaskTurn(message, task)) continue;
    if (message.background_lifecycle) {
      lifecycleIndex = index;
      lifecycleMessage = message;
      break;
    }
  }
  const lifecycleEntry = lifecycleMessage;
  if (!lifecycleEntry?.background_lifecycle) return null;
  const lifecycle = lifecycleEntry.background_lifecycle;
  // A running lifecycle remains authoritative even if descendant output is
  // interleaved with foreground rows. A completed marker, however, belongs
  // to the iteration that emitted it; a later foreground event or any higher
  // loop iteration starts a new visible foreground epoch on the same Task
  // retry/turn generation.
  if (lifecycle.state === 'running') return lifecycle;
  const lifecycleIteration = lifecycleEntry.loop_iteration;
  const foregroundTypes = new Set([
    'user_message',
    'message',
    'result',
    'tool_use',
    'tool_result',
    'system_init',
    'system_event',
    'thinking',
  ]);
  for (let index = 0; index < messages.length; index += 1) {
    const message = messages[index];
    if (
      !messageMatchesTaskTurn(message, task)
      || !foregroundTypes.has(message.event_type)
    ) continue;
    if (index > lifecycleIndex) return null;
    if (
      lifecycleIteration != null
      && message.loop_iteration != null
      && message.loop_iteration > lifecycleIteration
    ) return null;
  }
  return lifecycle;
}

function groupByIteration(messages: ChatMessage[]): Map<number, ChatMessage[]> {
  const map = new Map<number, ChatMessage[]>();
  for (const msg of messages) {
    if (msg.event_type === 'background_lifecycle') continue;
    const iter = msg.loop_iteration ?? 0;
    if (!map.has(iter)) map.set(iter, []);
    map.get(iter)!.push(msg);
  }
  return map;
}

type MessageGroup =
  | { type: 'tool-group'; messages: ChatMessage[] }
  | { type: 'single'; message: ChatMessage };

function groupMessages(messages: ChatMessage[]): MessageGroup[] {
  const groups: MessageGroup[] = [];
  let toolBuf: ChatMessage[] = [];

  const flush = () => {
    if (toolBuf.length > 0) {
      groups.push({ type: 'tool-group', messages: [...toolBuf] });
      toolBuf = [];
    }
  };

  for (const msg of messages) {
    const isTool = msg.event_type === 'tool_use' || msg.event_type === 'tool_result';
    if (isTool) {
      toolBuf.push(msg);
    } else {
      flush();
      groups.push({ type: 'single', message: msg });
    }
  }
  flush();
  return groups;
}

// ─── Small reusable rendering components ─────────────────────────────────────

function CollapsibleContent({ content, maxLines = 5 }: { content: string; maxLines?: number }) {
  const [expanded, setExpanded] = useState(false);
  const lines = content.split('\n');
  if (lines.length <= maxLines) {
    return <pre className="text-gray-400 whitespace-pre-wrap text-xs overflow-x-auto">{content}</pre>;
  }
  return (
    <div>
      <pre className={`text-gray-400 whitespace-pre-wrap text-xs overflow-x-auto ${expanded ? 'max-h-96 overflow-y-auto' : 'max-h-28 overflow-hidden'}`}>
        {content}
      </pre>
      <button
        onClick={() => setExpanded(!expanded)}
        className="flex items-center gap-1 text-xs text-indigo-400 hover:text-indigo-300 mt-1"
      >
        {expanded ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
        {expanded ? 'Collapse' : `Show all (${lines.length} lines)`}
      </button>
    </div>
  );
}

function formatToolInput(input: string): string {
  try {
    const parsed = JSON.parse(input);
    if (parsed.command) return parsed.command;
    if (parsed.file_path && parsed.old_string !== undefined)
      return `File: ${parsed.file_path}\n--- old ---\n${parsed.old_string}\n+++ new +++\n${parsed.new_string}`;
    if (parsed.file_path && parsed.content !== undefined)
      return `File: ${parsed.file_path}\n${parsed.content}`;
    if (parsed.file_path) return `File: ${parsed.file_path}`;
    if (parsed.pattern) return `Pattern: ${parsed.pattern}${parsed.path ? ` in ${parsed.path}` : ''}`;
    return JSON.stringify(parsed, null, 2);
  } catch {
    return input;
  }
}

function toolUseSummary(msg: ChatMessage): string {
  if (!msg.tool_input) return '';
  if (!msg.tool_input.startsWith('{') && !msg.tool_input.startsWith('[')) {
    return msg.tool_input;
  }
  try {
    const parsed = JSON.parse(msg.tool_input);
    if (parsed.command) {
      const cmd = parsed.command as string;
      return cmd.length > 80 ? cmd.slice(0, 80) + '...' : cmd;
    }
    if (parsed.file_path) return parsed.file_path as string;
    if (parsed.pattern) return `${parsed.pattern}${parsed.path ? ` in ${parsed.path}` : ''}`;
  } catch { /* ignore */ }
  return '';
}

function ToolItem({ message, taskId }: { message: ChatMessage; taskId: number }) {
  const [expanded, setExpanded] = useState(false);
  const [detail, setDetail] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const isToolUse = message.event_type === 'tool_use';
  const toolName = message.tool_name || (isToolUse ? 'tool' : 'result');

  const hasInlineDetail = isToolUse
    ? !!(message.tool_input && (message.tool_input.startsWith('{') || message.tool_input.startsWith('[')))
    : !!(message.tool_output || message.content);

  const getInlineDetail = (): string | null => {
    if (isToolUse && message.tool_input) return formatToolInput(message.tool_input);
    if (!isToolUse && (message.tool_output || message.content)) return message.tool_output || message.content;
    return message.content || null;
  };

  const handleExpand = async () => {
    if (expanded) { setExpanded(false); return; }
    setExpanded(true);
    if (hasInlineDetail) { setDetail(getInlineDetail()); return; }
    if (!detail && !loading) {
      setLoading(true);
      try {
        const d = await api.getMessageDetail(taskId, message.id);
        if (isToolUse && d.tool_input) {
          setDetail(formatToolInput(d.tool_input));
        } else if (!isToolUse && (d.tool_output || d.content)) {
          setDetail(d.tool_output || d.content);
        } else {
          setDetail(d.content || '(empty)');
        }
      } catch {
        setDetail('(failed to load)');
      } finally {
        setLoading(false);
      }
    }
  };

  if (isToolUse) {
    const summary = toolUseSummary(message);
    return (
      <div>
        <button
          onClick={handleExpand}
          className="flex items-center gap-1.5 text-xs text-gray-500 hover:text-gray-400 py-0.5 max-w-full"
        >
          {expanded ? <ChevronDown size={10} className="shrink-0" /> : <ChevronRight size={10} className="shrink-0" />}
          <span className="text-gray-500 font-medium">{toolName}</span>
          {summary && <span className="text-gray-600 truncate">{summary}</span>}
        </button>
        {expanded && (loading
          ? <div className="ml-4 mt-1 mb-1 text-xs text-gray-600">Loading...</div>
          : detail && <div className="ml-4 mt-1 mb-1"><CollapsibleContent content={detail} /></div>
        )}
      </div>
    );
  }

  const statusColor = message.is_error ? 'text-red-400' : 'text-green-600';
  return (
    <div>
      <button
        onClick={handleExpand}
        className="flex items-center gap-1.5 text-xs text-gray-600 hover:text-gray-400 py-0.5"
      >
        {expanded ? <ChevronDown size={10} className="shrink-0" /> : <ChevronRight size={10} className="shrink-0" />}
        <span className={statusColor}>{message.is_error ? '✗' : '✓'}</span>
        <span className="text-gray-600">{toolName}</span>
      </button>
      {expanded && (loading
        ? <div className="ml-4 mt-1 mb-1 text-xs text-gray-600">Loading...</div>
        : detail && <div className="ml-4 mt-1 mb-1"><CollapsibleContent content={detail} /></div>
      )}
    </div>
  );
}

function ToolGroup({ messages, taskId }: { messages: ChatMessage[]; taskId: number }) {
  const [expanded, setExpanded] = useState(false);
  const hasError = messages.some((m) => m.is_error);
  const toolUseCount = messages.filter((m) => m.event_type === 'tool_use').length;
  return (
    <div className="mx-4">
      <button
        onClick={() => setExpanded(!expanded)}
        className={`flex items-center gap-1.5 text-xs py-1 hover:text-gray-400 transition-colors ${hasError ? 'text-red-400/70' : 'text-gray-600'}`}
      >
        {expanded ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
        {hasError ? '⚠' : '🔧'} {toolUseCount} tool call{toolUseCount !== 1 ? 's' : ''}
      </button>
      {expanded && (
        <div className="ml-3 border-l border-gray-800 pl-3 space-y-1 mt-1">
          {messages.map((msg) => <ToolItem key={msg.id} message={msg} taskId={taskId} />)}
        </div>
      )}
    </div>
  );
}

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <button
      onClick={() => { navigator.clipboard.writeText(text).then(() => { setCopied(true); setTimeout(() => setCopied(false), 2000); }); }}
      className="absolute top-2 right-2 p-1 rounded bg-gray-700/80 hover:bg-gray-600 text-gray-400 hover:text-gray-200 opacity-0 group-hover:opacity-100 transition-opacity"
    >
      {copied ? <Check size={12} /> : <Copy size={12} />}
    </button>
  );
}

const taskRemarkPlugins = [remarkTaskArtifactPaths];
const markdownComponents: Components = {
  pre({ children }) {
    let codeText = '';
    if (children && typeof children === 'object' && 'props' in (children as React.ReactElement)) {
      const el = children as React.ReactElement<{ children?: React.ReactNode }>;
      codeText = typeof el.props.children === 'string' ? el.props.children : '';
    }
    return (
      <div className="relative group my-2">
        {codeText && <CopyButton text={codeText} />}
        <pre className="bg-gray-900 rounded-lg p-3 overflow-x-auto text-xs">{children}</pre>
      </div>
    );
  },
  code({ className: codeClassName, children, ...props }) {
    if (!codeClassName) return <code className="bg-gray-700/60 px-1.5 py-0.5 rounded text-xs" {...props}>{children}</code>;
    return <code className={`${codeClassName} text-xs`} {...props}>{children}</code>;
  },
  a({ href, children }) {
    return <a href={href} target="_blank" rel="noopener noreferrer" className="text-indigo-400 hover:text-indigo-300 underline">{children}</a>;
  },
};

const MarkdownContent = memo(function MarkdownContent({ content, taskId }: { content: string; taskId: number }) {
  const taskComponents = useMemo<Components>(() => ({
    ...markdownComponents,
    a({ href, title, children }) {
      return <TaskArtifactLink taskId={taskId} href={href} linkTitle={title}>{children}</TaskArtifactLink>;
    },
  }), [taskId]);
  return (
    <div className="markdown-body">
      <MarkdownRenderer
        content={content}
        components={taskComponents}
        remarkPlugins={taskRemarkPlugins}
      />
    </div>
  );
});

const MessageBubble = memo(function MessageBubble({ message, taskId }: { message: ChatMessage; taskId: number }) {
  if (message.event_type === 'thinking') {
    return (
      <div className="mx-4 px-3 py-2 bg-gray-800/30 rounded text-xs border border-gray-700/30">
        <div className="flex items-center gap-1.5 text-gray-500"><span>💭</span><span className="font-medium">Thinking</span></div>
        {message.content && <div className="mt-1.5"><CollapsibleContent content={message.content} maxLines={3} /></div>}
      </div>
    );
  }
  if (['system_init', 'process_exit', 'system_event'].includes(message.event_type)) {
    const label = message.event_type === 'system_init' ? '— Session started —'
      : message.event_type === 'process_exit' ? '— Done —'
      : `— ${message.content || 'system'} —`;
    return <div className="text-center text-xs text-gray-600 py-1">{label}</div>;
  }
  if (message.is_error) {
    return (
      <div className="mx-4 px-3 py-2 bg-red-500/10 border border-red-500/30 rounded text-sm text-red-400">
        {message.content}
      </div>
    );
  }
  return (
    <div className="flex justify-start">
      <div className="max-w-[85%] rounded-2xl rounded-bl-md px-4 py-2.5 text-sm bg-gray-800 text-gray-200">
        <MarkdownContent content={message.content || ''} taskId={taskId} />
      </div>
    </div>
  );
});

// ─── Iteration panel ──────────────────────────────────────────────────────────

interface IterationPanelProps {
  iteration: number;
  messages: ChatMessage[];
  meta: IterationMeta | null;
  isActive: boolean;       // currently running — default open, live scroll
  defaultOpen: boolean;
}

function IterationPanel({ iteration, messages, meta, isActive, defaultOpen, taskId }: IterationPanelProps & { taskId: number }) {
  const [open, setOpen] = useState(defaultOpen || isActive);
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (isActive) bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, isActive]);

  const statusIcon = isActive ? '⟳' : meta?.action === 'done' ? '✓' : meta?.action === 'abort' ? '✗' : '✓';
  const statusColor = isActive ? 'text-blue-400' : meta?.action === 'abort' ? 'text-red-400' : 'text-green-500';

  const grouped = useMemo(() => groupMessages(messages), [messages]);

  return (
    <div className="border border-gray-700/50 rounded-lg overflow-hidden">
      <button
        onClick={() => setOpen((v) => !v)}
        className="w-full flex items-center gap-2 px-4 py-2.5 bg-gray-800/60 hover:bg-gray-800 text-left transition-colors"
      >
        {open ? <ChevronDown size={14} className="text-gray-500 shrink-0" /> : <ChevronRight size={14} className="text-gray-500 shrink-0" />}
        <span className="text-xs font-semibold text-gray-400">Iteration {iteration + 1}</span>
        <span className={`text-xs font-medium ${statusColor}`}>{statusIcon} {isActive ? 'running' : (meta?.action ?? '')}</span>
        {(meta?.progress || meta?.reason) && (
          <span className="text-xs text-gray-500 truncate ml-auto">
            {meta.progress && <span className="mr-1.5">{meta.progress}</span>}
            {meta.reason}
          </span>
        )}
      </button>
      {open && (
        <div className="p-3 space-y-2 bg-gray-900/30">
          {grouped.map((group, i) =>
            group.type === 'tool-group' ? (
              <ToolGroup key={i} messages={group.messages} taskId={taskId} />
            ) : (
              <MessageBubble key={group.message.id} message={group.message} taskId={taskId} />
            )
          )}
          {isActive && (
            <div className="text-xs text-gray-500 px-4 animate-pulse">Claude is working...</div>
          )}
          <div ref={bottomRef} />
        </div>
      )}
    </div>
  );
}

// ─── Main view ────────────────────────────────────────────────────────────────

export function LoopChatView({ task, onBack, inline }: LoopChatViewProps) {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [iterMeta, setIterMeta] = useState<Map<number, IterationMeta>>(new Map());
  const [activeIteration, setActiveIteration] = useState<number | null>(null);
  const [showScrollBottom, setShowScrollBottom] = useState(false);
  const scrollContainerRef = useRef<HTMLDivElement>(null);
  const scrollBottomRef = useRef<HTMLDivElement>(null);
  const historyLoadedRef = useRef(false);
  const pendingWsRef = useRef<ChatMessage[]>([]);
  const historyGenerationRef = useRef(0);
  const currentTaskIdRef = useRef(task.id);
  const initializedTaskIdRef = useRef<number | null>(null);
  const [localStatus, setLocalStatus] = useState<string | null>(null);
  const [localBackgroundActive, setLocalBackgroundActive] = useState<boolean | null>(null);
  const lastWsStatusAt = useRef(0);
  const lastWsBackgroundAt = useRef(0);
  const effectiveStatus = localStatus || task.status;
  const backgroundLifecycle = useMemo(
    () => latestBackgroundLifecycle(messages, task),
    [messages, task.retry_count, task.turn_generation],
  );
  const backgroundActive = (
    backgroundLifecycle?.state === 'running'
    || (localBackgroundActive ?? task.background_active === true)
  );
  const foregroundActive = (
    !backgroundActive
    && backgroundLifecycle === null
    && ['executing', 'in_progress'].includes(effectiveStatus)
  );
  const hasActiveWork = foregroundActive || backgroundActive;
  const taskStatusRef = useRef(effectiveStatus);
  currentTaskIdRef.current = task.id;
  taskStatusRef.current = effectiveStatus;

  useEffect(() => {
    const prev = document.title;
    const label = task.title || task.description || '';
    const preview = label.length > 30 ? label.slice(0, 30) + '…' : label;
    document.title = preview ? `#${task.id} ${preview}` : `#${task.id} - CCM`;
    return () => { document.title = prev; };
  }, [task.id, task.title, task.description]);

  const handleWsMessage = useCallback((raw: Record<string, unknown>) => {
    const msg = raw as { channel?: string; data?: Record<string, unknown> };
    if (!msg.data) return;

    const isStatusChange = (
      (
        msg.channel === 'tasks'
        && msg.data.event === 'status_change'
        && Number(msg.data.task_id) === task.id
      )
      || (
        msg.channel === `task:${task.id}`
        && (
          msg.data.event === 'status_change'
          || msg.data.event_type === 'status_change'
        )
      )
    );
    if (isStatusChange) {
      if (typeof msg.data.background_active === 'boolean') {
        lastWsBackgroundAt.current = Date.now();
        setLocalBackgroundActive(msg.data.background_active);
      }
      const newStatus = msg.data.new_status;
      if (typeof newStatus === 'string' && newStatus) {
        lastWsStatusAt.current = Date.now();
        setLocalStatus(newStatus);
      }
      return;
    }

    const isBackgroundActivity = (
      (
        msg.channel === 'tasks'
        && msg.data.event === 'background_activity'
        && Number(msg.data.task_id) === task.id
      )
      || (
        msg.channel === `task:${task.id}`
        && (
          msg.data.event === 'background_activity'
          || msg.data.event_type === 'background_activity'
        )
      )
    );
    if (isBackgroundActivity) {
      if (typeof msg.data.background_active === 'boolean') {
        lastWsBackgroundAt.current = Date.now();
        setLocalBackgroundActive(msg.data.background_active);
      }
      return;
    }

    if (msg.channel !== `task:${task.id}`) return;

    const eventType = msg.data.event_type as string;

    if (eventType === 'background_lifecycle') {
      const retryCount = Number(msg.data.task_retry_count);
      const turnGeneration = Number(msg.data.task_turn_generation);
      if (
        !Number.isInteger(retryCount)
        || !Number.isInteger(turnGeneration)
        || retryCount !== task.retry_count
        || turnGeneration !== task.turn_generation
      ) return;
      const lifecycle: BackgroundLifecycle = {
        state: msg.data.background_state === 'completed' ? 'completed' : 'running',
        reason: String(msg.data.background_reason || 'waiting_for_descendants'),
        active_count: Math.max(0, Number(msg.data.background_active_count) || 0),
        active_thread_ids: Array.isArray(msg.data.background_active_thread_ids)
          ? msg.data.background_active_thread_ids.map(String)
          : [],
        started_at: String(msg.data.background_started_at || new Date().toISOString()),
        last_activity_at: typeof msg.data.background_last_activity_at === 'string'
          ? msg.data.background_last_activity_at
          : null,
      };
      const persistedId = Number(msg.data.id);
      const lifecycleIteration = Number(msg.data.loop_iteration);
      const entry: ChatMessage = {
        id: Number.isFinite(persistedId) && persistedId > 0
          ? persistedId
          : Date.now() + Math.random(),
        role: 'system',
        event_type: 'background_lifecycle',
        content: null,
        tool_name: null,
        tool_input: null,
        tool_output: null,
        is_error: false,
        loop_iteration: Number.isInteger(lifecycleIteration)
          ? lifecycleIteration
          : null,
        task_retry_count: retryCount,
        task_turn_generation: turnGeneration,
        timestamp: typeof msg.data.timestamp === 'string'
          ? msg.data.timestamp
          : new Date().toISOString(),
        image_urls: null,
        attachments: null,
        background_lifecycle: lifecycle,
        persisted: Number.isFinite(persistedId) && persistedId > 0,
      };
      if (!historyLoadedRef.current) {
        pendingWsRef.current.push(entry);
      } else {
        setMessages((previous) => mergeChatHistory([entry], previous));
      }
      return;
    }

    // Capture loop_iteration_end to update panel headers
    if (eventType === undefined && (msg.data as Record<string, unknown>).event === 'loop_iteration_end') {
      const d = msg.data as { iteration: number; action: string; reason: string; progress: string | null };
      setIterMeta((prev) => {
        const next = new Map(prev);
        next.set(d.iteration, { action: d.action as IterationMeta['action'], reason: d.reason, progress: d.progress });
        return next;
      });
      if (d.action === 'continue') setActiveIteration(d.iteration + 1);
      else setActiveIteration(null);
      return;
    }

    if (eventType === 'process_exit') {
      return;
    }

    const showTypes = ['message', 'result', 'tool_use', 'tool_result', 'system_init', 'system_event', 'thinking'];
    if (!showTypes.includes(eventType)) return;
    if (isLegacyCodexCollabCompleted({
      event_type: eventType,
      content: (msg.data.content as string) || null,
      native_item_type: (msg.data.native_item_type as string) || null,
      native_item_status: (msg.data.native_item_status as string) || null,
    })) return;
    const skipSystemContent = ['task_progress', 'thinking_tokens', 'token_usage', 'api_request', 'api_response'];
    if (eventType === 'system_event' && skipSystemContent.includes(msg.data.content as string)) return;
    const content = (msg.data.content as string) || null;
    if ((eventType === 'message' || eventType === 'result') && !content) return;

    const persistedId = Number(msg.data.id);
    const isPersisted = Number.isFinite(persistedId) && persistedId > 0;
    const itemId = (msg.data.item_id as string) || null;
    const declaredRetryCount = Number(msg.data.task_retry_count);
    const declaredTurnGeneration = Number(msg.data.task_turn_generation);
    if (
      (Number.isInteger(declaredRetryCount)
        && declaredRetryCount !== task.retry_count)
      || (Number.isInteger(declaredTurnGeneration)
        && declaredTurnGeneration !== task.turn_generation)
    ) return;
    const entry: ChatMessage = {
      id: isPersisted ? persistedId : Date.now() + Math.random(),
      role: (msg.data.role as string) || 'assistant',
      event_type: eventType,
      content,
      tool_name: (msg.data.tool_name as string) || null,
      tool_input: (msg.data.tool_input as string) || null,
      tool_output: (msg.data.tool_output as string) || null,
      is_error: (msg.data.is_error as boolean) || false,
      loop_iteration: (msg.data.loop_iteration as number) ?? 0,
      task_retry_count: Number.isInteger(declaredRetryCount)
        ? declaredRetryCount
        : task.retry_count,
      task_turn_generation: Number.isInteger(declaredTurnGeneration)
        ? declaredTurnGeneration
        : task.turn_generation,
      timestamp: (msg.data.timestamp as string) || new Date().toISOString(),
      image_urls: null,
      attachments: null,
      item_id: itemId,
      stream_item_id: itemId,
      native_item_type: (msg.data.native_item_type as string) || null,
      native_item_status: (msg.data.native_item_status as string) || null,
      persisted: isPersisted,
    };

    if (!historyLoadedRef.current) {
      pendingWsRef.current.push(entry);
      return;
    }
    setMessages((prev) => [...prev, entry]);
  }, [task.id, task.retry_count, task.turn_generation]);

  const refreshHistory = useCallback(() => {
    const requestedTaskId = task.id;
    const generation = ++historyGenerationRef.current;
    return api.getTaskChatHistory(task.id, true, 0, 0, true).then((msgs) => {
      if (
        generation !== historyGenerationRef.current
        || requestedTaskId !== currentTaskIdRef.current
      ) return;
      const filtered = msgs
        .filter((m) =>
          !isLegacyCodexCollabCompleted(m) &&
          !((m.event_type === 'message' || m.event_type === 'result') && !m.content)
        )
        .map((message) => ({ ...message, persisted: true }));
      const buffered = pendingWsRef.current;
      pendingWsRef.current = [];
      historyLoadedRef.current = true;
      setMessages((current) => mergeChatHistory(filtered, [...current, ...buffered]));
      if (['executing', 'in_progress'].includes(taskStatusRef.current)) {
        const maxIter = [...filtered, ...buffered]
          .reduce((acc, m) => Math.max(acc, m.loop_iteration ?? 0), 0);
        setActiveIteration((current) => Math.max(current ?? 0, maxIter));
      } else {
        setActiveIteration(null);
      }
    }).catch(() => {
      if (
        generation !== historyGenerationRef.current
        || requestedTaskId !== currentTaskIdRef.current
      ) return;
      const buffered = pendingWsRef.current;
      pendingWsRef.current = [];
      historyLoadedRef.current = true;
      if (buffered.length > 0) {
        setMessages((current) => mergeChatHistory([], [...current, ...buffered]));
      }
    });
  }, [task.id]);

  const handleSubscribed = useCallback((channels: string[]) => {
    if (channels.includes(`task:${task.id}`)) void refreshHistory();
  }, [refreshHistory, task.id]);

  useWebSocket(
    [`task:${task.id}`, 'tasks'],
    handleWsMessage,
    undefined,
    handleSubscribed,
  );

  useEffect(() => {
    if (initializedTaskIdRef.current !== task.id) {
      initializedTaskIdRef.current = task.id;
      historyGenerationRef.current += 1;
      historyLoadedRef.current = false;
      pendingWsRef.current = [];
      setMessages([]);
      setIterMeta(new Map());
      setActiveIteration(null);
      setLocalStatus(null);
      setLocalBackgroundActive(null);
    }
    void refreshHistory();
  }, [refreshHistory, task.id, task.status, task.background_active]);

  // A poll response may have started before a newer WebSocket event.  Keep
  // either local override for one full polling cycle so stale props cannot
  // briefly hide a still-live background generation (or resurrect a settled
  // one).
  useEffect(() => {
    if (localStatus === null) return;
    let timer: ReturnType<typeof setTimeout>;
    const clearWhenStale = () => {
      const remaining = 7000 - (Date.now() - lastWsStatusAt.current);
      if (remaining <= 0) {
        setLocalStatus(null);
      } else {
        timer = setTimeout(clearWhenStale, remaining);
      }
    };
    const remaining = 7000 - (Date.now() - lastWsStatusAt.current);
    if (remaining <= 0) {
      setLocalStatus(null);
      return;
    }
    timer = setTimeout(clearWhenStale, remaining);
    return () => clearTimeout(timer);
  }, [localStatus, task.status]);

  useEffect(() => {
    if (localBackgroundActive === null) return;
    let timer: ReturnType<typeof setTimeout>;
    const clearWhenStale = () => {
      const remaining = 7000 - (
        Date.now() - lastWsBackgroundAt.current
      );
      if (remaining <= 0) {
        setLocalBackgroundActive(null);
      } else {
        timer = setTimeout(clearWhenStale, remaining);
      }
    };
    const remaining = 7000 - (
      Date.now() - lastWsBackgroundAt.current
    );
    if (remaining <= 0) {
      setLocalBackgroundActive(null);
      return;
    }
    timer = setTimeout(clearWhenStale, remaining);
    return () => clearTimeout(timer);
  }, [localBackgroundActive, task.background_active]);

  useEffect(() => {
    const el = scrollContainerRef.current;
    if (!el) return;
    const handleScroll = () => {
      const distanceFromBottom = el.scrollHeight - el.scrollTop - el.clientHeight;
      setShowScrollBottom(distanceFromBottom > 300);
    };
    el.addEventListener('scroll', handleScroll, { passive: true });
    return () => el.removeEventListener('scroll', handleScroll);
  }, []);

  const iterationGroups = useMemo(() => {
    const map = groupByIteration(messages);
    return [...map.entries()].sort(([a], [b]) => a - b);
  }, [messages]);

  const maxIteration = iterationGroups.length > 0 ? iterationGroups[iterationGroups.length - 1][0] : 0;

  const [cancelling, setCancelling] = useState(false);
  const [cancelError, setCancelError] = useState<string | null>(null);

  const handleCancel = async () => {
    setCancelling(true);
    setCancelError(null);
    try {
      await api.cancelTask(task.id);
    } catch (e) {
      setCancelError(`Cancel failed: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setCancelling(false);
    }
  };

  useEffect(() => {
    if (!foregroundActive) {
      setActiveIteration(null);
      setCancelling(false);
    }
  }, [foregroundActive]);

  return (
    <div className={inline ? "flex flex-col h-full bg-gray-950" : "fixed inset-0 bg-gray-950 flex flex-col z-50"}>
      {/* Header */}
      <div className="flex items-center gap-3 px-4 py-3 pt-[max(0.75rem,env(safe-area-inset-top))] border-b border-gray-800 bg-gray-900">
        <button onClick={onBack} className="text-gray-400 hover:text-foreground">
          <ArrowLeft size={20} />
        </button>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <p className="text-foreground font-medium text-sm">Task #{task.id}</p>
            <span className="text-xs bg-indigo-600/20 text-indigo-400 px-1.5 rounded">Loop</span>
          </div>
          <p className="text-xs text-gray-500 truncate">
            {task.todo_file_path}
            {task.loop_progress && <span className="ml-2 text-indigo-400">{task.loop_progress}</span>}
          </p>
        </div>
      </div>

      {/* Iteration panels */}
      <div ref={scrollContainerRef} className="flex-1 overflow-y-auto p-4 space-y-3">
        {iterationGroups.length === 0 && (
          <div className="text-center text-gray-600 mt-20">
            <p className="text-lg mb-2">Loop task</p>
            <p className="text-sm">Iterations will appear here as Claude works through the todo list.</p>
          </div>
        )}
        {iterationGroups.map(([iter, msgs]) => (
          <IterationPanel
            key={iter}
            iteration={iter}
            messages={msgs}
            meta={iterMeta.get(iter) ?? null}
            isActive={foregroundActive && iter === (activeIteration ?? maxIteration)}
            defaultOpen={iter === maxIteration}
            taskId={task.id}
          />
        ))}
        <div ref={scrollBottomRef} />
      </div>
      {showScrollBottom && (
        <button
          onClick={() => scrollBottomRef.current?.scrollIntoView({ behavior: 'smooth' })}
          className="absolute bottom-28 right-6 z-10 p-2.5 bg-gray-700 hover:bg-gray-600 text-gray-300 hover:text-foreground rounded-full shadow-lg transition-all"
          title="Scroll to bottom"
        >
          <ArrowDown size={18} />
        </button>
      )}

      {/* Footer */}
      {hasActiveWork && (
        <div className="border-t border-gray-800 bg-gray-900 p-3 flex flex-col items-center gap-2">
          {backgroundActive && (
            <p className="text-xs text-sky-300">
              {foregroundActive
                ? 'Background agents are still running'
                : 'Main loop response finished; background agents are still running'}
            </p>
          )}
          {cancelError && (
            <p className="text-xs text-red-400">{cancelError}</p>
          )}
          <button
            onClick={handleCancel}
            disabled={cancelling}
            className="flex items-center gap-2 px-4 py-2 rounded text-sm font-medium bg-red-600/20 text-red-400 hover:bg-red-600/30 disabled:opacity-50"
          >
            <XCircle size={16} /> {cancelling ? 'Cancelling...' : 'Cancel Loop'}
          </button>
        </div>
      )}
    </div>
  );
}
