import { useState, useEffect, useRef, useMemo, useCallback, memo } from 'react';
import type { Components } from 'react-markdown';
import { api, isApiRequestError } from '../../api/client';
import type {
  AskUserAnswer,
  AskUserQuestion,
  AppliedPlanSnapshot,
  ChatMessage,
  CodexForkAnchor,
  FileAttachment,
  InjectTaskAttachments,
  MonitorSession,
  PlanResource,
  Project,
  Task,
  UploadResult,
} from '../../api/client';
import { useWebSocket } from '../../hooks/useWebSocket';
import { resolveAssetUrl } from '../../config/server';
import { Send, ArrowLeft, Loader2, ChevronDown, ChevronRight, ChevronUp, Copy, Check, Paperclip, X, StopCircle, Pencil, ArrowDown, Star, ListPlus, ListTodo, Trash2, AlertCircle, Sparkles, GitBranch } from '../icons';
import { SecretPicker } from '../Secrets/SecretPicker';
import { QuickPhraseDropdown } from '../QuickPhrases/QuickPhraseDropdown';
import { ListFilter, Syringe } from '../icons';
import { FastModeBadge, PlanPipelineBadge, TaskConfigBadge } from '../Tasks/TaskBadges';
import { VersionedPlansDialog } from '../PlanReview/VersionedPlansDialog';
import { planStalenessConfirmationMessage } from '../PlanReview/planStaleness';
import { AttentionTag } from '../Tasks/AttentionTag';
import { TaskSSHAccessBadge } from '../SSH/TaskSSHAccess';
import { ExpandableText } from '../ExpandableText';
import { copyToClipboard } from '../clipboard';
import { formatMessageTime } from '../../config/timezone';
import { useFileDrop } from '../../hooks/useFileDrop';
import { useVisualViewportBounds } from '../../hooks/useVisualViewportBounds';
import {
  dedupeUploadResults,
  isUploadResult,
  MAX_FILES,
  sameUploadResult,
  useFileUpload,
} from '../../hooks/useFileUpload';
import { SubAgentIndicator } from './SubAgentIndicator';
import { MonitorPanel } from './MonitorPanel';
import {
  isLegacyCodexCollabCompleted,
  mergeChatHistory,
} from './messageMerge';
import { TaskArtifactLink } from './TaskArtifactLink';
import { remarkTaskArtifactPaths } from './taskArtifactMarkdown';
import { MarkdownRenderer } from '../Markdown/MarkdownRenderer';

interface ChatViewProps {
  task: Task;
  projects: Project[];
  onBack: () => void;
  onTaskUpdated?: (task?: Task) => void;
  onTaskForked?: (task: Task) => void;
  inline?: boolean;
}

interface QueuedMessage {
  text: string;
  uploadResults?: UploadResult[];
  planTaskIds?: number[];
  planVersionIds?: number[];
}

interface LiveStreamCacheEntry {
  messages: ChatMessage[];
  updatedAt: number;
}

const LIVE_STREAM_CACHE_MAX_TASKS = 16;
const LIVE_STREAM_CACHE_MAX_ITEMS = 8;
const LIVE_STREAM_CACHE_MAX_CHARS_PER_ITEM = 200_000;
const LIVE_STREAM_CACHE_TTL_MS = 4 * 60 * 60 * 1000;
const liveStreamCache = new Map<number, LiveStreamCacheEntry>();

function taskHasActiveStream(task: Task): boolean {
  return (
    task.background_active === true
    || task.status === 'in_progress'
    || task.status === 'executing'
  );
}

function clearLiveStreamCache(taskId: number): void {
  liveStreamCache.delete(taskId);
}

function pruneLiveStreamCache(now: number): void {
  for (const [taskId, entry] of liveStreamCache) {
    if (now - entry.updatedAt > LIVE_STREAM_CACHE_TTL_MS) {
      liveStreamCache.delete(taskId);
    }
  }
  while (liveStreamCache.size > LIVE_STREAM_CACHE_MAX_TASKS) {
    const oldestTaskId = liveStreamCache.keys().next().value as number | undefined;
    if (oldestTaskId === undefined) break;
    liveStreamCache.delete(oldestTaskId);
  }
}

function syncLiveStreamCache(taskId: number, messages: ChatMessage[]): void {
  const liveMessages = messages
    .filter((message) => (
      !message.persisted
      && Boolean(message.stream_item_id)
      && (message.event_type === 'message' || message.event_type === 'thinking')
    ))
    .slice(-LIVE_STREAM_CACHE_MAX_ITEMS)
    .map((message) => ({
      ...message,
      content: message.content?.slice(-LIVE_STREAM_CACHE_MAX_CHARS_PER_ITEM) ?? null,
    }));
  if (liveMessages.length === 0) {
    clearLiveStreamCache(taskId);
    return;
  }

  const now = Date.now();
  liveStreamCache.delete(taskId);
  liveStreamCache.set(taskId, { messages: liveMessages, updatedAt: now });
  pruneLiveStreamCache(now);
}

function restoreLiveStreamCache(task: Task): ChatMessage[] {
  if (!taskHasActiveStream(task)) {
    clearLiveStreamCache(task.id);
    return [];
  }
  pruneLiveStreamCache(Date.now());
  return (liveStreamCache.get(task.id)?.messages || []).map((message) => ({ ...message }));
}

function loadStoredUploadResults(key: string): UploadResult[] {
  try {
    const parsed: unknown = JSON.parse(localStorage.getItem(key) || '[]');
    if (!Array.isArray(parsed)) return [];
    return dedupeUploadResults(parsed.filter(isUploadResult)).slice(0, MAX_FILES);
  } catch {
    return [];
  }
}

type MessageGroup =
  | { type: 'tool-group'; messages: ChatMessage[] }
  | { type: 'single'; message: ChatMessage };

/** Deduplicate consecutive system events with the same event_type AND content.
 *  In -p mode, retries cause duplicate "Session started" / task_started /
 *  task_notification entries. This keeps the first of each run. */
function deduplicateSystemEvents(messages: ChatMessage[]): ChatMessage[] {
  const systemDedup = new Set(['system_init', 'system_event']);
  const result: ChatMessage[] = [];
  for (const msg of messages) {
    if (systemDedup.has(msg.event_type)) {
      const prev = result[result.length - 1];
      if (
        prev &&
        prev.event_type === msg.event_type &&
        prev.content === msg.content
      ) {
        continue; // skip duplicate
      }
    }
    result.push(msg);
  }
  return result;
}

function groupMessages(messages: ChatMessage[]): MessageGroup[] {
  const groups: MessageGroup[] = [];
  let toolBuf: ChatMessage[] = [];

  const flushTools = () => {
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
      flushTools();
      groups.push({ type: 'single', message: msg });
    }
  }
  flushTools();
  return groups;
}

interface ContextUsage {
  input_tokens: number;
  cache_read_input_tokens: number;
  cache_creation_input_tokens: number;
  output_tokens: number;
  total_input_tokens: number;
  context_window?: number;
}

function formatTokenCount(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return String(n);
}

function ContextUsageIndicator({ usage }: { usage: ContextUsage }) {
  const contextWindow = usage.context_window;
  const totalUsed = usage.total_input_tokens + usage.output_tokens;
  const percentage = contextWindow ? Math.min((totalUsed / contextWindow) * 100, 100) : null;

  // Color based on usage level
  let barColor = 'bg-emerald-500';
  let textColor = 'text-emerald-400';
  if (percentage !== null && percentage > 80) {
    barColor = 'bg-red-500';
    textColor = 'text-red-400';
  } else if (percentage !== null && percentage > 50) {
    barColor = 'bg-amber-500';
    textColor = 'text-amber-400';
  }

  return (
    <div className="flex items-center gap-2 text-xs shrink-0" title={`Input: ${formatTokenCount(usage.input_tokens)} | Cache read: ${formatTokenCount(usage.cache_read_input_tokens)} | Cache create: ${formatTokenCount(usage.cache_creation_input_tokens)} | Output: ${formatTokenCount(usage.output_tokens)}${contextWindow ? ` | Context window: ${formatTokenCount(contextWindow)}` : ' | Context window: unknown'}`}>
      <div className="flex items-center gap-1.5">
        <span className={`${textColor} font-medium`}>{formatTokenCount(totalUsed)}</span>
        <span className="text-gray-600">/</span>
        <span className="text-gray-500">{contextWindow ? formatTokenCount(contextWindow) : 'unknown'}</span>
      </div>
      {percentage !== null && (
        <>
          <div className="w-16 h-1.5 bg-gray-700 rounded-full overflow-hidden">
            <div className={`h-full ${barColor} rounded-full transition-all duration-300`} style={{ width: `${percentage}%` }} />
          </div>
          <span className={`${textColor} w-8 text-right`}>{percentage.toFixed(0)}%</span>
        </>
      )}
    </div>
  );
}

function injectAttachments(uploadResults: UploadResult[]): InjectTaskAttachments {
  return {
    file_paths: uploadResults.map((result) => result.path),
    image_paths: uploadResults
      .filter((result) => result.is_image)
      .map((result) => result.path),
    attachments: uploadResults.map((result) => ({
      url: result.url,
      name: result.filename || result.url.split('/').pop() || 'file',
      is_image: result.is_image,
    })),
  };
}

export function ChatView({ task, projects, onBack, onTaskUpdated, onTaskForked, inline }: ChatViewProps) {
  const projectName = useMemo(() => {
    if (!task.project_id) return null;
    const p = projects.find((p) => p.id === task.project_id);
    return p?.name ?? null;
  }, [task.project_id, projects]);
  const providerLabel = task.provider === 'codex' ? 'Codex' : 'Claude';
  const [messages, setMessages] = useState<ChatMessage[]>(() => restoreLiveStreamCache(task));
  const forkSeedKey = `ccm-fork-seed-consumed-${task.id}`;
  const forkSeedUploadsKey = `ccm-fork-seed-uploads-${task.id}`;
  const forkSeedUploadsConsumedKey = `ccm-fork-seed-uploads-consumed-${task.id}`;
  const draftUploadsKey = `ccm-chat-draft-uploads-${task.id}`;
  const [forkSeedUploads, setForkSeedUploads] = useState<UploadResult[]>(() => {
    try {
      if (localStorage.getItem(forkSeedUploadsConsumedKey)) return [];
      const saved = localStorage.getItem(forkSeedUploadsKey);
      if (saved) return JSON.parse(saved) as UploadResult[];
    } catch { /* storage may be unavailable */ }
    return task.metadata_?.fork_seed_uploads || [];
  });
  // Draft buffer: unsent input survives refresh / re-entering the chat
  const [input, setInput] = useState(() => {
    try {
      const draft = localStorage.getItem(`ccm-chat-draft-${task.id}`);
      if (draft) return draft;
      const seed = task.metadata_?.fork_seed_message;
      if (seed && !localStorage.getItem(forkSeedKey)) {
        localStorage.setItem(forkSeedKey, '1');
        return seed;
      }
      return '';
    } catch {
      return task.metadata_?.fork_seed_message || '';
    }
  });
  const [sending, setSending] = useState(false);
  const [forkOpen, setForkOpen] = useState(false);
  const [forkAnchors, setForkAnchors] = useState<CodexForkAnchor[]>([]);
  const [selectedForkAnchor, setSelectedForkAnchor] = useState<CodexForkAnchor | null>(null);
  const [forkAnchorsLoading, setForkAnchorsLoading] = useState(false);
  const [forkTitle, setForkTitle] = useState('');
  const [forking, setForking] = useState(false);
  const [forkError, setForkError] = useState<string | null>(null);
  const refreshHistoryRef = useRef<() => void>(() => {});
  // A pending HTTP snapshot can arrive after the corresponding WS resolution.
  // Keep request-scoped tombstones for this mounted task so such a snapshot
  // cannot turn an answered/timed-out card back into an actionable one.
  const resolvedAskRequestIdsRef = useRef(new Set<string>());
  const markAskUserResolved = useCallback((
    requestId: string,
    status: 'answered' | 'timed_out' | 'expired',
  ) => {
    resolvedAskRequestIdsRef.current.add(requestId);
    setMessages((prev) => prev.map((message) =>
      message.event_type === 'ask_user_question' && message.request_id === requestId
        ? { ...message, ask_status: status }
        : message
    ));
  }, []);
  // WS 驱动的实时状态覆盖。task.status prop（5s 轮询）才是最终一致的事实源：
  // prop 变化时清掉覆盖（见下方 effect），否则错过一次 WS 事件就永久陈旧。
  const [localStatus, setLocalStatus] = useState<string | null>(null);
  const [localBackgroundActive, setLocalBackgroundActive] = useState<boolean | null>(null);
  // 最近一次 WS status_change 时刻：在途旧轮询快照返回（prop 回退旧值）时
  // 不能击穿刚到的 WS 状态——否则终态 effect 会误触发 autoDequeue，把排队
  // 消息在 turn 进行中提前发出。超过一个轮询周期没有 WS 事件才允许清除。
  const lastWsStatusAt = useRef(0);
  // Background markers need the same stale-poll protection in both directions:
  // an older request can return `true` just after the authoritative WS `false`.
  const lastWsBackgroundAt = useRef(0);
  const [historyLoading, setHistoryLoading] = useState(true);
  const [interrupting, setInterrupting] = useState(false);
  const [stillRunning, setStillRunning] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [dropError, setDropError] = useState<string | null>(null);
  const initialDraftUploads = useMemo(
    () => loadStoredUploadResults(draftUploadsKey),
    [draftUploadsKey],
  );
  const fileUpload = useFileUpload(initialDraftUploads);
  const addChatFiles = fileUpload.addFiles;
  const consumeForkSeedUploads = useCallback(() => {
    if (forkSeedUploads.length === 0) return;
    try {
      localStorage.setItem(forkSeedUploadsConsumedKey, '1');
      localStorage.removeItem(forkSeedUploadsKey);
    } catch { /* storage may be unavailable */ }
    setForkSeedUploads([]);
  }, [
    forkSeedUploads.length,
    forkSeedUploadsConsumedKey,
    forkSeedUploadsKey,
  ]);
  const [selectedSecretIds, setSelectedSecretIds] = useState<number[]>([]);
  const [contextUsage, setContextUsage] = useState<ContextUsage | null>(task.context_window_usage ?? null);
  const [editingTitle, setEditingTitle] = useState(false);
  const [editingAttentionTag, setEditingAttentionTag] = useState(false);
  const [titleDraft, setTitleDraft] = useState(task.title || '');
  const titleInputRef = useRef<HTMLInputElement>(null);
  const [titleExpanded, setTitleExpanded] = useState(false);
  const chatRootRef = useRef<HTMLDivElement>(null);
  const bottomRef = useRef<HTMLDivElement>(null);
  const messagesContainerRef = useRef<HTMLDivElement>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const [starred, setStarred] = useState(task.starred);

  useVisualViewportBounds(chatRootRef, !inline);

  // Temp model override (one-shot per message, not persisted to the task)
  const [modelOverride, setModelOverride] = useState<string | null>(null);
  const [showModelMenu, setShowModelMenu] = useState(false);
  const [modelOptions, setModelOptions] = useState<string[]>([]);
  const [modelContextWindows, setModelContextWindows] = useState<Record<string, number>>({});
  const [codexModelServiceTiers, setCodexModelServiceTiers] = useState<Record<string, string[]>>({});
  const [ptyMode, setPtyMode] = useState(false);
  const [codexAppServerEnabled, setCodexAppServerEnabled] = useState(false);
  const [codexMainMcpEnabled, setCodexMainMcpEnabled] = useState<boolean | null>(null);
  const [codexMonitorEnabled, setCodexMonitorEnabled] = useState<boolean | null>(null);
  const [injecting, setInjecting] = useState(false);
  const injectingRef = useRef(false);
  // 注入模式开关：开启后「发送」直达当前 turn，而不是排队新 turn。
  const [injectMode, setInjectMode] = useState(false);
  const canInject = task.worker_id == null && task.shared_from_id == null && (
    task.provider === 'codex' ? codexAppServerEnabled : ptyMode
  );
  const injectTransport = task.provider === 'codex' ? 'Codex turn/steer' : 'Claude PTY';

  useEffect(() => {
    let active = true;
    setCodexMainMcpEnabled(null);
    setCodexMonitorEnabled(null);
    const settingsRequest = task.worker_id == null
      ? api.getRuntimeSettings()
      : api.getWorkerRuntimeSettings(task.worker_id);
    settingsRequest.then((s) => {
      if (!active) return;
      setPtyMode(s.use_pty_mode);
      setCodexAppServerEnabled(s.codex_app_server_enabled);
      setCodexMainMcpEnabled(
        typeof s.codex_main_mcp_enabled === 'boolean'
          ? s.codex_main_mcp_enabled
          : null,
      );
      setCodexMonitorEnabled(
        typeof s.codex_monitor_enabled === 'boolean'
          ? s.codex_monitor_enabled
          : null,
      );
    }).catch(() => {});
    return () => { active = false; };
  }, [task.worker_id]);

  useEffect(() => {
    if (!canInject) setInjectMode(false);
  }, [canInject]);

  useEffect(() => {
    if (!showModelMenu) return;
    if (modelOptions.length === 0) {
      api.config().then((c) => {
        const opts = (task.provider === 'codex' ? c.codex_model_options : c.model_options).filter((m) => m !== 'default');
        setModelOptions(opts);
        setModelContextWindows(
          task.provider === 'codex' ? {} : (c.claude_model_context_windows || {}),
        );
        setCodexModelServiceTiers(
          task.provider === 'codex' ? (c.codex_model_service_tiers || {}) : {},
        );
      }).catch(() => {});
    }
    const handle = (e: MouseEvent) => {
      if (!(e.target as HTMLElement).closest('[data-temp-model]')) setShowModelMenu(false);
    };
    document.addEventListener('mousedown', handle);
    return () => document.removeEventListener('mousedown', handle);
  }, [showModelMenu, modelOptions.length, task.provider]);

  const handleInject = async (text: string, uploadResults: UploadResult[]) => {
    if ((!text && uploadResults.length === 0) || injectingRef.current) return;
    injectingRef.current = true;
    setInjecting(true);
    setError(null);
    try {
      if (uploadResults.length > 0) {
        const capabilities = await api.getInjectCapabilities(task.id);
        if (capabilities.attachment_protocol !== 1) {
          throw new Error(
            '当前服务器未确认附件注入协议，已在发送前停止；消息和附件未发送',
          );
        }
      }
      const result = await api.injectTaskMessage(
        task.id,
        text || '(files attached)',
        {
          provider: task.provider,
          model: task.model,
          codex_service_tier: task.codex_service_tier,
        },
        uploadResults.length > 0 ? injectAttachments(uploadResults) : undefined,
      );
      if (!result.ok || !result.injected) {
        throw new Error('服务器没有确认消息已注入，输入和附件已保留');
      }
      if (
        uploadResults.length > 0
        && (
          !Number.isInteger(result.attachment_count)
          || result.attachment_count !== uploadResults.length
        )
      ) {
        throw new Error(
          '服务器没有确认全部附件均已注入，输入和附件已保留',
        );
      }
      setInput((current) => (
        current.trim() === text ? '' : current
      ));
      fileUpload.clear();
      consumeForkSeedUploads();
    } catch (e) {
      setError(
        `未收到注入成功确认，消息和附件已保留；请先查看聊天记录或运行日志，再决定是否重试：${
          e instanceof Error ? e.message : String(e)
        }`,
      );
      onTaskUpdated?.();
      refreshHistoryRef.current();
    } finally {
      injectingRef.current = false;
      setInjecting(false);
    }
  };

  // Persist the draft as the user types; cleared when input empties (e.g. send)
  useEffect(() => {
    try {
      if (input) localStorage.setItem(`ccm-chat-draft-${task.id}`, input);
      else localStorage.removeItem(`ccm-chat-draft-${task.id}`);
    } catch { /* storage may be unavailable */ }
  }, [input, task.id]);
  useEffect(() => {
    try {
      if (fileUpload.uploadedResults.length > 0) {
        localStorage.setItem(
          draftUploadsKey,
          JSON.stringify(fileUpload.uploadedResults),
        );
      } else {
        localStorage.removeItem(draftUploadsKey);
      }
    } catch { /* storage may be unavailable */ }
  }, [draftUploadsKey, fileUpload.uploadedResults]);
  useEffect(() => {
    try {
      if (localStorage.getItem(forkSeedUploadsConsumedKey)) {
        localStorage.removeItem(forkSeedUploadsKey);
      } else {
        localStorage.setItem(forkSeedUploadsKey, JSON.stringify(forkSeedUploads));
      }
    } catch { /* storage may be unavailable */ }
  }, [forkSeedUploads, forkSeedUploadsConsumedKey, forkSeedUploadsKey]);
  const [monitorSessions, setMonitorSessions] = useState<MonitorSession[]>([]);
  const [showMonitorPanel, setShowMonitorPanel] = useState(false);

  // Canonical first-class Plans associated with this Task. Legacy carrier
  // Task ids remain queue-readable only so drafts from an older release can
  // still be delivered after migration.
  const [plansOpen, setPlansOpen] = useState(false);
  const [selectedPlanIds, setSelectedPlanIds] = useState<number[]>([]);
  const [versionedPlans, setVersionedPlans] = useState<PlanResource[]>([]);
  const [planRefreshGeneration, setPlanRefreshGeneration] = useState(0);
  const [selectedPlanVersionIds, setSelectedPlanVersionIds] = useState<number[]>([]);
  useEffect(() => {
    setPlansOpen(false);
    setSelectedPlanIds([]);
    setVersionedPlans([]);
    setSelectedPlanVersionIds([]);
  }, [task.id]);

  const refreshVersionedPlans = useCallback(async () => {
    if (!task.session_id || task.shared_from_id != null) return;
    try {
      const rows = await api.listPlans({ target_task_id: task.id });
      setVersionedPlans(rows);
      const attachable = new Set(
        rows
          .filter((plan) => plan.current_version?.human_decision === 'approved' && !plan.current_version.applied)
          .map((plan) => plan.current_version!.id),
      );
      setSelectedPlanVersionIds((current) => current.filter((id) => attachable.has(id)));
    } catch { /* modal exposes actionable errors; passive polling is best-effort */ }
  }, [task.id, task.session_id, task.shared_from_id]);

  useEffect(() => {
    void refreshVersionedPlans();
    const timer = window.setInterval(() => void refreshVersionedPlans(), 5000);
    return () => window.clearInterval(timer);
  }, [refreshVersionedPlans]);

  const togglePlanVersionAttachment = useCallback((versionId: number) => {
    setSelectedPlanVersionIds((current) => current.includes(versionId)
      ? current.filter((id) => id !== versionId)
      : [...current, versionId]);
  }, []);

  const attachPlanVersion = useCallback((versionId: number) => {
    setSelectedPlanVersionIds((current) => current.includes(versionId)
      ? current
      : [...current, versionId]);
  }, []);

  // Distill state
  const [distillOpen, setDistillOpen] = useState(false);
  const [distilling, setDistilling] = useState(false);
  const [distillResult, setDistillResult] = useState<{ suggested_name: string; content: string } | null>(null);
  const [distillName, setDistillName] = useState('');
  const [distillContent, setDistillContent] = useState('');
  const [distillSaving, setDistillSaving] = useState(false);
  const [distillError, setDistillError] = useState<string | null>(null);
  const [distillInstruction, setDistillInstruction] = useState('');
  const effectiveStatus = localStatus || task.status;
  const backgroundActive = localBackgroundActive ?? task.background_active === true;
  // A native agent/monitor tail can remain active while the owning foreground
  // turn is still `executing`; keep the marker independently visible.
  const isProcessing = sending || backgroundActive || ['in_progress', 'executing'].includes(effectiveStatus);
  useEffect(() => {
    if (!stillRunning) return;
    const timer = window.setTimeout(() => {
      setStillRunning(false);
      onTaskUpdated?.();
    }, 15_000);
    return () => window.clearTimeout(timer);
  }, [onTaskUpdated, stillRunning]);
  const planAttentionCount = versionedPlans.filter((plan) =>
    ['waiting_user', 'awaiting_review', 'planner', 'reviewer', 'queued', 'running'].includes(plan.display_state)
    || (plan.display_state === 'approved' && Boolean(plan.current_version && !plan.current_version.applied))
  ).length;
  const [hasMoreHistory, setHasMoreHistory] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const historyCursorRef = useRef<{
    taskId: number;
    beforeId: number | null;
  }>({
    taskId: task.id,
    beforeId: null,
  });
  if (historyCursorRef.current.taskId !== task.id) {
    historyCursorRef.current = {
      taskId: task.id,
      beforeId: null,
    };
  }
  const HISTORY_PAGE_SIZE = 200;

  const navigateUserMessage = useCallback((direction: 'up' | 'down') => {
    const container = messagesContainerRef.current;
    if (!container) return;
    const nodes = Array.from(container.querySelectorAll<HTMLElement>('[data-user-msg]'));
    if (nodes.length === 0) return;

    const containerRect = container.getBoundingClientRect();
    const threshold = 30;

    const scrollToNode = (node: HTMLElement) => {
      const nodeTop = node.offsetTop;
      container.scrollTo({ top: nodeTop, behavior: 'smooth' });
    };

    if (direction === 'up') {
      for (let i = nodes.length - 1; i >= 0; i--) {
        const rect = nodes[i].getBoundingClientRect();
        if (rect.top < containerRect.top - threshold) {
          scrollToNode(nodes[i]);
          return;
        }
      }
    } else {
      for (const node of nodes) {
        const rect = node.getBoundingClientRect();
        if (rect.top > containerRect.top + threshold) {
          scrollToNode(node);
          return;
        }
      }
    }
  }, []);

  // Message queue: pre-queue messages to auto-send after current turn completes
  const [messageQueue, setMessageQueue] = useState<QueuedMessage[]>(() => {
    try {
      const saved = localStorage.getItem(`ccm-chat-queue-${task.id}`);
      if (!saved) return [];
      const parsed = JSON.parse(saved);
      // Migrate legacy string[] format
      if (Array.isArray(parsed) && parsed.length > 0 && typeof parsed[0] === 'string') {
        return (parsed as string[]).map(text => ({ text }));
      }
      if (!Array.isArray(parsed)) return [];
      return parsed.flatMap((item): QueuedMessage[] => {
        if (!item || typeof item !== 'object') return [];
        const candidate = item as Partial<QueuedMessage>;
        if (typeof candidate.text !== 'string') return [];
        const uploadResults = Array.isArray(candidate.uploadResults)
          ? dedupeUploadResults(candidate.uploadResults.filter(isUploadResult))
          : undefined;
        return [{
          text: candidate.text,
          uploadResults: uploadResults?.length ? uploadResults : undefined,
        }];
      });
    } catch { return []; }
  });
  const messageQueueRef = useRef(messageQueue);
  useEffect(() => {
    messageQueueRef.current = messageQueue;
    localStorage.setItem(`ccm-chat-queue-${task.id}`, JSON.stringify(messageQueue));
  }, [messageQueue, task.id]);

  const addToQueue = useCallback((
    text: string,
    uploadResults?: UploadResult[],
    planTaskIds?: number[],
    planVersionIds?: number[],
  ) => {
    setMessageQueue(prev => [...prev, { text, uploadResults, planTaskIds, planVersionIds }]);
    if (planTaskIds?.length) {
      setSelectedPlanIds((current) =>
        current.filter((id) => !planTaskIds.includes(id))
      );
    }
    if (planVersionIds?.length) {
      setSelectedPlanVersionIds((current) =>
        current.filter((id) => !planVersionIds.includes(id))
      );
    }
  }, []);

  const removeFromQueue = useCallback((index: number) => {
    setMessageQueue(prev => {
      const removed = prev[index];
      if (removed?.planTaskIds?.length) {
        setSelectedPlanIds((current) => [
          ...current,
          ...removed.planTaskIds!.filter((id) => !current.includes(id)),
        ]);
      }
      if (removed?.planVersionIds?.length) {
        setSelectedPlanVersionIds((current) => [
          ...current,
          ...removed.planVersionIds!.filter((id) => !current.includes(id)),
        ]);
      }
      return prev.filter((_, i) => i !== index);
    });
  }, []);

  const restoreQueuedUploads = useCallback((items: QueuedMessage[]): boolean => {
    const queuedUploads = dedupeUploadResults(
      items.flatMap((item) => item.uploadResults || []),
    );
    const existingUploaded = dedupeUploadResults([
      ...forkSeedUploads,
      ...fileUpload.uploadedResults,
    ]);
    const additions = queuedUploads.filter(
      (upload) => !existingUploaded.some(
        (existing) => sameUploadResult(existing, upload),
      ),
    );
    const uploadsWithoutResults = (
      fileUpload.uploads.length - fileUpload.uploadedResults.length
    );
    const projectedCount = (
      existingUploaded.length + uploadsWithoutResults + additions.length
    );
    if (projectedCount > MAX_FILES) {
      setError(
        `合并后将有 ${projectedCount} 个附件，单条消息最多支持 ${MAX_FILES} 个；`
        + '请先删除部分附件或队列消息。',
      );
      return false;
    }
    if (!fileUpload.addUploadedResults(additions)) {
      setError(
        `合并后附件超过 ${MAX_FILES} 个，队列已保持原样；请先删除部分附件。`,
      );
      return false;
    }
    return true;
  }, [fileUpload, forkSeedUploads]);

  const editQueueItem = useCallback((index: number) => {
    const item = messageQueueRef.current[index];
    if (!item) return;
    if (!restoreQueuedUploads([item])) return;
    setInput(prev => prev.trim() ? `${prev.trim()}\n\n${item.text}` : item.text);
    if (item.planTaskIds?.length) {
      setSelectedPlanIds((current) => [
        ...current,
        ...item.planTaskIds!.filter((id) => !current.includes(id)),
      ]);
    }
    if (item.planVersionIds?.length) {
      setSelectedPlanVersionIds((current) => [
        ...current,
        ...item.planVersionIds!.filter((id) => !current.includes(id)),
      ]);
    }
    setMessageQueue(prev => prev.filter((_, i) => i !== index));
    requestAnimationFrame(() => textareaRef.current?.focus());
  }, [restoreQueuedUploads]);

  const mergeQueueToInput = useCallback(() => {
    const queued = messageQueueRef.current;
    if (queued.length === 0) return;
    if (!restoreQueuedUploads(queued)) return;
    setInput(prev => {
      const current = prev.trim();
      const merged = queued.map(q => q.text).join('\n\n');
      return current ? `${current}\n\n${merged}` : merged;
    });
    const queuedPlanIds = [
      ...new Set(queued.flatMap((item) => item.planTaskIds || [])),
    ];
    if (queuedPlanIds.length > 0) {
      setSelectedPlanIds((current) => [
        ...current,
        ...queuedPlanIds.filter((id) => !current.includes(id)),
      ]);
    }
    const queuedVersionIds = [
      ...new Set(queued.flatMap((item) => item.planVersionIds || [])),
    ];
    if (queuedVersionIds.length > 0) {
      setSelectedPlanVersionIds((current) => [
        ...current,
        ...queuedVersionIds.filter((id) => !current.includes(id)),
      ]);
    }
    setMessageQueue([]);
    requestAnimationFrame(() => textareaRef.current?.focus());
  }, [restoreQueuedUploads]);

  const clearMessageQueue = useCallback(() => {
    const queuedPlanIds = [
      ...new Set(
        messageQueueRef.current.flatMap((item) => item.planTaskIds || []),
      ),
    ];
    if (queuedPlanIds.length > 0) {
      setSelectedPlanIds((current) => [
        ...current,
        ...queuedPlanIds.filter((id) => !current.includes(id)),
      ]);
    }
    const queuedVersionIds = [
      ...new Set(
        messageQueueRef.current.flatMap((item) => item.planVersionIds || []),
      ),
    ];
    if (queuedVersionIds.length > 0) {
      setSelectedPlanVersionIds((current) => [
        ...current,
        ...queuedVersionIds.filter((id) => !current.includes(id)),
      ]);
    }
    setMessageQueue([]);
  }, []);

  const moveQueueItem = useCallback((index: number, direction: 'up' | 'down') => {
    setMessageQueue(prev => {
      const next = [...prev];
      const target = direction === 'up' ? index - 1 : index + 1;
      if (target < 0 || target >= next.length) return prev;
      [next[index], next[target]] = [next[target], next[index]];
      return next;
    });
  }, []);

  // Auto-dequeue: triggered by process_exit via flag increment
  const [autoDequeueFlag, setAutoDequeueFlag] = useState(0);
  const sendingRef = useRef(false);
  sendingRef.current = sending;
  const backgroundActiveRef = useRef(false);
  backgroundActiveRef.current = backgroundActive;
  const handleSendRef = useRef<(
    text: string,
    uploadResults?: UploadResult[],
    planTaskIds?: number[],
    planVersionIds?: number[],
  ) => void>(() => {});

  useEffect(() => {
    if (autoDequeueFlag === 0) return;
    // Delay to let React flush setSending(false) from status_change/process_exit
    // before we check sendingRef. Without this, PTY mode (no process_exit)
    // triggers autoDequeue in the same cycle as setSending(false) and the
    // ref still reads true → skips the queued message.
    const timer = setTimeout(() => {
      if (sendingRef.current || backgroundActiveRef.current) return;
      const queue = messageQueueRef.current;
      if (queue.length > 0) {
        const next = queue[0];
        setMessageQueue(prev => prev.slice(1));
        setTimeout(
          () => handleSendRef.current(
            next.text,
            next.uploadResults,
            next.planTaskIds,
            next.planVersionIds,
          ),
          300,
        );
      }
    }, 200);
    return () => clearTimeout(timer);
  }, [autoDequeueFlag]);

  useEffect(() => {
    const prev = document.title;
    const label = task.title || task.description || '';
    const preview = label.length > 30 ? label.slice(0, 30) + '…' : label;
    document.title = preview ? `#${task.id} ${preview}` : `#${task.id} - CCM`;
    return () => { document.title = prev; };
  }, [task.id, task.title, task.description]);

  // Handle real-time WebSocket messages via callback (not state) to avoid
  // losing messages when React batches rapid state updates.
  const handleWsMessage = useCallback((raw: Record<string, unknown>) => {
    const msg = raw as { channel?: string; data?: Record<string, unknown> };
    if (
      msg.channel === 'plans'
      && typeof msg.data?.event === 'string'
      && msg.data.event.startsWith('plan_')
      && Number(msg.data.plan_id) > 0
    ) {
      setPlanRefreshGeneration((generation) => generation + 1);
      void refreshVersionedPlans();
      return;
    }
    // System channel: react to PTY mode toggling without a refresh
    if (msg.channel === 'system' && msg.data?.event === 'runtime_settings_changed') {
      // Manager broadcasts describe Manager capabilities only. Worker tasks
      // display the proxied Worker runtime settings loaded above.
      if (task.worker_id == null) {
        setPtyMode(Boolean(msg.data.use_pty_mode));
        if (typeof msg.data.codex_app_server_enabled === 'boolean') {
          setCodexAppServerEnabled(msg.data.codex_app_server_enabled);
        }
        if (typeof msg.data.codex_main_mcp_enabled === 'boolean') {
          setCodexMainMcpEnabled(msg.data.codex_main_mcp_enabled);
        }
        if (typeof msg.data.codex_monitor_enabled === 'boolean') {
          setCodexMonitorEnabled(msg.data.codex_monitor_enabled);
        }
      }
      return;
    }
    // Status change: update local override for "thinking" indicator.
    // Handles both "tasks" global channel and "task:{id}" channel (from SharedRelay mirror).
    const isStatusChange = (
      (msg.channel === 'tasks' && msg.data?.event === 'status_change' && msg.data.task_id === task.id) ||
      (msg.channel === `task:${task.id}` && (msg.data?.event === 'status_change' || msg.data?.event_type === 'status_change'))
    );
    if (isStatusChange) {
      const newStatus = (msg.data!.new_status as string) || '';
      const nextBackground = msg.data!.background_active;
      if (typeof msg.data!.background_active === 'boolean') {
        lastWsBackgroundAt.current = Date.now();
        setLocalBackgroundActive(msg.data!.background_active);
      }
      if (newStatus) {
        lastWsStatusAt.current = Date.now();
        setLocalStatus(newStatus);
      }
      if (
        ['completed', 'failed', 'cancelled', 'conflict'].includes(newStatus)
        && nextBackground !== true
      ) {
        setStillRunning(false);
      }
      return;
    }

    const isBackgroundActivity = (
      msg.data
      && (
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
      )
    );
    if (isBackgroundActivity) {
      if (typeof msg.data!.background_active === 'boolean') {
        lastWsBackgroundAt.current = Date.now();
        setLocalBackgroundActive(msg.data!.background_active);
      }
      return;
    }

    if (msg.channel !== `task:${task.id}` || !msg.data) return;

    const eventType = msg.data.event_type as string || (msg.data.event as string);
    if (eventType === 'monitor_session_created' || eventType === 'monitor_session_status'
        || eventType === 'sub_agent_session_created' || eventType === 'sub_agent_session_status') {
      api.listMonitorSessions(task.id).then(setMonitorSessions).catch(() => {});
      return;
    }

    // 权限透传：CC 请求权限 → 聊天卡片；用户点按钮回包
    if (eventType === 'permission_request') {
      const entry: ChatMessage = {
        id: Date.now() + Math.random(),
        role: 'system',
        event_type: 'permission_request',
        content: (msg.data.description as string) || null,
        tool_name: (msg.data.tool_name as string) || null,
        tool_input: (msg.data.input_preview as string) || null,
        tool_output: null,
        is_error: false,
        loop_iteration: null,
        timestamp: new Date().toISOString(),
        image_urls: null,
        attachments: null,
        request_id: (msg.data.request_id as string) || null,
        permission_status: 'pending',
      };
      setMessages((prev) => [...prev, entry]);
      return;
    }
    if (eventType === 'permission_resolved') {
      const rid = msg.data.request_id as string;
      const behavior = msg.data.behavior as 'allow' | 'deny';
      setMessages((prev) => prev.map((m) =>
        m.event_type === 'permission_request' && m.request_id === rid
          ? { ...m, permission_status: behavior }
          : m
      ));
      return;
    }

    // ask_user：CC 调用内置 AskUserQuestion 被 hook 拦截 → 可选卡片；用户选完回包
    if (eventType === 'ask_user_question') {
      const rid = (msg.data.request_id as string) || null;
      const questions = (msg.data.questions as AskUserQuestion[]) || [];
      if (rid && resolvedAskRequestIdsRef.current.has(rid)) return;
      setMessages((prev) => {
        if (rid && prev.some((m) => m.event_type === 'ask_user_question' && m.request_id === rid)) {
          return prev; // 去重（重连回填可能与 WS 撞车）
        }
        const entry: ChatMessage = {
          id: Date.now() + Math.random(),
          role: 'system',
          event_type: 'ask_user_question',
          content: null,
          tool_name: 'AskUserQuestion',
          tool_input: null,
          tool_output: null,
          is_error: false,
          loop_iteration: null,
          timestamp: new Date().toISOString(),
          image_urls: null,
          attachments: null,
          request_id: rid,
          ask_questions: questions,
          ask_status: 'pending',
        };
        return [...prev, entry];
      });
      return;
    }
    if (eventType === 'ask_user_resolved') {
      const rid = msg.data.request_id as string;
      const timedOut = !!msg.data.timed_out;
      if (rid) markAskUserResolved(rid, timedOut ? 'timed_out' : 'answered');
      return;
    }

    // 模型原生子 agent 的进度（PTY 观测，经 sub_agent_sessions 镜像）
    if (eventType === 'sub_agent_report') {
      api.listMonitorSessions(task.id).then(setMonitorSessions).catch(() => {});
      return;
    }

    // CCM Sub-Agent progress: show in chat as system_event, update panel
    if (eventType === 'sub_agent_progress') {
      api.listMonitorSessions(task.id).then(setMonitorSessions).catch(() => {});
      const summary = msg.data.summary as string;
      const saSessionId = msg.data.sub_agent_session_id as number;
      const description = msg.data.description as string;
      if (summary) {
        const entry: ChatMessage = {
          id: Date.now() + Math.random(),
          role: 'system',
          event_type: 'system_event',
          content: `[Sub-Agent #${saSessionId}: ${description}] ${summary}`,
          tool_name: null,
          tool_input: null,
          tool_output: null,
          is_error: false,
          loop_iteration: null,
          timestamp: new Date().toISOString(),
          image_urls: null,
          attachments: null,
          source: 'sub-agent',
        };
        setMessages((prev) => [...prev, entry]);
      }
      return;
    }

    // Sub-Agent session status change: just refresh panel
    if (eventType === 'sub_agent_session_status' || eventType === 'sub_agent_session_created') {
      api.listMonitorSessions(task.id).then(setMonitorSessions).catch(() => {});
      return;
    }

    if (eventType === 'monitor_check') {
      // Always refresh panel data
      api.listMonitorSessions(task.id).then(setMonitorSessions).catch(() => {});
      // Dedup: don't insert into chat flow. If chat_injected=true, a separate
      // user_message event will arrive. If false, it's a non-important check
      // that only belongs in MonitorPanel. Legacy events (chat_injected
      // missing) get a muted card for backward compat.
      const chatInjected = msg.data.chat_injected;
      if (chatInjected === undefined) {
        // Legacy data without chat_injected field — render muted card
        const summary = msg.data.summary as string;
        const monitorSessionId = msg.data.monitor_session_id as number;
        const checkNumber = msg.data.check_number as number;
        if (summary) {
          const entry: ChatMessage = {
            id: Date.now() + Math.random(),
            role: 'system',
            event_type: 'system_event',
            content: `[Monitor #${monitorSessionId}] Check #${checkNumber}: ${summary}`,
            tool_name: null,
            tool_input: null,
            tool_output: null,
            is_error: (msg.data.status as string) === 'failed',
            loop_iteration: null,
            timestamp: new Date().toISOString(),
            image_urls: null,
            attachments: null,
            source: 'monitor',
          };
          setMessages((prev) => [...prev, entry]);
        }
      }
      // chat_injected true/false: do NOT insert into chat flow
      return;
    }

    // Anthropic 基础设施侧临时限流/过载（非额度用尽）：后端正在退避后用同一
    // 账号自动重试。提示用户并保持"处理中"指示（PTY 下这是 exit_code=0 的
    // 中止 turn，process_exit 可能先到、会熄灭 spinner，这里重新点亮）。
    if (eventType === 'transient_retry') {
      const attempt = (msg.data.attempt as number) || 1;
      const maxAttempts = (msg.data.max_attempts as number) || 0;
      const delay = (msg.data.delay as number) || 0;
      setSending(true);
      const entry: ChatMessage = {
        id: Date.now() + Math.random(),
        role: 'system',
        event_type: 'transient_retry',
        content: `服务端临时限流（非额度用尽）· 第 ${attempt}${maxAttempts ? `/${maxAttempts}` : ''} 次自动重试，约 ${delay}s 后继续…`,
        tool_name: null,
        tool_input: null,
        tool_output: null,
        is_error: false,
        loop_iteration: null,
        timestamp: new Date().toISOString(),
        image_urls: null,
        attachments: null,
        source: 'transient_retry',
      };
      setMessages((prev) => [...prev, entry]);
      return;
    }

    if (eventType === 'process_exit') {
      clearLiveStreamCache(task.id);
      // Small delay so any final output messages queued just before
      // process_exit are rendered before the "thinking" indicator hides.
      setTimeout(() => {
        // A foreground process_exit is not terminal while an exact native
        // background epoch is still active. Its false marker will drive the
        // normal terminal effect after the final autonomous output arrives.
        if (backgroundActiveRef.current) {
          refreshHistoryRef.current();
          return;
        }
        setSending(false);
        setStillRunning(false);
        setLocalStatus(null);  // Reset — status_change WS may have been missed
        setAutoDequeueFlag(f => f + 1);
        // Replace live-only bubbles with their persisted LogEntry ids so every
        // completed Codex turn immediately becomes a valid fork anchor.
        refreshHistoryRef.current();
      }, 500);
      return;
    }

    // Track context window usage
    if (eventType === 'context_usage' && msg.data) {
      setContextUsage((prev) => ({
        input_tokens: (msg.data!.input_tokens as number) || 0,
        cache_read_input_tokens: (msg.data!.cache_read_input_tokens as number) || 0,
        cache_creation_input_tokens: (msg.data!.cache_creation_input_tokens as number) || 0,
        output_tokens: (msg.data!.output_tokens as number) || 0,
        total_input_tokens: (msg.data!.total_input_tokens as number) || 0,
        context_window: (msg.data!.context_window as number) || prev?.context_window,
      }));
      return;
    }

    // WS user_message: append unless already shown (optimistic queue send).
    // Also trigger "thinking" indicator.
    if (eventType === 'user_message') {
      const content = (msg.data.content as string) || '';
      const source = (msg.data.source as string) || null;
      const rawContent = typeof msg.data.raw_content === 'string' ? msg.data.raw_content : null;
      const imageUrls = (msg.data.image_urls as string[]) || null;
      const attachments = (msg.data.attachments as { url: string; name: string; is_image: boolean }[]) || null;
      const appliedPlans = (msg.data.applied_plans as AppliedPlanSnapshot[]) || null;
      const persistedId = Number(msg.data.id);
      const isPersisted = Number.isFinite(persistedId) && persistedId > 0;
      const eventTimestamp = (msg.data.timestamp as string) || new Date().toISOString();
      const entry: ChatMessage = {
        id: isPersisted ? persistedId : Date.now() + Math.random(),
        role: 'user',
        event_type: 'user_message',
        content,
        tool_name: null,
        tool_input: null,
        tool_output: null,
        is_error: false,
        loop_iteration: null,
        timestamp: eventTimestamp,
        image_urls: imageUrls,
        attachments,
        source,
        raw_content: rawContent,
        applied_plans: appliedPlans,
        persisted: isPersisted,
      };
      setSending(true);
      setMessages((prev) => {
        // Reconcile the optimistic bubble with the authoritative broadcast.
        // The optimistic content can be raw text while the server content is
        // prefixed with the sender name. Worker Manager broadcasts also omit
        // attachment and Plan snapshots from their first persisted event, so
        // that event must participate in reconciliation before fingerprinting.
        // raw_content is the canonical user input.
        let optimisticIndex = -1;
        for (let index = prev.length - 1; index >= 0; index -= 1) {
          const candidate = prev[index];
          if (
            !candidate.persisted
            && candidate.role === 'user'
            && candidate.event_type === 'user_message'
            && (
              candidate.content === content
              || (
                rawContent !== null
                && candidate.raw_content === rawContent
              )
            )
          ) {
            optimisticIndex = index;
            break;
          }
        }
        if (optimisticIndex >= 0) {
          const optimistic = prev[optimisticIndex];
          const reconciled = {
            ...entry,
            // A live event without a durable LogEntry id is still the same
            // optimistic send. Keep its local id so HTTP failure compensation
            // can remove the bubble it originally created.
            id: isPersisted ? entry.id : optimistic.id,
            content,
            source,
            raw_content: rawContent ?? optimistic.raw_content,
            timestamp: eventTimestamp,
            image_urls: imageUrls?.length ? imageUrls : optimistic.image_urls,
            attachments: attachments?.length ? attachments : optimistic.attachments,
            applied_plans: appliedPlans?.length ? appliedPlans : optimistic.applied_plans,
            persisted: isPersisted,
          };
          if (isPersisted) {
            return mergeChatHistory(
              [reconciled],
              prev.filter((_candidate, index) => index !== optimisticIndex),
            );
          }
          const next = [...prev];
          next[optimisticIndex] = reconciled;
          return next;
        }
        if (isPersisted) {
          return mergeChatHistory([entry], prev);
        }
        return [...prev, entry];
      });
      return;
    }

    // Codex app-server emits true token deltas.  Keep them live-only and merge
    // by item id; item/completed later replaces this provisional bubble with
    // the authoritative persisted message.
    if (eventType === 'message_delta' || eventType === 'thinking_delta') {
      const delta = (msg.data.content as string) || '';
      const itemId = (msg.data.item_id as string) || null;
      if (!delta || !itemId) return;
      const renderedType = eventType === 'message_delta' ? 'message' : 'thinking';
      setMessages((prev) => {
        const index = prev.findIndex((entry) => entry.stream_item_id === itemId);
        if (index >= 0) {
          const next = [...prev];
          next[index] = { ...next[index], content: `${next[index].content || ''}${delta}` };
          syncLiveStreamCache(task.id, next);
          return next;
        }
        const next = [...prev, {
          id: Date.now() + Math.random(), role: 'assistant', event_type: renderedType,
          content: delta, tool_name: null, tool_input: null, tool_output: null,
          is_error: false, loop_iteration: null, timestamp: new Date().toISOString(),
          image_urls: null, attachments: null, stream_item_id: itemId,
        }];
        syncLiveStreamCache(task.id, next);
        return next;
      });
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

    // Skip noisy system events (heartbeats, telemetry subtypes)
    const skipSystemContent = ['task_progress', 'thinking_tokens', 'token_usage', 'api_request', 'api_response'];
    if (eventType === 'system_event' && skipSystemContent.includes(msg.data.content as string)) return;

    const content = (msg.data.content as string) || null;
    // Skip empty assistant messages (partial streaming chunks with no text)
    if ((eventType === 'message' || eventType === 'result') && !content) return;
    // Skip CC internal messages (compact summaries, task-notifications) — real user input uses event_type=user_message
    if (eventType === 'message' && (msg.data.role as string) === 'user') return;

    const persistedId = Number(msg.data.id);
    const isPersisted = Number.isFinite(persistedId) && persistedId > 0;
    const itemId = (msg.data.item_id as string) || null;
    const entry: ChatMessage = {
      id: isPersisted ? persistedId : Date.now() + Math.random(),
      role: (msg.data.role as string) || 'assistant',
      event_type: eventType,
      content,
      tool_name: (msg.data.tool_name as string) || null,
      tool_input: (msg.data.tool_input as string) || null,
      tool_output: (msg.data.tool_output as string) || null,
      is_error: (msg.data.is_error as boolean) || false,
      loop_iteration: (msg.data.loop_iteration as number) || null,
      timestamp: (msg.data.timestamp as string) || new Date().toISOString(),
      image_urls: (msg.data.image_urls as string[]) || null,
      attachments: (msg.data.attachments as FileAttachment[]) || null,
      source: (msg.data.source as string) || null,
      item_id: itemId,
      stream_item_id: itemId,
      native_item_type: (msg.data.native_item_type as string) || null,
      native_item_status: (msg.data.native_item_status as string) || null,
      pty_cold_start: Boolean(msg.data.pty_cold_start),
      persisted: isPersisted,
    };
    setMessages((prev) => {
      const current = isPersisted
        ? prev.filter((candidate) => !candidate.pty_cold_start)
        : prev;
      if (isPersisted) {
        const next = mergeChatHistory([entry], current);
        syncLiveStreamCache(task.id, next);
        return next;
      }
      if (itemId) {
        const index = current.findIndex((candidate) => candidate.stream_item_id === itemId);
        if (index >= 0) {
          const next = [...current];
          next[index] = entry;
          syncLiveStreamCache(task.id, next);
          return next;
        }
      }
      const next = [...current, entry];
      syncLiveStreamCache(task.id, next);
      return next;
    });
  }, [markAskUserResolved, refreshVersionedPlans, task.id, task.worker_id]);

  const fetchHistory = useCallback(() => {
    setHistoryLoading(true);
    Promise.all([
      api.getTaskChatHistory(task.id, true, HISTORY_PAGE_SIZE, 0, true),
      api.getAskUserPending(task.id).catch(() => ({ pending: [] as { request_id: string; questions: AskUserQuestion[] }[] })),
    ]).then(([msgs, askPending]) => {
      const filtered = msgs
        .filter((m) =>
          !isLegacyCodexCollabCompleted(m) &&
          !((m.event_type === 'message' || m.event_type === 'result') && !m.content)
        )
        .map((m) => ({ ...m, persisted: true }));
      const pageOldestId = filtered.reduce<number | null>(
        (oldest, message) => (
          oldest === null ? message.id : Math.min(oldest, message.id)
        ),
        null,
      );
      if (
        pageOldestId !== null
        && historyCursorRef.current.taskId === task.id
      ) {
        historyCursorRef.current.beforeId = (
          historyCursorRef.current.beforeId === null
            ? pageOldestId
            : Math.min(historyCursorRef.current.beforeId, pageOldestId)
        );
      }
      setHasMoreHistory(msgs.length >= HISTORY_PAGE_SIZE);
      const existingIds = new Set(
        filtered.filter((m) => m.event_type === 'ask_user_question').map((m) => m.request_id)
      );
      const cards: ChatMessage[] = (askPending.pending || [])
        .filter((p) =>
          !existingIds.has(p.request_id)
          && !resolvedAskRequestIdsRef.current.has(p.request_id)
        )
        .map((p) => ({
          id: Date.now() + Math.random(),
          role: 'system' as const,
          event_type: 'ask_user_question',
          content: null,
          tool_name: 'AskUserQuestion',
          tool_input: null,
          tool_output: null,
          is_error: false,
          loop_iteration: null,
          timestamp: new Date().toISOString(),
          image_urls: null,
          attachments: null,
          request_id: p.request_id,
          ask_questions: p.questions,
          ask_status: 'pending',
        }));
      const snapshot = cards.length ? [...filtered, ...cards] : filtered;
      setMessages((current) => {
        const next = mergeChatHistory(snapshot, current);
        syncLiveStreamCache(task.id, next);
        return next;
      });
    }).catch(() => {}).finally(() => setHistoryLoading(false));
  }, [task.id]);
  useEffect(() => {
    refreshHistoryRef.current = fetchHistory;
  }, [fetchHistory]);

  const scrollRestorationRef = useRef<number | null>(null);

  const loadMoreHistory = useCallback(() => {
    if (loadingMore || !hasMoreHistory || messages.length === 0) return;
    const oldestHistoryId = (
      historyCursorRef.current.taskId === task.id
        ? historyCursorRef.current.beforeId
        : null
    );
    if (oldestHistoryId === null) return;
    const container = messagesContainerRef.current;
    if (container) scrollRestorationRef.current = container.scrollHeight;
    setLoadingMore(true);
    api.getTaskChatHistory(task.id, true, HISTORY_PAGE_SIZE, oldestHistoryId).then((msgs) => {
      const filtered = msgs
        .filter((m) =>
          !isLegacyCodexCollabCompleted(m) &&
          !((m.event_type === 'message' || m.event_type === 'result') && !m.content)
        )
        .map((m) => ({ ...m, persisted: true }));
      if (filtered.length > 0) {
        const pageOldestId = filtered.reduce(
          (oldest, message) => Math.min(oldest, message.id),
          filtered[0].id,
        );
        if (historyCursorRef.current.taskId === task.id) {
          historyCursorRef.current.beforeId = pageOldestId;
        }
        setMessages((prev) => mergeChatHistory(filtered, prev));
      }
      setHasMoreHistory(msgs.length >= HISTORY_PAGE_SIZE);
    }).catch(() => {}).finally(() => setLoadingMore(false));
  }, [task.id, messages, loadingMore, hasMoreHistory]);

  useEffect(() => {
    if (scrollRestorationRef.current !== null && !loadingMore) {
      const container = messagesContainerRef.current;
      if (container) {
        container.scrollTop += container.scrollHeight - scrollRestorationRef.current;
      }
      scrollRestorationRef.current = null;
    }
  }, [loadingMore]);

  // Re-fetch history when WebSocket reconnects to pick up any messages
  // that arrived during the disconnection gap
  const handleReconnect = useCallback(() => {
    fetchHistory();
  }, [fetchHistory]);

  const handleSubscribed = useCallback((channels: string[]) => {
    if (channels.includes(`task:${task.id}`)) fetchHistory();
  }, [fetchHistory, task.id]);

  useWebSocket(
    [`task:${task.id}`, 'system', 'tasks', 'plans'],
    handleWsMessage,
    handleReconnect,
    handleSubscribed,
  );

  // Keep a WS status for one full polling cycle, then independently expire it.
  // Depending only on a prop change leaves the override pinned forever when a
  // poll returns the same scalar status (or when no later poll changes it).
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

  // Keep either WS marker value for one full polling cycle. Even when it
  // currently equals the prop, clearing it immediately would let an older
  // in-flight poll response in the opposite direction overwrite the event.
  useEffect(() => {
    if (localBackgroundActive === null) return;
    let timer: ReturnType<typeof setTimeout>;
    const clearWhenStale = () => {
      const remaining = 7000 - (Date.now() - lastWsBackgroundAt.current);
      if (remaining <= 0) {
        setLocalBackgroundActive(null);
      } else {
        // A same-value WS event updates the ref without causing a render.
        // Re-check at the old deadline so that fresh event still gets its
        // complete protection window.
        timer = setTimeout(clearWhenStale, remaining);
      }
    };
    const remaining = 7000 - (Date.now() - lastWsBackgroundAt.current);
    if (remaining <= 0) {
      setLocalBackgroundActive(null);
      return;
    }
    timer = setTimeout(clearWhenStale, remaining);
    return () => clearTimeout(timer);
  }, [localBackgroundActive, task.background_active]);

  // Reset sending state when task reaches a terminal status
  // (catches cases where process_exit WebSocket event is missed — e.g. WS disconnect)
  // Also trigger auto-dequeue so pending box messages get sent.
  useEffect(() => {
    if (
      !backgroundActive
      && ['completed', 'failed', 'cancelled', 'pending'].includes(effectiveStatus)
    ) {
      clearLiveStreamCache(task.id);
      setSending(false);
      setAutoDequeueFlag(f => f + 1);
    }
  }, [backgroundActive, effectiveStatus, task.id]);

  // Load chat history
  useEffect(() => {
    fetchHistory();
  }, [fetchHistory]);

  // Always load monitor sessions (commands can create monitors even without permanent skill)
  useEffect(() => {
    api.listMonitorSessions(task.id).then(setMonitorSessions).catch(() => {});
  }, [task.id]);


  const monitorCount = useMemo(
    () => monitorSessions.filter((s) => s.status === 'running').length,
    [monitorSessions]
  );
  const workerManagedTask = task.metadata_?.ccm_worker_managed_task === true
    || task.metadata_?.ccm_user_skill_snapshots !== undefined;
  const monitorSupported = task.provider !== 'codex' || (
    codexMainMcpEnabled === true
    && codexMonitorEnabled === true
    && task.worker_id == null
    && task.shared_from_id == null
    && !workerManagedTask
  );

  const grouped = useMemo(() => groupMessages(deduplicateSystemEvents(messages)), [messages]);

  // Reset scroll flag when switching tasks
  const hasScrolledRef = useRef(false);
  useEffect(() => {
    hasScrolledRef.current = false;
  }, [task.id]);

  // Lock body scroll while ChatView is open to prevent scroll bleed-through.
  // iOS Safari (especially PWA) ignores overflow:hidden on body — setting
  // position:fixed is the only reliable way to prevent background scrolling.
  useEffect(() => {
    const scrollY = window.scrollY;
    const { body } = document;
    body.style.position = 'fixed';
    body.style.top = `-${scrollY}px`;
    body.style.left = '0';
    body.style.right = '0';
    body.style.overflow = 'hidden';
    return () => {
      body.style.position = '';
      body.style.top = '';
      body.style.left = '';
      body.style.right = '';
      body.style.overflow = '';
      window.scrollTo(0, scrollY);
    };
  }, []);


  const loadMoreRef = useRef(loadMoreHistory);
  loadMoreRef.current = loadMoreHistory;

  // Auto-scroll only on initial history load — use scrollTop instead of
  // scrollIntoView to avoid accidentally scrolling ancestor containers.
  useEffect(() => {
    if (messages.length > 0 && !hasScrolledRef.current) {
      hasScrolledRef.current = true;
      const container = messagesContainerRef.current;
      if (container) {
        container.scrollTop = container.scrollHeight;
      }
    }
  }, [messages]);

  useEffect(() => {
    const el = textareaRef.current;
    if (!el) return;
    el.style.height = 'auto';
    el.style.height = el.scrollHeight + 'px';
  }, [input]);

  useFileDrop({
    onDrop: (files) => {
      if (!injectingRef.current) {
        addChatFiles(files, (msg) => setDropError(msg));
      }
    },
    disabled: injecting || (!task.session_id && !task.shared_from_id),
  });

  useEffect(() => {
    if (injecting || (!task.session_id && !task.shared_from_id)) return;
    const handlePaste = (e: ClipboardEvent) => {
      if (injectingRef.current) return;
      const target = e.target;
      if (target instanceof Element && target.closest('[data-attachment-paste-target]')) return;
      const items = e.clipboardData?.items;
      if (!items) return;
      const files: File[] = [];
      for (const item of items) {
        if (item.kind === 'file') {
          const f = item.getAsFile();
          if (f) files.push(f);
        }
      }
      if (files.length > 0) {
        e.preventDefault();
        addChatFiles(files, (msg) => setDropError(msg));
      }
    };
    document.addEventListener('paste', handlePaste);
    return () => document.removeEventListener('paste', handlePaste);
  }, [
    task.session_id,
    task.shared_from_id,
    addChatFiles,
    injecting,
  ]);

  useEffect(() => {
    if (dropError) {
      const t = setTimeout(() => setDropError(null), 2000);
      return () => clearTimeout(t);
    }
  }, [dropError]);

  const handleFileSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (injectingRef.current) {
      e.target.value = '';
      return;
    }
    const files = Array.from(e.target.files || []);
    if (!files.length) return;
    addChatFiles(files, (msg) => setDropError(msg));
    e.target.value = '';
  };

  const handleTitleSave = async () => {
    const trimmed = titleDraft.trim();
    if (trimmed === (task.title || '')) {
      setEditingTitle(false);
      return;
    }
    try {
      await api.updateTask(task.id, { title: trimmed });
      onTaskUpdated?.();
    } catch { /* ignore */ }
    setEditingTitle(false);
  };

  const handleStar = async () => {
    try {
      const updated = await api.starTask(task.id);
      setStarred(updated.starred);
      onTaskUpdated?.();
    } catch { /* ignore */ }
  };

  const openFork = async () => {
    if (task.provider !== 'codex' || !task.session_id) return;
    setForkOpen(true);
    setSelectedForkAnchor(null);
    setForkAnchors([]);
    setForkTitle('');
    setForkError(null);
    setForkAnchorsLoading(true);
    try {
      setForkAnchors(await api.listForkAnchors(task.id));
    } catch (e) {
      setForkError(e instanceof Error ? e.message : 'Could not load user messages');
    } finally {
      setForkAnchorsLoading(false);
    }
  };

  const confirmFork = async () => {
    if (!selectedForkAnchor || forking) return;
    setForking(true);
    setForkError(null);
    try {
      const forked = await api.forkTask(
        task.id,
        selectedForkAnchor.type !== 'user_message'
          ? { type: selectedForkAnchor.type }
          : { type: 'user_message', id: selectedForkAnchor.id! },
        forkTitle,
      );
      setForkOpen(false);
      onTaskForked?.(forked);
      onTaskUpdated?.();
    } catch (e) {
      setForkError(e instanceof Error ? e.message : 'Fork failed');
    } finally {
      setForking(false);
    }
  };

  const handleSend = async (
    overrideText?: string,
    fromQueue?: boolean,
    preUploadedResults?: UploadResult[],
    preSelectedPlanIds?: number[],
    preSelectedPlanVersionIds?: number[],
  ) => {
    const text = (overrideText ?? input).trim();
    const planIdsForTurn = fromQueue
      ? (preSelectedPlanIds || [])
      : selectedPlanIds;
    const planVersionIdsForTurn = fromQueue
      ? (preSelectedPlanVersionIds || [])
      : selectedPlanVersionIds;
    const fileUploadResultsForTurn = dedupeUploadResults(
      fileUpload.uploadedResults,
    );
    const uploadedResultsForTurn = fromQueue
      ? dedupeUploadResults(preUploadedResults || [])
      : dedupeUploadResults([
        ...forkSeedUploads,
        ...fileUploadResultsForTurn,
      ]);
    const sendableAttachmentCount = uploadedResultsForTurn.length;
    if (!text && sendableAttachmentCount === 0) return;

    if (!fromQueue && fileUpload.isUploading) {
      setError('附件仍在上传，请等待上传完成后再发送。');
      return;
    }
    if (!fromQueue && fileUpload.hasFailed) {
      setError('Retry or remove failed attachments before sending.');
      return;
    }
    // 注入模式：文本和已上传附件直达当前 turn，不新开 turn、不排队。
    if (injectMode && canInject && !fromQueue) {
      if (!isProcessing) {
        setError('注入仅在 turn 正在运行时可用；空闲时请关闭注入模式发送普通消息。');
        return;
      }
      await handleInject(
        text,
        uploadedResultsForTurn,
      );
      return;
    }

    // If currently sending and not from auto-dequeue, add to queue (with already-uploaded results)
    if (isProcessing && !fromQueue) {
      if (text || sendableAttachmentCount > 0) {
        addToQueue(
          text,
          uploadedResultsForTurn.length > 0 ? uploadedResultsForTurn : undefined,
          planIdsForTurn.length > 0 ? [...planIdsForTurn] : undefined,
          planVersionIdsForTurn.length > 0 ? [...planVersionIdsForTurn] : undefined,
        );
        setInput('');
        fileUpload.clear();
        consumeForkSeedUploads();
      }
      return;
    }

    if (!fromQueue) {
      setInput('');
    }
    setSending(true);
    setError(null);

    let optimisticMessageId: number | null = null;
    try {
      let uploadedPaths: string[] | undefined;
      const uploadedResults = uploadedResultsForTurn;
      if (uploadedResults.length > 0) uploadedPaths = uploadedResults.map((r) => r.path);
      if (!fromQueue) fileUpload.clear();

      // Optimistic message — show immediately, always with user prefix.
      // 附件也要立刻带上：WS 回包按内容去重时若整条丢弃，图片就再也不显示了
      if (text) {
        optimisticMessageId = Date.now() + Math.random();
        const optimisticAttachments: FileAttachment[] | null = uploadedResults.length > 0
          ? uploadedResults.map((r) => ({ url: r.url, name: r.filename || r.url.split('/').pop() || 'file', is_image: r.is_image }))
          : null;
        const ccU = JSON.parse(localStorage.getItem('cc_user') || '{}');
        const displayText = ccU.name ? `[${ccU.name}] ${text}` : text;
        const optimisticAppliedPlans: AppliedPlanSnapshot[] = [];
        const versionSnapshots: AppliedPlanSnapshot[] = [];
        planVersionIdsForTurn.forEach((selectedVersionId) => {
          const plan = versionedPlans.find((item) => item.current_version?.id === selectedVersionId);
          const version = plan?.current_version;
          if (plan && version) {
            versionSnapshots.push({
              id: plan.id,
              plan_id: plan.id,
              version_id: version.id,
              version_number: version.version_number,
              title: plan.title,
              content: version.content,
            });
          }
        });
        optimisticAppliedPlans.push(...versionSnapshots);
        setMessages(prev => [...prev, {
          id: optimisticMessageId!, role: 'user', event_type: 'user_message',
          content: displayText, tool_name: null, tool_input: null, tool_output: null,
          is_error: false, loop_iteration: null, timestamp: new Date().toISOString(),
          image_urls: optimisticAttachments?.filter((a) => a.is_image).map((a) => a.url) || null,
          attachments: optimisticAttachments,
          raw_content: text,
          applied_plans: optimisticAppliedPlans.length > 0
            ? optimisticAppliedPlans
            : null,
        }]);
        setSending(true);
      }

      const routing = {
        provider: task.provider,
        model: modelOverride || task.model,
        codex_service_tier: task.codex_service_tier,
      };
      const confirmedStalePlanIds: number[] = [];
      for (;;) {
        try {
          if (planVersionIdsForTurn.length > 0) {
            await api.sendTaskChat(
              task.id,
              text || '(files attached)',
              uploadedPaths,
              selectedSecretIds.length > 0 ? selectedSecretIds : undefined,
              modelOverride,
              routing,
              undefined,
              undefined,
              planVersionIdsForTurn,
              confirmedStalePlanIds,
            );
          } else if (planIdsForTurn.length > 0) {
            await api.sendTaskChat(
              task.id,
              text || '(files attached)',
              uploadedPaths,
              selectedSecretIds.length > 0 ? selectedSecretIds : undefined,
              modelOverride,
              routing,
              planIdsForTurn,
              confirmedStalePlanIds,
            );
          } else {
            // Keep the legacy six-argument call for ordinary messages.
            await api.sendTaskChat(
              task.id,
              text || '(files attached)',
              uploadedPaths,
              selectedSecretIds.length > 0 ? selectedSecretIds : undefined,
              modelOverride,
              routing,
            );
          }
          break;
        } catch (sendError) {
          const detail = isApiRequestError(sendError)
            ? sendError.detail
            : null;
          const stalePlanId = (
            detail
            && typeof detail === 'object'
            && (
              ('plan_version_id' in detail && typeof detail.plan_version_id === 'number')
              || ('plan_task_id' in detail && typeof detail.plan_task_id === 'number')
            )
          ) ? (
            'plan_version_id' in detail && typeof detail.plan_version_id === 'number'
              ? detail.plan_version_id
              : ('plan_task_id' in detail && typeof detail.plan_task_id === 'number' ? detail.plan_task_id : null)
          ) : null;
          const selectedIdsForStale = planVersionIdsForTurn.length > 0
            ? planVersionIdsForTurn
            : planIdsForTurn;
          const staleState = (
            detail
            && typeof detail === 'object'
            && 'staleness' in detail
            && detail.staleness
            && typeof detail.staleness === 'object'
          ) ? detail.staleness as Record<string, unknown> : null;
          if (
            stalePlanId == null
            || !selectedIdsForStale.includes(stalePlanId)
            || confirmedStalePlanIds.includes(stalePlanId)
            || staleState?.stale !== true
            || staleState.hard_conflict === true
            || staleState.can_confirm === false
            || !window.confirm(planStalenessConfirmationMessage(staleState, 'apply'))
          ) {
            throw sendError;
          }
          confirmedStalePlanIds.push(stalePlanId);
        }
      }
      if (planIdsForTurn.length > 0) {
        setSelectedPlanIds((current) =>
          current.filter((id) => !planIdsForTurn.includes(id))
        );
        // Replace the optimistic bubble with the durable user-message row,
        // including the exact approved Plan snapshots used by the backend.
        fetchHistory();
      }
      if (planVersionIdsForTurn.length > 0) {
        setSelectedPlanVersionIds((current) =>
          current.filter((id) => !planVersionIdsForTurn.includes(id))
        );
        setVersionedPlans((current) => current.map((plan) => (
          plan.current_version && planVersionIdsForTurn.includes(plan.current_version.id)
            ? { ...plan, display_state: 'applied', current_version: { ...plan.current_version, applied: true } }
            : plan
        )));
        fetchHistory();
      }
      if (!fromQueue) {
        consumeForkSeedUploads();
      }
      setModelOverride(null);
    } catch (e) {
      setSending(false);
      if (optimisticMessageId !== null) {
        setMessages((current) =>
          current.filter((message) => message.id !== optimisticMessageId)
        );
      }
      onTaskUpdated?.();
      fetchHistory();
      const errMsg = String(e);
      const conflictDetail = (
        isApiRequestError(e) && typeof e.detail === 'string'
          ? e.detail
          : errMsg
      ).toLowerCase();
      const isBusyConflict = (
        (!isApiRequestError(e) || e.status === 409)
        && (
          conflictDetail.includes('currently being processed')
          || conflictDetail.includes('still running')
          || conflictDetail.includes('current turn to finish')
        )
      );
      setStillRunning(isBusyConflict);
      setError(errMsg);
      if (!fromQueue && text) setInput(text);
      if (!fromQueue && fileUploadResultsForTurn.length > 0) {
        fileUpload.addUploadedResults(fileUploadResultsForTurn);
      }
      if (fromQueue && (text || preUploadedResults?.length)) {
        setMessageQueue(prev => [{
          text,
          uploadResults: preUploadedResults,
          planTaskIds: preSelectedPlanIds,
          planVersionIds: preSelectedPlanVersionIds,
        }, ...prev]);
      }
    }
  };

  // Keep ref updated for auto-dequeue effect
  handleSendRef.current = (
    text: string,
    uploadResults?: UploadResult[],
    planTaskIds?: number[],
    planVersionIds?: number[],
  ) => handleSend(text, true, uploadResults, planTaskIds, planVersionIds);

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && (e.ctrlKey || e.metaKey) && !e.nativeEvent.isComposing) {
      e.preventDefault();
      handleSend();
    }
  };

  return (
    <div ref={chatRootRef} className={inline ? "flex flex-col h-full bg-gray-950" : "fixed inset-0 bg-gray-950 flex flex-col z-50"}>
      {/* Header — two rows */}
      <div className="px-3 sm:px-4 py-1.5 pt-[max(0.375rem,env(safe-area-inset-top))] border-b border-gray-800 bg-gray-900">
        {/* Row 1: back + task info + action buttons */}
        <div className="flex items-center gap-2 sm:gap-3">
          <button onClick={onBack} className="text-gray-400 hover:text-foreground shrink-0">
            <ArrowLeft size={20} />
          </button>
          <div className="flex items-center gap-1.5 min-w-0 flex-1">
            <p className="text-foreground font-medium text-sm whitespace-nowrap">Task #{task.id}</p>
            {task.mode === 'plan' ? (
              <PlanPipelineBadge task={task} />
            ) : (
              <>
                <span className={`text-xs px-1.5 rounded font-medium whitespace-nowrap ${task.provider === 'codex' ? 'bg-green-600/30 text-green-300' : 'bg-blue-600/30 text-blue-300'}`}>
                  {providerLabel}
                </span>
                <FastModeBadge task={task} />
                <TaskSSHAccessBadge task={task} />
              </>
            )}
            {backgroundActive && (
              <span className="text-xs bg-teal-600/25 text-teal-300 px-1.5 rounded font-medium whitespace-nowrap animate-pulse">
                后台运行中
              </span>
            )}
            {task.mode !== 'plan' && task.provider === 'codex' && codexMainMcpEnabled !== null && (
              <span
                data-testid="codex-main-mcp-status"
                className={`text-xs px-1.5 rounded font-medium whitespace-nowrap ${
                  codexMainMcpEnabled
                    ? 'bg-teal-600/25 text-teal-300'
                    : 'bg-gray-700 text-gray-400'
                }`}
                title={
                  codexMainMcpEnabled
                    ? 'Codex 主任务 MCP 已启用'
                    : 'Codex 主任务 MCP 已关闭'
                }
              >
                MCP {codexMainMcpEnabled ? '已启用' : '已关闭'}
              </span>
            )}
            {projectName && (
              <span className="text-xs bg-emerald-600/30 text-emerald-300 px-1.5 rounded font-medium whitespace-nowrap truncate">{projectName}</span>
            )}
          </div>
          <div className="flex items-center gap-1 shrink-0">
            <SubAgentIndicator
              taskId={task.id}
              count={monitorCount}
              active={monitorCount > 0}
              onNavigate={() => setShowMonitorPanel(!showMonitorPanel)}
            />
            {task.session_id && task.shared_from_id == null && (
              <button
                onClick={() => setPlansOpen((open) => !open)}
                className={`flex items-center gap-1.5 rounded px-2 py-1 text-xs font-medium transition-colors ${
                  plansOpen
                    ? 'bg-indigo-500/15 text-indigo-300'
                    : 'text-gray-500 hover:bg-gray-800 hover:text-indigo-300'
                }`}
                title="Independent Plans"
                aria-label="Plans"
              >
                <ListTodo size={16} />
                <span>Plans</span>
                {planAttentionCount > 0 && (
                  <span className="min-w-4 rounded-full bg-indigo-500 px-1 text-center text-[9px] font-bold leading-4 text-white">
                    {planAttentionCount}
                  </span>
                )}
              </button>
            )}
            {task.mode !== 'plan' && (
              <TaskConfigBadge task={task} onRefresh={() => onTaskUpdated?.()} align="right" />
            )}
            <button
              onClick={() => {
                setDistillOpen(true);
                setDistillResult(null);
                setDistillError(null);
                setDistilling(false);
              }}
              disabled={messages.length === 0}
              className="p-1.5 transition-colors text-gray-600 hover:text-purple-400 disabled:opacity-30 disabled:cursor-not-allowed"
              title="Distill skill from conversation"
            >
              <Sparkles size={18} />
            </button>
            <button
              onClick={handleStar}
              className={`p-1.5 transition-colors ${starred ? 'text-yellow-400 hover:text-yellow-300' : 'text-gray-600 hover:text-yellow-400'}`}
              title={starred ? "Unstar" : "Star"}
            >
              <Star size={18} fill={starred ? 'currentColor' : 'none'} />
            </button>
            {(isProcessing || stillRunning) && (
              <button
                onClick={async () => {
                  setInterrupting(true);
                  try {
                    const resp = await api.stopTaskSession(task.id);
                    setSending(false);
                    setStillRunning(false);
                    if (resp.stopped === false) {
                      const cleared = resp.cleared_messages ?? 0;
                      setError(
                        `Interrupt: no running process found${cleared > 0 ? `, cleared ${cleared} queued message(s)` : ''}. ` +
                        'If output keeps arriving, the session may still be finishing.'
                      );
                    } else {
                      setError(null);
                    }
                  } catch (interruptError) {
                    setSending(false);
                    const noRunningSession = isApiRequestError(interruptError)
                      && interruptError.status === 400
                      && String(interruptError.detail || '').toLowerCase().includes('no running session');
                    setStillRunning(!noRunningSession);
                    setLocalStatus(null);
                    setError(
                      noRunningSession
                        ? 'Interrupt: the session had already finished before the stop request arrived.'
                        : `Interrupt failed: ${interruptError instanceof Error
                          ? interruptError.message
                          : String(interruptError)}`,
                    );
                  }
                  finally { setInterrupting(false); }
                }}
                disabled={interrupting}
                className="flex items-center gap-1 px-2.5 py-1.5 text-xs text-red-400 hover:text-red-300 border border-red-500/30 rounded hover:bg-red-500/10 disabled:opacity-50"
                title="Interrupt session"
              >
                <StopCircle size={14} />
                <span className="hidden sm:inline">{interrupting ? 'Interrupting...' : 'Interrupt'}</span>
              </button>
            )}
          </div>
        </div>
        {/* Row 2: title + context usage */}
        <div className="flex items-center gap-2 mt-0.5 pl-7 sm:pl-8">
          {!editingAttentionTag && <div className="flex-1 min-w-0">
            {editingTitle ? (
              <input
                ref={titleInputRef}
                autoFocus
                value={titleDraft}
                onChange={(e) => setTitleDraft(e.target.value)}
                onBlur={handleTitleSave}
                onKeyDown={(e) => { if (e.key === 'Enter') handleTitleSave(); if (e.key === 'Escape') { setTitleDraft(task.title || ''); setEditingTitle(false); } }}
                className="w-full bg-gray-800 text-foreground text-xs rounded px-2 py-0.5 focus:outline-none focus:ring-1 focus:ring-indigo-500"
                placeholder="Enter title..."
              />
            ) : (
              <div className="flex items-center gap-1 min-w-0 group/title">
                <span className={`text-xs text-gray-500 ${titleExpanded ? 'whitespace-normal break-all' : 'truncate'}`}>{task.title || task.description || 'Untitled'}</span>
                <button
                  onClick={() => setTitleExpanded(!titleExpanded)}
                  className="text-[10px] text-gray-600 hover:text-gray-300 shrink-0 whitespace-nowrap"
                >{titleExpanded ? 'less' : 'more'}</button>
                <button
                  onClick={() => { setTitleDraft(task.title || ''); setEditingTitle(true); }}
                  className="text-gray-600 hover:text-gray-400 opacity-0 group-hover/title:opacity-100 transition-opacity shrink-0"
                  title="Edit title"
                >
                  <Pencil size={10} />
                </button>
              </div>
            )}
          </div>}
          <AttentionTag
            taskId={task.id}
            value={task.attention_tag}
            editing={editingAttentionTag}
            onEdit={() => {
              setEditingTitle(false);
              setEditingAttentionTag(true);
            }}
            onCancel={() => setEditingAttentionTag(false)}
            onSaved={(updated) => onTaskUpdated?.(updated)}
            showAddButton
            className={editingAttentionTag ? 'flex-1' : 'max-w-[45vw] sm:max-w-xs'}
          />
          {contextUsage && (
            <span className="flex items-center shrink-0">
              <ContextUsageIndicator usage={contextUsage} />
            </span>
          )}
        </div>
      </div>

      {task.metadata_?.forked_from_task_id && (
        <div className="px-4 py-1.5 border-b border-indigo-500/20 bg-indigo-500/5 text-xs text-indigo-300 flex items-center gap-1.5">
          <GitBranch size={12} />
          <span>Forked from Task #{task.metadata_.forked_from_task_id}</span>
        </div>
      )}

      {forkOpen && (
        <div className="fixed inset-0 z-[80] flex items-center justify-center bg-black/60 p-4">
          <div className="flex max-h-[80vh] w-full max-w-lg flex-col rounded-xl border border-gray-700 bg-gray-800 shadow-2xl">
            <div className="flex items-center justify-between border-b border-gray-700 px-4 py-3">
              <div className="flex items-center gap-2 text-sm font-medium text-gray-100">
                <GitBranch size={16} className="text-indigo-400" />
                复制或分叉 Codex Task
              </div>
              <button
                onClick={() => !forking && setForkOpen(false)}
                className="text-gray-500 hover:text-gray-300 disabled:opacity-40"
                disabled={forking}
              >
                <X size={16} />
              </button>
            </div>
            <div className="min-h-0 flex-1 space-y-3 overflow-y-auto px-4 py-4">
              <p className="text-sm text-gray-300">
                “完整复制”会保留最后一个已完成 turn 的全部上下文；选择用户消息则从该消息之前分叉，并把消息预填到输入框中。
              </p>
              <p className="text-xs text-amber-400/90">
                注入消息属于运行中 turn，无法作为精确边界，因此不会出现在列表中。两个 Task 仍使用同一工作目录。
              </p>
              <div className="space-y-1.5">
                {forkAnchorsLoading && (
                  <div className="flex items-center justify-center gap-2 py-8 text-sm text-gray-500">
                    <Loader2 size={15} className="animate-spin" />
                    加载用户消息…
                  </div>
                )}
                {!forkAnchorsLoading && forkAnchors.length === 0 && !forkError && (
                  <div className="rounded border border-gray-700 bg-gray-900/40 px-3 py-6 text-center text-sm text-gray-500">
                    当前会话没有可精确分叉的后续用户消息
                  </div>
                )}
                {forkAnchors.map((anchor) => (
                  <button
                    key={`${anchor.type}-${anchor.id ?? 'initial'}`}
                    type="button"
                    onClick={() => setSelectedForkAnchor(anchor)}
                    className={`w-full rounded-lg border px-3 py-2.5 text-left transition-colors ${
                      selectedForkAnchor?.type === anchor.type
                        && selectedForkAnchor?.id === anchor.id
                        ? 'border-indigo-400 bg-indigo-500/10'
                        : 'border-gray-700 bg-gray-900/40 hover:border-gray-600 hover:bg-gray-700/40'
                    }`}
                  >
                    <div className="line-clamp-3 whitespace-pre-wrap text-sm text-gray-200">
                      {anchor.content}
                    </div>
                    {anchor.type === 'latest' && (
                      <div className="mt-1 text-[11px] text-indigo-300">
                        包含全部用户消息和回答，新 Task 输入框为空
                      </div>
                    )}
                    <div className="mt-1.5 flex items-center gap-2 text-[11px] text-gray-500">
                      {anchor.timestamp && <span>{formatMessageTime(anchor.timestamp)}</span>}
                      {anchor.attachments.length > 0 && (
                        <span className="inline-flex items-center gap-1">
                          <Paperclip size={10} />
                          {anchor.attachments.length}
                        </span>
                      )}
                    </div>
                  </button>
                ))}
              </div>
              <div>
                <label className="mb-1 block text-xs text-gray-400">New Task title (optional)</label>
                <input
                  value={forkTitle}
                  onChange={(e) => setForkTitle(e.target.value)}
                  placeholder={`Fork of #${task.id}`}
                  maxLength={200}
                  className="w-full rounded border border-gray-600 bg-gray-700 px-3 py-2 text-sm text-gray-100 outline-none focus:border-indigo-500"
                />
              </div>
              {forkError && (
                <div className="rounded border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs text-red-400">
                  {forkError}
                </div>
              )}
            </div>
            <div className="flex justify-end gap-2 border-t border-gray-700 px-4 py-3">
              <button
                onClick={() => setForkOpen(false)}
                disabled={forking}
                className="rounded px-3 py-1.5 text-xs text-gray-400 hover:bg-gray-700 hover:text-gray-200 disabled:opacity-40"
              >
                Cancel
              </button>
              <button
                onClick={confirmFork}
                disabled={forking || !selectedForkAnchor}
                className="flex items-center gap-1.5 rounded bg-indigo-600 px-3 py-1.5 text-xs text-white hover:bg-indigo-500 disabled:opacity-50"
              >
                {forking ? <Loader2 size={13} className="animate-spin" /> : <GitBranch size={13} />}
                {forking ? 'Forking…' : 'Create fork'}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Monitor Panel */}
      {showMonitorPanel && (
        <div className="px-4 py-2 border-b border-gray-800">
          <MonitorPanel
            taskId={task.id}
            sessions={monitorSessions}
            onSessionsChange={setMonitorSessions}
            onClose={() => setShowMonitorPanel(false)}
            provider={task.provider}
            monitorSupported={monitorSupported}
          />
        </div>
      )}

      {plansOpen && <VersionedPlansDialog
        open={plansOpen}
        taskId={task.id}
        refreshGeneration={planRefreshGeneration}
        selectedVersionIds={selectedPlanVersionIds}
        onToggleVersion={togglePlanVersionAttachment}
        onAttachVersion={attachPlanVersion}
        onPlansChange={setVersionedPlans}
        onClose={() => setPlansOpen(false)}
      />}

      {/* Distill modal */}
      {distillOpen && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60">
          <div className="bg-gray-800 rounded-lg shadow-xl w-full max-w-2xl max-h-[80vh] flex flex-col m-4 border border-gray-700">
            <div className="flex items-center justify-between px-4 py-3 border-b border-gray-700">
              <div className="flex items-center gap-2">
                <Sparkles size={16} className="text-purple-400" />
                <span className="text-sm font-medium text-foreground">Distill Skill</span>
              </div>
              <button onClick={() => { setDistillOpen(false); setDistillResult(null); setDistillError(null); }} className="text-gray-500 hover:text-gray-300">
                <X size={16} />
              </button>
            </div>

            {/* State 1: Initial — show description and Distill button */}
            {!distilling && !distillResult && !distillError && (
              <div className="p-6 space-y-4">
                <p className="text-sm text-gray-300">
                  从当前 Task 的对话记录中提取可复用的经验，生成一份结构化的 Skill 卡片。
                </p>
                <div>
                  <label className="text-xs text-gray-400 mb-1 block">补充说明（可选）</label>
                  <textarea
                    value={distillInstruction}
                    onChange={(e) => setDistillInstruction(e.target.value)}
                    placeholder="例如：只提取上传功能相关的经验 / 重点关注 bug 排查过程..."
                    className="w-full h-20 bg-gray-700 text-foreground text-sm rounded px-3 py-2 border border-gray-600 focus:outline-none focus:border-purple-500 resize-y"
                  />
                </div>
                <p className="text-xs text-gray-500">
                  将使用当前 Task 的 {providerLabel} 分析对话历史，提取关键步骤、踩坑点和验证方法。可多次蒸馏，每次指定不同侧重点。
                </p>
                <div className="flex justify-end">
                  <button
                    onClick={async () => {
                      setDistilling(true);
                      setDistillError(null);
                      try {
                        const result = await api.distillTask(
                          task.id,
                          distillInstruction.trim() || undefined,
                          {
                            provider: task.provider,
                            model: task.model,
                            codex_service_tier: task.codex_service_tier,
                          },
                        );
                        setDistillResult(result);
                        setDistillName(result.suggested_name);
                        setDistillContent(result.content);
                      } catch (e) {
                        setDistillError(e instanceof Error ? e.message : 'Distill failed');
                        onTaskUpdated?.();
                      } finally {
                        setDistilling(false);
                      }
                    }}
                    className="flex items-center gap-2 px-4 py-2 text-sm text-white bg-purple-600 rounded-lg hover:bg-purple-700"
                  >
                    <Sparkles size={14} />
                    Start Distill
                  </button>
                </div>
              </div>
            )}

            {/* State 2: Distilling — loading */}
            {distilling && (
              <div className="p-8 flex flex-col items-center gap-3">
                <Loader2 size={32} className="animate-spin text-purple-400" />
                <p className="text-sm text-gray-400">Distilling skill from conversation...</p>
                <p className="text-xs text-gray-600">This may take 30-60 seconds</p>
              </div>
            )}

            {/* State 3: Error */}
            {distillError && !distilling && (
              <div className="p-4 space-y-3">
                <div className="text-red-400 text-sm flex items-center gap-2">
                  <AlertCircle size={14} /> {distillError}
                </div>
                <div className="flex justify-end">
                  <button
                    onClick={() => setDistillError(null)}
                    className="px-3 py-1.5 text-xs text-gray-400 hover:text-gray-200 bg-gray-700 rounded hover:bg-gray-600"
                  >
                    Retry
                  </button>
                </div>
              </div>
            )}

            {/* State 4: Result — preview and save */}
            {distillResult && !distilling && (
              <>
                <div className="flex-1 overflow-auto p-4 space-y-3">
                  <div>
                    <label className="text-xs text-gray-400 mb-1 block">Skill Name</label>
                    <input
                      value={distillName}
                      onChange={(e) => setDistillName(e.target.value)}
                      className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-1.5 border border-gray-600 focus:outline-none focus:border-purple-500"
                      placeholder="Enter skill name..."
                    />
                  </div>
                  <div className="flex-1">
                    <label className="text-xs text-gray-400 mb-1 block">Content (editable)</label>
                    <textarea
                      value={distillContent}
                      onChange={(e) => setDistillContent(e.target.value)}
                      className="w-full h-80 bg-gray-700 text-foreground text-xs font-mono rounded px-3 py-2 border border-gray-600 focus:outline-none focus:border-purple-500 resize-y"
                    />
                  </div>
                </div>
                <div className="flex items-center justify-end gap-2 px-4 py-3 border-t border-gray-700">
                  <button
                    onClick={() => { setDistillOpen(false); setDistillResult(null); setDistillError(null); }}
                    className="px-3 py-1.5 text-xs text-gray-400 hover:text-gray-200 bg-gray-700 rounded hover:bg-gray-600"
                  >
                    Cancel
                  </button>
                  <button
                    onClick={async () => {
                      if (!distillName.trim()) { setDistillError('Name is required'); return; }
                      setDistillSaving(true);
                      setDistillError(null);
                      try {
                        await api.saveDistilledSkill(task.id, { name: distillName.trim(), content: distillContent, description: `Distilled from task #${task.id}` });
                        setDistillOpen(false);
                        setDistillResult(null);
                      } catch (e) {
                        setDistillError(e instanceof Error ? e.message : 'Save failed');
                      } finally {
                        setDistillSaving(false);
                      }
                    }}
                    disabled={distillSaving || !distillName.trim()}
                    className="flex items-center gap-1.5 px-3 py-1.5 text-xs text-white bg-purple-600 rounded hover:bg-purple-700 disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    {distillSaving ? <Loader2 size={12} className="animate-spin" /> : <Sparkles size={12} />}
                    Save as Skill
                  </button>
                </div>
              </>
            )}
          </div>
        </div>
      )}

      {/* Interrupting banner */}
      {interrupting && (
        <div className="flex items-center gap-2 px-4 py-2 bg-yellow-500/10 border-b border-yellow-500/30 text-yellow-400 text-xs">
          <Loader2 size={14} className="animate-spin" />
          Interrupting {providerLabel}... waiting for graceful shutdown
        </div>
      )}

      {/* Load older messages banner — fixed above scroll area */}
      {messages.length > 0 && hasMoreHistory && (
        <div className="flex justify-center py-1.5 border-b border-gray-800 bg-gray-950/80 shrink-0">
          <button
            onClick={loadMoreHistory}
            disabled={loadingMore}
            className="text-xs text-gray-400 hover:text-gray-200 px-3 py-1 rounded-full bg-gray-800 hover:bg-gray-700 transition-colors disabled:opacity-50 flex items-center gap-1.5"
          >
            {loadingMore ? <Loader2 size={12} className="animate-spin" /> : <ChevronUp size={12} />}
            {loadingMore ? 'Loading...' : 'Load older messages'}
          </button>
        </div>
      )}

      {/* Messages */}
      <div ref={messagesContainerRef} className="flex-1 overflow-y-auto overscroll-contain p-4 space-y-3 min-h-0">
        {messages.length === 0 && historyLoading && (
          <div className="flex items-center justify-center gap-2 text-gray-500 mt-20">
            <Loader2 size={16} className="animate-spin" />
            <span className="text-sm">Loading chat history...</span>
          </div>
        )}
        {messages.length === 0 && !historyLoading && (
          <div className="text-center text-gray-600 mt-20">
            <p className="text-lg mb-2">Chat with this task</p>
            <p className="text-sm">
              {task.session_id
                ? 'Send a follow-up message to continue the conversation'
                : 'This task has no session yet. Run it first via Ralph Loop or manually.'}
            </p>
          </div>
        )}
        {/* Initial prompt bubble */}
        {task.description && (
          <div data-user-msg>
            <div className="text-center text-xs text-gray-600 py-1 mb-1">— Initial Prompt —</div>
            <div className="flex justify-end">
              <div className="max-w-[85%] group">
                <div className="rounded-2xl px-4 py-2.5 text-sm bg-indigo-600 text-white rounded-br-md shadow-md shadow-indigo-600/10">
                  {task.metadata_?.attachments && task.metadata_.attachments.length > 0 && (
                    <div className="mb-2 flex flex-wrap gap-2">
                      {task.metadata_.attachments.filter((a) => a.is_image).length > 0 && (
                        <MessageImages urls={task.metadata_.attachments.filter((a) => a.is_image).map((a) => a.url)} />
                      )}
                      {task.metadata_.attachments.filter((a) => !a.is_image).map((a, i) => (
                        <a key={i} href={resolveAssetUrl(a.url)} target="_blank" rel="noopener noreferrer"
                          className="flex items-center gap-1.5 px-3 py-1.5 bg-indigo-500/30 rounded-lg text-xs text-indigo-100 hover:bg-indigo-500/40 transition-colors max-w-[200px]"
                        >
                          <Paperclip size={12} className="shrink-0" />
                          <span className="truncate">{a.name}</span>
                        </a>
                      ))}
                    </div>
                  )}
                  <ExpandableText
                    text={task.description!}
                    collapsedLines={6}
                    className="whitespace-pre-wrap text-white"
                    expandedClassName="whitespace-pre-wrap text-white"
                  />
                </div>
                <div className="flex items-center justify-end gap-1 mt-0.5 pr-1">
                  {task.created_at && <MessageTimestamp timestamp={task.created_at} />}
                  <MessageCopyButton text={task.description} />
                </div>
              </div>
            </div>
          </div>
        )}
        {grouped.map((group, i) =>
          group.type === 'tool-group' ? (
            <ToolGroup
              key={i}
              messages={group.messages}
              taskId={task.id}
            />
          ) : (
            <MessageBubble
              key={group.message.id}
              message={group.message}
              taskId={task.id}
              onAskUserResolved={markAskUserResolved}
            />
          )
        )}
        {isProcessing && (
          <div className="flex gap-2 items-center text-gray-500 text-sm px-3">
            <Loader2 size={14} className="animate-spin" />
            <span>{providerLabel} is thinking...</span>
          </div>
        )}
        <div ref={bottomRef} className="h-4" />
      </div>

      {/* Error */}
      {error && (
        <div className="mx-4 mb-2 px-3 py-2 bg-red-500/10 border border-red-500/30 rounded text-sm text-red-400">
          {error}
        </div>
      )}
      {dropError && (
        <div className="mx-4 mb-2 px-3 py-2 bg-yellow-500/10 border border-yellow-500/30 rounded text-sm text-yellow-400">
          {dropError}
        </div>
      )}

      {/* Message Queue Display */}
      {messageQueue.length > 0 && (
        <div className="border-t border-gray-800 bg-gray-900/50 px-4 py-2">
          <div className="max-w-3xl mx-auto">
            <div className="flex items-center justify-between mb-1.5">
              <span className="text-xs text-amber-400 font-medium flex items-center gap-1.5">
                <ListPlus size={12} />
                Queued messages ({messageQueue.length})
              </span>
              <div className="flex items-center gap-2">
                <button
                  onClick={mergeQueueToInput}
                  className="inline-flex items-center gap-1 text-xs text-gray-500 hover:text-amber-300 transition-colors"
                  title="Merge queued messages into input"
                >
                  <Copy size={11} />
                  Merge
                </button>
                <button
                  onClick={clearMessageQueue}
                  className="text-xs text-gray-500 hover:text-red-400 transition-colors"
                >
                  Clear all
                </button>
              </div>
            </div>
            <div className="space-y-1 max-h-32 overflow-y-auto">
              {messageQueue.map((item, idx) => (
                <div key={idx} className="flex items-center gap-1.5 group/q">
                  <span className="text-[10px] text-gray-600 w-4 text-right shrink-0">{idx + 1}</span>
                  <div className="flex-1 min-w-0 bg-gray-800/60 rounded px-2.5 py-1 text-xs text-gray-300 truncate flex items-center gap-1.5">
                    {item.planTaskIds && item.planTaskIds.length > 0 && (
                      <span
                        className="inline-flex shrink-0 items-center gap-0.5 text-indigo-300"
                        title={`Plans: ${item.planTaskIds.map((id) => `#${id}`).join(', ')}`}
                      >
                        <ListTodo size={10} />
                        <span className="text-[10px]">{item.planTaskIds.length}</span>
                      </span>
                    )}
                    {item.uploadResults && item.uploadResults.length > 0 && (
                      <span className="inline-flex items-center gap-0.5 text-amber-400 shrink-0" title={item.uploadResults.map(r => r.filename).join(', ')}>
                        <Paperclip size={10} />
                        <span className="text-[10px]">{item.uploadResults.length}</span>
                      </span>
                    )}
                    <span className="truncate">{item.text}</span>
                  </div>
                  <div className="flex items-center gap-0.5 opacity-0 group-hover/q:opacity-100 transition-opacity shrink-0">
                    <button
                      onClick={() => editQueueItem(idx)}
                      className="p-0.5 text-gray-500 hover:text-amber-300"
                      title="Edit in input"
                    >
                      <Pencil size={12} />
                    </button>
                    <button
                      onClick={() => moveQueueItem(idx, 'up')}
                      disabled={idx === 0}
                      className="p-0.5 text-gray-500 hover:text-gray-300 disabled:opacity-30"
                      title="Move up"
                    >
                      <ChevronDown size={12} className="rotate-180" />
                    </button>
                    <button
                      onClick={() => moveQueueItem(idx, 'down')}
                      disabled={idx === messageQueue.length - 1}
                      className="p-0.5 text-gray-500 hover:text-gray-300 disabled:opacity-30"
                      title="Move down"
                    >
                      <ChevronDown size={12} />
                    </button>
                    <button
                      onClick={() => removeFromQueue(idx)}
                      className="p-0.5 text-gray-500 hover:text-red-400"
                      title="Remove"
                    >
                      <Trash2 size={12} />
                    </button>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* Input */}
      <div className="border-t border-gray-800 bg-gray-900 p-3">
        <div className="flex flex-col gap-2 max-w-3xl mx-auto">
          {selectedPlanVersionIds.length > 0 && (
            <div className="flex flex-wrap items-center gap-1.5">
              <span className="inline-flex items-center gap-1 text-[10px] font-medium uppercase tracking-wide text-indigo-300">
                <ListTodo size={11} /> Next message
              </span>
              {selectedPlanVersionIds.map((versionId) => {
                const plan = versionedPlans.find((item) => item.current_version?.id === versionId);
                return (
                  <span key={versionId} className="inline-flex max-w-[240px] items-center gap-1 rounded-full border border-indigo-500/40 bg-indigo-500/10 px-2 py-1 text-[11px] text-indigo-200" title={plan?.title || `Plan Version #${versionId}`}>
                    <span className="truncate">Plan #{plan?.id || '?'} · v{plan?.current_version?.version_number || '?' }{plan?.title ? ` · ${plan.title}` : ''}</span>
                    <button type="button" onClick={() => togglePlanVersionAttachment(versionId)} className="shrink-0 text-indigo-300 hover:text-white" aria-label={`Detach Plan Version #${versionId}`}><X size={10} /></button>
                  </span>
                );
              })}
              <span className="text-[10px] text-gray-500">applied only when this message is sent</span>
            </div>
          )}
          {/* File preview strip */}
          {(forkSeedUploads.length > 0 || fileUpload.uploads.length > 0) && (
            <div className="flex gap-2 flex-wrap">
              {forkSeedUploads.map((upload) => (
                <div key={upload.id} className="relative rounded overflow-hidden border border-indigo-500/60">
                  {upload.is_image ? (
                    <div className="w-14 h-14">
                      <img src={resolveAssetUrl(upload.url)} alt="" className="w-full h-full object-cover" />
                    </div>
                  ) : (
                    <div className="flex items-center gap-1.5 px-2.5 py-1.5 bg-gray-800 text-xs text-gray-300 max-w-[150px]">
                      <Paperclip size={12} className="shrink-0" />
                      <span className="truncate">{upload.filename || upload.url.split('/').pop()}</span>
                    </div>
                  )}
                  <button
                    type="button"
                    aria-label={`Remove ${upload.filename || 'fork attachment'}`}
                    onClick={() => setForkSeedUploads((prev) => prev.filter((item) => item.id !== upload.id))}
                    disabled={injecting}
                    className="absolute top-0 right-0 bg-gray-900/80 rounded-bl p-0.5 text-gray-300 hover:text-foreground"
                  >
                    <X size={10} />
                  </button>
                </div>
              ))}
              {fileUpload.uploads.map((upload) => {
                const preview = upload.preview || (
                  upload.result?.is_image
                    ? resolveAssetUrl(upload.result.url)
                    : ''
                );
                const filename = (
                  upload.file?.name
                  || upload.result?.filename
                  || upload.result?.url.split('/').pop()
                  || 'attachment'
                );
                return (
                <div key={upload.id} className="relative rounded overflow-hidden border border-gray-600">
                  {preview ? (
                    <div className="w-14 h-14">
                      <img src={preview} alt={filename} className="w-full h-full object-cover" />
                    </div>
                  ) : (
                    <div className="flex items-center gap-1.5 px-2.5 py-1.5 bg-gray-800 text-xs text-gray-300 max-w-[150px]">
                      <Paperclip size={12} className="shrink-0" />
                      <span className="truncate">{filename}</span>
                    </div>
                  )}
                  {upload.status === 'uploading' && (
                    <div className="absolute inset-0 bg-black/50 flex items-center justify-center">
                      <Loader2 size={16} className="animate-spin text-white" />
                    </div>
                  )}
                  {upload.status === 'failed' && (
                    <div
                      className={`absolute inset-0 bg-red-900/50 flex items-center justify-center ${injecting ? 'cursor-not-allowed' : 'cursor-pointer'}`}
                      onClick={() => {
                        if (!injecting) fileUpload.retryFile(upload.id);
                      }}
                      title={injecting ? 'Injection in progress' : 'Click to retry'}
                    >
                      <AlertCircle size={16} className="text-red-400" />
                    </div>
                  )}
                  <button
                    type="button"
                    aria-label={`Remove ${filename}`}
                    onClick={() => fileUpload.removeFile(upload.id)}
                    disabled={injecting}
                    className="absolute top-0 right-0 bg-gray-900/80 rounded-bl p-0.5 text-gray-300 hover:text-foreground"
                  >
                    <X size={10} />
                  </button>
                </div>
              );
              })}
            </div>
          )}
          <div className="space-y-1.5">
          {/* Row 1: action buttons */}
          <div className="flex gap-1 items-center">
            <input
              ref={fileInputRef}
              type="file"
              multiple
              disabled={injecting}
              className="hidden"
              onChange={handleFileSelect}
            />
            <button
              type="button"
              onClick={() => fileInputRef.current?.click()}
              disabled={
                injecting
                || (!task.session_id && !task.shared_from_id)
                || fileUpload.uploads.length + forkSeedUploads.length >= MAX_FILES
              }
              className="p-2 text-gray-500 hover:text-gray-300 disabled:opacity-40 disabled:cursor-not-allowed"
              title="Attach files"
            >
              <Paperclip size={18} />
            </button>
            <SecretPicker selectedIds={selectedSecretIds} onChange={setSelectedSecretIds} disabled={injecting || (!task.session_id && !task.shared_from_id) || (injectMode && canInject)} />
            <QuickPhraseDropdown onSelect={(text) => handleSend(text)} disabled={injecting || (!task.session_id && !task.shared_from_id)} />
            {/* Temp model override (one-shot) */}
            <div className="relative" data-temp-model>
              <button
                type="button"
                onClick={() => setShowModelMenu((v) => !v)}
                disabled={injecting || (!task.session_id && !task.shared_from_id)}
                className={`p-2 rounded-lg transition-colors disabled:opacity-40 ${
                  modelOverride ? 'text-indigo-300 bg-indigo-600/20' : 'text-gray-500 hover:text-gray-300'
                }`}
                title={modelOverride ? `下一条消息用 ${modelOverride}（点击更换）` : '临时切换模型（仅下一条消息）'}
              >
                <ListFilter size={18} />
              </button>
              {showModelMenu && (
                <div className="absolute bottom-full mb-1 left-0 bg-gray-800 border border-gray-600 rounded shadow-lg z-30 min-w-[200px] py-1 max-h-60 overflow-y-auto">
                  <div className="px-3 py-1 text-[10px] text-gray-500 uppercase tracking-wider">下一条消息使用</div>
                  <button
                    onClick={() => { setModelOverride(null); setShowModelMenu(false); }}
                    className={`w-full px-3 py-1.5 text-xs text-left hover:bg-gray-700 ${!modelOverride ? 'text-indigo-300 bg-indigo-600/20' : 'text-gray-300'}`}
                  >
                    默认（{task.model || 'default'}）
                  </button>
                  {modelOptions.map((m) => {
                    // Known fixed windows come from the backend capability table.
                    const win = modelContextWindows[m]
                      ?? ((m.includes('[1m]') || m.includes('fable')) ? 1_000_000 : 200_000);
                    const over = !!contextUsage && contextUsage.total_input_tokens > win;
                    const fastUnsupported = task.provider === 'codex'
                      && task.codex_service_tier === 'priority'
                      && !(codexModelServiceTiers[m] || []).includes('priority');
                    return (
                    <button
                      key={m}
                      disabled={fastUnsupported}
                      onClick={() => { setModelOverride(m === task.model ? null : m); setShowModelMenu(false); }}
                      className={`w-full px-3 py-1.5 text-xs text-left hover:bg-gray-700 flex items-center justify-between gap-2 disabled:cursor-not-allowed disabled:text-gray-600 disabled:hover:bg-transparent ${modelOverride === m ? 'text-indigo-300 bg-indigo-600/20' : over ? 'text-amber-400/80' : 'text-gray-300'}`}
                      title={fastUnsupported
                        ? `${m} 不支持 Fast；先在 Task Config 中切换为 Standard`
                        : over
                          ? `当前上下文（${Math.round(contextUsage!.total_input_tokens/1000)}K tokens）可能超出该模型 ${win/1000}K 窗口，会报 Prompt is too long`
                          : undefined}
                    >
                      <span>{m}</span>
                      {fastUnsupported ? <span className="shrink-0">需 Standard</span> : over && <span className="shrink-0">⚠</span>}
                    </button>
                  );})}
                </div>
              )}
            </div>
            {/* Live-turn injection: Claude PTY or Codex app-server steering. */}
            {canInject && (
              <button
                type="button"
                onClick={() => setInjectMode((v) => !v)}
                disabled={injecting || !task.session_id}
                className={`p-2 rounded-lg transition-colors disabled:opacity-40 ${
                  injectMode ? 'text-teal-300 bg-teal-600/20' : 'text-gray-500 hover:text-teal-300'
                }`}
                title={injectMode
                  ? `注入模式已开启：消息将通过 ${injectTransport} 插入运行中的 turn（点击关闭）`
                  : `开启注入模式：通过 ${injectTransport} 插入运行中的 turn（不开新 turn）`}
              >
                <Syringe size={18} />
              </button>
            )}
            {task.provider === 'codex' && task.session_id && task.worker_id == null && task.shared_from_id == null && (
              <ForkButton onClick={openFork} disabled={isProcessing} />
            )}
            {/* Message navigation — always visible, right-aligned */}
            <div className="ml-auto flex items-center gap-0.5">
              <button
                onClick={() => navigateUserMessage('up')}
                className="p-1.5 text-gray-500 hover:text-gray-300 rounded transition-colors"
                title="Previous user message"
              >
                <ChevronUp size={16} />
              </button>
              <button
                onClick={() => navigateUserMessage('down')}
                className="p-1.5 text-gray-500 hover:text-gray-300 rounded transition-colors"
                title="Next user message"
              >
                <ChevronDown size={16} />
              </button>
              <button
                onClick={() => { const c = messagesContainerRef.current; if (c) c.scrollTo({ top: c.scrollHeight, behavior: 'smooth' }); }}
                className="p-1.5 text-gray-500 hover:text-gray-300 rounded transition-colors"
                title="Scroll to bottom"
              >
                <ArrowDown size={16} />
              </button>
            </div>
          </div>
          {/* Row 2: full-width input */}
          {injectMode && canInject && (
            <div className="text-[10px] leading-relaxed text-teal-300/80">
              文本、图片和文件会注入当前 turn；只有服务器明确确认成功后才会清空输入和附件。
            </div>
          )}
          <div className="flex gap-2 items-end">
            <textarea
              ref={textareaRef}
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder={
                !task.session_id && !task.shared_from_id
                  ? 'Run the task first to start a session...'
                  : injectMode && canInject
                    ? '注入模式：消息将直接注入运行中的 turn...'
                    : isProcessing
                      ? 'Type next message to queue...'
                      : 'Type a follow-up message...'
              }
              disabled={injecting || (!task.session_id && !task.shared_from_id)}
              rows={1}
              className="flex-1 bg-gray-800 text-foreground rounded-xl px-4 py-2.5 text-sm border border-gray-700/70 focus:outline-none focus:border-indigo-500 focus:ring-2 focus:ring-indigo-500/25 resize-none disabled:opacity-50 max-h-48 overflow-y-auto transition-colors"
              style={{ minHeight: '40px' }}
            />
            <button
              onClick={() => handleSend()}
              disabled={(!input.trim() && fileUpload.uploadedResults.length === 0 && forkSeedUploads.length === 0) || (!task.session_id && !task.shared_from_id) || (injectMode && canInject && !isProcessing) || injecting || fileUpload.isUploading || fileUpload.hasFailed}
              title={fileUpload.hasFailed
                ? 'Retry or remove failed attachments before sending'
                : injectMode && canInject
                ? (isProcessing ? '注入到运行中的 turn (Ctrl+Enter)' : '注入模式：仅在 turn 运行中可用，空闲时请关闭注入模式')
                : isProcessing ? 'Add to queue (Ctrl+Enter)' : 'Send (Ctrl+Enter)'}
              className={`p-2.5 text-white rounded-xl transition-colors disabled:opacity-40 disabled:cursor-not-allowed shadow-md ${
                injectMode && canInject ? 'bg-teal-600 hover:bg-teal-700 shadow-teal-600/20'
                : isProcessing ? 'bg-amber-600 hover:bg-amber-700 shadow-amber-600/20' : 'bg-indigo-600 hover:bg-indigo-500 shadow-indigo-600/25'
              }`}
            >
              {injectMode && canInject ? <Syringe size={18} /> : isProcessing ? <ListPlus size={18} /> : <Send size={18} />}
            </button>
          </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function CollapsibleContent({ content, maxLines = 5 }: { content: string; maxLines?: number }) {
  const [expanded, setExpanded] = useState(false);
  const lines = content.split('\n');
  const shouldCollapse = lines.length > maxLines;

  if (!shouldCollapse) {
    return (
      <pre className="text-gray-400 whitespace-pre-wrap text-xs overflow-x-auto">{content}</pre>
    );
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
    // For common tools, show a readable format
    if (parsed.command) return parsed.command; // Bash
    if (parsed.file_path && parsed.old_string !== undefined) {
      // Edit tool
      return `File: ${parsed.file_path}\n--- old ---\n${parsed.old_string}\n+++ new +++\n${parsed.new_string}`;
    }
    if (parsed.file_path && parsed.content !== undefined) {
      // Write tool
      return `File: ${parsed.file_path}\n${parsed.content}`;
    }
    if (parsed.file_path) return `File: ${parsed.file_path}`; // Read
    if (parsed.pattern) return `Pattern: ${parsed.pattern}${parsed.path ? ` in ${parsed.path}` : ''}`; // Grep/Glob
    return JSON.stringify(parsed, null, 2);
  } catch {
    return input;
  }
}

/** Extract a short one-line summary for a tool_use message.
 *  In compact mode, tool_input is already a plain summary string from the backend.
 *  In full mode, tool_input is the original JSON. */
function toolUseSummary(msg: ChatMessage): string {
  if (!msg.tool_input) return '';
  // compact mode: backend already returns a plain-text summary (not JSON)
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

function ToolGroup({
  messages,
  taskId,
}: {
  messages: ChatMessage[];
  taskId: number;
}) {
  const [expanded, setExpanded] = useState(false);
  const hasError = messages.some((m) => m.is_error);
  const toolUseCount = messages.filter((m) => m.event_type === 'tool_use').length;

  return (
    <div className="group mx-4">
      <div className="flex items-center gap-1">
        <button
          onClick={() => setExpanded(!expanded)}
          className={`flex items-center gap-1.5 text-xs py-1 hover:text-gray-400 transition-colors ${hasError ? 'text-red-400/70' : 'text-gray-600'}`}
        >
          {expanded ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
          <span>
            {hasError ? '⚠' : '🔧'} {toolUseCount} tool call{toolUseCount !== 1 ? 's' : ''}
          </span>
        </button>
      </div>
      {expanded && (
        <div className="ml-3 border-l border-gray-800 pl-3 space-y-1 mt-1">
          {messages.map((msg) => (
            <ToolItem key={msg.id} message={msg} taskId={taskId} />
          ))}
        </div>
      )}
    </div>
  );
}

function ToolItem({ message, taskId }: { message: ChatMessage; taskId: number }) {
  const [expanded, setExpanded] = useState(false);
  const [detail, setDetail] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const isToolUse = message.event_type === 'tool_use';
  const toolName = message.tool_name || (isToolUse ? 'tool' : 'result');

  // Check if we already have full content (from WebSocket live messages, not compact)
  const hasInlineDetail = isToolUse
    ? !!(message.tool_input && (message.tool_input.startsWith('{') || message.tool_input.startsWith('[')))
    : !!(message.tool_output || message.content);

  const getInlineDetail = (): string | null => {
    if (isToolUse && message.tool_input) return formatToolInput(message.tool_input);
    if (!isToolUse && (message.tool_output || message.content)) return message.tool_output || message.content;
    return message.content || null;
  };

  const handleExpand = async () => {
    if (expanded) {
      setExpanded(false);
      return;
    }
    setExpanded(true);
    if (hasInlineDetail) {
      setDetail(getInlineDetail());
      return;
    }
    // Lazy-load from backend
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

  // tool_result
  const statusIcon = message.is_error ? '✗' : '✓';
  const statusColor = message.is_error ? 'text-red-400' : 'text-green-600';
  return (
    <div>
      <button
        onClick={handleExpand}
        className="flex items-center gap-1.5 text-xs text-gray-600 hover:text-gray-400 py-0.5"
      >
        {expanded ? <ChevronDown size={10} className="shrink-0" /> : <ChevronRight size={10} className="shrink-0" />}
        <span className={statusColor}>{statusIcon}</span>
        <span className="text-gray-600">{toolName}</span>
      </button>
      {expanded && (loading
        ? <div className="ml-4 mt-1 mb-1 text-xs text-gray-600">Loading...</div>
        : detail && <div className="ml-4 mt-1 mb-1"><CollapsibleContent content={detail} /></div>
      )}
    </div>
  );
}

function MessageCopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);
  const handleCopy = () => {
    copyToClipboard(text).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  };
  return (
    <button
      onClick={handleCopy}
      className="copy-btn opacity-0 group-hover:opacity-100 pointer-events-none group-hover:pointer-events-auto p-1 rounded hover:bg-gray-700/60 text-gray-600 hover:text-gray-400 transition-opacity"
      title="Copy message"
    >
      {copied ? <Check size={14} /> : <Copy size={14} />}
    </button>
  );
}

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);
  const handleCopy = () => {
    copyToClipboard(text).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  };
  return (
    <button
      onClick={handleCopy}
      className="copy-btn pointer-events-none absolute right-2 top-2 rounded bg-gray-700/80 p-1 text-gray-400 opacity-0 transition-opacity hover:bg-gray-600 hover:text-gray-200 group-hover:pointer-events-auto group-hover:opacity-100"
      title="Copy"
    >
      {copied ? <Check size={12} /> : <Copy size={12} />}
    </button>
  );
}

function ForkButton({ onClick, disabled = false }: { onClick: () => void; disabled?: boolean }) {
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      className="inline-flex h-8 w-8 items-center justify-center rounded-md text-gray-500 transition-colors hover:bg-indigo-500/10 hover:text-indigo-400 focus-visible:text-indigo-400 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-indigo-400 disabled:cursor-not-allowed disabled:opacity-40"
      title={disabled ? '当前 Codex turn 结束后才能分叉' : '从一条用户消息之前的上下文创建 Fork'}
      aria-label="Fork Codex session"
    >
      <GitBranch size={16} />
    </button>
  );
}

function stripSenderPrefix(text: string): string {
  return text.replace(/^\[[^\]\r\n]+\][ \t]+/, '');
}

const taskRemarkPlugins = [remarkTaskArtifactPaths];
const markdownComponents: Components = {
  pre({ children }) {
    let codeText = '';
    if (children && typeof children === 'object' && 'props' in (children as React.ReactElement)) {
      const codeEl = children as React.ReactElement<{ children?: React.ReactNode }>;
      codeText = typeof codeEl.props.children === 'string' ? codeEl.props.children : '';
    }
    return (
      <div className="relative group my-2">
        {codeText && <CopyButton text={codeText} />}
        <pre className="bg-gray-900 rounded-lg p-3 overflow-x-auto text-xs">{children}</pre>
      </div>
    );
  },
  code({ className: codeClassName, children, ...props }) {
    const isInline = !codeClassName;
    if (isInline) {
      return <code className="bg-gray-700/60 px-1.5 py-0.5 rounded text-xs" {...props}>{children}</code>;
    }
    return <code className={`${codeClassName || ''} text-xs`} {...props}>{children}</code>;
  },
  a({ href, children }) {
    return <a href={href} target="_blank" rel="noopener noreferrer" className="text-indigo-400 hover:text-indigo-300 underline">{children}</a>;
  },
  table({ children }) {
    return <div className="overflow-x-auto my-2"><table className="border-collapse text-xs w-full">{children}</table></div>;
  },
  th({ children }) {
    return <th className="border border-gray-700 px-2 py-1 bg-gray-800/50 text-left">{children}</th>;
  },
  td({ children }) {
    return <td className="border border-gray-700 px-2 py-1">{children}</td>;
  },
};

const MarkdownContent = memo(function MarkdownContent({
  content,
  taskId,
  className,
}: {
  content: string;
  taskId?: number;
  className?: string;
}) {
  const taskComponents = useMemo<Components>(() => ({
    ...markdownComponents,
    a({ href, title, children }) {
      return taskId == null
        ? <a href={href} target="_blank" rel="noopener noreferrer" className="text-indigo-400 hover:text-indigo-300 underline">{children}</a>
        : <TaskArtifactLink taskId={taskId} href={href} linkTitle={title}>{children}</TaskArtifactLink>;
    },
  }), [taskId]);
  return (
    <div className={`markdown-body ${className || ''}`}>
      <MarkdownRenderer
        content={content}
        components={taskComponents}
        remarkPlugins={taskRemarkPlugins}
      />
    </div>
  );
});
function MessageTimestamp({ timestamp, className }: { timestamp: string | null; className?: string }) {
  if (!timestamp) return null;
  return (
    <span className={`text-[10px] text-gray-600 select-none ${className || ''}`}>
      {formatMessageTime(timestamp)}
    </span>
  );
}

function AppliedPlansInMessage({ plans }: { plans: AppliedPlanSnapshot[] }) {
  return (
    <div className="applied-plan-message mt-2 space-y-1.5 border-t border-white/25 pt-2">
      {plans.map((plan) => (
        <details
          key={plan.id}
          className="rounded-lg border border-white/25 bg-black/15 px-2.5 py-1.5"
        >
          <summary className="cursor-pointer select-none text-xs font-medium text-white marker:text-white/70">
            Applied Plan #{plan.id}: {plan.title}
          </summary>
          <div className="applied-plan-content mt-2 max-h-80 overflow-y-auto rounded-md bg-transparent p-2.5">
            <MarkdownContent
              content={plan.content}
              className="text-xs text-white"
            />
          </div>
        </details>
      ))}
    </div>
  );
}

function ImageLightbox({ src, onClose }: { src: string; onClose: () => void }) {
  return (
    <div className="fixed inset-0 z-[9999] bg-black/80 flex items-center justify-center" onClick={onClose}>
      <button onClick={onClose} className="absolute top-4 right-4 text-white/70 hover:text-white text-3xl font-light">&times;</button>
      <img src={src} alt="" className="max-w-[90vw] max-h-[90vh] object-contain rounded-lg" onClick={(e) => e.stopPropagation()} />
    </div>
  );
}

function MessageImages({ urls }: { urls: string[] }) {
  const [lightboxSrc, setLightboxSrc] = useState<string | null>(null);
  return (
    <>
      <div className="flex flex-wrap gap-2">
        {urls.map((rawUrl, i) => {
          const url = resolveAssetUrl(rawUrl);
          return (
          <img
            key={i}
            src={url}
            alt=""
            className="max-w-[200px] max-h-[150px] rounded-lg object-cover cursor-pointer hover:opacity-80 transition-opacity"
            onClick={() => setLightboxSrc(url)}
          />
          );
        })}
      </div>
      {lightboxSrc && <ImageLightbox src={lightboxSrc} onClose={() => setLightboxSrc(null)} />}
    </>
  );
}

/** 权限透传卡片：CC 在 PTY 里请求权限 → 用户点 允许/拒绝 回包。
 * CC 侧最多等 120s，超时默认拒绝；过期点击会得到 410 并标记过期。
 * 历史消息没有 request_id（只入库描述），渲染为只读。 */
function PermissionCard({ message, taskId }: { message: ChatMessage; taskId?: number }) {
  const [submitting, setSubmitting] = useState(false);
  const [localStatus, setLocalStatus] = useState<string | null>(null);
  const status = localStatus || message.permission_status || (message.request_id ? 'pending' : 'expired');
  const actionable = status === 'pending' && !!message.request_id && taskId !== undefined;

  const decide = async (behavior: 'allow' | 'deny') => {
    if (!actionable || submitting) return;
    setSubmitting(true);
    try {
      await api.resolvePermission(taskId!, message.request_id!, behavior);
      setLocalStatus(behavior);
    } catch {
      setLocalStatus('expired');
    } finally {
      setSubmitting(false);
    }
  };

  const statusBadge: Record<string, { text: string; cls: string }> = {
    allow: { text: '✓ 已允许', cls: 'text-emerald-400' },
    deny: { text: '✕ 已拒绝', cls: 'text-red-400' },
    expired: { text: '⏱ 已过期（CC 侧默认拒绝）', cls: 'text-gray-500' },
  };

  return (
    <div className="mx-4">
      <div className="px-3 py-2.5 bg-amber-500/10 border border-amber-500/40 rounded-lg text-sm">
        <div className="flex items-center gap-2 text-amber-300 font-medium">
          <span>🔐</span>
          <span>权限请求{message.tool_name ? `：${message.tool_name}` : ''}</span>
          {message.timestamp && (
            <MessageTimestamp timestamp={message.timestamp} className="ml-auto" />
          )}
        </div>
        {message.content && (
          <div className="mt-1 text-gray-300">{message.content}</div>
        )}
        {message.tool_input && (
          <pre className="mt-1.5 px-2 py-1.5 bg-gray-900/60 rounded text-xs text-gray-400 whitespace-pre-wrap break-all max-h-32 overflow-y-auto">{message.tool_input}</pre>
        )}
        <div className="mt-2 flex items-center gap-2">
          {actionable ? (
            <>
              <button
                onClick={() => decide('allow')}
                disabled={submitting}
                className="px-3 py-1 text-xs rounded bg-emerald-600 hover:bg-emerald-500 text-white disabled:opacity-50"
              >
                允许
              </button>
              <button
                onClick={() => decide('deny')}
                disabled={submitting}
                className="px-3 py-1 text-xs rounded bg-red-600/80 hover:bg-red-500 text-white disabled:opacity-50"
              >
                拒绝
              </button>
              <span className="text-xs text-gray-500">120s 内有效，超时默认拒绝</span>
            </>
          ) : (
            <span className={`text-xs ${statusBadge[status]?.cls || 'text-gray-500'}`}>
              {statusBadge[status]?.text || status}
            </span>
          )}
        </div>
      </div>
    </div>
  );
}

function AskUserCard({
  message,
  taskId,
  onResolved,
}: {
  message: ChatMessage;
  taskId?: number;
  onResolved?: (requestId: string, status: 'answered' | 'expired') => void;
}) {
  const questions = message.ask_questions || [];
  const [submitting, setSubmitting] = useState(false);
  const [localStatus, setLocalStatus] = useState<string | null>(null);
  // 每个问题的选中 label 集合 + 自定义文本
  const [selected, setSelected] = useState<Record<number, Set<string>>>({});
  const [custom, setCustom] = useState<Record<number, string>>({});

  const status = localStatus || message.ask_status || (message.request_id ? 'pending' : 'expired');
  const actionable = status === 'pending' && !!message.request_id && taskId !== undefined;

  const toggle = (qi: number, label: string, multi: boolean) => {
    setSelected((prev) => {
      const cur = new Set(prev[qi] || []);
      if (multi) {
        if (cur.has(label)) cur.delete(label);
        else cur.add(label);
      } else {
        cur.clear();
        cur.add(label);
      }
      return { ...prev, [qi]: cur };
    });
  };

  const submit = async () => {
    if (!actionable || submitting) return;
    const answers: AskUserAnswer[] = questions.map((_, qi) => ({
      labels: Array.from(selected[qi] || []),
      text: (custom[qi] || '').trim() || undefined,
    }));
    // 至少一个问题要有答案（label 或自定义文本）
    if (!answers.some((a) => a.labels.length || a.text)) return;
    setSubmitting(true);
    try {
      await api.submitAskUser(taskId!, message.request_id!, answers);
      setLocalStatus('answered');
      onResolved?.(message.request_id!, 'answered');
    } catch {
      setLocalStatus('expired');
      onResolved?.(message.request_id!, 'expired');
    } finally {
      setSubmitting(false);
    }
  };

  const statusBadge: Record<string, { text: string; cls: string }> = {
    answered: { text: '✓ 已回答', cls: 'text-emerald-400' },
    timed_out: { text: '⏱ 已超时（已放行原生工具）', cls: 'text-gray-500' },
    expired: { text: '⏱ 已过期', cls: 'text-gray-500' },
  };

  return (
    <div className="mx-4">
      <div className="px-3 py-2.5 bg-sky-500/10 border border-sky-500/40 rounded-lg text-sm">
        <div className="flex items-center gap-2 text-sky-300 font-medium">
          <span>💬</span>
          <span>需要你的选择</span>
          {message.timestamp && (
            <MessageTimestamp timestamp={message.timestamp} className="ml-auto" />
          )}
        </div>
        {questions.map((q, qi) => {
          const multi = !!q.multiSelect;
          const sel = selected[qi] || new Set<string>();
          return (
            <div key={qi} className="mt-2">
              <div className="text-gray-200">{q.question}</div>
              <div className="mt-1.5 flex flex-col gap-1">
                {q.options.map((opt) => {
                  const checked = sel.has(opt.label);
                  return (
                    <button
                      key={opt.label}
                      onClick={() => actionable && toggle(qi, opt.label, multi)}
                      disabled={!actionable}
                      className={`text-left px-2.5 py-1.5 rounded border text-xs transition-colors disabled:opacity-60 ${
                        checked
                          ? 'bg-sky-600/30 border-sky-500 text-sky-100'
                          : 'bg-gray-900/40 border-gray-700 text-gray-300 hover:border-sky-600/60'
                      }`}
                    >
                      <span className="font-medium">{multi ? (checked ? '☑' : '☐') : (checked ? '◉' : '○')} {opt.label}</span>
                      {opt.description && <span className="text-gray-500"> — {opt.description}</span>}
                    </button>
                  );
                })}
              </div>
              {actionable && (
                <input
                  type="text"
                  value={custom[qi] || ''}
                  onChange={(e) => setCustom((p) => ({ ...p, [qi]: e.target.value }))}
                  placeholder="或自定义回答…"
                  className="mt-1 w-full px-2 py-1 text-xs bg-gray-900/60 border border-gray-700 rounded text-gray-200 placeholder-gray-600 focus:border-sky-600 outline-none"
                />
              )}
            </div>
          );
        })}
        <div className="mt-2.5 flex items-center gap-2">
          {actionable ? (
            <>
              <button
                onClick={submit}
                disabled={submitting}
                className="px-3 py-1 text-xs rounded bg-sky-600 hover:bg-sky-500 text-white disabled:opacity-50"
              >
                提交
              </button>
              <span className="text-xs text-gray-500">提交后回答会喂回给模型继续</span>
            </>
          ) : (
            <span className={`text-xs ${statusBadge[status]?.cls || 'text-gray-500'}`}>
              {statusBadge[status]?.text || status}
            </span>
          )}
        </div>
      </div>
    </div>
  );
}

const MessageBubble = memo(function MessageBubble({
  message,
  taskId,
  onAskUserResolved,
}: {
  message: ChatMessage;
  taskId: number;
  onAskUserResolved?: (requestId: string, status: 'answered' | 'expired') => void;
}) {
  const isUser = message.role === 'user';

  if (message.event_type === 'permission_request') {
    return <PermissionCard message={message} taskId={taskId} />;
  }

  if (message.event_type === 'ask_user_question') {
    return (
      <AskUserCard
        message={message}
        taskId={taskId}
        onResolved={onAskUserResolved}
      />
    );
  }

  if (message.event_type === 'thinking') {
    const text = message.content || '';
    const isEncrypted = text.startsWith('[encrypted thinking');
    return (
      <div className="group mx-4 px-3 py-2 bg-gray-800/30 rounded text-xs border border-gray-700/30">
        <div className="flex items-center gap-1.5 text-gray-500">
          <span>💭</span>
          <span className="font-medium">Thinking</span>
          {message.timestamp && (
            <MessageTimestamp timestamp={message.timestamp} className="ml-auto" />
          )}
        </div>
        <div className="mt-1.5">
          {text && !isEncrypted ? (
            <CollapsibleContent content={text} maxLines={20} />
          ) : (
            <span className="text-gray-600 italic">
              {isEncrypted
                ? text
                : '[no thinking text in stream — model may have returned encrypted thinking]'}
            </span>
          )}
        </div>
      </div>
    );
  }

  if (message.event_type === 'transient_retry') {
    return (
      <div className="mx-4">
        <div className="px-3 py-2 bg-amber-500/10 border border-amber-500/30 rounded text-sm text-amber-500 flex items-center gap-2">
          <Loader2 className="w-3.5 h-3.5 shrink-0 animate-spin" />
          <span>{message.content}</span>
        </div>
        {message.timestamp && (
          <div className="mt-0.5 px-1">
            <MessageTimestamp timestamp={message.timestamp} />
          </div>
        )}
      </div>
    );
  }

  if (message.event_type === 'system_init' || message.event_type === 'process_exit' || message.event_type === 'system_event') {
    const content = message.content || 'system';
    const isMonitor = content.startsWith('[Monitor') || content.startsWith('[Agent') || content.startsWith('[Sub-Agent');
    if (isMonitor) {
      // Legacy monitor/agent system_events: render with reduced opacity
      // (new messages arrive as user_message with source=monitor/sub-agent)
      return (
        <div className="border-l-2 border-gray-600 pl-2 py-1 my-0.5 opacity-50">
          <MarkdownContent content={content} taskId={taskId} className="text-xs text-gray-500" />
          {message.timestamp && <MessageTimestamp timestamp={message.timestamp} className="mt-0.5" />}
        </div>
      );
    }
    if (message.pty_cold_start) {
      return (
        <div className="flex items-center justify-center gap-2 text-xs text-yellow-500/70 py-2">
          <svg className="animate-spin h-3 w-3" viewBox="0 0 24 24"><circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" fill="none"/><path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"/></svg>
          {content}
        </div>
      );
    }
    const label = message.event_type === 'system_init'
      ? '— Session started —'
      : message.event_type === 'process_exit'
        ? '— Done —'
        : `— ${content} —`;
    return (
      <div className="text-center text-xs text-gray-600 py-1">
        {label}
        {message.timestamp && (
          <>
            {' '}
            <MessageTimestamp timestamp={message.timestamp} />
          </>
        )}
      </div>
    );
  }

  if (message.is_error) {
    return (
      <div className="mx-4">
        <div className="px-3 py-2 bg-red-500/10 border border-red-500/30 rounded text-sm text-red-400">
          {message.content}
        </div>
        {message.timestamp && (
          <div className="mt-0.5 px-1">
            <MessageTimestamp timestamp={message.timestamp} />
          </div>
        )}
      </div>
    );
  }

  const isMonitor = message.source === 'monitor';
  const isSubAgent = message.source === 'sub-agent' || message.source === 'sub-agent:result';
  // 仅用户消息标注注入；回复不标注
  const isInjected = message.source === 'inject' && isUser;

  return (
    <div className={`flex flex-col ${isUser ? 'items-end' : 'items-start'}`} {...(isUser ? { 'data-user-msg': '' } : {})}>
      <div className="max-w-[85%] group">
        {isMonitor && !isUser && (
          <div className="flex items-center gap-1 mb-0.5 pl-1">
            <span className="text-xs bg-teal-600/30 text-teal-300 px-1.5 py-0.5 rounded">Monitor</span>
          </div>
        )}
        {isSubAgent && !isUser && (
          <div className="flex items-center gap-1 mb-0.5 pl-1">
            <span className="text-xs bg-amber-600/30 text-amber-300 px-1.5 py-0.5 rounded">Sub-Agent</span>
          </div>
        )}
        {isInjected && (
          <div className="flex items-center gap-1 mb-0.5 pr-1 justify-end">
            <span className="text-xs bg-teal-600/30 text-teal-300 px-1.5 py-0.5 rounded" title="注入到运行中的 turn">💉 注入</span>
          </div>
        )}
        <div
          className={`rounded-2xl px-4 py-2.5 text-sm ${
            isUser
              ? 'bg-indigo-600 text-white rounded-br-md whitespace-pre-wrap shadow-md shadow-indigo-600/10'
              : isMonitor
                ? 'bg-teal-900/40 text-gray-200 rounded-bl-md border border-teal-700/30'
                : isSubAgent
                  ? 'bg-amber-900/40 text-gray-200 rounded-bl-md border border-amber-700/30'
                  : 'bg-gray-800 text-gray-200 rounded-bl-md border border-gray-700/50 shadow-sm'
          }`}
        >
          {isUser ? (
            <>
              {message.attachments && message.attachments.length > 0 && (
                <div className="mb-2 flex flex-wrap gap-2">
                  {message.attachments.filter((a) => a.is_image).length > 0 && (
                    <MessageImages urls={message.attachments.filter((a) => a.is_image).map((a) => a.url)} />
                  )}
                  {message.attachments.filter((a) => !a.is_image).map((a, i) => (
                    <a key={i} href={resolveAssetUrl(a.url)} target="_blank" rel="noopener noreferrer"
                      className="flex items-center gap-1.5 px-3 py-1.5 bg-indigo-500/30 rounded-lg text-xs text-indigo-100 hover:bg-indigo-500/40 transition-colors max-w-[200px]"
                    >
                      <Paperclip size={12} className="shrink-0" />
                      <span className="truncate">{a.name}</span>
                    </a>
                  ))}
                </div>
              )}
              {message.image_urls && !message.attachments && message.image_urls.length > 0 && (
                <div className="mb-2">
                  <MessageImages urls={message.image_urls} />
                </div>
              )}
              {message.content && message.content !== '(files attached)' && message.content !== '(images attached)' ? message.content : !message.attachments?.length && !message.image_urls?.length ? message.content || '' : null}
              {message.applied_plans && message.applied_plans.length > 0 && (
                <AppliedPlansInMessage plans={message.applied_plans} />
              )}
            </>
          ) : (
            <MarkdownContent content={message.content || ''} taskId={taskId} />
          )}
        </div>
        <div className={`flex items-center gap-1 mt-0.5 ${isUser ? 'justify-end pr-1' : 'pl-1'}`}>
          {message.timestamp && <MessageTimestamp timestamp={message.timestamp} />}
          {message.content && <MessageCopyButton text={isUser ? (message.raw_content ?? stripSenderPrefix(message.content)) : message.content} />}
        </div>
      </div>
    </div>
  );
});
