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
  FrontendReviewGoalCapabilities,
  InjectTaskAttachments,
  MonitorSession,
  PlanResource,
  Project,
  Task,
  TestHarnessRun,
  UploadResult,
  WorkspaceReviewCapabilities,
  BackgroundLifecycle,
} from '../../api/client';
import { DEFAULT_BROWSER_CHANNEL } from '../../config/browserReview';
import { useWebSocket } from '../../hooks/useWebSocket';
import { useVisibilityAwareInterval } from '../../hooks/useVisibilityAwareInterval';
import { resolveAssetUrl } from '../../config/server';
import { Send, ArrowLeft, Loader2, ChevronDown, ChevronRight, ChevronUp, Copy, Check, Paperclip, X, StopCircle, Pencil, ArrowDown, Star, ListPlus, ListTodo, Trash2, AlertCircle, Sparkles, GitBranch, Eye, RefreshCw, Pin } from '../icons';
import { SecretPicker } from '../Secrets/SecretPicker';
import { QuickPhraseDropdown } from '../QuickPhrases/QuickPhraseDropdown';
import { ListFilter, Syringe } from '../icons';
import { FastModeBadge, PlanPipelineBadge, TaskConfigBadge } from '../Tasks/TaskBadges';
import {
  canControlTask,
  readStoredUserIdentity,
  taskHasSession,
} from '../Tasks/taskSharePermissions';
import { VersionedPlansDialog } from '../PlanReview/VersionedPlansDialog';
import { planStalenessConfirmationMessage } from '../PlanReview/planStaleness';
import { AttentionTag } from '../Tasks/AttentionTag';
import { TaskSSHAccessBadge } from '../SSH/TaskSSHAccess';
import { DeliveryRunPanel } from '../Tasks/DeliveryRunPanel';
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
  BrowserReviewPanel,
  type BrowserReviewDisplayMode,
  type BrowserReviewGoalProgress,
  type BrowserReviewGoalStart,
} from './BrowserReviewPanel';
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
  /** The previous transport may have accepted this input without returning an ACK. */
  requiresConfirmation?: boolean;
}

type PtyFollowupBoundaryState = 'completed' | 'uncertain';

interface ActivePtyFollowup {
  operationId: string;
  httpSettled: boolean;
}

type InjectOutcome = 'injected' | 'rejected' | 'uncertain' | 'no_active_turn';

/**
 * A stale Task projection can still advertise an executing turn after the
 * provider process has already handed its turn back.  The inject endpoint
 * deliberately rejects that race with a specific 409.  This is safe to
 * downgrade to an ordinary queued message; all other 409s must remain
 * rejected because they may represent an authority/routing or delivery
 * race.
 */
function isNoActiveTurnInjectionError(error: unknown): boolean {
  if (!isApiRequestError(error) || error.status !== 409) return false;
  const detail = typeof error.detail === 'string'
    ? error.detail
    : error.message;
  return /没有正在运行的 turn|no active provider turn to inject/i.test(detail);
}

const WORKSPACE_REVIEW_START_TOOLS = new Set([
  'ccm_workspace_review.test_current_changes',
  'ccm_workspace_review.test_git_target',
]);

const BACKGROUND_STALLED_AFTER_MS = 30 * 60 * 1000;
const BACKGROUND_LONG_STALLED_AFTER_MS = 2 * 60 * 60 * 1000;

function createClientMessageId(): string {
  try {
    if (typeof globalThis.crypto?.randomUUID === 'function') {
      return globalThis.crypto.randomUUID();
    }
  } catch {
    // Fall through to the UUID v4 formatter below.
  }
  const bytes = new Uint8Array(16);
  let usedCrypto = false;
  try {
    if (typeof globalThis.crypto?.getRandomValues === 'function') {
      globalThis.crypto.getRandomValues(bytes);
      usedCrypto = true;
    }
  } catch {
    // Math.random is sufficient for this UI-only reconciliation identity.
  }
  if (!usedCrypto) {
    for (let index = 0; index < bytes.length; index += 1) {
      bytes[index] = Math.floor(Math.random() * 256);
    }
  }
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = [...bytes].map((value) => value.toString(16).padStart(2, '0')).join('');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function latestBackgroundLifecycle(
  messages: ChatMessage[],
  identity: TaskTurnIdentity,
): BackgroundLifecycle | null {
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const message = messages[index];
    if (!messageMatchesTaskTurn(message, identity)) continue;
    const lifecycle = message.background_lifecycle;
    if (lifecycle) return lifecycle;
  }
  return null;
}

function formatBackgroundSilence(milliseconds: number): string {
  const minutes = Math.max(0, Math.floor(milliseconds / 60_000));
  if (minutes < 1) return '刚刚';
  if (minutes < 60) return `${minutes} 分钟前`;
  const hours = Math.floor(minutes / 60);
  const remainder = minutes % 60;
  return remainder ? `${hours} 小时 ${remainder} 分钟前` : `${hours} 小时前`;
}

function workspaceReviewGoalFromToolInput(rawInput: unknown): string | null {
  if (typeof rawInput !== 'string' || !rawInput.trim()) return null;
  try {
    const parsed = JSON.parse(rawInput) as Record<string, unknown>;
    const goal = parsed.goal ?? parsed.objective;
    return typeof goal === 'string' && goal.trim() ? goal.trim() : null;
  } catch {
    return null;
  }
}

interface TaskTurnIdentity {
  taskId: number;
  retryCount: number;
  turnGeneration: number;
}

interface SuppressedCompletedLifecycle extends TaskTurnIdentity {
  suppressedAt: number;
}

interface LiveStreamCacheEntry extends TaskTurnIdentity {
  messages: ChatMessage[];
  updatedAt: number;
}

const LIVE_STREAM_CACHE_MAX_TASKS = 16;
const LIVE_STREAM_CACHE_MAX_ITEMS = 8;
const LIVE_STREAM_CACHE_MAX_CHARS_PER_ITEM = 200_000;
const LIVE_STREAM_CACHE_TTL_MS = 4 * 60 * 60 * 1000;
const liveStreamCache = new Map<number, LiveStreamCacheEntry>();

function taskTurnIdentity(task: Pick<Task, 'id' | 'retry_count' | 'turn_generation'>): TaskTurnIdentity {
  return {
    taskId: task.id,
    retryCount: task.retry_count,
    turnGeneration: task.turn_generation,
  };
}

function eventTaskTurnIdentity(
  data: Record<string, unknown>,
  taskId: number,
): TaskTurnIdentity | null {
  const retryCount = data.task_retry_count;
  const turnGeneration = data.task_turn_generation;
  const payloadTaskId = data.task_id;
  if (
    !Number.isInteger(retryCount)
    || (retryCount as number) < 0
    || !Number.isInteger(turnGeneration)
    || (turnGeneration as number) < 0
    || (
      payloadTaskId !== undefined
      && payloadTaskId !== null
      && payloadTaskId !== taskId
    )
  ) {
    return null;
  }
  return {
    taskId,
    retryCount: retryCount as number,
    turnGeneration: turnGeneration as number,
  };
}

function eventDeclaresTaskTurn(data: Record<string, unknown>): boolean {
  return (
    Object.prototype.hasOwnProperty.call(data, 'task_retry_count')
    || Object.prototype.hasOwnProperty.call(data, 'task_turn_generation')
  );
}

function sameTaskTurn(left: TaskTurnIdentity, right: TaskTurnIdentity): boolean {
  return (
    left.taskId === right.taskId
    && left.retryCount === right.retryCount
    && left.turnGeneration === right.turnGeneration
  );
}

function compareTaskTurn(left: TaskTurnIdentity, right: TaskTurnIdentity): number {
  if (left.taskId !== right.taskId) return 0;
  if (left.turnGeneration !== right.turnGeneration) {
    return left.turnGeneration - right.turnGeneration;
  }
  return left.retryCount - right.retryCount;
}

function messageMatchesTaskTurn(
  message: ChatMessage,
  identity: TaskTurnIdentity,
): boolean {
  return (
    message.task_retry_count === identity.retryCount
    && message.task_turn_generation === identity.turnGeneration
  );
}

function messageMatchesStreamItem(
  message: ChatMessage,
  identity: TaskTurnIdentity,
  itemId: string,
): boolean {
  return (
    message.stream_item_id === itemId
    && messageMatchesTaskTurn(message, identity)
  );
}

function removeOtherTurnProvisionals(
  messages: ChatMessage[],
  identity: TaskTurnIdentity,
): ChatMessage[] {
  return messages.filter((message) => (
    message.persisted
    || !message.stream_item_id
    || messageMatchesTaskTurn(message, identity)
  ));
}

function taskHasActiveStream(task: Task): boolean {
  return (
    task.background_active === true
    || task.status === 'in_progress'
    || task.status === 'executing'
  );
}

function clearLiveStreamCache(taskId: number, identity?: TaskTurnIdentity): void {
  const cached = liveStreamCache.get(taskId);
  if (!identity || (cached && sameTaskTurn(cached, identity))) {
    liveStreamCache.delete(taskId);
  }
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

function syncLiveStreamCache(identity: TaskTurnIdentity, messages: ChatMessage[]): void {
  const liveMessages = messages
    .filter((message) => (
      !message.persisted
      && Boolean(message.stream_item_id)
      && (message.event_type === 'message' || message.event_type === 'thinking')
      && messageMatchesTaskTurn(message, identity)
    ))
    .slice(-LIVE_STREAM_CACHE_MAX_ITEMS)
    .map((message) => ({
      ...message,
      content: message.content?.slice(-LIVE_STREAM_CACHE_MAX_CHARS_PER_ITEM) ?? null,
    }));
  if (liveMessages.length === 0) {
    clearLiveStreamCache(identity.taskId, identity);
    return;
  }

  const now = Date.now();
  const cached = liveStreamCache.get(identity.taskId);
  if (cached && compareTaskTurn(identity, cached) < 0) return;
  liveStreamCache.delete(identity.taskId);
  liveStreamCache.set(identity.taskId, {
    ...identity,
    messages: liveMessages,
    updatedAt: now,
  });
  pruneLiveStreamCache(now);
}

function restoreLiveStreamCache(task: Task): ChatMessage[] {
  if (!taskHasActiveStream(task)) {
    clearLiveStreamCache(task.id);
    return [];
  }
  pruneLiveStreamCache(Date.now());
  const cached = liveStreamCache.get(task.id);
  const identity = taskTurnIdentity(task);
  if (!cached || !sameTaskTurn(cached, identity)) {
    clearLiveStreamCache(task.id);
    return [];
  }
  return cached.messages.map((message) => ({ ...message }));
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

function parseStoredMessageQueue(raw: string | null): QueuedMessage[] {
  try {
    if (!raw) return [];
    const parsed: unknown = JSON.parse(raw);
    // Migrate the historical string[] representation without carrying any
    // now-stale control metadata into a fresh browser session.
    if (Array.isArray(parsed) && parsed.length > 0 && typeof parsed[0] === 'string') {
      return (parsed as string[]).map((text) => ({ text }));
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
        requiresConfirmation: candidate.requiresConfirmation === true,
      }];
    });
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
  let lastUserTurnAssistant = new Set<string>();
  for (const msg of messages) {
    if (msg.role === 'user' && msg.event_type === 'user_message') {
      lastUserTurnAssistant = new Set();
    }
    if (
      msg.role === 'assistant'
      && (msg.event_type === 'message' || msg.event_type === 'result')
      && msg.content
      && lastUserTurnAssistant.has(msg.content.replace(/\s+/g, ' ').trim())
    ) {
      continue;
    }
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
    if (msg.role === 'assistant' && (msg.event_type === 'message' || msg.event_type === 'result') && msg.content) {
      lastUserTurnAssistant.add(msg.content.replace(/\s+/g, ' ').trim());
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
  const hasTaskSession = taskHasSession(task);
  const hasControlAccess = canControlTask(task);
  const projectName = useMemo(() => {
    if (!task.project_id) return null;
    const p = projects.find((p) => p.id === task.project_id);
    return p?.name ?? null;
  }, [task.project_id, projects]);
  const providerLabel = task.provider === 'codex' ? 'Codex' : 'Claude';
  const deliveryReadOnly = task.mode === 'delivery_loop' || task.delivery_run_id != null;
  const [messages, setMessages] = useState<ChatMessage[]>(() => restoreLiveStreamCache(task));
  const activeTaskTurnRef = useRef<TaskTurnIdentity>(taskTurnIdentity(task));
  const [suppressedCompletedLifecycleTurn, setSuppressedCompletedLifecycleTurn] =
    useState<SuppressedCompletedLifecycle | null>(null);
  const storedUser = readStoredUserIdentity();
  const storagePrincipal = typeof storedUser.id === 'number'
    ? `user-${storedUser.id}`
    : 'anonymous';
  const storageAccessScope = task.access_scope === 'control'
    ? 'control'
    : task.access_scope === 'chat'
      ? 'chat'
      : hasControlAccess ? 'legacy-control' : 'restricted';
  const draftStorageNamespace = `${task.id}-${storageAccessScope}-${storagePrincipal}`;
  const legacyForkSeedKey = `ccm-fork-seed-consumed-${task.id}`;
  const legacyForkSeedUploadsKey = `ccm-fork-seed-uploads-${task.id}`;
  const legacyForkSeedUploadsConsumedKey = `ccm-fork-seed-uploads-consumed-${task.id}`;
  const legacyDraftKey = `ccm-chat-draft-${task.id}`;
  const legacyDraftUploadsKey = `ccm-chat-draft-uploads-${task.id}`;
  const forkSeedKey = `ccm-fork-seed-consumed-${draftStorageNamespace}`;
  const forkSeedUploadsKey = `ccm-fork-seed-uploads-${draftStorageNamespace}`;
  const forkSeedUploadsConsumedKey = `ccm-fork-seed-uploads-consumed-${draftStorageNamespace}`;
  const draftKey = `ccm-chat-draft-${draftStorageNamespace}`;
  const draftUploadsKey = `ccm-chat-draft-uploads-${draftStorageNamespace}`;
  const legacyMessageQueueKey = `ccm-chat-queue-${task.id}`;
  const messageQueueKey = `ccm-chat-queue-${draftStorageNamespace}`;
  const readScopedItem = (key: string, legacyKey: string): string | null => {
    const scoped = localStorage.getItem(key);
    if (scoped !== null || !hasControlAccess) return scoped;
    return localStorage.getItem(legacyKey);
  };
  const [forkSeedUploads, setForkSeedUploads] = useState<UploadResult[]>(() => {
    try {
      if (readScopedItem(forkSeedUploadsConsumedKey, legacyForkSeedUploadsConsumedKey)) return [];
      const saved = readScopedItem(forkSeedUploadsKey, legacyForkSeedUploadsKey);
      if (saved) return JSON.parse(saved) as UploadResult[];
    } catch { /* storage may be unavailable */ }
    return hasControlAccess ? task.metadata_?.fork_seed_uploads || [] : [];
  });
  // Draft buffer: unsent input survives refresh / re-entering the chat
  const [input, setInput] = useState(() => {
    try {
      const draft = readScopedItem(draftKey, legacyDraftKey);
      if (draft) return draft;
      const seed = task.metadata_?.fork_seed_message;
      if (hasControlAccess && seed && !readScopedItem(forkSeedKey, legacyForkSeedKey)) {
        localStorage.setItem(forkSeedKey, '1');
        return seed;
      }
      return '';
    } catch {
      return hasControlAccess ? task.metadata_?.fork_seed_message || '' : '';
    }
  });
  const [sending, setSending] = useState(false);
  const [injecting, setInjecting] = useState(false);
  const injectingRef = useRef(false);
  // A retained follow-up shares the Task generation with the lifecycle record
  // that handed the root turn off to background descendants. Keep that exact
  // generation fenced so a late ``background_lifecycle: running`` event cannot
  // turn a newer follow-up back into an apparently idle composer.
  const retainedFollowupTurnRef = useRef<TaskTurnIdentity | null>(null);
  const activePtyFollowupRef = useRef<ActivePtyFollowup | null>(null);
  const pendingPtyFollowupRequestRef = useRef(false);
  const ptyFollowupRequestEpochRef = useRef(0);
  const historyRequestEpochRef = useRef(0);
  const ptyFollowupBoundaryReceiptsRef = useRef(
    new Map<string, PtyFollowupBoundaryState>(),
  );
  const reconciledPtyFollowupOperationsRef = useRef(new Set<string>());
  const [ptyFollowupBoundaryEpoch, setPtyFollowupBoundaryEpoch] = useState(0);
  const rememberPtyFollowupBoundary = useCallback((
    operationId: string,
    state: PtyFollowupBoundaryState,
  ) => {
    const receipts = ptyFollowupBoundaryReceiptsRef.current;
    receipts.delete(operationId);
    receipts.set(operationId, state);
    while (receipts.size > 100) {
      const oldest = receipts.keys().next().value as string | undefined;
      if (!oldest) break;
      receipts.delete(oldest);
    }
    setPtyFollowupBoundaryEpoch((epoch) => epoch + 1);
  }, []);
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
  const retireAskUserRequest = useCallback((requestId: string) => {
    resolvedAskRequestIdsRef.current.add(requestId);
    setMessages((previous) => previous.filter((message) => !(
      message.event_type === 'ask_user_question'
      && message.request_id === requestId
    )));
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
  const [terminalReconciliationPending, setTerminalReconciliationPending] = useState(false);
  const [stillRunning, setStillRunning] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [dropError, setDropError] = useState<string | null>(null);
  const resetPtyFollowupTracking = useCallback(() => {
    ptyFollowupRequestEpochRef.current += 1;
    retainedFollowupTurnRef.current = null;
    activePtyFollowupRef.current = null;
    pendingPtyFollowupRequestRef.current = false;
    ptyFollowupBoundaryReceiptsRef.current.clear();
    reconciledPtyFollowupOperationsRef.current.clear();
    injectingRef.current = false;
    setInjecting(false);
    setSending(false);
    setStillRunning(false);
    setLocalStatus(null);
    setLocalBackgroundActive(null);
    lastWsStatusAt.current = 0;
    lastWsBackgroundAt.current = 0;
    setPtyFollowupBoundaryEpoch((epoch) => epoch + 1);
  }, []);
  const initialDraftUploads = useMemo(() => {
    try {
      const key = localStorage.getItem(draftUploadsKey) !== null
        ? draftUploadsKey
        : hasControlAccess ? legacyDraftUploadsKey : draftUploadsKey;
      return loadStoredUploadResults(key);
    } catch {
      return [];
    }
  }, [draftUploadsKey, hasControlAccess, legacyDraftUploadsKey]);
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
  const activeDraftStorageNamespaceRef = useRef(draftStorageNamespace);
  useEffect(() => {
    if (activeDraftStorageNamespaceRef.current === draftStorageNamespace) return;
    activeDraftStorageNamespaceRef.current = draftStorageNamespace;
    let nextInput = '';
    let nextForkSeedUploads: UploadResult[] = [];
    let nextDraftUploads: UploadResult[] = [];
    try {
      nextInput = localStorage.getItem(draftKey) || '';
      if (!localStorage.getItem(forkSeedUploadsConsumedKey)) {
        nextForkSeedUploads = loadStoredUploadResults(forkSeedUploadsKey);
      }
      nextDraftUploads = loadStoredUploadResults(draftUploadsKey);
    } catch { /* storage may be unavailable */ }
    setInput(nextInput);
    setForkSeedUploads(nextForkSeedUploads);
    setSelectedSecretIds([]);
    fileUpload.clear();
    fileUpload.addUploadedResults(nextDraftUploads);
  }, [
    draftKey,
    draftStorageNamespace,
    draftUploadsKey,
    fileUpload.addUploadedResults,
    fileUpload.clear,
    forkSeedUploadsConsumedKey,
    forkSeedUploadsKey,
  ]);
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

  useEffect(() => {
    const incoming = taskTurnIdentity(task);
    const current = activeTaskTurnRef.current;
    if (incoming.taskId !== current.taskId) {
      clearLiveStreamCache(current.taskId, current);
      resetPtyFollowupTracking();
      activeTaskTurnRef.current = incoming;
      setSending(false);
      setSuppressedCompletedLifecycleTurn(null);
      setTerminalReconciliationPending(false);
      setMessages(restoreLiveStreamCache(task));
      return;
    }
    if (sameTaskTurn(incoming, current) || compareTaskTurn(incoming, current) < 0) {
      return;
    }

    clearLiveStreamCache(task.id, current);
    resetPtyFollowupTracking();
    activeTaskTurnRef.current = incoming;
    setSending(false);
    setSuppressedCompletedLifecycleTurn(null);
    setTerminalReconciliationPending(false);
    setMessages((previous) => {
      const next = removeOtherTurnProvisionals(previous, incoming);
      syncLiveStreamCache(incoming, next);
      return next;
    });
  }, [resetPtyFollowupTracking, task.id, task.retry_count, task.turn_generation]);

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
  // 注入模式开关：开启后「发送」直达当前 turn，而不是排队新 turn。
  const [injectMode, setInjectMode] = useState(false);
  const canInject = hasControlAccess && !deliveryReadOnly && task.worker_id == null && task.shared_from_id == null && (
    task.provider === 'codex' ? codexAppServerEnabled : ptyMode
  );
  const injectTransport = task.provider === 'codex' ? 'Codex turn/steer' : 'Claude PTY';

  useEffect(() => {
    let active = true;
    setCodexMainMcpEnabled(null);
    setCodexMonitorEnabled(null);
    setPtyMode(false);
    setCodexAppServerEnabled(false);
    if (!hasControlAccess) return () => { active = false; };
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
  }, [hasControlAccess, task.worker_id]);

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

  const handleInject = async (
    text: string,
    uploadResults: UploadResult[],
    preserveComposer = false,
    expectPtyFollowupBoundary = false,
  ): Promise<InjectOutcome> => {
    if ((!text && uploadResults.length === 0) || injectingRef.current) {
      return 'rejected';
    }
    const requestEpoch = ++ptyFollowupRequestEpochRef.current;
    const requestIdentity = { ...activeTaskTurnRef.current };
    const requestStillCurrent = () => (
      ptyFollowupRequestEpochRef.current === requestEpoch
      && sameTaskTurn(requestIdentity, activeTaskTurnRef.current)
    );
    injectingRef.current = true;
    pendingPtyFollowupRequestRef.current = expectPtyFollowupBoundary;
    setInjecting(true);
    setError(null);
    let transportAttempted = false;
    let settledFollowupOperationId: string | null = null;
    const reportUnconfirmed = (
      outcome: Exclude<InjectOutcome, 'injected'>,
      reason: unknown,
    ): InjectOutcome => {
      if (!requestStillCurrent()) return outcome;
      if (sameTaskTurn(requestIdentity, activeTaskTurnRef.current)) {
        retainedFollowupTurnRef.current = null;
      }
      // A rejected injection did not start a new turn; an uncertain one may
      // have started it but has no client-visible acknowledgement. In either
      // case, restore the prior lifecycle hint until history reconciliation.
      setSuppressedCompletedLifecycleTurn(null);
      const detail = reason instanceof Error ? reason.message : String(reason);
      setError(outcome === 'uncertain'
        ? `注入结果不确定，消息和附件已保留且不会自动重试；请先查看聊天记录或运行日志，再决定是否手动重试：${detail}`
        : outcome === 'no_active_turn'
          ? `当前 turn 已结束，消息和附件已保留，可转入普通队列：${detail}`
          : `未收到注入成功确认，消息和附件已保留：${detail}`
      );
      onTaskUpdated?.();
      refreshHistoryRef.current();
      return outcome;
    };
    try {
      if (uploadResults.length > 0) {
        const capabilities = await api.getInjectCapabilities(task.id);
        if (capabilities.attachment_protocol !== 1) {
          throw new Error(
            '当前服务器未确认附件注入协议，已在发送前停止；消息和附件未发送',
          );
        }
      }
      transportAttempted = true;
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
      if (typeof result.operation_id === 'string' && result.operation_id) {
        settledFollowupOperationId = result.operation_id;
      }
      if (!result.ok || !result.injected) {
        return reportUnconfirmed(
          'rejected',
          new Error('服务器没有确认消息已注入'),
        );
      }
      if (
        uploadResults.length > 0
        && (
          !Number.isInteger(result.attachment_count)
          || result.attachment_count !== uploadResults.length
        )
      ) {
        return reportUnconfirmed(
          'uncertain',
          new Error('服务器没有确认全部附件均已注入'),
        );
      }
      if (
        requestStillCurrent()
        && (
          task.provider === 'codex'
          || (
            settledFollowupOperationId !== null
            && !reconciledPtyFollowupOperationsRef.current.has(
              settledFollowupOperationId,
            )
          )
        )
      ) {
        // The HTTP admission is the first reliable foreground receipt for
        // Codex (which has no PTY boundary operation id). Claude may also
        // reach this point after its durable boundary; do not resurrect a
        // receipt already reconciled by the boundary/user-message pair.
        setSending(true);
      }
      if (!preserveComposer && requestStillCurrent()) {
        setInput((current) => (
          current.trim() === text ? '' : current
        ));
        fileUpload.clear();
        consumeForkSeedUploads();
      }
      return 'injected';
    } catch (e) {
      if (isNoActiveTurnInjectionError(e)) {
        return reportUnconfirmed('no_active_turn', e);
      }
      const explicitRejection = isApiRequestError(e) && e.status < 500;
      return reportUnconfirmed(
        transportAttempted && !explicitRejection ? 'uncertain' : 'rejected',
        e,
      );
    } finally {
      // A task turn can advance while the HTTP request is unwinding.  The
      // old request must not re-install its operation receipt or clear state
      // belonging to the newer turn.
      if (ptyFollowupRequestEpochRef.current === requestEpoch) {
        injectingRef.current = false;
        pendingPtyFollowupRequestRef.current = false;
        if (
          settledFollowupOperationId
          && sameTaskTurn(requestIdentity, activeTaskTurnRef.current)
          && !reconciledPtyFollowupOperationsRef.current.has(
            settledFollowupOperationId,
          )
        ) {
          const current = activePtyFollowupRef.current;
          if (!current || current.operationId === settledFollowupOperationId) {
            activePtyFollowupRef.current = {
              operationId: settledFollowupOperationId,
              httpSettled: true,
            };
            setSending(true);
            setPtyFollowupBoundaryEpoch((epoch) => epoch + 1);
            refreshHistoryRef.current();
          }
        }
        setInjecting(false);
      }
    }
  };

  // Persist the draft after a short idle window so every keystroke does not
  // synchronously write localStorage (which can block the main thread).
  const draftValueRef = useRef(input);
  const draftIdentityRef = useRef({ draftKey, legacyDraftKey, hasControlAccess });
  useEffect(() => {
    draftValueRef.current = input;
    draftIdentityRef.current = { draftKey, legacyDraftKey, hasControlAccess };
  }, [draftKey, hasControlAccess, input, legacyDraftKey]);
  useEffect(() => {
    const timer = window.setTimeout(() => {
      try {
        if (input) localStorage.setItem(draftKey, input);
        else localStorage.removeItem(draftKey);
        if (hasControlAccess) localStorage.removeItem(legacyDraftKey);
      } catch { /* storage may be unavailable */ }
    }, 500);
    return () => window.clearTimeout(timer);
  }, [draftKey, hasControlAccess, input, legacyDraftKey]);
  useEffect(() => () => {
    try {
      const { draftKey: key, legacyDraftKey: legacyKey, hasControlAccess: controlAccess } = draftIdentityRef.current;
      if (draftValueRef.current) localStorage.setItem(key, draftValueRef.current);
      else localStorage.removeItem(key);
      if (controlAccess) localStorage.removeItem(legacyKey);
    } catch { /* storage may be unavailable */ }
  }, []);
  const draftUploadsRef = useRef(fileUpload.uploadedResults);
  const draftUploadsIdentityRef = useRef({ draftUploadsKey, legacyDraftUploadsKey, hasControlAccess });
  useEffect(() => {
    draftUploadsRef.current = fileUpload.uploadedResults;
    draftUploadsIdentityRef.current = { draftUploadsKey, legacyDraftUploadsKey, hasControlAccess };
  }, [draftUploadsKey, fileUpload.uploadedResults, hasControlAccess, legacyDraftUploadsKey]);
  useEffect(() => {
    const timer = window.setTimeout(() => {
      try {
        if (fileUpload.uploadedResults.length > 0) {
          localStorage.setItem(draftUploadsKey, JSON.stringify(fileUpload.uploadedResults));
        } else {
          localStorage.removeItem(draftUploadsKey);
        }
        if (hasControlAccess) localStorage.removeItem(legacyDraftUploadsKey);
      } catch { /* storage may be unavailable */ }
    }, 500);
    return () => window.clearTimeout(timer);
  }, [draftUploadsKey, fileUpload.uploadedResults, hasControlAccess, legacyDraftUploadsKey]);
  useEffect(() => () => {
    try {
      const { draftUploadsKey: key, legacyDraftUploadsKey: legacyKey, hasControlAccess: controlAccess } = draftUploadsIdentityRef.current;
      if (draftUploadsRef.current.length > 0) {
        localStorage.setItem(key, JSON.stringify(draftUploadsRef.current));
      } else {
        localStorage.removeItem(key);
      }
      if (controlAccess) localStorage.removeItem(legacyKey);
    } catch { /* storage may be unavailable */ }
  }, []);
  useEffect(() => {
    try {
      if (localStorage.getItem(forkSeedUploadsConsumedKey)) {
        localStorage.removeItem(forkSeedUploadsKey);
      } else {
        localStorage.setItem(forkSeedUploadsKey, JSON.stringify(forkSeedUploads));
      }
      if (hasControlAccess) {
        localStorage.removeItem(legacyForkSeedUploadsKey);
        localStorage.removeItem(legacyForkSeedUploadsConsumedKey);
      }
    } catch { /* storage may be unavailable */ }
  }, [
    forkSeedUploads,
    forkSeedUploadsConsumedKey,
    forkSeedUploadsKey,
    hasControlAccess,
    legacyForkSeedUploadsConsumedKey,
    legacyForkSeedUploadsKey,
  ]);
  const [monitorSessions, setMonitorSessions] = useState<MonitorSession[]>([]);
  const [showMonitorPanel, setShowMonitorPanel] = useState(false);
  const [browserReviewAvailable, setBrowserReviewAvailable] = useState(false);
  const [browserReviewActive, setBrowserReviewActive] = useState(false);
  const [showBrowserReviewPanel, setShowBrowserReviewPanel] = useState(false);
  const [frontendReviewComposerMode, setFrontendReviewComposerMode] = useState(false);
  const [workspaceReviewComposerMode, setWorkspaceReviewComposerMode] = useState(false);
  const [frontendReviewGoalCapability, setFrontendReviewGoalCapability] = useState<FrontendReviewGoalCapabilities | null>(null);
  const [workspaceReviewCapability, setWorkspaceReviewCapability] = useState<WorkspaceReviewCapabilities | null>(null);
  const [startedWorkspaceReview, setStartedWorkspaceReview] = useState<TestHarnessRun | null>(null);
  const [expectedWorkspaceReviewBaseline, setExpectedWorkspaceReviewBaseline] = useState<string | null | undefined>(undefined);
  const [frontendReviewGoalStart, setFrontendReviewGoalStart] = useState<BrowserReviewGoalStart | null>(null);
  const [frontendReviewGoalLocallyActive, setFrontendReviewGoalLocallyActive] = useState(
    task.metadata_?.frontend_review?.mode === 'goal'
      && ['pending', 'in_progress', 'executing'].includes(task.status),
  );
  const frontendReviewGoalRequestSequence = useRef(0);
  const signalledWorkspaceReviewItemsRef = useRef(new Set<string>());
  const frontendReviewGoalActiveRef = useRef(
    task.metadata_?.frontend_review?.mode === 'goal'
      && ['pending', 'in_progress', 'executing'].includes(task.status),
  );
  const [frontendReviewGoalCapabilityLoading, setFrontendReviewGoalCapabilityLoading] = useState(false);
  const [browserReviewDisplayMode, setBrowserReviewDisplayMode] = useState<BrowserReviewDisplayMode>(() => (
    localStorage.getItem('ccm-browser-review-display-mode') === 'floating' ? 'floating' : 'docked'
  ));
  const previousBrowserReviewAvailable = useRef(false);
  const previousBrowserReviewActive = useRef(false);
  const handleBrowserReviewAvailable = useCallback((available: boolean) => {
    setBrowserReviewAvailable(available);
    previousBrowserReviewAvailable.current = available;
  }, []);
  const handleBrowserReviewActive = useCallback((active: boolean) => {
    setBrowserReviewActive(active);
    if (active && !previousBrowserReviewActive.current) {
      setShowBrowserReviewPanel(true);
    }
    previousBrowserReviewActive.current = active;
  }, []);
  const handleBrowserReviewDisplayModeChange = useCallback((mode: BrowserReviewDisplayMode) => {
    setBrowserReviewDisplayMode(mode);
    setShowBrowserReviewPanel(true);
    try { localStorage.setItem('ccm-browser-review-display-mode', mode); } catch { /* storage may be unavailable */ }
  }, []);
  const handleNewBrowserReview = useCallback(() => {
    setShowBrowserReviewPanel(true);
  }, []);
  const handleExpectedWorkspaceReviewFound = useCallback(() => {
    setExpectedWorkspaceReviewBaseline(undefined);
  }, []);
  const handleFrontendReviewGoalFound = useCallback(() => {
    setFrontendReviewGoalStart(null);
  }, []);

  useEffect(() => {
    previousBrowserReviewAvailable.current = false;
    previousBrowserReviewActive.current = false;
    setBrowserReviewAvailable(false);
    setBrowserReviewActive(false);
    setShowBrowserReviewPanel(false);
    setFrontendReviewComposerMode(false);
    setWorkspaceReviewComposerMode(false);
    setStartedWorkspaceReview(null);
    setExpectedWorkspaceReviewBaseline(undefined);
    setFrontendReviewGoalStart(null);
    setFrontendReviewGoalLocallyActive(false);
    signalledWorkspaceReviewItemsRef.current.clear();
    frontendReviewGoalActiveRef.current = false;
  }, [task.id]);

  useEffect(() => {
    let active = true;
    setFrontendReviewGoalCapability(null);
    setWorkspaceReviewCapability(null);
    if (!hasControlAccess || task.worker_id != null || task.shared_from_id != null || !hasTaskSession) {
      setFrontendReviewGoalCapabilityLoading(false);
      return () => { active = false; };
    }
    setFrontendReviewGoalCapabilityLoading(true);
    Promise.all([
      api.getFrontendReviewGoalCapabilities(task.id),
      api.getWorkspaceReviewCapabilities(task.id),
    ])
      .then(([goalCapability, reviewCapability]) => {
        if (active) {
          setFrontendReviewGoalCapability(goalCapability);
          setWorkspaceReviewCapability(reviewCapability);
        }
      })
      .catch(() => {
        if (active) {
          setFrontendReviewGoalCapability({
            available: false,
            reason: '无法确认本地 Git 仓库，请刷新后重试',
            repo_path: null,
          });
          setWorkspaceReviewCapability({
            available: false,
            reason: '无法确认本地 Git 仓库，请刷新后重试',
            repo_path: null,
            configured: false,
            config: null,
            suggested_config: null,
          });
        }
      })
      .finally(() => {
        if (active) setFrontendReviewGoalCapabilityLoading(false);
      });
    return () => { active = false; };
  }, [
    task.id,
    hasControlAccess,
    hasTaskSession,
    task.project_id,
    task.shared_from_id,
    task.status,
    task.target_repo,
    task.worker_id,
  ]);

  useEffect(() => {
    if (
      frontendReviewGoalCapability?.available === false
      || (workspaceReviewCapability !== null
        && !workspaceReviewCapability.available
        && workspaceReviewCapability.suggested_config === null)
    ) {
      setFrontendReviewComposerMode(false);
    }
  }, [frontendReviewGoalCapability?.available, workspaceReviewCapability]);

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
    if (!hasControlAccess || !hasTaskSession || task.shared_from_id != null) return;
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
  }, [hasControlAccess, hasTaskSession, task.id, task.shared_from_id]);

  useVisibilityAwareInterval(() => refreshVersionedPlans(), 5000, hasControlAccess && hasTaskSession && task.shared_from_id == null);

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
  useEffect(() => {
    if (hasControlAccess) return;
    setDistillOpen(false);
    setForkOpen(false);
    setShowModelMenu(false);
    setModelOverride(null);
    setPlansOpen(false);
    setShowMonitorPanel(false);
    setShowBrowserReviewPanel(false);
    setFrontendReviewComposerMode(false);
    setWorkspaceReviewComposerMode(false);
    setInjectMode(false);
    setSelectedSecretIds([]);
  }, [hasControlAccess]);
  const effectiveStatus = localStatus || task.status;
  // Codex descendants are task-thread scoped, not PTY background generations,
  // so their durable Task projection deliberately keeps ``background_active``
  // false.  A current-turn lifecycle record is the authoritative signal for
  // that retained native work, including after reconnect/history reload.
  const rawBackgroundLifecycle = useMemo(
    () => latestBackgroundLifecycle(messages, activeTaskTurnRef.current),
    [messages, task.retry_count, task.turn_generation],
  );
  const backgroundActive = (
    rawBackgroundLifecycle?.state === 'running'
    || (localBackgroundActive ?? task.background_active === true)
  );
  const isWaitingCapability = effectiveStatus === 'waiting_capability';
  const canInjectNow = canInject && !isWaitingCapability;
  // A retained background marker is itself authoritative evidence that an
  // exact provider session still exists. Do not wait for the asynchronously
  // loaded runtime-settings toggle before routing a follow-up, or a message
  // sent immediately after opening the Task can race onto the ordinary /chat
  // path. The backend remains the final exact-session admission check.
  const canInjectBackgroundFollowup = (
    hasControlAccess
    && !deliveryReadOnly
    && task.worker_id == null
    && task.shared_from_id == null
    && !isWaitingCapability
  );
  useEffect(() => {
    if (isWaitingCapability && injectMode) setInjectMode(false);
  }, [injectMode, isWaitingCapability]);
  // A provider may keep the Task row executing while its root response has
  // already ended and only native/background work remains. That lifecycle is
  // visible through ``backgroundActive``; it must not turn the composer into
  // a queue-only control.
  const foregroundActive = sending
    || (
      !backgroundActive
      && ['in_progress', 'executing', 'waiting_capability'].includes(effectiveStatus)
  );
  const backgroundOnly = backgroundActive && !foregroundActive;
  const hasActiveWork = foregroundActive || backgroundActive;
  // Starting a new turn must not reuse the previous turn's terminal lifecycle
  // as an optimistic status. Running descendants remain visible throughout.
  const lifecycleTimestamp = rawBackgroundLifecycle
    ? Date.parse(rawBackgroundLifecycle.last_activity_at || rawBackgroundLifecycle.started_at)
    : Number.NaN;
  const suppressesCurrentLifecycle = (
    rawBackgroundLifecycle?.state === 'completed'
    && suppressedCompletedLifecycleTurn
    && sameTaskTurn(suppressedCompletedLifecycleTurn, activeTaskTurnRef.current)
    && (!Number.isFinite(lifecycleTimestamp)
      || lifecycleTimestamp <= suppressedCompletedLifecycleTurn.suppressedAt)
  );
  const backgroundLifecycle = (
    suppressesCurrentLifecycle ? null : rawBackgroundLifecycle
  );
  const [, refreshBackgroundAge] = useState(0);
  useVisibilityAwareInterval(
    () => refreshBackgroundAge((generation) => generation + 1),
    60_000,
    Boolean(backgroundLifecycle),
    false,
  );
  const workspaceReviewCanBeConfigured = (
    workspaceReviewCapability?.available === true
    || workspaceReviewCapability?.suggested_config != null
  );
  const canStartWorkspaceReview = (
    hasControlAccess
    && task.worker_id == null
    && task.shared_from_id == null
    && hasTaskSession
    && ['completed', 'failed', 'cancelled', 'conflict'].includes(effectiveStatus)
    && !hasActiveWork
    && workspaceReviewCanBeConfigured
  );
  const canStartConfiguredBrowserReview = (
    hasControlAccess
    && task.worker_id == null
    && task.shared_from_id == null
    && ['completed', 'failed', 'cancelled', 'conflict'].includes(effectiveStatus)
    && !hasActiveWork
  );
  const canStartFrontendReviewGoal = (
    hasControlAccess
    && task.worker_id == null
    && task.shared_from_id == null
    && hasTaskSession
    && ['completed', 'failed', 'cancelled', 'conflict'].includes(effectiveStatus)
    && !hasActiveWork
    && frontendReviewGoalCapability?.available === true
    && workspaceReviewCanBeConfigured
  );
  const workspaceReviewUnavailableReason = !hasControlAccess
    ? '此 Task 仅授予聊天权限'
    : !hasTaskSession
    ? 'Task 完成并建立 session 后才能审查当前分支'
    : !['completed', 'failed', 'cancelled', 'conflict'].includes(effectiveStatus) || hasActiveWork
      ? 'Task 正在执行；Agent 可在对话中调用测试工具，或等待完成后从这里启动'
      : frontendReviewGoalCapabilityLoading
        ? '正在确认本地 Git 仓库与 Preview 配置…'
        : workspaceReviewCapability?.reason || '当前 Task 没有可运行的本地 Preview';
  const frontendReviewGoalUnavailableReason = !hasControlAccess
    ? '此 Task 仅授予聊天权限'
    : !hasTaskSession
    ? 'Task 完成并建立 session 后才能启动循环审查'
    : !['completed', 'failed', 'cancelled', 'conflict'].includes(effectiveStatus) || hasActiveWork
      ? 'Task 正在执行，请等待完成后再启动循环审查'
      : frontendReviewGoalCapabilityLoading
        ? '正在确认可修改的本地 Git 仓库…'
        : workspaceReviewCapability?.reason || frontendReviewGoalCapability?.reason || '尚未确认存在可修改的本地 Git 仓库';
  const configuredBrowserReviewUnavailableReason = !hasControlAccess
    ? '此 Task 仅授予聊天权限'
    : task.worker_id != null
    ? 'Worker Task 暂不支持从 Manager 界面直接启动网站测试'
    : task.shared_from_id != null
      ? '共享 Task 只能查看已有测试记录'
      : !['completed', 'failed', 'cancelled', 'conflict'].includes(effectiveStatus) || hasActiveWork
        ? 'Task 正在执行；Agent 可在对话中调用测试工具，或等待完成后从这里启动'
        : null;
  const isFrontendReviewGoal = task.metadata_?.frontend_review?.mode === 'goal';
  const frontendReviewGoalTaskActive = ['pending', 'in_progress', 'executing'].includes(effectiveStatus);
  const showFrontendReviewGoal = frontendReviewGoalStart !== null
    || ((isFrontendReviewGoal || frontendReviewGoalLocallyActive) && frontendReviewGoalTaskActive);
  useEffect(() => {
    if (frontendReviewGoalStart !== null || (isFrontendReviewGoal && frontendReviewGoalTaskActive)) {
      frontendReviewGoalActiveRef.current = true;
      setFrontendReviewGoalLocallyActive(true);
    } else if (!frontendReviewGoalTaskActive) {
      frontendReviewGoalActiveRef.current = false;
      setFrontendReviewGoalLocallyActive(false);
    }
  }, [frontendReviewGoalStart, frontendReviewGoalTaskActive, isFrontendReviewGoal]);
  const [frontendReviewGoalProgress, setFrontendReviewGoalProgress] = useState<BrowserReviewGoalProgress>({
    turn: task.goal_turns_used || 0,
    maxTurns: task.goal_max_turns || 5,
    lastReason: task.goal_last_reason,
    active: foregroundActive,
  });
  useEffect(() => {
    setFrontendReviewGoalProgress({
      turn: task.goal_turns_used || 0,
      maxTurns: task.goal_max_turns || 5,
      lastReason: task.goal_last_reason,
      active: ['pending', 'in_progress', 'executing'].includes(task.status),
    });
  }, [task.id, task.goal_turns_used, task.goal_max_turns, task.goal_last_reason, task.status]);
  useEffect(() => {
    setFrontendReviewGoalProgress((current) => ({ ...current, active: foregroundActive }));
  }, [foregroundActive]);
  useEffect(() => {
    if (!stillRunning) return;
    const timer = window.setTimeout(() => {
      setStillRunning(false);
      onTaskUpdated?.();
    }, 15_000);
    return () => window.clearTimeout(timer);
  }, [onTaskUpdated, stillRunning]);
  const planAttentionCount = versionedPlans.filter((plan) => (
    plan.display_state === 'waiting_user'
    || (!plan.read_only && (
      ['awaiting_review', 'planner', 'reviewer', 'queued', 'running', 'cancelling'].includes(plan.display_state)
      || (plan.display_state === 'approved' && Boolean(plan.current_version && !plan.current_version.applied))
    ))
  )).length;
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
      const scoped = localStorage.getItem(messageQueueKey);
      const saved = scoped ?? (
        hasControlAccess ? localStorage.getItem(legacyMessageQueueKey) : null
      );
      return parseStoredMessageQueue(saved);
    } catch { return []; }
  });
  const messageQueueRef = useRef(messageQueue);
  const activeMessageQueueKeyRef = useRef(messageQueueKey);
  useEffect(() => {
    if (activeMessageQueueKeyRef.current !== messageQueueKey) {
      activeMessageQueueKeyRef.current = messageQueueKey;
      let next: QueuedMessage[] = [];
      try {
        next = parseStoredMessageQueue(localStorage.getItem(messageQueueKey));
      } catch { /* storage may be unavailable */ }
      messageQueueRef.current = next;
      setMessageQueue(next);
      return;
    }
    messageQueueRef.current = messageQueue;
    try {
      localStorage.setItem(messageQueueKey, JSON.stringify(messageQueue));
      if (hasControlAccess) localStorage.removeItem(legacyMessageQueueKey);
    } catch { /* storage may be unavailable */ }
  }, [hasControlAccess, legacyMessageQueueKey, messageQueue, messageQueueKey]);

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

  // Auto-dequeue requests come from terminal/process_exit reconciliation and
  // from the foreground -> retained-background handoff below.
  const [autoDequeueFlag, setAutoDequeueFlag] = useState(0);
  const foregroundActiveRef = useRef(false);
  foregroundActiveRef.current = foregroundActive;
  const handleSendRef = useRef<(
    text: string,
    uploadResults?: UploadResult[],
    planTaskIds?: number[],
    planVersionIds?: number[],
  ) => void>(() => {});

  useEffect(() => {
    if (autoDequeueFlag === 0) return;
    if (historyLoading) return;
    // Delay to let React flush the foreground state from status_change/process_exit
    // before checking the shared predicate. Without this, PTY mode
    // triggers autoDequeue in the same cycle as setSending(false) and the
    // ref still reads true and skips the queued message.
    const timer = setTimeout(() => {
      if (foregroundActiveRef.current) return;
      const queue = messageQueueRef.current;
      if (queue.length > 0) {
        const next = queue[0];
        // An acknowledgement was lost after this item may already have been
        // accepted. Preserve ordering and require an explicit human edit/send
        // instead of ever replaying it from a later terminal transition.
        if (next.requiresConfirmation) return;
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
  }, [autoDequeueFlag, historyLoading]);

  // A retained Claude follow-up has two independent acknowledgements: the
  // HTTP transport admission and the provider's foreground boundary. Either
  // can arrive first. Reconcile only after both are present, then wake one
  // queued item; this prevents an early boundary from racing the user_message
  // audit or an injectingRef that is still in flight.
  useEffect(() => {
    const active = activePtyFollowupRef.current;
    if (!active?.httpSettled) return;
    const boundary = ptyFollowupBoundaryReceiptsRef.current.get(
      active.operationId,
    );
    if (!boundary) return;
    activePtyFollowupRef.current = null;
    const reconciled = reconciledPtyFollowupOperationsRef.current;
    reconciled.add(active.operationId);
    while (reconciled.size > 100) {
      const oldest = reconciled.values().next().value as string | undefined;
      if (!oldest) break;
      reconciled.delete(oldest);
    }
    if (
      retainedFollowupTurnRef.current
      && sameTaskTurn(
        retainedFollowupTurnRef.current,
        activeTaskTurnRef.current,
      )
    ) {
      retainedFollowupTurnRef.current = null;
    }
    setSending(false);
    setStillRunning(false);
    if (boundary === 'completed') {
      setAutoDequeueFlag((generation) => generation + 1);
    } else {
      setError(
        'Claude follow-up 的结果状态不确定；系统不会自动发送下一条排队消息，请先核对聊天记录。',
      );
      setMessageQueue((previous) => {
        if (previous.length === 0) return previous;
        const [first, ...rest] = previous;
        return [{ ...first, requiresConfirmation: true }, ...rest];
      });
      refreshHistoryRef.current();
    }
  }, [ptyFollowupBoundaryEpoch]);

  // Codex deliberately keeps its Task adapter executing while native
  // descendants remain, so neither a terminal status nor process_exit is
  // guaranteed at the root-turn handoff. Attempt one dequeue when this exact
  // Task turn first becomes background-only. Mark the epoch attempted even
  // when the queue is empty so an injection failure that requeues the item
  // cannot create an automatic retry loop in the same retained tail.
  const backgroundOnlyEpoch = backgroundOnly
    ? `${activeTaskTurnRef.current.taskId}:${activeTaskTurnRef.current.retryCount}:${activeTaskTurnRef.current.turnGeneration}`
    : null;
  const attemptedBackgroundOnlyEpochRef = useRef<string | null>(null);
  useEffect(() => {
    if (historyLoading) return;
    if (backgroundOnlyEpoch === null) {
      attemptedBackgroundOnlyEpochRef.current = null;
      return;
    }
    if (attemptedBackgroundOnlyEpochRef.current === backgroundOnlyEpoch) return;
    attemptedBackgroundOnlyEpochRef.current = backgroundOnlyEpoch;
    if (messageQueueRef.current.length > 0) {
      setAutoDequeueFlag((flag) => flag + 1);
    }
  }, [backgroundOnlyEpoch, historyLoading]);

  useEffect(() => {
    const prev = document.title;
    const label = task.title || task.description || '';
    const preview = label.length > 30 ? label.slice(0, 30) + '…' : label;
    document.title = preview ? `#${task.id} ${preview}` : `#${task.id} - CCM`;
    return () => { document.title = prev; };
  }, [task.id, task.title, task.description]);

  // Handle real-time WebSocket messages via callback (not state) to avoid
  // losing messages when React batches rapid state updates.
  const observeTaskTurn = useCallback((incoming: TaskTurnIdentity): number => {
    const current = activeTaskTurnRef.current;
    if (incoming.taskId !== current.taskId) return -1;
    const comparison = compareTaskTurn(incoming, current);
    if (comparison > 0) {
      clearLiveStreamCache(current.taskId, current);
      resetPtyFollowupTracking();
      activeTaskTurnRef.current = incoming;
      setSending(false);
      setSuppressedCompletedLifecycleTurn(null);
      setTerminalReconciliationPending(false);
    }
    return comparison;
  }, [resetPtyFollowupTracking]);

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
      if (hasControlAccess && task.worker_id == null) {
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
      const statusIdentity = eventTaskTurnIdentity(msg.data!, task.id);
      if (
        (statusIdentity && observeTaskTurn(statusIdentity) < 0)
        || (!statusIdentity && eventDeclaresTaskTurn(msg.data!))
      ) {
        return;
      }
      const newStatus = (msg.data!.new_status as string) || '';
      const nextBackground = msg.data!.background_active;
      if (typeof msg.data!.background_active === 'boolean') {
        lastWsBackgroundAt.current = Date.now();
        setLocalBackgroundActive(msg.data!.background_active);
      }
      if (newStatus) {
        lastWsStatusAt.current = Date.now();
        setLocalStatus(newStatus);
        if (['completed', 'failed', 'cancelled', 'conflict'].includes(newStatus)) {
          frontendReviewGoalActiveRef.current = false;
          setFrontendReviewGoalLocallyActive(false);
          setFrontendReviewGoalStart(null);
        }
      }
      if (
        ['completed', 'failed', 'cancelled', 'conflict'].includes(newStatus)
        && nextBackground !== true
      ) {
        retainedFollowupTurnRef.current = null;
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
      const backgroundIdentity = eventTaskTurnIdentity(msg.data!, task.id);
      if (
        (backgroundIdentity && observeTaskTurn(backgroundIdentity) < 0)
        || (!backgroundIdentity && eventDeclaresTaskTurn(msg.data!))
      ) {
        return;
      }
      if (typeof msg.data!.background_active === 'boolean') {
        lastWsBackgroundAt.current = Date.now();
        setLocalBackgroundActive(msg.data!.background_active);
      }
      return;
    }

    if (msg.channel !== `task:${task.id}` || !msg.data) return;

    const eventType = msg.data.event_type as string || (msg.data.event as string);
    if (eventType === 'pty_background_followup_boundary') {
      const identity = eventTaskTurnIdentity(msg.data, task.id);
      if (!identity || observeTaskTurn(identity) < 0) return;
      const operationId = typeof msg.data.followup_operation_id === 'string'
        ? msg.data.followup_operation_id
        : null;
      const boundaryState = (
        msg.data.pty_followup_state || msg.data.state
      ) as string | undefined;
      if (
        !operationId
        || (boundaryState !== 'completed' && boundaryState !== 'uncertain')
      ) return;
      rememberPtyFollowupBoundary(operationId, boundaryState);
      return;
    }
    if (eventType === 'background_lifecycle') {
      const identity = eventTaskTurnIdentity(msg.data, task.id);
      if (!identity || observeTaskTurn(identity) < 0) return;
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
      if (lifecycle.state === 'running') {
        // This record is emitted only after the root Codex turn has reached
        // its native terminal boundary and CCM is retaining descendants/Goal
        // work. The adapter remains `executing`, so clear the foreground UI
        // explicitly instead of waiting for a process_exit that may not occur.
        const retainedFollowupIsActive = (
          retainedFollowupTurnRef.current !== null
          && sameTaskTurn(
            retainedFollowupTurnRef.current,
            activeTaskTurnRef.current,
          )
        );
        if (!retainedFollowupIsActive) {
          setSending(false);
          setStillRunning(false);
        }
      }
      const persistedId = Number(msg.data.id);
      const entry: ChatMessage = {
        id: Number.isFinite(persistedId) && persistedId > 0
          ? persistedId
          : Date.now() + Math.random(),
        role: 'system', event_type: 'background_lifecycle', content: null,
        tool_name: null, tool_input: null, tool_output: null, is_error: false,
        loop_iteration: null,
        task_retry_count: identity.retryCount,
        task_turn_generation: identity.turnGeneration,
        timestamp: typeof msg.data.timestamp === 'string'
          ? msg.data.timestamp
          : new Date().toISOString(),
        image_urls: null, attachments: null,
        background_lifecycle: lifecycle,
        persisted: Number.isFinite(persistedId) && persistedId > 0,
      };
      setMessages((previous) => mergeChatHistory([entry], previous));
      return;
    }
    if (eventType === 'goal_evaluation') {
      setFrontendReviewGoalProgress({
        turn: Number(msg.data.turn) || 0,
        maxTurns: Number(msg.data.max_turns) || task.goal_max_turns || 5,
        lastReason: typeof msg.data.reason === 'string' ? msg.data.reason : null,
        active: !msg.data.achieved,
      });
      return;
    }
    const workspaceReviewToolName = typeof msg.data.tool_name === 'string'
      ? msg.data.tool_name
      : '';
    if (
      hasControlAccess
      && eventType === 'tool_use'
      && frontendReviewGoalActiveRef.current
      && WORKSPACE_REVIEW_START_TOOLS.has(workspaceReviewToolName)
    ) {
      const itemKey = typeof msg.data.item_id === 'string'
        ? msg.data.item_id
        : `${workspaceReviewToolName}:${String(msg.data.timestamp || '')}`;
      if (!signalledWorkspaceReviewItemsRef.current.has(itemKey)) {
        signalledWorkspaceReviewItemsRef.current.add(itemKey);
        setFrontendReviewGoalStart((current) => ({
          requestId: ++frontendReviewGoalRequestSequence.current,
          prompt: workspaceReviewGoalFromToolInput(msg.data!.tool_input)
            || current?.prompt
            || '按当前 Goal 对最新代码创建新的浏览器审查',
          maxTurns: current?.maxTurns || task.goal_max_turns || 5,
          phase: 'starting_review',
        }));
        setShowBrowserReviewPanel(true);
      }
    }
    if (eventType === 'monitor_session_created' || eventType === 'monitor_session_status'
        || eventType === 'sub_agent_session_created' || eventType === 'sub_agent_session_status') {
      api.listAllSubAgentSessions(task.id).then(setMonitorSessions).catch(() => {});
      if (eventType.startsWith('sub_agent_')) onTaskUpdated?.();
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
      if (rid) retireAskUserRequest(rid);
      return;
    }

    // 模型原生子 agent 的进度（Provider 生命周期观测，经通用表镜像）
    if (eventType === 'sub_agent_report') {
      api.listAllSubAgentSessions(task.id).then(setMonitorSessions).catch(() => {});
      return;
    }

    // CCM Sub-Agent progress: show in chat as system_event, update panel
    if (eventType === 'sub_agent_progress') {
      api.listAllSubAgentSessions(task.id).then(setMonitorSessions).catch(() => {});
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
      api.listAllSubAgentSessions(task.id).then(setMonitorSessions).catch(() => {});
      return;
    }

    if (eventType === 'monitor_check') {
      // Always refresh panel data
      api.listAllSubAgentSessions(task.id).then(setMonitorSessions).catch(() => {});
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
      const exitIdentity = eventTaskTurnIdentity(msg.data, task.id);
      if (!exitIdentity || observeTaskTurn(exitIdentity) < 0) return;
      clearLiveStreamCache(task.id, exitIdentity);
      // Small delay so any final output messages queued just before
      // process_exit are rendered before the "thinking" indicator hides.
      setTimeout(() => {
        // A newer logical turn may start during this deliberate delay. The
        // old exit may refresh history, but it must not stop the new spinner
        // or dequeue another prompt into that active native session.
        if (!sameTaskTurn(exitIdentity, activeTaskTurnRef.current)) return;
        // A process exit can arrive without its preceding terminal status
        // event (for example during a WebSocket gap). Re-read the authoritative
        // Task before clearing the exact generation, rather than leaving a
        // stale executing prop rendering the generic thinking indicator.
        setTerminalReconciliationPending(true);
        void api.getTask(task.id).then((updated) => {
          if (!sameTaskTurn(exitIdentity, activeTaskTurnRef.current)) return;
          const updatedIdentity = taskTurnIdentity(updated);
          if (!sameTaskTurn(exitIdentity, updatedIdentity)) {
            observeTaskTurn(updatedIdentity);
            return;
          }
          const terminal = ['completed', 'failed', 'cancelled', 'conflict']
            .includes(updated.status);
          if (terminal) {
            // This authoritative HTTP snapshot is the same kind of fresh
            // status evidence as a WS terminal event.  Without refreshing
            // these clocks, the stale-override effects see their initial
            // timestamp (0) and can immediately clear the reconciled status
            // on an unrelated render, reviving the old executing prop.
            const reconciledAt = Date.now();
            lastWsStatusAt.current = reconciledAt;
            lastWsBackgroundAt.current = reconciledAt;
            setLocalStatus(updated.status);
            setLocalBackgroundActive(updated.background_active === true);
            retainedFollowupTurnRef.current = null;
            setSending(false);
            setStillRunning(false);
            setAutoDequeueFlag(f => f + 1);
            onTaskUpdated?.(updated);
          }
        }).catch(() => {
          // Keep the existing status until the next poll; the UI uses a
          // distinct reconciliation message while this request is unresolved.
        }).finally(() => {
          if (sameTaskTurn(exitIdentity, activeTaskTurnRef.current)) {
            setTerminalReconciliationPending(false);
          }
        });
        retainedFollowupTurnRef.current = null;
        setSending(false);
        setStillRunning(false);
        // Keep an already observed terminal status sticky. A late process_exit
        // must not revive stale `task.status=executing` props and bring the
        // Goal/thinking indicator back after completion.
        setLocalStatus((current) => (
          ['completed', 'failed', 'cancelled', 'conflict'].includes(current || '')
            ? current
            : null
        ));
        setAutoDequeueFlag(f => f + 1);
        // Replace live-only bubbles with their persisted LogEntry ids so every
        // completed Codex turn immediately becomes a valid fork anchor.
        refreshHistoryRef.current();
      }, 500);
      return;
    }

    // Track context window usage
    if (eventType === 'context_usage' && msg.data) {
      const usageIdentity = eventTaskTurnIdentity(msg.data, task.id);
      if (
        (usageIdentity && observeTaskTurn(usageIdentity) < 0)
        || (!usageIdentity && eventDeclaresTaskTurn(msg.data))
      ) {
        return;
      }
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
      const userIdentity = eventTaskTurnIdentity(msg.data, task.id);
      if (
        (userIdentity && observeTaskTurn(userIdentity) < 0)
        || (!userIdentity && eventDeclaresTaskTurn(msg.data))
      ) return;
      const content = (msg.data.content as string) || '';
      const source = (msg.data.source as string) || null;
      const rawContent = typeof msg.data.raw_content === 'string' ? msg.data.raw_content : null;
      const clientMessageId = typeof msg.data.client_message_id === 'string'
        ? msg.data.client_message_id
        : null;
      const imageUrls = (msg.data.image_urls as string[]) || null;
      const attachments = (msg.data.attachments as { url: string; name: string; is_image: boolean }[]) || null;
      const appliedPlans = (msg.data.applied_plans as AppliedPlanSnapshot[]) || null;
      const persistedId = Number(msg.data.id);
      const isPersisted = Number.isFinite(persistedId) && persistedId > 0;
      const eventTimestamp = (msg.data.timestamp as string) || new Date().toISOString();
      const followupOperationId = (
        typeof msg.data.followup_operation_id === 'string'
        && msg.data.followup_operation_id
      ) || null;
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
        client_message_id: clientMessageId,
        applied_plans: appliedPlans,
        followup_operation_id: followupOperationId,
        persisted: isPersisted,
      };
      if (followupOperationId) {
        const alreadyReconciled = reconciledPtyFollowupOperationsRef.current.has(
          followupOperationId,
        );
        const current = activePtyFollowupRef.current;
        // The durable user-message audit may arrive after the boundary. It is
        // still a real chat bubble, but must not resurrect its spinner.
        if (
          !alreadyReconciled
          && (!current || current.operationId === followupOperationId)
        ) {
          activePtyFollowupRef.current = current || {
            operationId: followupOperationId,
            httpSettled: !pendingPtyFollowupRequestRef.current,
          };
          const tracker = activePtyFollowupRef.current;
          const boundary = ptyFollowupBoundaryReceiptsRef.current.get(
            followupOperationId,
          );
          setSending(
            boundary === undefined || tracker?.httpSettled === false,
          );
          if (boundary !== undefined) {
            setPtyFollowupBoundaryEpoch((epoch) => epoch + 1);
          }
        }
      } else {
        setSending(true);
      }
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
              clientMessageId !== null
                ? candidate.client_message_id === clientMessageId
                : (
                  candidate.content === content
                  || (
                    rawContent !== null
                    && candidate.raw_content === rawContent
                  )
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
            client_message_id: clientMessageId ?? optimistic.client_message_id,
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
      const identity = eventTaskTurnIdentity(msg.data, task.id);
      if (!delta || !itemId || !identity) return;
      const turnComparison = observeTaskTurn(identity);
      if (turnComparison < 0) return;
      const renderedType = eventType === 'message_delta' ? 'message' : 'thinking';
      const nativeTurnId = typeof msg.data.native_turn_id === 'string'
        ? msg.data.native_turn_id
        : null;
      setMessages((prev) => {
        const current = turnComparison > 0
          ? removeOtherTurnProvisionals(prev, identity)
          : prev;
        const index = current.findIndex((entry) => (
          messageMatchesStreamItem(entry, identity, itemId)
        ));
        if (index >= 0) {
          const next = [...current];
          next[index] = { ...next[index], content: `${next[index].content || ''}${delta}` };
          syncLiveStreamCache(identity, next);
          return next;
        }
        const next = [...current, {
          id: Date.now() + Math.random(), role: 'assistant', event_type: renderedType,
          content: delta, tool_name: null, tool_input: null, tool_output: null,
          is_error: false, loop_iteration: null, timestamp: new Date().toISOString(),
          image_urls: null, attachments: null, stream_item_id: itemId,
          task_retry_count: identity.retryCount,
          task_turn_generation: identity.turnGeneration,
          native_turn_id: nativeTurnId,
          turn_id: nativeTurnId,
        }];
        syncLiveStreamCache(identity, next);
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
    const identity = eventTaskTurnIdentity(msg.data, task.id);
    let turnComparison = 0;
    if (itemId) {
      if (!identity && !isPersisted) return;
      if (identity) {
        turnComparison = observeTaskTurn(identity);
        if (turnComparison < 0 && !isPersisted) return;
      }
    }
    const nativeTurnId = typeof msg.data.native_turn_id === 'string'
      ? msg.data.native_turn_id
      : typeof msg.data.turn_id === 'string'
        ? msg.data.turn_id
        : null;
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
      task_retry_count: identity?.retryCount ?? null,
      task_turn_generation: identity?.turnGeneration ?? null,
      native_turn_id: nativeTurnId,
      item_id: itemId,
      stream_item_id: itemId,
      turn_id: nativeTurnId,
      native_item_type: (msg.data.native_item_type as string) || null,
      native_item_status: (msg.data.native_item_status as string) || null,
      protocol_anomaly: msg.data.protocol_anomaly === 'legacy_tool_markup'
        ? 'legacy_tool_markup'
        : null,
      pty_cold_start: Boolean(msg.data.pty_cold_start),
      persisted: isPersisted,
    };
    setMessages((prev) => {
      const generationCurrent = turnComparison > 0 && identity
        ? removeOtherTurnProvisionals(prev, identity)
        : prev;
      const current = isPersisted
        ? generationCurrent.filter((candidate) => !candidate.pty_cold_start)
        : generationCurrent;
      if (isPersisted) {
        const next = mergeChatHistory([entry], current);
        syncLiveStreamCache(activeTaskTurnRef.current, next);
        return next;
      }
      if (itemId && identity) {
        const index = current.findIndex((candidate) => (
          messageMatchesStreamItem(candidate, identity, itemId)
        ));
        if (index >= 0) {
          const next = [...current];
          next[index] = entry;
          syncLiveStreamCache(identity, next);
          return next;
        }
      }
      const next = [...current, entry];
      syncLiveStreamCache(activeTaskTurnRef.current, next);
      return next;
    });
  }, [hasControlAccess, observeTaskTurn, onTaskUpdated, refreshVersionedPlans, rememberPtyFollowupBoundary, retireAskUserRequest, task.goal_max_turns, task.id, task.worker_id]);

  const fetchHistory = useCallback(() => {
    const requestEpoch = ++historyRequestEpochRef.current;
    const requestIdentity = { ...activeTaskTurnRef.current };
    const requestTaskTurnVersion = `${task.retry_count}:${task.turn_generation}`;
    setHistoryLoading(true);
    Promise.all([
      api.getTaskChatHistory(
        task.id,
        true,
        HISTORY_PAGE_SIZE,
        0,
        hasControlAccess,
      ),
      api.getAskUserPending(task.id).catch(() => ({ pending: [] as { request_id: string; questions: AskUserQuestion[] }[] })),
    ]).then(([msgs, askPending]) => {
      if (
        historyRequestEpochRef.current !== requestEpoch
        || !sameTaskTurn(requestIdentity, activeTaskTurnRef.current)
        || requestTaskTurnVersion
          !== `${activeTaskTurnRef.current.retryCount}:${activeTaskTurnRef.current.turnGeneration}`
      ) return;
      const filtered = msgs
        .filter((m) =>
          !isLegacyCodexCollabCompleted(m) &&
          !((m.event_type === 'message' || m.event_type === 'result') && !m.content)
        )
        .map((m) => ({ ...m, persisted: true }));
      let receiptChanged = false;
      for (const message of filtered) {
        if (
          message.event_type !== 'pty_background_followup_boundary'
          || !messageMatchesTaskTurn(message, requestIdentity)
          || !message.followup_operation_id
          || (
            message.pty_followup_state !== 'completed'
            && message.pty_followup_state !== 'uncertain'
          )
        ) continue;
        const receipts = ptyFollowupBoundaryReceiptsRef.current;
        if (
          receipts.get(message.followup_operation_id)
          !== message.pty_followup_state
        ) {
          receipts.set(
            message.followup_operation_id,
            message.pty_followup_state,
          );
          receiptChanged = true;
        }
      }
      const historyReceipts = ptyFollowupBoundaryReceiptsRef.current;
      while (historyReceipts.size > 100) {
        const oldest = historyReceipts.keys().next().value as string | undefined;
        if (!oldest) break;
        historyReceipts.delete(oldest);
      }
      const latestFollowupMessage = [...filtered].reverse().find((message) => (
        message.event_type === 'user_message'
        && messageMatchesTaskTurn(message, requestIdentity)
        && Boolean(message.followup_operation_id)
      ));
      const latestOperationId = latestFollowupMessage?.followup_operation_id;
      if (latestOperationId) {
        const boundary = ptyFollowupBoundaryReceiptsRef.current.get(
          latestOperationId,
        );
        const active = activePtyFollowupRef.current;
        if (!active || active.operationId === latestOperationId) {
          if (!boundary && task.background_active === true) {
            activePtyFollowupRef.current = {
              operationId: latestOperationId,
              httpSettled: true,
            };
            setSending(true);
          } else if (
            boundary === 'uncertain'
            && !reconciledPtyFollowupOperationsRef.current.has(
              latestOperationId,
            )
          ) {
            activePtyFollowupRef.current = {
              operationId: latestOperationId,
              httpSettled: true,
            };
            receiptChanged = true;
          } else if (active && boundary) {
            receiptChanged = true;
          }
        }
      }
      if (receiptChanged) {
        setPtyFollowupBoundaryEpoch((epoch) => epoch + 1);
      }
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
        syncLiveStreamCache(activeTaskTurnRef.current, next);
        return next;
      });
    }).catch(() => {}).finally(() => {
      if (historyRequestEpochRef.current === requestEpoch) {
        setHistoryLoading(false);
      }
    });
  }, [
    hasControlAccess,
    task.background_active,
    task.id,
    task.retry_count,
    task.turn_generation,
  ]);
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
      ['completed', 'failed', 'cancelled', 'conflict', 'pending'].includes(effectiveStatus)
    ) {
      clearLiveStreamCache(task.id);
      setSending(false);
      setAutoDequeueFlag(f => f + 1);
    }
  }, [effectiveStatus, task.id]);

  // Load chat history
  useEffect(() => {
    fetchHistory();
  }, [fetchHistory]);

  // Load the unified read model: CCM monitors, one-shot agents, and native
  // provider children all share the same header indicator and detail panel.
  useEffect(() => {
    api.listAllSubAgentSessions(task.id).then(setMonitorSessions).catch(() => {});
  }, [task.id]);


  const activeSubAgentCount = useMemo(
    () => monitorSessions.filter((s) => s.status === 'running').length,
    [monitorSessions]
  );
  const workerManagedTask = task.is_worker_managed;
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
    const frame = requestAnimationFrame(() => {
      const container = messagesContainerRef.current;
      if (container && container.scrollHeight - container.scrollTop - container.clientHeight < 80) {
        api.markTaskRead(task.id).catch(() => {});
      }
    });
    return () => cancelAnimationFrame(frame);
  }, [messages, task.id]);

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
    disabled: deliveryReadOnly || injecting || (!hasTaskSession && !task.shared_from_id),
  });

  useEffect(() => {
    if (deliveryReadOnly || injecting || (!hasTaskSession && !task.shared_from_id)) return;
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
    hasTaskSession,
    task.shared_from_id,
    addChatFiles,
    deliveryReadOnly,
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
    if (!hasControlAccess || task.provider !== 'codex' || !hasTaskSession) return;
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

  const ensureWorkspacePreviewConfigured = async (): Promise<boolean> => {
    if (workspaceReviewCapability?.available) return true;
    const suggestion = workspaceReviewCapability?.suggested_config;
    if (!suggestion) {
      setError(workspaceReviewCapability?.reason || '当前 Project 没有可用的 Preview 配置。');
      return false;
    }
    const commands = [
      ...suggestion.setup.map((item) => `${item.cwd}: ${item.command.join(' ')}`),
      ...suggestion.processes.map((item) => `${item.cwd}: ${item.command.join(' ')}`),
    ].join('\n');
    const approved = window.confirm(
      `首次运行需要保存并信任 Project Preview 配置。\n\n${suggestion.name}\n${commands}\n\n这些命令会以 CCM 当前系统用户执行该工作区代码；服务仅监听本机回环地址，模型/云凭证环境变量会被清除。只应确认你信任的本地分支。是否继续？`,
    );
    if (!approved) return false;
    try {
      const capability = await api.approveWorkspacePreviewConfig(task.id, suggestion);
      setWorkspaceReviewCapability(capability);
      return capability.available;
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : '保存 Preview 配置失败');
      return false;
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
    if (injectMode && canInjectNow && !fromQueue) {
      if (!foregroundActive && !backgroundActive) {
        setError('注入仅在 turn 正在运行时可用；空闲时请关闭注入模式发送普通消息。');
        return;
      }
      if (backgroundActive) {
        retainedFollowupTurnRef.current = { ...activeTaskTurnRef.current };
        setSuppressedCompletedLifecycleTurn({
          ...activeTaskTurnRef.current,
          suppressedAt: Date.now(),
        });
      }
      const outcome = await handleInject(
        text,
        uploadedResultsForTurn,
        false,
        task.provider === 'claude' && backgroundOnly,
      );
      if (outcome === 'no_active_turn') {
        // The Task row may lag behind the provider turn boundary.  Keep the
        // exact input (including Plan metadata and uploads) for a normal next
        // turn instead of asking the user to retype it or replaying the
        // injection request.  Closing inject mode also makes the next action
        // unambiguously use the ordinary chat route.
        addToQueue(
          text,
          uploadedResultsForTurn.length > 0 ? uploadedResultsForTurn : undefined,
          planIdsForTurn.length > 0 ? [...planIdsForTurn] : undefined,
          planVersionIdsForTurn.length > 0 ? [...planVersionIdsForTurn] : undefined,
        );
        setInput('');
        fileUpload.clear();
        consumeForkSeedUploads();
        setInjectMode(false);
        setError('当前 turn 已结束，消息和附件已转入普通队列，将在下一次普通 turn 发送。');
      }
      return;
    }

    // Once the visible root turn has handed ownership to a retained native
    // child/session, a regular follow-up is a new provider input on that same
    // hot session. Route it through the exact injection protocol so it does
    // not sit behind the background marker in the dispatcher queue.
    if (backgroundOnly && canInjectBackgroundFollowup) {
      // The exact injection protocol has no Plan application fields. Preserve
      // those messages as ordinary next-turn work instead of silently dropping
      // their selected Plan/Version metadata.
      if (planIdsForTurn.length > 0 || planVersionIdsForTurn.length > 0) {
        if (fromQueue) {
          setMessageQueue((previous) => [{
            text,
            uploadResults: preUploadedResults,
            planTaskIds: preSelectedPlanIds,
            planVersionIds: preSelectedPlanVersionIds,
          }, ...previous]);
        } else {
          addToQueue(
            text,
            uploadedResultsForTurn.length > 0 ? uploadedResultsForTurn : undefined,
            planIdsForTurn.length > 0 ? [...planIdsForTurn] : undefined,
            planVersionIdsForTurn.length > 0 ? [...planVersionIdsForTurn] : undefined,
          );
          setInput('');
          fileUpload.clear();
          consumeForkSeedUploads();
          setError('后台仍在运行；关联 Plan 的消息已保留在队列中，将在普通 next turn 发送。');
        }
        return;
      }
      retainedFollowupTurnRef.current = { ...activeTaskTurnRef.current };
      setSuppressedCompletedLifecycleTurn({
        ...activeTaskTurnRef.current,
        suppressedAt: Date.now(),
      });
      const outcome = await handleInject(
        text,
        uploadedResultsForTurn,
        fromQueue === true,
        task.provider === 'claude',
      );
      if (outcome !== 'injected' && fromQueue && (text || preUploadedResults?.length)) {
        setMessageQueue((previous) => [{
          text,
          uploadResults: preUploadedResults,
          planTaskIds: preSelectedPlanIds,
          planVersionIds: preSelectedPlanVersionIds,
          requiresConfirmation: outcome === 'uncertain',
        }, ...previous]);
      }
      return;
    }

    // A retained tail cannot safely accept a queued message through /chat.
    // Keep it queued until the exact injection transport becomes available.
    if (backgroundActive && fromQueue) {
      setMessageQueue((previous) => [{
        text,
        uploadResults: preUploadedResults,
        planTaskIds: preSelectedPlanIds,
        planVersionIds: preSelectedPlanVersionIds,
      }, ...previous]);
      return;
    }

    if (frontendReviewComposerMode && !fromQueue && !canStartFrontendReviewGoal) {
      setError(frontendReviewGoalUnavailableReason);
      return;
    }
    if (workspaceReviewComposerMode && !fromQueue && !canStartWorkspaceReview) {
      setError(workspaceReviewUnavailableReason);
      return;
    }
    if (workspaceReviewComposerMode && !fromQueue) {
      if (sendableAttachmentCount > 0) {
        setError('单次黑盒审查暂不接收附件；请在目标分支中保存改动后再启动。');
        return;
      }
      setSending(true);
      setError(null);
      try {
        if (!await ensureWorkspacePreviewConfigured()) return;
        const startedReview = await api.startTestRun(task.id, {
          target_kind: 'current_workspace',
          target: {},
          goal: text,
          profile: 'standard',
          allow_actions: true,
          browser_channel: DEFAULT_BROWSER_CHANNEL,
          viewport_width: 1440,
          viewport_height: 900,
        });
        setStartedWorkspaceReview(startedReview);
        setInput('');
        setWorkspaceReviewComposerMode(false);
        setShowBrowserReviewPanel(true);
        onTaskUpdated?.();
      } catch (nextError) {
        setError(nextError instanceof Error ? nextError.message : '启动当前分支审查失败');
      } finally {
        setSending(false);
      }
      return;
    }
    if (frontendReviewComposerMode && !fromQueue) {
      if (!await ensureWorkspacePreviewConfigured()) return;
    }

    // If currently sending and not from auto-dequeue, add to queue (with already-uploaded results)
    if (foregroundActive && !fromQueue) {
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
    setSuppressedCompletedLifecycleTurn({
      ...activeTaskTurnRef.current,
      suppressedAt: Date.now(),
    });
    setSending(true);
    setError(null);

    let optimisticMessageId: number | null = null;
    let frontendReviewGoalRequestId: number | null = null;
    const clientMessageId = createClientMessageId();
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
          client_message_id: clientMessageId,
          applied_plans: optimisticAppliedPlans.length > 0
            ? optimisticAppliedPlans
            : null,
        }]);
        setSending(true);
      }

      if (frontendReviewComposerMode && !fromQueue) {
        frontendReviewGoalRequestId = ++frontendReviewGoalRequestSequence.current;
        frontendReviewGoalActiveRef.current = true;
        setFrontendReviewGoalLocallyActive(true);
        setFrontendReviewGoalStart({
          requestId: frontendReviewGoalRequestId,
          prompt: text,
          maxTurns: 5,
          phase: 'starting_goal',
        });
        setFrontendReviewGoalProgress({
          turn: 0,
          maxTurns: 5,
          lastReason: null,
          active: true,
        });
        setShowBrowserReviewPanel(true);
        const activatedTask = await api.startFrontendReviewGoal(task.id, {
          message: text,
          file_paths: uploadedPaths,
          secret_ids: selectedSecretIds.length > 0 ? selectedSecretIds : undefined,
          profile: 'standard',
          max_iterations: 5,
          expected_routing: {
            provider: task.provider,
            model: task.model,
            codex_service_tier: task.codex_service_tier,
          },
          client_message_id: clientMessageId,
        });
        setFrontendReviewComposerMode(false);
        setLocalStatus(activatedTask.status || 'pending');
        setFrontendReviewGoalProgress({
          turn: 0,
          maxTurns: activatedTask.goal_max_turns || 5,
          lastReason: null,
          active: true,
        });
        setFrontendReviewGoalStart((current) => (
          current?.requestId === frontendReviewGoalRequestId
            ? { ...current, maxTurns: activatedTask.goal_max_turns || 5 }
            : current
        ));
        onTaskUpdated?.();
      } else {
        const routing = {
          provider: task.provider,
          model: modelOverride || task.model,
          codex_service_tier: task.codex_service_tier,
        };
        const confirmedStalePlanIds: number[] = [];
        let chatResponse: Awaited<ReturnType<typeof api.sendTaskChat>> | null = null;
        for (;;) {
          try {
            if (!hasControlAccess) {
              // A Task chat share may contribute only text and already
              // validated CCM uploads.  Do not serialize the control-plane
              // routing tuple (or any hidden composer state) into its request.
              chatResponse = await api.sendTaskChat(
                task.id,
                text || '(files attached)',
                uploadedPaths,
                undefined,
                undefined,
                undefined,
                undefined,
                undefined,
                undefined,
                undefined,
                clientMessageId,
              );
            } else if (planVersionIdsForTurn.length > 0) {
              chatResponse = await api.sendTaskChat(
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
                clientMessageId,
              );
            } else if (planIdsForTurn.length > 0) {
              chatResponse = await api.sendTaskChat(
                task.id,
                text || '(files attached)',
                uploadedPaths,
                selectedSecretIds.length > 0 ? selectedSecretIds : undefined,
                modelOverride,
                routing,
                planIdsForTurn,
                confirmedStalePlanIds,
                undefined,
                undefined,
                clientMessageId,
              );
            } else {
              chatResponse = await api.sendTaskChat(
                task.id,
                text || '(files attached)',
                uploadedPaths,
                selectedSecretIds.length > 0 ? selectedSecretIds : undefined,
                modelOverride,
                routing,
                undefined,
                undefined,
                undefined,
                undefined,
                clientMessageId,
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
        if (chatResponse?.workspace_review_expected) {
          setExpectedWorkspaceReviewBaseline(
            chatResponse.workspace_review_baseline_run_id,
          );
          setShowBrowserReviewPanel(true);
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
      }
      if (!fromQueue) {
        consumeForkSeedUploads();
      }
      setModelOverride(null);
    } catch (e) {
      setSending(false);
      if (frontendReviewGoalRequestId !== null) {
        setFrontendReviewGoalStart((current) => (
          current?.requestId === frontendReviewGoalRequestId ? null : current
        ));
        setFrontendReviewGoalLocallyActive(false);
        frontendReviewGoalActiveRef.current = false;
      }
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

  const composerHasContent = (
    input.trim().length > 0
    || fileUpload.uploadedResults.length > 0
    || forkSeedUploads.length > 0
  );
  // Context compaction briefly clears the native session while the current
  // foreground generation is still running. Keep the composer usable during
  // that handoff so a follow-up is retained in the queue instead of looking
  // like a dead send button; the queue consumer will send it after recovery.
  const composerNoSessionBlocked = (
    !hasTaskSession
    && !task.shared_from_id
    && !foregroundActive
  );
  const composerSubmitDisabled = (
    !composerHasContent
    || ((frontendReviewComposerMode || workspaceReviewComposerMode) && !input.trim())
    || composerNoSessionBlocked
    || (frontendReviewComposerMode && !canStartFrontendReviewGoal)
    || (workspaceReviewComposerMode && !canStartWorkspaceReview)
    || (injectMode && canInjectNow && !foregroundActive && !backgroundActive)
    || (injecting && (!foregroundActive || injectMode))
    || fileUpload.isUploading
  );
  const composerSubmitTitle = fileUpload.hasFailed
    ? 'Retry or remove failed attachments before sending'
    : injectMode && canInjectNow
      ? (foregroundActive || backgroundActive ? '注入到运行中的 Session (Ctrl+Enter)' : '注入模式：仅在 turn 运行中可用，空闲时请关闭注入模式')
      : frontendReviewComposerMode ? '启动循环审查 (Ctrl+Enter)'
      : workspaceReviewComposerMode ? '启动单次审查 (Ctrl+Enter)'
      : foregroundActive ? 'Add to queue (Ctrl+Enter)' : 'Send (Ctrl+Enter)';

  const handleComposerSubmit = (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    if (composerSubmitDisabled) return;
    void handleSend();
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && (e.ctrlKey || e.metaKey) && !e.nativeEvent.isComposing) {
      e.preventDefault();
      if (!composerSubmitDisabled) e.currentTarget.form?.requestSubmit();
    }
  };

  return (
    <div ref={chatRootRef} className={inline ? "flex flex-col h-full bg-gray-950" : "fixed inset-0 bg-gray-950 flex flex-col z-50"}>
      {/* Header — two rows */}
      <div className="px-3 sm:px-4 py-1.5 pt-[max(0.375rem,env(safe-area-inset-top))] border-b border-gray-800 bg-gray-900">
        {/* Row 1: back + task info + action buttons */}
        <div
          data-testid="chat-header-primary"
          className="flex items-center gap-2 sm:gap-3"
        >
          <div className="flex min-w-0 flex-1 items-center gap-2 sm:gap-3">
            <button onClick={onBack} className="shrink-0 text-gray-400 hover:text-foreground">
              <ArrowLeft size={20} />
            </button>
            <div
              data-testid="chat-task-badges"
              className="flex min-w-0 flex-1 flex-nowrap items-center gap-1.5 overflow-hidden"
            >
              <p className="text-foreground font-medium text-sm whitespace-nowrap">Task #{task.id}</p>
              {task.mode === 'plan' ? (
                <PlanPipelineBadge task={task} />
              ) : (
                <>
                  <span className={`text-xs px-1.5 rounded font-medium whitespace-nowrap ${task.provider === 'codex' ? 'bg-green-600/30 text-green-300' : 'bg-blue-600/30 text-blue-300'}`}>
                    {providerLabel}
                  </span>
                  <FastModeBadge task={task} />
                </>
              )}
              {backgroundActive && (
                <span className="text-xs bg-teal-600/25 text-teal-300 px-1.5 rounded font-medium whitespace-nowrap">
                  后台运行中
                </span>
              )}
              {showFrontendReviewGoal && (
                <span
                  className="whitespace-nowrap rounded bg-indigo-500/20 px-1.5 text-xs font-medium text-indigo-300"
                  title={frontendReviewGoalProgress.lastReason || '模型将自动判断是否继续下一轮'}
                >
                  Goal 审查 · 第 {Math.max(1, frontendReviewGoalProgress.turn + (frontendReviewGoalProgress.active ? 1 : 0))} 轮 · 自动
                </span>
              )}
              {hasControlAccess && !deliveryReadOnly && task.mode !== 'plan' && task.provider === 'codex' && codexMainMcpEnabled !== null && (
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
                  MCP <span className="hidden sm:inline">{codexMainMcpEnabled ? '已启用' : '已关闭'}</span>
                </span>
              )}
              {projectName && (
                <span
                  data-testid="chat-project-badge"
                  className="min-w-0 w-fit max-w-full shrink truncate whitespace-nowrap rounded bg-emerald-600/30 px-1.5 text-xs font-medium text-emerald-300"
                  title={projectName}
                >
                  {projectName}
                </span>
              )}
            </div>
          </div>
          {hasControlAccess && !deliveryReadOnly && task.mode !== 'plan' && (
            <TaskSSHAccessBadge task={task} />
          )}
          <div
            data-testid="chat-header-actions"
            className="flex shrink-0 items-center gap-0.5 sm:gap-1"
          >
            <SubAgentIndicator
              taskId={task.id}
              count={activeSubAgentCount}
              active={activeSubAgentCount > 0}
              onNavigate={hasControlAccess
                ? () => setShowMonitorPanel(!showMonitorPanel)
                : undefined}
            />
            {hasControlAccess && !deliveryReadOnly && (
              <button
                type="button"
                onClick={() => setShowBrowserReviewPanel((value) => !value)}
                className={`relative inline-flex items-center gap-1.5 rounded-lg border px-2 py-1.5 text-xs font-medium transition-all ${
                  browserReviewActive
                    ? 'border-cyan-400/40 bg-cyan-400/12 text-cyan-200 shadow-sm shadow-cyan-950/40'
                    : showBrowserReviewPanel
                      ? 'border-indigo-500/35 bg-indigo-500/15 text-indigo-300'
                      : 'border-transparent text-gray-500 hover:border-indigo-500/25 hover:bg-indigo-500/8 hover:text-indigo-300'
                }`}
                title={browserReviewActive
                  ? '前端 Browser Agent 正在执行，点击查看实时过程'
                  : browserReviewDisplayMode === 'floating' ? '打开前端测试浮窗' : '打开前端测试栏'}
                aria-label="Toggle Frontend Review panel"
              >
                {browserReviewActive
                  ? <Loader2 size={16} className="animate-spin" />
                  : <Eye size={16} />}
                <span className="hidden sm:inline">{browserReviewActive ? '前端测试中' : '前端测试'}</span>
                {browserReviewActive ? (
                  <span className="animate-pulse rounded bg-emerald-400/15 px-1 py-0.5 text-[8px] font-bold uppercase tracking-wide text-emerald-300">Live</span>
                ) : (
                  <span
                    className={`h-1.5 w-1.5 rounded-full ${browserReviewAvailable ? 'bg-emerald-400' : 'bg-gray-500'}`}
                    aria-hidden="true"
                  />
                )}
              </button>
            )}
            {hasControlAccess && !deliveryReadOnly && hasTaskSession && task.shared_from_id == null && (
              <button
                onClick={() => setPlansOpen((open) => !open)}
                className={`flex items-center gap-1.5 rounded px-1.5 py-1 text-xs font-medium transition-colors sm:px-2 ${
                  plansOpen
                    ? 'bg-indigo-500/15 text-indigo-300'
                    : 'text-gray-500 hover:bg-gray-800 hover:text-indigo-300'
                }`}
                title="Independent Plans"
                aria-label="Plans"
              >
                <ListTodo size={16} />
                <span className="hidden sm:inline">Plans</span>
                {planAttentionCount > 0 && (
                  <span className="min-w-4 rounded-full bg-indigo-500 px-1 text-center text-[9px] font-bold leading-4 text-white">
                    {planAttentionCount}
                  </span>
                )}
              </button>
            )}
            {hasControlAccess && !deliveryReadOnly && task.mode !== 'plan' && (
              <TaskConfigBadge task={task} onRefresh={() => onTaskUpdated?.()} align="right" />
            )}
            {hasControlAccess && !deliveryReadOnly && (
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
            )}
            {hasControlAccess && <button
              onClick={handleStar}
              className={`p-1.5 transition-colors ${starred ? 'text-yellow-400 hover:text-yellow-300' : 'text-gray-600 hover:text-yellow-400'}`}
              title={starred ? "Unstar" : "Star"}
            >
              <Star size={18} fill={starred ? 'currentColor' : 'none'} />
            </button>}
            {hasControlAccess && !deliveryReadOnly && (hasActiveWork || stillRunning) && (
              <button
                onClick={async () => {
                  setInterrupting(true);
                  try {
                    const resp = await api.stopTaskSession(task.id);
                    retainedFollowupTurnRef.current = null;
                    setSending(false);
                    setStillRunning(false);
                    const stoppedStatus = resp.task_status
                      || (resp.stopped !== false ? 'completed' : null)
                      || (resp.note?.includes('marked as completed') ? 'completed' : null);
                    if (stoppedStatus) {
                      lastWsStatusAt.current = Date.now();
                      setLocalStatus(stoppedStatus);
                    }
                    if (typeof resp.background_active === 'boolean') {
                      lastWsBackgroundAt.current = Date.now();
                      setLocalBackgroundActive(resp.background_active);
                    } else if (stoppedStatus) {
                      lastWsBackgroundAt.current = Date.now();
                      setLocalBackgroundActive(false);
                    }
                    if (resp.stopped === false) {
                      const cleared = resp.cleared_messages ?? 0;
                      if (stoppedStatus === 'completed') {
                        setError(null);
                      } else {
                        setError(
                          `Interrupt: no running process found${cleared > 0 ? `, cleared ${cleared} queued message(s)` : ''}. ` +
                          'The latest Task status is being refreshed.'
                        );
                      }
                    } else {
                      setError(null);
                    }
                    onTaskUpdated?.();
                  } catch (interruptError) {
                    retainedFollowupTurnRef.current = null;
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
                    onTaskUpdated?.();
                  }
                  finally { setInterrupting(false); }
                }}
                disabled={interrupting}
                className="flex items-center gap-1 px-1.5 py-1.5 text-xs text-red-400 hover:text-red-300 border border-red-500/30 rounded hover:bg-red-500/10 disabled:opacity-50 sm:px-2.5"
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
                {hasControlAccess && !deliveryReadOnly && (
                  <button
                    onClick={() => { setTitleDraft(task.title || ''); setEditingTitle(true); }}
                    className="text-gray-600 hover:text-gray-400 opacity-0 group-hover/title:opacity-100 transition-opacity shrink-0"
                    title="Edit title"
                  >
                    <Pencil size={10} />
                  </button>
                )}
              </div>
            )}
          </div>}
          {deliveryReadOnly || !hasControlAccess ? (
            task.attention_tag ? (
              <span
                className="inline-flex min-w-0 max-w-[45vw] items-center gap-1 rounded-md border border-amber-400/25 bg-amber-500/15 px-1.5 py-0.5 text-xs font-medium text-amber-300 sm:max-w-xs"
                title="Delivery-owned Task attention tag"
              >
                <Pin size={11} className="shrink-0" />
                <span className="truncate">{task.attention_tag}</span>
              </span>
            ) : null
          ) : (
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
          )}
          {contextUsage && (
            <span className="flex items-center shrink-0">
              <ContextUsageIndicator usage={contextUsage} />
            </span>
          )}
        </div>
      </div>

      {hasControlAccess && deliveryReadOnly && task.delivery_run_id != null && (
        <div className="border-b border-gray-800 bg-gray-950 px-3 py-2 sm:px-4">
          <DeliveryRunPanel runId={task.delivery_run_id} />
        </div>
      )}

      {task.metadata_?.forked_from_task_id && (
        <div className="px-4 py-1.5 border-b border-indigo-500/20 bg-indigo-500/5 text-xs text-indigo-300 flex items-center gap-1.5">
          <GitBranch size={12} />
          <span>Forked from Task #{task.metadata_.forked_from_task_id}</span>
        </div>
      )}

      {hasControlAccess && forkOpen && (
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
      {hasControlAccess && showMonitorPanel && (
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

      {hasControlAccess && plansOpen && <VersionedPlansDialog
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
      {hasControlAccess && distillOpen && (
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

      <div className="flex min-h-0 flex-1 flex-col lg:flex-row">
        <div className="flex min-h-0 min-w-0 flex-1 flex-col">
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
              {hasTaskSession
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
              canResolvePermission={hasControlAccess}
              onAskUserResolved={retireAskUserRequest}
            />
          )
        )}
        {backgroundActive && backgroundLifecycle && (() => {
          if (backgroundLifecycle.state === 'completed') {
            return (
              <div className="mx-3 flex items-center gap-2 rounded-lg border border-emerald-500/25 bg-emerald-500/10 px-3 py-2 text-sm text-emerald-300">
                <Check size={14} />
                <span>后台任务已完成，正在收尾…</span>
              </div>
            );
          }
          const lastActivity = Date.parse(
            backgroundLifecycle.last_activity_at || backgroundLifecycle.started_at,
          );
          const silenceMs = Number.isFinite(lastActivity)
            ? Math.max(0, Date.now() - lastActivity)
            : 0;
          const longStalled = silenceMs >= BACKGROUND_LONG_STALLED_AFTER_MS;
          const possiblyStalled = silenceMs >= BACKGROUND_STALLED_AFTER_MS;
          const reason = backgroundLifecycle.reason === 'waiting_for_native_goal'
            ? '原生 Goal'
            : `${backgroundLifecycle.active_count} 个子 Agent`;
          return (
            <div className={`mx-3 rounded-lg border px-3 py-2 text-sm ${
              longStalled
                ? 'border-red-500/30 bg-red-500/10 text-red-300'
                : possiblyStalled
                  ? 'border-amber-500/30 bg-amber-500/10 text-amber-300'
                  : 'border-sky-500/25 bg-sky-500/10 text-sky-300'
            }`}>
              <div className="flex items-center gap-2 font-medium">
                {possiblyStalled
                  ? <AlertCircle size={14} />
                  : <Loader2 size={14} className="animate-spin" />}
                <span>{possiblyStalled ? '后台任务可能停滞' : '主回复已完成，后台仍在运行'}</span>
              </div>
              <div className="mt-1 text-xs opacity-75">
                正在等待{reason} · 最近活动 {formatBackgroundSilence(silenceMs)}
              </div>
            </div>
          );
        })()}
        {backgroundActive && !backgroundLifecycle && (
          <div className="mx-3 flex items-center gap-2 rounded-lg border border-sky-500/25 bg-sky-500/10 px-3 py-2 text-sm text-sky-300">
            <Loader2 size={14} className="animate-spin" />
            <span>{foregroundActive ? '后台子 Agent 仍在运行' : '主回复已完成，后台仍在运行'}</span>
          </div>
        )}
        {foregroundActive && (
          <div className="flex gap-2 items-start text-gray-500 text-sm px-3">
            <Loader2 size={14} className="animate-spin" />
            {showFrontendReviewGoal ? (
              <div>
                <div>Goal Agent 第 {Math.max(1, frontendReviewGoalProgress.turn + 1)} 轮正在执行…</div>
                <div className="mt-0.5 text-[11px] text-gray-500">
                  审查报告不会结束本轮；Agent 会继续处理必要修改、构建测试和修改后复查。
                </div>
              </div>
            ) : isWaitingCapability ? (
              <span>Waiting for requested capability...</span>
            ) : terminalReconciliationPending ? (
              <span>正在确认任务状态...</span>
            ) : (
              <span>{providerLabel} is thinking...</span>
            )}
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
      {!deliveryReadOnly && messageQueue.length > 0 && (
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
                    {item.requiresConfirmation && (
                      <span
                        className="inline-flex shrink-0 items-center gap-0.5 text-red-300"
                        title="上次发送结果不确定；此消息不会自动重试，请先核对聊天记录后手动编辑发送"
                      >
                        <AlertCircle size={10} />
                        <span className="text-[10px]">需确认</span>
                      </span>
                    )}
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
      {deliveryReadOnly ? (
        <div
          className="border-t border-indigo-500/20 bg-indigo-500/5 px-4 py-3 text-center text-xs text-indigo-200"
          data-testid="delivery-read-only"
        >
          Delivery Run #{task.delivery_run_id ?? '?'} owns this Developer Task. Its conversation is read-only; workflow controls and PR status are shown above.
        </div>
      ) : (
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
                || (!hasTaskSession && !task.shared_from_id)
                || fileUpload.uploads.length + forkSeedUploads.length >= MAX_FILES
              }
              className="p-2 text-gray-500 hover:text-gray-300 disabled:opacity-40 disabled:cursor-not-allowed"
              title="Attach files"
            >
              <Paperclip size={18} />
            </button>
            {hasControlAccess && (
              <SecretPicker
                selectedIds={selectedSecretIds}
                onChange={setSelectedSecretIds}
                disabled={injecting || (!hasTaskSession && !task.shared_from_id) || (injectMode && canInjectNow)}
              />
            )}
            <QuickPhraseDropdown onSelect={(text) => handleSend(text)} disabled={injecting || frontendReviewComposerMode || workspaceReviewComposerMode || (!hasTaskSession && !task.shared_from_id)} />
            {hasControlAccess && task.worker_id == null && task.shared_from_id == null && (
              <button
                type="button"
                onClick={() => {
                  setWorkspaceReviewComposerMode((value) => !value);
                  setFrontendReviewComposerMode(false);
                  setInjectMode(false);
                  setModelOverride(null);
                }}
                disabled={injecting || !canStartWorkspaceReview}
                aria-label="单次审查当前分支"
                aria-pressed={workspaceReviewComposerMode}
                className={`inline-flex h-8 w-8 shrink-0 items-center justify-center rounded-md transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-cyan-400 disabled:cursor-not-allowed disabled:opacity-40 ${
                  workspaceReviewComposerMode
                    ? 'bg-cyan-500/15 text-cyan-300 ring-1 ring-inset ring-cyan-400/40'
                    : 'text-gray-500 hover:bg-cyan-500/10 hover:text-cyan-300'
                }`}
                title={canStartWorkspaceReview
                  ? workspaceReviewComposerMode
                    ? '单次审查已选择：启动隔离 Preview，并由独立浏览器 Agent 黑盒测试'
                    : '单次审查当前开发分支（不修改代码）'
                  : workspaceReviewUnavailableReason}
              >
                <Eye size={16} />
              </button>
            )}
            {hasControlAccess && task.worker_id == null && task.shared_from_id == null && (
              <button
                type="button"
                onClick={() => {
                  setFrontendReviewComposerMode((value) => !value);
                  setWorkspaceReviewComposerMode(false);
                  setInjectMode(false);
                  setModelOverride(null);
                }}
                disabled={injecting || !canStartFrontendReviewGoal}
                aria-label="循环审查"
                aria-pressed={frontendReviewComposerMode}
                className={`inline-flex h-8 w-8 shrink-0 items-center justify-center rounded-md transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-indigo-400 disabled:cursor-not-allowed disabled:opacity-40 ${
                  frontendReviewComposerMode
                    ? 'bg-indigo-500/15 text-indigo-400 ring-1 ring-inset ring-indigo-400/40'
                    : 'text-gray-500 hover:bg-indigo-500/10 hover:text-indigo-400'
                }`}
                title={canStartFrontendReviewGoal
                  ? frontendReviewComposerMode
                    ? '循环审查已选择：发送后在当前 Task/session 中自动审查、修改并复查'
                    : '启动循环审查（当前 Task/session，最多 5 轮）'
                  : frontendReviewGoalUnavailableReason}
              >
                <RefreshCw size={16} />
              </button>
            )}
            {/* Temp model override (one-shot) */}
            <div className="relative" data-temp-model>
              <button
                type="button"
                onClick={() => setShowModelMenu((v) => !v)}
                disabled={!hasControlAccess || injecting || frontendReviewComposerMode || workspaceReviewComposerMode || (!hasTaskSession && !task.shared_from_id)}
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
            {canInjectNow && (
              <button
                type="button"
                onClick={() => setInjectMode((v) => !v)}
                disabled={injecting || !hasTaskSession}
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
            {hasControlAccess && task.provider === 'codex' && hasTaskSession && task.worker_id == null && task.shared_from_id == null && (
              <ForkButton onClick={openFork} disabled={hasActiveWork} />
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
          {injectMode && canInjectNow && (
            <div className="text-[10px] leading-relaxed text-teal-300/80">
              文本、图片和文件会注入当前 turn；只有服务器明确确认成功后才会清空输入和附件。
            </div>
          )}
          {frontendReviewComposerMode && (
            <div className="flex min-w-0 items-center gap-1.5 text-[10px] leading-relaxed text-indigo-400">
              <RefreshCw size={11} className="shrink-0" />
              <span className="truncate">当前 Task/session：浏览器审查 → 必要修改 → 测试 → 重新审查（最多 5 轮）</span>
            </div>
          )}
          {workspaceReviewComposerMode && (
            <div className="flex min-w-0 items-center gap-1.5 text-[10px] leading-relaxed text-cyan-300">
              <Eye size={11} className="shrink-0" />
              <span className="truncate">单次黑盒审查当前分支：隔离 Preview → 浏览器验证 → 截图与报告（不修改代码）</span>
            </div>
          )}
          <form className="flex gap-2 items-end" onSubmit={handleComposerSubmit}>
            <textarea
              ref={textareaRef}
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder={
                composerNoSessionBlocked
                  ? 'Run the task first to start a session...'
                  : injectMode && canInjectNow
                    ? '注入模式：消息将直接注入运行中的 turn...'
                    : frontendReviewComposerMode
                      ? '描述这次要循环审查的页面、URL、流程或前端改动...'
                    : workspaceReviewComposerMode
                      ? '描述要验证的功能、页面和关键流程；无需提供 URL...'
                    : foregroundActive
                      ? 'Type next message to queue...'
                      : 'Type a follow-up message...'
              }
              disabled={(injecting && (!foregroundActive || injectMode)) || composerNoSessionBlocked}
              rows={1}
              className="min-w-0 flex-1 bg-gray-800 text-foreground rounded-xl px-4 py-2.5 text-sm border border-gray-700/70 focus:outline-none focus:border-indigo-500 focus:ring-2 focus:ring-indigo-500/25 resize-none disabled:opacity-50 max-h-48 overflow-y-auto transition-colors"
              style={{ minHeight: '40px' }}
            />
            <button
              type="submit"
              disabled={composerSubmitDisabled}
              title={composerSubmitTitle}
              aria-label={composerSubmitTitle}
              className={`inline-flex h-10 w-10 shrink-0 touch-manipulation items-center justify-center p-2.5 text-white rounded-xl transition-colors disabled:opacity-40 disabled:cursor-not-allowed shadow-md ${
                injectMode && canInjectNow ? 'bg-teal-600 hover:bg-teal-700 shadow-teal-600/20'
                : frontendReviewComposerMode ? 'bg-indigo-600 hover:bg-indigo-500 shadow-indigo-600/25'
                : workspaceReviewComposerMode ? 'bg-cyan-600 hover:bg-cyan-500 shadow-cyan-600/25'
                : foregroundActive ? 'bg-amber-600 hover:bg-amber-700 shadow-amber-600/20' : 'bg-indigo-600 hover:bg-indigo-500 shadow-indigo-600/25'
              }`}
            >
              {injectMode && canInjectNow ? <Syringe size={18} /> : frontendReviewComposerMode ? <RefreshCw size={18} /> : workspaceReviewComposerMode ? <Eye size={18} /> : foregroundActive ? <ListPlus size={18} /> : <Send size={18} />}
            </button>
          </form>
          </div>
        </div>
      </div>
      )}
        </div>
        {hasControlAccess && <BrowserReviewPanel
          taskId={task.id}
          taskActive={hasActiveWork}
          taskProvider={task.provider}
          taskModel={task.model}
          taskEffort={task.effort_level}
          taskServiceTier={task.codex_service_tier}
          canStartConfiguredReview={canStartConfiguredBrowserReview}
          configuredReviewUnavailableReason={configuredBrowserReviewUnavailableReason || undefined}
          open={showBrowserReviewPanel}
          displayMode={browserReviewDisplayMode}
          onAvailableChange={handleBrowserReviewAvailable}
          onActiveChange={handleBrowserReviewActive}
          onClose={() => setShowBrowserReviewPanel(false)}
          onDisplayModeChange={handleBrowserReviewDisplayModeChange}
          onNewReview={handleNewBrowserReview}
          startedWorkspaceRun={startedWorkspaceReview}
          expectedWorkspaceReviewBaseline={expectedWorkspaceReviewBaseline}
          onExpectedWorkspaceReviewFound={handleExpectedWorkspaceReviewFound}
          goalStart={frontendReviewGoalStart}
          onGoalReviewFound={handleFrontendReviewGoalFound}
          goalProgress={showFrontendReviewGoal ? frontendReviewGoalProgress : undefined}
        />}
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
function PermissionCard({
  message,
  taskId,
  canResolve,
}: {
  message: ChatMessage;
  taskId?: number;
  canResolve: boolean;
}) {
  const [submitting, setSubmitting] = useState(false);
  const [localStatus, setLocalStatus] = useState<string | null>(null);
  const status = localStatus || message.permission_status || (message.request_id ? 'pending' : 'expired');
  const actionable = canResolve
    && status === 'pending'
    && !!message.request_id
    && taskId !== undefined;

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
    pending: { text: '等待 Task 控制者处理', cls: 'text-amber-300' },
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
  onResolved?: (requestId: string) => void;
}) {
  const questions = message.ask_questions || [];
  const [submitting, setSubmitting] = useState(false);
  const [localStatus, setLocalStatus] = useState<string | null>(null);
  const [submitError, setSubmitError] = useState<string | null>(null);
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
    setSubmitError(null);
    try {
      await api.submitAskUser(taskId!, message.request_id!, answers);
      setLocalStatus('answered');
      onResolved?.(message.request_id!);
    } catch (error) {
      if (isApiRequestError(error) && error.status === 410) {
        setLocalStatus('expired');
        onResolved?.(message.request_id!);
      } else {
        setSubmitError('回答提交结果未确认，卡片已保留，请重试。');
      }
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
        {submitError && (
          <p className="mt-1.5 text-xs text-red-400" role="alert">{submitError}</p>
        )}
      </div>
    </div>
  );
}

function ProtocolAnomalyCard({ message }: { message: ChatMessage }) {
  const originalText = message.content || '';
  return (
    <div className="mx-4" role="alert" data-protocol-anomaly="legacy_tool_markup">
      <div className="rounded-lg border border-amber-500/40 bg-amber-500/10 px-3 py-2.5 text-sm">
        <div className="flex items-start gap-2 text-amber-300">
          <AlertCircle size={16} className="mt-0.5 shrink-0" />
          <div className="min-w-0 flex-1">
            <div className="font-medium">工具协议异常</div>
            <div className="mt-0.5 text-xs leading-5 text-amber-200/80">
              以下内容是模型输出的未解析工具标记，未作为工具调用执行。
            </div>
          </div>
          {message.timestamp && (
            <MessageTimestamp timestamp={message.timestamp} className="shrink-0" />
          )}
        </div>
        {originalText && (
          <details className="mt-2 rounded-md border border-amber-500/20 bg-gray-950/40">
            <summary className="cursor-pointer select-none px-3 py-2 text-xs text-amber-200/80 marker:text-amber-400">
              查看未执行原文
            </summary>
            <div className="group relative border-t border-amber-500/20">
              <CopyButton text={originalText} />
              <pre
                aria-label="未执行的工具标记原文"
                className="max-h-80 overflow-auto whitespace-pre-wrap break-words px-3 py-2 pr-9 text-xs leading-5 text-gray-300 [overflow-wrap:anywhere]"
              >
                {originalText}
              </pre>
            </div>
          </details>
        )}
      </div>
    </div>
  );
}

const MessageBubble = memo(function MessageBubble({
  message,
  taskId,
  canResolvePermission,
  onAskUserResolved,
}: {
  message: ChatMessage;
  taskId: number;
  canResolvePermission: boolean;
  onAskUserResolved?: (requestId: string) => void;
}) {
  const isUser = message.role === 'user';

  if (
    message.event_type === 'background_lifecycle'
    || message.event_type === 'pty_background_followup_boundary'
  ) return null;

  if (message.event_type === 'permission_request') {
    return (
      <PermissionCard
        message={message}
        taskId={taskId}
        canResolve={canResolvePermission}
      />
    );
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

  if (
    message.role === 'assistant'
    && message.protocol_anomaly === 'legacy_tool_markup'
  ) {
    return <ProtocolAnomalyCard message={message} />;
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
