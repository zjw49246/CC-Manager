import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { PointerEvent as ReactPointerEvent } from 'react';
import { createPortal } from 'react-dom';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

import { api } from '../../api/client';
import type {
  BrowserReviewJob,
  CodexServiceTier,
  TestHarnessCapabilities,
  TestHarnessRun,
  TestHarnessRuntimeConfig,
  TestHarnessTargetKind,
  WorkspaceReviewRun,
} from '../../api/client';
import { DEFAULT_BROWSER_CHANNEL } from '../../config/browserReview';
import {
  Activity,
  AlertCircle,
  CheckCircle2,
  Clock,
  Download,
  Eye,
  FileText,
  GripVertical,
  Image,
  Loader2,
  PanelLeftOpen,
  Play,
  RefreshCw,
  Settings,
  Shield,
  Square,
  ChevronDown,
  ChevronUp,
  X,
} from '../icons';

const TERMINAL = new Set(['completed', 'failed', 'cancelled', 'stale']);
const STAGE_LABELS: Record<string, string> = {
  validating_workspace: '正在校验本地仓库',
  fingerprinted: '已锁定当前工作区版本',
  starting_preview: '正在启动隔离预览',
  waiting_for_preview: '等待预览服务就绪',
  creating_agent: '正在创建黑盒审查 Agent',
  preview_ready: '隔离预览已就绪',
  browser_agent_queued: '黑盒浏览器 Agent 已排队',
  reviewing: '浏览器 Agent 正在审查',
  checking_fingerprint: '正在核对工作区版本',
  publishing_report: '正在回传 Task 报告',
  cleaning_up: '正在清理预览进程',
  queued: '等待工具启动',
  waiting_for_browser: '正在准备浏览器',
  browser_ready: '页面已打开',
  executing_actions: '正在验证页面状态',
  agent_reported: '正在保存报告',
  completed: '审查完成',
  browser_closed: '浏览器已关闭',
  cancelling: '正在停止',
  cancelled: '已停止',
  failed: '审查失败',
  stale: '结果已过期',
  interrupted: '服务重启中断',
  resolving_target: '正在解析 Git 目标',
  target_resolved: '已锁定精确 Git 提交',
  preparing_sandbox: '正在创建隔离 Sandbox',
  acquiring_source: '正在 Sandbox 内获取源码',
  preparing_preview: '正在隔离环境准备 Preview',
  resolving_git_target: '正在解析 Git 目标',
  detached_worktree_ready: '隔离 Git worktree 已就绪',
  preparing_environment: '正在准备测试环境',
  collecting_evidence: '正在归档测试证据',
  evaluating: '正在生成结构化结论',
};

const TARGET_LABELS: Record<TestHarnessTargetKind, string> = {
  current_workspace: '当前工作区',
  fixed_url: '固定 URL',
  pull_request: 'GitHub PR',
  git_ref: 'Git ref / 分支',
};

interface BrowserReviewPanelProps {
  taskId: number;
  taskActive: boolean;
  taskProvider?: string;
  taskModel?: string | null;
  taskEffort?: string | null;
  taskServiceTier?: string;
  canStartConfiguredReview?: boolean;
  configuredReviewUnavailableReason?: string;
  open: boolean;
  displayMode: BrowserReviewDisplayMode;
  onAvailableChange: (available: boolean) => void;
  onClose: () => void;
  onDisplayModeChange: (mode: BrowserReviewDisplayMode) => void;
  onNewReview: () => void;
  startedWorkspaceRun?: TestHarnessRun | null;
  expectedWorkspaceReviewBaseline?: string | null;
  onExpectedWorkspaceReviewFound?: () => void;
  goalStart?: BrowserReviewGoalStart | null;
  onGoalReviewFound?: () => void;
  goalProgress?: BrowserReviewGoalProgress;
}

export type BrowserReviewDisplayMode = 'docked' | 'floating';

export interface BrowserReviewGoalProgress {
  turn: number;
  maxTurns: number;
  lastReason: string | null;
  active: boolean;
}

export interface BrowserReviewGoalStart {
  requestId: number;
  prompt: string;
  maxTurns: number;
  phase: 'starting_goal' | 'starting_review';
}

interface FloatingPosition {
  x: number;
  y: number;
}

const FLOATING_POSITION_KEY = 'ccm-browser-review-floating-position';
const FLOATING_WIDTH = 430;
const FLOATING_MARGIN = 12;
const FLOATING_HEADER_HEIGHT = 54;
const DEFAULT_REVIEW_GOAL = '审查这个前端页面的视觉布局、交互反馈、明显的可访问性问题，以及控制台和网络错误。按严重程度输出问题、证据和复现步骤。';

function clampFloatingPosition(position: FloatingPosition): FloatingPosition {
  const width = Math.min(FLOATING_WIDTH, Math.max(280, window.innerWidth - FLOATING_MARGIN * 2));
  return {
    x: Math.max(FLOATING_MARGIN, Math.min(position.x, window.innerWidth - width - FLOATING_MARGIN)),
    y: Math.max(FLOATING_MARGIN, Math.min(position.y, window.innerHeight - FLOATING_HEADER_HEIGHT - FLOATING_MARGIN)),
  };
}

function loadFloatingPosition(): FloatingPosition {
  try {
    const parsed = JSON.parse(localStorage.getItem(FLOATING_POSITION_KEY) || 'null') as Partial<FloatingPosition> | null;
    if (typeof parsed?.x === 'number' && typeof parsed?.y === 'number') {
      return clampFloatingPosition({ x: parsed.x, y: parsed.y });
    }
  } catch { /* storage may be unavailable */ }
  return clampFloatingPosition({
    x: window.innerWidth - FLOATING_WIDTH - 24,
    y: Math.max(72, window.innerHeight - 690),
  });
}

function errorText(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function statusClass(status: string): string {
  if (status === 'completed') return 'border-emerald-500/30 bg-emerald-500/10 text-emerald-300';
  if (status === 'failed') return 'border-red-500/30 bg-red-500/10 text-red-300';
  if (status === 'cancelled') return 'border-gray-600 bg-gray-700/50 text-gray-300';
  return 'border-blue-500/30 bg-blue-500/10 text-blue-300';
}

export function BrowserReviewPanel({
  taskId,
  taskActive,
  taskProvider = 'codex',
  taskModel = null,
  taskEffort = null,
  taskServiceTier = 'default',
  canStartConfiguredReview = !taskActive,
  configuredReviewUnavailableReason,
  open,
  displayMode,
  onAvailableChange,
  onClose,
  onDisplayModeChange,
  onNewReview,
  startedWorkspaceRun,
  expectedWorkspaceReviewBaseline,
  onExpectedWorkspaceReviewFound,
  goalStart,
  onGoalReviewFound,
  goalProgress,
}: BrowserReviewPanelProps) {
  const [runs, setRuns] = useState<TestHarnessRun[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [waitingForWorkspaceReview, setWaitingForWorkspaceReview] = useState(
    expectedWorkspaceReviewBaseline !== undefined,
  );
  const [error, setError] = useState<string | null>(null);
  const [screenshotUrl, setScreenshotUrl] = useState<string | null>(null);
  const screenshotObjectUrlRef = useRef<string | null>(null);
  const [minimized, setMinimized] = useState(false);
  const [cancellingJobId, setCancellingJobId] = useState<string | null>(null);
  const [repeatingRunId, setRepeatingRunId] = useState<string | null>(null);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [configuredTargetKind, setConfiguredTargetKind] = useState<TestHarnessTargetKind>('fixed_url');
  const [configuredUrl, setConfiguredUrl] = useState('');
  const [configuredPrNumber, setConfiguredPrNumber] = useState('');
  const [configuredGitRef, setConfiguredGitRef] = useState('');
  const [configuredGoal, setConfiguredGoal] = useState(DEFAULT_REVIEW_GOAL);
  const [configuredProfile, setConfiguredProfile] = useState<'quick' | 'standard' | 'exhaustive'>('standard');
  const [configuredViewport, setConfiguredViewport] = useState('1440x900');
  const [configuredBrowserChannel, setConfiguredBrowserChannel] = useState<'chrome' | 'chromium'>(DEFAULT_BROWSER_CHANNEL);
  const [configuredAllowActions, setConfiguredAllowActions] = useState(false);
  const [runtimeConfig, setRuntimeConfig] = useState<TestHarnessRuntimeConfig | null>(null);
  const [capabilities, setCapabilities] = useState<TestHarnessCapabilities | null>(null);
  const [capabilitiesLoading, setCapabilitiesLoading] = useState(false);
  const [runtimeConfigLoading, setRuntimeConfigLoading] = useState(false);
  const [runtimeConfigSaving, setRuntimeConfigSaving] = useState(false);
  const [inheritTaskRuntime, setInheritTaskRuntime] = useState(true);
  const [reviewProvider, setReviewProvider] = useState<'claude' | 'codex'>(
    taskProvider === 'claude' ? 'claude' : 'codex',
  );
  const [reviewModel, setReviewModel] = useState(taskModel || '');
  const [reviewEffort, setReviewEffort] = useState(taskEffort || 'medium');
  const [reviewServiceTier, setReviewServiceTier] = useState<CodexServiceTier>(
    taskServiceTier === 'priority' ? 'priority' : 'default',
  );
  const [startingConfiguredReview, setStartingConfiguredReview] = useState(false);
  const [floatingPosition, setFloatingPosition] = useState<FloatingPosition>(loadFloatingPosition);
  const floatingPanelRef = useRef<HTMLElement | null>(null);
  const dragRef = useRef<{ offsetX: number; offsetY: number } | null>(null);
  const latestReviewIdRef = useRef<string | null>(null);
  const startedWorkspaceRunRef = useRef<TestHarnessRun | null>(null);
  const expectedWorkspaceReviewBaselineRef = useRef<string | null | undefined>(undefined);
  const expectedGoalReviewBaselineRef = useRef<string | null | undefined>(undefined);
  const goalReviewRequestIdRef = useRef<number | null>(null);

  const applyRuntimeConfig = useCallback((config: TestHarnessRuntimeConfig) => {
    setRuntimeConfig(config);
    setInheritTaskRuntime(config.inherit_task);
    setReviewProvider(config.provider);
    setReviewModel(config.model);
    setReviewEffort(config.reasoning_effort);
    setReviewServiceTier(config.codex_service_tier);
  }, []);

  const loadRuntimeConfig = useCallback(async () => {
    setRuntimeConfigLoading(true);
    try {
      const config = await api.getTestHarnessRuntimeConfig(taskId);
      applyRuntimeConfig(config);
      return config;
    } catch (nextError) {
      setError(`加载 Browser Agent 配置失败：${errorText(nextError)}`);
      return null;
    } finally {
      setRuntimeConfigLoading(false);
    }
  }, [applyRuntimeConfig, taskId]);

  const loadCapabilities = useCallback(async () => {
    setCapabilitiesLoading(true);
    try {
      const nextCapabilities = await api.getTestHarnessCapabilities(taskId);
      setCapabilities(nextCapabilities);
      return nextCapabilities;
    } catch (nextError) {
      setError(`加载测试能力失败：${errorText(nextError)}`);
      return null;
    } finally {
      setCapabilitiesLoading(false);
    }
  }, [taskId]);

  const refresh = useCallback(async () => {
    try {
      const nextRuns = await api.listTestRuns(taskId);
      const seededRun = startedWorkspaceRunRef.current;
      const visibleRuns = seededRun && !nextRuns.some((run) => run.id === seededRun.id)
        ? [seededRun, ...nextRuns]
        : nextRuns;
      const nextLatestId = visibleRuns[0]?.id ?? null;
      const expectedBaseline = expectedWorkspaceReviewBaselineRef.current;
      const expectedWorkspaceRun = expectedBaseline !== undefined
        && visibleRuns[0]
        && visibleRuns[0].id !== expectedBaseline
        ? visibleRuns[0]
        : null;
      const expectedGoalBaseline = expectedGoalReviewBaselineRef.current;
      const expectedGoalRun = expectedGoalBaseline !== undefined
        && visibleRuns[0]
        && visibleRuns[0].id !== expectedGoalBaseline
        ? visibleRuns[0]
        : null;
      const previousLatestId = latestReviewIdRef.current;
      const hasNewReview = Boolean(
        previousLatestId
        && nextLatestId
        && previousLatestId !== nextLatestId,
      );
      latestReviewIdRef.current = nextLatestId;
      setRuns(visibleRuns);
      setSelectedId((current) => (
        expectedGoalRun || expectedWorkspaceRun
          ? (expectedGoalRun || expectedWorkspaceRun)!.id
          : !hasNewReview && current && (
          visibleRuns.some((run) => run.id === current)
        )
          ? current
          : nextLatestId
      ));
      if (expectedGoalRun) {
        expectedGoalReviewBaselineRef.current = undefined;
        setMinimized(false);
        setSettingsOpen(false);
        onGoalReviewFound?.();
        onNewReview();
      } else if (expectedWorkspaceRun) {
        expectedWorkspaceReviewBaselineRef.current = undefined;
        setWaitingForWorkspaceReview(false);
        setMinimized(false);
        setSettingsOpen(false);
        onExpectedWorkspaceReviewFound?.();
        onNewReview();
      } else if (hasNewReview) {
        setMinimized(false);
        setSettingsOpen(false);
        onNewReview();
      }
      setError(null);
      onAvailableChange(visibleRuns.length > 0);
    } catch (nextError) {
      setError(errorText(nextError));
    } finally {
      setLoading(false);
    }
  }, [onAvailableChange, onExpectedWorkspaceReviewFound, onGoalReviewFound, onNewReview, taskId]);

  useEffect(() => {
    startedWorkspaceRunRef.current = null;
    expectedWorkspaceReviewBaselineRef.current = undefined;
    expectedGoalReviewBaselineRef.current = undefined;
    goalReviewRequestIdRef.current = null;
    setRuns([]);
    setSelectedId(null);
    setLoading(true);
    setWaitingForWorkspaceReview(false);
    setSettingsOpen(false);
    setConfiguredTargetKind('fixed_url');
    setConfiguredUrl('');
    setConfiguredPrNumber('');
    setConfiguredGitRef('');
    setCapabilities(null);
    setCapabilitiesLoading(false);
    latestReviewIdRef.current = null;
    onAvailableChange(false);
    void refresh();
  }, [onAvailableChange, refresh, taskId]);

  useEffect(() => {
    setRuntimeConfig(null);
    setRuntimeConfigLoading(false);
    setRuntimeConfigSaving(false);
    setInheritTaskRuntime(true);
    setReviewProvider(taskProvider === 'claude' ? 'claude' : 'codex');
    setReviewModel(taskModel || '');
    setReviewEffort(taskEffort || 'medium');
    setReviewServiceTier(taskServiceTier === 'priority' ? 'priority' : 'default');
  }, [taskEffort, taskId, taskModel, taskProvider, taskServiceTier]);

  useEffect(() => {
    if (!startedWorkspaceRun || startedWorkspaceRun.task_id !== taskId) return;
    startedWorkspaceRunRef.current = startedWorkspaceRun;
    latestReviewIdRef.current = startedWorkspaceRun.id;
    setRuns((current) => [
      startedWorkspaceRun,
      ...current.filter((run) => run.id !== startedWorkspaceRun.id),
    ]);
    setSelectedId(startedWorkspaceRun.id);
    setLoading(false);
    setError(null);
    setMinimized(false);
    setSettingsOpen(false);
    onAvailableChange(true);
  }, [onAvailableChange, startedWorkspaceRun, taskId]);

  useEffect(() => {
    expectedWorkspaceReviewBaselineRef.current = expectedWorkspaceReviewBaseline;
    setWaitingForWorkspaceReview(expectedWorkspaceReviewBaseline !== undefined);
    if (expectedWorkspaceReviewBaseline === undefined) return;
    setLoading(true);
    setError(null);
    setMinimized(false);
    setSettingsOpen(false);
    void refresh();
  }, [expectedWorkspaceReviewBaseline, refresh]);

  useEffect(() => {
    if (!goalStart) {
      expectedGoalReviewBaselineRef.current = undefined;
      goalReviewRequestIdRef.current = null;
      return;
    }
    if (goalReviewRequestIdRef.current === goalStart.requestId) return;
    goalReviewRequestIdRef.current = goalStart.requestId;
    expectedGoalReviewBaselineRef.current = latestReviewIdRef.current;
    setLoading(true);
    setError(null);
    setMinimized(false);
    setSettingsOpen(false);
    void refresh();
  }, [goalStart, refresh]);

  const hasActiveReview = runs.some((run) => !TERMINAL.has(run.status));
  const waitingForGoalReview = goalStart != null;
  useEffect(() => {
    if (!taskActive && !hasActiveReview && !waitingForWorkspaceReview && !waitingForGoalReview) return;
    const timer = window.setInterval(() => { void refresh(); }, 1000);
    return () => window.clearInterval(timer);
  }, [hasActiveReview, refresh, taskActive, waitingForGoalReview, waitingForWorkspaceReview]);

  const harnessRun = runs.find((item) => item.id === selectedId)
    ?? runs[0]
    ?? null;
  const displayedRun = waitingForWorkspaceReview || waitingForGoalReview ? null : harnessRun;
  const selectedRunIndex = harnessRun
    ? runs.findIndex((item) => item.id === harnessRun.id)
    : -1;
  const selectAdjacentRun = (offset: number) => {
    if (selectedRunIndex < 0) return;
    const nextRun = runs[selectedRunIndex + offset];
    if (nextRun) setSelectedId(nextRun.id);
  };
  const workspaceRun: WorkspaceReviewRun | null = displayedRun?.workspace_review ?? null;
  const job: BrowserReviewJob | null = displayedRun?.browser_review ?? null;
  const displayedObjective = String(displayedRun?.test_plan.objective || '前端黑盒测试').trim();
  const resolvedTarget = displayedRun?.resolved_target || null;
  const resolvedRepository = typeof resolvedTarget?.repository === 'string'
    ? resolvedTarget.repository
    : null;
  const resolvedHead = typeof resolvedTarget?.head_sha === 'string'
    ? resolvedTarget.head_sha
    : displayedRun?.source_git_head || null;
  const resolvedBase = typeof resolvedTarget?.base_sha === 'string'
    ? resolvedTarget.base_sha
    : null;
  const resolvedChangedFiles = Array.isArray(resolvedTarget?.changed_files)
    ? resolvedTarget.changed_files.filter((item): item is Record<string, unknown> => (
      item != null && typeof item === 'object' && !Array.isArray(item)
    ))
    : [];
  const showBrowserGoal = Boolean(
    job?.goal?.trim()
    && job.goal.trim() !== displayedObjective
    && job.goal.trim() !== workspaceRun?.goal?.trim(),
  );
  const harnessRunId = displayedRun?.id ?? null;
  const latestScreenshot = job?.latest_screenshot ?? null;

  useEffect(() => {
    if (!latestScreenshot) {
      if (screenshotObjectUrlRef.current) URL.revokeObjectURL(screenshotObjectUrlRef.current);
      screenshotObjectUrlRef.current = null;
      setScreenshotUrl(null);
      return;
    }
    let active = true;
    if (!harnessRunId) return;
    api.getTestRunEvidence(taskId, harnessRunId, latestScreenshot)
      .then((blob) => {
        const nextObjectUrl = URL.createObjectURL(blob);
        if (!active) {
          URL.revokeObjectURL(nextObjectUrl);
          return;
        }
        const previousObjectUrl = screenshotObjectUrlRef.current;
        screenshotObjectUrlRef.current = nextObjectUrl;
        setScreenshotUrl(nextObjectUrl);
        if (previousObjectUrl) URL.revokeObjectURL(previousObjectUrl);
      })
      .catch((nextError) => {
        if (active) setError(errorText(nextError));
      });
    return () => {
      active = false;
    };
  }, [harnessRunId, latestScreenshot, taskId]);

  useEffect(() => () => {
    if (screenshotObjectUrlRef.current) URL.revokeObjectURL(screenshotObjectUrlRef.current);
  }, []);

  const telemetry = useMemo(() => Object.entries(job?.telemetry || {})
    .filter((entry): entry is [string, Record<string, unknown>[]] => Array.isArray(entry[1]))
    .filter(([, entries]) => entries.length > 0), [job?.telemetry]);

  const reviewModels = runtimeConfig?.models_by_provider[reviewProvider]
    || (reviewModel ? [reviewModel] : []);
  const reviewEfforts = runtimeConfig?.model_efforts[reviewProvider]?.[reviewModel]
    || runtimeConfig?.effort_options[reviewProvider]
    || (reviewEffort ? [reviewEffort] : []);
  const reviewServiceTiers = reviewProvider === 'codex'
    ? runtimeConfig?.codex_model_service_tiers[reviewModel]
      || runtimeConfig?.codex_service_tiers
      || ['default']
    : ['default'];
  const selectedTargetAvailable = capabilities
    ? Boolean(capabilities.targets[configuredTargetKind])
    : configuredTargetKind === 'fixed_url';
  const selectedTargetReason = capabilities?.target_reasons[configuredTargetKind]
    || (configuredTargetKind === 'current_workspace' ? capabilities?.preview.reason : null)
    || (configuredTargetKind === 'pull_request' || configuredTargetKind === 'git_ref'
      ? capabilities?.sandbox.reason
      : null)
    || null;
  const configuredTargetReady = configuredTargetKind === 'current_workspace'
    || (configuredTargetKind === 'fixed_url' && Boolean(configuredUrl.trim()))
    || (configuredTargetKind === 'pull_request'
      && /^\d+$/.test(configuredPrNumber.trim())
      && Number(configuredPrNumber.trim()) > 0)
    || (configuredTargetKind === 'git_ref' && Boolean(configuredGitRef.trim()));

  const openRuntimeSettings = () => {
    setSettingsOpen(true);
    if (!runtimeConfig && !runtimeConfigLoading) void loadRuntimeConfig();
    if (!capabilities && !capabilitiesLoading) void loadCapabilities();
  };

  const selectReviewProvider = (provider: 'claude' | 'codex') => {
    setReviewProvider(provider);
    const nextModel = runtimeConfig?.default_models[provider]
      || runtimeConfig?.models_by_provider[provider]?.[0]
      || '';
    const nextEfforts = runtimeConfig?.model_efforts[provider]?.[nextModel]
      || runtimeConfig?.effort_options[provider]
      || [];
    setReviewModel(nextModel);
    setReviewEffort(nextEfforts.includes(runtimeConfig?.default_effort || '')
      ? runtimeConfig!.default_effort
      : nextEfforts[0] || 'medium');
    setReviewServiceTier('default');
  };

  const selectReviewModel = (model: string) => {
    setReviewModel(model);
    const nextEfforts = runtimeConfig?.model_efforts[reviewProvider]?.[model]
      || runtimeConfig?.effort_options[reviewProvider]
      || [];
    if (!nextEfforts.includes(reviewEffort)) {
      setReviewEffort(nextEfforts.includes(runtimeConfig?.default_effort || '')
        ? runtimeConfig!.default_effort
        : nextEfforts[0] || 'medium');
    }
    const nextTiers = reviewProvider === 'codex'
      ? runtimeConfig?.codex_model_service_tiers[model] || ['default']
      : ['default'];
    if (!nextTiers.includes(reviewServiceTier)) setReviewServiceTier('default');
  };

  const persistRuntimeConfig = async () => {
    setRuntimeConfigSaving(true);
    try {
      const saved = await api.updateTestHarnessRuntimeConfig(taskId, inheritTaskRuntime
        ? { inherit_task: true }
        : {
            inherit_task: false,
            provider: reviewProvider,
            model: reviewModel,
            reasoning_effort: reviewEffort,
            codex_service_tier: reviewProvider === 'codex' ? reviewServiceTier : 'default',
          });
      applyRuntimeConfig(saved);
      return saved;
    } finally {
      setRuntimeConfigSaving(false);
    }
  };

  const saveRuntimeConfig = async () => {
    setError(null);
    try {
      await persistRuntimeConfig();
    } catch (nextError) {
      setError(`保存 Browser Agent 配置失败：${errorText(nextError)}`);
    }
  };

  const download = async (name: string) => {
    if (!harnessRun) return;
    try {
      const blob = await api.getTestRunEvidence(taskId, harnessRun.id, name);
      const objectUrl = URL.createObjectURL(blob);
      const anchor = document.createElement('a');
      anchor.href = objectUrl;
      anchor.download = name;
      anchor.click();
      URL.revokeObjectURL(objectUrl);
    } catch (nextError) {
      setError(errorText(nextError));
    }
  };
  const stopReview = async () => {
    if (!harnessRun || TERMINAL.has(harnessRun.status) || cancellingJobId === harnessRun.id) return;
    setCancellingJobId(harnessRun.id);
    setError(null);
    try {
      const cancelled = await api.cancelTestRun(taskId, harnessRun.id);
      setRuns((current) => current.map((item) => (
        item.id === cancelled.id ? cancelled : item
      )));
    } catch (nextError) {
      setError(`停止审查失败，Task 可能仍在运行：${errorText(nextError)}`);
    } finally {
      setCancellingJobId(null);
    }
  };
  const repeatReview = async () => {
    if (
      !harnessRun
      || !TERMINAL.has(harnessRun.status)
      || taskActive
      || repeatingRunId === harnessRun.id
    ) return;
    setRepeatingRunId(harnessRun.id);
    setError(null);
    try {
      const repeated = await api.repeatTestRun(taskId, harnessRun.id);
      startedWorkspaceRunRef.current = repeated;
      latestReviewIdRef.current = repeated.id;
      setRuns((current) => [
        repeated,
        ...current.filter((item) => item.id !== repeated.id),
      ]);
      setSelectedId(repeated.id);
      setMinimized(false);
      onAvailableChange(true);
      onNewReview();
    } catch (nextError) {
      setError(`重新测试失败：${errorText(nextError)}`);
    } finally {
      setRepeatingRunId(null);
    }
  };
  const startConfiguredReview = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (startingConfiguredReview || taskActive || hasActiveReview || !canStartConfiguredReview) return;

    const goal = configuredGoal.trim();
    if (!goal || !configuredTargetReady) return;
    if (!selectedTargetAvailable) {
      setError(selectedTargetReason || `${TARGET_LABELS[configuredTargetKind]} 当前不可用`);
      return;
    }

    let target: Record<string, unknown> = {};
    if (configuredTargetKind === 'fixed_url') {
      const url = configuredUrl.trim();
      try {
        const parsed = new URL(url);
        if (!['http:', 'https:'].includes(parsed.protocol)) {
          throw new Error('只支持 http:// 或 https:// 地址');
        }
      } catch (nextError) {
        setError(nextError instanceof Error && nextError.message === '只支持 http:// 或 https:// 地址'
          ? nextError.message
          : '请输入完整的 http:// 或 https:// 网站地址');
        return;
      }
      target = { url };
    } else if (configuredTargetKind === 'pull_request') {
      target = { remote: 'origin', pr_number: Number(configuredPrNumber.trim()) };
    } else if (configuredTargetKind === 'git_ref') {
      target = { remote: 'origin', ref: configuredGitRef.trim(), fetch: false };
    }

    setStartingConfiguredReview(true);
    setError(null);
    const [viewportWidth, viewportHeight] = configuredViewport.split('x').map(Number);
    try {
      const savedRuntime = await persistRuntimeConfig();
      const started = await api.startTestRun(taskId, {
        target_kind: configuredTargetKind,
        target,
        goal,
        profile: configuredProfile,
        allow_actions: configuredAllowActions,
        browser_channel: configuredBrowserChannel,
        viewport_width: viewportWidth,
        viewport_height: viewportHeight,
        max_steps: 20,
        max_actions: 60,
        ...(savedRuntime.inherit_task ? {} : {
          provider: savedRuntime.provider,
          model: savedRuntime.model,
          reasoning_effort: savedRuntime.reasoning_effort,
          codex_service_tier: savedRuntime.codex_service_tier,
        }),
      });
      startedWorkspaceRunRef.current = started;
      latestReviewIdRef.current = started.id;
      setRuns((current) => [
        started,
        ...current.filter((item) => item.id !== started.id),
      ]);
      setSelectedId(started.id);
      setLoading(false);
      setMinimized(false);
      setSettingsOpen(false);
      onAvailableChange(true);
      onNewReview();
    } catch (nextError) {
      setError(`启动测试失败：${errorText(nextError)}`);
    } finally {
      setStartingConfiguredReview(false);
    }
  };
  const displayedGoalRound = goalProgress
    ? Math.min(
      Math.max(1, goalProgress.maxTurns),
      Math.max(1, goalProgress.turn + (goalProgress.active ? 1 : 0)),
    )
    : null;

  const startDragging = useCallback((event: ReactPointerEvent<HTMLDivElement>) => {
    if (displayMode !== 'floating' || event.button !== 0) return;
    if ((event.target as HTMLElement).closest('button, select, a')) return;
    const bounds = floatingPanelRef.current?.getBoundingClientRect();
    dragRef.current = {
      offsetX: event.clientX - (bounds?.left ?? floatingPosition.x),
      offsetY: event.clientY - (bounds?.top ?? floatingPosition.y),
    };
    document.body.style.userSelect = 'none';
    event.preventDefault();
  }, [displayMode, floatingPosition]);

  useEffect(() => {
    if (displayMode !== 'floating') return;

    const onPointerMove = (event: PointerEvent) => {
      if (!dragRef.current) return;
      setFloatingPosition(clampFloatingPosition({
        x: event.clientX - dragRef.current.offsetX,
        y: event.clientY - dragRef.current.offsetY,
      }));
    };
    const onPointerUp = () => {
      if (!dragRef.current) return;
      dragRef.current = null;
      document.body.style.userSelect = '';
      setFloatingPosition((current) => {
        try { localStorage.setItem(FLOATING_POSITION_KEY, JSON.stringify(current)); } catch { /* storage may be unavailable */ }
        return current;
      });
    };
    const onResize = () => setFloatingPosition((current) => clampFloatingPosition(current));

    window.addEventListener('pointermove', onPointerMove);
    window.addEventListener('pointerup', onPointerUp);
    window.addEventListener('resize', onResize);
    return () => {
      window.removeEventListener('pointermove', onPointerMove);
      window.removeEventListener('pointerup', onPointerUp);
      window.removeEventListener('resize', onResize);
      dragRef.current = null;
      document.body.style.userSelect = '';
    };
  }, [displayMode]);

  useEffect(() => {
    if (displayMode === 'docked') setMinimized(false);
  }, [displayMode]);

  if (!open) return null;

  const panel = (
    <aside
      ref={floatingPanelRef}
      aria-label="Frontend Review progress"
      data-display-mode={displayMode}
      className={displayMode === 'floating'
        ? `fixed z-[70] flex w-[min(430px,calc(100vw-24px))] flex-col overflow-hidden rounded-xl border border-gray-600/60 bg-gray-950/98 shadow-2xl shadow-black/60 backdrop-blur ${minimized ? '' : 'max-h-[min(720px,calc(100vh-24px))]'}`
        : 'flex max-h-[46vh] w-full shrink-0 flex-col border-t border-gray-600/60 bg-gray-950/95 lg:max-h-none lg:w-[430px] lg:border-l lg:border-t-0'}
      style={displayMode === 'floating' ? { left: floatingPosition.x, top: floatingPosition.y } : undefined}
    >
      <div
        data-floating-drag-handle={displayMode === 'floating' ? 'true' : undefined}
        onPointerDown={startDragging}
        className={`flex items-center gap-2 border-b border-gray-600/50 px-3 py-2.5 ${displayMode === 'floating' ? 'cursor-move touch-none' : ''}`}
      >
        {displayMode === 'floating' && <GripVertical size={14} className="shrink-0 text-gray-600" />}
        <Eye size={16} className="text-indigo-400" />
        <div className="min-w-0 flex-1">
          <div className="text-sm font-medium text-gray-100">前端运行审查</div>
          <div className="truncate text-[10px] text-gray-500">
            Task #{taskId}{displayedGoalRound ? ` · Goal Agent 第 ${displayedGoalRound} 轮` : ''} · {settingsOpen
              ? '测试配置'
              : waitingForGoalReview
              ? goalStart?.phase === 'starting_review'
                ? '正在创建新的浏览器复查'
                : '正在启动循环审查'
              : waitingForWorkspaceReview
              ? '等待 Agent 创建新的浏览器审查'
              : displayedRun
              ? STAGE_LABELS[displayedRun.stage] || displayedRun.stage
              : loading
                ? '加载测试记录'
                : '待机 · 尚未启动测试'}
          </div>
        </div>
        <button
          type="button"
          onClick={() => {
            if (settingsOpen) setSettingsOpen(false);
            else openRuntimeSettings();
          }}
          className={`rounded p-1.5 transition-colors ${settingsOpen
            ? 'bg-indigo-500/15 text-indigo-300'
            : 'text-gray-500 hover:bg-gray-800 hover:text-indigo-300'}`}
          title={settingsOpen ? '返回测试进度' : '配置并启动前端测试'}
          aria-label="Configure frontend test"
          aria-pressed={settingsOpen}
        >
          <Settings size={14} />
        </button>
        <button
          type="button"
          onClick={() => void refresh()}
          className="rounded p-1.5 text-gray-500 hover:bg-gray-800 hover:text-gray-300"
          title="刷新审查进度"
        >
          <RefreshCw size={14} />
        </button>
        {displayMode === 'docked' ? (
          <button
            type="button"
            onClick={() => onDisplayModeChange('floating')}
            className="rounded p-1.5 text-gray-500 hover:bg-gray-800 hover:text-indigo-300"
            title="切换为浮窗"
            aria-label="Open Frontend Review as floating window"
          >
            <Square size={13} />
          </button>
        ) : (
          <>
            <button
              type="button"
              onClick={() => onDisplayModeChange('docked')}
              className="rounded p-1.5 text-gray-500 hover:bg-gray-800 hover:text-indigo-300"
              title="停靠到右侧"
              aria-label="Dock Frontend Review panel"
            >
              <PanelLeftOpen size={14} />
            </button>
            <button
              type="button"
              onClick={() => setMinimized((value) => !value)}
              className="rounded p-1.5 text-gray-500 hover:bg-gray-800 hover:text-gray-300"
              title={minimized ? '展开浮窗' : '最小化浮窗'}
              aria-label={minimized ? 'Restore Frontend Review window' : 'Minimize Frontend Review window'}
            >
              {minimized ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
            </button>
          </>
        )}
        <button
          type="button"
          onClick={onClose}
          className="rounded p-1.5 text-gray-500 hover:bg-gray-800 hover:text-gray-300"
          aria-label="Close Frontend Review panel"
        >
          <X size={14} />
        </button>
      </div>

      {!minimized && !settingsOpen && !waitingForWorkspaceReview && !waitingForGoalReview && runs.length > 1 && (
        <div className="border-b border-gray-600/50 px-3 py-2">
          <div className="flex items-center gap-1.5">
            <select
              value={harnessRun?.id || ''}
              onChange={(event) => setSelectedId(event.target.value)}
              aria-label="Select test run"
              className="min-w-0 flex-1 rounded border border-gray-600/60 bg-gray-900 px-2 py-1.5 text-xs text-gray-200 outline-none focus:border-indigo-500"
            >
              {runs.map((item, index) => (
                <option key={item.id} value={item.id}>
                  #{runs.length - index} · {TARGET_LABELS[item.target_kind]} · {STAGE_LABELS[item.stage] || item.stage} · {String(item.test_plan.objective || '')}
                </option>
              ))}
            </select>
            <button
              type="button"
              onClick={() => selectAdjacentRun(1)}
              disabled={selectedRunIndex < 0 || selectedRunIndex >= runs.length - 1}
              className="rounded border border-gray-600/60 bg-gray-900 p-1.5 text-gray-400 hover:border-indigo-500/60 hover:text-indigo-300 disabled:cursor-not-allowed disabled:opacity-35"
              title="切换到更早的测试"
              aria-label="Select older test run"
            >
              <ChevronDown size={14} />
            </button>
            <button
              type="button"
              onClick={() => selectAdjacentRun(-1)}
              disabled={selectedRunIndex <= 0}
              className="rounded border border-gray-600/60 bg-gray-900 p-1.5 text-gray-400 hover:border-indigo-500/60 hover:text-indigo-300 disabled:cursor-not-allowed disabled:opacity-35"
              title="切换到更新的测试"
              aria-label="Select newer test run"
            >
              <ChevronUp size={14} />
            </button>
          </div>
        </div>
      )}

      {!minimized && settingsOpen && (
        <div className="min-h-0 flex-1 overflow-y-auto p-3">
          <form onSubmit={startConfiguredReview} className="space-y-4" data-testid="frontend-test-settings">
            <section className="rounded-lg border border-indigo-500/25 bg-indigo-500/8 p-3">
              <div className="flex items-center gap-2 text-sm font-medium text-indigo-300">
                <Settings size={15} />前端测试配置
              </div>
              <p className="mt-1 text-[11px] leading-relaxed text-gray-400">
                在当前 Task 中直接创建当前工作区、固定 URL、GitHub PR 或 Git ref 的独立 Harness Run。
              </p>
            </section>

            {error && (
              <div role="alert" className="flex items-start gap-2 rounded border border-red-500/30 bg-red-500/10 px-2.5 py-2 text-xs text-red-300">
                <AlertCircle size={14} className="mt-0.5 shrink-0" />
                <span className="break-words">{error}</span>
              </div>
            )}

            <section className="space-y-3 rounded-lg border border-gray-600/60 bg-gray-900/55 p-3">
              <div className="flex items-start gap-2">
                <Shield size={14} className="mt-0.5 shrink-0 text-emerald-400" />
                <div className="min-w-0 flex-1">
                  <div className="text-xs font-medium text-gray-200">Browser Agent 独立运行配置</div>
                  <div className="mt-1 text-[10px] leading-relaxed text-gray-500">
                    这组设置只控制浏览器审查 Agent，不会修改当前 Task 的模型或推理强度。普通对话、单次审查和 Goal 复查都会使用它。
                  </div>
                </div>
              </div>

              {runtimeConfigLoading ? (
                <div className="flex items-center gap-2 rounded border border-gray-600/45 bg-gray-950/40 px-2.5 py-2 text-[10px] text-gray-500">
                  <Loader2 size={12} className="animate-spin" />正在加载可用模型…
                </div>
              ) : (
                <>
                  <label className="flex cursor-pointer items-start gap-2 rounded border border-gray-600/45 bg-gray-950/40 px-2.5 py-2 text-xs text-gray-300">
                    <input
                      type="checkbox"
                      checked={inheritTaskRuntime}
                      onChange={(event) => {
                        const inherit = event.target.checked;
                        setInheritTaskRuntime(inherit);
                        if (inherit && runtimeConfig) {
                          setReviewProvider(runtimeConfig.task_runtime.provider);
                          setReviewModel(runtimeConfig.task_runtime.model);
                          setReviewEffort(runtimeConfig.task_runtime.reasoning_effort);
                          setReviewServiceTier(runtimeConfig.task_runtime.codex_service_tier);
                        }
                      }}
                      className="mt-0.5 rounded border-gray-600 bg-gray-800 text-indigo-500"
                    />
                    <span>
                      跟随当前 Task
                      <span className="mt-0.5 block text-[10px] text-gray-500">
                        {taskProvider === 'claude' ? 'Claude' : 'Codex'} · {taskModel || '默认模型'} · effort {taskEffort || '默认'}
                        {taskProvider === 'codex' ? ` · ${taskServiceTier === 'priority' ? 'Fast' : 'Standard'}` : ''}
                      </span>
                    </span>
                  </label>

                  {!inheritTaskRuntime && (
                    <div className="grid grid-cols-1 gap-3">
                      <div>
                        <label htmlFor={`frontend-test-provider-${taskId}`} className="mb-1 block text-[10px] font-medium text-gray-400">审查 Provider</label>
                        <select
                          id={`frontend-test-provider-${taskId}`}
                          value={reviewProvider}
                          onChange={(event) => selectReviewProvider(event.target.value as 'claude' | 'codex')}
                          className="w-full rounded border border-gray-600/60 bg-gray-950 px-2.5 py-2 text-xs text-gray-100 outline-none focus:border-indigo-500"
                        >
                          {(runtimeConfig?.providers || ['claude', 'codex']).map((provider) => (
                            <option key={provider} value={provider}>{provider === 'claude' ? 'Claude' : 'Codex'}</option>
                          ))}
                        </select>
                      </div>
                      <div>
                        <label htmlFor={`frontend-test-model-${taskId}`} className="mb-1 block text-[10px] font-medium text-gray-400">审查模型</label>
                        <select
                          id={`frontend-test-model-${taskId}`}
                          value={reviewModel}
                          onChange={(event) => selectReviewModel(event.target.value)}
                          className="w-full rounded border border-gray-600/60 bg-gray-950 px-2.5 py-2 text-xs text-gray-100 outline-none focus:border-indigo-500"
                        >
                          {reviewModels.map((model) => <option key={model} value={model}>{model}</option>)}
                        </select>
                      </div>
                      <div className={`grid gap-3 ${reviewProvider === 'codex' ? 'grid-cols-2' : 'grid-cols-1'}`}>
                        <div>
                          <label htmlFor={`frontend-test-effort-${taskId}`} className="mb-1 block text-[10px] font-medium text-gray-400">推理强度</label>
                          <select
                            id={`frontend-test-effort-${taskId}`}
                            value={reviewEffort}
                            onChange={(event) => setReviewEffort(event.target.value)}
                            className="w-full rounded border border-gray-600/60 bg-gray-950 px-2.5 py-2 text-xs text-gray-100 outline-none focus:border-indigo-500"
                          >
                            {reviewEfforts.map((effort) => <option key={effort} value={effort}>{effort}</option>)}
                          </select>
                        </div>
                        {reviewProvider === 'codex' && (
                          <div>
                            <label htmlFor={`frontend-test-tier-${taskId}`} className="mb-1 block text-[10px] font-medium text-gray-400">速度</label>
                            <select
                              id={`frontend-test-tier-${taskId}`}
                              value={reviewServiceTier}
                              onChange={(event) => setReviewServiceTier(event.target.value as CodexServiceTier)}
                              className="w-full rounded border border-gray-600/60 bg-gray-950 px-2.5 py-2 text-xs text-gray-100 outline-none focus:border-indigo-500"
                            >
                              {reviewServiceTiers.map((tier) => (
                                <option key={tier} value={tier}>{tier === 'priority' ? 'Fast' : 'Standard'}</option>
                              ))}
                            </select>
                          </div>
                        )}
                      </div>
                    </div>
                  )}

                  <button
                    type="button"
                    onClick={() => void saveRuntimeConfig()}
                    disabled={runtimeConfigSaving || runtimeConfigLoading || taskActive || (!inheritTaskRuntime && (!reviewModel || !reviewEffort))}
                    className="inline-flex w-full items-center justify-center gap-1.5 rounded border border-indigo-500/35 bg-indigo-500/10 px-3 py-2 text-xs font-medium text-indigo-300 hover:bg-indigo-500/15 disabled:cursor-not-allowed disabled:opacity-45"
                  >
                    {runtimeConfigSaving ? <Loader2 size={13} className="animate-spin" /> : <Settings size={13} />}
                    {runtimeConfigSaving ? '正在保存…' : '保存审查 Agent 配置'}
                  </button>
                  {taskActive && (
                    <div className="text-center text-[10px] leading-relaxed text-amber-300">
                      当前 Task 回合结束后可保存；已经启动的 Harness Run 始终使用冻结配置。
                    </div>
                  )}
                </>
              )}
            </section>

            <section className="space-y-3 rounded-lg border border-gray-600/60 bg-gray-900/55 p-3">
              <div>
                <label htmlFor={`frontend-test-target-${taskId}`} className="mb-1.5 block text-xs font-medium text-gray-300">测试目标类型</label>
                <select
                  id={`frontend-test-target-${taskId}`}
                  value={configuredTargetKind}
                  onChange={(event) => {
                    setConfiguredTargetKind(event.target.value as TestHarnessTargetKind);
                    setError(null);
                  }}
                  className="w-full rounded-lg border border-gray-600/60 bg-gray-950 px-3 py-2 text-sm text-gray-100 outline-none focus:border-indigo-500"
                >
                  <option value="current_workspace">当前工作区</option>
                  <option value="fixed_url">固定 URL</option>
                  <option value="pull_request">GitHub PR</option>
                  <option value="git_ref">Git ref / 分支</option>
                </select>
              </div>

              {configuredTargetKind === 'fixed_url' && (
                <div>
                  <label htmlFor={`frontend-test-url-${taskId}`} className="mb-1.5 block text-xs font-medium text-gray-300">待检测网站</label>
                  <input
                    id={`frontend-test-url-${taskId}`}
                    type="url"
                    required
                    value={configuredUrl}
                    onChange={(event) => setConfiguredUrl(event.target.value)}
                    placeholder="https://example.com"
                    className="w-full rounded-lg border border-gray-600/60 bg-gray-950 px-3 py-2.5 text-sm text-gray-100 outline-none placeholder:text-gray-500 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500"
                  />
                  <p className="mt-1.5 text-[10px] leading-relaxed text-gray-500">
                    仅允许公网 HTTP(S)，浏览器所有连接都经过受控网络出口。
                  </p>
                </div>
              )}

              {configuredTargetKind === 'pull_request' && (
                <div>
                  <label htmlFor={`frontend-test-pr-${taskId}`} className="mb-1.5 block text-xs font-medium text-gray-300">Pull Request 编号</label>
                  <input
                    id={`frontend-test-pr-${taskId}`}
                    type="number"
                    min={1}
                    step={1}
                    required
                    value={configuredPrNumber}
                    onChange={(event) => setConfiguredPrNumber(event.target.value)}
                    placeholder="99"
                    className="w-full rounded-lg border border-gray-600/60 bg-gray-950 px-3 py-2.5 text-sm text-gray-100 outline-none placeholder:text-gray-500 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500"
                  />
                </div>
              )}

              {configuredTargetKind === 'git_ref' && (
                <div>
                  <label htmlFor={`frontend-test-ref-${taskId}`} className="mb-1.5 block text-xs font-medium text-gray-300">Git ref 或分支名</label>
                  <input
                    id={`frontend-test-ref-${taskId}`}
                    type="text"
                    required
                    value={configuredGitRef}
                    onChange={(event) => setConfiguredGitRef(event.target.value)}
                    placeholder="feature/browser-review"
                    className="w-full rounded-lg border border-gray-600/60 bg-gray-950 px-3 py-2.5 text-sm text-gray-100 outline-none placeholder:text-gray-500 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500"
                  />
                </div>
              )}

              {configuredTargetKind === 'current_workspace' && (
                <p className="text-[10px] leading-relaxed text-gray-500">
                  使用 Task 绑定的可信本地 Git 工作区和管理员已确认的 Preview 配置；不会切换或覆盖当前分支。
                </p>
              )}

              {capabilitiesLoading && configuredTargetKind !== 'fixed_url' && (
                <div className="flex items-center gap-2 text-[10px] text-gray-500">
                  <Loader2 size={11} className="animate-spin" />正在检查目标与 Sandbox 能力…
                </div>
              )}
              {!capabilitiesLoading && !selectedTargetAvailable && selectedTargetReason && (
                <div role="alert" className="flex items-start gap-2 rounded border border-amber-500/30 bg-amber-500/10 px-2.5 py-2 text-[10px] leading-relaxed text-amber-300">
                  <AlertCircle size={12} className="mt-0.5 shrink-0" />
                  <span>{selectedTargetReason}</span>
                </div>
              )}
              {(configuredTargetKind === 'pull_request' || configuredTargetKind === 'git_ref') && capabilities?.sandbox.available && (
                <div className="flex items-start gap-2 rounded border border-emerald-500/25 bg-emerald-500/8 px-2.5 py-2 text-[10px] leading-relaxed text-emerald-300">
                  <Shield size={12} className="mt-0.5 shrink-0" />
                  <span>Sandbox 已就绪 · {capabilities.sandbox.backend || 'isolated runtime'} · exact SHA 获取、依赖与 Preview 均不在 Manager 宿主机执行。</span>
                </div>
              )}
            </section>

            <div>
              <label htmlFor={`frontend-test-goal-${taskId}`} className="mb-1.5 block text-xs font-medium text-gray-300">测试目标</label>
              <textarea
                id={`frontend-test-goal-${taskId}`}
                required
                rows={4}
                value={configuredGoal}
                onChange={(event) => setConfiguredGoal(event.target.value)}
                className="w-full resize-y rounded-lg border border-gray-600/60 bg-gray-900 px-3 py-2.5 text-sm leading-relaxed text-gray-100 outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500"
              />
            </div>

            <div className="grid grid-cols-1 gap-3 sm:grid-cols-3 lg:grid-cols-1">
              <div>
                <label htmlFor={`frontend-test-profile-${taskId}`} className="mb-1.5 block text-xs font-medium text-gray-300">测试深度</label>
                <select
                  id={`frontend-test-profile-${taskId}`}
                  value={configuredProfile}
                  onChange={(event) => setConfiguredProfile(event.target.value as 'quick' | 'standard' | 'exhaustive')}
                  className="w-full rounded-lg border border-gray-600/60 bg-gray-900 px-3 py-2 text-sm text-gray-100 outline-none focus:border-indigo-500"
                >
                  <option value="quick">快速</option>
                  <option value="standard">标准</option>
                  <option value="exhaustive">完整</option>
                </select>
              </div>
              <div>
                <label htmlFor={`frontend-test-viewport-${taskId}`} className="mb-1.5 block text-xs font-medium text-gray-300">视口</label>
                <select
                  id={`frontend-test-viewport-${taskId}`}
                  value={configuredViewport}
                  onChange={(event) => setConfiguredViewport(event.target.value)}
                  className="w-full rounded-lg border border-gray-600/60 bg-gray-900 px-3 py-2 text-sm text-gray-100 outline-none focus:border-indigo-500"
                >
                  <option value="1440x900">桌面 1440×900</option>
                  <option value="1280x720">桌面 1280×720</option>
                  <option value="768x1024">平板 768×1024</option>
                  <option value="390x844">手机 390×844</option>
                </select>
              </div>
              <div>
                <label htmlFor={`frontend-test-browser-${taskId}`} className="mb-1.5 block text-xs font-medium text-gray-300">浏览器</label>
                <select
                  id={`frontend-test-browser-${taskId}`}
                  value={configuredBrowserChannel}
                  onChange={(event) => setConfiguredBrowserChannel(event.target.value as 'chrome' | 'chromium')}
                  className="w-full rounded-lg border border-gray-600/60 bg-gray-900 px-3 py-2 text-sm text-gray-100 outline-none focus:border-indigo-500"
                >
                  <option value="chromium">Playwright Chromium</option>
                  <option value="chrome">系统 Chrome</option>
                </select>
              </div>
            </div>

            <label className="flex cursor-pointer items-start gap-2 rounded-lg border border-gray-600/45 bg-gray-900/55 px-3 py-2.5 text-xs text-gray-300">
              <input
                type="checkbox"
                checked={configuredAllowActions}
                onChange={(event) => setConfiguredAllowActions(event.target.checked)}
                className="mt-0.5 rounded border-gray-600 bg-gray-800 text-indigo-500"
              />
              <span>
                允许安全的点击和输入
                <span className="mt-0.5 block text-[10px] leading-relaxed text-gray-500">仍会阻止跨域顶层跳转、弹窗和下载；不要用于生产写操作或含敏感账号的页面。</span>
              </span>
            </label>

            <button
              type="submit"
              disabled={startingConfiguredReview || runtimeConfigSaving || runtimeConfigLoading || capabilitiesLoading || taskActive || hasActiveReview || !canStartConfiguredReview || !configuredTargetReady || !selectedTargetAvailable || !configuredGoal.trim() || (!inheritTaskRuntime && (!reviewModel || !reviewEffort))}
              className="inline-flex w-full items-center justify-center gap-2 rounded-lg bg-indigo-600 px-4 py-2.5 text-sm font-medium text-white shadow-md shadow-indigo-600/20 transition-colors hover:bg-indigo-500 disabled:cursor-not-allowed disabled:opacity-45"
            >
              {startingConfiguredReview ? <Loader2 size={16} className="animate-spin" /> : <Play size={16} />}
              {startingConfiguredReview
                ? '正在创建测试…'
                : configuredTargetKind === 'fixed_url'
                  ? '开始网站测试'
                  : `开始 ${TARGET_LABELS[configuredTargetKind]} 测试`}
            </button>
            {(taskActive || hasActiveReview || !canStartConfiguredReview) && (
              <p className="text-center text-[10px] leading-relaxed text-amber-300">
                {hasActiveReview
                  ? '当前已有测试正在执行，请等待结束或先停止。'
                  : configuredReviewUnavailableReason || '等待当前 Task 回合结束后再从界面启动测试。'}
              </p>
            )}
          </form>
        </div>
      )}

      {!minimized && !settingsOpen && <div className="min-h-0 flex-1 space-y-3 overflow-y-auto p-3">
        {waitingForGoalReview && goalStart && (
          <section data-testid="frontend-review-goal-starting" className="flex min-h-72 flex-col items-center justify-center rounded-lg border border-indigo-500/35 bg-indigo-500/8 px-5 py-8 text-center">
            <div className="flex h-11 w-11 items-center justify-center rounded-full border border-indigo-400/35 bg-indigo-400/10">
              <Loader2 size={19} className="animate-spin text-indigo-300" />
            </div>
            <div className="mt-3 text-sm font-medium text-indigo-300">
              {goalStart.phase === 'starting_review'
                ? '正在创建本轮浏览器复查'
                : 'Goal 循环审查正在启动'}
            </div>
            <div className="mt-1 max-w-80 text-[11px] leading-relaxed text-gray-400">
              {goalStart.phase === 'starting_review'
                ? 'Agent 已发起新的 Harness Run。本页已与上一轮证据分离，新 Run 建立后会自动切换到实时截图和操作轨迹。'
                : '已接收 Goal，正在当前 Task/session 中启动“浏览器审查 → 必要修改 → 测试 → 重新审查”的执行链。'}
            </div>
            <div className="mt-4 w-full max-w-80 rounded-lg border border-gray-600/45 bg-gray-900/75 px-3 py-2 text-left">
              <div className="text-[9px] font-medium uppercase tracking-wide text-gray-500">本次目标</div>
              <div className="mt-1 whitespace-pre-wrap break-words text-[11px] leading-relaxed text-gray-300">{goalStart.prompt}</div>
            </div>
            <div className="mt-3 grid w-full max-w-80 gap-1.5 text-left text-[10px] text-gray-400">
              <div className="rounded border border-gray-600/35 bg-gray-900/60 px-2.5 py-1.5">1 · 创建独立 Harness Run 与代码指纹</div>
              <div className="rounded border border-gray-600/35 bg-gray-900/60 px-2.5 py-1.5">2 · 启动隔离预览与黑盒 Browser Agent</div>
              <div className="rounded border border-gray-600/35 bg-gray-900/60 px-2.5 py-1.5">3 · 回传截图、操作轨迹和审查报告</div>
            </div>
            <div className="mt-3 text-[10px] text-gray-500">
              Goal Agent 最多 {goalStart.maxTurns} 轮；同一轮内可以执行多次浏览器审查与复查。
            </div>
          </section>
        )}
        {!waitingForGoalReview && waitingForWorkspaceReview && (
          <section data-testid="workspace-review-expected" className="flex min-h-64 flex-col items-center justify-center rounded-lg border border-cyan-500/25 bg-cyan-500/8 px-5 py-8 text-center">
            <div className="flex h-10 w-10 items-center justify-center rounded-full border border-cyan-400/25 bg-cyan-400/10">
              <Loader2 size={18} className="animate-spin text-cyan-300" />
            </div>
            <div className="mt-3 text-sm font-medium text-cyan-300">正在创建新的前端测试</div>
            <div className="mt-1 max-w-72 text-[11px] leading-relaxed text-gray-400">
              已收到本次测试请求，正在等待 Agent 创建独立的 Harness Run。新 Run 就绪后，右栏会自动切换到它的实时截图和操作轨迹。
            </div>
            <div className="mt-4 grid w-full max-w-72 gap-1.5 text-left text-[10px] text-gray-500">
              <div className="rounded border border-gray-600/35 bg-gray-900/60 px-2.5 py-1.5">1 · 识别本次测试目标</div>
              <div className="rounded border border-gray-600/35 bg-gray-900/60 px-2.5 py-1.5">2 · 创建独立 Harness Run</div>
              <div className="rounded border border-gray-600/35 bg-gray-900/60 px-2.5 py-1.5">3 · 绑定浏览器 Agent</div>
            </div>
            <div className="mt-3 text-[10px] text-gray-500">上一轮测试仍保留在历史记录中，本页不会继续展示其内容。</div>
          </section>
        )}
        {loading && !waitingForWorkspaceReview && !waitingForGoalReview && !displayedRun && (
          <div className="flex items-center justify-center gap-2 py-10 text-sm text-gray-500">
            <Loader2 size={15} className="animate-spin" />
            加载测试记录…
          </div>
        )}
        {!loading && !waitingForWorkspaceReview && !waitingForGoalReview && !displayedRun && (
          <section data-testid="frontend-test-idle" className="flex min-h-80 flex-col items-center justify-center rounded-xl border border-dashed border-gray-600/60 bg-gray-900/35 px-6 py-10 text-center">
            <div className="flex h-12 w-12 items-center justify-center rounded-2xl border border-indigo-500/25 bg-indigo-500/10 text-indigo-300">
              <Eye size={22} />
            </div>
            <div className="mt-4 text-sm font-medium text-gray-100">尚未启动前端测试</div>
            <p className="mt-1 max-w-72 text-[11px] leading-relaxed text-gray-400">
              这里会显示实时浏览器画面、操作轨迹、运行时错误和最终报告。
            </p>
            <button
              type="button"
              onClick={openRuntimeSettings}
              className="mt-5 inline-flex items-center gap-2 rounded-lg border border-indigo-500/35 bg-indigo-500/10 px-3.5 py-2 text-xs font-medium text-indigo-300 transition-colors hover:bg-indigo-500/15"
            >
              <Settings size={14} />配置并启动测试
            </button>
            <div className="mt-4 max-w-72 text-[10px] leading-relaxed text-gray-500">
              也可以使用输入框上方的“单次审查”或“循环审查”，测试当前分支的新功能。
            </div>
          </section>
        )}
        {error && (
          <div role="alert" className="flex items-start gap-2 rounded border border-red-500/30 bg-red-500/10 px-2.5 py-2 text-xs text-red-300">
            <AlertCircle size={14} className="mt-0.5 shrink-0" />
            <span className="break-words">{error}</span>
          </div>
        )}
        {displayedRun && (
          <section data-testid="test-harness-progress" className="rounded-lg border border-indigo-500/25 bg-indigo-500/8 p-3">
            <div className="flex items-start justify-between gap-2">
              <div className="min-w-0">
                <div className="text-xs font-medium text-indigo-300">Test Harness · {TARGET_LABELS[displayedRun.target_kind]}</div>
                <div className="mt-0.5 line-clamp-2 text-[10px] text-gray-500">
                  {displayedObjective}
                </div>
                {typeof displayedRun.runtime.provider === 'string' && (
                  <div className="mt-1 truncate text-[10px] text-indigo-300/80" title={`${String(displayedRun.runtime.provider)} · ${String(displayedRun.runtime.model || '')} · ${String(displayedRun.runtime.reasoning_effort || '')}`}>
                    Browser Agent · {displayedRun.runtime.provider === 'claude' ? 'Claude' : 'Codex'} · {String(displayedRun.runtime.model || '默认模型')} · effort {String(displayedRun.runtime.reasoning_effort || '默认')}
                    {displayedRun.runtime.provider === 'codex' ? ` · ${displayedRun.runtime.codex_service_tier === 'priority' ? 'Fast' : 'Standard'}` : ''}
                  </div>
                )}
              </div>
              <div className="flex shrink-0 items-center gap-1.5">
                {!TERMINAL.has(displayedRun.status) && (
                  <button
                    type="button"
                    onClick={() => void stopReview()}
                    disabled={cancellingJobId === displayedRun.id}
                    className="inline-flex items-center gap-1 rounded border border-red-500/30 px-1.5 py-0.5 text-[10px] text-red-300 hover:bg-red-500/10 disabled:cursor-wait disabled:opacity-60"
                    aria-label="Stop test run"
                  >
                    {cancellingJobId === displayedRun.id
                      ? <Loader2 size={10} className="animate-spin" />
                      : <Square size={9} />}
                    {cancellingJobId === displayedRun.id ? '停止中' : '停止'}
                  </button>
                )}
                {TERMINAL.has(displayedRun.status) && !taskActive && (
                  <button
                    type="button"
                    onClick={() => void repeatReview()}
                    disabled={repeatingRunId === displayedRun.id}
                    className="inline-flex items-center gap-1 rounded border border-indigo-500/30 px-1.5 py-0.5 text-[10px] text-indigo-300 hover:bg-indigo-500/10 disabled:cursor-wait disabled:opacity-60"
                    aria-label="Repeat test run"
                  >
                    <RefreshCw size={10} className={repeatingRunId === displayedRun.id ? 'animate-spin' : ''} />
                    {repeatingRunId === displayedRun.id ? '创建中' : '重新测试'}
                  </button>
                )}
                <span className={`rounded-full border px-2 py-0.5 text-[10px] ${statusClass(displayedRun.status)}`}>
                  {STAGE_LABELS[displayedRun.stage] || displayedRun.stage}
                </span>
              </div>
            </div>
            <div className="mt-2 grid grid-cols-2 gap-2 text-[10px] text-gray-500">
              <div className="truncate rounded bg-gray-950/60 px-2 py-1.5" title={displayedRun.source_git_head || ''}>
                {displayedRun.source_git_head ? `HEAD ${displayedRun.source_git_head.slice(0, 10)}` : `Run ${displayedRun.id.slice(0, 10)}`}
              </div>
              <div className={`rounded px-2 py-1.5 ${displayedRun.stale ? 'bg-amber-500/10 text-amber-300' : 'bg-gray-950/60'}`}>
                {displayedRun.stale ? '代码已变化 · 结果过期' : `结论 ${displayedRun.verdict || '待定'}`}
              </div>
            </div>
            {resolvedTarget && (
              <div data-testid="git-target-resolution" className="mt-2 rounded border border-cyan-500/20 bg-cyan-500/5 p-2 text-[10px] text-gray-400">
                <div className="flex items-center justify-between gap-2">
                  <span className="truncate font-medium text-cyan-300" title={resolvedRepository || ''}>
                    {resolvedRepository || 'GitHub target'}
                  </span>
                  {typeof resolvedTarget.pr_number === 'number' && (
                    <span className="shrink-0 rounded border border-cyan-500/25 px-1.5 py-0.5 text-cyan-300">PR #{resolvedTarget.pr_number}</span>
                  )}
                </div>
                {resolvedHead && (
                  <div className="mt-1 font-mono text-[9px] text-gray-500" title={resolvedHead}>
                    {resolvedBase ? `${resolvedBase.slice(0, 10)} → ` : ''}{resolvedHead.slice(0, 12)}
                  </div>
                )}
                {resolvedChangedFiles.length > 0 && (
                  <div className="mt-2">
                    <div className="mb-1 text-[9px] uppercase tracking-wide text-gray-500">
                      变更文件 {resolvedChangedFiles.length}
                    </div>
                    <div className="space-y-1">
                      {resolvedChangedFiles.slice(0, 6).map((item, index) => (
                        <div key={`${String(item.path || '')}-${index}`} className="flex min-w-0 items-center gap-1.5">
                          <span className="w-10 shrink-0 uppercase text-cyan-300/75">{String(item.status || 'changed')}</span>
                          <span className="truncate" title={String(item.path || '')}>{String(item.path || '')}</span>
                        </div>
                      ))}
                      {resolvedChangedFiles.length > 6 && (
                        <div className="text-gray-500">另有 {resolvedChangedFiles.length - 6} 个文件…</div>
                      )}
                    </div>
                  </div>
                )}
              </div>
            )}
            {displayedRun.cleanup_status !== 'pending' && (
              <div className={`mt-2 flex items-center gap-1.5 text-[10px] ${displayedRun.cleanup_status === 'completed' ? 'text-emerald-300' : 'text-red-300'}`}>
                <Shield size={11} />
                {displayedRun.cleanup_status === 'completed'
                  ? '隔离资源已完成身份校验与清理'
                  : `隔离资源清理：${displayedRun.cleanup_status}`}
              </div>
            )}
            {displayedRun.evidence_archive_state && (
              <div
                data-testid="evidence-archive-state"
                className={`mt-1.5 flex items-start gap-1.5 text-[10px] ${displayedRun.evidence_archive_state === 'complete'
                  ? 'text-emerald-600 dark:text-emerald-300'
                  : displayedRun.evidence_archive_state === 'staging' || displayedRun.evidence_archive_state === 'archiving'
                    ? 'text-sky-600 dark:text-sky-300'
                    : 'text-red-600 dark:text-red-300'}`}
              >
                <Shield size={11} className="mt-0.5 shrink-0" />
                <span className="min-w-0 break-words">
                  {displayedRun.evidence_archive_state === 'complete'
                    ? '截图与报告已完成哈希校验和持久化归档'
                    : displayedRun.evidence_archive_state === 'archiving'
                      ? '正在校验并归档截图与报告'
                      : displayedRun.evidence_archive_state === 'staging'
                        ? '正在收集截图与报告'
                        : `证据归档未完成：${displayedRun.evidence_archive_error || displayedRun.evidence_archive_state}`}
                </span>
              </div>
            )}
            {workspaceRun && (
              <div data-testid="workspace-review-progress" className="mt-2 border-t border-gray-600/40 pt-2 text-[10px] text-gray-500">
                <div className="truncate" title={workspaceRun.workspace_fingerprint}>
                  工作区指纹 {workspaceRun.workspace_fingerprint.slice(0, 10)}
                </div>
                {workspaceRun.preview_url && (
                  <div className="mt-1 truncate" title={workspaceRun.preview_url}>
                    隔离预览：{workspaceRun.preview_url}
                  </div>
                )}
                {(workspaceRun.error || workspaceRun.cleanup_error)
                  && (workspaceRun.error || workspaceRun.cleanup_error) !== (displayedRun.error || displayedRun.cleanup_error) && (
                  <div className="mt-1 whitespace-pre-wrap break-words text-red-300">
                    {workspaceRun.error || workspaceRun.cleanup_error}
                  </div>
                )}
              </div>
            )}
            {(displayedRun.error || displayedRun.cleanup_error) && (
              <div className="mt-2 whitespace-pre-wrap break-words text-[10px] text-red-300">
                {displayedRun.error || displayedRun.cleanup_error}
              </div>
            )}
          </section>
        )}
        {goalProgress && displayedRun && (
          <section data-testid="frontend-review-goal-progress" className="rounded-lg border border-indigo-500/30 bg-indigo-500/8 p-3">
            <div className="flex items-center justify-between gap-3">
              <div>
                <div className="text-xs font-medium text-indigo-300">Goal 循环审查 · 模型自动判断</div>
                <div className="mt-0.5 text-[10px] text-gray-500">
                  Agent 第 {displayedGoalRound ?? 1} 轮 · 安全上限 {goalProgress.maxTurns} 轮
                </div>
              </div>
              <span className={`h-2 w-2 shrink-0 rounded-full ${goalProgress.active ? 'animate-pulse bg-blue-400' : 'bg-emerald-400'}`} />
            </div>
            <div className="mt-2 h-1 overflow-hidden rounded-full bg-gray-600/35">
              <div
                className="h-full rounded-full bg-indigo-500 transition-all"
                style={{ width: `${Math.min(100, ((displayedGoalRound ?? 1) / Math.max(1, goalProgress.maxTurns)) * 100)}%` }}
              />
            </div>
            <div className="mt-2 text-[10px] leading-relaxed text-gray-500">
              本轮 Harness 报告只是 Goal 证据；Agent 仍会按需修改、测试并创建新的复查。
            </div>
            {goalProgress.lastReason && (
              <div className="mt-2 line-clamp-3 text-[10px] leading-relaxed text-gray-400">
                评估器：{goalProgress.lastReason}
              </div>
            )}
          </section>
        )}
        {job && (
          <section className="overflow-hidden rounded-lg border border-gray-600/60 bg-black">
            <div className="flex items-center gap-1.5 border-b border-gray-600/50 bg-gray-900 px-2.5 py-2 text-[11px] text-gray-400">
              <Image size={13} />
              最新浏览器画面
            </div>
            {screenshotUrl ? (
              <img src={screenshotUrl} alt="Latest frontend review screenshot" className="block h-auto w-full" />
            ) : (
              <div className="flex aspect-video items-center justify-center text-xs text-gray-500">
                {TERMINAL.has(job.status) ? '没有可用截图' : '等待浏览器截图…'}
              </div>
            )}
          </section>
        )}
        {displayedRun && (
          <section className="rounded-lg border border-gray-600/60 bg-gray-900/55">
            <div className="flex items-center gap-1.5 border-b border-gray-600/50 px-3 py-2 text-xs font-medium text-gray-200">
              <Activity size={13} className="text-indigo-400" />
              模型观察与操作轨迹
            </div>
            <div className="max-h-72 space-y-0 overflow-y-auto px-3 py-1">
              {displayedRun.events.length === 0 && (
                <div className="py-5 text-center text-[11px] text-gray-500">等待测试 Harness 开始…</div>
              )}
              {displayedRun.events.map((event, index) => (
                <div key={event.id} className="relative border-l border-gray-600/60 py-2 pl-4">
                  <span className={`absolute -left-1 top-3 h-2 w-2 rounded-full ${event.event_type === 'decision' ? 'bg-indigo-400' : 'bg-cyan-400'}`} />
                  <div className="flex items-center justify-between gap-2">
                    <span className="text-[11px] font-medium text-gray-300">{event.title}</span>
                    <span className="text-[9px] text-gray-500">{index + 1}</span>
                  </div>
                  {event.detail && <div className="mt-0.5 whitespace-pre-wrap break-words text-[10px] leading-relaxed text-gray-500">{event.detail}</div>}
                </div>
              ))}
            </div>
          </section>
        )}
        {job && (
          <>
            <section className="rounded-lg border border-gray-600/60 bg-gray-900/55 p-3">
              <div className="flex items-start justify-between gap-2">
                <div className="min-w-0">
                  <div className="truncate text-xs font-medium text-gray-100" title={job.url}>{job.url}</div>
                  {showBrowserGoal && (
                    <div className="mt-1 line-clamp-2 text-[11px] text-gray-500">{job.goal}</div>
                  )}
                </div>
                <div className="flex shrink-0 items-center gap-1.5">
                  <span className={`rounded-full border px-2 py-0.5 text-[10px] ${statusClass(job.status)}`}>
                    {STAGE_LABELS[job.stage] || job.stage}
                  </span>
                </div>
              </div>
              <div className="mt-2 grid grid-cols-3 gap-2 text-center text-[10px]">
                <div className="rounded bg-gray-950/70 px-2 py-1.5 text-gray-400">
                  <Activity size={12} className="mx-auto mb-0.5 text-blue-400" />
                  {job.steps}/{job.max_steps} 步
                </div>
                <div className="rounded bg-gray-950/70 px-2 py-1.5 text-gray-400">
                  <Clock size={12} className="mx-auto mb-0.5 text-amber-400" />
                  {TERMINAL.has(job.status) ? '已结束' : '运行中'}
                </div>
                <div className="rounded bg-gray-950/70 px-2 py-1.5 text-gray-400">
                  <CheckCircle2 size={12} className="mx-auto mb-0.5 text-emerald-400" />
                  {job.actions} 动作
                </div>
              </div>
            </section>

            {telemetry.length > 0 && (
              <section className="rounded-lg border border-amber-500/20 bg-amber-500/5 p-3">
                <div className="mb-2 flex items-center gap-1.5 text-xs font-medium text-amber-300">
                  <AlertCircle size={13} />运行时信号
                </div>
                <div className="flex flex-wrap gap-1.5">
                  {telemetry.map(([name, entries]) => (
                    <span key={name} className="rounded border border-amber-500/20 bg-gray-950/60 px-2 py-1 text-[10px] text-gray-400">
                      {name.replaceAll('_', ' ')}: {entries.length}
                    </span>
                  ))}
                </div>
              </section>
            )}

            {(displayedRun?.report || job.report) && (
              <section className="rounded-lg border border-emerald-500/20 bg-emerald-500/5">
                <div className="flex items-center gap-1.5 border-b border-emerald-500/15 px-3 py-2 text-xs font-medium text-emerald-300">
                  <FileText size={13} />审查报告
                </div>
                <div className="prose prose-invert prose-sm max-w-none px-3 py-2 text-xs text-gray-300 prose-headings:text-gray-100 prose-p:text-gray-300 prose-li:text-gray-300 prose-strong:text-gray-200 prose-a:text-indigo-300 prose-code:text-gray-200">
                  <ReactMarkdown remarkPlugins={[remarkGfm]}>{displayedRun?.report || job.report}</ReactMarkdown>
                </div>
              </section>
            )}

            {(displayedRun?.findings.length ?? 0) > 0 && (
              <section className="rounded-lg border border-amber-500/20 bg-amber-500/5 p-3">
                <div className="mb-2 flex items-center gap-1.5 text-xs font-medium text-amber-300">
                  <AlertCircle size={13} />结构化发现
                </div>
                <div className="space-y-2">
                  {displayedRun!.findings.map((finding) => (
                    <div key={finding.fingerprint || `${finding.scenario_id}-${finding.title}`} className="rounded border border-gray-600/55 bg-gray-950/55 p-2">
                      <div className="flex items-center gap-2">
                        <span className="rounded border border-gray-600/50 bg-gray-900 px-1.5 py-0.5 text-[9px] uppercase text-gray-300">{finding.severity}</span>
                        <span className="text-[11px] font-medium text-gray-200">{finding.title}</span>
                      </div>
                      {finding.actual && <div className="mt-1 text-[10px] leading-relaxed text-gray-500">{finding.actual}</div>}
                    </div>
                  ))}
                </div>
              </section>
            )}

            {(displayedRun?.evidence.length ?? 0) > 0 && (
              <section className="flex flex-wrap gap-1.5">
                {displayedRun!.evidence.map((item) => (
                  <button
                    key={item.id}
                    type="button"
                    onClick={() => void download(item.name)}
                    className="inline-flex items-center gap-1 rounded border border-gray-600/60 bg-gray-900 px-2 py-1 text-[10px] text-gray-400 hover:border-indigo-500/60 hover:text-gray-200"
                  >
                    <Download size={10} />{item.name}
                  </button>
                ))}
              </section>
            )}
          </>
        )}
      </div>}
    </aside>
  );

  return displayMode === 'floating' ? createPortal(panel, document.body) : panel;
}
