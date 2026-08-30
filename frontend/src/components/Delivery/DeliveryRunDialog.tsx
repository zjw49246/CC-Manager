import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import {
  api,
  type DeliveryCycle,
  type DeliveryProgress,
  type DeliveryRunDetail,
  type DeliveryStageKey,
  type DeliveryStageProgress,
  type DeliveryTimelineEvent,
  type PlanInputRequest,
  type PlanResource,
  type PlanVersion,
  type PRMonitorRun,
  type Project,
  type Task,
  type TestHarnessRun,
} from '../../api/client';
import { useWebSocket } from '../../hooks/useWebSocket';
import { formatDateTime, parseBackendTimestamp } from '../../config/timezone';
import { MarkdownRenderer } from '../Markdown/MarkdownRenderer';
import { PlanInputForm } from '../PlanReview/PlanInputForm';
import { PlanDetail } from '../PlanReview/PlanDetail';
import {
  Activity,
  Bot,
  CheckCircle2,
  ChevronLeft,
  ChevronRight,
  Circle,
  Download,
  Eye,
  GitPullRequest,
  Loader2,
  MessageCircle,
  RefreshCw,
  ShieldCheck,
  X,
  XCircle,
} from '../icons';
import { DeliveryRunPanel } from '../Tasks/DeliveryRunPanel';

const STAGE_META: Record<DeliveryStageKey, { label: string; description: string }> = {
  planning: { label: 'Plan', description: 'Planner, reviewer and your decisions' },
  coding: { label: 'Development', description: 'Developer turns and commits' },
  pre_review: { label: 'Code review', description: 'Exact commit-range review' },
  frontend_review: { label: 'Frontend review', description: 'Black-box Browser Agent evidence' },
  publishing: { label: 'Publish PR', description: 'Push and pull request publication' },
  monitoring: { label: 'CI & PR review', description: 'Exact-head checks and merge readiness' },
};

interface Props {
  runId: number;
  project?: Project;
  embedded?: boolean;
  onClose: () => void;
  onOpenTask: (taskId: number) => void;
  onOpenPlan: (planId: number) => void;
  onOpenPRMonitor: () => void;
}

function titleCase(value: string | null | undefined): string {
  if (!value) return 'Pending';
  return value.split('_').map((part) => part.charAt(0).toUpperCase() + part.slice(1)).join(' ');
}

function deliveryStatusLabel(run: DeliveryRunDetail): string {
  if (run.activity !== 'terminal') return `${titleCase(run.phase)} · ${titleCase(run.activity)}`;
  if (run.outcome === 'success') {
    if (isReportOnlySuccess(run)) return 'Report completed';
    return run.terminal === 'merged' ? 'Merged' : 'Ready to merge';
  }
  return titleCase(run.outcome || 'completed');
}

function isReportOnlySuccess(run: DeliveryRunDetail): boolean {
  return Boolean(
    run.activity === 'terminal'
    && run.outcome === 'success'
    && !run.pr_url
    && run.base_sha
    && run.head_sha === run.base_sha
  );
}

function isPlanInputRequester(value: string): value is PlanInputRequest['requested_by'] {
  return value === 'planner' || value === 'reviewer';
}

const HARNESS_TERMINAL = new Set(['completed', 'failed', 'cancelled', 'stale']);
const HARNESS_STAGE_LABELS: Record<string, string> = {
  queued: '等待测试资源',
  fingerprinted: '已锁定代码版本',
  starting_preview: '正在启动隔离预览',
  preview_ready: '隔离预览已就绪',
  waiting_for_browser: '正在准备浏览器',
  agent_starting: 'Browser Agent 正在启动',
  browser_ready: '页面已打开',
  executing_actions: 'Browser Agent 正在验证页面',
  agent_reported: 'Agent 已提交报告',
  collecting_evidence: '正在归档截图与报告',
  evaluating: '正在生成结构化结论',
  cleaning: '正在清理隔离资源',
  finalizing_agent_cleanup: '正在收口 Agent 运行资源',
  completed: '前端审查已完成',
  failed: '前端审查失败',
  cancelled: '前端审查已停止',
};

function harnessStageLabel(stage: string | null | undefined): string {
  if (!stage) return '等待 Browser Agent';
  return HARNESS_STAGE_LABELS[stage] || titleCase(stage);
}

function formatTime(value: string | null | undefined): string {
  if (!value) return '—';
  const date = parseBackendTimestamp(value);
  return Number.isNaN(date.getTime()) ? value : formatDateTime(value);
}

function relativeTime(value: string | null | undefined): string {
  if (!value) return 'No public activity yet';
  const seconds = Math.max(0, Math.floor((Date.now() - parseBackendTimestamp(value).getTime()) / 1000));
  if (seconds < 10) return 'just now';
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  return `${Math.floor(seconds / 3600)}h ago`;
}

function deliveryDetailSummary(detail: string): string {
  const normalized = detail.trim();
  if (/permission denied \(publickey\)/i.test(normalized)) {
    return 'GitHub authentication failed while preparing the Delivery workspace.';
  }
  if (/repository contains unsafe git configuration/i.test(normalized)) {
    return 'The repository contains a Git setting that Delivery cannot safely use.';
  }
  if (/delivery workspace validation failed/i.test(normalized)) {
    return 'Delivery could not prepare a safe workspace for this run.';
  }
  if (normalized.length <= 280) return normalized;
  return `${normalized.slice(0, 277).trimEnd()}…`;
}

function DetailText({ detail }: { detail: string }) {
  const summary = deliveryDetailSummary(detail);
  const hasTechnicalDetail = summary !== detail.trim();
  return (
    <div className="mt-1 space-y-1.5">
      <p className="text-xs leading-5 text-gray-400">{summary}</p>
      {hasTechnicalDetail && (
        <details className="text-[11px] text-gray-500">
          <summary className="w-fit cursor-pointer select-none text-gray-500 hover:text-gray-300">Technical details</summary>
          <pre className="mt-2 max-h-48 overflow-auto whitespace-pre-wrap break-words rounded-lg border border-gray-800 bg-gray-950/70 p-3 font-mono text-[10px] leading-4 text-gray-500">{detail}</pre>
        </details>
      )}
    </div>
  );
}

function currentStage(progress: DeliveryProgress): DeliveryStageKey {
  if (progress.phase !== 'done') return progress.phase;
  const terminal = progress.stages.find((stage) => ['failed', 'cancelled'].includes(stage.state));
  if (terminal) return terminal.key;
  const lastCompleted = [...progress.stages].reverse().find((stage) => stage.state === 'completed');
  return lastCompleted?.key || 'monitoring';
}

interface CycleContext {
  label: string;
  detail: string;
  findingCount: number;
}

function asRecord(value: unknown): Record<string, unknown> | null {
  return value != null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function textValue(value: unknown): string | null {
  return typeof value === 'string' && value.trim() ? value.trim() : null;
}

function shorten(value: string, max = 220): string {
  return value.length <= max ? value : `${value.slice(0, max - 1).trimEnd()}…`;
}

function cycleContext(cycle: DeliveryCycle): CycleContext {
  const payload = asRecord(cycle.trigger_payload) || {};
  const evidence = asRecord(payload.evidence);
  const findingsValue = Array.isArray(payload.findings)
    ? payload.findings
    : Array.isArray(evidence?.findings)
      ? evidence.findings
      : [];
  const firstFinding = asRecord(findingsValue[0]);
  const firstTitle = textValue(firstFinding?.title);
  const summary = textValue(payload.summary);
  const findingDetail = findingsValue.length > 0
    ? `${findingsValue.length} blocking finding${findingsValue.length === 1 ? '' : 's'}${firstTitle ? ` · ${firstTitle}` : ''}`
    : null;

  switch (cycle.trigger_kind) {
    case 'initial_request':
      return {
        label: 'Initial implementation round',
        detail: 'Started from the original Delivery requirement.',
        findingCount: 0,
      };
    case 'operator_retry': {
      const reason = textValue(payload.reason);
      const previousError = textValue(payload.previous_error_message);
      const resumePhase = textValue(payload.resume_phase);
      const stageLabel = resumePhase === 'coding'
        ? 'Development'
        : resumePhase === 'pre_review'
          ? 'Code review'
          : resumePhase === 'frontend_review'
            ? 'Frontend review'
            : resumePhase === 'publishing'
              ? 'Publish PR'
              : resumePhase === 'monitoring'
                ? 'CI & PR review'
                : 'Plan';
      return {
        label: `Retried from ${stageLabel}`,
        detail: shorten(reason || previousError || `An operator restarted the failed Delivery from ${stageLabel}.`),
        findingCount: 0,
      };
    }
    case 'pre_review_changes_requested':
      return {
        label: 'Code review requested fixes',
        detail: shorten(summary || findingDetail || 'The exact commit-range review requested another implementation pass.'),
        findingCount: findingsValue.length,
      };
    case 'frontend_review_changes_requested':
      return {
        label: 'Frontend review requested fixes',
        detail: shorten(summary || findingDetail || 'Browser validation requested another implementation pass.'),
        findingCount: findingsValue.length,
      };
    case 'pr_monitor_blocked':
      return {
        label: 'PR review requested fixes',
        detail: shorten(findingDetail || 'PR Monitor blocked the current head and opened another repair round.'),
        findingCount: findingsValue.length,
      };
    case 'developer_no_progress':
      return {
        label: 'Developer made no progress',
        detail: 'The previous Developer turn produced no new commit, so the Loop returned to Plan.',
        findingCount: 0,
      };
    default:
      return {
        label: titleCase(cycle.trigger_kind || 'delivery_round'),
        detail: shorten(summary || 'The Delivery controller opened another round.'),
        findingCount: findingsValue.length,
      };
  }
}

function cycleEvents(
  cycles: DeliveryCycle[],
  cycle: DeliveryCycle,
  events: DeliveryTimelineEvent[],
): DeliveryTimelineEvent[] {
  const ordered = [...cycles].sort((left, right) => left.cycle_number - right.cycle_number);
  const index = ordered.findIndex((item) => item.id === cycle.id);
  const start = parseBackendTimestamp(cycle.created_at).getTime();
  const nextStart = index >= 0 && ordered[index + 1]
    ? parseBackendTimestamp(ordered[index + 1].created_at).getTime()
    : Number.POSITIVE_INFINITY;
  if (Number.isNaN(start)) {
    return index === ordered.length - 1 ? events : [];
  }
  return events.filter((event) => {
    const createdAt = parseBackendTimestamp(event.created_at).getTime();
    return !Number.isNaN(createdAt) && createdAt >= start && createdAt < nextStart;
  });
}

function lastEventStage(events: DeliveryTimelineEvent[]): DeliveryStageKey {
  const stage = [...events].reverse().find((event) => event.stage in STAGE_META)?.stage;
  return (stage as DeliveryStageKey | undefined) || 'planning';
}

const STAGE_COMPLETION_EVENTS: Record<DeliveryStageKey, string[]> = {
  planning: ['plan_ready'],
  coding: ['code_completed', 'developer_no_progress'],
  pre_review: ['review_approved', 'review_changes_requested'],
  frontend_review: ['frontend_review_profile_passed', 'frontend_review_passed', 'frontend_review_skipped', 'frontend_review_changes_requested'],
  publishing: ['pr_bound'],
  monitoring: ['monitor_blocked', 'monitor_ready'],
};

function historicalStageState(
  stage: DeliveryStageKey,
  cycle: DeliveryCycle,
  events: DeliveryTimelineEvent[],
): DeliveryStageProgress['state'] {
  const stageEvents = events.filter((event) => event.stage === stage);
  if (stageEvents.some((event) => event.kind === 'frontend_review_skipped')) return 'skipped';
  if (stageEvents.some((event) => STAGE_COMPLETION_EVENTS[stage].includes(event.kind))) return 'completed';
  if (cycle.status === 'failed' && lastEventStage(events) === stage) return 'failed';
  if (stageEvents.length > 0) return cycle.completed_at ? 'completed' : 'waiting';
  return 'pending';
}

export function DeliveryRunDialog({
  runId,
  project,
  embedded = false,
  onClose,
  onOpenTask,
  onOpenPRMonitor,
}: Props) {
  const [run, setRun] = useState<DeliveryRunDetail | null>(null);
  const [progress, setProgress] = useState<DeliveryProgress | null>(null);
  const [task, setTask] = useState<Task | null>(null);
  const [plans, setPlans] = useState<Record<number, PlanVersion>>({});
  const [openedPlan, setOpenedPlan] = useState<PlanResource | null>(null);
  const [openedPlanLoading, setOpenedPlanLoading] = useState(false);
  const [openedPlanError, setOpenedPlanError] = useState('');
  const [monitor, setMonitor] = useState<PRMonitorRun | null>(null);
  const [harness, setHarness] = useState<TestHarnessRun | null>(null);
  const [harnessScreenshotUrl, setHarnessScreenshotUrl] = useState<string | null>(null);
  const harnessScreenshotObjectUrl = useRef<string | null>(null);
  const [activeStage, setActiveStage] = useState<DeliveryStageKey>('planning');
  const [selectedCycleId, setSelectedCycleId] = useState<number | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState('');
  const requestSequence = useRef(0);
  const lastInputId = useRef<number | null>(null);
  const lastPhase = useRef<string | null>(null);
  const lastCurrentCycleId = useRef<number | null>(null);

  const load = useCallback(async (initial = false) => {
    const sequence = ++requestSequence.current;
    if (initial) setLoading(true); else setRefreshing(true);
    try {
      const [detail, projected] = await Promise.all([
        api.getDeliveryRun(runId),
        api.getDeliveryProgress(runId),
      ]);
      if (sequence !== requestSequence.current) return;
      const versionIds = Array.from(new Set(
        detail.cycles
          .map((cycle) => cycle.plan_version_id)
          .filter((id): id is number => id != null),
      ));
      const frontendRunId = projected.frontend_review.run_id;
      const [developer, versions, prRun, frontendRun] = await Promise.all([
        detail.developer_task_id != null ? api.getTask(detail.developer_task_id) : Promise.resolve(null),
        Promise.all(versionIds.map((id) => api.getPlanVersion(id))),
        detail.pr_monitor_run_id != null ? api.getPRMonitorRun(detail.pr_monitor_run_id) : Promise.resolve(null),
        frontendRunId && detail.developer_task_id != null
          ? api.getTestRun(detail.developer_task_id, frontendRunId).catch(() => null)
          : Promise.resolve(null),
      ]);
      if (sequence !== requestSequence.current) return;
      setRun(detail);
      setProgress(projected);
      setTask(developer);
      setPlans(Object.fromEntries(versions.map((version) => [version.id, version])));
      setMonitor(prRun);
      setHarness(frontendRun);
      setError('');
    } catch (reason) {
      if (sequence === requestSequence.current) {
        setError(reason instanceof Error ? reason.message : String(reason));
      }
    } finally {
      if (sequence === requestSequence.current) {
        setLoading(false);
        setRefreshing(false);
      }
    }
  }, [runId]);

  const openPlan = useCallback(async (planId: number) => {
    setOpenedPlanLoading(true);
    setOpenedPlanError('');
    try {
      setOpenedPlan(await api.getPlan(planId));
    } catch (reason) {
      setOpenedPlanError(reason instanceof Error ? reason.message : String(reason));
    } finally {
      setOpenedPlanLoading(false);
    }
  }, []);

  const refreshOpenedPlan = useCallback(async () => {
    if (!openedPlan) return;
    setOpenedPlan(await api.getPlan(openedPlan.id));
  }, [openedPlan]);

  useEffect(() => { void load(true); }, [load]);
  useEffect(() => {
    const frontendAgentActive = progress?.phase === 'frontend_review'
      && harness != null
      && !HARNESS_TERMINAL.has(harness.status);
    const timer = window.setInterval(() => void load(), frontendAgentActive ? 1500 : 10000);
    return () => window.clearInterval(timer);
  }, [harness?.status, load, progress?.phase]);
  useWebSocket(
    [`delivery:${runId}`],
    () => { void load(); },
    () => { void load(); },
    () => { void load(); },
  );

  useEffect(() => {
    if (!progress || !run) return;
    const inputId = progress.plan_input?.request.id || null;
    if (inputId !== null && inputId !== lastInputId.current) {
      setSelectedCycleId(run.current_cycle_id);
      setActiveStage('planning');
    } else if (
      lastPhase.current !== progress.phase
      && inputId === null
      && (selectedCycleId == null || selectedCycleId === run.current_cycle_id)
    ) {
      setActiveStage(currentStage(progress));
    }
    lastInputId.current = inputId;
    lastPhase.current = progress.phase;
  }, [progress, run, selectedCycleId]);

  useEffect(() => {
    const currentCycleId = run?.current_cycle_id ?? null;
    if (currentCycleId == null || currentCycleId === lastCurrentCycleId.current) return;
    lastCurrentCycleId.current = currentCycleId;
    setSelectedCycleId(currentCycleId);
    if (progress) setActiveStage(currentStage(progress));
  }, [run?.current_cycle_id, progress]);

  const selectedCycle = useMemo(() => {
    if (!run || run.cycles.length === 0) return null;
    return run.cycles.find((cycle) => cycle.id === selectedCycleId)
      || run.cycles.find((cycle) => cycle.id === run.current_cycle_id)
      || run.cycles[run.cycles.length - 1];
  }, [run, selectedCycleId]);
  const selectedIsCurrent = Boolean(
    run
    && selectedCycle
    && (
      selectedCycle.id === run.current_cycle_id
      || (run.current_cycle_id == null && selectedCycle === run.cycles[run.cycles.length - 1])
    ),
  );
  const selectedEvents = useMemo(
    () => run && progress && selectedCycle
      ? cycleEvents(run.cycles, selectedCycle, progress.events)
      : [],
    [run, progress, selectedCycle],
  );
  const selectedTurns = useMemo(
    () => run && selectedCycle
      ? run.turns.filter((turn) => turn.cycle_id === selectedCycle.id)
      : [],
    [run, selectedCycle],
  );
  const selectedContext = selectedCycle ? cycleContext(selectedCycle) : null;
  const displayedStages = useMemo(
    () => progress?.stages.map((stage) => (
      selectedIsCurrent || !selectedCycle
        ? stage
        : {
          ...stage,
          state: historicalStageState(stage.key, selectedCycle, selectedEvents),
          summary: `Round ${selectedCycle.cycle_number} history`,
        }
    )) || [],
    [progress, selectedCycle, selectedEvents, selectedIsCurrent],
  );
  const selectCycle = (cycle: DeliveryCycle) => {
    setSelectedCycleId(cycle.id);
    if (cycle.id === run?.current_cycle_id && progress) {
      setActiveStage(currentStage(progress));
      return;
    }
    setActiveStage(lastEventStage(run && progress ? cycleEvents(run.cycles, cycle, progress.events) : []));
  };

  const downloadEvidence = async (name: string) => {
    if (!task || !harness) return;
    try {
      const blob = await api.getTestRunEvidence(task.id, harness.id, name);
      const objectUrl = URL.createObjectURL(blob);
      const anchor = document.createElement('a');
      anchor.href = objectUrl;
      anchor.download = name;
      anchor.click();
      window.setTimeout(() => URL.revokeObjectURL(objectUrl), 0);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : String(reason));
    }
  };

  const latestHarnessScreenshot = harness?.browser_review?.latest_screenshot || null;
  const frontendHarnessActive = Boolean(
    harness
    && !HARNESS_TERMINAL.has(harness.status),
  );
  const frontendHarnessEvents = harness && Array.isArray(harness.events) ? harness.events : [];
  const frontendHarnessEvidence = harness && Array.isArray(harness.evidence) ? harness.evidence : [];
  const frontendHarnessLatestEvent = frontendHarnessEvents.at(-1) || null;
  const runIsTerminal = Boolean(run?.activity === 'terminal' || progress?.phase === 'done');
  const runSucceeded = Boolean(runIsTerminal && run?.outcome === 'success');
  const reportOnlySuccess = Boolean(run && isReportOnlySuccess(run));
  useEffect(() => {
    if (!task || !harness || !latestHarnessScreenshot) {
      if (harnessScreenshotObjectUrl.current) URL.revokeObjectURL(harnessScreenshotObjectUrl.current);
      harnessScreenshotObjectUrl.current = null;
      setHarnessScreenshotUrl(null);
      return;
    }
    let active = true;
    api.getTestRunEvidence(task.id, harness.id, latestHarnessScreenshot)
      .then((blob) => {
        const objectUrl = URL.createObjectURL(blob);
        if (!active) {
          URL.revokeObjectURL(objectUrl);
          return;
        }
        const previous = harnessScreenshotObjectUrl.current;
        harnessScreenshotObjectUrl.current = objectUrl;
        setHarnessScreenshotUrl(objectUrl);
        if (previous) URL.revokeObjectURL(previous);
      })
      .catch(() => {
        // Evidence can briefly be between staging and archival. The next
        // Harness refresh retries without hiding the rest of the live trace.
      });
    return () => { active = false; };
  }, [harness?.id, latestHarnessScreenshot, task?.id]);

  useEffect(() => () => {
    if (harnessScreenshotObjectUrl.current) URL.revokeObjectURL(harnessScreenshotObjectUrl.current);
  }, []);

  const stageContent = (stage: DeliveryStageKey) => {
    if (!run || !progress || !selectedCycle) return null;
    if (stage === 'planning') {
      const plan = selectedCycle.plan_version_id ? plans[selectedCycle.plan_version_id] : null;
      const planInput = selectedIsCurrent ? progress.plan_input : null;
      const planInputRequest = planInput && isPlanInputRequester(planInput.request.requested_by)
        ? { ...planInput.request, requested_by: planInput.request.requested_by }
        : null;
      const currentPlanId = selectedIsCurrent ? progress.plan_id : null;
      return (
        <div className="space-y-4">
          {planInput && (
            <div className="rounded-xl border border-amber-500/30 bg-amber-500/5 p-4">
              <div className="mb-3 flex items-center justify-between gap-3">
                <div>
                  <p className="text-sm font-semibold text-amber-200">The Loop needs your choice</p>
                  <p className="mt-1 text-xs text-amber-100/60">Answer here; you do not need to switch to the Plans page.</p>
                </div>
                <button type="button" onClick={() => void openPlan(planInput.plan_id)} className="text-xs text-indigo-300 hover:underline">Open Plan conversation</button>
              </div>
              {planInputRequest ? (
                <PlanInputForm
                  compact
                  run={planInput.run}
                  request={planInputRequest}
                  onAnswered={() => load()}
                />
              ) : (
                <Notice
                  tone="red"
                  text="The server returned an invalid Plan input requester. Inline answering is disabled until the response is corrected."
                />
              )}
            </div>
          )}
          {plan ? (
            <article className="rounded-xl border border-gray-800 bg-gray-950/50 p-4">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <span className="text-xs font-medium text-gray-300">
                  Round {selectedCycle.cycle_number} · Plan #{plan.plan_id} v{plan.version_number}
                </span>
                <button type="button" onClick={() => void openPlan(plan.plan_id)} className="text-xs text-indigo-300 hover:underline">Open Plan conversation</button>
              </div>
              <div className="prose prose-invert mt-3 max-w-none text-xs text-gray-300"><MarkdownRenderer content={plan.content} /></div>
            </article>
          ) : currentPlanId && !planInput ? (
            <div className="flex flex-wrap items-center justify-between gap-3 border-l-2 border-indigo-400/60 bg-indigo-500/5 px-4 py-3">
              <div>
                <p className="text-sm font-medium text-gray-200">Plan #{currentPlanId} is being prepared</p>
                <p className="mt-1 text-xs text-gray-500">The Plan exists. Its first approved Version is not ready yet.</p>
              </div>
              <button type="button" onClick={() => void openPlan(currentPlanId)} className="text-xs font-medium text-indigo-300 hover:text-indigo-200">Open Plan #{currentPlanId}</button>
            </div>
          ) : selectedCycle.error_message ? (
            <Notice tone="red" text={selectedCycle.error_message} />
          ) : (
            <EmptyState text={selectedIsCurrent ? 'This round is still producing an approved Plan.' : 'No approved Plan was produced in this round.'} />
          )}
        </div>
      );
    }
    if (stage === 'coding') {
      return (
        <div className="space-y-4">
          <div className="grid gap-2 sm:grid-cols-3">
            <Metric label="Developer Task" value={task ? `#${task.id}` : 'Not created'} />
            <Metric label="Turns this round" value={String(selectedTurns.length)} />
            <Metric label="Round head" value={(selectedCycle.result_head_sha || (selectedIsCurrent ? run.head_sha : null))?.slice(0, 12) || 'Pending'} mono />
          </div>
          {task && (
            <button type="button" onClick={() => onOpenTask(task.id)} className="inline-flex items-center gap-1.5 rounded-lg bg-indigo-600/20 px-3 py-2 text-xs text-indigo-300 hover:bg-indigo-600/30">
              <MessageCircle size={14} /> Open real Task Chat
            </button>
          )}
          <div className="space-y-2">
            {selectedTurns.length === 0 && <EmptyState text="No Developer turn ran in this round." />}
            {selectedTurns.map((turn) => (
              <div key={turn.id} className="rounded-lg border border-gray-800 px-3 py-2 text-xs text-gray-400">
                <span className="font-medium text-gray-300">Turn {turn.generation}</span> · {titleCase(turn.status)} · attempt {turn.attempts}
                {turn.last_error && <div className="mt-1 text-red-300">{turn.last_error}</div>}
              </div>
            ))}
          </div>
        </div>
      );
    }
    if (stage === 'pre_review') {
      return (
        <article className="rounded-lg border border-gray-800 px-3 py-3 text-xs">
          <div className="flex justify-between gap-2 text-gray-300"><span>Round {selectedCycle.cycle_number}</span><span>{selectedCycle.review_verdict ? titleCase(selectedCycle.review_verdict) : 'Pending'}</span></div>
          {selectedCycle.review_summary && <p className="mt-2 whitespace-pre-wrap leading-5 text-gray-500">{selectedCycle.review_summary}</p>}
          {selectedCycle.error_message && <p className="mt-2 text-red-300">{selectedCycle.error_message}</p>}
          {!selectedCycle.review_summary && !selectedCycle.error_message && <p className="mt-2 text-gray-600">No code-review result has been recorded for this round.</p>}
        </article>
      );
    }
    if (stage === 'frontend_review') {
      const summary = progress.frontend_review;
      const historicalStatus = selectedCycle.frontend_review_verdict
        || (selectedCycle.frontend_review_skip_reason ? 'skipped' : 'pending');
      const selectedSummary = selectedIsCurrent ? summary : {
        ...summary,
        run_id: selectedCycle.frontend_review_run_id,
        status: historicalStatus,
        verdict: selectedCycle.frontend_review_verdict,
        report: selectedCycle.frontend_review_summary,
        skip_reason: selectedCycle.frontend_review_skip_reason,
        finding_count: 0,
        evidence_count: 0,
      };
      const selectedHarness = selectedIsCurrent ? harness : null;
      const harnessEvents = selectedHarness && Array.isArray(selectedHarness.events)
        ? selectedHarness.events
        : [];
      const latestHarnessEvent = harnessEvents.at(-1) || null;
      const recentHarnessEvents = harnessEvents.slice(-6);
      const browserJob = selectedHarness?.browser_review || null;
      const profileIds = selectedCycle.frontend_review_profile_ids || [];
      const profileResults = new Map(
        (selectedCycle.frontend_review_results || [])
          .filter((item): item is Record<string, unknown> => item != null && typeof item === 'object')
          .map((item) => [String(item.profile_id || ''), item]),
      );
      const selectedHarnessActive = Boolean(
        selectedHarness
        && selectedHarness.stage !== 'completed'
        && !HARNESS_TERMINAL.has(selectedHarness.status),
      );
      return (
        <div className="space-y-4">
          <div className="grid gap-2 sm:grid-cols-4">
            <Metric label="Policy" value={titleCase(selectedSummary.policy)} />
            <Metric label="Status" value={titleCase(selectedSummary.status || (selectedSummary.skip_reason ? 'skipped' : 'pending'))} />
            <Metric label="Findings" value={selectedIsCurrent ? String(selectedSummary.finding_count) : 'See summary'} />
            <Metric label="Evidence" value={selectedIsCurrent ? String(selectedSummary.evidence_count) : 'Archived'} />
          </div>
          {profileIds.length > 0 && (
            <div className="rounded-xl border border-gray-800 bg-gray-950/50 p-3">
              <div className="flex items-center justify-between gap-2">
                <span className="text-xs font-medium text-gray-300">Preview profiles</span>
                <span className="text-[10px] text-gray-500">
                  {Math.min(selectedCycle.frontend_review_profile_index + 1, profileIds.length)} / {profileIds.length}
                </span>
              </div>
              <div className="mt-2 space-y-1.5">
                {profileIds.map((profileId, index) => {
                  const result = profileResults.get(profileId);
                  const verdict = typeof result?.verdict === 'string' ? result.verdict : null;
                  const isCurrent = selectedIsCurrent
                    && index === selectedCycle.frontend_review_profile_index
                    && !verdict;
                  return (
                    <div key={profileId} className="flex items-center gap-2 rounded-lg border border-gray-800/80 bg-gray-900/60 px-2.5 py-2 text-xs">
                      {verdict === 'passed' ? (
                        <CheckCircle2 size={14} className="shrink-0 text-emerald-400" />
                      ) : verdict ? (
                        <XCircle size={14} className="shrink-0 text-red-400" />
                      ) : isCurrent ? (
                        <Loader2 size={14} className="shrink-0 animate-spin text-cyan-400" />
                      ) : (
                        <Circle size={14} className="shrink-0 text-gray-600" />
                      )}
                      <span className="font-mono text-gray-300">{profileId}</span>
                      <span className={`ml-auto text-[10px] ${verdict === 'passed' ? 'text-emerald-400' : verdict ? 'text-red-400' : isCurrent ? 'text-cyan-300' : 'text-gray-600'}`}>
                        {verdict ? titleCase(verdict) : isCurrent ? 'Running' : index < selectedCycle.frontend_review_profile_index ? 'Completed' : 'Pending'}
                      </span>
                    </div>
                  );
                })}
              </div>
            </div>
          )}
          {selectedSummary.skip_reason && <Notice tone="amber" text={selectedSummary.skip_reason} />}
          {selectedSummary.error && <Notice tone="red" text={selectedSummary.error} />}
          {!selectedIsCurrent && selectedSummary.report && <p className="whitespace-pre-wrap rounded-lg border border-gray-800 bg-gray-950/40 px-3 py-2 text-xs leading-5 text-gray-400">{selectedSummary.report}</p>}
          {selectedHarness && (
            <div className="space-y-3 rounded-xl border border-gray-800 bg-gray-950/50 p-4">
              <div className="flex flex-wrap items-center justify-between gap-2 text-xs">
                <span className="inline-flex items-center gap-1.5 font-medium text-gray-300"><Eye size={14} /> Harness {selectedHarness.id.slice(0, 8)}</span>
                <span className="text-gray-500">{harnessStageLabel(selectedHarness.stage)} · cleanup {titleCase(selectedHarness.cleanup_status)}</span>
              </div>
              {selectedHarnessActive && (
                <div
                  data-testid="delivery-frontend-agent-live"
                  aria-live="polite"
                  className="overflow-hidden rounded-xl border border-cyan-400/40 bg-gradient-to-br from-cyan-500/12 via-indigo-500/8 to-gray-950 shadow-lg shadow-cyan-950/20"
                >
                  <div className="flex items-start gap-3 p-3">
                    <div className="relative flex h-10 w-10 shrink-0 items-center justify-center rounded-full border border-cyan-400/40 bg-cyan-400/10 text-cyan-300">
                      <Bot size={18} />
                      <span className="absolute -right-0.5 -top-0.5 h-2.5 w-2.5 animate-pulse rounded-full border-2 border-gray-950 bg-emerald-400" />
                    </div>
                    <div className="min-w-0 flex-1">
                      <div className="flex items-center justify-between gap-2">
                        <div className="text-sm font-semibold text-cyan-100">前端 Browser Agent 正在执行</div>
                        <span className="animate-pulse rounded-full border border-cyan-400/30 bg-cyan-400/10 px-2 py-0.5 text-[9px] font-bold uppercase tracking-wide text-cyan-300">Live</span>
                      </div>
                      <div className="mt-0.5 text-xs text-gray-300">{harnessStageLabel(selectedHarness.stage)}</div>
                      {latestHarnessEvent && (
                        <div className="mt-2 rounded-lg border border-white/8 bg-black/25 px-2.5 py-2">
                          <div className="flex items-center justify-between gap-2">
                            <span className="font-medium text-gray-100">{latestHarnessEvent.title}</span>
                            <span className="shrink-0 text-[10px] text-gray-500">{formatTime(latestHarnessEvent.created_at)}</span>
                          </div>
                          {latestHarnessEvent.detail && <p className="mt-1 line-clamp-3 whitespace-pre-wrap text-[11px] leading-5 text-gray-400">{latestHarnessEvent.detail}</p>}
                        </div>
                      )}
                      <div className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-[10px] text-gray-400">
                        <span>事件 {harnessEvents.length}</span>
                        <span>步骤 {browserJob ? `${browserJob.steps}/${browserJob.max_steps}` : '等待绑定'}</span>
                        <span>动作 {browserJob?.actions ?? 0}</span>
                        <span>每 1.5 秒刷新</span>
                      </div>
                    </div>
                  </div>
                  {harnessScreenshotUrl && (
                    <div className="border-t border-cyan-400/15 bg-black">
                      <div className="flex items-center justify-between px-3 py-1.5 text-[10px] text-gray-500">
                        <span>最新浏览器画面</span>
                        <span>{latestHarnessScreenshot}</span>
                      </div>
                      <img src={harnessScreenshotUrl} alt="Live frontend Browser Agent screenshot" className="block h-auto w-full" />
                    </div>
                  )}
                </div>
              )}
              {recentHarnessEvents.length > 0 && (
                <div data-testid="delivery-frontend-agent-events" className="rounded-lg border border-gray-800 bg-gray-950/60">
                  <div className="flex items-center gap-1.5 border-b border-gray-800 px-3 py-2 text-xs font-medium text-gray-300">
                    <Activity size={13} className={selectedHarnessActive ? 'animate-pulse text-cyan-400' : 'text-indigo-400'} />
                    Agent 执行过程
                    <span className="ml-auto text-[10px] font-normal text-gray-600">最近 {recentHarnessEvents.length} / {harnessEvents.length}</span>
                  </div>
                  <div className="divide-y divide-gray-800/80 px-3">
                    {recentHarnessEvents.map((event, index) => {
                      const current = index === recentHarnessEvents.length - 1;
                      return (
                        <div key={event.id} className={`py-2 text-xs ${current && selectedHarnessActive ? 'bg-cyan-400/5' : ''}`}>
                          <div className="flex items-center justify-between gap-2">
                            <span className={`font-medium ${current && selectedHarnessActive ? 'text-cyan-200' : 'text-gray-300'}`}>{event.title}</span>
                            <span className="shrink-0 text-[10px] text-gray-600">{formatTime(event.created_at)}</span>
                          </div>
                          {event.detail && <p className="mt-0.5 line-clamp-2 whitespace-pre-wrap text-[11px] leading-5 text-gray-500">{event.detail}</p>}
                        </div>
                      );
                    })}
                  </div>
                </div>
              )}
              {task && (
                <button type="button" onClick={() => onOpenTask(task.id)} className="inline-flex items-center gap-1.5 rounded-lg bg-indigo-600/20 px-3 py-2 text-xs text-indigo-300 hover:bg-indigo-600/30">
                  <Eye size={13} /> 打开完整实时测试面板
                </button>
              )}
              {selectedHarness.findings.length > 0 && (
                <div className="space-y-2">
                  {selectedHarness.findings.map((finding) => (
                    <div key={finding.id} className="rounded-lg border border-red-500/20 bg-red-500/5 px-3 py-2 text-xs">
                      <div className="font-medium text-red-200">{finding.severity.toUpperCase()} · {finding.title}</div>
                      {finding.actual && <p className="mt-1 leading-5 text-gray-400">{finding.actual}</p>}
                    </div>
                  ))}
                </div>
              )}
              {selectedHarness.report && <div className="prose prose-invert max-w-none text-xs text-gray-300"><MarkdownRenderer content={selectedHarness.report} /></div>}
              {selectedHarness.evidence.length > 0 && (
                <div className="flex flex-wrap gap-1.5">
                  {selectedHarness.evidence.map((item) => (
                    <button
                      key={item.id}
                      type="button"
                      onClick={() => void downloadEvidence(item.name)}
                      className="inline-flex items-center gap-1 rounded border border-gray-700 bg-gray-900 px-2 py-1 text-[10px] text-gray-400 hover:border-indigo-500/60 hover:text-gray-200"
                    >
                      <Download size={10} /> {item.name}
                    </button>
                  ))}
                </div>
              )}
            </div>
          )}
          {!selectedHarness && !selectedSummary.skip_reason && !selectedSummary.report && <EmptyState text="The Browser Agent did not record evidence for this round." />}
        </div>
      );
    }
    if (stage === 'publishing') {
      return (
        <div className="grid gap-2 sm:grid-cols-2">
          <Metric label="Delivery branch" value={run.delivery_branch} mono />
          <Metric label="Pull request" value={run.pr_number ? `#${run.pr_number}` : 'Pending'} />
          <Metric label="Round head" value={(selectedCycle.result_head_sha || selectedCycle.start_head_sha)?.slice(0, 12) || 'Pending'} mono />
          {run.pr_url && <a href={run.pr_url} target="_blank" rel="noreferrer" className="inline-flex items-center gap-1 text-xs text-indigo-300 hover:underline"><GitPullRequest size={13} /> Open GitHub PR</a>}
        </div>
      );
    }
    return (
      <div className="space-y-3">
        <div className="grid gap-2 sm:grid-cols-3">
          <Metric label="Monitor Run" value={monitor ? `#${monitor.id}` : 'Pending'} />
          <Metric label="Status" value={monitor ? titleCase(monitor.status) : titleCase(run.wait_reason || 'pending')} />
          <Metric label="Repairs" value={monitor ? `${monitor.repair_attempts}/${monitor.max_repair_attempts}` : '—'} />
        </div>
        {monitor && <button type="button" onClick={onOpenPRMonitor} className="inline-flex items-center gap-1.5 rounded-lg bg-emerald-600/20 px-3 py-2 text-xs text-emerald-300 hover:bg-emerald-600/30"><GitPullRequest size={14} /> Open in PR Monitor</button>}
      </div>
    );
  };

  return (
    <div
      className={embedded ? 'min-h-0' : 'fixed inset-0 z-[85] flex items-end justify-center bg-black/70 sm:items-center sm:p-5'}
      onMouseDown={embedded ? undefined : (event) => event.target === event.currentTarget && onClose()}
    >
      <div
        role={embedded ? 'region' : 'dialog'}
        aria-modal={embedded ? undefined : true}
        aria-label={`Delivery #${runId}`}
        className={embedded
          ? 'relative flex min-h-[calc(100dvh-8rem)] w-full flex-col overflow-hidden rounded-xl border border-gray-800 bg-gray-900/70'
          : 'relative flex h-[94dvh] w-full max-w-6xl flex-col overflow-hidden border border-gray-700 bg-gray-900 shadow-2xl sm:h-[90vh] sm:rounded-2xl'}
      >
        <header className="flex shrink-0 items-start justify-between gap-3 border-b border-gray-800 px-4 py-3 sm:px-5">
          <div className="min-w-0">
            <div className="flex flex-wrap items-center gap-2">
              <h2 className="truncate text-lg font-semibold text-gray-100">Delivery #{runId}</h2>
              {run && <span className={`rounded-full px-2 py-0.5 text-[11px] font-medium ${runSucceeded ? 'bg-emerald-500/15 text-emerald-300' : runIsTerminal ? 'bg-red-500/15 text-red-300' : 'bg-indigo-500/15 text-indigo-300'}`}>{deliveryStatusLabel(run)}</span>}
            </div>
            <p className="mt-1 truncate text-xs text-gray-500">
              {run?.title || 'Loading…'}{project ? ` · ${project.name}` : ''}
              {run ? ` · Round ${run.cycle_count}/${run.max_cycles} · ${run.turn_count} turn${run.turn_count === 1 ? '' : 's'} · ${run.delivery_branch}` : ''}
            </p>
          </div>
          <div className="flex gap-1">
            <button type="button" onClick={() => void load()} className="rounded p-2 text-gray-500 hover:bg-gray-800 hover:text-gray-200" title="Refresh"><RefreshCw size={16} className={refreshing ? 'animate-spin' : ''} /></button>
            <button type="button" onClick={onClose} className="inline-flex items-center gap-1.5 rounded px-2 py-1.5 text-xs text-gray-500 hover:bg-gray-800 hover:text-gray-200" aria-label={embedded ? 'Back to Deliveries' : 'Close Delivery'}>{embedded ? <ChevronLeft size={16} /> : <X size={16} />}{embedded && 'Back'}</button>
          </div>
        </header>

        {(openedPlan || openedPlanLoading || openedPlanError) && (
          <div className="absolute inset-0 z-30 flex min-h-0 flex-col bg-gray-900">
            {openedPlanLoading && !openedPlan ? (
              <div className="flex h-full items-center justify-center gap-2 text-sm text-gray-500"><Loader2 size={17} className="animate-spin" /> Loading Plan…</div>
            ) : openedPlanError && !openedPlan ? (
              <div className="m-5 space-y-3"><Notice tone="red" text={openedPlanError} /><button type="button" onClick={() => setOpenedPlanError('')} className="text-xs text-indigo-300">Back to Delivery</button></div>
            ) : openedPlan ? (
              <PlanDetail
                plan={openedPlan}
                onRefresh={refreshOpenedPlan}
                onClose={() => { setOpenedPlan(null); setOpenedPlanError(''); }}
                onNavigateTask={onOpenTask}
                embedded
                contextLabel={`Delivery #${runId}`}
                conversationRequest={run?.requirements}
                activity={progress ? { headline: progress.headline, detail: progress.detail, last_activity_at: progress.last_activity_at, active_agent: progress.active_agent } : undefined}
              />
            ) : null}
          </div>
        )}

        {progress && runSucceeded && run ? (
          <section data-testid="delivery-outcome-summary" className="shrink-0 border-b border-emerald-500/20 bg-gradient-to-r from-emerald-500/10 via-emerald-500/5 to-gray-950/30 px-4 py-4 sm:px-5">
            <div className="flex flex-wrap items-center gap-3 sm:flex-nowrap">
              <span className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl border border-emerald-400/30 bg-emerald-400/10 text-emerald-300">
                <ShieldCheck size={20} />
              </span>
              <div className="min-w-0 flex-1">
                <h3 className="text-base font-semibold text-emerald-100">{reportOnlySuccess ? 'Report completed' : run.terminal === 'merged' ? 'Delivered and merged' : 'Ready to merge'}</h3>
                <p className="mt-0.5 text-xs text-gray-400">{reportOnlySuccess ? 'The requested read-only inspection completed without repository changes or a pull request.' : `All required gates completed successfully across ${run.cycle_count} delivery round${run.cycle_count === 1 ? '' : 's'}.`}</p>
                <div className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-[10px] text-gray-500">
                  <span className="inline-flex items-center gap-1"><CheckCircle2 size={11} className="text-emerald-400" /> {reportOnlySuccess ? 'Plan and read-only report complete · code and PR gates not applicable' : 'Plan, code, frontend and PR checks complete'}</span>
                  <span>{run.turn_count} developer turn{run.turn_count === 1 ? '' : 's'}</span>
                  {run.head_sha && <span className="font-mono">{run.head_sha.slice(0, 10)}</span>}
                  <span>Updated {relativeTime(progress.last_activity_at)}</span>
                </div>
              </div>
              {run.pr_url && (
                <a href={run.pr_url} target="_blank" rel="noreferrer" className="inline-flex shrink-0 items-center gap-2 rounded-lg bg-emerald-500 px-3 py-2 text-xs font-semibold text-gray-950 transition-colors hover:bg-emerald-400">
                  <GitPullRequest size={14} /> Open PR #{run.pr_number ?? '?'} <ChevronRight size={13} />
                </a>
              )}
            </div>
          </section>
        ) : progress && run && (
          <div className={`shrink-0 border-b px-4 py-3 sm:px-5 ${progress.attention_required ? 'border-amber-500/25 bg-amber-500/5' : 'border-gray-800 bg-gray-950/25'}`}>
            <div className="flex flex-wrap items-start justify-between gap-3">
              <div className="min-w-0 flex-1">
                {progress.detail ? <DetailText detail={progress.detail} /> : <p className="text-sm text-gray-300">{progress.headline}</p>}
                {progress.active_agent && <p className="mt-1 text-[10px] text-gray-600">{titleCase(progress.active_agent.role)} · {progress.active_agent.provider || 'provider'} {progress.active_agent.model || ''} · updated {relativeTime(progress.last_activity_at)}</p>}
              </div>
              <DeliveryRunPanel runId={run.id} compact />
            </div>
          </div>
        )}

        {harness && selectedIsCurrent && frontendHarnessActive && (
          <button
            type="button"
            data-testid="delivery-frontend-live-jump"
            onClick={() => {
              if (run?.current_cycle_id != null) setSelectedCycleId(run.current_cycle_id);
              setActiveStage('frontend_review');
            }}
            className="group shrink-0 border-b border-cyan-400/25 bg-gradient-to-r from-cyan-500/12 via-indigo-500/8 to-gray-950 px-4 py-3 text-left transition-colors hover:from-cyan-500/18 sm:px-5"
          >
            <div className="flex items-center gap-3">
              <span className="relative flex h-8 w-8 shrink-0 items-center justify-center rounded-full border border-cyan-400/35 bg-cyan-400/10 text-cyan-300">
                <Loader2 size={15} className="animate-spin" />
                <span className="absolute -right-0.5 -top-0.5 h-2.5 w-2.5 animate-pulse rounded-full border-2 border-gray-950 bg-emerald-400" />
              </span>
              <span className="min-w-0 flex-1">
                <span className="flex flex-wrap items-center gap-2">
                  <span className="text-xs font-semibold text-cyan-100">Frontend Browser Agent</span>
                  <span className="rounded-full border border-cyan-400/30 bg-cyan-400/10 px-2 py-0.5 text-[9px] font-bold uppercase tracking-wide text-cyan-300">
                    Live
                  </span>
                  <span className="text-[10px] text-gray-400">{harnessStageLabel(harness.stage)}</span>
                </span>
                <span className="mt-1 block truncate text-[11px] text-gray-300">
                  {frontendHarnessLatestEvent?.title || '等待 Browser Agent 事件'}
                </span>
                <span className="mt-1 flex flex-wrap gap-x-3 gap-y-0.5 text-[10px] text-gray-500">
                  <span>步骤 {harness.browser_review ? `${harness.browser_review.steps}/${harness.browser_review.max_steps}` : '等待绑定'}</span>
                  <span>动作 {harness.browser_review?.actions ?? 0}</span>
                  <span>事件 {frontendHarnessEvents.length}</span>
                  <span>证据 {frontendHarnessEvidence.length}</span>
                </span>
              </span>
              <span className="inline-flex shrink-0 items-center gap-1 text-[10px] font-medium text-cyan-300 group-hover:text-cyan-200">打开实时过程 <ChevronRight size={12} /></span>
            </div>
          </button>
        )}

        {run && run.cycles.length > 1 && selectedCycle && selectedContext && (
          <section aria-label="Delivery rounds" className="shrink-0 border-b border-gray-800 bg-gray-950/50 px-3 py-2.5 sm:px-5">
            <div className="overflow-x-auto pb-1" role="tablist" aria-label="Delivery round history">
              <div className="flex min-w-max gap-1.5">
                {run.cycles.map((cycle) => {
                  const context = cycleContext(cycle);
                  const isCurrent = cycle.id === run.current_cycle_id;
                  const isSelected = cycle.id === selectedCycle.id;
                  const isFinal = isCurrent && runIsTerminal;
                  return (
                    <button
                      key={cycle.id}
                      type="button"
                      role="tab"
                      aria-selected={isSelected}
                      aria-current={isCurrent ? 'step' : undefined}
                      aria-label={`View round ${cycle.cycle_number}: ${context.label}`}
                      onClick={() => selectCycle(cycle)}
                      className={`rounded border px-2.5 py-1.5 text-left transition-colors ${isSelected ? isFinal ? 'border-emerald-400/40 bg-emerald-500/10' : 'border-indigo-400/50 bg-indigo-500/15' : 'border-gray-800 bg-gray-900/70 hover:border-gray-700 hover:bg-gray-800/80'}`}
                    >
                      <span className="flex items-center justify-between gap-2">
                        <span className={`text-xs font-semibold ${isSelected ? isFinal ? 'text-emerald-100' : 'text-indigo-100' : 'text-gray-300'}`}>Round {cycle.cycle_number}</span>
                        {!isCurrent && <RoundIcon status={cycle.status} />}
                      </span>
                    </button>
                  );
                })}
              </div>
            </div>
          </section>
        )}

        <div className="shrink-0 overflow-x-auto border-b border-gray-800 px-4 py-4 sm:px-6" aria-label="Delivery flow">
          <div className="flex min-w-[720px] items-start" role="group" aria-label="Delivery stages">
            {displayedStages.map((stage, index) => {
              const selected = activeStage === stage.key;
              const completed = stage.state === 'completed' || stage.state === 'skipped';
              return <div key={stage.key} className="flex min-w-0 flex-1 items-start">
                <button
                  type="button"
                  aria-pressed={selected}
                  aria-label={`${stage.label}: ${titleCase(stage.state)}`}
                  onClick={() => setActiveStage(stage.key)}
                  className="group flex w-24 shrink-0 flex-col items-center gap-2 text-center"
                >
                  <span className={`flex h-9 w-9 items-center justify-center rounded-full border-2 transition-colors ${selected ? 'border-indigo-300 bg-indigo-500/20 text-indigo-200 ring-4 ring-indigo-500/10' : completed ? 'border-emerald-500/60 bg-emerald-500/10 text-emerald-300' : stage.state === 'failed' || stage.state === 'cancelled' ? 'border-red-500/60 bg-red-500/10 text-red-300' : 'border-gray-700 bg-gray-900 text-gray-500 group-hover:border-gray-500 group-hover:text-gray-300'}`}>
                    <StageIcon state={stage.state} />
                  </span>
                  <span className={`text-[11px] font-medium leading-4 ${selected ? 'text-indigo-200' : 'text-gray-400'}`}>{stage.label}</span>
                </button>
                {index < displayedStages.length - 1 && <span aria-hidden="true" className={`mt-[17px] h-px min-w-5 flex-1 ${completed ? 'bg-emerald-500/50' : 'bg-gray-700'}`} />}
              </div>;
            })}
          </div>
        </div>

        <div className="min-h-0 flex-1 overflow-y-auto p-4 sm:p-5">
          {loading && !run ? (
            <div className="flex h-full items-center justify-center gap-2 text-sm text-gray-500"><Loader2 size={17} className="animate-spin" /> Loading Delivery…</div>
          ) : error && !run ? (
            <Notice tone="red" text={error} />
          ) : run && progress && selectedCycle && selectedContext ? (
            <div className="space-y-4">
              {error && <Notice tone="red" text={error} />}
              <div className="min-w-0 space-y-4" aria-live="polite">
                <section className="rounded-xl border border-gray-800 bg-gray-950/30 p-4">
                  <div className="mb-4"><h3 className="text-sm font-semibold text-gray-100">{STAGE_META[activeStage].label}</h3><p className="mt-1 text-xs text-gray-600">{STAGE_META[activeStage].description}</p></div>
                  {stageContent(activeStage)}
                </section>
              </div>
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
}

function Metric({ label, value, mono = false }: { label: string; value: string; mono?: boolean }) {
  return <div className="rounded-lg border border-gray-800 bg-gray-950/60 p-2.5"><div className="text-[10px] uppercase tracking-wide text-gray-600">{label}</div><div className={`mt-1 break-all text-xs text-gray-300 ${mono ? 'font-mono' : ''}`}>{value}</div></div>;
}

function EmptyState({ text }: { text: string }) {
  return <div className="rounded-lg border border-dashed border-gray-800 px-3 py-5 text-center text-xs text-gray-600">{text}</div>;
}

function Notice({ tone, text }: { tone: 'amber' | 'red'; text: string }) {
  return <div className={`rounded-lg border px-3 py-2 text-xs leading-5 ${tone === 'red' ? 'border-red-500/30 bg-red-500/10 text-red-300' : 'border-amber-500/25 bg-amber-500/10 text-amber-300'}`}>{text}</div>;
}

function StageIcon({ state }: { state: string }) {
  if (state === 'completed') return <CheckCircle2 size={17} className="shrink-0 text-emerald-400" />;
  if (state === 'failed' || state === 'cancelled') return <XCircle size={17} className="shrink-0 text-red-400" />;
  if (state === 'skipped') return <Circle size={17} className="shrink-0 text-gray-600" />;
  return <Circle size={17} className={`shrink-0 ${['running', 'waiting', 'paused', 'ready'].includes(state) ? 'fill-indigo-500/20 text-indigo-400' : 'text-gray-700'}`} />;
}

function RoundIcon({ status }: { status: string }) {
  if (status === 'completed') return <CheckCircle2 size={12} className="text-emerald-500" aria-label="Completed" />;
  if (['failed', 'cancelled', 'superseded'].includes(status)) return <XCircle size={12} className="text-red-400" aria-label={titleCase(status)} />;
  return <Circle size={12} className="fill-indigo-500/20 text-indigo-400" aria-label={titleCase(status)} />;
}
