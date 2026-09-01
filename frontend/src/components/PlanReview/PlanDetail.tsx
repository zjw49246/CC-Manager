import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import {
  api,
  isApiRequestError,
  type DeliveryAgentActivity,
  type PlanInputRequest,
  type PlanResource,
  type PlanRun,
  type PlanStaleness,
  type PlanVersion,
  type UploadResult,
} from '../../api/client';
import { useFileUpload } from '../../hooks/useFileUpload';
import { useVisibilityAwareInterval } from '../../hooks/useVisibilityAwareInterval';
import { AlertCircle, Archive, ArchiveRestore, Check, ChevronLeft, Loader2, Paperclip, Play, RefreshCw, X } from '../icons';
import { MarkdownContent } from '../MarkdownContent';
import { CollapsiblePlanningRequest } from './CollapsiblePlanningRequest';
import { PlanInputForm } from './PlanInputForm';
import { planDisplayStateLabel, planVersionDisplayLabel } from './planResourceStatus';
import { PlanRunInputAudit } from './PlanRunInputAudit';
import {
  planHardConflictMessages,
  planStalenessConfirmationMessage,
  planStalenessMessages,
} from './planStaleness';

interface Props {
  plan: PlanResource;
  onRefresh: () => void | Promise<void>;
  onClose?: () => void;
  selectedVersionIds?: number[];
  onToggleVersion?: (versionId: number) => void;
  onAttachVersion?: (versionId: number) => void;
  onNavigateTask?: (taskId: number) => void;
  onNavigateDelivery?: (runId: number) => void;
  embedded?: boolean;
  contextLabel?: string;
  conversationRequest?: string;
  activity?: {
    headline: string;
    detail: string | null;
    last_activity_at: string | null;
    active_agent: DeliveryAgentActivity | null;
  };
}

function uploadPayload(results: UploadResult[]) {
  return results.length ? {
    file_paths: results.map((item) => item.path),
    image_paths: results.filter((item) => item.is_image).map((item) => item.path),
    attachments: results.map((item) => ({
      url: item.url,
      name: item.filename || item.url.split('/').pop() || 'file',
      is_image: item.is_image,
    })),
  } : {};
}

function confirmableStaleness(error: unknown): PlanStaleness | null {
  if (!isApiRequestError(error) || error.status !== 409 || !error.detail || typeof error.detail !== 'object') return null;
  const detail = error.detail as Record<string, unknown>;
  return detail.stale === true && detail.can_confirm !== false && detail.hard_conflict !== true
    ? detail as unknown as PlanStaleness
    : null;
}

const ACTIVE_RUN_STATUSES = new Set<PlanRun['status']>(['queued', 'running', 'waiting_user', 'cancelling']);

function planStatusText(plan: PlanResource, candidateVersionNumber: number, run: PlanRun | null) {
  if (!run || !ACTIVE_RUN_STATUSES.has(run.status)) {
    return planDisplayStateLabel(plan.display_state);
  }
  if (run.status === 'waiting_user') return `v${candidateVersionNumber} needs your input`;
  if (run.status === 'cancelling') return `Cancelling v${candidateVersionNumber} generation`;
  if (run.current_stage === 'reviewer' || plan.display_state === 'reviewer') {
    return `Reviewing v${candidateVersionNumber} candidate · actions unlock when review finishes`;
  }
  if (run.status === 'queued') return `v${candidateVersionNumber} generation queued`;
  const base = plan.current_version?.version_number;
  return base == null
    ? `Creating v${candidateVersionNumber} draft`
    : `Creating v${candidateVersionNumber} draft from v${base}`;
}

function stepDebugLabel(run: PlanRun, stepIndex: number) {
  const step = run.steps[stepIndex];
  const peers = run.steps.filter((item) => item.step_type === step.step_type && item.round === step.round);
  if (peers.length <= 1) return step.step_type;
  return `${step.step_type} call ${peers.findIndex((item) => item.id === step.id) + 1}`;
}

function uniqueRunError(run: PlanRun) {
  const normalized = run.error?.trim();
  if (!normalized || run.steps.some((step) => step.error?.trim() === normalized)) return null;
  return normalized;
}

function PlanConversation({ plan, runs, request, activity }: { plan: PlanResource; runs: PlanRun[]; request?: string; activity?: Props['activity'] }) {
  const orderedRuns = [...runs].sort((left, right) => left.id - right.id);
  return (
    <section aria-label="Plan conversation" className="mt-3 space-y-3 border-t border-gray-800 pt-3 sm:mt-4 sm:pt-4">
      <div className="flex items-center justify-between gap-3">
        <div>
          <h3 className="text-xs font-semibold uppercase tracking-wide text-gray-400">Conversation</h3>
        </div>
        {orderedRuns.some((run) => ACTIVE_RUN_STATUSES.has(run.status)) && <span className="inline-flex items-center gap-1.5 rounded-full bg-indigo-500/10 px-2 py-1 text-[10px] text-indigo-300"><Loader2 size={11} className="animate-spin" /> Live</span>}
      </div>
      <div className="space-y-4">
        <div className="ml-auto max-w-[min(72ch,88%)] rounded-2xl rounded-br-sm bg-indigo-600 px-3.5 py-2.5 text-sm text-white">
          <div className="mb-0.5 text-[10px] font-semibold uppercase tracking-wide text-indigo-200">You</div>
          <div className="whitespace-pre-wrap leading-5">{request || plan.initial_request}</div>
        </div>
        {activity && (activity.headline || activity.active_agent) && <div className="max-w-[min(76ch,92%)] rounded-2xl rounded-bl-sm border border-gray-800 bg-gray-900 px-3.5 py-2.5 text-sm text-gray-300">
          <div className="mb-1 flex flex-wrap items-center gap-2 text-[10px]">
            <span className="font-semibold uppercase tracking-wide text-indigo-300">{activity.active_agent ? activity.active_agent.role.replaceAll('_', ' ') : 'Planner'}</span>
            {activity.active_agent?.provider && <span className="text-gray-500">{activity.active_agent.provider}{activity.active_agent.model ? ` · ${activity.active_agent.model}` : ''}</span>}
            {activity.last_activity_at && <span className="text-gray-600">{activity.last_activity_at}</span>}
          </div>
          <p className="text-sm text-gray-200">{activity.headline}</p>
          {activity.detail && <p className="mt-1 whitespace-pre-wrap text-xs leading-5 text-gray-500">{activity.detail}</p>}
        </div>}
        {orderedRuns.map((run) => (
          <div key={run.id} className="space-y-3">
            {run.steps.map((step) => {
              const role = step.step_type === 'reviewer' ? 'Reviewer' : 'Planner';
              const active = ['queued', 'running'].includes(step.status);
              return (
                <div key={step.id} className="max-w-[min(76ch,92%)] rounded-2xl rounded-bl-sm border border-gray-800 bg-gray-900 px-3.5 py-2.5 text-sm text-gray-300">
                  <div className="mb-2 flex flex-wrap items-center gap-2 text-[10px]">
                    <span className="font-semibold uppercase tracking-wide text-indigo-300">{role}</span>
                    <span className="text-gray-600">Round {step.round}</span>
                    <span className="text-gray-600">{step.provider}{step.model ? ` · ${step.model}` : ''}</span>
                    <span className={active ? 'text-indigo-300' : step.status === 'failed' ? 'text-red-300' : 'text-gray-500'}>{active && <Loader2 size={10} className="mr-1 inline animate-spin" />}{step.status}</span>
                  </div>
                  {step.output ? <MarkdownContent content={step.output} /> : active ? (
                    <p className="text-xs text-gray-500">{step.last_event_type ? `Working · ${step.last_event_type}` : 'Working…'}{step.streamed_output_chars ? ` · ${step.streamed_output_chars} visible characters` : ''}</p>
                  ) : <p className="text-xs text-gray-600">No public message was produced.</p>}
                  {step.error && <pre className="mt-2 overflow-x-auto whitespace-pre-wrap break-words text-xs text-red-300/80">{step.error}</pre>}
                </div>
              );
            })}
          </div>
        ))}
        {orderedRuns.length === 0 && <div className="rounded-lg border border-dashed border-gray-800 px-3 py-5 text-center text-xs text-gray-600">The Planner has not started yet.</div>}
      </div>
    </section>
  );
}

export function PlanDetail({ plan, onRefresh, onClose, selectedVersionIds = [], onToggleVersion, onAttachVersion, onNavigateTask, onNavigateDelivery, embedded = false, contextLabel, conversationRequest, activity }: Props) {
  const [versions, setVersions] = useState<PlanVersion[]>([]);
  const [runs, setRuns] = useState<PlanRun[]>([]);
  const [versionId, setVersionId] = useState<number | null>(plan.current_version_id);
  const [revision, setRevision] = useState('');
  const [compare, setCompare] = useState(false);
  const [busy, setBusy] = useState(false);
  const [busyLabel, setBusyLabel] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [staleness, setStaleness] = useState<PlanStaleness | null>(null);
  const uploads = useFileUpload();
  const clearUploads = uploads.clear;
  const fileInput = useRef<HTMLInputElement>(null);
  const previousPlanId = useRef(plan.id);
  const previousCurrentVersionId = useRef(plan.current_version_id);
  const loadRequest = useRef(0);

  const load = useCallback(async () => {
    const requestId = ++loadRequest.current;
    const [versionRows, runRows] = await Promise.all([
      api.listPlanVersions(plan.id),
      api.listPlanResourceRuns(plan.id),
    ]);
    if (requestId !== loadRequest.current) return;
    setVersions(versionRows);
    setRuns(runRows);
    setVersionId((current) => versionRows.some((item) => item.id === current) ? current : plan.current_version_id);
  }, [plan.current_version_id, plan.id]);

  useEffect(() => {
    if (previousPlanId.current === plan.id) return;
    previousPlanId.current = plan.id;
    setVersions([]);
    setRuns([]);
    setVersionId(plan.current_version_id);
    setRevision('');
    setCompare(false);
    setBusy(false);
    setBusyLabel(null);
    setError(null);
    setStaleness(null);
    clearUploads();
  }, [plan.id, plan.current_version_id, clearUploads]);
  useEffect(() => { void load().catch((reason) => setError(reason instanceof Error ? reason.message : String(reason))); }, [load]);
  useEffect(() => {
    setVersionId((current) => current === previousCurrentVersionId.current ? plan.current_version_id : current);
    previousCurrentVersionId.current = plan.current_version_id;
  }, [plan.current_version_id]);
  const shown = versions.find((item) => item.id === versionId) || plan.current_version || null;
  const previous = useMemo(() => shown ? versions.find((item) => item.version_number === shown.version_number - 1) || null : null, [shown, versions]);
  const appliedVersion = versions.find((item) => item.applied) || null;
  const executionApplications = (plan.applications || []).filter((item) => item.execution_task_id != null);
  const uncertainApplications = (plan.applications || []).filter((item) => (
    item.delivery_status === 'uncertain' && item.application_receipt_key
  ));
  const applicationAttempts = plan.application_attempts || [];
  const currentUser = (() => {
    try { return JSON.parse(localStorage.getItem('cc_user') || '{}') as { id?: number; role?: string }; }
    catch { return {} as { id?: number; role?: string }; }
  })();
  const isAdmin = currentUser.role === 'admin' || currentUser.role === 'super_admin' || !currentUser.id;
  const latestFailedRun = runs.find((run) => run.status === 'failed') || null;
  const activeRunSnapshot = runs.find((run) => run.id === plan.active_run_id) || plan.active_run;
  const activeRun = activeRunSnapshot && ACTIVE_RUN_STATUSES.has(activeRunSnapshot.status)
    ? activeRunSnapshot
    : null;
  const candidateVersionNumber = Math.max(
    plan.current_version?.version_number || 0,
    ...versions.map((version) => version.version_number),
  ) + 1;

  useEffect(() => {
    if (!shown) { setStaleness(null); return; }
    void api.getPlanVersionStaleness(shown.id).then(setStaleness).catch(() => setStaleness(null));
  }, [shown]);

  const refreshDetail = useCallback(async () => {
    await Promise.all([onRefresh(), load()]);
  }, [load, onRefresh]);

  useVisibilityAwareInterval(
    () => refreshDetail().catch(() => undefined),
    2_000,
    Boolean(activeRun),
    false,
  );

  const mutate = async (
    label: string,
    operation: () => Promise<unknown>,
    onSuccess?: () => void,
  ) => {
    setBusy(true); setBusyLabel(label); setError(null);
    try { await operation(); await refreshDetail(); onSuccess?.(); }
    catch (reason) { setError(reason instanceof Error ? reason.message : String(reason)); }
    finally { setBusy(false); setBusyLabel(null); }
  };

  const decide = async (decision: 'approve' | 'reject', attach = false) => {
    if (!shown || shown.id !== plan.current_version_id) return;
    await mutate(decision === 'approve' ? 'Approving Plan' : 'Rejecting Plan', async () => {
      const invoke = (confirm: boolean) => decision === 'approve'
        ? api.approvePlanVersion(shown.id, shown.id, confirm)
        : api.rejectPlanVersion(shown.id, shown.id, confirm);
      try { await invoke(false); }
      catch (reason) {
        const stale = confirmableStaleness(reason);
        if (!stale || !window.confirm(planStalenessConfirmationMessage(stale, 'approve'))) throw reason;
        await invoke(true);
      }
      if (decision === 'approve' && attach) onAttachVersion?.(shown.id);
    }, onClose);
  };

  const createExecution = async (approveIfPending: boolean) => {
    if (!shown || shown.id !== plan.current_version_id) return;
    await mutate('Creating execution Task', async () => {
      const invoke = (confirm: boolean) => api.createVersionExecutionTask(shown.id, shown.id, confirm, approveIfPending);
      let result;
      try { result = await invoke(false); }
      catch (reason) {
        const stale = confirmableStaleness(reason);
        if (!stale || !window.confirm(planStalenessConfirmationMessage(stale, 'execute'))) throw reason;
        result = await invoke(true);
      }
      onNavigateTask?.(result.execution_task_id);
    }, onClose);
  };

  const revise = async () => {
    if (!shown || !revision.trim() || plan.active_run_id != null) return;
    await mutate('Starting revision', async () => {
      await api.createPlanRun(plan.id, {
        run_type: 'user_revision',
        request: revision.trim(),
        base_version_id: shown.id,
        expected_current_version_id: plan.current_version_id || undefined,
        ...uploadPayload(uploads.uploadedResults),
      });
      setRevision(''); uploads.clear();
    });
  };

  const resolveDelivery = async (
    receiptKey: string,
    action: 'confirm_launched' | 'release_for_retry',
  ) => {
    const confirmation = action === 'confirm_launched'
      ? 'Confirm only after verifying that this exact Task generation/turn exists or executed. Mark this delivery as launched?'
      : 'Confirm only after verifying that this exact Task generation/turn never launched. Release the Version so it can be applied again?';
    if (!window.confirm(confirmation)) return;
    const note = window.prompt('Record the evidence used for this decision:')?.trim();
    if (!note) return;
    await mutate(
      action === 'confirm_launched' ? 'Confirming Plan delivery' : 'Releasing Plan delivery',
      () => api.resolvePlanApplicationDelivery(plan.id, receiptKey, action, note),
    );
  };

  const answered = async (answeredRequest?: PlanInputRequest) => {
    if (answeredRequest) {
      setRuns((currentRuns) => currentRuns.map((run) => run.id !== answeredRequest.run_id ? run : {
        ...run,
        status: 'queued',
        current_stage: 'planner',
        generation: run.generation + 1,
        open_input_request_id: null,
        input_requests: run.input_requests.map((request) => (
          request.id === answeredRequest.id ? answeredRequest : request
        )),
      }));
    }
    await refreshDetail();
  };

  const current = shown?.id === plan.current_version_id;
  const route = plan.pipeline_config;
  const staleMessages = planStalenessMessages(staleness);
  const hardConflictMessages = planHardConflictMessages(staleness);
  const showStaleness = Boolean(shown && !shown.applied && !plan.read_only);
  return <div className="flex h-full min-h-0 min-w-0 flex-col overflow-x-hidden">
    <header className="flex items-start gap-3 border-b border-gray-800 px-4 py-2.5 sm:px-5">
      <div className="min-w-0 flex-1">
        {contextLabel && <div className="mb-0.5 text-[10px] font-medium uppercase tracking-wide text-indigo-300">{contextLabel}</div>}
        <div className="truncate text-base font-semibold text-gray-100">{plan.title}</div>
        <div role={activeRun ? 'status' : undefined} aria-live={activeRun ? 'polite' : undefined} className="mt-1 flex flex-wrap items-center gap-x-1.5 gap-y-0.5 text-[11px] text-gray-400">
          {activeRun && activeRun.status !== 'waiting_user' && <Loader2 size={11} className="animate-spin text-indigo-300" />}
          <span className={activeRun ? 'text-indigo-300' : ''}>{planStatusText(plan, candidateVersionNumber, activeRun)}</span>
          {plan.current_version && <span>· v{plan.current_version.version_number} current</span>}
          {appliedVersion && appliedVersion.id !== plan.current_version_id && <span className="text-teal-300">· v{appliedVersion.version_number} applied</span>}
          {showStaleness && staleness?.stale && <span className="rounded-full bg-amber-500/15 px-2 py-0.5 text-amber-300">stale context</span>}
          {showStaleness && staleness?.hard_conflict && <span className="rounded-full bg-red-500/15 px-2 py-0.5 text-red-300">target conflict</span>}
        </div>
      </div>
      {onClose && <button type="button" onClick={onClose} aria-label={contextLabel ? `Back to ${contextLabel}` : embedded ? 'Back to Plans' : 'Close Plan'} className="inline-flex items-center gap-1 rounded-lg px-2 py-1.5 text-xs text-gray-500 transition-colors hover:bg-gray-800 hover:text-gray-200">{embedded ? <><ChevronLeft size={16} /> {contextLabel ? 'Back to Delivery' : 'Back'}</> : <X size={16} />}</button>}
    </header>

    <div className="min-h-0 min-w-0 flex-1 overflow-x-hidden overflow-y-auto p-4 sm:p-6">
      {error && <div className="mb-4 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs text-red-300">{error}</div>}
      {busyLabel && <div role="status" aria-live="polite" className="mb-4 flex items-center gap-2 rounded-lg border border-indigo-500/30 bg-indigo-500/10 px-3 py-2 text-xs text-indigo-300"><Loader2 size={13} className="animate-spin" /> {busyLabel}…</div>}
      {plan.read_only && <div data-testid="capability-plan-read-only" className="mb-3 flex items-center gap-2 text-[11px] text-gray-500"><span className="h-1.5 w-1.5 rounded-full bg-indigo-400" />Delivery-owned plan · actions are managed from the Delivery flow</div>}
      {plan.latest_run_status === 'failed' && plan.latest_run_error && <div role="alert" className="mb-4 rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs text-amber-300"><span className="font-semibold">Latest planning attempt failed.</span> {plan.read_only ? 'Capability Core controls retry and terminal handling.' : 'You can retry it; technical details are available below.'}</div>}
      {uncertainApplications.map((application) => <div key={application.application_receipt_key} role="alert" className="mb-4 rounded-lg border border-red-500/40 bg-red-500/10 px-3.5 py-3 text-sm text-red-100">
        <div className="flex items-start gap-2.5">
          <AlertCircle size={18} className="mt-0.5 shrink-0 text-red-300" />
          <div className="min-w-0 flex-1">
            <div className="font-semibold text-red-300">Plan delivery needs reconciliation</div>
            <p className="mt-1 leading-5">CCM restarted after claiming this delivery, so automatic replay was blocked. The Version remains applied until an administrator verifies whether the exact turn launched.</p>
            {application.delivery_error && <p className="mt-1 text-xs text-red-200/80">{application.delivery_error}</p>}
            <details className="mt-2 text-xs text-red-100/75"><summary className="cursor-pointer">Launch evidence and receipt</summary><pre className="mt-1 overflow-x-auto whitespace-pre-wrap break-all">{application.application_receipt_key}{'\n'}{JSON.stringify(application.launch_evidence || {}, null, 2)}</pre></details>
            {plan.read_only ? <p className="mt-2 text-xs text-red-200/80">Capability Core controls reconciliation for this Plan.</p> : isAdmin ? <div className="mt-3 flex flex-wrap gap-2">
              <button type="button" disabled={busy} onClick={() => void resolveDelivery(application.application_receipt_key!, 'confirm_launched')} className="rounded-lg border border-emerald-500/50 px-3 py-1.5 text-xs text-emerald-200 transition-colors hover:bg-emerald-500/10 disabled:pointer-events-none disabled:opacity-40">Confirm exact turn launched</button>
              <button type="button" disabled={busy} onClick={() => void resolveDelivery(application.application_receipt_key!, 'release_for_retry')} className="rounded-lg border border-red-400/50 px-3 py-1.5 text-xs text-red-100 transition-colors hover:bg-red-500/15 disabled:pointer-events-none disabled:opacity-40">Confirm no turn · release Version</button>
            </div> : <p className="mt-2 text-xs text-red-200/80">Ask an administrator to reconcile this delivery.</p>}
          </div>
        </div>
      </div>)}
      {showStaleness && staleness?.stale && !staleness.hard_conflict && <div role="alert" className="mb-4 flex items-start gap-2.5 rounded-lg border border-amber-500/50 bg-amber-500/15 px-3.5 py-3 text-gray-200">
        <AlertCircle size={18} className="mt-0.5 shrink-0 text-amber-400" />
        <div className="min-w-0">
          <div className="text-sm font-semibold text-amber-300">Context changed</div>
          <div className="mt-0.5 text-sm leading-5">{staleMessages.join(' ')} Confirm to continue, or regenerate first.</div>
        </div>
      </div>}
      {showStaleness && staleness?.hard_conflict && <div className="mb-4 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs text-red-300"><span className="font-semibold">This action is blocked.</span> {hardConflictMessages.join(' ')}</div>}
      {!plan.read_only && <CollapsiblePlanningRequest content={plan.initial_request} />}

      <PlanConversation plan={plan} runs={runs} request={conversationRequest} activity={activity} />

      {activeRun?.status === 'waiting_user' && plan.open_input_request && <div className="mt-4"><PlanInputForm key={plan.open_input_request.id} run={activeRun} request={plan.open_input_request} onAnswered={answered} /></div>}
      {activeRun && <PlanRunInputAudit run={activeRun} title={`v${candidateVersionNumber} ${activeRun.run_type === 'user_revision' ? 'revision & input history' : 'input history'}`} defaultOpen />}

      {activeRun?.draft_content && <section className="mt-4 min-w-0 rounded-xl border border-indigo-500/35 bg-indigo-500/5 p-4">
        <div className="mb-2 text-xs font-semibold text-indigo-300">v{candidateVersionNumber} candidate · not a Version yet</div>
        <MarkdownContent content={activeRun.draft_content} />
      </section>}

      {shown ? <>
        {!current && <div className="mt-4 rounded-lg border border-gray-700 bg-gray-800/60 px-3 py-2 text-xs text-gray-400">Historical Version. {plan.read_only ? 'Capability-owned history is read-only.' : 'You can revise from it explicitly; approval remains limited to the current Version.'}</div>}
        <div className="mt-4 flex flex-wrap items-center gap-2">
          <select aria-label="Plan Version" value={shown.id} onChange={(event) => setVersionId(Number(event.target.value))} className="rounded-lg border border-gray-700 bg-gray-800 px-2 py-1.5 text-xs text-gray-200">
            {versions.map((version) => <option key={version.id} value={version.id}>v{version.version_number} · {planVersionDisplayLabel(version)}{version.id === plan.current_version_id ? ' · Current' : ''}</option>)}
          </select>
          {previous && <button type="button" onClick={() => setCompare((value) => !value)} className="rounded-lg border border-gray-700 px-2.5 py-1.5 text-xs text-gray-300 transition-colors hover:border-gray-600 hover:bg-gray-800">{compare ? 'Hide comparison' : `Compare with v${previous.version_number}`}</button>}
        </div>
        <div className={`mt-3 grid min-w-0 gap-3 ${compare && previous ? 'lg:grid-cols-2' : ''}`}>
          {compare && previous && <div className="min-w-0 rounded-xl border border-gray-700 bg-gray-950/60 p-4"><div className="mb-2 text-xs text-gray-500">v{previous.version_number}</div><MarkdownContent content={previous.content} /></div>}
          <div className="min-w-0 rounded-xl border border-gray-700 bg-gray-950/60 p-4"><div className="mb-2 text-xs text-indigo-300">v{shown.version_number} · {planVersionDisplayLabel(shown)}{current ? ' · Current' : ''}</div><MarkdownContent content={shown.content} /></div>
        </div>
        {shown.review_feedback && <div className="mt-3 rounded-xl border border-gray-700 bg-gray-800/60 p-3 text-sm text-gray-300"><div className="mb-1 text-xs font-semibold text-gray-500">Reviewer feedback</div>{shown.review_feedback}</div>}
        <PlanRunInputAudit runs={runs} version={shown} />
      </> : <p className="mt-3 text-xs text-gray-500">No version has been produced yet.</p>}

      {!plan.read_only && <details className="mt-4 rounded-lg border border-gray-800 bg-gray-950/30 px-3 py-2 text-xs text-gray-400">
        <summary className="cursor-pointer font-medium text-gray-500">Technical details</summary>
        <div className="mt-3 space-y-4">
          <section>
            <div className="mb-1 font-semibold text-gray-300">Pipeline routes</div>
            <div className="grid gap-2 sm:grid-cols-2">
              <div>Planner: {route.planner.primary.provider} / {route.planner.primary.model} / {route.planner.primary.effort || 'default'}<br />Fallback: {route.planner.fallback.provider} / {route.planner.fallback.model}</div>
              <div>Reviewer: {route.reviewer.enabled ? `${route.reviewer.primary.provider} / ${route.reviewer.primary.model} / ${route.reviewer.primary.effort || 'default'}` : 'disabled'}<br />Fallback: {route.reviewer.enabled ? `${route.reviewer.fallback.provider} / ${route.reviewer.fallback.model} / ${route.reviewer.fallback.effort || 'default'}` : 'disabled'}<br />Input pauses: {route.max_interactions} · revision rounds: {route.max_revision_cycles}</div>
            </div>
          </section>
          {runs.length > 0 && <section>
            <div className="mb-1 font-semibold text-gray-300">Runs</div>
            <div className="space-y-2">{runs.map((run) => <div key={run.id} className="border-t border-gray-800 pt-2 first:border-0 first:pt-0">Run #{run.id} · {run.run_type} · {run.status} · round {run.round}{run.steps.map((step, stepIndex) => <div key={step.id} className="ml-3 text-gray-500"><div>{stepDebugLabel(run, stepIndex)}: {step.provider}/{step.model || 'default'} ({step.route_slot || 'primary'}) · {step.status} · generation {step.generation ?? '?'}</div>{(step.last_delta_at || step.last_event_type || (step.streamed_output_chars ?? 0) > 0) && <div className="ml-3 text-[10px] text-gray-600">last delta: {step.last_delta_at || 'none'} · streamed chars: {step.streamed_output_chars || 0} · last event: {step.last_event_type || 'none'}</div>}{step.error && <pre className="ml-3 mt-1 overflow-x-auto whitespace-pre-wrap break-all text-red-300/80">{step.error}</pre>}</div>)}{uniqueRunError(run) && <pre className="mt-1 overflow-x-auto whitespace-pre-wrap break-all text-red-300/80">{uniqueRunError(run)}</pre>}</div>)}</div>
          </section>}
          {shown && (shown.repo_revision || shown.reviewer_repo_revision) && <section>
            <div className="mb-1 font-semibold text-gray-300">Repository audit</div>
            <div className="grid gap-2 lg:grid-cols-2"><div><div className="mb-1 text-[10px] uppercase tracking-wide text-gray-500">Planner snapshot</div><pre className="overflow-x-auto whitespace-pre-wrap break-all">{JSON.stringify(shown.repo_revision, null, 2)}</pre></div><div><div className="mb-1 text-[10px] uppercase tracking-wide text-gray-500">Reviewer snapshot</div><pre className="overflow-x-auto whitespace-pre-wrap break-all">{JSON.stringify(shown.reviewer_repo_revision, null, 2)}</pre></div></div>
          </section>}
          {plan.applications.length > 0 && <section>
            <div className="mb-1 font-semibold text-gray-300">Applications ({plan.applications.length})</div>
            <div className="space-y-1">{plan.applications.map((application) => { const applied = versions.find((item) => item.id === application.plan_version_id); return <div key={application.id}>v{applied?.version_number || '?'} · {application.application_type === 'execution_task' ? `execution Task #${application.execution_task_id}` : `chat message #${application.user_log_id}`}{application.delivery_status ? ` · delivery ${application.delivery_status}` : ''}</div>; })}</div>
          </section>}
          {applicationAttempts.length > 0 && <section>
            <div className="mb-1 font-semibold text-gray-300">Delivery history ({applicationAttempts.length})</div>
            <div className="space-y-2">{applicationAttempts.map((attempt) => {
              const applied = versions.find((item) => item.id === attempt.plan_version_id);
              const action = typeof attempt.delivery_resolution?.action === 'string' ? attempt.delivery_resolution.action : null;
              const note = typeof attempt.delivery_resolution?.note === 'string' ? attempt.delivery_resolution.note : null;
              return <div key={attempt.id} className="border-t border-gray-800 pt-2 first:border-0 first:pt-0">
                <div>v{applied?.version_number || '?'} · receipt {attempt.application_receipt_key} · delivery {attempt.delivery_status}{action ? ` · ${action}` : ''}</div>
                {note && <div className="mt-1 text-gray-300">Resolution note: {note}</div>}
                {attempt.delivery_error && <div className="mt-1 text-red-300/80">{attempt.delivery_error}</div>}
                <details className="mt-1"><summary className="cursor-pointer">Evidence</summary><pre className="mt-1 overflow-x-auto whitespace-pre-wrap break-all">{JSON.stringify(attempt.launch_evidence || {}, null, 2)}</pre></details>
              </div>;
            })}</div>
          </section>}
        </div>
      </details>}

      {shown && !plan.active_run_id && !plan.read_only && <div className="mt-5 space-y-2 border-t border-gray-800 pt-4">
        <textarea value={revision} onChange={(event) => setRevision(event.target.value)} rows={3} maxLength={50000} placeholder={`Revise from v${shown.version_number}…`} disabled={busy} className="w-full resize-y rounded-lg border border-gray-700 bg-gray-800 px-3 py-2 text-sm text-gray-100 outline-none focus:border-indigo-500 disabled:opacity-60" />
        {uploads.uploads.length > 0 && <div className="flex flex-wrap gap-2">{uploads.uploads.map((upload) => <span key={upload.id} className="flex items-center gap-1 rounded-lg border border-gray-700 bg-gray-800 px-2 py-1 text-xs text-gray-300">{upload.preview && <img src={upload.preview} alt="" className="h-8 w-8 rounded object-cover" />}<span className="max-w-40 truncate">{upload.file?.name || upload.result?.filename || 'file'}</span>{upload.status === 'uploading' && <Loader2 size={11} className="animate-spin" />}<button type="button" disabled={busy} onClick={() => uploads.removeFile(upload.id)} className="rounded p-0.5 text-gray-500 transition-colors hover:bg-gray-700 hover:text-gray-200 disabled:pointer-events-none"><X size={11} /></button></span>)}</div>}
        <div className="flex flex-wrap gap-2">
          <input ref={fileInput} type="file" multiple className="hidden" onChange={(event) => { uploads.addFiles(Array.from(event.target.files || []), setError); event.target.value = ''; }} />
          <button type="button" disabled={busy} onClick={() => fileInput.current?.click()} className="flex items-center gap-1 rounded-lg border border-gray-700 px-3 py-2 text-xs text-gray-300 transition-colors hover:border-gray-600 hover:bg-gray-800 disabled:pointer-events-none disabled:opacity-40"><Paperclip size={12} /> Revision files</button>
          <button type="button" disabled={busy || !revision.trim() || uploads.isUploading || uploads.hasFailed} onClick={() => void revise()} className="rounded-lg border border-indigo-500/40 px-3 py-2 text-xs text-indigo-300 transition-colors hover:border-indigo-400/60 hover:bg-indigo-500/10 disabled:pointer-events-none disabled:opacity-40">Revise from v{shown.version_number}</button>
        </div>
      </div>}

      <div className="mt-4 flex flex-wrap gap-2">
        {plan.target_task_id != null && onNavigateTask && (plan.delivery_run_id == null || !embedded) && <button type="button" disabled={busy} onClick={() => { if (plan.delivery_run_id != null) { onNavigateDelivery?.(plan.delivery_run_id); return; } onNavigateTask(plan.target_task_id!); onClose?.(); }} className="rounded-lg border border-indigo-500/40 px-3 py-2 text-xs text-indigo-300 transition-colors hover:border-indigo-400/60 hover:bg-indigo-500/10 disabled:pointer-events-none disabled:opacity-40">{plan.delivery_run_id != null ? `Open Delivery DLV-${plan.delivery_run_id}` : `Open related Task #${plan.target_task_id}`}</button>}
        {!plan.read_only && !plan.active_run_id && latestFailedRun && <button type="button" disabled={busy} onClick={() => void mutate('Retrying planning', () => api.createPlanRun(plan.id, { run_type: 'retry', request: latestFailedRun.request_text || plan.initial_request, base_version_id: latestFailedRun.base_version_id || undefined, expected_current_version_id: plan.current_version_id || undefined, source_run_id: latestFailedRun.id }))} className="flex items-center gap-1 rounded-lg bg-indigo-600 px-3 py-2 text-xs font-semibold text-white transition-colors hover:bg-indigo-500 disabled:pointer-events-none disabled:opacity-40"><RefreshCw size={12} /> Retry planning</button>}
        {!plan.read_only && shown && current && plan.display_state === 'awaiting_review' && plan.target_task_id != null && <><button type="button" disabled={busy || staleness?.hard_conflict} onClick={() => void decide('approve', true)} className="flex items-center gap-1 rounded-lg bg-emerald-600 px-3 py-2 text-xs font-semibold text-white transition-colors hover:bg-emerald-500 disabled:pointer-events-none disabled:opacity-40"><Check size={12} /> Approve & attach v{shown.version_number}</button><button type="button" disabled={busy || staleness?.hard_conflict} onClick={() => void decide('approve')} className="rounded-lg border border-emerald-500/40 px-3 py-2 text-xs text-emerald-300 transition-colors hover:border-emerald-400/60 hover:bg-emerald-500/10 disabled:pointer-events-none disabled:opacity-40">Approve v{shown.version_number} only</button><button type="button" disabled={busy} onClick={() => void decide('reject')} className="rounded-lg border border-red-500/40 px-3 py-2 text-xs text-red-300 transition-colors hover:border-red-400/60 hover:bg-red-500/10 disabled:pointer-events-none disabled:opacity-40">Reject v{shown.version_number}</button></>}
        {!plan.read_only && shown && current && plan.display_state === 'awaiting_review' && plan.target_task_id == null && <><button type="button" disabled={busy || staleness?.hard_conflict} onClick={() => void createExecution(true)} className="flex items-center gap-1 rounded-lg bg-indigo-600 px-3 py-2 text-xs font-semibold text-white transition-colors hover:bg-indigo-500 disabled:pointer-events-none disabled:opacity-40"><Play size={12} /> Approve v{shown.version_number} & create execution Task</button><button type="button" disabled={busy || staleness?.hard_conflict} onClick={() => void decide('approve')} className="rounded-lg border border-emerald-500/40 px-3 py-2 text-xs text-emerald-300 transition-colors hover:border-emerald-400/60 hover:bg-emerald-500/10 disabled:pointer-events-none disabled:opacity-40">Approve v{shown.version_number} only</button><button type="button" disabled={busy} onClick={() => void decide('reject')} className="rounded-lg border border-red-500/40 px-3 py-2 text-xs text-red-300 transition-colors hover:border-red-400/60 hover:bg-red-500/10 disabled:pointer-events-none disabled:opacity-40">Reject v{shown.version_number}</button></>}
        {!plan.read_only && shown && current && plan.display_state === 'approved' && plan.target_task_id == null && <button type="button" disabled={busy || staleness?.hard_conflict} onClick={() => void createExecution(false)} className="flex items-center gap-1 rounded-lg bg-indigo-600 px-3 py-2 text-xs font-semibold text-white transition-colors hover:bg-indigo-500 disabled:pointer-events-none disabled:opacity-40"><Play size={12} /> Create execution Task</button>}
        {executionApplications.map((application) => { const applied = versions.find((item) => item.id === application.plan_version_id); return application.execution_task_available === false
          ? <span key={application.id} className="rounded-lg border border-gray-700 bg-gray-800/60 px-3 py-2 text-xs text-gray-400">v{applied?.version_number || '?'} applied · execution Task #{application.execution_task_id} unavailable</span>
          : <button key={application.id} type="button" onClick={() => onNavigateTask?.(application.execution_task_id!)} className="rounded-lg border border-teal-500/40 px-3 py-2 text-xs text-teal-300 transition-colors hover:border-teal-400/60 hover:bg-teal-500/10">Open v{applied?.version_number || '?'} execution Task #{application.execution_task_id}</button>; })}
        {!plan.read_only && shown && current && plan.target_task_id != null && shown.human_decision === 'approved' && !shown.applied && onToggleVersion && <button type="button" disabled={busy} onClick={() => { onToggleVersion(shown.id); onClose?.(); }} className="rounded-lg border border-teal-500/40 px-3 py-2 text-xs text-teal-300 transition-colors hover:border-teal-400/60 hover:bg-teal-500/10 disabled:pointer-events-none disabled:opacity-40">{selectedVersionIds.includes(shown.id) ? 'Detach from next message' : 'Attach to next message'}</button>}
        {!plan.read_only && shown && current && !plan.active_run_id && showStaleness && staleness?.stale && <button type="button" disabled={busy} onClick={() => void mutate('Refreshing Plan context', () => api.createPlanRun(plan.id, { run_type: 'refresh_context', request: 'Refresh all contexts and regenerate this Plan using the latest task conversation and repository state.', base_version_id: shown.id, expected_current_version_id: shown.id }))} className="flex items-center gap-1 rounded-lg border border-gray-700 px-3 py-2 text-xs text-gray-300 transition-colors hover:border-gray-600 hover:bg-gray-800 disabled:pointer-events-none disabled:opacity-40"><RefreshCw size={12} /> Refresh contexts and regenerate Plan</button>}
        {!plan.read_only && activeRun && activeRun.status !== 'cancelling' && <button type="button" disabled={busy} onClick={() => void mutate('Cancelling planning', () => api.cancelPlanRun(activeRun.id))} className="rounded-lg border border-red-500/40 px-3 py-2 text-xs text-red-300 transition-colors hover:border-red-400/60 hover:bg-red-500/10 disabled:pointer-events-none disabled:opacity-40">Cancel planning</button>}
        {!plan.read_only && !plan.active_run_id && <button type="button" disabled={busy} onClick={() => void mutate(plan.archived_at ? 'Restoring Plan' : 'Archiving Plan', () => api.updatePlan(plan.id, { archived: plan.archived_at == null, expected_lock_version: plan.lock_version }))} className="flex items-center gap-1 rounded-lg border border-gray-700 px-3 py-2 text-xs text-gray-400 transition-colors hover:border-gray-600 hover:bg-gray-800 hover:text-gray-200 disabled:pointer-events-none disabled:opacity-40">{plan.archived_at ? <ArchiveRestore size={12} /> : <Archive size={12} />}{plan.archived_at ? 'Restore' : 'Archive'}</button>}
      </div>
    </div>
  </div>;
}
