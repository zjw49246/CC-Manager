import { useCallback, useEffect, useState } from 'react';

import { api } from '../../api/client';
import type { DeliveryRun } from '../../api/client';
import {
  GitPullRequest,
  Loader2,
  Play,
  RefreshCw,
  StopCircle,
  XCircle,
} from '../icons';

type DeliveryAction = 'pause' | 'resume' | 'cancel' | 'retry';

interface DeliveryRunPanelProps {
  runId: number;
  className?: string;
  showStatusDetails?: boolean;
  compact?: boolean;
}

function titleCase(value: string): string {
  return value
    .split('_')
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

function statusLabel(run: DeliveryRun): string {
  if (run.activity === 'terminal') {
    return run.outcome === 'success'
      ? (run.terminal === 'merged' ? 'Merged' : 'Ready to Merge')
      : `Delivery ${titleCase(run.outcome || 'done')}`;
  }
  return `${titleCase(run.phase)} · ${titleCase(run.activity)}`;
}

const actionLabels: Record<DeliveryAction, string> = {
  pause: 'Pause',
  resume: 'Resume',
  cancel: 'Cancel',
  retry: 'Retry failed step',
};

function actionRequiresReason(action: DeliveryAction): boolean {
  return action === 'pause' || action === 'cancel';
}

function isObservationOnly(run: DeliveryRun): boolean {
  return (
    run.activity !== 'terminal'
    && (run.phase === 'publishing'
      || run.phase === 'monitoring'
      || run.pr_number != null
      || run.pr_monitor_run_id != null)
  );
}

export function DeliveryRunPanel({
  runId,
  className = '',
  showStatusDetails = true,
  compact = false,
}: DeliveryRunPanelProps) {
  const [run, setRun] = useState<DeliveryRun | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState('');
  const [pendingAction, setPendingAction] = useState<DeliveryAction | null>(null);
  const [reason, setReason] = useState('');
  const [acting, setActing] = useState(false);

  const load = useCallback(async (background = false) => {
    if (background) setRefreshing(true);
    else setLoading(true);
    try {
      const next = await api.getDeliveryRun(runId);
      setRun(next);
      setError('');
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, [runId]);

  useEffect(() => {
    void load();
    const interval = window.setInterval(() => void load(true), 5000);
    return () => window.clearInterval(interval);
  }, [load]);

  useEffect(() => {
    if (
      pendingAction
      && run
      && (
        isObservationOnly(run)
        || !run.allowed_actions.includes(pendingAction)
      )
    ) {
      setPendingAction(null);
      setReason('');
    }
  }, [pendingAction, run]);

  const allowedActions = run && !isObservationOnly(run)
    ? run.allowed_actions
    : [];

  const chooseAction = (action: DeliveryAction) => {
    setPendingAction(action);
    setReason('');
    setError('');
  };

  const submitAction = async () => {
    if (!run || !pendingAction || acting) return;
    const normalizedReason = reason.trim();
    if (actionRequiresReason(pendingAction) && !normalizedReason) {
      setError(`${actionLabels[pendingAction]} requires a reason.`);
      return;
    }
    setActing(true);
    setError('');
    try {
      const next = pendingAction === 'pause'
        ? await api.pauseDeliveryRun(run.id, normalizedReason)
        : pendingAction === 'resume'
          ? await api.resumeDeliveryRun(run.id, undefined)
          : pendingAction === 'retry'
            ? await api.retryDeliveryRun(
              run.id,
              run.state_version,
              normalizedReason || undefined,
            )
            : await api.cancelDeliveryRun(run.id, normalizedReason);
      setRun(next);
      setPendingAction(null);
      setReason('');
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
      // The controller may have crossed a safe-point boundary after the last
      // poll. Refresh so stale action buttons disappear after a 409.
      void api.getDeliveryRun(run.id).then(setRun).catch(() => {});
    } finally {
      setActing(false);
    }
  };

  if (compact) {
    return (
      <div className={`min-w-0 ${className}`}>
        {run && (
          <div className="flex flex-wrap items-center justify-end gap-2">
            {allowedActions.includes('pause') && <button type="button" onClick={() => chooseAction('pause')} className="inline-flex items-center gap-1 rounded bg-amber-600/20 px-2 py-1 text-xs font-medium text-amber-300 hover:bg-amber-600/30"><StopCircle size={13} /> Pause</button>}
            {allowedActions.includes('resume') && <button type="button" onClick={() => chooseAction('resume')} className="inline-flex items-center gap-1 rounded bg-indigo-600 px-2.5 py-1.5 text-xs font-medium text-white hover:bg-indigo-500"><Play size={13} /> Resume</button>}
            {allowedActions.includes('cancel') && <button type="button" onClick={() => chooseAction('cancel')} className="inline-flex items-center gap-1 rounded px-2 py-1 text-xs font-medium text-red-300 hover:bg-red-500/10"><XCircle size={13} /> Cancel</button>}
            {allowedActions.includes('retry') && <button type="button" onClick={() => chooseAction('retry')} className="inline-flex items-center gap-1 rounded bg-indigo-600 px-2.5 py-1.5 text-xs font-medium text-white hover:bg-indigo-500"><RefreshCw size={13} /> Retry</button>}
          </div>
        )}
        {pendingAction && (
          <div className="mt-2 w-full rounded border border-gray-700 bg-gray-950/70 p-2">
            {pendingAction === 'resume' ? (
              <p className="text-xs text-gray-300">Resume this Delivery?</p>
            ) : (
              <label className="block text-xs text-gray-300">
                {actionLabels[pendingAction]} reason{actionRequiresReason(pendingAction) ? '' : ' (optional)'}
                <textarea aria-label={`${actionLabels[pendingAction]} reason`} value={reason} onChange={(event) => setReason(event.target.value)} maxLength={2000} rows={2} className="mt-1 w-full resize-none rounded border border-gray-700 bg-gray-800 px-2 py-1.5 text-xs text-gray-200 outline-none focus:border-indigo-500" />
              </label>
            )}
            <div className="mt-2 flex justify-end gap-2">
              <button type="button" onClick={() => { setPendingAction(null); setReason(''); setError(''); }} disabled={acting} className="rounded px-2 py-1 text-xs text-gray-400 hover:bg-gray-800">Back</button>
              <button type="button" onClick={() => void submitAction()} disabled={acting || (actionRequiresReason(pendingAction) && !reason.trim())} className={`rounded px-2 py-1 text-xs font-medium text-white disabled:opacity-40 ${pendingAction === 'cancel' ? 'bg-red-600 hover:bg-red-500' : 'bg-indigo-600 hover:bg-indigo-500'}`}>{acting ? 'Applying…' : `Confirm ${actionLabels[pendingAction]}`}</button>
            </div>
          </div>
        )}
        {error && <p role="alert" className="mt-2 text-xs text-red-300">{error}</p>}
      </div>
    );
  }

  return (
    <section
      aria-label={`Delivery Run #${runId}`}
      className={`rounded-lg border border-indigo-500/30 bg-indigo-500/5 p-3 ${className}`}
    >
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <span className="text-sm font-semibold text-indigo-200">Delivery Run #{runId}</span>
            {run && (
              <span className={`rounded px-1.5 py-0.5 text-[11px] font-medium ${
                run.activity === 'terminal'
                  ? run.outcome === 'success'
                    ? 'bg-emerald-500/15 text-emerald-300'
                    : 'bg-red-500/15 text-red-300'
                  : run.activity === 'paused'
                    ? 'bg-amber-500/15 text-amber-300'
                    : 'bg-indigo-500/15 text-indigo-300'
              }`}>
                {statusLabel(run)}
              </span>
            )}
          </div>
          {run && (
            <p className="mt-1 text-xs text-gray-400">
              Round {run.cycle_count} of {run.max_cycles} · {run.turn_count} developer turn{run.turn_count === 1 ? '' : 's'}
            </p>
          )}
        </div>
        <button
          type="button"
          onClick={() => void load(true)}
          disabled={loading || refreshing}
          className="rounded p-1.5 text-gray-500 hover:bg-gray-800 hover:text-indigo-300 disabled:opacity-40"
          title="Refresh Delivery Run"
        >
          <RefreshCw size={14} className={refreshing ? 'animate-spin' : ''} />
        </button>
      </div>

      {loading && !run && (
        <div className="flex items-center gap-2 py-3 text-xs text-gray-500">
          <Loader2 size={13} className="animate-spin" /> Loading Delivery Run…
        </div>
      )}

      {run && (
        <>
          <dl className="mt-2 grid gap-x-4 gap-y-1 text-xs sm:grid-cols-2">
            <div className="min-w-0">
              <dt className="inline text-gray-500">Branch: </dt>
              <dd className="inline break-all text-gray-300">{run.delivery_branch}</dd>
            </div>
            {run.wait_reason && (
              <div className="min-w-0">
                <dt className="inline text-gray-500">Waiting: </dt>
                <dd className="inline break-words text-gray-300">{titleCase(run.wait_reason)}</dd>
              </div>
            )}
            {showStatusDetails && run.pause_reason && (
              <div className="min-w-0 sm:col-span-2">
                <dt className="inline text-gray-500">Paused: </dt>
                <dd className="inline break-words text-amber-300">{run.pause_reason}</dd>
              </div>
            )}
            {showStatusDetails && run.error_message && (
              <div className="min-w-0 sm:col-span-2">
                <dt className="inline text-gray-500">Error{run.error_code ? ` (${run.error_code})` : ''}: </dt>
                <dd className="inline break-words text-red-300">{run.error_message}</dd>
              </div>
            )}
          </dl>

          <div className="mt-3 flex flex-wrap items-center gap-2">
            {run.pr_url && (
              <a
                href={run.pr_url}
                target="_blank"
                rel="noreferrer"
                className="inline-flex items-center gap-1 rounded bg-emerald-600/20 px-2 py-1 text-xs font-medium text-emerald-300 hover:bg-emerald-600/30"
              >
                <GitPullRequest size={13} /> PR #{run.pr_number ?? '?'}
              </a>
            )}
            {allowedActions.includes('pause') && (
              <button
                type="button"
                onClick={() => chooseAction('pause')}
                className="inline-flex items-center gap-1 rounded bg-amber-600/20 px-2 py-1 text-xs font-medium text-amber-300 hover:bg-amber-600/30"
              >
                <StopCircle size={13} /> Pause
              </button>
            )}
            {allowedActions.includes('resume') && (
              <button
                type="button"
                onClick={() => chooseAction('resume')}
                className="inline-flex items-center gap-1 rounded bg-indigo-600/20 px-2 py-1 text-xs font-medium text-indigo-300 hover:bg-indigo-600/30"
              >
                <Play size={13} /> Resume
              </button>
            )}
            {allowedActions.includes('cancel') && (
              <button
                type="button"
                onClick={() => chooseAction('cancel')}
                className="inline-flex items-center gap-1 rounded bg-red-600/20 px-2 py-1 text-xs font-medium text-red-300 hover:bg-red-600/30"
              >
                <XCircle size={13} /> Cancel
              </button>
            )}
            {allowedActions.includes('retry') && (
              <button
                type="button"
                onClick={() => chooseAction('retry')}
                className="inline-flex items-center gap-1 rounded bg-indigo-600/20 px-2 py-1 text-xs font-medium text-indigo-300 hover:bg-indigo-600/30"
              >
                <RefreshCw size={13} /> Retry failed step
              </button>
            )}
            {allowedActions.length === 0
              && (isObservationOnly(run) || run.activity !== 'terminal') && (
              <span className="text-[11px] text-gray-500">
                {isObservationOnly(run)
                  ? 'Publishing and PR monitoring are observation-only; controls are no longer available.'
                  : 'Controls become available at the next safe point.'}
              </span>
            )}
          </div>

          {pendingAction && (
            <div className="mt-3 rounded border border-gray-700 bg-gray-900/60 p-2">
              {pendingAction === 'resume' ? (
                <p className="text-xs text-gray-300">Resume this Delivery?</p>
              ) : (
                <label className="block text-xs text-gray-300">
                  {actionLabels[pendingAction]} reason{actionRequiresReason(pendingAction) ? '' : ' (optional)'}
                  <textarea
                    aria-label={`${actionLabels[pendingAction]} reason`}
                    value={reason}
                    onChange={(event) => setReason(event.target.value)}
                    maxLength={2000}
                    rows={2}
                    className="mt-1 w-full resize-none rounded border border-gray-700 bg-gray-800 px-2 py-1.5 text-xs text-gray-200 outline-none focus:border-indigo-500"
                  />
                </label>
              )}
              <div className="mt-2 flex justify-end gap-2">
                <button
                  type="button"
                  onClick={() => { setPendingAction(null); setReason(''); setError(''); }}
                  disabled={acting}
                  className="rounded px-2 py-1 text-xs text-gray-400 hover:bg-gray-800 hover:text-gray-200 disabled:opacity-40"
                >
                  Back
                </button>
                <button
                  type="button"
                  onClick={() => void submitAction()}
                  disabled={acting || (actionRequiresReason(pendingAction) && !reason.trim())}
                  className={`rounded px-2 py-1 text-xs font-medium text-white disabled:opacity-40 ${
                    pendingAction === 'cancel'
                      ? 'bg-red-600 hover:bg-red-500'
                      : 'bg-indigo-600 hover:bg-indigo-500'
                  }`}
                >
                  {acting ? 'Applying…' : `Confirm ${actionLabels[pendingAction]}`}
                </button>
              </div>
            </div>
          )}
        </>
      )}

      {error && (
        <p role="alert" className="mt-2 rounded bg-red-500/10 px-2 py-1.5 text-xs text-red-300">
          {error}
        </p>
      )}
    </section>
  );
}
