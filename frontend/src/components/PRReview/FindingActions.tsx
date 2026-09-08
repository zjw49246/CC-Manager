import { useEffect, useState } from 'react';
import { api } from '../../api/client';
import { useVisibilityAwareInterval } from '../../hooks/useVisibilityAwareInterval';
import type { PRFinding, PRFindingAction } from '../../api/client';

function actionKey(prefix: string, findingId: number): string {
  const nonce = globalThis.crypto?.randomUUID?.() || `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  return `${prefix}-${findingId}-${nonce}`.slice(0, 64);
}

export function FindingActions({ finding, currentSnapshot, onChanged }: {
  finding: PRFinding;
  currentSnapshot: boolean;
  onChanged: () => Promise<void>;
}) {
  const [action, setAction] = useState<PRFindingAction | null>(finding.latest_action);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [reviewedDiff, setReviewedDiff] = useState<{
    actionId: number;
    actionStatus: PRFindingAction['status'];
    expectedHeadSha: string;
    patchSha256: string;
    receipt: string;
    confirmationToken: string;
  } | null>(null);

  useEffect(() => setAction(finding.latest_action), [finding.latest_action]);

  useEffect(() => {
    setReviewedDiff(previous => {
      if (!previous) return null;
      if (
        !action
        || previous.actionId !== action.id
        || previous.actionStatus !== action.status
        || previous.expectedHeadSha !== action.expected_head_sha
        || previous.patchSha256 !== action.patch_sha256
      ) {
        return null;
      }
      return previous;
    });
  }, [action]);

  const actionActive = Boolean(action && ['pending', 'running', 'cancelling'].includes(action.status));
  useVisibilityAwareInterval(async () => {
      try {
        if (!action) return;
        const next = await api.getReviewFindingAction(action.id);
        setAction(next);
        if (!['pending', 'running', 'cancelling'].includes(next.status)) await onChanged();
      } catch (reason) {
        setError(String(reason));
      }
  }, 2000, actionActive, false);

  const run = async (operation: () => Promise<PRFindingAction>) => {
    setBusy(true);
    setError(null);
    try {
      const next = await operation();
      setAction(next);
      await onChanged();
    } catch (reason) {
      setError(String(reason));
    } finally {
      setBusy(false);
    }
  };

  const downloadDiff = async () => {
    if (!action?.patch_sha256) return;
    const identity = {
      actionId: action.id,
      actionStatus: action.status,
      expectedHeadSha: action.expected_head_sha,
      patchSha256: action.patch_sha256,
    };
    setBusy(true);
    setError(null);
    try {
      const file = await api.downloadReviewFindingDiff(action.id);
      const url = URL.createObjectURL(file.blob);
      const anchor = document.createElement('a');
      anchor.href = url;
      anchor.download = file.filename;
      anchor.click();
      URL.revokeObjectURL(url);
      setReviewedDiff({
        ...identity,
        receipt: file.receipt,
        confirmationToken: file.confirmationToken,
      });
    } catch (reason) {
      setError(String(reason));
    } finally {
      setBusy(false);
    }
  };

  const diffIsCurrent = Boolean(
    action
    && reviewedDiff?.actionId === action.id
    && reviewedDiff.actionStatus === action.status
    && reviewedDiff.expectedHeadSha === action.expected_head_sha
    && reviewedDiff.patchSha256 === action.patch_sha256,
  );
  const activeFix = Boolean(
    action?.action_type === 'ai_fix'
    && ['pending', 'running', 'awaiting_confirmation', 'cancelling'].includes(action.status),
  );
  const result = action?.result;
  const targetRepo = typeof result?.head_repo_full_name === 'string' ? result.head_repo_full_name : null;
  const targetRef = typeof result?.head_ref === 'string' ? result.head_ref : null;
  const prNumber = typeof result?.pr_number === 'number' ? result.pr_number : null;
  const allowedFiles = Array.isArray(result?.allowed_files)
    ? result.allowed_files.filter((value): value is string => typeof value === 'string')
    : [];
  const targetIdentityComplete = Boolean(targetRepo && targetRef && prNumber && allowedFiles.length);
  const canStart = currentSnapshot && finding.status === 'open' && !activeFix;
  const canConfirm = Boolean(
    currentSnapshot
    && action?.status === 'awaiting_confirmation'
    && action.patch_sha256
    && targetIdentityComplete,
  );
  const canCancel = Boolean(
    action?.action_type === 'ai_fix'
    && (
      action.status === 'pending'
      || (action.status === 'running' && action.confirmed_at === null)
      || action.status === 'awaiting_confirmation'
    ),
  );

  return (
    <div className="mt-3 border-t border-gray-700/60 pt-3">
      {action && <p className="mb-2 text-gray-500">Action: {action.action_type} · {action.status}</p>}
      {error && <p role="alert" className="mb-2 text-red-400">{error}</p>}
      {!currentSnapshot && <p className="mb-2 text-gray-500">Historical snapshot — new actions are locked.</p>}
      {activeFix && <p className="mb-2 text-gray-500">Finish the active AI fix before recording another action.</p>}
      {canConfirm && action && (
        <div className="mb-2 rounded border border-amber-700/50 bg-amber-950/20 p-2 text-xs text-gray-300">
          <div>Target: {targetRepo}#{prNumber} · {targetRef}</div>
          <div>Head: {action.expected_head_sha}</div>
          <div>Files: {allowedFiles.join(', ')}</div>
          <div>Patch SHA-256: {action.patch_sha256}</div>
        </div>
      )}
      <div className="flex flex-wrap gap-2">
        {canStart && (
          <>
            <button disabled={busy} className="rounded bg-gray-700 px-2 py-1 disabled:opacity-50" onClick={() => {
              if (window.confirm('Ignore this finding in CCM? The Panel gate remains authoritative.')) {
                void run(() => api.ignoreReviewFinding(finding.id, actionKey('ignore', finding.id)));
              }
            }}>Ignore</button>
            <button disabled={busy} className="rounded bg-gray-700 px-2 py-1 disabled:opacity-50" onClick={() => {
              const advice = window.prompt('Human repair advice');
              if (advice?.trim()) {
                void run(() => api.saveReviewFindingAdvice(finding.id, advice.trim(), actionKey('advice', finding.id)));
              }
            }}>Human advice</button>
            <button disabled={busy} className="rounded bg-indigo-600 px-2 py-1 text-white disabled:opacity-50" onClick={() => {
              void run(() => api.createReviewFindingFix(finding.id, actionKey('fix', finding.id)));
            }}>Generate AI fix</button>
          </>
        )}
        {canCancel && action && (
          <button disabled={busy} className="rounded bg-red-700 px-2 py-1 text-white disabled:opacity-50" onClick={() => {
            if (window.confirm('Cancel this active AI fix? Any unconfirmed generated patch will be discarded.')) {
              setReviewedDiff(null);
              void run(() => api.cancelPRFindingAction(action.id));
            }
          }}>Cancel AI fix</button>
        )}
        {canConfirm && (
          <>
            <button disabled={busy} className="rounded bg-gray-700 px-2 py-1 disabled:opacity-50" onClick={() => void downloadDiff()}>Download diff</button>
            <button disabled={busy || !diffIsCurrent} className="rounded bg-amber-600 px-2 py-1 text-white disabled:opacity-50" onClick={() => {
              if (!action || !reviewedDiff || !diffIsCurrent) return;
              const confirmation = [
                'Create a commit and exact-head conditional push this reviewed diff?',
                `PR: ${targetRepo}#${prNumber}`,
                `Source ref: ${targetRef}`,
                `Expected head: ${reviewedDiff.expectedHeadSha}`,
                `Files: ${allowedFiles.join(', ')}`,
                `Patch SHA-256: ${reviewedDiff.patchSha256}`,
              ].join('\n');
              if (!window.confirm(confirmation)) return;
              void run(() => api.confirmReviewFindingFix(
                action.id,
                reviewedDiff.confirmationToken,
                reviewedDiff.patchSha256,
                reviewedDiff.receipt,
              ));
            }}>Confirm and push</button>
          </>
        )}
      </div>
    </div>
  );
}
