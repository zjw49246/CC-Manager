import type {
  PRFailureStage,
  PRLifecycleState,
  PRPublicationState,
  PRReviewResult,
} from '../../api/client';
import {
  AlertCircle,
  CheckCircle2,
  Clock,
  GitPullRequest,
  MessageSquare,
  Plus,
  RefreshCw,
  XCircle,
} from '../icons';
import { canonicalGitHubPRUrl, canonicalGitHubReviewUrl } from './githubUrls';

const PUBLICATION_LABELS: Record<PRPublicationState, string> = {
  not_started: 'GitHub publication not started',
  publishing: 'Publishing GitHub comment',
  reconciling: 'Reconciling GitHub publication',
  published: 'GitHub comment published',
  failed: 'GitHub publication failed',
  not_applicable: 'GitHub publication not applicable',
};

const LIFECYCLE_LABELS: Record<PRLifecycleState, string> = {
  unknown: 'Historical lifecycle unavailable',
  reviewing: 'PR open',
  superseding: 'Updating to a newer head',
  superseded: 'Review superseded',
  cancelled: 'Review cancelled',
  merged: 'PR merged',
  closed: 'PR closed',
  failed: 'PR lifecycle failed',
};

const FAILURE_STAGE_LABELS: Record<PRFailureStage, string> = {
  reviewer: 'reviewer execution',
  ci: 'CI gate',
  github_identity: 'CCM GitHub identity',
  publication: 'GitHub publication',
  merge: 'merge',
  recovery: 'recovery',
  lifecycle: 'PR lifecycle',
};

function formatTimestamp(value: string | null): string | null {
  if (!value) return null;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}

function codeVerdictLabel(result: PRReviewResult): string {
  if (result.verdict_state === 'pending') return 'Code review pending';
  if (result.error_category === 'unsupported_input_size') return 'Code review not started: input too large';
  if (result.verdict_state === 'unavailable') return 'Code verdict unavailable';
  if (result.aggregate_verdict === 'pass') return 'Code verdict: Pass';
  if (result.aggregate_verdict === 'changes_required') return 'Code verdict: Changes required';
  return 'Code verdict unavailable';
}

function verdictClasses(result: PRReviewResult): string {
  if (result.verdict_state === 'pending') return 'border-blue-500/30 bg-blue-500/10 text-blue-300';
  if (result.error_category === 'unsupported_input_size') return 'border-amber-500/30 bg-amber-500/10 text-amber-300';
  if (result.verdict_state === 'unavailable') return 'border-red-500/30 bg-red-500/10 text-red-300';
  if (result.aggregate_verdict === 'pass') return 'border-emerald-500/30 bg-emerald-500/10 text-emerald-300';
  return 'border-amber-500/30 bg-amber-500/10 text-amber-300';
}

function lifecycleClasses(state: PRLifecycleState): string {
  if (state === 'merged') return 'border-emerald-500/30 bg-emerald-500/10 text-emerald-300';
  if (state === 'unknown' || state === 'closed' || state === 'cancelled' || state === 'superseded') {
    return 'border-gray-600 bg-gray-700/40 text-gray-300';
  }
  if (state === 'failed') return 'border-red-500/30 bg-red-500/10 text-red-300';
  return 'border-blue-500/30 bg-blue-500/10 text-blue-300';
}

function publicationClasses(state: PRPublicationState): string {
  if (state === 'published') return 'border-indigo-500/30 bg-indigo-500/10 text-indigo-200';
  if (state === 'failed') return 'border-red-500/30 bg-red-500/10 text-red-300';
  if (state === 'not_applicable') return 'border-gray-600 bg-gray-700/40 text-gray-300';
  return 'border-blue-500/30 bg-blue-500/10 text-blue-300';
}

export interface PRReviewResultCardProps {
  result: PRReviewResult;
  onOpenDetail?: (result: PRReviewResult, reviewId?: number) => void;
  onCreateFollowUp?: (result: PRReviewResult) => void;
  onRerun?: (result: PRReviewResult) => Promise<void>;
  /** Hide all review mutation/deep-link actions when embedded in a display Task. */
  readOnly?: boolean;
  rerunPending?: boolean;
  rerunError?: string | null;
  rerunSuccess?: { reviewId: number; message: string } | null;
}

/** A deliberately read-only PR result surface; it is not an executable Task. */
export function PRReviewResultCard({
  result,
  onOpenDetail,
  onCreateFollowUp,
  onRerun,
  readOnly = false,
  rerunPending = false,
  rerunError = null,
  rerunSuccess = null,
}: PRReviewResultCardProps) {
  const publishedAt = formatTimestamp(result.published_at);
  const updatedAt = formatTimestamp(result.updated_at);
  const canRerun = result.can_rerun && result.review_id != null && Boolean(result.head_sha) && !rerunSuccess;
  const prUrl = canonicalGitHubPRUrl(result.pr_url, result.repo_full_name, result.pr_number);
  const reviewUrl = canonicalGitHubReviewUrl(
    result.github_review_url,
    result.repo_full_name,
    result.pr_number,
    result.github_review_id,
  );
  const inputTooLarge = result.error_category === 'unsupported_input_size';

  return (
    <article
      aria-label={`PR Review Result ${result.repo_full_name} #${result.pr_number}`}
      className="rounded-xl border border-gray-700/70 bg-gray-800/80 p-4 shadow-sm"
      data-pr-review-result-key={result.result_key}
    >
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex items-center gap-2 text-xs font-medium uppercase tracking-wide text-indigo-300">
            <GitPullRequest size={14} /> PR Review Result
          </div>
          <h3 className="mt-1 truncate text-sm font-semibold text-gray-100">
            {result.repo_full_name} #{result.pr_number} · {result.pr_title}
          </h3>
          {result.head_sha && (
            <code className="mt-1 block text-[11px] text-gray-500" title={result.head_sha}>
              head {result.head_sha.slice(0, 8)}
            </code>
          )}
        </div>
        {updatedAt && (
          <span className="flex items-center gap-1 text-[11px] text-gray-500">
            <Clock size={12} /> {updatedAt}
          </span>
        )}
      </div>

      <div className="mt-3 grid gap-2 text-xs md:grid-cols-3">
        <div className={`rounded border px-2.5 py-2 ${verdictClasses(result)}`}>
          <div className="flex items-center gap-1.5 font-medium">
            {inputTooLarge
              ? <AlertCircle size={14} aria-label="Review input exceeds the safe limit" />
              : result.verdict_state === 'complete' && result.aggregate_verdict === 'pass'
              ? <CheckCircle2 size={14} />
              : result.verdict_state === 'unavailable'
                ? <XCircle size={14} />
                : <Clock size={14} />}
            {codeVerdictLabel(result)}
          </div>
        </div>
        <div className={`rounded border px-2.5 py-2 ${publicationClasses(result.publication_state)}`}>
          <div className="flex items-center gap-1.5 font-medium">
            <MessageSquare size={14} /> {PUBLICATION_LABELS[result.publication_state]}
          </div>
          {result.publication_state === 'published' && (
            <p className="mt-1 text-[11px] opacity-80">
              {result.published_actor ? `Published by CCM as ${result.published_actor}` : 'Published by CCM'}
              {publishedAt ? ` · ${publishedAt}` : ''}
              {result.github_state ? ` · GitHub ${result.github_state}` : ''}
            </p>
          )}
        </div>
        <div className={`rounded border px-2.5 py-2 ${lifecycleClasses(result.lifecycle_state)}`}>
          <div className="font-medium">{LIFECYCLE_LABELS[result.lifecycle_state]}</div>
        </div>
      </div>

      {result.display_summary && (
        <p className="mt-3 line-clamp-3 text-xs leading-5 text-gray-300">{result.display_summary}</p>
      )}
      {result.error_category === 'unsupported_input_size'
        && result.error_measured != null
        && result.error_limit != null
        && result.error_unit && (
          <p className="mt-2 text-[11px] text-amber-300">
            Review input: {result.error_measured.toLocaleString()} {result.error_unit}; safe limit:{' '}
            {result.error_limit.toLocaleString()} {result.error_unit}. No Reviewer Task was created.
          </p>
        )}
      {result.failure_stage && (
        <p className={`mt-2 text-[11px] ${
          result.error_category === 'unsupported_input_size' ? 'text-amber-300' : 'text-red-300'
        }`}>
          Failure stage: {result.error_category === 'unsupported_input_size'
            ? 'review input admission'
            : FAILURE_STAGE_LABELS[result.failure_stage]}
        </p>
      )}
      {rerunError && <p className="mt-2 text-xs text-red-300" role="alert">{rerunError}</p>}
      {!readOnly && rerunSuccess && onOpenDetail && (
        <p className="mt-2 text-xs text-emerald-300" role="status">
          {rerunSuccess.message}{' '}
          <button
            type="button"
            onClick={() => onOpenDetail(result, rerunSuccess.reviewId)}
            className="font-medium underline underline-offset-2 hover:text-emerald-200"
          >
            Open started review
          </button>
        </p>
      )}

      {!readOnly && <div className="mt-3 flex flex-wrap gap-2">
        {onOpenDetail && <button
          type="button"
          onClick={() => rerunSuccess
            ? onOpenDetail(result, rerunSuccess.reviewId)
            : onOpenDetail(result)}
          className="rounded bg-indigo-600 px-2.5 py-1.5 text-xs font-medium text-white hover:bg-indigo-500"
        >
          Open review details
        </button>}
        {prUrl && (
          <a
            href={prUrl}
            target="_blank"
            rel="noopener noreferrer"
            className="rounded bg-gray-700 px-2.5 py-1.5 text-xs text-gray-200 hover:bg-gray-600"
          >
            Open GitHub PR
          </a>
        )}
        {reviewUrl && (
          <a
            href={reviewUrl}
            target="_blank"
            rel="noopener noreferrer"
            className="rounded bg-gray-700 px-2.5 py-1.5 text-xs text-gray-200 hover:bg-gray-600"
          >
            Open published comment
          </a>
        )}
        {onCreateFollowUp && <button
          type="button"
          onClick={() => onCreateFollowUp(result)}
          className="inline-flex items-center gap-1 rounded bg-gray-700 px-2.5 py-1.5 text-xs text-gray-200 hover:bg-gray-600"
        >
          <Plus size={12} /> Create follow-up Task
        </button>}
        {canRerun && onRerun && (
          <button
            type="button"
            disabled={rerunPending}
            onClick={() => void onRerun(result)}
            className="inline-flex items-center gap-1 rounded border border-indigo-500/40 px-2.5 py-1.5 text-xs text-indigo-200 hover:bg-indigo-500/10 disabled:cursor-wait disabled:opacity-50"
          >
            <RefreshCw size={12} className={rerunPending ? 'animate-spin' : ''} />
            {rerunPending ? 'Starting exact-head review…' : 'Re-run exact head'}
          </button>
        )}
      </div>}
    </article>
  );
}
