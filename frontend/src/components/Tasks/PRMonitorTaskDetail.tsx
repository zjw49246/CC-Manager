import { useEffect, useState } from 'react';
import { api } from '../../api/client';
import type { PRMonitorReviewAttempt, PRMonitorRun, PRReview, PRReviewResult, Task } from '../../api/client';
import { ArrowLeft, GitBranch, GitPullRequest } from '../icons';
import { MarkdownContent } from '../MarkdownContent';
import { canonicalGitHubPRUrl, canonicalGitHubReviewUrl } from '../PRReview/githubUrls';
import { PRReviewResultCard } from '../PRReview/PRReviewResultCard';

const BASE_ANCESTRY_ERROR = 'GitHub PR base ancestry is unsafe';

interface PRMonitorTaskDetailProps {
  task: Pick<Task, 'title' | 'description' | 'metadata_'>;
  result?: PRReviewResult | null;
  onBack: () => void;
}

/** Read-only Task view for the stable PR Monitor display projection. */
export function PRMonitorTaskDetail({ task, result, onBack }: PRMonitorTaskDetailProps) {
  const rawReviewId = result?.review_id ?? task.metadata_?.pr_monitor_review_id;
  const reviewId = typeof rawReviewId === 'number' ? rawReviewId : null;
  const rawRunId = result?.run_id ?? task.metadata_?.pr_monitor_run_id;
  const runId = typeof rawRunId === 'number' ? rawRunId : null;
  const refreshKey = result?.updated_at ?? '';
  const detailLoadKey = reviewId == null ? null : `${reviewId}:${refreshKey}`;
  const historyLoadKey = runId == null ? null : `${runId}:${refreshKey}`;
  const [detailState, setDetailState] = useState<{
    loadKey: string;
    value: PRReview | null;
    error: string | null;
  } | null>(null);
  const [historyState, setHistoryState] = useState<{
    loadKey: string;
    value: PRMonitorRun | null;
    error: boolean;
  } | null>(null);
  const [mergePending, setMergePending] = useState(false);
  const [mergeError, setMergeError] = useState<string | null>(null);

  useEffect(() => {
    let active = true;
    if (reviewId == null || detailLoadKey == null) return undefined;
    void api.getReviewDetail(reviewId)
      .then((detail) => {
        if (active) setDetailState({ loadKey: detailLoadKey, value: detail, error: null });
      })
      .catch((error: unknown) => {
        if (active) setDetailState({ loadKey: detailLoadKey, value: null, error: String(error) });
      });
    return () => { active = false; };
  }, [detailLoadKey, reviewId]);

  useEffect(() => {
    let active = true;
    if (runId == null || historyLoadKey == null) return undefined;
    void api.getPRMonitorRun(runId)
      .then((run) => {
        if (active) setHistoryState({
          loadKey: historyLoadKey,
          value: run,
          error: false,
        });
      })
      .catch(() => {
        if (active) setHistoryState({ loadKey: historyLoadKey, value: null, error: true });
      });
    return () => { active = false; };
  }, [historyLoadKey, runId]);

  const reviewDetail = detailState?.loadKey === detailLoadKey ? detailState.value : null;
  const detailLoading = detailLoadKey != null && detailState?.loadKey !== detailLoadKey;
  const detailError = detailState?.loadKey === detailLoadKey ? detailState.error : null;
  const monitorRun = historyState?.loadKey === historyLoadKey ? historyState.value : null;
  const reviewHistory = monitorRun?.review_history ?? [];
  const historyLoading = historyLoadKey != null && historyState?.loadKey !== historyLoadKey;
  const historyError = historyState?.loadKey === historyLoadKey && historyState.error;
  const reviewerRuns = reviewDetail?.reviewer_runs ?? [];
  const currentReviewId = monitorRun?.current_review_id ?? result?.review_id ?? null;
  const resultMatchesCurrentHead = Boolean(
    result
    && result.review_id === currentReviewId
    && result.head_sha === monitorRun?.current_head_sha,
  );
  const baseUpdateRequired = Boolean(
    monitorRun?.pause_reason === 'direct_merge_base_update_required'
    || monitorRun?.merge_actions?.some((action) => action.last_error?.includes(BASE_ANCESTRY_ERROR)),
  );
  const prUrl = result
    ? canonicalGitHubPRUrl(result.pr_url, result.repo_full_name, result.pr_number)
    : null;
  const canMerge = Boolean(
    result?.aggregate_verdict === 'pass'
    && resultMatchesCurrentHead
    && monitorRun?.status === 'ready_to_merge'
    && !baseUpdateRequired
    && !mergePending,
  );

  const mergePR = async () => {
    if (runId == null || !canMerge) return;
    setMergePending(true);
    setMergeError(null);
    try {
      const updatedRun = await api.mergePRMonitorRun(runId);
      setHistoryState({
        loadKey: historyLoadKey ?? `${runId}:${refreshKey}`,
        value: updatedRun,
        error: false,
      });
    } catch (error: unknown) {
      setMergeError(String(error));
    } finally {
      setMergePending(false);
    }
  };

  return (
    <div className="flex h-full min-h-0 flex-col bg-gray-950/20">
      <div className="flex shrink-0 items-center gap-3 border-b border-gray-800 px-4 py-3">
        <button
          type="button"
          onClick={onBack}
          className="rounded p-1.5 text-gray-400 transition-colors hover:bg-gray-800 hover:text-gray-200"
          title="Back to tasks"
          aria-label="Back to tasks"
        >
          <ArrowLeft size={16} />
        </button>
        <div className="min-w-0">
          <div className="flex items-center gap-1.5 text-xs font-medium uppercase tracking-wide text-indigo-300">
            <GitPullRequest size={14} aria-hidden="true" /> PR Monitor result
          </div>
          <h2 className="truncate text-sm font-semibold text-gray-100">
            {task.title || 'Pull request review'}
          </h2>
        </div>
      </div>
      <div className="min-h-0 flex-1 overflow-y-auto p-4">
        <div className="space-y-4">
          {result && <PRReviewResultCard result={result} readOnly />}
          {(historyLoading || historyError || reviewHistory.length > 0) && (
            <ReviewHistory
              reviews={reviewHistory}
              currentReviewId={currentReviewId}
              repoFullName={result?.repo_full_name ?? null}
              prNumber={result?.pr_number ?? null}
              loading={historyLoading}
              error={historyError}
            />
          )}
          {monitorRun && result?.aggregate_verdict === 'pass' && resultMatchesCurrentHead && (
            <section aria-label="Merge controls" className="border-y border-gray-800 py-3">
              <div className="flex flex-wrap items-center justify-between gap-3">
                <h3 className="text-sm font-medium text-gray-200">Merge exact reviewed head</h3>
                {baseUpdateRequired ? (
                  <span className="text-xs font-medium text-amber-300">Branch update required</span>
                ) : monitorRun.status === 'ready_to_merge' ? (
                  <button
                    type="button"
                    onClick={() => void mergePR()}
                    disabled={mergePending}
                    className="inline-flex items-center gap-1.5 rounded border border-emerald-500/50 px-3 py-1.5 text-xs text-emerald-200 hover:bg-emerald-500/10 disabled:opacity-50"
                  >
                    <GitPullRequest size={14} aria-hidden="true" />
                    {mergePending ? 'Merging…' : 'Merge PR'}
                  </button>
                ) : (
                  <span className="text-xs text-gray-400">
                    {mergeStatusLabel(monitorRun.status)}
                  </span>
                )}
              </div>
              {baseUpdateRequired && (
                <p role="alert" className="mt-2 text-xs text-amber-300">
                  The base branch advanced after this review. Update the PR branch and wait for CCM to review the new head before merging.
                  {prUrl && (
                    <>
                      {' '}
                      <a
                        href={prUrl}
                        target="_blank"
                        rel="noreferrer"
                        className="font-medium text-indigo-300 hover:text-indigo-200"
                      >
                        Open PR on GitHub
                      </a>
                    </>
                  )}
                </p>
              )}
              {mergeError && <p role="alert" className="mt-2 text-xs text-amber-300">Merge failed: {mergeError}</p>}
              {monitorRun.merge_actions?.map((action) => action.last_error && !action.last_error.includes(BASE_ANCESTRY_ERROR) && (
                <p key={`merge-error-${action.id}`} role="alert" className="mt-2 text-xs text-amber-300">
                  Merge action #{action.id}: {action.last_error}
                </p>
              ))}
            </section>
          )}
          {detailLoading && <p className="text-xs text-gray-500">Loading reviewer details…</p>}
          {detailError && (
            <p className="text-xs text-amber-300" role="alert">
              Reviewer details could not be loaded. The aggregate result will continue to refresh.
            </p>
          )}
          {!result && !reviewDetail && !detailLoading && !detailError && (
            <div className="rounded border border-gray-700 bg-gray-900/40 p-4 text-sm text-gray-400">
              The PR review result is not available yet. Refresh this Task to check again.
            </div>
          )}
          {reviewDetail && (
            <section aria-label="Reviewer details" className="space-y-3">
              {(reviewDetail.review_summary || reviewDetail.display_summary) && (
                <div className="rounded border border-gray-700 bg-gray-900/40 p-3">
                  <h3 className="mb-2 text-sm font-medium text-gray-200">Review summary</h3>
                  <MarkdownContent
                    content={reviewDetail.display_summary || reviewDetail.review_summary || ''}
                    className="text-xs text-gray-300"
                  />
                </div>
              )}
              {reviewerRuns.length > 0 && (
                <div className="space-y-2">
                  <h3 className="text-sm font-medium text-gray-200">Reviewer results</h3>
                  {reviewerRuns.map((run) => (
                    <ReviewerResult key={run.id} run={run} />
                  ))}
                </div>
              )}
            </section>
          )}
        </div>
      </div>
    </div>
  );
}

function mergeStatusLabel(status: string): string {
  if (status === 'merge_pending') return 'Merging';
  if (status === 'merged') return 'Merged';
  return status.replaceAll('_', ' ').replace(/\b\w/g, (value) => value.toUpperCase());
}

function reviewVerdictLabel(review: PRMonitorReviewAttempt): string {
  if (review.aggregate_verdict === 'pass') return 'Pass';
  if (review.aggregate_verdict === 'changes_required') return 'Changes required';
  return review.status.replaceAll('_', ' ').replace(/\b\w/g, (value) => value.toUpperCase());
}

function reviewTimestamp(review: PRMonitorReviewAttempt): string {
  const value = review.completed_at ?? review.created_at;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}

function ReviewHistory({
  reviews,
  currentReviewId,
  repoFullName,
  prNumber,
  loading,
  error,
}: {
  reviews: PRMonitorReviewAttempt[];
  currentReviewId: number | null;
  repoFullName: string | null;
  prNumber: number | null;
  loading: boolean;
  error: boolean;
}) {
  const commitCount = new Set(reviews.map((review) => review.head_sha).filter(Boolean)).size;
  return (
    <section aria-label="Review history" className="border-y border-gray-800 py-3">
      <div className="mb-2 flex flex-wrap items-center justify-between gap-2">
        <h3 className="flex items-center gap-1.5 text-sm font-medium text-gray-200">
          <GitBranch size={14} aria-hidden="true" /> Review history
        </h3>
        {reviews.length > 0 && (
          <span className="text-xs text-gray-400">
            {reviews.length} review {reviews.length === 1 ? 'attempt' : 'attempts'} · {commitCount} {commitCount === 1 ? 'commit' : 'commits'}
          </span>
        )}
      </div>
      {loading && <p className="text-xs text-gray-500">Loading review history…</p>}
      {error && <p className="text-xs text-amber-300">Review history could not be loaded.</p>}
      {!loading && !error && (
        <ol className="divide-y divide-gray-800 border-y border-gray-800">
          {reviews.map((review, index) => {
            const isCurrent = review.id === currentReviewId;
            const reviewUrl = repoFullName && prNumber != null
              ? canonicalGitHubReviewUrl(
                  review.github_review_url,
                  repoFullName,
                  prNumber,
                  review.github_review_id,
                )
              : null;
            return (
              <li key={review.id} className="flex flex-wrap items-center justify-between gap-2 py-2 text-xs">
                <div className="min-w-0">
                  <div className="flex flex-wrap items-center gap-2">
                    <span className="font-medium text-gray-200">Review {index + 1}</span>
                    {isCurrent && <span className="text-emerald-300">Current</span>}
                    <span className={review.aggregate_verdict === 'pass' ? 'text-emerald-300' : review.aggregate_verdict === 'changes_required' ? 'text-amber-300' : 'text-gray-400'}>
                      {reviewVerdictLabel(review)}
                    </span>
                  </div>
                  <div className="mt-1 flex flex-wrap gap-x-2 text-gray-500">
                    <code title={review.head_sha ?? undefined}>head {review.head_sha?.slice(0, 8) ?? 'unknown'}</code>
                    <span>{reviewTimestamp(review)}</span>
                  </div>
                </div>
                {reviewUrl && (
                  <a className="text-indigo-300 hover:text-indigo-200" href={reviewUrl} target="_blank" rel="noopener noreferrer">
                    Open GitHub review
                  </a>
                )}
              </li>
            );
          })}
        </ol>
      )}
    </section>
  );
}

function roleLabel(role: string): string {
  return role.split('_').map((part) => part.charAt(0).toUpperCase() + part.slice(1)).join(' ');
}

function ReviewerResult({ run }: { run: NonNullable<PRReview['reviewer_runs']>[number] }) {
  const severityOrder: Record<string, number> = { critical: 0, high: 1, medium: 2, low: 3 };
  const findings = [...(run.findings || [])].sort(
    (a, b) => (severityOrder[a.severity] ?? 9) - (severityOrder[b.severity] ?? 9),
  );
  return (
    <article className="rounded border border-gray-700 bg-gray-900/40 p-3">
      <div className="flex flex-wrap items-center justify-between gap-2 text-sm">
        <h4 className="font-medium text-gray-200">{roleLabel(run.role)}</h4>
        <span className="text-xs text-gray-400">
          {run.verdict ? `${run.status} · ${run.verdict}` : run.status}
        </span>
      </div>
      {run.result_body && <MarkdownContent content={run.result_body} className="mt-2 text-xs text-gray-300" />}
      {run.outcome_kind === 'infrastructure_error' && !run.result_body && (
        <p className="mt-2 text-xs text-red-300">This reviewer did not produce a code verdict.</p>
      )}
      {run.error_message && <p className="mt-2 text-xs text-red-300">{run.error_message}</p>}
      {findings.map((finding) => (
        <div key={finding.id} className="mt-3 space-y-1 border-l-2 border-orange-500 pl-3 text-xs">
          <p className="text-orange-300">
            [{finding.severity}] {finding.path}{finding.line ? `:${finding.line}` : ''} · {finding.title}
          </p>
          <p className="text-gray-300">Evidence: {finding.evidence}</p>
          <p className="text-gray-400">Impact: {finding.impact}</p>
          <p className="text-gray-400">Required fix: {finding.required_fix}</p>
          <p className="text-gray-400">Test: {finding.test}</p>
          <p className="text-gray-500">Thread: {finding.thread_status}</p>
        </div>
      ))}
    </article>
  );
}
