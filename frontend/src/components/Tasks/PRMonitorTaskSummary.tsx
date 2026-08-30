import type { PRReviewResult, Task } from '../../api/client';
import { AlertCircle, CheckCircle2, Clock, GitPullRequest, XCircle } from '../icons';

interface PRMonitorTaskSummaryProps {
  task: Pick<Task, 'title' | 'description'>;
  result?: PRReviewResult | null;
  compact?: boolean;
}

function verdictLabel(result: PRReviewResult): string {
  if (result.error_category === 'unsupported_input_size') return 'Input too large';
  if (result.verdict_state === 'pending') return 'Review pending';
  if (result.verdict_state === 'unavailable') return 'Verdict unavailable';
  if (result.aggregate_verdict === 'pass') return 'Pass';
  if (result.aggregate_verdict === 'changes_required') return 'Changes required';
  return 'Verdict unavailable';
}

function VerdictIcon({ result }: { result: PRReviewResult }) {
  if (result.error_category === 'unsupported_input_size') {
    return <AlertCircle size={13} aria-hidden="true" />;
  }
  if (result.verdict_state === 'pending') return <Clock size={13} aria-hidden="true" />;
  if (result.aggregate_verdict === 'pass') return <CheckCircle2 size={13} aria-hidden="true" />;
  return <XCircle size={13} aria-hidden="true" />;
}

/** Small, non-interactive projection shown inside the ordinary Task row. */
export function PRMonitorTaskSummary({ task, result, compact = false }: PRMonitorTaskSummaryProps) {
  if (!result) {
    return (
      <div className="mt-1 flex items-center gap-1.5 text-xs text-gray-500">
        <GitPullRequest size={13} aria-hidden="true" />
        <span>{task.title || task.description || 'PR review result pending'}</span>
      </div>
    );
  }

  const verdict = verdictLabel(result);
  const verdictColor = result.aggregate_verdict === 'pass'
    ? 'text-emerald-300'
    : result.error_category === 'unsupported_input_size'
      ? 'text-amber-300'
      : result.verdict_state === 'pending'
        ? 'text-blue-300'
        : 'text-amber-300';

  return (
    <div className={`mt-1 min-w-0 ${compact ? 'space-y-0.5' : 'space-y-1'}`}>
      <div className="flex min-w-0 items-center gap-1.5 text-xs text-gray-300">
        <GitPullRequest size={13} className="shrink-0 text-indigo-300" aria-hidden="true" />
        <span className="truncate font-medium">
          {result.repo_full_name} #{result.pr_number} · {result.pr_title}
        </span>
      </div>
      <div className="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-0.5 text-[11px]">
        <span className={`inline-flex items-center gap-1 font-medium ${verdictColor}`}>
          <VerdictIcon result={result} />
          {verdict}
        </span>
        <span className="truncate text-gray-500">{result.display_status}</span>
      </div>
      {!compact && result.display_summary && (
        <p className="line-clamp-2 text-xs leading-5 text-gray-400">{result.display_summary}</p>
      )}
    </div>
  );
}
