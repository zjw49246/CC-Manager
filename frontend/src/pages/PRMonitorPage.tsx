import { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import { api } from '../api/client';
import type { GitHubPublisherIdentity, MonitoredRepo, PRFinding, PRMonitorRun, PRReview, RequiredCheckPolicy } from '../api/client';
import { Plus, ArrowLeft, X, Copy, RefreshCw, ToggleLeft, ToggleRight, Trash2, GitPullRequest, Check } from '../components/icons';
import { FindingActions } from '../components/PRReview/FindingActions';
import { MarkdownContent } from '../components/MarkdownContent';
import { useDialogA11y } from '../hooks/useDialogA11y';
import { useWebSocket } from '../hooks/useWebSocket';
import { useVisibilityAwareInterval } from '../hooks/useVisibilityAwareInterval';
import {
  canonicalGitHubFindingCommentUrl,
  canonicalGitHubPRUrl,
  canonicalGitHubReviewUrl,
} from '../components/PRReview/githubUrls';

const DEFAULT_WEBHOOK_URL = `${window.location.origin}/api/github/webhook`;
const FINDING_SEVERITY_ORDER: Record<string, number> = { critical: 0, high: 1, medium: 2, low: 3 };

function currentUserIsAdmin(): boolean {
  const stored = localStorage.getItem('cc_user');
  if (stored === null) return true;
  try {
    const user = JSON.parse(stored) as { id?: unknown; role?: unknown };
    return user.role === 'admin' || user.role === 'super_admin' || !user.id;
  } catch {
    return false;
  }
}

function parseRequiredChecks(value: string): RequiredCheckPolicy[] {
  return value.split('\n').map((line) => line.trim()).filter(Boolean).map((line) => {
    const [kind, name, appSlug, ...extra] = line.split(',').map((part) => part.trim());
    if (extra.length || !name || !appSlug || (kind !== 'check_run' && kind !== 'status')) {
      throw new Error('Required CI 每行格式必须是：check_run,检查名,GitHub App slug（或 status,context,机器人登录名）');
    }
    return { kind: kind as RequiredCheckPolicy['kind'], name, app_slug: appSlug };
  });
}

function renderRequiredChecks(repo: MonitoredRepo) {
  return (repo.required_checks || []).map((item) => `${item.kind},${item.name},${item.app_slug}`).join('\n');
}

function mergePolicyLabel(repo: MonitoredRepo): string {
  return repo.auto_merge ? 'AUTO' : 'MANUAL';
}

function mergePolicyHelp(autoMerge: boolean): string {
  if (autoMerge) {
    return 'Direct auto-merge is ON: CCM confirms the exact-head merge, then comments that the PR was merged.';
  }
  return 'Direct auto-merge is OFF: CCM leaves the PR open until a human clicks Merge PR.';
}

function availableProvider(
  requested: string | null | undefined,
  providers: readonly string[],
): string {
  if (requested && providers.includes(requested)) return requested;
  return providers[0] || '';
}

function useProviderModels(): {
  providers: string[];
  defaultProvider: string;
  providerConfigLoaded: boolean;
  modelsFor: (p: string) => string[];
  effortsFor: (p: string, model: string) => string[];
} {
  const [cfg, setCfg] = useState<{
    providers: string[];
    defaultProvider: string;
    providerConfigLoaded: boolean;
    defaultClaudeModel: string;
    defaultCodexModel: string;
    claude: string[];
    codex: string[];
    claudeEfforts: Record<string, string[]>;
    codexEfforts: Record<string, string[]>;
    defaultEfforts: string[];
    codexDefaultEfforts: string[];
  }>({
    providers: [], defaultProvider: '', providerConfigLoaded: false,
    defaultClaudeModel: '', defaultCodexModel: '',
    claude: [], codex: [], claudeEfforts: {}, codexEfforts: {},
    defaultEfforts: [], codexDefaultEfforts: [],
  });
  useEffect(() => {
    api.config().then((c) => {
      const configuredProviders = c.provider_options?.length
        ? c.provider_options
        : ['claude', 'codex'];
      const providers = Array.from(new Set(configuredProviders.filter(
        (provider) => provider === 'claude' || provider === 'codex',
      )));
      setCfg({
        providers,
        defaultProvider: availableProvider(c.default_provider, providers),
        providerConfigLoaded: true,
        defaultClaudeModel: c.default_model,
        defaultCodexModel: c.default_codex_model,
        claude: c.model_options.filter((m) => m !== 'default'),
        codex: (c.codex_model_options || []).filter((m) => m !== 'default'),
        claudeEfforts: c.claude_model_efforts || {},
        codexEfforts: c.codex_model_efforts || {},
        defaultEfforts: c.effort_options || [],
        codexDefaultEfforts: c.codex_effort_options || [],
      });
    }).catch(() => setCfg((current) => ({
      ...current,
      providers: [],
      defaultProvider: '',
      providerConfigLoaded: true,
    })));
  }, []);
  return {
    providers: cfg.providers,
    defaultProvider: cfg.defaultProvider,
    providerConfigLoaded: cfg.providerConfigLoaded,
    modelsFor: (p: string) => (p === 'codex' ? cfg.codex : cfg.claude),
    effortsFor: (p: string, model: string) => {
      const effectiveModel = model || (p === 'codex' ? cfg.defaultCodexModel : cfg.defaultClaudeModel);
      const mapped = (p === 'codex' ? cfg.codexEfforts : cfg.claudeEfforts)[effectiveModel];
      return mapped?.length
        ? mapped
        : (p === 'codex' ? cfg.codexDefaultEfforts : cfg.defaultEfforts);
    },
  };
}

const STATUS_COLORS: Record<string, string> = {
  pending: 'bg-yellow-500/20 text-yellow-400',
  waiting_ci: 'bg-yellow-500/20 text-yellow-400',
  reviewing: 'bg-blue-500/20 text-blue-400',
  passed: 'bg-green-500/20 text-green-400',
  changes_required: 'bg-orange-500/20 text-orange-400',
  merged: 'bg-green-500/20 text-green-400',
  approved: 'bg-green-500/20 text-green-400',
  commented: 'bg-orange-500/20 text-orange-400',
  error: 'bg-red-500/20 text-red-400',
  superseded: 'bg-gray-500/20 text-gray-400',
};

const TERMINAL_RUN_STATUSES = new Set(['merged', 'closed']);
const READY_RUN_STATUSES = new Set(['ready_to_merge', 'merge_group_passed']);
const BUSY_RUN_STATUSES = new Set([
  'adjudicating',
  'repair_migrating',
  'repairing',
  'resolving_fixed_threads',
  'merge_queued',
  'merge_group_checking',
]);
const ACTIVE_REVIEW_STATUSES = new Set([
  'pending',
  'waiting_ci',
  'reviewing',
  'publishing',
  'superseding',
]);
const ACTIVE_PUBLICATION_STATUSES = new Set(['publishing', 'superseding']);
const STARTED_REPAIR_STATUSES = new Set(['delivering', 'accepted', 'awaiting_push', 'running']);
const STARTED_MERGE_STATUSES = new Set(['pending', 'enqueuing', 'queued', 'checking']);
const ACTIVE_ADJUDICATION_STATUSES = new Set(['pending', 'adjudicating', 'accepted']);
const BASE_ANCESTRY_ERROR = 'GitHub PR base ancestry is unsafe';

const REVIEW_STATUS_LABELS: Record<string, string> = {
  pending: 'Queued',
  waiting_ci: 'Waiting for CI',
  reviewing: 'Reviewing',
  publishing: 'Publishing result',
  superseding: 'Updating to a newer head',
  passed: 'Passed',
  // "approved" is a legacy CCM code-verdict status. New GitHub publications
  // are COMMENT events, so never label this as a GitHub approval.
  approved: 'Code review passed',
  changes_required: 'Changes required',
  commented: 'Comments published',
  error: 'Review failed',
  superseded: 'Superseded by a newer head',
  merged: 'Merged',
  closed: 'Closed',
  merge_pending: 'Merging',
  merge_queued: 'Legacy queue recovery',
  merge_group_checking: 'Legacy queue recovery',
};

const REVIEWER_ROLE_LABELS: Record<string, string> = {
  principal_engineer: 'Principal engineer',
  senior_engineer: 'Senior engineer',
  qa_engineer: 'QA engineer',
};

function statusText(status: string): string {
  return REVIEW_STATUS_LABELS[status]
    || status.split('_').map((part) => part.charAt(0).toUpperCase() + part.slice(1)).join(' ');
}

const REVIEW_ACTION_LABELS: Record<string, string> = {
  lgtm_comment: 'Pass comment published',
  review_comments: 'Change-request comment published',
  approved_merged: 'Pass comment published · PR merged',
  error: 'Action not completed',
};

function reviewActionText(action: string): string {
  if (REVIEW_ACTION_LABELS[action]) return REVIEW_ACTION_LABELS[action];
  return action
    .split('_')
    .map((part) => part.toLowerCase() === 'approved'
      ? 'passed review'
      : part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

function reviewerRoleText(role: string): string {
  return REVIEWER_ROLE_LABELS[role] || statusText(role);
}

function reviewStatusText(review: PRReview): string {
  return review.display_status?.trim() || statusText(review.status);
}

function reviewStatusClasses(review: PRReview): string {
  if (review.error_category === 'unsupported_input_size') {
    return 'bg-amber-500/15 text-amber-300';
  }
  if (review.aggregate_verdict === 'pass') return STATUS_COLORS.passed;
  if (review.aggregate_verdict === 'changes_required') return STATUS_COLORS.changes_required;
  return STATUS_COLORS[review.status] || 'bg-gray-600 text-gray-300';
}

function publicationStatusText(review: PRReview): string | null {
  if (!review.publication_state) return null;
  const labels = {
    not_started: 'GitHub publication not started',
    publishing: 'Publishing GitHub comment',
    reconciling: 'Reconciling GitHub publication',
    published: 'GitHub comment published',
    failed: 'GitHub publication failed',
    not_applicable: 'GitHub publication not applicable',
  } as const;
  return labels[review.publication_state];
}

function lifecycleStatusText(review: PRReview): string | null {
  if (!review.lifecycle_state) return null;
  const labels = {
    unknown: 'Historical lifecycle unavailable',
    reviewing: 'PR open',
    superseding: 'Updating to a newer head',
    superseded: 'Review superseded',
    cancelled: 'Review cancelled',
    merged: 'PR merged',
    closed: 'PR closed',
    failed: 'PR lifecycle failed',
  } as const;
  return labels[review.lifecycle_state];
}

function verdictStatusText(review: PRReview): string | null {
  if (review.verdict_state === 'pending') return 'Code review pending';
  if (review.verdict_state === 'unavailable') return 'Code verdict unavailable';
  if (review.aggregate_verdict === 'pass') return 'Code verdict: Pass';
  if (review.aggregate_verdict === 'changes_required') return 'Code verdict: Changes required';
  return null;
}

function reviewHasNoCodeVerdict(review: PRReview): boolean {
  if (review.aggregate_verdict) return false;
  if (review.verdict_state) return review.verdict_state === 'unavailable';
  return review.outcome_kind === 'infrastructure_error';
}

function parsePRMonitorDeepLink(): { repoId: number; reviewId: number | null } | null {
  const query = window.location.hash.split('?', 2)[1];
  if (!query) return null;
  const params = new URLSearchParams(query);
  const repoId = Number(params.get('repo'));
  const reviewId = Number(params.get('review'));
  if (!Number.isSafeInteger(repoId) || repoId <= 0) return null;
  return {
    repoId,
    reviewId: Number.isSafeInteger(reviewId) && reviewId > 0 ? reviewId : null,
  };
}

function setPRMonitorDeepLink(repoId?: number, reviewId?: number | null) {
  const params = new URLSearchParams();
  if (repoId) params.set('repo', String(repoId));
  if (reviewId) params.set('review', String(reviewId));
  const query = params.toString();
  window.history.replaceState(null, '', `#/pr-monitor${query ? `?${query}` : ''}`);
}

function shortSha(sha: string | null): string {
  return sha ? sha.slice(0, 8) : 'not captured';
}

function countSummary(counts: Record<string, number> | undefined): string | null {
  if (!counts) return null;
  const entries = Object.entries(counts).filter(([, count]) => count > 0);
  return entries.length
    ? entries.map(([status, count]) => `${count} ${statusText(status).toLowerCase()}`).join(' · ')
    : null;
}

function isActiveReview(review: PRReview): boolean {
  return review.outcome_kind === 'in_progress' || ACTIVE_REVIEW_STATUSES.has(review.status);
}

function copyToClipboard(text: string) {
  navigator.clipboard.writeText(text);
}

function FindingRebuttalForm({ finding, onSubmitted }: { finding: PRFinding; onSubmitted: () => Promise<void> }) {
  const [evidence, setEvidence] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const active = finding.rebuttals?.some(item => ['pending', 'adjudicating', 'accepted'].includes(item.status));
  if (finding.status !== 'open' || finding.severity === 'low') return null;
  return (
    <div className="mt-2 space-y-1">
      {finding.rebuttals?.map(item => (
        <p key={item.id} className="text-gray-500">Rebuttal #{item.attempt}: {item.status}{item.result_body ? ` · ${item.result_body}` : ''}</p>
      ))}
      <textarea value={evidence} onChange={(event) => setEvidence(event.target.value)}
        disabled={active || submitting} rows={3} placeholder="Concrete code/test/policy evidence for this exact head"
        className="w-full bg-gray-700 rounded px-2 py-1 text-xs" />
      {error && <p className="text-red-400">{error}</p>}
      <button disabled={active || submitting || evidence.trim().length < 20}
        className="bg-indigo-600 text-white rounded px-2 py-1 disabled:opacity-50"
        onClick={async () => {
          setSubmitting(true); setError(null);
          try { await api.submitPRFindingRebuttal(finding.id, evidence.trim()); setEvidence(''); await onSubmitted(); }
          catch (caught) { setError(String(caught)); }
          finally { setSubmitting(false); }
        }}>{active ? 'Adjudicating…' : 'Submit rebuttal'}</button>
    </div>
  );
}

function AddRepoModal({
  isAdmin,
  onClose,
  onSaved,
}: {
  isAdmin: boolean;
  onClose: () => void;
  onSaved: () => void;
}) {
  const [repoName, setRepoName] = useState('');
  const [autoMerge, setAutoMerge] = useState(false);
  const [autoRepair, setAutoRepair] = useState(false);
  const [reviewMode, setReviewMode] = useState<'single' | 'panel'>('single');
  const [waitForCi, setWaitForCi] = useState(false);
  const [requiredChecks, setRequiredChecks] = useState('');
  const [provider, setProvider] = useState('codex');
  const [reviewModel, setReviewModel] = useState('');
  const [reviewEffort, setReviewEffort] = useState('');
  const {
    providers,
    defaultProvider,
    providerConfigLoaded,
    modelsFor,
    effortsFor,
  } = useProviderModels();
  const modelOptions = modelsFor(provider);
  const effortOptions = effortsFor(provider, reviewModel);
  const [defaultBranch, setDefaultBranch] = useState('main');
  const [allowedAuthors, setAllowedAuthors] = useState('');
  const [workerId, setWorkerId] = useState('');
  const [workers, setWorkers] = useState<{ id: number; name: string }[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [createdSecret, setCreatedSecret] = useState<string | null>(null);
  const dialogRef = useDialogA11y(true, onClose);

  useEffect(() => {
    api.listWorkers().then(w => setWorkers(w.filter(wk => wk.status !== 'terminated'))).catch(() => {});
  }, []);

  useEffect(() => {
    if (providerConfigLoaded) setProvider(defaultProvider);
  }, [defaultProvider, providerConfigLoaded]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!isAdmin || !currentUserIsAdmin()) {
      setError('Only administrators can add a PR Monitor repository.');
      return;
    }
    setSubmitting(true);
    setError(null);
    try {
      if (!providerConfigLoaded || !providers.includes(provider)) {
        throw new Error('No supported PR Monitor provider is available');
      }
      const authors = allowedAuthors.trim() ? allowedAuthors.split(',').map(a => a.trim()).filter(Boolean) : [];
      const checks = reviewMode === 'panel' ? parseRequiredChecks(requiredChecks) : [];
      if (reviewMode === 'panel' && waitForCi && checks.length === 0) {
        throw new Error('启用 CI Gate 时至少配置一个 required check');
      }
      const created = await api.createMonitoredRepo({
        repo_full_name: repoName.trim(),
        auto_merge: autoMerge,
        auto_repair: reviewMode === 'panel' && autoRepair,
        max_repair_attempts: 3,
        merge_queue_mode: 'manual',
        provider,
        review_model: reviewModel.trim() || undefined,
        review_effort: reviewEffort || undefined,
        review_mode: reviewMode,
        wait_for_ci: reviewMode === 'panel' && waitForCi,
        required_checks: reviewMode === 'panel' ? checks : [],
        default_branch: defaultBranch.trim() || 'main',
        allowed_authors: authors,
        worker_id: workerId ? Number(workerId) : undefined,
      });
      setCreatedSecret(created.webhook_secret);
      onSaved();
    } catch (e) {
      setError(String(e));
    } finally {
      setSubmitting(false);
    }
  };

  if (createdSecret) {
    return (
      <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50 p-4">
        <div className="bg-gray-800 rounded-xl shadow-2xl w-full max-w-md">
          <div className="px-5 py-4 border-b border-gray-700">
            <h3 className="text-foreground font-semibold">Repository added</h3>
          </div>
          <div className="p-5 space-y-4">
            <p className="text-sm text-amber-300" role="alert">
              Copy this webhook secret now. It will not be shown again.
            </p>
            <code className="block bg-gray-700 text-foreground text-xs rounded px-3 py-2 break-all">
              {createdSecret}
            </code>
            <div className="flex justify-end gap-2">
              <button type="button" onClick={() => copyToClipboard(createdSecret)}
                className="px-4 py-2 text-sm bg-gray-700 text-gray-200 rounded hover:bg-gray-600">
                Copy secret
              </button>
              <button type="button" onClick={onClose}
                className="px-4 py-2 text-sm bg-indigo-600 text-white rounded hover:bg-indigo-500">
                Done
              </button>
            </div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4"
      onMouseDown={(event) => event.target === event.currentTarget && onClose()}
    >
      <div
        ref={dialogRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby="add-repository-title"
        className="flex max-h-[calc(100dvh-2rem)] w-full max-w-md flex-col overflow-hidden rounded-xl bg-gray-800 shadow-2xl"
      >
        <div className="flex shrink-0 items-center justify-between border-b border-gray-700 px-5 py-4">
          <h3 id="add-repository-title" className="text-foreground font-semibold">Add Repository</h3>
          <button type="button" aria-label="Close Add Repository" onClick={onClose} className="text-gray-400 hover:text-gray-200"><X size={18} /></button>
        </div>
        <form onSubmit={handleSubmit} className="min-h-0 flex-1 space-y-4 overflow-y-auto overscroll-contain p-5">
          {error && <p className="text-red-400 text-sm">{error}</p>}
          <div>
            <label className="block text-xs text-gray-400 mb-1">Repository (owner/repo)</label>
            <input
              className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
              value={repoName} onChange={(e) => setRepoName(e.target.value)}
              placeholder="owner/repo" required
            />
          </div>
          <div className="flex items-center gap-2">
            <input type="checkbox" id="autoMerge" checked={autoMerge}
              onChange={(e) => setAutoMerge(e.target.checked)}
              className="rounded bg-gray-700 border-gray-600" />
            <label htmlFor="autoMerge" className="text-sm text-gray-300">Direct auto-merge after review and exact-head gates pass</label>
          </div>
          <p className="text-xs text-gray-500">
            {mergePolicyHelp(autoMerge)}
          </p>
          <div className="flex items-center gap-2">
            <input type="checkbox" id="autoRepair" checked={autoRepair}
              disabled={reviewMode !== 'panel'} onChange={(e) => setAutoRepair(e.target.checked)} />
            <label htmlFor="autoRepair" className="text-sm text-gray-300">Auto-resume bound local Developer Task (max 3 heads)</label>
          </div>
          {reviewMode === 'panel' && waitForCi && (
            <div>
              <label className="block text-xs text-gray-400 mb-1">Required CI identities（每行一个）</label>
              <textarea className="w-full bg-gray-700 text-foreground text-xs rounded px-3 py-2 font-mono"
                rows={3} value={requiredChecks} onChange={(e) => setRequiredChecks(e.target.value)}
                placeholder={'check_run,tests,github-actions\nstatus,lint,ci-bot'} required />
              <p className="text-xs text-gray-500 mt-1">精确匹配当前 commit 的检查名和发布者身份，避免同名假绿。</p>
            </div>
          )}
          <div>
            <label className="block text-xs text-gray-400 mb-1">Review Harness</label>
            <select className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2"
              value={reviewMode} onChange={(e) => {
                const value = e.target.value as 'single' | 'panel';
                setReviewMode(value);
                if (value === 'single') { setAutoRepair(false); setWaitForCi(false); }
                else setWaitForCi(true);
              }}>
              <option value="single">Single reviewer (recommended)</option>
              <option value="panel">3-reviewer panel: Principal / Senior / QA</option>
            </select>
            {reviewMode === 'panel' ? (
              <p className="mt-1 text-xs text-amber-300">
                Panel runs three independent review Tasks and uses roughly 3× the model work. Choose it only when you need separate role coverage.
              </p>
            ) : (
              <p className="mt-1 text-xs text-gray-500">One review Task with a bounded PR context.</p>
            )}
          </div>
          <div className="flex items-center gap-2">
            <input type="checkbox" id="newWaitForCi" checked={waitForCi}
              disabled={reviewMode !== 'panel'} onChange={(e) => {
                setWaitForCi(e.target.checked);
              }} />
            <label htmlFor="newWaitForCi" className="text-sm text-gray-300">Wait for exact-head CI</label>
          </div>
          <div>
            <label className="block text-xs text-gray-400 mb-1">Provider</label>
            <select
              className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
              disabled={!providerConfigLoaded || providers.length === 0}
              value={provider} onChange={(e) => { setProvider(e.target.value); setReviewModel(''); setReviewEffort(''); }}
            >
              {providers.map((p) => <option key={p} value={p}>{p === 'codex' ? 'Codex' : 'Claude Code'}</option>)}
            </select>
          </div>
          <div>
            <label className="block text-xs text-gray-400 mb-1">Review Model (optional)</label>
            <select
              className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
              value={reviewModel} onChange={(e) => { setReviewModel(e.target.value); setReviewEffort(''); }}
            >
              <option value="">default</option>
              {modelOptions.map((m) => <option key={m} value={m}>{m}</option>)}
            </select>
          </div>
          <div>
            <label className="block text-xs text-gray-400 mb-1">Review Effort (optional)</label>
            <select
              className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
              value={reviewEffort} onChange={(e) => setReviewEffort(e.target.value)}
            >
              <option value="">default</option>
              {effortOptions.map((effort) => <option key={effort} value={effort}>{effort}</option>)}
            </select>
          </div>
          <div>
            <label className="block text-xs text-gray-400 mb-1">Default Branch</label>
            <input
              className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
              value={defaultBranch} onChange={(e) => setDefaultBranch(e.target.value)}
            />
          </div>
          <div>
            <label className="block text-xs text-gray-400 mb-1">Run on</label>
            <select
              className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
              value={workerId} onChange={(e) => setWorkerId(e.target.value)}
            >
              {isAdmin && <option value="">本机</option>}
              {workers.map(w => <option key={w.id} value={w.id}>{w.name}</option>)}
            </select>
          </div>
          <div>
            <label className="block text-xs text-gray-400 mb-1">Allowed Authors (comma-separated, empty = all)</label>
            <input
              className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
              value={allowedAuthors} onChange={(e) => setAllowedAuthors(e.target.value)}
              placeholder="user1, user2"
            />
          </div>
          <div className="flex justify-end gap-2 pt-1">
            <button type="button" onClick={onClose} className="px-4 py-2 text-sm text-gray-300 hover:text-foreground">Cancel</button>
            <button type="submit" disabled={
              submitting
              || !repoName.trim()
              || !providerConfigLoaded
              || !providers.includes(provider)
            }
              className="px-4 py-2 text-sm bg-indigo-600 text-white rounded hover:bg-indigo-500 disabled:opacity-50">
              {submitting ? 'Adding...' : 'Add'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

function RepoDetail({
  repo,
  initialReviewId,
  onBack,
  onRefresh,
}: {
  repo: MonitoredRepo;
  initialReviewId?: number | null;
  onBack: () => void;
  onRefresh: () => void;
}) {
  const isAdmin = currentUserIsAdmin();
  const initialReviewMode = repo.review_mode || 'single';
  const initialWaitForCi = initialReviewMode === 'panel' && Boolean(repo.wait_for_ci);
  const [detail, setDetail] = useState<MonitoredRepo>(repo);
  const [reviews, setReviews] = useState<PRReview[]>([]);
  const [page, setPage] = useState(1);
  const [autoMerge, setAutoMerge] = useState(Boolean(repo.auto_merge));
  const [autoRepair, setAutoRepair] = useState(initialReviewMode === 'panel' && Boolean(repo.auto_repair));
  const [maxRepairAttempts, setMaxRepairAttempts] = useState(repo.max_repair_attempts || 3);
  const [provider, setProvider] = useState(repo.provider || 'claude');
  const [reviewModel, setReviewModel] = useState(repo.review_model || '');
  const [reviewEffort, setReviewEffort] = useState(repo.review_effort || '');
  const [reviewMode, setReviewMode] = useState<'single' | 'panel'>(initialReviewMode);
  const [waitForCi, setWaitForCi] = useState(initialWaitForCi);
  const [requiredChecks, setRequiredChecks] = useState(renderRequiredChecks(repo));
  const [selectedReview, setSelectedReview] = useState<PRReview | null>(null);
  const [monitorRun, setMonitorRun] = useState<PRMonitorRun | null>(null);
  const selectedReviewIdRef = useRef<number | null>(null);
  selectedReviewIdRef.current = selectedReview?.id ?? null;
  const [developerTaskId, setDeveloperTaskId] = useState('');
  const { providers, providerConfigLoaded, modelsFor, effortsFor } = useProviderModels();
  const modelOptions = modelsFor(provider);
  const effortOptions = effortsFor(provider, reviewModel);
  const [defaultBranch, setDefaultBranch] = useState(repo.default_branch);
  const [authorsInput, setAuthorsInput] = useState((repo.allowed_authors || []).join(', '));
  const [saving, setSaving] = useState(false);
  const [copied, setCopied] = useState<string | null>(null);
  const [revealedSecret, setRevealedSecret] = useState<string | null>(null);
  const [webhookUrl, setWebhookUrl] = useState(DEFAULT_WEBHOOK_URL);
  const [saveError, setSaveError] = useState<string | null>(null);
  const [runActionError, setRunActionError] = useState<string | null>(null);
  const [runActionPending, setRunActionPending] = useState<string | null>(null);
  const [rerunPending, setRerunPending] = useState(false);
  const [rerunNotice, setRerunNotice] = useState<string | null>(null);
  const rerunIdempotencyRef = useRef<{
    reviewId: number;
    headSha: string;
    key: string;
  } | null>(null);
  const initialReviewOpenedRef = useRef<number | null>(null);
  const openReviewRequestRef = useRef(0);
  const runActionRequestRef = useRef(0);
  const [githubIdentity, setGitHubIdentity] = useState<GitHubPublisherIdentity | null>(null);
  const [githubIdentityLoading, setGitHubIdentityLoading] = useState(true);
  const [githubIdentityHttpError, setGitHubIdentityHttpError] = useState<string | null>(null);
  const githubIdentityRequestRef = useRef(0);

  useEffect(() => {
    api.getWebhookInfo()
      .then(info => setWebhookUrl(info.webhook_url || DEFAULT_WEBHOOK_URL))
      .catch(() => setWebhookUrl(DEFAULT_WEBHOOK_URL));
  }, []);

  const loadGitHubIdentity = useCallback(async (forceRefresh = false) => {
    const requestId = ++githubIdentityRequestRef.current;
    const expectedRepoId = repo.id;
    setGitHubIdentityLoading(true);
    setGitHubIdentityHttpError(null);
    try {
      const identity = await api.getPRMonitorGitHubIdentity(expectedRepoId, forceRefresh);
      if (requestId !== githubIdentityRequestRef.current || expectedRepoId !== repo.id) return;
      setGitHubIdentity(identity);
    } catch (error) {
      if (requestId !== githubIdentityRequestRef.current || expectedRepoId !== repo.id) return;
      setGitHubIdentity(null);
      setGitHubIdentityHttpError(error instanceof Error ? error.message : String(error));
    } finally {
      if (requestId === githubIdentityRequestRef.current && expectedRepoId === repo.id) {
        setGitHubIdentityLoading(false);
      }
    }
  }, [repo.id]);

  useEffect(() => {
    void loadGitHubIdentity();
    return () => {
      githubIdentityRequestRef.current += 1;
    };
  }, [loadGitHubIdentity]);

  useEffect(() => {
    if (providerConfigLoaded && !providers.includes(provider)) {
      setProvider(availableProvider(provider, providers));
      setReviewModel('');
      setReviewEffort('');
    }
  }, [provider, providerConfigLoaded, providers]);

  const loadDetail = useCallback(async () => {
    try {
      const d = await api.getMonitoredRepo(repo.id);
      setDetail(d);
    } catch (error) { setSaveError(String(error)); }
  }, [repo.id]);

  const loadReviews = useCallback(async () => {
    try {
      const r = await api.getRepoReviews(repo.id, page);
      setReviews(r);
    } catch (error) { setSaveError(String(error)); }
  }, [repo.id, page]);

  const openReview = useCallback(async (reviewId: number, updateDeepLink = true) => {
    const requestId = ++openReviewRequestRef.current;
    // Selecting a Review invalidates any slow Run mutation started from the
    // previous selection. The backend operation may still complete, but its
    // response must never be rendered as the newly selected Review's Run.
    runActionRequestRef.current += 1;
    setRunActionPending(null);
    setRunActionError(null);
    setRerunNotice(null);
    setDeveloperTaskId('');
    try {
      const reviewDetail = await api.getReviewDetail(reviewId);
      if (requestId !== openReviewRequestRef.current) return;
      if (reviewDetail.repo_id !== repo.id) {
        throw new Error('The requested Review does not belong to this monitored repository');
      }
      selectedReviewIdRef.current = reviewDetail.id;
      setSelectedReview(reviewDetail);
      setMonitorRun(null);
      if (updateDeepLink) setPRMonitorDeepLink(repo.id, reviewDetail.id);
      if (reviewDetail.monitor_run_id) {
        try {
          const expectedRunId = reviewDetail.monitor_run_id;
          const nextRun = await api.getPRMonitorRun(expectedRunId);
          if (
            requestId === openReviewRequestRef.current
            && selectedReviewIdRef.current === reviewDetail.id
            && nextRun.id === expectedRunId
          ) {
            setMonitorRun(nextRun);
          }
        } catch (error) {
          if (
            requestId === openReviewRequestRef.current
            && selectedReviewIdRef.current === reviewDetail.id
          ) {
            setRunActionError(String(error));
          }
        }
      }
      return requestId === openReviewRequestRef.current
        && selectedReviewIdRef.current === reviewDetail.id
        ? reviewDetail
        : null;
    } catch (error) {
      if (requestId !== openReviewRequestRef.current) return null;
      selectedReviewIdRef.current = null;
      setSelectedReview(null);
      setMonitorRun(null);
      setRunActionError(String(error));
      return null;
    }
  }, [repo.id]);

  useEffect(() => { loadDetail(); loadReviews(); }, [loadDetail, loadReviews]);

  useEffect(() => {
    if (!initialReviewId || initialReviewOpenedRef.current === initialReviewId) return;
    initialReviewOpenedRef.current = initialReviewId;
    void openReview(initialReviewId, false);
  }, [initialReviewId, openReview]);

  const refreshSelectedReview = useCallback(async () => {
    const reviewId = selectedReviewIdRef.current;
    if (reviewId == null) return;
    try {
      const reviewDetail = await api.getReviewDetail(reviewId);
      if (selectedReviewIdRef.current !== reviewId) return;
      setSelectedReview(reviewDetail);
      if (reviewDetail.monitor_run_id) {
        const run = await api.getPRMonitorRun(reviewDetail.monitor_run_id);
        if (selectedReviewIdRef.current === reviewId) setMonitorRun(run);
      } else {
        setMonitorRun(null);
      }
      setRunActionError(null);
    } catch (error) {
      if (selectedReviewIdRef.current === reviewId) setRunActionError(String(error));
    }
  }, []);

  const refreshReviewData = useCallback(() => {
    void loadReviews();
    void refreshSelectedReview();
  }, [loadReviews, refreshSelectedReview]);

  const shouldPollReviews = reviews.some(isActiveReview)
    || Boolean(selectedReview && isActiveReview(selectedReview))
    || Boolean(monitorRun?.merge_actions?.some((action) => STARTED_MERGE_STATUSES.has(action.status)));
  const prMonitorWs = useWebSocket(
    ['pr-monitor'],
    refreshReviewData,
    refreshReviewData,
    refreshReviewData,
  );
  useVisibilityAwareInterval(refreshReviewData, prMonitorWs?.isConnected ? 30000 : 5000, shouldPollReviews, false);

  const latestReviewIdByPr = useMemo(() => {
    const latest = new Map<number, number>();
    reviews.forEach((review) => {
      if (!latest.has(review.pr_number)) latest.set(review.pr_number, review.id);
    });
    return latest;
  }, [reviews]);

  const handleSave = async () => {
    setSaving(true);
    setSaveError(null);
    try {
      if (!providerConfigLoaded || !providers.includes(provider)) {
        throw new Error('No supported PR Monitor provider is available');
      }
      const authors = authorsInput.trim() ? authorsInput.split(',').map(a => a.trim()).filter(Boolean) : [];
      const checks = reviewMode === 'panel' ? parseRequiredChecks(requiredChecks) : [];
      if (reviewMode === 'panel' && waitForCi && checks.length === 0) {
        throw new Error('启用 CI Gate 时至少配置一个 required check');
      }
      const updated = await api.updateMonitoredRepo(repo.id, {
        auto_merge: autoMerge,
        auto_repair: reviewMode === 'panel' && autoRepair,
        max_repair_attempts: maxRepairAttempts,
        merge_queue_mode: 'manual',
        provider,
        // 显式 null 才能清空（undefined 会被后端 exclude_unset 丢弃，
        // 换 provider 后旧模型残留会让 CLI 拿到错家族的 --model）
        review_model: reviewModel.trim() ? reviewModel.trim() : null,
        review_effort: reviewEffort || null,
        review_mode: reviewMode,
        wait_for_ci: reviewMode === 'panel' && waitForCi,
        required_checks: reviewMode === 'panel' ? checks : [],
        default_branch: defaultBranch.trim() || 'main',
        allowed_authors: authors,
      });
      setDetail(updated);
      setRequiredChecks(renderRequiredChecks(updated));
      onRefresh();
    } catch (error) { setSaveError(String(error)); }
    setSaving(false);
  };

  const handleRegenerate = async () => {
    if (!confirm('Regenerate webhook secret? You will need to update the GitHub webhook config.')) return;
    try {
      const updated = await api.regenerateSecret(repo.id);
      setRevealedSecret(updated.webhook_secret);
      setDetail({
        ...updated,
        webhook_secret: `${updated.webhook_secret.slice(0, 4)}***`,
      });
      setSaveError(null);
    } catch (error) { setSaveError(String(error)); }
  };

  const performRunAction = async (
    action: string,
    operation: () => Promise<PRMonitorRun>,
  ) => {
    const sourceReviewId = selectedReviewIdRef.current;
    const sourceRunId = monitorRun?.id ?? null;
    const sourceOpenRequest = openReviewRequestRef.current;
    if (sourceReviewId == null || sourceRunId == null) return;
    const actionRequestId = ++runActionRequestRef.current;
    const stillCurrent = () => (
      actionRequestId === runActionRequestRef.current
      && sourceOpenRequest === openReviewRequestRef.current
      && sourceReviewId === selectedReviewIdRef.current
    );
    setRunActionPending(action);
    setRunActionError(null);
    try {
      const updatedRun = await operation();
      if (stillCurrent() && updatedRun.id === sourceRunId) {
        setMonitorRun(updatedRun);
      }
    } catch (error) {
      if (stillCurrent()) setRunActionError(String(error));
    } finally {
      if (stillCurrent()) setRunActionPending(null);
    }
  };

  const rerunSelectedReview = async () => {
    if (!selectedReview?.can_rerun || !selectedReview.head_sha || rerunPending) return;
    const sourceReviewId = selectedReview.id;
    const sourceHeadSha = selectedReview.head_sha;
    setRerunPending(true);
    setRunActionError(null);
    setRerunNotice(null);
    const priorAttempt = rerunIdempotencyRef.current;
    const attempt = priorAttempt
      && priorAttempt.reviewId === selectedReview.id
      && priorAttempt.headSha === selectedReview.head_sha
      ? priorAttempt
      : {
          reviewId: selectedReview.id,
          headSha: selectedReview.head_sha,
          key: typeof crypto.randomUUID === 'function'
            ? crypto.randomUUID()
            : `pr-rerun-${selectedReview.id}-${Date.now()}`,
        };
    rerunIdempotencyRef.current = attempt;
    let rerunId: number;
    try {
      const receipt = await api.rerunPRReview(sourceReviewId, sourceHeadSha, attempt.key);
      rerunIdempotencyRef.current = null;
      rerunId = receipt.id;
    } catch (error) {
      if (
        selectedReviewIdRef.current === sourceReviewId
        && selectedReview?.head_sha === sourceHeadSha
      ) {
        setRunActionError(`Could not start the exact-head review: ${String(error)}`);
      }
      setRerunPending(false);
      return;
    }
    if (
      selectedReviewIdRef.current !== sourceReviewId
      || selectedReview?.head_sha !== sourceHeadSha
    ) {
      setRerunPending(false);
      void loadReviews();
      return;
    }
    const rerun = await openReview(rerunId);
    if (rerun && selectedReviewIdRef.current === rerun.id) {
      setRerunNotice('Exact-head review started.');
    }
    setRerunPending(false);
    void loadReviews();
  };

  const reviewerRuns = selectedReview?.reviewer_runs ?? [];
  const selectedReviewSummary = selectedReview?.display_summary?.trim()
    || selectedReview?.review_summary?.trim()
    || (selectedReview && isActiveReview(selectedReview)
      ? 'The review is still running. This page will refresh automatically.'
      : 'No review summary was recorded.');
  const reviewerStatusSummary = countSummary(selectedReview?.reviewer_status_counts);
  const reviewerVerdictSummary = countSummary(selectedReview?.reviewer_verdict_counts);
  const reviewerCount = selectedReview?.reviewer_count ?? reviewerRuns.length;
  const noCodeVerdict = Boolean(selectedReview && reviewHasNoCodeVerdict(selectedReview));
  const inputAdmissionRejected = selectedReview?.error_category === 'unsupported_input_size';
  const selectedGitHubReviewUrl = selectedReview
    ? canonicalGitHubReviewUrl(
      selectedReview.github_review_url,
      detail.repo_full_name,
      selectedReview.pr_number,
      selectedReview.github_review_id,
    )
    : null;
  const terminalRun = monitorRun ? TERMINAL_RUN_STATUSES.has(monitorRun.status) : false;
  const readyRun = monitorRun ? READY_RUN_STATUSES.has(monitorRun.status) : false;
  const busyRun = monitorRun ? BUSY_RUN_STATUSES.has(monitorRun.status) : false;
  const baseUpdateRequired = Boolean(
    monitorRun?.pause_reason === 'direct_merge_base_update_required'
    || monitorRun?.pause_reason === 'direct_merge_base_update_requested'
    || monitorRun?.merge_actions?.some((action) => action.last_error?.includes(BASE_ANCESTRY_ERROR)),
  );
  const branchUpdateRequested = monitorRun?.pause_reason === 'direct_merge_base_update_requested';
  const activeReview = Boolean(
    (selectedReview && ACTIVE_REVIEW_STATUSES.has(selectedReview.status))
    || (monitorRun && ACTIVE_REVIEW_STATUSES.has(monitorRun.status)),
  );
  const activePublication = Boolean(
    (selectedReview && ACTIVE_PUBLICATION_STATUSES.has(selectedReview.status))
    || (monitorRun && ACTIVE_PUBLICATION_STATUSES.has(monitorRun.status)),
  );
  const activeRepair = Boolean(monitorRun?.wakes.some((wake) => STARTED_REPAIR_STATUSES.has(wake.status)));
  const activeMerge = Boolean(monitorRun?.merge_actions?.some((action) => STARTED_MERGE_STATUSES.has(action.status)));
  const activeAdjudication = reviewerRuns.some((reviewerRun) => reviewerRun.findings.some(
    (finding) => finding.rebuttals?.some((rebuttal) => ACTIVE_ADJUDICATION_STATUSES.has(rebuttal.status)),
  ));
  const protectedRun = terminalRun || readyRun || busyRun || activeRepair || activeMerge || activeAdjudication;
  const canChangeBinding = Boolean(monitorRun && !protectedRun && !activePublication);
  const canBindDeveloper = canChangeBinding && detail.enabled;
  const canPause = Boolean(
    monitorRun
    && monitorRun.status !== 'paused'
    && !protectedRun
    && !activeReview,
  );
  const canResume = Boolean(
    monitorRun
    && monitorRun.status === 'paused'
    && !baseUpdateRequired
    && detail.enabled
    && !protectedRun
    && !activeReview
    && !activePublication,
  );
  const canMerge = Boolean(
    monitorRun?.status === 'ready_to_merge'
    && !busyRun
    && !activeRepair
    && !activeMerge
    && !activeAdjudication
    && !activeReview
    && !activePublication,
  );
  const developerTaskNumber = Number(developerTaskId);
  const validDeveloperTaskId = Number.isInteger(developerTaskNumber) && developerTaskNumber > 0;
  const secretForDisplay = revealedSecret ?? detail.webhook_secret;

  const handleCopy = (text: string, label: string) => {
    copyToClipboard(text);
    setCopied(label);
    setTimeout(() => setCopied(null), 2000);
  };

  return (
    <div className="space-y-6">
      <button onClick={onBack} className="flex items-center gap-1 text-sm text-gray-400 hover:text-foreground">
        <ArrowLeft size={16} /> Back to repositories
      </button>

      <div className="bg-gray-800 rounded-lg p-5 space-y-4">
        <h3 className="text-foreground font-semibold text-lg">{detail.repo_full_name}</h3>
        {saveError && <p className="text-sm text-red-400">{saveError}</p>}

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div className="flex items-center gap-2">
            <input type="checkbox" id="detailAutoMerge" checked={autoMerge}
              onChange={(e) => setAutoMerge(e.target.checked)}
              className="rounded bg-gray-700 border-gray-600" />
            <label htmlFor="detailAutoMerge" className="text-sm text-gray-300">Direct auto-merge after review and exact-head gates pass</label>
          </div>
          <p className="text-xs text-gray-500 md:col-span-2">
            {mergePolicyHelp(autoMerge)}
          </p>
          <div className="flex items-center gap-2">
            <input type="checkbox" id="detailAutoRepair" checked={autoRepair}
              disabled={reviewMode !== 'panel'} onChange={(e) => setAutoRepair(e.target.checked)} />
            <label htmlFor="detailAutoRepair" className="text-sm text-gray-300">Auto-resume bound Developer Task</label>
          </div>
          <div>
            <label className="block text-xs text-gray-400 mb-1">Max automatic repair heads</label>
            <input type="number" min={1} max={20} value={maxRepairAttempts}
              onChange={(e) => setMaxRepairAttempts(Number(e.target.value))}
              className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2" />
          </div>
          {reviewMode === 'panel' && waitForCi && (
            <div className="md:col-span-2">
              <label className="block text-xs text-gray-400 mb-1">Required CI identities（每行一个）</label>
              <textarea className="w-full bg-gray-700 text-foreground text-xs rounded px-3 py-2 font-mono"
                rows={3} value={requiredChecks} onChange={(e) => setRequiredChecks(e.target.value)}
                placeholder={'check_run,tests,github-actions\nstatus,lint,ci-bot'} />
            </div>
          )}
          <div>
            <label className="block text-xs text-gray-400 mb-1">Provider</label>
            <select className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
              disabled={!providerConfigLoaded || providers.length === 0}
              value={provider} onChange={(e) => { setProvider(e.target.value); setReviewModel(''); setReviewEffort(''); }}>
              {providers.map((p) => <option key={p} value={p}>{p === 'codex' ? 'Codex' : 'Claude Code'}</option>)}
            </select>
          </div>
          <div>
            <label className="block text-xs text-gray-400 mb-1">Review Model</label>
            <select className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
              value={reviewModel} onChange={(e) => { setReviewModel(e.target.value); setReviewEffort(''); }}>
              <option value="">default</option>
              {reviewModel && !modelOptions.includes(reviewModel) && (
                <option value={reviewModel}>{reviewModel}</option>
              )}
              {modelOptions.map((m) => <option key={m} value={m}>{m}</option>)}
            </select>
          </div>
          <div>
            <label className="block text-xs text-gray-400 mb-1">Review Effort</label>
            <select className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
              value={reviewEffort} onChange={(e) => setReviewEffort(e.target.value)}>
              <option value="">default</option>
              {reviewEffort && !effortOptions.includes(reviewEffort) && (
                <option value={reviewEffort}>{reviewEffort}</option>
              )}
              {effortOptions.map((effort) => <option key={effort} value={effort}>{effort}</option>)}
            </select>
          </div>
          <div>
            <label className="block text-xs text-gray-400 mb-1">Default Branch</label>
            <input className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
              value={defaultBranch} onChange={(e) => setDefaultBranch(e.target.value)} />
          </div>
          <div>
            <label className="block text-xs text-gray-400 mb-1">Review Harness</label>
            <select className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2"
              value={reviewMode} onChange={(e) => {
                const value = e.target.value as 'single' | 'panel';
                setReviewMode(value);
                if (value === 'single') { setAutoRepair(false); setWaitForCi(false); }
                else setWaitForCi(true);
              }}>
              <option value="single">Single reviewer (recommended)</option>
              <option value="panel">3-reviewer panel: Principal / Senior / QA</option>
            </select>
            {reviewMode === 'panel' ? (
              <p className="mt-1 text-xs text-amber-300">
                Panel runs three independent review Tasks and uses roughly 3× the model work.
              </p>
            ) : (
              <p className="mt-1 text-xs text-gray-500">One review Task with a bounded PR context.</p>
            )}
          </div>
          <div className="flex items-center gap-2">
            <input type="checkbox" id="waitForCi" checked={reviewMode === 'panel' && waitForCi}
              disabled={reviewMode !== 'panel'} onChange={(e) => {
                setWaitForCi(e.target.checked);
              }} />
            <label htmlFor="waitForCi" className="text-sm text-gray-300">Wait for exact-head CI before review</label>
          </div>
          <div>
            <label className="block text-xs text-gray-400 mb-1">Allowed Authors (comma-separated)</label>
            <input className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
              value={authorsInput} onChange={(e) => setAuthorsInput(e.target.value)} placeholder="All authors" />
          </div>
        </div>

        <button onClick={handleSave} disabled={
          saving || !providerConfigLoaded || !providers.includes(provider)
        }
          className="px-4 py-2 text-sm bg-indigo-600 text-white rounded hover:bg-indigo-500 disabled:opacity-50">
          {saving ? 'Saving...' : 'Save Changes'}
        </button>
      </div>

      <div className="bg-gray-800 rounded-lg p-5 space-y-3">
        <h4 className="text-foreground font-semibold">Webhook Configuration</h4>
        <div className="space-y-2">
          <div>
            <label className="block text-xs text-gray-400 mb-1">Payload URL</label>
            <div className="flex items-center gap-2">
              <code className="flex-1 bg-gray-700 text-foreground text-xs rounded px-3 py-2 overflow-x-auto">{webhookUrl}</code>
              <button onClick={() => handleCopy(webhookUrl, 'url')}
                className="p-2 text-gray-400 hover:text-foreground">
                {copied === 'url' ? <Check size={16} className="text-green-400" /> : <Copy size={16} />}
              </button>
            </div>
          </div>
          <div>
            <label className="block text-xs text-gray-400 mb-1">Secret</label>
            <div className="flex items-center gap-2">
              <code className="flex-1 bg-gray-700 text-foreground text-xs rounded px-3 py-2 overflow-x-auto">{secretForDisplay}</code>
              <button onClick={() => handleCopy(secretForDisplay, 'secret')}
                disabled={!revealedSecret}
                title={revealedSecret ? 'Copy newly generated secret' : 'Rotate to reveal a new secret'}
                className="p-2 text-gray-400 hover:text-foreground disabled:opacity-40">
                {copied === 'secret' ? <Check size={16} className="text-green-400" /> : <Copy size={16} />}
              </button>
              <button onClick={handleRegenerate} className="p-2 text-gray-400 hover:text-foreground" title="Regenerate secret">
                <RefreshCw size={16} />
              </button>
            </div>
          </div>
          <p className="text-xs text-gray-500">
            The stored secret is hidden. Rotate it to receive a new value once.
            {' '}Content type: application/json. Events: Pull requests only.
          </p>
        </div>
      </div>

      <section aria-label="GitHub publishing identity" className="bg-gray-800 rounded-lg p-5 space-y-3">
        <div className="flex items-center justify-between gap-3">
          <h4 className="text-foreground font-semibold">GitHub Publishing Identity</h4>
          {isAdmin && (
            <button
              type="button"
              onClick={() => void loadGitHubIdentity(true)}
              disabled={githubIdentityLoading}
              className="inline-flex items-center gap-1 rounded px-2 py-1 text-xs text-gray-300 hover:bg-gray-700 disabled:cursor-wait disabled:opacity-50"
            >
              <RefreshCw size={13} className={githubIdentityLoading ? 'animate-spin' : ''} />
              {githubIdentityLoading ? 'Checking…' : 'Refresh identity'}
            </button>
          )}
        </div>
        <p className="text-xs leading-5 text-gray-400">
          This is the backend <code>gh</code> identity CCM uses to publish PR comments.
          It is independent of Codex authentication and any GitHub login in this browser or connector.
        </p>
        {githubIdentityLoading && !githubIdentity && !githubIdentityHttpError && (
          <p role="status" className="text-xs text-blue-300">Checking the CCM backend identity…</p>
        )}
        {githubIdentityHttpError && (
          <p role="alert" className="rounded border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs text-red-300">
            Identity request failed: {githubIdentityHttpError}
          </p>
        )}
        {githubIdentity?.available && githubIdentity.actor && (
          <div className="rounded border border-emerald-500/30 bg-emerald-500/10 px-3 py-2 text-xs text-emerald-200">
            <p className="font-medium">CCM will publish as {githubIdentity.actor}</p>
            <p className="mt-1 opacity-75">Checked {new Date(githubIdentity.checked_at).toLocaleString()}</p>
          </div>
        )}
        {githubIdentity && (!githubIdentity.available || !githubIdentity.actor) && (
          <div className="rounded border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs text-amber-200">
            <p className="font-medium">CCM publishing identity unavailable</p>
            <p className="mt-1 opacity-80">{githubIdentity.error || 'The backend could not confirm its GitHub identity.'}</p>
            <p className="mt-1 opacity-75">Checked {new Date(githubIdentity.checked_at).toLocaleString()}</p>
          </div>
        )}
      </section>

      <div className="bg-gray-800 rounded-lg p-5 space-y-3">
        <div className="flex items-center justify-between gap-3">
          <h4 className="text-foreground font-semibold">Review History</h4>
          <button type="button" title="Refresh review history" onClick={refreshReviewData}
            className="rounded p-2 text-gray-400 hover:bg-gray-700 hover:text-foreground">
            <RefreshCw size={15} />
          </button>
        </div>
        {runActionError && <p className="text-sm text-red-400" role="alert">{runActionError}</p>}
        {reviews.length === 0 ? (
          <p className="text-gray-500 text-sm">No reviews yet</p>
        ) : (
          <>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-gray-400 text-left border-b border-gray-700">
                    <th className="pb-2 pr-4">PR</th>
                    <th className="pb-2 pr-4">Attempt</th>
                    <th className="pb-2 pr-4">Head</th>
                    <th className="pb-2 pr-4">Title</th>
                    <th className="pb-2 pr-4">Author</th>
                    <th className="pb-2 pr-4">Status</th>
                    <th className="pb-2 pr-4">Action</th>
                    <th className="pb-2">Time</th>
                  </tr>
                </thead>
                <tbody>
                  {reviews.map((r) => {
                    const prUrl = canonicalGitHubPRUrl(r.pr_url, detail.repo_full_name, r.pr_number);
                    const snapshotState = r.is_current_snapshot
                      ?? (selectedReview?.id === r.id ? selectedReview.is_current_snapshot : undefined);
                    const headLabel = snapshotState === true
                      ? 'Current head'
                      : snapshotState === false
                        ? 'Historical head'
                        : latestReviewIdByPr.get(r.pr_number) === r.id
                          ? 'Latest review'
                          : 'Previous review';
                    return (
                    <tr key={r.id} className={`border-b border-gray-700/50 text-gray-300 cursor-pointer hover:bg-gray-700/30 focus-visible:outline focus-visible:outline-2 focus-visible:outline-indigo-400 ${
                      selectedReview?.id === r.id ? 'bg-indigo-500/10' : ''
                    }`}
                      tabIndex={0}
                      aria-label={`Open Review ${r.id}, attempt ${r.attempt}`}
                      onClick={() => void openReview(r.id)}
                      onKeyDown={(event) => {
                        if (event.key !== 'Enter' && event.key !== ' ') return;
                        event.preventDefault();
                        void openReview(r.id);
                      }}>
                      <td className="py-2 pr-4">
                        {prUrl ? (
                          <a href={prUrl} target="_blank" rel="noopener noreferrer"
                            onClick={(event) => event.stopPropagation()}
                            onKeyDown={(event) => event.stopPropagation()}
                            className="text-indigo-400 hover:text-indigo-300">#{r.pr_number}</a>
                        ) : <span>#{r.pr_number}</span>}
                      </td>
                      <td className="py-2 pr-4 text-xs">
                        <span>Attempt {r.attempt}</span>
                        {r.rerun_of_review_id != null && (
                          <span className="mt-1 block text-[10px] text-gray-500">
                            Re-run of Review #{r.rerun_of_review_id}
                          </span>
                        )}
                      </td>
                      <td className="py-2 pr-4">
                        <code className="text-xs text-gray-300" title={r.head_sha || undefined}>{shortSha(r.head_sha)}</code>
                        <span className={`mt-1 block w-fit rounded px-1.5 py-0.5 text-[10px] ${
                          snapshotState === true
                            ? 'bg-green-500/15 text-green-300'
                            : snapshotState === false
                              ? 'bg-gray-600/50 text-gray-400'
                              : 'bg-blue-500/10 text-blue-300'
                        }`}>{headLabel}</span>
                      </td>
                      <td className="py-2 pr-4 max-w-xs truncate">{r.pr_title}</td>
                      <td className="py-2 pr-4">{r.pr_author}</td>
                      <td className="py-2 pr-4">
                        <span className={`px-2 py-0.5 rounded text-xs ${reviewStatusClasses(r)}`}>
                          {reviewStatusText(r)}
                        </span>
                      </td>
                      <td className="py-2 pr-4 text-xs">{r.action_taken ? reviewActionText(r.action_taken) : '-'}</td>
                      <td className="py-2 text-xs text-gray-500">{new Date(r.created_at).toLocaleString()}</td>
                    </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
            {selectedReview && (
              <>
                <div className="mt-4 border-t border-gray-700 pt-4 space-y-3">
                <div className="flex justify-between">
                  <h5 className="font-medium text-foreground">Review Detail · PR #{selectedReview.pr_number}</h5>
                  <button className="text-xs text-gray-400" onClick={() => {
                    openReviewRequestRef.current += 1;
                    selectedReviewIdRef.current = null;
                    setSelectedReview(null);
                    setMonitorRun(null);
                    setRunActionError(null);
                    setPRMonitorDeepLink(repo.id, null);
                  }}>Close</button>
                </div>
                <div className="flex flex-wrap items-center gap-2 text-xs">
                  <span className={`rounded px-2 py-0.5 ${reviewStatusClasses(selectedReview)}`}>
                    {reviewStatusText(selectedReview)}
                  </span>
                  {selectedReview.aggregate_verdict && (
                    <span className="rounded bg-indigo-500/15 px-2 py-0.5 text-indigo-200">
                      Verdict: {statusText(selectedReview.aggregate_verdict)}
                    </span>
                  )}
                  {selectedReview.is_current_snapshot === true && (
                    <span className="rounded bg-green-500/15 px-2 py-0.5 text-green-300">Current head</span>
                  )}
                  {selectedReview.is_current_snapshot === false && (
                    <span className="rounded bg-gray-600/50 px-2 py-0.5 text-gray-400">Historical head</span>
                  )}
                  <code className="text-gray-400" title={selectedReview.head_sha || undefined}>
                    head {shortSha(selectedReview.head_sha)}
                  </code>
                </div>
                {(verdictStatusText(selectedReview) || publicationStatusText(selectedReview) || lifecycleStatusText(selectedReview)) && (
                  <div aria-label="Review outcome" className="grid gap-2 text-xs md:grid-cols-3">
                    {verdictStatusText(selectedReview) && (
                      <div className={`rounded border px-3 py-2 ${
                        selectedReview.verdict_state === 'unavailable'
                          ? inputAdmissionRejected
                            ? 'border-amber-500/30 bg-amber-500/10 text-amber-300'
                            : 'border-red-500/30 bg-red-500/10 text-red-300'
                          : selectedReview.aggregate_verdict === 'pass'
                            ? 'border-green-500/30 bg-green-500/10 text-green-300'
                            : 'border-amber-500/30 bg-amber-500/10 text-amber-300'
                      }`}>
                        <span className="font-medium">{verdictStatusText(selectedReview)}</span>
                      </div>
                    )}
                    {publicationStatusText(selectedReview) && (
                      <div className={`rounded border px-3 py-2 ${
                        selectedReview.publication_state === 'failed'
                          ? 'border-red-500/30 bg-red-500/10 text-red-300'
                          : selectedReview.publication_state === 'published'
                            ? 'border-indigo-500/30 bg-indigo-500/10 text-indigo-200'
                            : 'border-gray-600 bg-gray-700/40 text-gray-300'
                      }`}>
                        <span className="font-medium">{publicationStatusText(selectedReview)}</span>
                        {selectedReview.publication_state === 'published' && (
                          <p className="mt-1 text-[11px] opacity-80">
                            {selectedReview.published_actor
                              ? `Published by CCM as ${selectedReview.published_actor}`
                              : 'Published by CCM'}
                            {selectedReview.published_at
                              ? ` · ${new Date(selectedReview.published_at).toLocaleString()}`
                              : ''}
                            {selectedReview.github_state ? ` · GitHub ${selectedReview.github_state}` : ''}
                          </p>
                        )}
                      </div>
                    )}
                    {lifecycleStatusText(selectedReview) && (
                      <div className="rounded border border-gray-600 bg-gray-700/40 px-3 py-2 text-gray-300">
                        <span className="font-medium">{lifecycleStatusText(selectedReview)}</span>
                      </div>
                    )}
                  </div>
                )}
                <div role={noCodeVerdict ? 'alert' : undefined}
                  className={`rounded border p-3 ${
                    noCodeVerdict
                      ? inputAdmissionRejected
                        ? 'border-amber-500/30 bg-amber-500/10'
                        : 'border-red-500/30 bg-red-500/10'
                      : 'border-gray-700 bg-gray-900/40'
                  }`}>
                  <h6 className="mb-2 text-sm font-medium text-foreground">
                    {inputAdmissionRejected
                      ? 'Review not started: input too large'
                      : noCodeVerdict
                        ? 'Review system failure'
                        : 'Review summary'}
                  </h6>
                  <MarkdownContent content={selectedReviewSummary} className="text-xs text-gray-300" />
                  {inputAdmissionRejected
                    && selectedReview.error_measured != null
                    && selectedReview.error_limit != null
                    && selectedReview.error_unit && (
                    <p className="mt-2 text-xs text-amber-300">
                      Exact review input: {selectedReview.error_measured.toLocaleString()} {selectedReview.error_unit}; safe limit:{' '}
                      {selectedReview.error_limit.toLocaleString()} {selectedReview.error_unit}. No Reviewer Task was created.
                    </p>
                  )}
                  {noCodeVerdict && !inputAdmissionRejected && (
                    <p className="mt-2 text-xs text-red-300">No code verdict was produced by this failed review run.</p>
                  )}
                </div>
                {reviewerCount > 0 && (
                  <div className="flex flex-wrap gap-x-4 gap-y-1 text-xs text-gray-400">
                    {reviewerCount > 0 && <span>{reviewerCount} reviewer{reviewerCount === 1 ? '' : 's'}</span>}
                    {reviewerStatusSummary && <span>Progress: {reviewerStatusSummary}</span>}
                    {reviewerVerdictSummary && <span>Results: {reviewerVerdictSummary}</span>}
                  </div>
                )}
                {selectedReview.failure_stage && (
                  <p className={`text-xs ${inputAdmissionRejected ? 'text-amber-300' : 'text-red-300'}`}>
                    Failure stage: {inputAdmissionRejected
                      ? 'review input admission'
                      : statusText(selectedReview.failure_stage)}
                  </p>
                )}
                {selectedGitHubReviewUrl && (
                  <a
                    href={selectedGitHubReviewUrl}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="inline-flex text-xs text-indigo-300 hover:text-indigo-200"
                  >
                    Open published GitHub comment
                  </a>
                )}
                {selectedReview.can_rerun && selectedReview.head_sha && (
                  <button
                    type="button"
                    disabled={rerunPending}
                    onClick={() => void rerunSelectedReview()}
                    className="rounded border border-indigo-500/40 px-2.5 py-1.5 text-xs text-indigo-200 hover:bg-indigo-500/10 disabled:opacity-50"
                  >
                    {rerunPending ? 'Starting exact-head review…' : 'Re-run exact head'}
                  </button>
                )}
                {rerunNotice && <p role="status" className="text-xs text-emerald-300">{rerunNotice}</p>}
                {selectedReview.ci_summary && <p className="text-xs text-gray-400">CI: {selectedReview.ci_summary}</p>}
                {monitorRun && (
                  <div className="space-y-2 text-xs">
                    <details className="rounded bg-gray-900/50">
                      <summary className="cursor-pointer px-3 py-2 text-gray-400 hover:text-gray-200">
                        Advanced diagnostics
                      </summary>
                      <div className="space-y-2 border-t border-gray-700 px-3 py-3">
                        <p>Loop: {statusText(monitorRun.status)} · repair {monitorRun.repair_attempts}/{monitorRun.max_repair_attempts}</p>
                        <p>Developer Task: {monitorRun.developer_task_id ? `#${monitorRun.developer_task_id}` : 'not bound'}</p>
                        {monitorRun.pause_reason && <p className="text-yellow-500">Paused: {monitorRun.pause_reason}</p>}
                        {monitorRun.wakes.map(wake => <p key={wake.id}>Wake #{wake.id}: {statusText(wake.status)} · {statusText(wake.reason_kind)}{wake.last_error ? ` · ${wake.last_error}` : ''}</p>)}
                        {monitorRun.merge_actions?.map(action => <p key={action.id}>Merge #{action.id}: {statusText(action.status)}{action.ci_status ? ` · CI ${statusText(action.ci_status)}` : ''}{action.last_error ? ` · ${action.last_error}` : ''}</p>)}
                      </div>
                    </details>
                    {canBindDeveloper && !monitorRun.developer_task_id && (
                      <div className="flex gap-2">
                        <input value={developerTaskId} onChange={(e) => setDeveloperTaskId(e.target.value)}
                          disabled={runActionPending !== null} placeholder="Developer Task ID"
                          className="bg-gray-700 rounded px-2 py-1" />
                        <button className="bg-indigo-600 text-white rounded px-2 py-1 disabled:opacity-50"
                          disabled={runActionPending !== null || !validDeveloperTaskId}
                          onClick={() => performRunAction('bind', () => (
                            api.bindPRMonitorDeveloper(monitorRun.id, developerTaskNumber)
                          ))}>{runActionPending === 'bind' ? 'Binding…' : 'Bind'}</button>
                      </div>
                    )}
                    <div className="flex flex-wrap gap-2">
                      {canResume && (
                        <button className="bg-indigo-600 text-white rounded px-2 py-1 disabled:opacity-50"
                          disabled={runActionPending !== null}
                          onClick={() => performRunAction('resume', () => api.resumePRMonitorRun(monitorRun.id))}>
                          {runActionPending === 'resume' ? 'Resuming…' : 'Resume loop'}
                        </button>
                      )}
                      {canPause && (
                        <button className="bg-gray-700 rounded px-2 py-1 disabled:opacity-50"
                          disabled={runActionPending !== null}
                          onClick={() => performRunAction('pause', () => api.pausePRMonitorRun(monitorRun.id))}>
                          {runActionPending === 'pause' ? 'Pausing…' : 'Pause loop'}
                        </button>
                      )}
                      {canChangeBinding && monitorRun.developer_task_id && (
                        <button className="bg-gray-700 rounded px-2 py-1 disabled:opacity-50"
                          disabled={runActionPending !== null}
                          onClick={() => performRunAction('unbind', () => api.unbindPRMonitorDeveloper(monitorRun.id))}>
                          {runActionPending === 'unbind' ? 'Unbinding…' : 'Unbind Developer'}
                        </button>
                      )}
                      {canMerge && (
                        <button className="bg-green-700 text-white rounded px-2 py-1 disabled:opacity-50"
                          disabled={runActionPending !== null}
                          onClick={() => performRunAction('merge', () => api.mergePRMonitorRun(monitorRun.id))}>
                          {runActionPending === 'merge' ? 'Merging…' : 'Merge PR'}
                        </button>
                      )}
                      {baseUpdateRequired && monitorRun.current_head_sha && (
                        <button className="inline-flex items-center gap-1 bg-amber-700 text-white rounded px-2 py-1 disabled:opacity-50"
                          disabled={runActionPending !== null || branchUpdateRequested}
                          onClick={() => performRunAction('update-branch', async () => {
                            await api.updatePRMonitorBranch(monitorRun.id, monitorRun.current_head_sha);
                            return api.getPRMonitorRun(monitorRun.id);
                          })}>
                          <RefreshCw size={13} className={runActionPending === 'update-branch' ? 'animate-spin' : ''} />
                          {runActionPending === 'update-branch' || branchUpdateRequested
                            ? 'Waiting for branch update…'
                            : 'Update branch & re-review'}
                        </button>
                      )}
                    </div>
                  </div>
                )}
                {selectedReview.ci_details?.observed.map((item) => (
                  <p key={`${item.kind}:${item.name}:${item.app_slug}`} className="text-xs text-gray-500">
                    {item.state} · {item.name} · {item.app_slug}
                  </p>
                ))}
                {reviewerRuns.length === 0 && selectedReview.status === 'waiting_ci' && (
                  <p className="text-xs text-gray-500">Reviewer panel has not started yet.</p>
                )}
                </div>
                {reviewerRuns.length > 0 && <h6 className="text-sm font-medium text-foreground">Reviewer results</h6>}
                {[...reviewerRuns].map(run => (
                  <div key={run.id} className="rounded bg-gray-900/40 p-3">
                    <div className="flex flex-wrap items-center justify-between gap-2 text-sm">
                      <span className="font-medium text-gray-200">
                        {reviewerRoleText(run.role)}
                      </span>
                      <span className={STATUS_COLORS[run.status] || 'text-gray-400'}>{statusText(run.status)}</span>
                    </div>
                    {run.result_body && (
                      <MarkdownContent content={run.result_body} className="mt-2 text-xs text-gray-300" />
                    )}
                    {run.outcome_kind === 'infrastructure_error' && !run.result_body && (
                      <p className="mt-2 text-xs text-red-300">This reviewer did not produce a code verdict.</p>
                    )}
                    {run.error_message && <p className="text-xs text-red-400 mt-1">{run.error_message}</p>}
                    {[...run.findings].sort((a, b) => (
                      (FINDING_SEVERITY_ORDER[a.severity] ?? 9) - (FINDING_SEVERITY_ORDER[b.severity] ?? 9)
                    )).map(finding => {
                      const findingThreadUrl = canonicalGitHubFindingCommentUrl(
                        finding.github_comment_url,
                        detail.repo_full_name,
                        selectedReview.pr_number,
                        finding.github_comment_id,
                      );
                      return (
                      <div key={finding.id} className="mt-3 border-l-2 border-orange-500 pl-3 text-xs space-y-1">
                        <p className="text-orange-300">[{finding.severity}] {finding.path}{finding.line ? `:${finding.line}` : ''} — {finding.title}</p>
                        <p className="text-gray-300">Evidence: {finding.evidence}</p>
                        <p className="text-gray-400">Impact: {finding.impact}</p>
                        <p className="text-gray-400">Required fix: {finding.required_fix}</p>
                        <p className="text-gray-400">Test: {finding.test}</p>
                        <p className="text-gray-500">
                          Thread: {findingThreadUrl ? (
                            <a className="text-indigo-400 hover:text-indigo-300" href={findingThreadUrl}
                              target="_blank" rel="noopener noreferrer">{finding.thread_status}</a>
                          ) : finding.thread_status}
                        </p>
                        {finding.thread_error && <p className="text-yellow-500">{finding.thread_error}</p>}
                        <FindingActions
                          finding={finding}
                          currentSnapshot={selectedReview.is_current_snapshot !== false}
                          onChanged={refreshSelectedReview}
                        />
                        <FindingRebuttalForm
                          finding={finding}
                          onSubmitted={refreshSelectedReview}
                        />
                      </div>
                      );
                    })}
                  </div>
                ))}
              </>
            )}
            <div className="flex gap-2 pt-2">
              <button onClick={() => setPage(p => Math.max(1, p - 1))} disabled={page === 1}
                className="px-3 py-1 text-xs bg-gray-700 text-gray-300 rounded disabled:opacity-50">Prev</button>
              <span className="text-xs text-gray-400 py-1">Page {page}</span>
              <button onClick={() => setPage(p => p + 1)} disabled={reviews.length < 20}
                className="px-3 py-1 text-xs bg-gray-700 text-gray-300 rounded disabled:opacity-50">Next</button>
            </div>
          </>
        )}
      </div>
    </div>
  );
}

export function PRMonitorPage() {
  const [repos, setRepos] = useState<MonitoredRepo[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [showModal, setShowModal] = useState(false);
  const [selectedRepo, setSelectedRepo] = useState<MonitoredRepo | null>(null);
  const [initialDeepLink, setInitialDeepLink] = useState(parsePRMonitorDeepLink);
  const deepLinkAppliedRef = useRef(false);
  const isAdmin = currentUserIsAdmin();

  useEffect(() => {
    const handleHashChange = () => {
      const route = window.location.hash.replace(/^#\/?/, '').split('?', 1)[0];
      if (route !== 'pr-monitor') return;
      const deepLink = parsePRMonitorDeepLink();
      deepLinkAppliedRef.current = false;
      setInitialDeepLink(deepLink);
      // A pushState/hashchange navigation must never leave the prior repository
      // rendered while the new (or invalid) deep link is being resolved.
      setSelectedRepo(null);
    };
    window.addEventListener('hashchange', handleHashChange);
    return () => window.removeEventListener('hashchange', handleHashChange);
  }, []);

  const refresh = useCallback(async () => {
    try {
      const data = await api.getMonitoredRepos();
      setRepos(data);
      setError(null);
    } catch (e) {
      setError(String(e));
    }
  }, []);

  useEffect(() => {
    let active = true;
    api.getMonitoredRepos()
      .then((data) => {
        if (!active) return;
        setRepos(data);
        setError(null);
        if (initialDeepLink && !deepLinkAppliedRef.current) {
          deepLinkAppliedRef.current = true;
          const linkedRepo = data.find((repo) => repo.id === initialDeepLink.repoId);
          if (linkedRepo) setSelectedRepo(linkedRepo);
          else {
            setSelectedRepo(null);
            setError(`Monitored repository #${initialDeepLink.repoId} was not found`);
          }
        }
      })
      .catch((caught) => {
        if (active) setError(String(caught));
      });
    return () => { active = false; };
  }, [initialDeepLink]);

  const handleToggle = async (repo: MonitoredRepo) => {
    try {
      await api.toggleMonitoredRepo(repo.id);
      await refresh();
    } catch (caught) { setError(String(caught)); }
  };

  const handleDelete = async (repo: MonitoredRepo) => {
    if (!confirm(`Delete monitoring for ${repo.repo_full_name}? This will also delete all review history.`)) return;
    try {
      await api.deleteMonitoredRepo(repo.id);
      await refresh();
    } catch (caught) { setError(String(caught)); }
  };

  if (selectedRepo) {
    return (
      <div className="p-4 md:p-6 max-w-6xl mx-auto">
        <RepoDetail
          key={selectedRepo.id}
          repo={selectedRepo}
          initialReviewId={initialDeepLink?.repoId === selectedRepo.id ? initialDeepLink.reviewId : null}
          onBack={() => {
            setInitialDeepLink(null);
            setPRMonitorDeepLink();
            setSelectedRepo(null);
            refresh();
          }}
          onRefresh={refresh}
        />
      </div>
    );
  }

  return (
    <div className="p-4 md:p-6 max-w-6xl mx-auto">
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-2">
          <GitPullRequest size={22} className="text-indigo-400" />
          <h2 className="text-xl font-bold text-foreground">PR Monitor</h2>
        </div>
        {isAdmin && (
          <button onClick={() => setShowModal(true)}
            className="flex items-center gap-1 px-4 py-2 text-sm bg-indigo-600 text-white rounded hover:bg-indigo-500">
            <Plus size={16} /> Add Repository
          </button>
        )}
      </div>

      {error && <p className="text-red-400 text-sm mb-4">{error}</p>}

      {repos.length === 0 ? (
        <div className="text-center py-16 text-gray-500">
          <GitPullRequest size={48} className="mx-auto mb-4 opacity-30" />
          <p>No repositories monitored yet</p>
          <p className="text-sm mt-1">Add a repository to start auto-reviewing PRs</p>
        </div>
      ) : (
        <div className="bg-gray-800 rounded-lg overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-gray-400 text-left border-b border-gray-700">
                <th className="px-4 py-3">Repository</th>
                <th className="px-4 py-3">Status</th>
                <th className="px-4 py-3">Merge Policy</th>
                <th className="px-4 py-3">Enabled</th>
                <th className="px-4 py-3">Created</th>
                <th className="px-4 py-3"></th>
              </tr>
            </thead>
            <tbody>
              {repos.map(repo => (
                <tr key={repo.id} className="border-b border-gray-700/50 hover:bg-gray-700/30 cursor-pointer text-gray-300"
                  tabIndex={0}
                  aria-label={`Open monitored repository ${repo.repo_full_name}`}
                  onClick={() => {
                    setInitialDeepLink(null);
                    setPRMonitorDeepLink(repo.id, null);
                    setSelectedRepo(repo);
                  }}
                  onKeyDown={(event) => {
                    if (event.target !== event.currentTarget) return;
                    if (event.key !== 'Enter' && event.key !== ' ') return;
                    event.preventDefault();
                    setInitialDeepLink(null);
                    setPRMonitorDeepLink(repo.id, null);
                    setSelectedRepo(repo);
                  }}>
                  <td className="px-4 py-3 font-medium text-foreground">{repo.repo_full_name}</td>
                  <td className="px-4 py-3">
                    <span className={`inline-block w-2 h-2 rounded-full mr-2 ${repo.status === 'active' ? 'bg-green-400' : 'bg-red-400'}`} />
                    {repo.status}
                  </td>
                  <td className="px-4 py-3">
                    <span className={`px-2 py-0.5 rounded text-xs ${
                      repo.auto_merge
                        ? 'bg-green-500/20 text-green-400'
                        : 'bg-gray-600/50 text-gray-400'
                    }`}>
                      {mergePolicyLabel(repo)}
                    </span>
                  </td>
                  <td className="px-4 py-3" onClick={(e) => e.stopPropagation()}>
                    <button aria-label={`${repo.enabled ? 'Disable' : 'Enable'} monitoring for ${repo.repo_full_name}`}
                      onClick={() => handleToggle(repo)} className="text-gray-400 hover:text-foreground">
                      {repo.enabled ? <ToggleRight size={22} className="text-green-400" /> : <ToggleLeft size={22} />}
                    </button>
                  </td>
                  <td className="px-4 py-3 text-xs text-gray-500">{new Date(repo.created_at).toLocaleDateString()}</td>
                  <td className="px-4 py-3" onClick={(e) => e.stopPropagation()}>
                    <button aria-label={`Delete monitoring for ${repo.repo_full_name}`}
                      onClick={() => handleDelete(repo)} className="text-gray-400 hover:text-red-400">
                      <Trash2 size={16} />
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {showModal && (
        <AddRepoModal isAdmin={isAdmin} onClose={() => setShowModal(false)} onSaved={refresh} />
      )}
    </div>
  );
}
