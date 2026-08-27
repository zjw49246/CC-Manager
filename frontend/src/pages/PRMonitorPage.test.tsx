import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, cleanup, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { PRMonitorPage } from './PRMonitorPage';
import type { MonitoredRepo, PRMonitorRun, PRReview } from '../api/client';

vi.mock('../hooks/useWebSocket', () => ({
  useWebSocket: vi.fn(),
}));

vi.mock('../api/client', () => ({
  api: {
    config: vi.fn(),
    listWorkers: vi.fn(),
    getMonitoredRepos: vi.fn(),
    getMonitoredRepo: vi.fn(),
    createMonitoredRepo: vi.fn(),
    updateMonitoredRepo: vi.fn(),
    deleteMonitoredRepo: vi.fn(),
    toggleMonitoredRepo: vi.fn(),
    regenerateSecret: vi.fn(),
    getRepoReviews: vi.fn(),
    getReviewDetail: vi.fn(),
    rerunPRReview: vi.fn(),
    getPRMonitorGitHubIdentity: vi.fn(),
    getPRMonitorRun: vi.fn(),
    bindPRMonitorDeveloper: vi.fn(),
    pausePRMonitorRun: vi.fn(),
    resumePRMonitorRun: vi.fn(),
    unbindPRMonitorDeveloper: vi.fn(),
    submitPRFindingRebuttal: vi.fn(),
    mergePRMonitorRun: vi.fn(),
    enqueuePRMonitorMerge: vi.fn(),
    getWebhookInfo: vi.fn(),
  },
}));

import { api } from '../api/client';
import { useWebSocket } from '../hooks/useWebSocket';

const baseRepo: MonitoredRepo = {
  id: 1,
  repo_full_name: 'acme/widgets',
  project_id: 10,
  enabled: true,
  auto_merge: false,
  webhook_secret: 'secr***',
  provider: 'codex',
  review_model: null,
  review_effort: null,
  review_mode: 'panel',
  wait_for_ci: true,
  required_checks: [{ kind: 'check_run', name: 'tests', app_slug: 'github-actions' }],
  auto_repair: true,
  max_repair_attempts: 3,
  merge_queue_mode: 'shadow',
  default_branch: 'main',
  allowed_authors: [],
  status: 'active',
  error_message: null,
  created_at: '2026-08-01T00:00:00Z',
  updated_at: '2026-08-01T00:00:00Z',
};

function reviewFixture(overrides: Partial<PRReview> = {}): PRReview {
  return {
    id: 11,
    attempt: 1,
    rerun_of_review_id: null,
    monitor_run_id: 21,
    repo_id: baseRepo.id,
    pr_number: 42,
    base_sha: 'base-sha',
    head_sha: 'head-sha',
    delivery_id: 'delivery-1',
    pr_title: 'Harden the widget loop',
    pr_author: 'developer',
    pr_url: 'https://github.com/acme/widgets/pull/42',
    task_id: null,
    status: 'changes_required',
    review_summary: 'One exact-head review is being tracked.',
    action_taken: null,
    ci_status: 'failure',
    ci_summary: 'Failed: tests',
    ci_details: {
      head_sha: 'head-sha',
      required: [{ kind: 'check_run', name: 'tests', app_slug: 'github-actions' }],
      observed: [{ kind: 'check_run', name: 'tests', app_slug: 'github-actions', state: 'failure' }],
    },
    reviewer_runs: [],
    created_at: '2026-08-02T00:00:00Z',
    completed_at: null,
    ...overrides,
  };
}

function runFixture(overrides: Partial<PRMonitorRun> = {}): PRMonitorRun {
  return {
    id: 21,
    repo_id: baseRepo.id,
    pr_number: 42,
    status: 'waiting_for_fix',
    current_head_sha: 'head-sha',
    developer_task_id: null,
    repair_attempts: 0,
    max_repair_attempts: 3,
    pause_reason: null,
    wakes: [],
    merge_actions: [],
    ...overrides,
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((promiseResolve, promiseReject) => {
    resolve = promiseResolve;
    reject = promiseReject;
  });
  return { promise, resolve, reject };
}

function selectFollowingLabel(label: string): HTMLSelectElement {
  const select = screen.getByText(label).parentElement?.querySelector('select');
  if (!(select instanceof HTMLSelectElement)) throw new Error(`No select found for ${label}`);
  return select;
}

async function openRepo(user: ReturnType<typeof userEvent.setup>, repo = baseRepo) {
  vi.mocked(api.getMonitoredRepos).mockResolvedValue([repo]);
  vi.mocked(api.getMonitoredRepo).mockResolvedValue(repo);
  render(<PRMonitorPage />);
  await user.click(await screen.findByText(repo.repo_full_name));
  await screen.findByRole('button', { name: 'Save Changes' });
}

async function openReview(
  user: ReturnType<typeof userEvent.setup>,
  review: PRReview,
  run: PRMonitorRun,
  repo: MonitoredRepo = baseRepo,
) {
  vi.mocked(api.getRepoReviews).mockResolvedValue([review]);
  vi.mocked(api.getReviewDetail).mockResolvedValue(review);
  vi.mocked(api.getPRMonitorRun).mockResolvedValue(run);
  await openRepo(user, repo);
  await user.click(await screen.findByText(review.pr_title));
  await screen.findByText(`Review Detail · PR #${review.pr_number}`);
}

describe('PRMonitorPage safety controls', () => {
  beforeEach(() => {
    window.history.replaceState(null, '', '#/pr-monitor');
    localStorage.setItem('cc_user', JSON.stringify({ id: 1, role: 'admin' }));
    vi.mocked(api.config).mockResolvedValue({
      default_provider: 'codex',
      provider_options: ['claude', 'codex'],
      default_model: 'claude-opus-4-6',
      model_options: ['default', 'claude-opus-4-6'],
      default_codex_model: 'gpt-5.6-sol',
      codex_model_options: ['default', 'gpt-5.6-sol'],
      default_effort: 'medium',
      effort_options: ['low', 'medium', 'high'],
      claude_model_efforts: {},
      claude_model_context_windows: {},
      codex_effort_options: ['low', 'medium', 'high'],
      codex_model_efforts: {},
      codex_model_service_tiers: {},
    });
    vi.mocked(api.listWorkers).mockResolvedValue([]);
    vi.mocked(api.getMonitoredRepos).mockResolvedValue([]);
    vi.mocked(api.getMonitoredRepo).mockResolvedValue(baseRepo);
    vi.mocked(api.createMonitoredRepo).mockResolvedValue({
      ...baseRepo,
      webhook_secret: 'newly-created-raw-secret',
    });
    vi.mocked(api.updateMonitoredRepo).mockResolvedValue(baseRepo);
    vi.mocked(api.deleteMonitoredRepo).mockResolvedValue({ ok: true });
    vi.mocked(api.toggleMonitoredRepo).mockResolvedValue(baseRepo);
    vi.mocked(api.regenerateSecret).mockResolvedValue({
      ...baseRepo,
      webhook_secret: 'newly-rotated-raw-secret',
    });
    vi.mocked(api.getRepoReviews).mockResolvedValue([]);
    vi.mocked(api.getReviewDetail).mockResolvedValue(reviewFixture());
    vi.mocked(api.rerunPRReview).mockResolvedValue({
      id: 12,
      attempt: 2,
      rerun_of_review_id: 11,
      monitor_run_id: 21,
      status: 'queued',
      head_sha: 'head-sha',
    });
    vi.mocked(api.getPRMonitorGitHubIdentity).mockResolvedValue({
      available: true,
      actor: 'ccm-publisher',
      error: null,
      checked_at: '2026-08-16T01:00:00Z',
    });
    vi.mocked(api.getPRMonitorRun).mockResolvedValue(runFixture());
    vi.mocked(api.bindPRMonitorDeveloper).mockResolvedValue(runFixture({ developer_task_id: 99 }));
    vi.mocked(api.pausePRMonitorRun).mockResolvedValue(runFixture({ status: 'paused' }));
    vi.mocked(api.resumePRMonitorRun).mockResolvedValue(runFixture());
    vi.mocked(api.unbindPRMonitorDeveloper).mockResolvedValue(runFixture());
    vi.mocked(api.mergePRMonitorRun).mockResolvedValue(runFixture({ status: 'merge_pending' }));
    vi.mocked(api.getWebhookInfo).mockResolvedValue({ webhook_url: '/api/github/webhook' });
    vi.spyOn(window, 'confirm').mockReturnValue(true);
  });

  afterEach(() => {
    cleanup();
    localStorage.clear();
    vi.restoreAllMocks();
    vi.clearAllMocks();
  });

  it('keeps the add-repository dialog scrollable and dismissible within the viewport', async () => {
    const user = userEvent.setup();
    render(<PRMonitorPage />);
    await user.click(await screen.findByRole('button', { name: 'Add Repository' }));

    const dialog = screen.getByRole('dialog', { name: 'Add Repository' });
    const form = dialog.querySelector('form');
    expect(dialog).toHaveClass('max-h-[calc(100dvh-2rem)]', 'overflow-hidden');
    expect(form).toHaveClass('min-h-0', 'overflow-y-auto', 'overscroll-contain');
    expect(document.body.style.overflow).toBe('hidden');

    await user.keyboard('{Escape}');
    expect(screen.queryByRole('dialog', { name: 'Add Repository' })).not.toBeInTheDocument();
    expect(document.body.style.overflow).toBe('');

    await user.click(screen.getByRole('button', { name: 'Add Repository' }));
    const reopenedDialog = screen.getByRole('dialog', { name: 'Add Repository' });
    await user.click(reopenedDialog.parentElement!);
    expect(screen.queryByRole('dialog', { name: 'Add Repository' })).not.toBeInTheDocument();
  });

  it('hides projectless PR Monitor creation from members', async () => {
    localStorage.setItem('cc_user', JSON.stringify({ id: 9, role: 'member' }));

    render(<PRMonitorPage />);

    await waitFor(() => expect(api.getMonitoredRepos).toHaveBeenCalled());
    expect(screen.queryByRole('button', { name: /Add Repository/i })).not.toBeInTheDocument();
  });

  it('fails closed if administrator identity changes while the add dialog is open', async () => {
    const user = userEvent.setup();
    render(<PRMonitorPage />);

    await user.click(await screen.findByRole('button', { name: /Add Repository/i }));
    localStorage.setItem('cc_user', JSON.stringify({ id: 9, role: 'member' }));
    await user.type(screen.getByPlaceholderText('owner/repo'), 'acme/new-repo');
    await user.click(screen.getByRole('button', { name: 'Add' }));

    expect(await screen.findByText('Only administrators can add a PR Monitor repository.')).toBeInTheDocument();
    expect(api.createMonitoredRepo).not.toHaveBeenCalled();
  });

  it('defaults new repositories to one bounded reviewer Task', async () => {
    const user = userEvent.setup();
    render(<PRMonitorPage />);
    await user.click(await screen.findByRole('button', { name: 'Add Repository' }));

    expect(selectFollowingLabel('Review Harness')).toHaveValue('single');
    expect(screen.getByText('One review Task with a bounded PR context.')).toBeInTheDocument();
    expect(screen.queryByText(/roughly 3× the model work/)).not.toBeInTheDocument();
    expect(screen.getByLabelText('Wait for exact-head CI')).toBeDisabled();

    await user.type(screen.getByPlaceholderText('owner/repo'), 'acme/default-single');
    await user.click(screen.getByRole('button', { name: 'Add' }));

    await waitFor(() => expect(api.createMonitoredRepo).toHaveBeenCalledWith(
      expect.objectContaining({
        repo_full_name: 'acme/default-single',
        review_mode: 'single',
        wait_for_ci: false,
        required_checks: [],
      }),
    ));
  });

  it('allows panel direct auto-merge with exact-head CI', async () => {
    const user = userEvent.setup();
    render(<PRMonitorPage />);
    await user.click(await screen.findByRole('button', { name: 'Add Repository' }));

    await user.selectOptions(selectFollowingLabel('Review Harness'), 'panel');
    expect(screen.getByText(/three independent review Tasks/)).toHaveTextContent('roughly 3×');
    const autoMerge = screen.getByLabelText('Direct auto-merge after review and exact-head gates pass');
    expect(autoMerge).toBeEnabled();
    expect(autoMerge).not.toBeChecked();
    await user.click(autoMerge);
    expect(autoMerge).toBeChecked();

    await user.type(screen.getByPlaceholderText('owner/repo'), 'acme/new-repo');
    await user.type(
      screen.getByPlaceholderText(/check_run,tests,github-actions/),
      'check_run,tests,github-actions',
    );
    await user.click(screen.getByRole('button', { name: 'Add' }));

    await waitFor(() => {
      expect(api.createMonitoredRepo).toHaveBeenCalledWith(expect.objectContaining({
        repo_full_name: 'acme/new-repo',
        review_mode: 'panel',
        auto_merge: true,
        auto_repair: false,
        wait_for_ci: true,
        merge_queue_mode: 'manual',
        required_checks: [{ kind: 'check_run', name: 'tests', app_slug: 'github-actions' }],
      }));
    });
    expect(await screen.findByText('newly-created-raw-secret')).toBeInTheDocument();
    expect(screen.getByRole('alert')).toHaveTextContent('will not be shown again');
  });

  it('creates a Claude PR Monitor when Claude is the only available provider', async () => {
    const configRequest = deferred<Awaited<ReturnType<typeof api.config>>>();
    vi.mocked(api.config).mockReturnValue(configRequest.promise);
    const claudeOnlyConfig = {
      default_provider: 'codex',
      provider_options: ['claude'],
      default_model: 'claude-opus-4-6',
      model_options: ['default', 'claude-opus-4-6'],
      default_codex_model: 'gpt-5.6-sol',
      codex_model_options: ['default', 'gpt-5.6-sol'],
      default_effort: 'medium',
      effort_options: ['low', 'medium', 'high'],
      claude_model_efforts: {},
      claude_model_context_windows: {},
      codex_effort_options: ['low', 'medium', 'high'],
      codex_model_efforts: {},
      codex_model_service_tiers: {},
    } as Awaited<ReturnType<typeof api.config>>;
    const user = userEvent.setup();
    render(<PRMonitorPage />);
    await user.click(await screen.findByRole('button', { name: 'Add Repository' }));

    await user.selectOptions(selectFollowingLabel('Review Harness'), 'single');
    await user.type(screen.getByPlaceholderText('owner/repo'), 'acme/claude-only');
    expect(screen.getByRole('button', { name: 'Add' })).toBeDisabled();
    await act(async () => configRequest.resolve(claudeOnlyConfig));

    const provider = selectFollowingLabel('Provider');
    await waitFor(() => expect(provider).toHaveValue('claude'));
    expect(provider.options).toHaveLength(1);
    await user.click(screen.getByRole('button', { name: 'Add' }));

    await waitFor(() => expect(api.createMonitoredRepo).toHaveBeenCalledWith(
      expect.objectContaining({ provider: 'claude' }),
    ));
  });

  it('allows direct auto-merge with the single-reviewer harness', async () => {
    const user = userEvent.setup();
    render(<PRMonitorPage />);
    await user.click(await screen.findByRole('button', { name: 'Add Repository' }));

    await user.selectOptions(selectFollowingLabel('Review Harness'), 'single');
    const autoMerge = screen.getByLabelText('Direct auto-merge after review and exact-head gates pass');
    expect(autoMerge).toBeEnabled();
    await user.click(autoMerge);
    await user.type(screen.getByPlaceholderText('owner/repo'), 'acme/single-review');
    await user.click(screen.getByRole('button', { name: 'Add' }));

    await waitFor(() => {
      expect(api.createMonitoredRepo).toHaveBeenCalledWith(expect.objectContaining({
        repo_full_name: 'acme/single-review',
        review_mode: 'single',
        auto_merge: true,
        auto_repair: false,
        wait_for_ci: false,
        merge_queue_mode: 'manual',
        required_checks: [],
      }));
    });
  });

  it('keeps stored secrets masked and reveals only the rotated value', async () => {
    const user = userEvent.setup();
    await openRepo(user);

    expect(await screen.findByText('secr***')).toBeInTheDocument();
    expect(screen.getByTitle('Rotate to reveal a new secret')).toBeDisabled();

    await user.click(screen.getByTitle('Regenerate secret'));

    expect(await screen.findByText('newly-rotated-raw-secret')).toBeInTheDocument();
    expect(screen.getByTitle('Copy newly generated secret')).toBeEnabled();
  });

  it('preserves auto-merge when editing a panel repository', async () => {
    const user = userEvent.setup();
    const autoRepo = { ...baseRepo, auto_merge: true, merge_queue_mode: 'manual' as const };
    await openRepo(user, autoRepo);

    const autoMerge = screen.getByLabelText('Direct auto-merge after review and exact-head gates pass');
    expect(autoMerge).toBeEnabled();
    expect(autoMerge).toBeChecked();
    expect(screen.getByText(/ON: CCM confirms the exact-head merge/)).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Save Changes' }));

    await waitFor(() => {
      expect(api.updateMonitoredRepo).toHaveBeenCalledWith(autoRepo.id, expect.objectContaining({
        review_mode: 'panel',
        auto_merge: true,
        auto_repair: true,
        wait_for_ci: true,
        merge_queue_mode: 'manual',
      }));
    });
  });

  it('shows only direct merge policies', async () => {
    vi.mocked(api.getMonitoredRepos).mockResolvedValue([
      { ...baseRepo, id: 1, repo_full_name: 'acme/shadow' },
      { ...baseRepo, id: 2, repo_full_name: 'acme/direct', auto_merge: true, merge_queue_mode: 'manual' },
    ]);

    render(<PRMonitorPage />);

    expect(await screen.findByText('Merge Policy')).toBeInTheDocument();
    expect(screen.getByText('MANUAL')).toBeInTheDocument();
    expect(screen.getByText('AUTO')).toBeInTheDocument();
    expect(screen.queryByText('QUEUE AUTO')).not.toBeInTheDocument();
  });

  it('shows the CCM backend publishing identity between Webhook and Review History', async () => {
    const user = userEvent.setup();
    await openRepo(user);

    const webhook = screen.getByText('Webhook Configuration');
    const identity = await screen.findByText('GitHub Publishing Identity');
    const history = screen.getByText('Review History');
    expect(webhook.compareDocumentPosition(identity) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(identity.compareDocumentPosition(history) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(screen.getByText('CCM will publish as ccm-publisher')).toBeInTheDocument();
    expect(screen.getByText(/independent of Codex authentication/i)).toBeInTheDocument();
    expect(screen.getByText(/browser or connector/i)).toBeInTheDocument();
    expect(api.getPRMonitorGitHubIdentity).toHaveBeenCalledWith(baseRepo.id, false);
  });

  it('renders loading, unavailable, HTTP error, and manual identity refresh states', async () => {
    const user = userEvent.setup();
    const first = deferred<Awaited<ReturnType<typeof api.getPRMonitorGitHubIdentity>>>();
    vi.mocked(api.getPRMonitorGitHubIdentity)
      .mockReturnValueOnce(first.promise)
      .mockRejectedValueOnce(new Error('HTTP 503'))
      .mockResolvedValueOnce({
        available: true,
        actor: 'restored-publisher',
        error: null,
        checked_at: '2026-08-16T02:05:00Z',
      });

    await openRepo(user);
    expect(screen.getByText('Checking the CCM backend identity…')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Checking…' })).toBeDisabled();

    first.resolve({
      available: false,
      actor: null,
      error: 'GitHub CLI is not authenticated',
      checked_at: '2026-08-16T02:00:00Z',
    });
    expect(await screen.findByText('CCM publishing identity unavailable')).toBeInTheDocument();
    expect(screen.getByText('GitHub CLI is not authenticated')).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Refresh identity' }));
    expect(await screen.findByText(/Identity request failed: HTTP 503/)).toBeInTheDocument();
    expect(api.getPRMonitorGitHubIdentity).toHaveBeenLastCalledWith(baseRepo.id, true);

    await user.click(screen.getByRole('button', { name: 'Refresh identity' }));
    expect(await screen.findByText('CCM will publish as restored-publisher')).toBeInTheDocument();
    expect(api.getPRMonitorGitHubIdentity).toHaveBeenLastCalledWith(baseRepo.id, true);
  });

  it('does not offer force-refreshing the backend identity to members', async () => {
    localStorage.setItem('cc_user', JSON.stringify({ id: 2, role: 'member' }));
    const user = userEvent.setup();
    await openRepo(user);

    expect(await screen.findByText('CCM will publish as ccm-publisher')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Refresh identity' })).not.toBeInTheDocument();
    expect(api.getPRMonitorGitHubIdentity).toHaveBeenCalledWith(baseRepo.id, false);
  });

  it('ignores a stale identity response after switching repositories', async () => {
    const repoTwo = { ...baseRepo, id: 2, repo_full_name: 'acme/other' };
    const first = deferred<Awaited<ReturnType<typeof api.getPRMonitorGitHubIdentity>>>();
    vi.mocked(api.getMonitoredRepos).mockResolvedValue([baseRepo, repoTwo]);
    vi.mocked(api.getMonitoredRepo).mockImplementation(async (id: number) => (
      id === repoTwo.id ? repoTwo : baseRepo
    ));
    vi.mocked(api.getPRMonitorGitHubIdentity).mockImplementation(async (repoId: number) => {
      if (repoId === baseRepo.id) return first.promise;
      return {
        available: true,
        actor: 'repo-two-publisher',
        error: null,
        checked_at: '2026-08-16T03:00:00Z',
      };
    });
    window.history.replaceState(null, '', '#/pr-monitor?repo=1');
    render(<PRMonitorPage />);
    await screen.findByText('Checking the CCM backend identity…');

    window.location.hash = '#/pr-monitor?repo=2';
    expect(await screen.findByText('CCM will publish as repo-two-publisher')).toBeInTheDocument();
    first.resolve({
      available: true,
      actor: 'stale-repo-one-publisher',
      error: null,
      checked_at: '2026-08-16T03:01:00Z',
    });
    await act(async () => { await Promise.resolve(); });

    expect(screen.queryByText('CCM will publish as stale-repo-one-publisher')).not.toBeInTheDocument();
    expect(screen.getByText('CCM will publish as repo-two-publisher')).toBeInTheDocument();
  });

  it('renders the backend human projection and reviewer summaries without canned advice', async () => {
    const user = userEvent.setup();
    const review = reviewFixture({
      task_ids: [301, 302, 303],
      task_id: null,
      display_status: 'Review system failed',
      display_summary: 'The Senior reviewer could not start, so this panel has no complete code verdict.',
      outcome_kind: 'infrastructure_error',
      aggregate_verdict: null,
      reviewer_count: 3,
      reviewer_status_counts: { completed: 2, error: 1 },
      reviewer_verdict_counts: { pass: 1, changes_required: 1 },
      reviewer_runs: [{
        id: 31,
        role: 'principal_engineer',
        task_id: 301,
        provider: 'codex',
        model: 'gpt-5.6-sol',
        effort: 'high',
        status: 'completed',
        verdict: 'changes_required',
        result_body: 'Principal found **one correctness issue** in the changed request path.',
        outcome_kind: 'review_result',
        error_message: null,
        created_at: '2026-08-02T00:00:00Z',
        completed_at: '2026-08-02T00:05:00Z',
        findings: [],
      }],
    });
    await openReview(user, review, runFixture());

    expect(screen.getAllByText('Review system failed').length).toBeGreaterThan(0);
    expect(screen.getByRole('alert')).toHaveTextContent('Senior reviewer could not start');
    expect(screen.getByRole('alert')).toHaveTextContent('No code verdict was produced');
    expect(screen.getByText('Principal engineer')).toBeInTheDocument();
    expect(screen.getByText('one correctness issue').tagName).toBe('STRONG');
    expect(screen.queryByText('Task #301')).not.toBeInTheDocument();
    expect(screen.queryByText('Task #302')).not.toBeInTheDocument();
    expect(screen.queryByText('Task #303')).not.toBeInTheDocument();
    expect(screen.getByText(/3 reviewers/)).toBeInTheDocument();
    expect(screen.getByText(/Progress: 2 completed · 1 review failed/)).toBeInTheDocument();
    expect(screen.queryByText(/优先处理高风险和中风险问题/)).not.toBeInTheDocument();
    expect(screen.queryByText(/应用修复前下载并检查 Diff/)).not.toBeInTheDocument();
  });

  it('renders input-size admission rejection as an actionable amber result, not a system failure', async () => {
    const user = userEvent.setup();
    const review = reviewFixture({
      status: 'error',
      display_status: 'Review input too large',
      display_summary: 'The exact PR review input exceeded the configured safe model limit.',
      outcome_kind: 'infrastructure_error',
      verdict_state: 'unavailable',
      aggregate_verdict: null,
      publication_state: 'not_applicable',
      lifecycle_state: 'reviewing',
      failure_stage: 'reviewer',
      error_category: 'unsupported_input_size',
      error_measured: 786_433,
      error_limit: 786_432,
      error_unit: 'characters',
      reviewer_count: 0,
      reviewer_runs: [],
    });

    await openReview(user, review, runFixture());

    const result = screen.getByRole('alert');
    expect(result).toHaveClass('border-amber-500/30');
    expect(result).toHaveTextContent('Review not started: input too large');
    expect(result).toHaveTextContent('786,433 characters');
    expect(result).toHaveTextContent('safe limit: 786,432 characters');
    expect(result).toHaveTextContent('No Reviewer Task was created');
    expect(screen.getByText('Failure stage: review input admission')).toHaveClass('text-amber-300');
    expect(screen.queryByText('Review system failure')).not.toBeInTheDocument();
    expect(screen.queryByText(/No code verdict was produced/)).not.toBeInTheDocument();
  });

  it('uses publication-action wording that never presents approved_merged as Approved', async () => {
    const user = userEvent.setup();
    const review = reviewFixture({
      status: 'merged',
      action_taken: 'approved_merged',
      lifecycle_state: 'merged',
    });
    vi.mocked(api.getRepoReviews).mockResolvedValue([review]);
    await openRepo(user);

    expect(await screen.findByText('Pass comment published · PR merged')).toBeInTheDocument();
    expect(screen.queryByText(/Approved/i)).not.toBeInTheDocument();
  });

  it('keeps action failure separate from a lifecycle race', async () => {
    const user = userEvent.setup();
    const review = reviewFixture({
      status: 'error',
      action_taken: 'error',
      publication_state: 'not_applicable',
      lifecycle_state: 'failed',
      failure_stage: 'lifecycle',
      display_status: 'PR lifecycle failed',
      display_summary: 'The PR changed state before the requested action could complete.',
    });

    await openReview(user, review, runFixture({ status: 'failed' }));

    expect(screen.getByText('Action not completed')).toBeInTheDocument();
    expect(screen.getByText('GitHub publication not applicable')).toBeInTheDocument();
    expect(screen.queryByText('GitHub publication failed')).not.toBeInTheDocument();
  });

  it('keeps a complete code verdict when publication becomes inapplicable after merge', async () => {
    const user = userEvent.setup();
    const review = reviewFixture({
      status: 'error',
      display_status: 'Changes required · PR merged',
      display_summary: 'The PR merged while CCM was reviewing this exact head, so no GitHub comment was published.',
      outcome_kind: 'infrastructure_error',
      verdict_state: 'complete',
      aggregate_verdict: 'changes_required',
      publication_state: 'not_applicable',
      lifecycle_state: 'merged',
      failure_stage: 'lifecycle',
    });

    await openReview(user, review, runFixture({ status: 'merged' }));

    expect(screen.getByText('Code verdict: Changes required')).toBeInTheDocument();
    expect(screen.getByText('GitHub publication not applicable')).toBeInTheDocument();
    expect(screen.getByText('PR merged')).toBeInTheDocument();
    expect(screen.queryByText('No code verdict was produced by this failed review run.')).not.toBeInTheDocument();
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  });

  it('colors a durable verdict independently from a later error status', async () => {
    const user = userEvent.setup();
    const review = reviewFixture({
      status: 'error',
      display_status: 'Passed',
      aggregate_verdict: 'pass',
      publication_state: 'failed',
      failure_stage: 'publication',
    });
    await openReview(user, review, runFixture());

    const statuses = screen.getAllByText('Passed');
    expect(statuses.length).toBeGreaterThanOrEqual(2);
    statuses.forEach((status) => {
      expect(status).toHaveClass('text-green-400');
      expect(status).not.toHaveClass('text-red-400');
    });
  });

  it('labels an immutable COMMENT publication as a GitHub comment with the CCM actor', async () => {
    const user = userEvent.setup();
    const review = reviewFixture({
      status: 'approved',
      display_status: 'Pass',
      verdict_state: 'complete',
      aggregate_verdict: 'pass',
      publication_state: 'published',
      lifecycle_state: 'reviewing',
      failure_stage: null,
      github_event: 'COMMENT',
      published_actor: 'youchengsong',
      published_at: '2026-08-16T01:02:03Z',
      github_review_id: 987,
      github_review_url: 'https://github.com/acme/widgets/pull/42#pullrequestreview-987',
      github_state: 'COMMENTED',
    });

    await openReview(user, review, runFixture());

    expect(screen.getByText('Code verdict: Pass')).toBeInTheDocument();
    expect(screen.getByText('GitHub comment published')).toBeInTheDocument();
    expect(screen.getByText(/Published by CCM as youchengsong/)).toBeInTheDocument();
    expect(screen.getByText(/GitHub COMMENTED/)).toBeInTheDocument();
    expect(screen.queryByText('Approved')).not.toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Open published GitHub comment' })).toHaveAttribute(
      'href',
      'https://github.com/acme/widgets/pull/42#pullrequestreview-987',
    );
  });

  it('renders untrusted GitHub URLs as inert text in history and detail', async () => {
    const user = userEvent.setup();
    const review = reviewFixture({
      pr_url: 'https://github.com.evil/acme/widgets/pull/42',
      publication_state: 'published',
      github_review_id: 987,
      github_review_url: 'https://github.com/acme/other/pull/42#pullrequestreview-987',
    });

    await openReview(user, review, runFixture());

    expect(screen.queryByRole('link', { name: '#42' })).not.toBeInTheDocument();
    expect(screen.queryByRole('link', { name: 'Open published GitHub comment' })).not.toBeInTheDocument();
    expect(screen.getByText('#42')).toBeInTheDocument();
  });

  it('opens an exact Review directly from the PR Monitor hash query', async () => {
    const review = reviewFixture({ id: 113 });
    window.history.replaceState(null, '', '#/pr-monitor?repo=1&review=113');
    vi.mocked(api.getMonitoredRepos).mockResolvedValue([baseRepo]);
    vi.mocked(api.getRepoReviews).mockResolvedValue([review]);
    vi.mocked(api.getReviewDetail).mockResolvedValue(review);
    vi.mocked(api.getPRMonitorRun).mockResolvedValue(runFixture());

    render(<PRMonitorPage />);

    expect(await screen.findByText('Review Detail · PR #42')).toBeInTheDocument();
    expect(api.getReviewDetail).toHaveBeenCalledWith(113);
    expect(window.location.hash).toBe('#/pr-monitor?repo=1&review=113');
  });

  it('responds to a new Review query while already on the PR Monitor page', async () => {
    const first = reviewFixture({ id: 113, display_summary: 'First review summary' });
    const second = reviewFixture({ id: 114, display_summary: 'Second review summary' });
    window.history.replaceState(null, '', '#/pr-monitor?repo=1&review=113');
    vi.mocked(api.getMonitoredRepos).mockResolvedValue([baseRepo]);
    vi.mocked(api.getRepoReviews).mockResolvedValue([first, second]);
    vi.mocked(api.getReviewDetail).mockImplementation(async (reviewId: number) => (
      reviewId === second.id ? second : first
    ));

    render(<PRMonitorPage />);
    expect(await screen.findByText('First review summary')).toBeInTheDocument();

    window.location.hash = '#/pr-monitor?repo=1&review=114';

    expect(await screen.findByText('Second review summary')).toBeInTheDocument();
    expect(api.getReviewDetail).toHaveBeenCalledWith(114);
  });

  it('does not attach a late Monitor Run from Review A to Review B', async () => {
    const user = userEvent.setup();
    const first = reviewFixture({ id: 113, monitor_run_id: 21, pr_title: 'Review A', display_summary: 'Summary A' });
    const second = reviewFixture({ id: 114, monitor_run_id: 22, pr_title: 'Review B', display_summary: 'Summary B' });
    const firstRun = deferred<PRMonitorRun>();
    vi.mocked(api.getRepoReviews).mockResolvedValue([first, second]);
    vi.mocked(api.getReviewDetail).mockImplementation(async (reviewId: number) => (
      reviewId === second.id ? second : first
    ));
    vi.mocked(api.getPRMonitorRun).mockImplementation(async (runId: number) => (
      runId === 21 ? firstRun.promise : runFixture({ id: 22, status: 'waiting_for_fix' })
    ));

    await openRepo(user);
    await user.click(await screen.findByText('Review A'));
    expect(await screen.findByText('Summary A')).toBeInTheDocument();
    await user.click(screen.getByText('Review B'));
    expect(await screen.findByText('Summary B')).toBeInTheDocument();
    firstRun.resolve(runFixture({ id: 21, status: 'paused' }));
    await act(async () => { await Promise.resolve(); });

    await user.type(screen.getByPlaceholderText('Developer Task ID'), '88');
    await user.click(screen.getByRole('button', { name: 'Bind' }));
    expect(api.bindPRMonitorDeveloper).toHaveBeenCalledWith(22, 88);
    expect(api.bindPRMonitorDeveloper).not.toHaveBeenCalledWith(21, 88);
  });

  it('does not let a late rerun receipt replace a newly selected Review', async () => {
    const user = userEvent.setup();
    const first = reviewFixture({ id: 113, pr_title: 'Review A', display_summary: 'Summary A', can_rerun: true });
    const second = reviewFixture({ id: 114, pr_title: 'Review B', display_summary: 'Summary B', can_rerun: false });
    const receipt = deferred<Awaited<ReturnType<typeof api.rerunPRReview>>>();
    vi.mocked(api.getRepoReviews).mockResolvedValue([first, second]);
    vi.mocked(api.getReviewDetail).mockImplementation(async (reviewId: number) => (
      reviewId === second.id ? second : first
    ));
    vi.mocked(api.rerunPRReview).mockReturnValue(receipt.promise);

    await openRepo(user);
    await user.click(await screen.findByText('Review A'));
    await user.click(await screen.findByRole('button', { name: 'Re-run exact head' }));
    await user.click(screen.getByText('Review B'));
    expect(await screen.findByText('Summary B')).toBeInTheDocument();
    receipt.resolve({
      id: 115,
      attempt: 2,
      rerun_of_review_id: first.id,
      monitor_run_id: first.monitor_run_id,
      status: 'pending',
      head_sha: first.head_sha!,
    });
    await act(async () => { await Promise.resolve(); });

    expect(screen.getByText('Summary B')).toBeInTheDocument();
    expect(api.getReviewDetail).not.toHaveBeenCalledWith(115);
  });

  it('shows rerun lineage and opens history rows from the keyboard without link bubbling', async () => {
    const user = userEvent.setup();
    const review = reviewFixture({ attempt: 2, rerun_of_review_id: 10 });
    vi.mocked(api.getRepoReviews).mockResolvedValue([review]);
    vi.mocked(api.getReviewDetail).mockResolvedValue(review);
    await openRepo(user);

    expect(screen.getByText('Attempt 2')).toBeInTheDocument();
    expect(screen.getByText('Re-run of Review #10')).toBeInTheDocument();
    await user.click(screen.getByRole('link', { name: '#42' }));
    expect(api.getReviewDetail).not.toHaveBeenCalled();

    const row = screen.getByLabelText('Open Review 11, attempt 2');
    row.focus();
    await user.keyboard('{Enter}');
    expect(await screen.findByText('Review Detail · PR #42')).toBeInTheDocument();
    expect(api.getReviewDetail).toHaveBeenCalledWith(11);
  });

  it('clears the selected repository when a new deep link is invalid', async () => {
    const user = userEvent.setup();
    vi.mocked(api.getMonitoredRepos).mockResolvedValue([baseRepo]);
    await openRepo(user);
    expect(screen.getByRole('button', { name: 'Save Changes' })).toBeInTheDocument();

    window.location.hash = '#/pr-monitor?repo=999&review=1';

    expect(await screen.findByText('Monitored repository #999 was not found')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Save Changes' })).not.toBeInTheDocument();
    expect(screen.getByText(baseRepo.repo_full_name)).toBeInTheDocument();
  });

  it('starts a distinct exact-head rerun from Review detail', async () => {
    const user = userEvent.setup();
    const review = reviewFixture({ can_rerun: true, head_sha: 'exact-head-sha' });
    const rerun = reviewFixture({ id: 12, can_rerun: false, head_sha: 'exact-head-sha' });
    vi.mocked(api.rerunPRReview).mockResolvedValue({
      id: rerun.id, attempt: 2, rerun_of_review_id: review.id,
      monitor_run_id: rerun.monitor_run_id, status: rerun.status, head_sha: rerun.head_sha!,
    });

    await openReview(user, review, runFixture());
    vi.mocked(api.getReviewDetail).mockResolvedValue(rerun);
    await user.click(screen.getByRole('button', { name: 'Re-run exact head' }));

    await waitFor(() => {
      expect(api.rerunPRReview).toHaveBeenCalledWith(
        review.id,
        'exact-head-sha',
        expect.any(String),
      );
    });
    expect(window.location.hash).toBe('#/pr-monitor?repo=1&review=12');
  });

  it('reuses the Review-detail rerun key after a response is lost', async () => {
    const user = userEvent.setup();
    const review = reviewFixture({ can_rerun: true, head_sha: 'exact-head-sha' });
    const rerun = reviewFixture({ id: 12, can_rerun: false, head_sha: 'exact-head-sha' });
    vi.mocked(api.rerunPRReview)
      .mockRejectedValueOnce(new Error('network response lost'))
      .mockResolvedValueOnce({
        id: rerun.id, attempt: 2, rerun_of_review_id: review.id,
        monitor_run_id: rerun.monitor_run_id, status: rerun.status, head_sha: rerun.head_sha!,
      });

    await openReview(user, review, runFixture());
    vi.mocked(api.getReviewDetail).mockResolvedValue(rerun);
    await user.click(screen.getByRole('button', { name: 'Re-run exact head' }));
    expect(await screen.findByText(/network response lost/)).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Re-run exact head' }));

    await waitFor(() => expect(api.rerunPRReview).toHaveBeenCalledTimes(2));
    const firstKey = vi.mocked(api.rerunPRReview).mock.calls[0][2];
    const secondKey = vi.mocked(api.rerunPRReview).mock.calls[1][2];
    expect(secondKey).toBe(firstKey);
  });

  it('keeps a successful rerun successful when Monitor detail refresh fails', async () => {
    const user = userEvent.setup();
    const review = reviewFixture({ can_rerun: true, head_sha: 'exact-head-sha' });
    const rerun = reviewFixture({ id: 12, can_rerun: false, head_sha: 'exact-head-sha' });
    vi.mocked(api.rerunPRReview).mockResolvedValue({
      id: rerun.id, attempt: 2, rerun_of_review_id: review.id,
      monitor_run_id: rerun.monitor_run_id, status: rerun.status, head_sha: rerun.head_sha!,
    });

    await openReview(user, review, runFixture());
    vi.mocked(api.getReviewDetail).mockResolvedValue(rerun);
    vi.mocked(api.getPRMonitorRun).mockRejectedValueOnce(new Error('refresh unavailable'));
    await user.click(screen.getByRole('button', { name: 'Re-run exact head' }));

    expect(await screen.findByText('Exact-head review started.')).toBeInTheDocument();
    expect(await screen.findByText('Error: refresh unavailable')).toBeInTheDocument();
    expect(screen.queryByText(/Could not start the exact-head review/)).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Re-run exact head' })).not.toBeInTheDocument();
    expect(api.rerunPRReview).toHaveBeenCalledTimes(1);
  });

  it('does not bind a late Run action response to a newly selected Review', async () => {
    const user = userEvent.setup();
    const reviewA = reviewFixture({
      id: 11,
      monitor_run_id: 21,
      pr_title: 'Review A with a slow pause',
    });
    const reviewB = reviewFixture({
      id: 12,
      monitor_run_id: 22,
      pr_title: 'Review B remains current',
      review_summary: 'Review B detail is selected.',
    });
    const runA = runFixture({ id: 21, status: 'waiting_for_fix' });
    const runB = runFixture({ id: 22, status: 'ready_to_merge' });
    const pause = deferred<PRMonitorRun>();
    vi.mocked(api.getRepoReviews).mockResolvedValue([reviewA, reviewB]);
    vi.mocked(api.getReviewDetail).mockImplementation(async (reviewId: number) => (
      reviewId === reviewA.id ? reviewA : reviewB
    ));
    vi.mocked(api.getPRMonitorRun).mockImplementation(async (runId: number) => (
      runId === runA.id ? runA : runB
    ));
    vi.mocked(api.pausePRMonitorRun).mockReturnValue(pause.promise);

    await openRepo(user);
    await user.click(await screen.findByText(reviewA.pr_title));
    await screen.findByText('Loop: Waiting For Fix · repair 0/3');
    await user.click(screen.getByRole('button', { name: 'Pause loop' }));
    await user.click(screen.getByText(reviewB.pr_title));
    expect(await screen.findByText('Review B detail is selected.')).toBeInTheDocument();
    expect(await screen.findByText('Loop: Ready To Merge · repair 0/3')).toBeInTheDocument();

    await act(async () => {
      pause.resolve(runFixture({ id: 21, status: 'paused' }));
      await pause.promise;
    });

    expect(screen.getByText('Review B detail is selected.')).toBeInTheDocument();
    expect(screen.getByText('Loop: Ready To Merge · repair 0/3')).toBeInTheDocument();
    expect(screen.queryByText('Loop: Paused · repair 0/3')).not.toBeInTheDocument();
  });

  it('keeps a newly selected Review after a late Finding rebuttal refresh', async () => {
    const user = userEvent.setup();
    const reviewA = reviewFixture({
      id: 31,
      monitor_run_id: 41,
      pr_title: 'Review A with a slow rebuttal',
      reviewer_runs: [{
        id: 51,
        role: 'principal_engineer',
        task_id: 301,
        provider: 'codex',
        model: 'gpt-5.6-sol',
        effort: 'high',
        status: 'changes_required',
        verdict: 'changes_required',
        error_message: null,
        created_at: '2026-08-02T00:00:00Z',
        completed_at: '2026-08-02T00:05:00Z',
        findings: [{
          id: 61,
          reviewer_run_id: 51,
          role: 'principal_engineer',
          severity: 'medium',
          category: 'correctness',
          path: 'backend/service.py',
          line: 10,
          hunk: null,
          title: 'Slow rebuttal finding',
          evidence: 'The result refresh can race another Review selection.',
          impact: 'A stale Review could replace the selected detail.',
          required_fix: 'Fence the refresh by selected Review identity.',
          test: 'Switch Reviews before the rebuttal response completes.',
          status: 'open',
          thread_status: 'published_inline',
          github_comment_id: 100,
          github_comment_url: 'https://github.com/acme/widgets/pull/42#discussion_r100',
          thread_error: null,
          rebuttals: [],
          latest_action: null,
        }],
      }],
    });
    const reviewB = reviewFixture({
      id: 32,
      monitor_run_id: 42,
      pr_title: 'Review B remains selected after rebuttal',
      display_summary: 'Review B is still selected.',
    });
    const submission = deferred<Awaited<ReturnType<typeof api.submitPRFindingRebuttal>>>();
    vi.mocked(api.getRepoReviews).mockResolvedValue([reviewA, reviewB]);
    vi.mocked(api.getReviewDetail).mockImplementation(async (reviewId: number) => (
      reviewId === reviewA.id ? reviewA : reviewB
    ));
    vi.mocked(api.getPRMonitorRun).mockImplementation(async (runId: number) => (
      runFixture({ id: runId })
    ));
    vi.mocked(api.submitPRFindingRebuttal).mockReturnValue(submission.promise);

    await openRepo(user);
    await user.click(await screen.findByText(reviewA.pr_title));
    await user.type(
      screen.getByPlaceholderText('Concrete code/test/policy evidence for this exact head'),
      'Concrete evidence that safely disputes this finding.',
    );
    await user.click(screen.getByRole('button', { name: 'Submit rebuttal' }));
    await user.click(screen.getByText(reviewB.pr_title));
    expect(await screen.findByText('Review B is still selected.')).toBeInTheDocument();

    await act(async () => {
      submission.resolve({} as Awaited<ReturnType<typeof api.submitPRFindingRebuttal>>);
      await submission.promise;
    });

    await waitFor(() => expect(api.getReviewDetail).toHaveBeenLastCalledWith(reviewB.id));
    expect(screen.getByText('Review B is still selected.')).toBeInTheDocument();
    expect(screen.queryByText(reviewA.review_summary!)).not.toBeInTheDocument();
  });

  it('opens monitored repositories from the keyboard and names icon actions', async () => {
    const user = userEvent.setup();
    vi.mocked(api.getMonitoredRepos).mockResolvedValue([baseRepo]);
    render(<PRMonitorPage />);

    const row = await screen.findByLabelText(`Open monitored repository ${baseRepo.repo_full_name}`);
    expect(screen.getByRole('button', {
      name: `Disable monitoring for ${baseRepo.repo_full_name}`,
    })).toBeInTheDocument();
    expect(screen.getByRole('button', {
      name: `Delete monitoring for ${baseRepo.repo_full_name}`,
    })).toBeInTheDocument();

    const toggle = screen.getByRole('button', {
      name: `Disable monitoring for ${baseRepo.repo_full_name}`,
    });
    toggle.focus();
    await user.keyboard('{Enter}');
    expect(api.toggleMonitoredRepo).toHaveBeenCalledWith(baseRepo.id);
    expect(screen.queryByRole('button', { name: 'Save Changes' })).not.toBeInTheDocument();

    row.focus();
    await user.keyboard('{Enter}');

    expect(await screen.findByRole('button', { name: 'Save Changes' })).toBeInTheDocument();
  });

  it('does not infer a historical review mode from the current repository setting', async () => {
    const user = userEvent.setup();
    const currentPanelRepo: MonitoredRepo = {
      ...baseRepo,
      review_mode: 'panel',
      wait_for_ci: true,
      required_checks: [],
      auto_repair: false,
      merge_queue_mode: 'manual',
    };
    const review = reviewFixture({
      task_id: 701,
      task_ids: undefined,
      reviewer_runs: [],
      reviewer_count: 0,
      display_status: 'Changes required',
      display_summary: 'The single reviewer found one blocking issue.',
      outcome_kind: 'review_result',
      aggregate_verdict: 'changes_required',
    });

    await openReview(user, review, runFixture(), currentPanelRepo);

    expect(screen.getByText('The single reviewer found one blocking issue.')).toBeInTheDocument();
    expect(screen.queryByText('#701')).not.toBeInTheDocument();
    expect(screen.queryByText('Reviewer panel has not started yet.')).not.toBeInTheDocument();
  });

  it('loads the complete review body when a bounded history row is opened', async () => {
    const user = userEvent.setup();
    const listReview = reviewFixture({
      display_summary: 'Authorization finding preview…',
      review_summary: 'Authorization finding preview…',
    });
    const detailReview = {
      ...listReview,
      display_summary: 'Authorization can be bypassed.\n\nCheck the project ACL before dispatch.',
      review_summary: 'Authorization can be bypassed.\n\nCheck the project ACL before dispatch.',
    };
    vi.mocked(api.getRepoReviews).mockResolvedValue([listReview]);
    vi.mocked(api.getReviewDetail).mockResolvedValue(detailReview);

    await openRepo(user);
    await user.click(await screen.findByText(listReview.pr_title));

    expect(await screen.findByText('Authorization can be bypassed.')).toBeInTheDocument();
    expect(screen.getByText('Check the project ACL before dispatch.')).toBeInTheDocument();
    expect(screen.queryByText('Authorization finding preview…')).not.toBeInTheDocument();
    expect(api.getReviewDetail).toHaveBeenCalledWith(listReview.id);
  });

  it('renders the complete review body as readable Markdown', async () => {
    const user = userEvent.setup();
    const markdownBody = [
      '# Review result',
      '',
      '- Rejects stale task claims',
      '- Preserves the exact head SHA',
      '',
      '```python',
      'assert task.incarnation == expected_incarnation',
      '```',
    ].join('\n');
    const review = reviewFixture({
      display_summary: markdownBody,
      review_summary: markdownBody,
    });

    await openReview(user, review, runFixture());

    expect(screen.getByRole('heading', { name: 'Review result', level: 1 })).toBeInTheDocument();
    expect(screen.getByRole('list')).toHaveTextContent('Rejects stale task claims');
    expect(screen.getByRole('list')).toHaveTextContent('Preserves the exact head SHA');
    expect(screen.getByText('assert task.incarnation == expected_incarnation')).toBeInTheDocument();
  });

  it('labels current and historical review heads in history', async () => {
    const user = userEvent.setup();
    vi.mocked(api.getRepoReviews).mockResolvedValue([
      reviewFixture({ id: 12, head_sha: '2222222222222222222222222222222222222222', is_current_snapshot: true }),
      reviewFixture({ id: 11, head_sha: '1111111111111111111111111111111111111111', is_current_snapshot: false }),
    ]);

    await openRepo(user);

    expect(await screen.findByText('22222222')).toBeInTheDocument();
    expect(screen.getByText('11111111')).toBeInTheDocument();
    expect(screen.getByText('Current head')).toBeInTheDocument();
    expect(screen.getByText('Historical head')).toBeInTheDocument();
  });

  it('subscribes to PR Monitor updates and refreshes an active review list', async () => {
    const user = userEvent.setup();
    const activeReview = reviewFixture({ status: 'reviewing', outcome_kind: 'in_progress' });
    vi.mocked(api.getRepoReviews).mockResolvedValue([activeReview]);
    await openRepo(user);

    const subscription = vi.mocked(useWebSocket).mock.calls.find(([channels]) => (
      channels.length === 1 && channels[0] === 'pr-monitor'
    ));
    expect(subscription).toBeDefined();

    vi.mocked(api.getRepoReviews).mockResolvedValue([{ ...activeReview, pr_title: 'Refreshed review title' }]);
    await act(async () => {
      subscription?.[1]?.({ type: 'review_updated', review_id: activeReview.id });
      await Promise.resolve();
    });

    expect(await screen.findByText('Refreshed review title')).toBeInTheDocument();
  });

  it('shows CI and monitor details before reviewer runs exist', async () => {
    const user = userEvent.setup();
    const review = reviewFixture({ status: 'waiting_ci', reviewer_runs: [] });
    const run = runFixture({
      wakes: [{
        id: 7,
        developer_task_id: null,
        trigger_head_sha: 'head-sha',
        reason_kind: 'review_findings',
        status: 'shadow',
        attempt: 1,
        last_error: null,
      }],
    });
    await openReview(user, review, run);

    expect(screen.getByText('CI: Failed: tests')).toBeInTheDocument();
    expect(screen.getByText('failure · tests · github-actions')).toBeInTheDocument();
    const diagnostics = screen.getByText('Advanced diagnostics').closest('details');
    expect(diagnostics).not.toHaveAttribute('open');
    await user.click(screen.getByText('Advanced diagnostics'));
    expect(screen.getByText(/Loop: Waiting For Fix/)).toBeInTheDocument();
    expect(screen.getByText(/Wake #7: Shadow/)).toBeInTheDocument();
    expect(screen.getByText('Reviewer panel has not started yet.')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Bind' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Pause loop' })).not.toBeInTheDocument();
  });

  it('offers direct merge for a ready run and renders a merge failure', async () => {
    const user = userEvent.setup();
    const review = reviewFixture({ status: 'approved', ci_status: 'success', ci_summary: 'All required checks passed' });
    const run = runFixture({ status: 'ready_to_merge', developer_task_id: 55 });
    const mergeRequest = deferred<PRMonitorRun>();
    vi.mocked(api.mergePRMonitorRun).mockReturnValueOnce(mergeRequest.promise);
    await openReview(user, review, run);

    expect(screen.queryByRole('button', { name: 'Bind' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Unbind Developer' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Pause loop' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Resume loop' })).not.toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Merge PR' }));
    expect(screen.getByRole('button', { name: 'Merging…' })).toBeDisabled();
    await act(async () => mergeRequest.reject(new Error('merge rejected')));
    expect(await screen.findByRole('alert')).toHaveTextContent('Error: merge rejected');
  });

  it('shows unbind pending and failure states only when the run is safe to mutate', async () => {
    const user = userEvent.setup();
    const unbindRequest = deferred<PRMonitorRun>();
    vi.mocked(api.unbindPRMonitorDeveloper).mockReturnValueOnce(unbindRequest.promise);
    await openReview(user, reviewFixture(), runFixture({ developer_task_id: 55 }));

    await user.click(screen.getByRole('button', { name: 'Unbind Developer' }));
    expect(screen.getByRole('button', { name: 'Unbinding…' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Pause loop' })).toBeDisabled();
    await act(async () => unbindRequest.reject(new Error('unbind rejected')));
    expect(await screen.findByRole('alert')).toHaveTextContent('Error: unbind rejected');
    expect(screen.getByRole('button', { name: 'Unbind Developer' })).toBeEnabled();
  });

  it.each(['merged', 'closed'])('does not expose run controls for a %s run', async (status) => {
    const user = userEvent.setup();
    await openReview(
      user,
      reviewFixture({ status }),
      runFixture({ status, developer_task_id: 55 }),
    );

    expect(screen.queryByRole('button', { name: 'Bind' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Unbind Developer' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Pause loop' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Resume loop' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Merge PR' })).not.toBeInTheDocument();
  });

  it('hides pause and binding controls while repair delivery is active', async () => {
    const user = userEvent.setup();
    const run = runFixture({
      developer_task_id: 55,
      wakes: [{
        id: 8,
        developer_task_id: 55,
        trigger_head_sha: 'head-sha',
        reason_kind: 'review_findings',
        status: 'delivering',
        attempt: 1,
        last_error: null,
      }],
    });
    await openReview(user, reviewFixture(), run);

    await user.click(screen.getByText('Advanced diagnostics'));
    expect(screen.getByText(/Wake #8: Delivering/)).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Unbind Developer' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Pause loop' })).not.toBeInTheDocument();
  });

  it('keeps an accepted rebuttal locked until durable resolution finishes', async () => {
    const user = userEvent.setup();
    const review = reviewFixture({
      reviewer_runs: [{
        id: 31,
        role: 'principal',
        task_id: 301,
        provider: 'codex',
        model: 'gpt-5.6-sol',
        effort: 'high',
        status: 'changes_required',
        verdict: 'changes_required',
        error_message: null,
        created_at: '2026-08-02T00:00:00Z',
        completed_at: '2026-08-02T00:05:00Z',
        findings: [{
          id: 41,
          reviewer_run_id: 31,
          role: 'principal',
          severity: 'medium',
          category: 'correctness',
          path: 'backend/service.py',
          line: 10,
          hunk: null,
          title: 'Durable resolution pending',
          evidence: 'The accepted rebuttal has not resolved its GitHub thread yet.',
          impact: 'A duplicate adjudication could race durable publication.',
          required_fix: 'Wait for the accepted effect to finish.',
          test: 'Verify a second rebuttal cannot be submitted.',
          status: 'open',
          thread_status: 'published_inline',
          github_comment_id: 100,
          github_comment_url: 'https://github.com/acme/widgets/pull/42#discussion_r100',
          thread_error: null,
          rebuttals: [{
            id: 51,
            finding_id: 41,
            task_id: 401,
            attempt: 1,
            evidence: 'Concrete accepted evidence',
            status: 'accepted',
            verdict: 'accepted',
            result_body: 'Accepted; resolving the durable thread.',
            error_message: null,
          }],
        }],
      }],
    });
    await openReview(user, review, runFixture({ status: 'adjudicating' }));

    expect(screen.getByRole('link', { name: 'published_inline' })).toHaveAttribute(
      'href',
      'https://github.com/acme/widgets/pull/42#discussion_r100',
    );
    expect(screen.getByPlaceholderText('Concrete code/test/policy evidence for this exact head')).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Adjudicating…' })).toBeDisabled();
    expect(api.submitPRFindingRebuttal).not.toHaveBeenCalled();
  });

  it.each([
    ['a script URL', 'javascript:alert(1)'],
    ['a lookalike host', 'https://github.com.evil/acme/widgets/pull/42#discussion_r100'],
    ['a different repository', 'https://github.com/acme/other/pull/42#discussion_r100'],
    ['a different PR', 'https://github.com/acme/widgets/pull/43#discussion_r100'],
    ['a different comment', 'https://github.com/acme/widgets/pull/42#discussion_r101'],
    ['an arbitrary anchor', 'https://github.com/acme/widgets/pull/42#files'],
  ])('does not expose an untrusted Finding thread link for %s', async (_label, url) => {
    const user = userEvent.setup();
    const review = reviewFixture({
      reviewer_runs: [{
        id: 31,
        role: 'principal',
        task_id: 301,
        provider: 'codex',
        model: 'gpt-5.6-sol',
        effort: 'high',
        status: 'changes_required',
        verdict: 'changes_required',
        error_message: null,
        created_at: '2026-08-02T00:00:00Z',
        completed_at: '2026-08-02T00:05:00Z',
        findings: [{
          id: 41,
          reviewer_run_id: 31,
          role: 'principal',
          severity: 'medium',
          category: 'correctness',
          path: 'backend/service.py',
          line: 10,
          hunk: null,
          title: 'Untrusted thread URL',
          evidence: 'The API projection is not a trusted navigation target.',
          impact: 'An operator could be sent away from the reviewed subject.',
          required_fix: 'Bind navigation to exact GitHub evidence.',
          test: 'Reject non-canonical links.',
          status: 'open',
          thread_status: 'published_inline',
          github_comment_id: 100,
          github_comment_url: url,
          thread_error: null,
          rebuttals: [],
          latest_action: null,
        }],
      }],
    });

    await openReview(user, review, runFixture());

    expect(screen.getByText(/Untrusted thread URL/)).toBeInTheDocument();
    expect(screen.queryByRole('link', { name: 'published_inline' })).not.toBeInTheDocument();
  });
});
