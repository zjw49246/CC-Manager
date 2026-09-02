import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { PRReview, PRReviewResult } from '../../api/client';
import { api } from '../../api/client';
import { PRMonitorTaskDetail } from './PRMonitorTaskDetail';

vi.mock('../../api/client', () => ({
  api: {
    getReviewDetail: vi.fn(),
    getPRMonitorRun: vi.fn(),
    mergePRMonitorRun: vi.fn(),
    updatePRMonitorBranch: vi.fn(),
    enqueuePRMonitorMerge: vi.fn(),
  },
}));

function resultFixture(): PRReviewResult {
  return {
    result_key: 'run:14',
    run_id: 14,
    display_task_id: 42,
    repo_id: 3,
    repo_full_name: 'acme/widget',
    pr_number: 133,
    pr_title: 'Read-only review',
    pr_url: 'https://github.com/acme/widget/pull/133',
    review_id: 113,
    base_ref: 'main',
    base_sha: 'b'.repeat(40),
    head_sha: 'a'.repeat(40),
    verdict_state: 'complete',
    aggregate_verdict: 'changes_required',
    publication_state: 'not_applicable',
    lifecycle_state: 'reviewing',
    failure_stage: null,
    error_category: null,
    error_measured: null,
    error_limit: null,
    error_unit: null,
    display_status: 'Changes required',
    display_summary: 'Review summary',
    published_actor: null,
    published_at: null,
    github_review_id: null,
    github_review_url: null,
    github_state: null,
    github_event: null,
    created_at: '2026-08-16T00:00:00Z',
    updated_at: '2026-08-16T00:02:00Z',
    completed_at: '2026-08-16T00:02:00Z',
    can_rerun: false,
  };
}

describe('PRMonitorTaskDetail', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(api.getPRMonitorRun).mockResolvedValue({
      id: 14,
      repo_id: 3,
      pr_number: 133,
      status: 'waiting_for_fix',
      current_head_sha: 'a'.repeat(40),
      display_task_id: 42,
      developer_task_id: null,
      repair_attempts: 0,
      max_repair_attempts: 3,
      pause_reason: null,
      wakes: [],
      merge_actions: [],
      review_history: [{
        id: 112,
        attempt: 1,
        head_sha: 'c'.repeat(40),
        status: 'commented',
        aggregate_verdict: 'changes_required',
        publication_state: 'published',
        github_review_id: 7001,
        github_review_url: 'https://github.com/acme/widget/pull/133#pullrequestreview-7001',
        created_at: '2026-08-15T23:00:00Z',
        completed_at: '2026-08-15T23:02:00Z',
      }, {
        id: 113,
        attempt: 1,
        head_sha: 'a'.repeat(40),
        status: 'commented',
        aggregate_verdict: 'changes_required',
        publication_state: 'published',
        github_review_id: 7002,
        github_review_url: 'https://github.com/acme/widget/pull/133#pullrequestreview-7002',
        created_at: '2026-08-16T00:00:00Z',
        completed_at: '2026-08-16T00:02:00Z',
      }],
    });
  });

  it('loads the current PRReviewDetail and keeps it read-only', async () => {
    vi.mocked(api.getReviewDetail).mockResolvedValue({
      ...resultFixture(),
      reviewer_runs: [{
        id: 1,
        role: 'principal_engineer',
        task_id: 900,
        provider: 'codex',
        model: 'gpt-5.6-sol',
        effort: 'high',
        status: 'completed',
        verdict: 'changes_required',
        result_body: 'Found a regression.',
        outcome_kind: 'review_result',
        error_message: null,
        created_at: '2026-08-16T00:00:00Z',
        completed_at: '2026-08-16T00:01:00Z',
        findings: [],
      }],
    } as PRReview);

    render(
      <PRMonitorTaskDetail
        task={{
          title: 'PR Review: acme/widget#133',
          description: null,
          metadata_: { pr_monitor_display: true, pr_monitor_review_id: 113 },
        }}
        result={resultFixture()}
        onBack={vi.fn()}
      />,
    );

    await waitFor(() => expect(api.getReviewDetail).toHaveBeenCalledWith(113));
    await waitFor(() => expect(api.getPRMonitorRun).toHaveBeenCalledWith(14));
    expect(await screen.findByRole('region', { name: 'Reviewer details' })).toBeInTheDocument();
    expect(screen.getByRole('region', { name: 'Review history' })).toHaveTextContent('2 review attempts · 2 commits');
    expect(screen.getByText('Review 1')).toBeInTheDocument();
    expect(screen.getByText('Review 2')).toBeInTheDocument();
    expect(screen.getByText('Current')).toBeInTheDocument();
    expect(screen.getAllByRole('link', { name: 'Open GitHub review' })).toHaveLength(2);
    expect(screen.getByText('Principal Engineer')).toBeInTheDocument();
    expect(screen.getByText('Found a regression.')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /Re-run|Create follow-up|Open review details/i })).not.toBeInTheDocument();
  });

  it('offers the exact-head merge action only after a passing run is ready', async () => {
    const readyRun = {
      id: 14,
      repo_id: 3,
      pr_number: 133,
      status: 'ready_to_merge',
      current_head_sha: 'a'.repeat(40),
      current_review_id: 113,
      display_task_id: 42,
      developer_task_id: null,
      repair_attempts: 0,
      max_repair_attempts: 3,
      pause_reason: null,
      wakes: [],
      merge_actions: [],
      review_history: [],
    };
    vi.mocked(api.getPRMonitorRun).mockResolvedValue(readyRun);
    vi.mocked(api.mergePRMonitorRun).mockResolvedValue({
      ...readyRun,
      status: 'merge_pending',
      merge_actions: [{
        id: 31,
        review_id: 113,
        trigger_head_sha: 'a'.repeat(40),
        status: 'pending',
        effect_kind: 'direct',
        github_queue_entry_id: null,
        merge_group_sha: null,
        ci_status: null,
        attempt_count: 0,
        last_error: null,
      }],
    });

    render(
      <PRMonitorTaskDetail
        task={{
          title: 'PR Review: acme/widget#133',
          description: null,
          metadata_: { pr_monitor_display: true, pr_monitor_run_id: 14, pr_monitor_review_id: 113 },
        }}
        result={{ ...resultFixture(), aggregate_verdict: 'pass' }}
        onBack={vi.fn()}
      />,
    );

    const mergeButton = await screen.findByRole('button', { name: 'Merge PR' });
    fireEvent.click(mergeButton);
    await waitFor(() => expect(api.mergePRMonitorRun).toHaveBeenCalledWith(14));
    expect(await screen.findByText('Merging')).toBeInTheDocument();
  });

  it('does not offer merge for a stale result projection', async () => {
    vi.mocked(api.getPRMonitorRun).mockResolvedValue({
      id: 14,
      repo_id: 3,
      pr_number: 133,
      status: 'ready_to_merge',
      current_head_sha: 'd'.repeat(40),
      current_review_id: 114,
      display_task_id: 42,
      developer_task_id: null,
      repair_attempts: 0,
      max_repair_attempts: 3,
      pause_reason: null,
      wakes: [],
      merge_actions: [],
      review_history: [],
    });

    render(
      <PRMonitorTaskDetail
        task={{
          title: 'PR Review: acme/widget#133',
          description: null,
          metadata_: { pr_monitor_display: true, pr_monitor_run_id: 14, pr_monitor_review_id: 113 },
        }}
        result={{ ...resultFixture(), aggregate_verdict: 'pass' }}
        onBack={vi.fn()}
      />,
    );

    await waitFor(() => expect(api.getPRMonitorRun).toHaveBeenCalledWith(14));
    expect(screen.queryByRole('button', { name: 'Merge PR' })).not.toBeInTheDocument();
  });

  it('explains when the base branch must be updated before a fresh review', async () => {
    const pausedRun = {
      id: 14,
      repo_id: 3,
      pr_number: 133,
      status: 'paused',
      current_head_sha: 'a'.repeat(40),
      current_review_id: 113,
      display_task_id: 42,
      developer_task_id: null,
      repair_attempts: 0,
      max_repair_attempts: 3,
      pause_reason: 'direct_merge_subject_changed',
      wakes: [],
      merge_actions: [{
        id: 31,
        review_id: 113,
        trigger_head_sha: 'a'.repeat(40),
        status: 'failed',
        effect_kind: 'direct',
        github_queue_entry_id: null,
        merge_group_sha: null,
        ci_status: null,
        attempt_count: 1,
        last_error: 'direct_merge_remote_absence_proven:GhError:GitHub PR base ancestry is unsafe for direct auto-merge',
      }],
      review_history: [],
    };
    vi.mocked(api.getPRMonitorRun)
      .mockResolvedValueOnce(pausedRun)
      .mockResolvedValueOnce({
        ...pausedRun,
        pause_reason: 'direct_merge_base_update_requested',
      });
    vi.mocked(api.updatePRMonitorBranch).mockResolvedValue({
      status: 'accepted',
      expected_head_sha: 'a'.repeat(40),
      message: 'GitHub accepted the branch update',
    });

    render(
      <PRMonitorTaskDetail
        task={{
          title: 'PR Review: acme/widget#133',
          description: null,
          metadata_: { pr_monitor_display: true, pr_monitor_run_id: 14, pr_monitor_review_id: 113 },
        }}
        result={{ ...resultFixture(), aggregate_verdict: 'pass' }}
        onBack={vi.fn()}
      />,
    );

    const updateButton = await screen.findByRole('button', { name: 'Update branch & re-review' });
    expect(screen.getByText(/The base branch advanced after this review/)).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Open PR on GitHub' })).toHaveAttribute(
      'href',
      'https://github.com/acme/widget/pull/133',
    );
    expect(screen.queryByRole('button', { name: 'Merge PR' })).not.toBeInTheDocument();
    expect(screen.queryByText(/direct_merge_remote_absence_proven/)).not.toBeInTheDocument();
    fireEvent.click(updateButton);
    await waitFor(() => expect(api.updatePRMonitorBranch).toHaveBeenCalledWith(14, 'a'.repeat(40)));
    await waitFor(() => expect(api.getPRMonitorRun).toHaveBeenCalledTimes(2));
  });

  it('ignores a failed merge action from an older reviewed head', async () => {
    vi.mocked(api.getPRMonitorRun).mockResolvedValue({
      id: 14,
      repo_id: 3,
      pr_number: 133,
      status: 'ready_to_merge',
      current_head_sha: 'a'.repeat(40),
      current_review_id: 113,
      display_task_id: 42,
      developer_task_id: null,
      repair_attempts: 0,
      max_repair_attempts: 3,
      pause_reason: null,
      wakes: [],
      merge_actions: [{
        id: 30,
        review_id: 112,
        trigger_head_sha: 'c'.repeat(40),
        status: 'failed',
        effect_kind: 'direct',
        github_queue_entry_id: null,
        merge_group_sha: null,
        ci_status: null,
        attempt_count: 1,
        last_error: 'direct_merge_remote_absence_proven:GhError:GitHub PR base ancestry is unsafe for direct auto-merge',
      }],
      review_history: [],
    });

    render(
      <PRMonitorTaskDetail
        task={{
          title: 'PR Review: acme/widget#133',
          description: null,
          metadata_: { pr_monitor_display: true, pr_monitor_run_id: 14, pr_monitor_review_id: 113 },
        }}
        result={{ ...resultFixture(), aggregate_verdict: 'pass' }}
        onBack={vi.fn()}
      />,
    );

    expect(await screen.findByRole('button', { name: 'Merge PR' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Update branch & re-review' })).not.toBeInTheDocument();
    expect(screen.queryByText(/The base branch advanced after this review/)).not.toBeInTheDocument();
  });

  it('hides stale merge controls after the PR was merged externally', async () => {
    vi.mocked(api.getPRMonitorRun).mockResolvedValue({
      id: 14,
      repo_id: 3,
      pr_number: 133,
      status: 'paused',
      current_head_sha: 'a'.repeat(40),
      current_review_id: 113,
      display_task_id: 42,
      developer_task_id: null,
      repair_attempts: 0,
      max_repair_attempts: 3,
      pause_reason: null,
      wakes: [],
      merge_actions: [{
        id: 30,
        review_id: 112,
        trigger_head_sha: 'c'.repeat(40),
        status: 'failed',
        effect_kind: 'direct',
        github_queue_entry_id: null,
        merge_group_sha: null,
        ci_status: null,
        attempt_count: 1,
        last_error: 'direct_merge_remote_absence_proven:GhError:GitHub PR base ancestry is unsafe for direct auto-merge',
      }],
      review_history: [],
    });

    render(
      <PRMonitorTaskDetail
        task={{
          title: 'PR Review: acme/widget#133',
          description: null,
          metadata_: { pr_monitor_display: true, pr_monitor_run_id: 14, pr_monitor_review_id: 113 },
        }}
        result={{ ...resultFixture(), aggregate_verdict: 'pass', lifecycle_state: 'merged' }}
        onBack={vi.fn()}
      />,
    );

    await waitFor(() => expect(api.getPRMonitorRun).toHaveBeenCalledWith(14));
    expect(screen.queryByRole('region', { name: 'Merge controls' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Merge PR' })).not.toBeInTheDocument();
    expect(screen.queryByText(/The base branch advanced after this review/)).not.toBeInTheDocument();
  });
});
