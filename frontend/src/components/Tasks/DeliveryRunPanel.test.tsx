import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import type { DeliveryRun } from '../../api/client';
import { DeliveryRunPanel } from './DeliveryRunPanel';

vi.mock('../../api/client', () => ({
  api: {
    getDeliveryRun: vi.fn(),
    pauseDeliveryRun: vi.fn(),
    resumeDeliveryRun: vi.fn(),
    cancelDeliveryRun: vi.fn(),
    retryDeliveryRun: vi.fn(),
  },
}));

import { api } from '../../api/client';

function makeRun(overrides: Partial<DeliveryRun> = {}): DeliveryRun {
  return {
    id: 7,
    created_by: 1,
    project_id: 2,
    monitored_repo_id: 3,
    source_todo_id: null,
    developer_task_id: 4,
    pr_monitor_run_id: null,
    worktree_id: 6,
    title: 'Ship the loop',
    requirements: 'Implement it',
    requirements_hash: 'requirements',
    policy_hash: 'policy',
    base_branch: 'main',
    delivery_branch: 'ccm/delivery/7-ship-the-loop',
    workspace_path: '/srv/repo/.claude-manager/worktrees/delivery-7',
    base_sha: 'a'.repeat(40),
    head_sha: 'b'.repeat(40),
    head_tree_sha: 'c'.repeat(40),
    patch_sha256: 'd'.repeat(64),
    head_generation: 1,
    pr_number: null,
    pr_url: null,
    phase: 'pre_review',
    activity: 'running',
    outcome: null,
    terminal: 'ready_to_merge',
    wait_reason: null,
    pause_reason: null,
    error_code: null,
    error_message: null,
    state_version: 8,
    current_cycle_id: 9,
    cycle_count: 2,
    turn_count: 2,
    max_cycles: 10,
    no_progress_count: 0,
    max_no_progress: 3,
    next_reconcile_at: null,
    created_at: '2026-08-05T00:00:00Z',
    updated_at: '2026-08-05T00:05:00Z',
    completed_at: null,
    allowed_actions: ['pause', 'cancel'],
    ...overrides,
  };
}

describe('DeliveryRunPanel', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(api.getDeliveryRun).mockResolvedValue(makeRun());
    vi.mocked(api.pauseDeliveryRun).mockResolvedValue(makeRun({
      activity: 'paused',
      pause_reason: 'Investigate flaky CI',
      allowed_actions: ['resume', 'cancel'],
    }));
  });

  it('shows exact pre-publication status and only server-allowed actions', async () => {
    render(<DeliveryRunPanel runId={7} />);

    expect(await screen.findByText('Pre Review · Running')).toBeInTheDocument();
    expect(screen.getByText('Round 2 of 10 · 2 developer turns')).toBeInTheDocument();
    expect(screen.queryByRole('link')).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Pause' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Cancel' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Resume' })).not.toBeInTheDocument();
  });

  it('renders only actions in compact mode', async () => {
    render(<DeliveryRunPanel runId={7} compact />);

    expect(await screen.findByRole('button', { name: 'Pause' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Cancel' })).toBeInTheDocument();
    expect(screen.queryByText('Delivery Run #7')).not.toBeInTheDocument();
    expect(screen.queryByText('Round 2 of 10 · 2 developer turns')).not.toBeInTheDocument();
  });

  it('requires an explicit reason and confirmation before pausing', async () => {
    render(<DeliveryRunPanel runId={7} />);

    await userEvent.click(await screen.findByRole('button', { name: 'Pause' }));
    const confirmButton = screen.getByRole('button', { name: 'Confirm Pause' });
    expect(confirmButton).toBeDisabled();
    await userEvent.type(screen.getByLabelText('Pause reason'), 'Investigate flaky CI');
    await userEvent.click(confirmButton);

    await waitFor(() => {
      expect(api.pauseDeliveryRun).toHaveBeenCalledWith(7, 'Investigate flaky CI');
    });
    expect(await screen.findByText('Pre Review · Paused')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Resume' })).toBeInTheDocument();
  });

  it('resumes after the in-app confirmation without a browser dialog', async () => {
    const paused = makeRun({
      activity: 'paused',
      pause_reason: 'Workspace validation recovered',
      allowed_actions: ['resume', 'cancel'],
    });
    const resumed = makeRun({
      phase: 'planning',
      activity: 'ready',
      pause_reason: null,
      allowed_actions: ['pause', 'cancel'],
    });
    vi.mocked(api.getDeliveryRun).mockResolvedValue(paused);
    vi.mocked(api.resumeDeliveryRun).mockResolvedValue(resumed);
    const browserConfirm = vi.spyOn(window, 'confirm');

    render(<DeliveryRunPanel runId={7} compact />);

    await userEvent.click(await screen.findByRole('button', { name: 'Resume' }));
    expect(screen.getByText('Resume this Delivery?')).toBeInTheDocument();
    expect(screen.queryByLabelText('Resume reason')).not.toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Confirm Resume' }));

    await waitFor(() => {
      expect(api.resumeDeliveryRun).toHaveBeenCalledWith(7, undefined);
    });
    expect(browserConfirm).not.toHaveBeenCalled();
    expect(await screen.findByRole('button', { name: 'Pause' })).toBeInTheDocument();
  });

  it('marks published monitoring as observation-only without safe-point controls', async () => {
    vi.mocked(api.getDeliveryRun).mockResolvedValue(makeRun({
      pr_monitor_run_id: 5,
      pr_number: 19,
      pr_url: 'https://github.com/acme/repo/pull/19',
      phase: 'monitoring',
      activity: 'waiting',
      wait_reason: 'pr_monitor',
      allowed_actions: [],
    }));

    render(<DeliveryRunPanel runId={7} />);

    expect(await screen.findByText('Monitoring · Waiting')).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'PR #19' })).toHaveAttribute(
      'href',
      'https://github.com/acme/repo/pull/19',
    );
    expect(screen.getByText(/observation-only/)).toBeInTheDocument();
    expect(screen.queryByText(/next safe point/)).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Pause' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Cancel' })).not.toBeInTheDocument();
  });

  it('distinguishes a confirmed automatic merge from manual readiness', async () => {
    vi.mocked(api.getDeliveryRun).mockResolvedValue(makeRun({
      phase: 'done',
      activity: 'terminal',
      outcome: 'success',
      terminal: 'merged',
      completed_at: '2026-08-05T00:10:00Z',
      allowed_actions: [],
    }));

    render(<DeliveryRunPanel runId={7} />);

    expect(await screen.findByText('Merged')).toBeInTheDocument();
    expect(screen.queryByText('Ready to Merge')).not.toBeInTheDocument();
  });

  it('retries a failed pre-publication run from Plan without recreating it', async () => {
    const failed = makeRun({
      phase: 'done',
      activity: 'terminal',
      outcome: 'failed',
      error_code: 'plan_run_failed',
      error_message: 'Both reviewer routes were unavailable',
      state_version: 11,
      completed_at: '2026-08-05T00:10:00Z',
      allowed_actions: ['retry'],
    });
    const retried = makeRun({
      phase: 'planning',
      activity: 'ready',
      outcome: null,
      error_code: null,
      error_message: null,
      state_version: 12,
      cycle_count: 3,
      completed_at: null,
      allowed_actions: ['pause', 'cancel'],
    });
    vi.mocked(api.getDeliveryRun).mockResolvedValue(failed);
    vi.mocked(api.retryDeliveryRun).mockResolvedValue(retried);

    render(<DeliveryRunPanel runId={7} />);

    await userEvent.click(
      await screen.findByRole('button', { name: 'Retry failed step' }),
    );
    expect(screen.getByRole('button', { name: 'Confirm Retry failed step' })).toBeEnabled();
    await userEvent.type(
      screen.getByLabelText('Retry failed step reason (optional)'),
      'Provider routes recovered',
    );
    await userEvent.click(
      screen.getByRole('button', { name: 'Confirm Retry failed step' }),
    );

    await waitFor(() => {
      expect(api.retryDeliveryRun).toHaveBeenCalledWith(
        7,
        11,
        'Provider routes recovered',
      );
    });
    expect(await screen.findByText('Planning · Ready')).toBeInTheDocument();
    expect(screen.getByText('Round 3 of 10 · 2 developer turns')).toBeInTheDocument();
  });
});
