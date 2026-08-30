import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { api } from '../../api/client';
import type { MonitorSession } from '../../api/client';
import { MonitorPanel } from './MonitorPanel';

vi.mock('../../api/client', () => ({
  api: {
    listAllSubAgentSessions: vi.fn(() => Promise.resolve([])),
    getSubAgentReports: vi.fn(() => Promise.resolve([])),
    deleteMonitorSession: vi.fn(() => Promise.resolve({ ok: true })),
    deleteSubAgentSession: vi.fn(() => Promise.resolve({ ok: true })),
  },
}));

const baseProps = {
  taskId: 1,
  sessions: [],
  onSessionsChange: vi.fn(),
  onClose: vi.fn(),
};

describe('MonitorPanel codex annotation', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('fails closed when the Codex Monitor capability is unknown', () => {
    render(<MonitorPanel {...baseProps} provider="codex" />);
    expect(
      screen.getByText(/Codex Monitor 当前仅支持 capability/),
    ).toBeInTheDocument();
  });

  it('shows no notice for a confirmed local Codex task', () => {
    render(
      <MonitorPanel
        {...baseProps}
        provider="codex"
        monitorSupported
      />,
    );
    expect(
      screen.queryByText(/Codex Monitor 当前仅支持 capability/),
    ).not.toBeInTheDocument();
  });

  it('shows no notice for claude tasks', () => {
    render(<MonitorPanel {...baseProps} provider="claude" />);
    expect(
      screen.queryByText(/Codex Monitor 当前仅支持 capability/),
    ).not.toBeInTheDocument();
  });

  it('shows no notice when provider is omitted', () => {
    render(<MonitorPanel {...baseProps} />);
    expect(
      screen.queryByText(/暂不支持 Codex/),
    ).not.toBeInTheDocument();
  });

  it('allows an existing internal Codex Monitor to be stopped', async () => {
    const session: MonitorSession = {
      id: 7,
      task_id: 1,
      agent_type: 'monitor',
      source: 'ccm',
      description: 'PR7B1 active-turn stop fixture',
      monitor_context: null,
      interval: 300,
      max_checks: 10,
      model: 'gpt-5.6-sol',
      provider: 'codex',
      status: 'running',
      checks_done: 0,
      last_summary: null,
      next_check_at: null,
      turn_generation: 1,
      active_turn_generation: 1,
      consecutive_failures: 0,
      last_error: null,
      codex_cleanup_pending: false,
      codex_cleanup_error: null,
      created_at: '2026-07-29T00:00:00Z',
      completed_at: null,
    };

    render(
      <MonitorPanel
        {...baseProps}
        provider="codex"
        sessions={[session]}
      />,
    );
    await userEvent.click(screen.getByTitle('Stop monitor'));

    expect(api.deleteMonitorSession).toHaveBeenCalledWith(1, 7);
  });

  it('shows and retries durable Codex cleanup failures', async () => {
    const session: MonitorSession = {
      id: 8,
      task_id: 1,
      agent_type: 'monitor',
      source: 'ccm',
      description: 'terminal cleanup fixture',
      monitor_context: null,
      interval: 300,
      max_checks: 10,
      model: 'gpt-5.6-sol',
      provider: 'codex',
      status: 'cancelled',
      checks_done: 0,
      last_summary: null,
      next_check_at: null,
      turn_generation: 1,
      active_turn_generation: null,
      consecutive_failures: 0,
      last_error: null,
      codex_cleanup_pending: true,
      codex_cleanup_error: 'thread/delete transport unavailable',
      created_at: '2026-07-29T00:00:00Z',
      completed_at: '2026-07-29T00:01:00Z',
    };

    render(
      <MonitorPanel
        {...baseProps}
        provider="codex"
        sessions={[session]}
      />,
    );

    expect(
      screen.getByText('Codex runtime cleanup pending'),
    ).toBeInTheDocument();
    expect(
      screen.getByText(/thread\/delete transport unavailable/),
    ).toBeInTheDocument();
    await userEvent.click(screen.getByTitle('Retry Codex cleanup'));

    expect(api.deleteMonitorSession).toHaveBeenCalledWith(1, 8);
  });

  it('refreshes the durable row when Stop reports cleanup failure', async () => {
    const session: MonitorSession = {
      id: 9,
      task_id: 1,
      agent_type: 'monitor',
      source: 'ccm',
      description: 'cleanup transition fixture',
      monitor_context: null,
      interval: 300,
      max_checks: 10,
      model: 'gpt-5.6-sol',
      provider: 'codex',
      status: 'running',
      checks_done: 0,
      last_summary: null,
      next_check_at: null,
      turn_generation: 1,
      active_turn_generation: 1,
      consecutive_failures: 0,
      last_error: null,
      codex_cleanup_pending: false,
      codex_cleanup_error: null,
      created_at: '2026-07-29T00:00:00Z',
      completed_at: null,
    };
    vi.mocked(api.deleteMonitorSession).mockRejectedValueOnce(
      new Error('409 cleanup pending'),
    );

    render(
      <MonitorPanel
        {...baseProps}
        provider="codex"
        sessions={[session]}
      />,
    );
    await waitFor(() => {
      expect(api.listAllSubAgentSessions).toHaveBeenCalled();
    });
    vi.mocked(api.listAllSubAgentSessions).mockClear();

    await userEvent.click(screen.getByTitle('Stop monitor'));

    await waitFor(() => {
      expect(api.listAllSubAgentSessions).toHaveBeenCalledTimes(1);
    });
  });

  it('shows provider-native children and their reports in the unified detail panel', async () => {
    const session: MonitorSession = {
      id: 10,
      task_id: 1,
      agent_type: 'native-agent',
      source: 'native',
      description: 'inspect scheduler',
      monitor_context: null,
      interval: 0,
      max_checks: 0,
      model: 'gpt-5.6-sol',
      provider: 'codex',
      status: 'running',
      checks_done: 1,
      last_summary: 'scheduler inspected',
      next_check_at: null,
      turn_generation: 0,
      active_turn_generation: null,
      consecutive_failures: 0,
      last_error: null,
      codex_cleanup_pending: false,
      codex_cleanup_error: null,
      created_at: '2026-08-15T00:00:00Z',
      completed_at: null,
    };
    vi.mocked(api.getSubAgentReports).mockResolvedValueOnce([{
      id: 101,
      monitor_session_id: 10,
      check_number: 1,
      status: 'running',
      summary: 'scheduler inspected',
      full_output: null,
      created_at: '2026-08-15T00:00:01Z',
    }]);

    render(<MonitorPanel {...baseProps} sessions={[session]} />);

    expect(screen.getByText('native-agent')).toBeInTheDocument();
    expect(screen.getByText('inspect scheduler')).toBeInTheDocument();
    expect(screen.getByText(/scheduler inspected/)).toBeInTheDocument();
    expect(screen.queryByTitle('Stop monitor')).not.toBeInTheDocument();

    const row = screen.getByText('inspect scheduler').closest('.border');
    expect(row).not.toBeNull();
    await userEvent.click(within(row as HTMLElement).getAllByRole('button')[0]);
    await waitFor(() => {
      expect(api.getSubAgentReports).toHaveBeenCalledWith(1, 10);
      expect(screen.getByText('#1')).toBeInTheDocument();
    });
  });
});
