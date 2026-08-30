import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { cleanup, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { InstanceGrid } from './InstanceGrid';
import type { Instance } from '../../api/client';

const apiMock = vi.hoisted(() => ({
  dispatcherStatus: vi.fn(),
  createInstance: vi.fn(),
  deleteInstance: vi.fn(),
  cleanupInstances: vi.fn(),
  stopInstance: vi.fn(),
  startDispatcher: vi.fn(),
  stopDispatcher: vi.fn(),
}));

vi.mock('../../api/client', () => ({ api: apiMock }));

function instance(overrides: Partial<Instance> = {}): Instance {
  return {
    id: 1,
    name: 'worker-1',
    pid: null,
    status: 'idle',
    current_task_id: null,
    current_task_retry_count: null,
    current_task_turn_generation: null,
    current_plan_run_id: null,
    worktree_path: null,
    provider: 'codex',
    model: 'gpt-5.6-sol',
    effort_level: null,
    thinking_budget: null,
    system_prompt_mode: null,
    total_tasks_completed: 0,
    total_cost_usd: 0,
    started_at: null,
    last_heartbeat: null,
    ...overrides,
  };
}

describe('InstanceGrid safety controls', () => {
  beforeEach(() => {
    apiMock.dispatcherStatus.mockResolvedValue({ running: true, active_tasks: {} });
    apiMock.createInstance.mockResolvedValue(instance());
    apiMock.deleteInstance.mockResolvedValue({ ok: true });
    apiMock.cleanupInstances.mockResolvedValue({ ok: true, deleted: 0, skipped_running: [] });
    apiMock.stopInstance.mockResolvedValue({ ok: true });
    apiMock.startDispatcher.mockResolvedValue({ ok: true });
    apiMock.stopDispatcher.mockResolvedValue({ ok: true });
  });

  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
    vi.clearAllMocks();
  });

  it('labels dispatcher stop as pause and explains that running tasks continue', async () => {
    render(<InstanceGrid instances={[]} onRefresh={vi.fn()} onViewLogs={vi.fn()} />);

    expect(await screen.findByRole('button', { name: 'Pause Dispatcher' })).toBeInTheDocument();
    expect(screen.getByText(/Tasks that are already running continue normally/i)).toBeInTheDocument();
  });

  it('requires confirmation before pausing and refreshes status from the server', async () => {
    const user = userEvent.setup();
    const confirm = vi.spyOn(window, 'confirm').mockReturnValue(true);
    apiMock.dispatcherStatus
      .mockResolvedValueOnce({ running: true, active_tasks: {} })
      .mockResolvedValueOnce({ running: false, active_tasks: {} });
    const refresh = vi.fn();
    render(<InstanceGrid instances={[]} onRefresh={refresh} onViewLogs={vi.fn()} />);

    await user.click(await screen.findByRole('button', { name: 'Pause Dispatcher' }));

    expect(confirm).toHaveBeenCalledWith(expect.stringContaining('Running tasks will continue'));
    expect(apiMock.stopDispatcher).toHaveBeenCalledOnce();
    expect(await screen.findByRole('button', { name: 'Start Dispatcher' })).toBeInTheDocument();
    expect(refresh).toHaveBeenCalledOnce();
  });

  it('does not allow a running instance to be deleted', async () => {
    const user = userEvent.setup();
    render(
      <InstanceGrid
        instances={[instance({
          status: 'running', pid: 123, current_task_id: 9,
          current_task_retry_count: 2, current_task_turn_generation: 7,
        })]}
        onRefresh={vi.fn()}
        onViewLogs={vi.fn()}
      />,
    );

    const deleteButton = screen.getByRole('button', { name: 'Delete worker-1' });
    expect(deleteButton).toBeDisabled();
    await user.click(deleteButton);
    expect(apiMock.deleteInstance).not.toHaveBeenCalled();
  });

  it('does not allow orphan process metadata to be deleted', async () => {
    const user = userEvent.setup();
    render(
      <InstanceGrid
        instances={[instance({ status: 'error', pid: 76543, current_task_id: 9 })]}
        onRefresh={vi.fn()}
        onViewLogs={vi.fn()}
      />,
    );

    const deleteButton = screen.getByRole('button', { name: 'Delete worker-1' });
    expect(deleteButton).toBeDisabled();
    await user.click(deleteButton);
    expect(apiMock.deleteInstance).not.toHaveBeenCalled();
  });

  it('confirms an idle instance deletion and surfaces API failures', async () => {
    const user = userEvent.setup();
    vi.spyOn(window, 'confirm').mockReturnValue(true);
    apiMock.deleteInstance.mockRejectedValueOnce(new Error('instance changed owner'));
    render(
      <InstanceGrid instances={[instance()]} onRefresh={vi.fn()} onViewLogs={vi.fn()} />,
    );

    await user.click(screen.getByRole('button', { name: 'Delete worker-1' }));

    expect(apiMock.deleteInstance).toHaveBeenCalledWith(1);
    expect(await screen.findByRole('alert')).toHaveTextContent('instance changed owner');
  });

  it('requires confirmation before interrupting a running instance', async () => {
    const user = userEvent.setup();
    vi.spyOn(window, 'confirm').mockReturnValue(false);
    render(
      <InstanceGrid
        instances={[instance({
          status: 'running', pid: 123, current_task_id: 9,
          current_task_retry_count: 2, current_task_turn_generation: 7,
        })]}
        onRefresh={vi.fn()}
        onViewLogs={vi.fn()}
      />,
    );

    await user.click(screen.getByRole('button', { name: 'Stop worker-1' }));
    expect(apiMock.stopInstance).not.toHaveBeenCalled();
  });

  it('submits the observed task owner when stopping a reused slot', async () => {
    const user = userEvent.setup();
    vi.spyOn(window, 'confirm').mockReturnValue(true);
    render(
      <InstanceGrid
        instances={[instance({
          status: 'running', pid: 123, current_task_id: 9,
          current_task_retry_count: 2, current_task_turn_generation: 7,
        })]}
        onRefresh={vi.fn()}
        onViewLogs={vi.fn()}
      />,
    );

    await user.click(screen.getByRole('button', { name: 'Stop worker-1' }));
    await waitFor(() =>
      expect(apiMock.stopInstance).toHaveBeenCalledWith(1, 9, 7, 123, null),
    );
  });

  it('fails closed when the current task turn observation is unavailable', async () => {
    const user = userEvent.setup();
    const confirm = vi.spyOn(window, 'confirm').mockReturnValue(true);
    render(
      <InstanceGrid
        instances={[instance({
          status: 'running', pid: 123, current_task_id: 9,
          current_task_retry_count: 2, current_task_turn_generation: null,
        })]}
        onRefresh={vi.fn()}
        onViewLogs={vi.fn()}
      />,
    );

    const stop = screen.getByRole('button', { name: 'Stop worker-1' });
    expect(stop).toBeDisabled();
    await user.click(stop);
    expect(confirm).not.toHaveBeenCalled();
    expect(apiMock.stopInstance).not.toHaveBeenCalled();
  });

  it('reuses the first free default worker name instead of the array length', async () => {
    const user = userEvent.setup();
    render(
      <InstanceGrid
        instances={[instance({ id: 2, name: 'worker-2' })]}
        onRefresh={vi.fn()}
        onViewLogs={vi.fn()}
      />,
    );

    await user.click(screen.getByRole('button', { name: 'Add' }));
    await waitFor(() => expect(apiMock.createInstance).toHaveBeenCalledWith({ name: 'worker-1' }));
  });
});
