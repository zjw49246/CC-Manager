import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { ProjectTodoList } from './ProjectTodoList';

vi.mock('../../api/client', () => ({
  api: {
    listProjectTodos: vi.fn(),
    createProjectTodo: vi.fn(),
    updateProjectTodo: vi.fn(),
    deleteProjectTodo: vi.fn(),
    createTask: vi.fn(),
    createTaskFromProjectTodo: vi.fn(),
    createDeliveryRun: vi.fn(),
    getMonitoredRepos: vi.fn(),
    config: vi.fn(() => Promise.resolve({ provider_options: ['claude', 'codex'], default_provider: 'claude' })),
  },
}));

import { api } from '../../api/client';

const todo = {
  id: 5,
  project_id: 7,
  title: 'Refactor auth',
  prompt: 'Inspect auth module first.',
  status: 'open' as const,
  sort_order: 100,
  created_task_id: null,
  created_at: '2026-06-22T00:00:00Z',
  updated_at: '2026-06-22T00:00:00Z',
};

const localProject = {
  id: 7,
  name: 'repo',
  worker_id: null,
  git_url: 'git@github.com:owner/repo.git',
  has_remote: true,
  local_path: '/srv/repo',
  default_branch: 'main',
  status: 'ready',
  error_message: null,
  show_in_selector: true,
  sort_order: 0,
  tags: [],
  env_files: [],
  git_author_name: null,
  git_author_email: null,
  git_credential_type: null,
  git_ssh_key_path: null,
  git_https_username: null,
  git_https_token: null,
  badge_color: null,
  created_at: '2026-08-05T00:00:00Z',
};

const monitoredRepo = {
  id: 9,
  repo_full_name: 'owner/repo',
  project_id: 7,
  worker_id: null,
  enabled: true,
  auto_merge: true,
  auto_repair: true,
  max_repair_attempts: 3,
  provider: 'claude',
  review_model: null,
  review_effort: null,
  review_mode: 'panel' as const,
  wait_for_ci: true,
  required_checks: [{ kind: 'check_run' as const, name: 'test', app_slug: 'github-actions' }],
  merge_queue_mode: 'manual' as const,
  default_branch: 'main',
  allowed_authors: [],
  status: 'active',
  error_message: null,
  created_at: '2026-08-05T00:00:00Z',
  updated_at: '2026-08-05T00:00:00Z',
};

describe('ProjectTodoList', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    sessionStorage.clear();
    vi.mocked(api.listProjectTodos).mockResolvedValue([]);
    vi.mocked(api.createProjectTodo).mockResolvedValue(todo);
    vi.mocked(api.updateProjectTodo).mockResolvedValue({ ...todo, status: 'done' });
    vi.mocked(api.deleteProjectTodo).mockResolvedValue({ ok: true });
    vi.mocked(api.createTask).mockResolvedValue({ id: 42 } as Awaited<ReturnType<typeof api.createTask>>);
    vi.mocked(api.createTaskFromProjectTodo).mockResolvedValue({ id: 42 } as Awaited<ReturnType<typeof api.createTaskFromProjectTodo>>);
    vi.mocked(api.createDeliveryRun).mockResolvedValue({
      id: 10,
      developer_task_id: 43,
    } as Awaited<ReturnType<typeof api.createDeliveryRun>>);
    vi.mocked(api.getMonitoredRepos).mockResolvedValue([]);
    vi.mocked(api.config).mockResolvedValue({
      provider_options: ['claude', 'codex'],
      default_provider: 'claude',
      delivery_loop_enabled: false,
    } as never);
    window.location.hash = '';
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  async function openDeliveryTaskModal() {
    await userEvent.click(screen.getByTitle('Expand todos'));
    await screen.findByText('Refactor auth');
    await userEvent.click(screen.getByTitle('Create task'));
    const dialog = screen.getByRole('dialog', { name: 'Create task' });
    await userEvent.selectOptions(
      await within(dialog).findByLabelText('Task mode'),
      'delivery_loop',
    );
    await userEvent.selectOptions(
      await within(dialog).findByLabelText('Delivery PR Monitor repository'),
      '9',
    );
    return dialog;
  }

  it('creates a project todo from the add modal', async () => {
    render(<ProjectTodoList projectId={7} project={localProject} />);

    await userEvent.click(screen.getByTitle('Add todo'));
    const dialog = screen.getByRole('dialog', { name: 'New todo' });
    await userEvent.type(within(dialog).getByLabelText('Title'), 'Refactor auth');
    await userEvent.type(within(dialog).getByLabelText('Prompt'), 'Inspect auth module first.');
    await userEvent.click(within(dialog).getByRole('button', { name: 'Create todo' }));

    await waitFor(() => {
      expect(api.createProjectTodo).toHaveBeenCalledWith(7, {
        title: 'Refactor auth',
        prompt: 'Inspect auth module first.',
      });
    });
  });

  it('surfaces a submit error inside the modal (not hidden behind the overlay)', async () => {
    vi.mocked(api.createProjectTodo).mockRejectedValue(new Error('Boom: server rejected'));
    render(<ProjectTodoList projectId={7} />);

    await userEvent.click(screen.getByTitle('Add todo'));
    const dialog = screen.getByRole('dialog', { name: 'New todo' });
    await userEvent.type(within(dialog).getByLabelText('Title'), 'X');
    await userEvent.type(within(dialog).getByLabelText('Prompt'), 'Y');
    await userEvent.click(within(dialog).getByRole('button', { name: 'Create todo' }));

    // The modal stays open AND the error is visible within it.
    await waitFor(() => {
      expect(within(dialog).getByText(/Boom: server rejected/)).toBeInTheDocument();
    });
  });

  it('creates a task from a todo after allowing prompt edits', async () => {
    vi.mocked(api.listProjectTodos).mockResolvedValue([todo]);
    render(<ProjectTodoList projectId={7} project={localProject} />);

    await userEvent.click(screen.getByTitle('Expand todos'));
    expect(await screen.findByText('Refactor auth')).toBeInTheDocument();

    await userEvent.click(screen.getByTitle('Create task'));
    const dialog = screen.getByRole('dialog', { name: 'Create task' });
    const prompt = within(dialog).getByLabelText('Prompt');
    await userEvent.clear(prompt);
    await userEvent.type(prompt, 'Write a focused patch.');
    await userEvent.click(within(dialog).getByRole('button', { name: 'Create task' }));

    await waitFor(() => {
      expect(api.createTaskFromProjectTodo).toHaveBeenCalledWith(7, 5, {
        title: 'Refactor auth',
        prompt: 'Write a focused patch.',
        provider: 'claude',
      });
    });
    expect(api.createTask).not.toHaveBeenCalled();
    expect(api.updateProjectTodo).not.toHaveBeenCalled();
    expect(window.location.hash).toBe('#/tasks/chat/42');
  });

  it('can atomically start a Delivery Run with todo provenance', async () => {
    vi.mocked(api.listProjectTodos).mockResolvedValue([todo]);
    vi.mocked(api.config).mockResolvedValue({
      provider_options: ['claude', 'codex'],
      default_provider: 'codex',
      delivery_loop_enabled: true,
    } as never);
    vi.mocked(api.getMonitoredRepos).mockResolvedValue([monitoredRepo]);
    render(<ProjectTodoList projectId={7} project={localProject} />);

    const dialog = await openDeliveryTaskModal();
    const providerSelect = within(dialog).getByLabelText('Task provider');
    expect(providerSelect).toHaveValue('codex');
    expect(providerSelect).not.toBeDisabled();
    expect(within(providerSelect).getByRole('option', { name: 'Claude Code' })).toBeInTheDocument();
    expect(within(providerSelect).getByRole('option', { name: 'Codex' })).toBeInTheDocument();
    expect(within(dialog).getByText(/PR Monitor Auto Merge is ON/)).toHaveTextContent(
      'completion waits for GitHub to confirm the merge',
    );
    await userEvent.click(within(dialog).getByRole('button', { name: 'Create task' }));

    await waitFor(() => expect(api.createDeliveryRun).toHaveBeenCalledWith({
      idempotency_key: expect.any(String),
      project_id: 7,
      monitored_repo_id: 9,
      source_todo_id: 5,
      title: 'Refactor auth',
      requirements: 'Inspect auth module first.',
      provider: 'codex',
    }));
    expect(api.createTask).not.toHaveBeenCalled();
    expect(api.updateProjectTodo).not.toHaveBeenCalled();
    expect(window.location.hash).toBe('#/tasks/chat/43');
  });

  it('starts a Claude Delivery Run when Claude is the only configured provider', async () => {
    vi.mocked(api.listProjectTodos).mockResolvedValue([todo]);
    vi.mocked(api.config).mockResolvedValue({
      provider_options: ['claude'],
      default_provider: 'claude',
      delivery_loop_enabled: true,
    } as never);
    vi.mocked(api.getMonitoredRepos).mockResolvedValue([monitoredRepo]);
    render(<ProjectTodoList projectId={7} project={localProject} />);

    const dialog = await openDeliveryTaskModal();
    const providerSelect = within(dialog).getByLabelText('Task provider');
    expect(providerSelect).toHaveValue('claude');
    expect(within(providerSelect).getAllByRole('option')).toHaveLength(1);
    expect(within(providerSelect).queryByRole('option', { name: 'Codex' })).not.toBeInTheDocument();
    await userEvent.click(within(dialog).getByRole('button', { name: 'Create task' }));

    await waitFor(() => expect(api.createDeliveryRun).toHaveBeenCalledOnce());
    expect(vi.mocked(api.createDeliveryRun).mock.calls[0][0]).toEqual(expect.objectContaining({
      project_id: 7,
      monitored_repo_id: 9,
      source_todo_id: 5,
      provider: 'claude',
    }));
    expect(window.location.hash).toBe('#/tasks/chat/43');
  });

  it('reuses a failed Delivery admission key after the Todo list remounts', async () => {
    vi.mocked(api.listProjectTodos).mockResolvedValue([todo]);
    vi.mocked(api.config).mockResolvedValue({
      provider_options: ['claude', 'codex'],
      default_provider: 'codex',
      delivery_loop_enabled: true,
    } as never);
    vi.mocked(api.getMonitoredRepos).mockResolvedValue([monitoredRepo]);
    vi.mocked(api.createDeliveryRun)
      .mockRejectedValueOnce(new Error('response lost'))
      .mockResolvedValue({ id: 10, developer_task_id: 43 } as never);

    const firstRender = render(<ProjectTodoList projectId={7} project={localProject} />);
    let dialog = await openDeliveryTaskModal();
    await userEvent.click(within(dialog).getByRole('button', { name: 'Create task' }));
    await within(dialog).findByText('Error: response lost');
    const firstKey = vi.mocked(api.createDeliveryRun).mock.calls[0][0].idempotency_key;
    firstRender.unmount();

    render(<ProjectTodoList projectId={7} project={localProject} />);
    dialog = await openDeliveryTaskModal();
    await userEvent.click(within(dialog).getByRole('button', { name: 'Create task' }));
    await waitFor(() => expect(api.createDeliveryRun).toHaveBeenCalledTimes(2));

    expect(vi.mocked(api.createDeliveryRun).mock.calls[1][0].idempotency_key).toBe(firstKey);
    expect(window.location.hash).toBe('#/tasks/chat/43');
  });

  it('recovers an atomically claimed Todo after an ambiguous create response', async () => {
    const claimed = { ...todo, created_task_id: 88 };
    vi.mocked(api.listProjectTodos)
      .mockResolvedValueOnce([todo])
      .mockResolvedValue([claimed]);
    vi.mocked(api.config).mockResolvedValue({
      provider_options: ['claude', 'codex'],
      default_provider: 'codex',
      delivery_loop_enabled: true,
    } as never);
    vi.mocked(api.getMonitoredRepos).mockResolvedValue([monitoredRepo]);
    vi.mocked(api.createDeliveryRun).mockRejectedValueOnce(new Error('response lost'));
    render(<ProjectTodoList projectId={7} project={localProject} />);

    const dialog = await openDeliveryTaskModal();
    await userEvent.click(within(dialog).getByRole('button', { name: 'Create task' }));

    await waitFor(() => expect(window.location.hash).toBe('#/tasks/chat/88'));
    expect(screen.queryByText('Error: response lost')).not.toBeInTheDocument();
  });

  it('opens claimed Todos read-only instead of offering rejected mutations', async () => {
    vi.mocked(api.listProjectTodos).mockResolvedValue([{ ...todo, created_task_id: 42 }]);
    render(<ProjectTodoList projectId={7} project={localProject} />);

    await userEvent.click(screen.getByTitle('Expand todos'));
    await screen.findByText('Refactor auth');

    expect(screen.queryByTitle('Mark done')).not.toBeInTheDocument();
    expect(screen.queryByTitle('Edit todo')).not.toBeInTheDocument();
    expect(screen.queryByTitle('Archive todo')).not.toBeInTheDocument();
    expect(screen.queryByText(/Run again/)).not.toBeInTheDocument();
    await userEvent.click(screen.getByTitle('Open created task #42'));

    expect(window.location.hash).toBe('#/tasks/chat/42');
    expect(api.updateProjectTodo).not.toHaveBeenCalled();
    expect(api.deleteProjectTodo).not.toHaveBeenCalled();
    expect(api.createTaskFromProjectTodo).not.toHaveBeenCalled();
    expect(api.createDeliveryRun).not.toHaveBeenCalled();
  });

  it('archives todos via a soft status update, not a hard delete', async () => {
    vi.mocked(api.listProjectTodos).mockResolvedValue([todo]);
    vi.spyOn(window, 'confirm').mockReturnValue(true);
    render(<ProjectTodoList projectId={7} />);

    await userEvent.click(screen.getByTitle('Expand todos'));
    expect(await screen.findByText('Refactor auth')).toBeInTheDocument();
    await userEvent.click(screen.getByTitle('Archive todo'));

    await waitFor(() => {
      expect(api.updateProjectTodo).toHaveBeenCalledWith(7, 5, { status: 'archived' });
      expect(screen.queryByText('Refactor auth')).not.toBeInTheDocument();
    });
    expect(api.deleteProjectTodo).not.toHaveBeenCalled();
  });

  it('reveals archived todos via the toggle even when none are loaded yet', async () => {
    const archived = { ...todo, id: 9, title: 'Archived item', status: 'archived' as const };
    // Default view (open/done) is empty; archived list returns the archived todo.
    vi.mocked(api.listProjectTodos).mockImplementation(async (_projectId, includeArchived) =>
      includeArchived ? [archived] : [],
    );
    render(<ProjectTodoList projectId={7} />);

    await userEvent.click(screen.getByTitle('Expand todos'));
    // The toggle must be reachable with zero archived todos loaded.
    const toggle = await screen.findByTitle('Show archived todos');
    await userEvent.click(toggle);

    await waitFor(() => {
      expect(api.listProjectTodos).toHaveBeenCalledWith(7, true);
      expect(screen.getByText('Archived item')).toBeInTheDocument();
    });
    // Archived rows expose Restore + Delete-permanently.
    expect(screen.getByTitle('Restore todo')).toBeInTheDocument();
    expect(screen.getByTitle('Delete permanently')).toBeInTheDocument();
  });

  it('does not expose restore or delete for an archived claimed Todo', async () => {
    const claimedArchived = {
      ...todo,
      id: 12,
      title: 'Claimed archive',
      status: 'archived' as const,
      created_task_id: 77,
    };
    vi.mocked(api.listProjectTodos).mockImplementation(async (_projectId, includeArchived) => (
      includeArchived ? [claimedArchived] : []
    ));
    render(<ProjectTodoList projectId={7} project={localProject} />);

    await userEvent.click(screen.getByTitle('Expand todos'));
    await userEvent.click(await screen.findByTitle('Show archived todos'));
    await screen.findByText('Claimed archive');

    expect(screen.queryByTitle('Restore todo')).not.toBeInTheDocument();
    expect(screen.queryByTitle('Delete permanently')).not.toBeInTheDocument();
    await userEvent.click(screen.getByTitle('Open created task #77'));
    expect(window.location.hash).toBe('#/tasks/chat/77');
  });
});
