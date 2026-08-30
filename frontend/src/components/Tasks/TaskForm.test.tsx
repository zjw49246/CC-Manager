import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor, act, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { TaskForm } from './TaskForm';

vi.mock('../../api/client', () => ({
  api: {
    listProjects: vi.fn().mockResolvedValue([
      { id: 1, name: 'test-project', git_url: '', has_remote: false, local_path: '/tmp/test', status: 'ready', show_in_selector: true, tags: [], sort_order: 0, badge_color: null, env_files: [] },
    ]),
    listTags: vi.fn().mockResolvedValue([]),
    listSecrets: vi.fn().mockResolvedValue([]),
    listSSHProfiles: vi.fn().mockResolvedValue([]),
    listTasks: vi.fn().mockResolvedValue([]),
    config: vi.fn().mockResolvedValue({
      default_provider: 'claude',
      provider_options: ['claude', 'codex'],
      default_model: 'claude-opus-4-6',
      model_options: ['claude-opus-4-6', 'claude-sonnet-4-6'],
      default_codex_model: 'gpt-5.5',
      codex_model_options: ['gpt-5.6-sol', 'gpt-5.6-terra', 'gpt-5.6-luna', 'gpt-5.5', 'gpt-5.4-mini'],
      default_effort: 'medium',
      effort_options: ['low', 'medium', 'high'],
      codex_effort_options: ['low', 'medium', 'high', 'xhigh'],
      codex_model_efforts: {
        'gpt-5.6-sol': ['low', 'medium', 'high', 'xhigh', 'max', 'ultra'],
        'gpt-5.6-terra': ['low', 'medium', 'high', 'xhigh', 'max', 'ultra'],
        'gpt-5.6-luna': ['low', 'medium', 'high', 'xhigh', 'max'],
      },
      codex_model_service_tiers: {
        'gpt-5.6-sol': ['default', 'priority'],
        'gpt-5.6-terra': ['default', 'priority'],
        'gpt-5.6-luna': ['default', 'priority'],
        'gpt-5.5': ['default', 'priority'],
        'gpt-5.4-mini': ['default'],
      },
    }),
    createTask: vi.fn().mockResolvedValue({ id: 1 }),
    createDeliveryRun: vi.fn().mockResolvedValue({ id: 11, developer_task_id: 12 }),
    uploadImages: vi.fn().mockResolvedValue([]),
    getMonitoredRepos: vi.fn().mockResolvedValue([]),
    createProject: vi.fn().mockResolvedValue({ id: 2 }),
    listWorkers: vi.fn().mockResolvedValue([]),
    listSkillsCached: vi.fn().mockResolvedValue([
      { key: 'monitor', label: 'Monitor', description: 'Background monitoring sub-agents' },
      { key: 'sub-agent', label: 'Sub-Agent', description: 'Parallel one-shot sub-agents' },
      { key: 'code-review', label: 'Code Review', description: 'Review code changes' },
    ]),
    listUserSkillsCached: vi.fn().mockResolvedValue([]),
    getDefaultSkills: vi.fn().mockResolvedValue({
      default_enabled_plugins: null,
      default_enabled_user_skills: null,
    }),
    getRuntimeSettings: vi.fn().mockResolvedValue({
      codex_main_mcp_enabled: true,
      codex_monitor_enabled: true,
    }),
    setDefaultSkills: vi.fn().mockResolvedValue({}),
  },
}));

import { api } from '../../api/client';

async function openConfigPanel() {
  // Mode/Model/Effort/Timeout 等选择器位于 Config 下拉面板内
  await userEvent.click(screen.getByText('Config'));
  await waitFor(() => screen.getByDisplayValue('Auto'));
}

async function selectLoopMode() {
  await openConfigPanel();
  const modeSelect = screen.getByDisplayValue('Auto');
  await userEvent.selectOptions(modeSelect, 'loop');
}

async function selectGoalMode() {
  await openConfigPanel();
  const modeSelect = screen.getByDisplayValue('Auto');
  await userEvent.selectOptions(modeSelect, 'goal');
}

async function selectProject() {
  const projectBtn = await waitFor(() => screen.getByText('Select project...'));
  await userEvent.click(projectBtn);
  const projectOption = await waitFor(() => screen.getByText('test-project'));
  await userEvent.click(projectOption);
}

describe('TaskForm number input fields', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('maxIterations (loop mode)', () => {
    it('allows clearing the input field completely', async () => {
      render(<TaskForm onCreated={vi.fn()} />);
      await selectLoopMode();

      const input = screen.getByDisplayValue('50');
      await userEvent.clear(input);

      expect(input).toHaveValue('');
    });

    it('normalizes empty input to 1 on blur', async () => {
      render(<TaskForm onCreated={vi.fn()} />);
      await selectLoopMode();

      const input = screen.getByDisplayValue('50');
      await userEvent.clear(input);
      fireEvent.blur(input);

      expect(input).toHaveValue('1');
    });

    it('allows typing a new value after clearing', async () => {
      render(<TaskForm onCreated={vi.fn()} />);
      await selectLoopMode();

      const input = screen.getByDisplayValue('50');
      await userEvent.clear(input);
      await userEvent.type(input, '5');

      expect(input).toHaveValue('5');
    });

    it('rejects non-numeric characters', async () => {
      render(<TaskForm onCreated={vi.fn()} />);
      await selectLoopMode();

      const input = screen.getByDisplayValue('50');
      await userEvent.clear(input);
      await userEvent.type(input, 'abc12.3xyz');

      expect(input).toHaveValue('123');
    });

    it('submits the displayed value correctly', async () => {
      const onCreated = vi.fn();
      render(<TaskForm onCreated={onCreated} />);
      await selectLoopMode();
      await selectProject();

      const input = screen.getByDisplayValue('50');
      await userEvent.clear(input);
      await userEvent.type(input, '5');

      const todoInput = screen.getByPlaceholderText('Todo file path (e.g. TODO.md)');
      await userEvent.type(todoInput, 'TODO.md');

      const submitBtn = screen.getByRole('button', { name: /create/i });
      await userEvent.click(submitBtn);

      await waitFor(() => {
        expect(api.createTask).toHaveBeenCalledWith(
          expect.objectContaining({ max_iterations: 5 }),
        );
      });
    });

    it('submits 1 when input was cleared (onBlur normalizes before submit)', async () => {
      const onCreated = vi.fn();
      render(<TaskForm onCreated={onCreated} />);
      await selectLoopMode();
      await selectProject();

      const input = screen.getByDisplayValue('50');
      await userEvent.clear(input);

      const todoInput = screen.getByPlaceholderText('Todo file path (e.g. TODO.md)');
      await userEvent.type(todoInput, 'TODO.md');

      const submitBtn = screen.getByRole('button', { name: /create/i });
      await userEvent.click(submitBtn);

      await waitFor(() => {
        expect(api.createTask).toHaveBeenCalledWith(
          expect.objectContaining({ max_iterations: 1 }),
        );
      });
    });

    it('normalizes 0 to 1 on blur', async () => {
      render(<TaskForm onCreated={vi.fn()} />);
      await selectLoopMode();

      const input = screen.getByDisplayValue('50');
      await userEvent.clear(input);
      await userEvent.type(input, '0');
      fireEvent.blur(input);

      expect(input).toHaveValue('1');
    });
  });

  describe('goalMaxTurns (goal mode)', () => {
    it('allows clearing the input field completely', async () => {
      render(<TaskForm onCreated={vi.fn()} />);
      await selectGoalMode();

      const input = screen.getByDisplayValue('30');
      await userEvent.clear(input);

      expect(input).toHaveValue('');
    });

    it('normalizes empty input to 1 on blur', async () => {
      render(<TaskForm onCreated={vi.fn()} />);
      await selectGoalMode();

      const input = screen.getByDisplayValue('30');
      await userEvent.clear(input);
      fireEvent.blur(input);

      expect(input).toHaveValue('1');
    });

    it('allows typing a new value after clearing', async () => {
      render(<TaskForm onCreated={vi.fn()} />);
      await selectGoalMode();

      const input = screen.getByDisplayValue('30');
      await userEvent.clear(input);
      await userEvent.type(input, '10');

      expect(input).toHaveValue('10');
    });

    it('submits the displayed value correctly', async () => {
      const onCreated = vi.fn();
      render(<TaskForm onCreated={onCreated} />);
      await selectGoalMode();
      await selectProject();

      const input = screen.getByDisplayValue('30');
      await userEvent.clear(input);
      await userEvent.type(input, '10');

      const descInput = screen.getByPlaceholderText('Prompt / Description (this will be sent to Claude Code)');
      await userEvent.type(descInput, 'test task');

      const goalInput = screen.getByPlaceholderText('Goal condition (e.g. all tests pass and lint is clean)');
      await userEvent.type(goalInput, 'all tests pass');

      const submitBtn = screen.getByRole('button', { name: /create/i });
      await userEvent.click(submitBtn);

      await waitFor(() => {
        expect(api.createTask).toHaveBeenCalledWith(
          expect.objectContaining({ goal_max_turns: 10 }),
        );
      });
    });
  });
});

describe('TaskForm copy-context-from select overflow fix', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('shows the copy-context-from select when project has tasks with sessions', async () => {
    const tasksWithSession = [
      { id: 10, description: 'A'.repeat(80), session_id: 'sess-1', title: null, project_id: 1 },
      { id: 11, description: 'Short task', session_id: 'sess-2', title: null, project_id: 1 },
    ];
    vi.mocked(api.listTasks).mockResolvedValue(tasksWithSession as any);

    render(<TaskForm onCreated={vi.fn()} />);
    await selectProject();

    const label = await waitFor(() => screen.getByText('Copy context from:'));
    expect(label).toBeInTheDocument();

    const select = screen.getByDisplayValue('None (start fresh)');
    expect(select).toBeInTheDocument();
  });

  it('copy-context-from select has min-w-0 to prevent overflow on mobile', async () => {
    const tasksWithSession = [
      { id: 10, description: 'Very long task description that could overflow the container on mobile devices', session_id: 'sess-1', title: null, project_id: 1 },
    ];
    vi.mocked(api.listTasks).mockResolvedValue(tasksWithSession as any);

    render(<TaskForm onCreated={vi.fn()} />);
    await selectProject();

    const select = await waitFor(() => screen.getByDisplayValue('None (start fresh)'));
    expect(select.className).toContain('min-w-0');
  });

  it('copy-context-from container has min-w-0 to constrain width', async () => {
    const tasksWithSession = [
      { id: 10, description: 'task', session_id: 'sess-1', title: null, project_id: 1 },
    ];
    vi.mocked(api.listTasks).mockResolvedValue(tasksWithSession as any);

    render(<TaskForm onCreated={vi.fn()} />);
    await selectProject();

    const label = await waitFor(() => screen.getByText('Copy context from:'));
    const container = label.closest('div');
    expect(container?.className).toContain('min-w-0');
  });

  it('copy-context-from label has shrink-0 to prevent label truncation', async () => {
    const tasksWithSession = [
      { id: 10, description: 'task', session_id: 'sess-1', title: null, project_id: 1 },
    ];
    vi.mocked(api.listTasks).mockResolvedValue(tasksWithSession as any);

    render(<TaskForm onCreated={vi.fn()} />);
    await selectProject();

    const label = await waitFor(() => screen.getByText('Copy context from:'));
    expect(label.className).toContain('shrink-0');
  });

  it('does not show copy-context-from when no project selected', async () => {
    render(<TaskForm onCreated={vi.fn()} />);
    await waitFor(() => screen.getByText('Select project...'));

    expect(screen.queryByText('Copy context from:')).not.toBeInTheDocument();
  });

  it('does not show any copy-from control for Codex', async () => {
    vi.mocked(api.listTasks).mockResolvedValue([
      { id: 10, description: 'task', session_id: 'sess-1', title: null, project_id: 1 },
    ] as any);

    render(<TaskForm onCreated={vi.fn()} />);
    await selectProject();
    expect(await screen.findByText('Copy context from:')).toBeInTheDocument();

    await openConfigPanel();
    await userEvent.selectOptions(screen.getByDisplayValue('Claude'), 'codex');

    expect(screen.queryByText('Copy context from:')).not.toBeInTheDocument();
    expect(screen.queryByText('Copy content from:')).not.toBeInTheDocument();
  });

  it('does not submit a previously selected Claude context for Codex', async () => {
    vi.mocked(api.listTasks).mockResolvedValue([
      { id: 10, description: 'task', session_id: 'sess-1', title: null, project_id: 1 },
    ] as any);

    render(<TaskForm onCreated={vi.fn()} />);
    await selectProject();
    await userEvent.selectOptions(
      await screen.findByDisplayValue('None (start fresh)'),
      '10',
    );

    await openConfigPanel();
    await userEvent.selectOptions(screen.getByDisplayValue('Claude'), 'codex');
    await userEvent.type(
      screen.getByPlaceholderText('Prompt / Description (this will be sent to Codex)'),
      'test task',
    );
    await userEvent.click(screen.getByRole('button', { name: /create/i }));

    await waitFor(() => expect(api.createTask).toHaveBeenCalled());
    expect(vi.mocked(api.createTask).mock.calls.at(-1)?.[0]).not.toHaveProperty(
      'clone_from_task_id',
    );
  });

  it('form uses overflow-visible so dropdown panels are not clipped', async () => {
    // 5c3e2c7 起 form 改为 overflow-visible（Config/Tools 下拉需要溢出渲染）；
    // 横向溢出问题由 copy-context select 自身的宽度约束解决
    const { container } = render(<TaskForm onCreated={vi.fn()} />);
    const form = container.querySelector('form');
    expect(form?.className).toContain('overflow-visible');
  });
});

describe('Codex GPT-5.6 per-model effort options', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  async function switchToCodex() {
    render(<TaskForm onCreated={vi.fn()} />);
    await openConfigPanel();
    const cliSelect = await waitFor(() => screen.getByDisplayValue('Claude'));
    await userEvent.selectOptions(cliSelect, 'codex');
  }

  it('lists all three GPT-5.6 models in the model dropdown', async () => {
    await switchToCodex();
    const modelSelect = screen.getByDisplayValue('gpt-5.5 (default)');
    const values = Array.from(modelSelect.querySelectorAll('option')).map((o) => (o as HTMLOptionElement).value);
    expect(values).toContain('gpt-5.6-sol');
    expect(values).toContain('gpt-5.6-terra');
    expect(values).toContain('gpt-5.6-luna');
    expect(values).not.toContain('gpt-5.6');
  });

  it('shows max/ultra efforts for gpt-5.6-sol but not for gpt-5.5', async () => {
    await switchToCodex();
    const modelSelect = screen.getByDisplayValue('gpt-5.5 (default)');
    await userEvent.selectOptions(modelSelect, 'gpt-5.6-sol');

    const effortSelect = screen.getByDisplayValue('medium (default)');
    let efforts = Array.from(effortSelect.querySelectorAll('option')).map((o) => (o as HTMLOptionElement).value);
    expect(efforts).toContain('max');
    expect(efforts).toContain('ultra');

    await userEvent.selectOptions(modelSelect, 'gpt-5.5');
    efforts = Array.from(effortSelect.querySelectorAll('option')).map((o) => (o as HTMLOptionElement).value);
    expect(efforts).not.toContain('max');
    expect(efforts).not.toContain('ultra');
  });

  it('shows max but not ultra for gpt-5.6-luna', async () => {
    await switchToCodex();
    const modelSelect = screen.getByDisplayValue('gpt-5.5 (default)');
    await userEvent.selectOptions(modelSelect, 'gpt-5.6-luna');

    const effortSelect = screen.getByDisplayValue('medium (default)');
    const efforts = Array.from(effortSelect.querySelectorAll('option')).map((o) => (o as HTMLOptionElement).value);
    expect(efforts).toContain('max');
    expect(efforts).not.toContain('ultra');
  });

  it('resets a stale effort when switching to a model that does not support it', async () => {
    await switchToCodex();
    const modelSelect = screen.getByDisplayValue('gpt-5.5 (default)');
    await userEvent.selectOptions(modelSelect, 'gpt-5.6-sol');

    const effortSelect = screen.getByDisplayValue('medium (default)');
    await userEvent.selectOptions(effortSelect, 'ultra');
    expect(effortSelect).toHaveValue('ultra');

    await userEvent.selectOptions(modelSelect, 'gpt-5.5');
    await waitFor(() => expect(effortSelect).toHaveValue(''));
  });
});

describe('Codex provider UI gating', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  async function renderAndOpenConfig() {
    render(<TaskForm onCreated={vi.fn()} />);
    await openConfigPanel();
    return waitFor(() => screen.getByDisplayValue('Claude'));
  }

  it('hides the Thinking budget dropdown for codex (backend ignores it)', async () => {
    const cliSelect = await renderAndOpenConfig();
    expect(screen.getByText('Thinking')).toBeInTheDocument();

    await userEvent.selectOptions(cliSelect, 'codex');
    expect(screen.queryByText('Thinking')).not.toBeInTheDocument();

    // 切回 claude 恢复
    await userEvent.selectOptions(cliSelect, 'claude');
    expect(screen.getByText('Thinking')).toBeInTheDocument();
  });

  it('keeps the local Monitor status quiet while exposing Monitor in Plugins', async () => {
    const cliSelect = await renderAndOpenConfig();
    expect(screen.queryByText(/本地 Codex Monitor 已启用/)).not.toBeInTheDocument();

    await userEvent.selectOptions(cliSelect, 'codex');
    expect(screen.queryByText(/本地 Codex Monitor 已启用/)).not.toBeInTheDocument();
    await waitFor(() => expect(screen.getByText(/Plugins/)).toBeInTheDocument());
    expect(screen.getByText('Skills')).toBeInTheDocument();
    await userEvent.click(screen.getByText(/Plugins/));
    expect(screen.getByText('Sub-Agent')).toBeInTheDocument();
    expect(screen.getByText('Code Review')).toBeInTheDocument();
    expect(screen.getByText('Monitor')).toBeInTheDocument();
  });

  it('keeps Codex Monitor hidden for a Worker project', async () => {
    vi.mocked(api.listProjects).mockResolvedValueOnce([
      {
        id: 1,
        name: 'test-project',
        worker_id: 9,
        git_url: '',
        has_remote: false,
        local_path: '/tmp/test',
        status: 'ready',
        show_in_selector: true,
        tags: [],
        sort_order: 0,
        badge_color: null,
        env_files: [],
      },
    ] as Awaited<ReturnType<typeof api.listProjects>>);
    render(<TaskForm onCreated={vi.fn()} />);
    await selectProject();
    await openConfigPanel();
    await userEvent.selectOptions(
      await waitFor(() => screen.getByDisplayValue('Claude')),
      'codex',
    );

    expect(screen.getByText('Monitor 仅支持本地 Codex')).toBeInTheDocument();
    await userEvent.click(screen.getByText(/Plugins/));
    expect(screen.getByText('Sub-Agent')).toBeInTheDocument();
    expect(screen.queryByText('Monitor')).not.toBeInTheDocument();
  });

  it('keeps only Sub-Agent when Codex main-task MCP is disabled', async () => {
    (api.getRuntimeSettings as ReturnType<typeof vi.fn>)
      .mockResolvedValueOnce({ codex_main_mcp_enabled: false });
    const cliSelect = await renderAndOpenConfig();

    await userEvent.selectOptions(cliSelect, 'codex');
    expect(
      await screen.findByText('主任务 MCP 已关闭 · 仅 Sub-Agent 可用'),
    ).toBeInTheDocument();
    expect(screen.queryByText('Skills')).not.toBeInTheDocument();
    await userEvent.click(screen.getByText(/Plugins/));
    expect(screen.getByText('Sub-Agent')).toBeInTheDocument();
    expect(screen.queryByText('Code Review')).not.toBeInTheDocument();
    expect(screen.queryByText('Monitor')).not.toBeInTheDocument();
  });
});

describe('Codex Fast speed configuration', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
    sessionStorage.clear();
    vi.mocked(api.config).mockResolvedValue({
      default_provider: 'claude',
      provider_options: ['claude', 'codex'],
      default_model: 'claude-opus-4-6',
      model_options: ['claude-opus-4-6'],
      default_codex_model: 'gpt-5.5',
      codex_model_options: ['gpt-5.6-sol', 'gpt-5.5', 'gpt-5.4-mini'],
      default_effort: 'medium',
      effort_options: ['low', 'medium', 'high'],
      claude_model_efforts: {},
      claude_model_context_windows: {},
      codex_effort_options: ['low', 'medium', 'high', 'xhigh'],
      codex_model_efforts: {},
      codex_model_service_tiers: {
        'gpt-5.6-sol': ['default', 'priority'],
        'gpt-5.5': ['default', 'priority'],
        'gpt-5.4-mini': ['default'],
      },
    });
  });

  async function switchToCodexFastForm() {
    render(<TaskForm onCreated={vi.fn()} />);
    await openConfigPanel();
    await userEvent.selectOptions(
      await waitFor(() => screen.getByDisplayValue('Claude')),
      'codex',
    );
    return waitFor(() => screen.getByLabelText('Codex speed'));
  }

  it('only shows Speed for Codex tasks', async () => {
    render(<TaskForm onCreated={vi.fn()} />);
    await openConfigPanel();
    expect(screen.queryByLabelText('Codex speed')).not.toBeInTheDocument();

    await userEvent.selectOptions(screen.getByDisplayValue('Claude'), 'codex');
    expect(await screen.findByLabelText('Codex speed')).toHaveValue('default');

    await userEvent.selectOptions(screen.getByDisplayValue('Codex'), 'claude');
    expect(screen.queryByLabelText('Codex speed')).not.toBeInTheDocument();
  });

  it('persists priority in the task creation payload', async () => {
    const speedSelect = await switchToCodexFastForm();
    await userEvent.selectOptions(speedSelect, 'priority');
    await selectProject();
    await userEvent.type(
      screen.getByPlaceholderText('Prompt / Description (this will be sent to Codex)'),
      'fast task',
    );
    await userEvent.click(screen.getByRole('button', { name: /create/i }));

    await waitFor(() => expect(api.createTask).toHaveBeenCalledWith(
      expect.objectContaining({ codex_service_tier: 'priority' }),
    ));
  });

  it('removes Plan mode and normalizes a legacy saved Plan default', async () => {
    localStorage.setItem('cc_default_task_config', JSON.stringify({
      mode: 'plan',
      provider: 'codex',
      codexServiceTier: 'priority',
    }));

    render(<TaskForm onCreated={vi.fn()} />);
    await openConfigPanel();

    expect(screen.getByDisplayValue('Auto')).toBeInTheDocument();
    expect(screen.queryByRole('option', { name: 'Plan' })).not.toBeInTheDocument();
  });

  it('atomically resets Fast when switching to an unsupported model', async () => {
    const speedSelect = await switchToCodexFastForm();
    await userEvent.selectOptions(speedSelect, 'priority');
    expect(speedSelect).toHaveValue('priority');

    await userEvent.selectOptions(
      screen.getByDisplayValue('gpt-5.5 (default)'),
      'gpt-5.4-mini',
    );

    await waitFor(() => expect(speedSelect).toHaveValue('default'));
    expect(screen.getByRole('option', { name: 'Fast' })).toBeDisabled();
    expect(screen.getByText('gpt-5.4-mini 不支持 Fast')).toBeInTheDocument();
  });

  it('stores and restores Fast in the new-task localStorage defaults', async () => {
    const speedSelect = await switchToCodexFastForm();
    await userEvent.selectOptions(speedSelect, 'priority');
    await userEvent.click(screen.getByRole('button', { name: '设为默认配置' }));

    expect(JSON.parse(localStorage.getItem('cc_default_task_config') || '{}')).toEqual(
      expect.objectContaining({ codexServiceTier: 'priority' }),
    );
  });
});

describe('TaskForm persisted defaults', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
  });

  it('keeps a stored Codex default when the server config resolves later', async () => {
    let resolveConfig!: (value: Awaited<ReturnType<typeof api.config>>) => void;
    (api.config as ReturnType<typeof vi.fn>).mockReturnValue(new Promise((resolve) => {
      resolveConfig = resolve;
    }));
    localStorage.setItem('cc_default_task_config', JSON.stringify({
      provider: 'codex',
      model: 'gpt-5.6-sol',
      effort: 'ultra',
      codexServiceTier: 'priority',
    }));

    render(<TaskForm onCreated={vi.fn()} />);
    await openConfigPanel();
    expect(screen.getByDisplayValue('Codex')).toBeInTheDocument();

    await act(async () => resolveConfig({
      default_provider: 'claude',
      provider_options: ['claude', 'codex'],
      default_model: 'claude-opus-4-6',
      model_options: ['claude-opus-4-6'],
      default_codex_model: 'gpt-5.5',
      codex_model_options: ['gpt-5.6-sol', 'gpt-5.5'],
      default_effort: 'medium',
      effort_options: ['low', 'medium', 'high'],
      codex_effort_options: ['low', 'medium', 'high', 'xhigh', 'ultra'],
      codex_model_efforts: {
        'gpt-5.6-sol': ['low', 'medium', 'high', 'xhigh', 'max', 'ultra'],
      },
      codex_model_service_tiers: {
        'gpt-5.6-sol': ['default', 'priority'],
        'gpt-5.5': ['default', 'priority'],
      },
    }));

    await waitFor(() => {
      expect(screen.getByDisplayValue('Codex')).toBeInTheDocument();
      expect(screen.getByDisplayValue('gpt-5.6-sol')).toBeInTheDocument();
      expect(screen.getByDisplayValue('ultra')).toBeInTheDocument();
      expect(screen.getByLabelText('Codex speed')).toHaveValue('priority');
    });
  });

  it('restores stored defaults when the server config request fails', async () => {
    (api.config as ReturnType<typeof vi.fn>).mockRejectedValue(new Error('offline'));
    localStorage.setItem('cc_default_task_config', JSON.stringify({
      provider: 'codex',
      mode: 'goal',
      timeoutHours: '2',
    }));

    render(<TaskForm onCreated={vi.fn()} />);
    await userEvent.click(screen.getByText('Config'));

    await waitFor(() => {
      expect(screen.getByDisplayValue('Codex')).toBeInTheDocument();
      expect(screen.getByDisplayValue('Goal')).toBeInTheDocument();
      expect(screen.getByDisplayValue('2 hours')).toBeInTheDocument();
    });
  });
});

describe('TaskForm result follow-up prefill', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
  });

  it('loads an explicit PR result draft into the ordinary Task form', async () => {
    const { rerender } = render(<TaskForm onCreated={vi.fn()} />);
    const prompt = screen.getByPlaceholderText(/Prompt \/ Description/);
    expect(prompt).toHaveValue('');

    rerender(
      <TaskForm
        onCreated={vi.fn()}
        prefill={{ key: 'run-41', description: 'Follow up acme/widget#133\nPR: https://github.com/acme/widget/pull/133' }}
      />,
    );

    await waitFor(() => {
      expect(prompt).toHaveValue('Follow up acme/widget#133\nPR: https://github.com/acme/widget/pull/133');
    });
    expect(prompt).toHaveFocus();
  });

  it('does not overwrite an unsaved draft unless the user confirms replacement', async () => {
    const confirm = vi.spyOn(window, 'confirm').mockReturnValue(false);
    const user = userEvent.setup();
    const { rerender } = render(<TaskForm onCreated={vi.fn()} />);
    const prompt = screen.getByPlaceholderText(/Prompt \/ Description/);
    await user.type(prompt, 'Keep my unsaved draft');

    rerender(
      <TaskForm
        onCreated={vi.fn()}
        prefill={{ key: 'run-42', description: 'Replace with PR follow-up' }}
      />,
    );

    await waitFor(() => expect(confirm).toHaveBeenCalledWith(
      'Replace your unsaved Task draft with this PR follow-up?',
    ));
    expect(prompt).toHaveValue('Keep my unsaved draft');
    confirm.mockRestore();
  });
});

describe('TaskForm frontend review entry location', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
  });

  it('does not show the looping frontend review control on Task creation', () => {
    render(<TaskForm onCreated={vi.fn()} />);

    expect(screen.queryByRole('button', { name: '选择前端审查模式' }))
      .not.toBeInTheDocument();
  });
});

describe('TaskForm Auto capabilities', () => {
  const enabledConfig = {
    default_provider: 'claude',
    provider_options: ['claude', 'codex'],
    default_model: 'claude-opus-4-6',
    model_options: ['claude-opus-4-6'],
    default_codex_model: 'gpt-5.6-sol',
    codex_model_options: ['gpt-5.6-sol'],
    default_effort: 'medium',
    effort_options: ['low', 'medium', 'high'],
    codex_effort_options: ['low', 'medium', 'high', 'xhigh'],
    codex_model_efforts: {},
    codex_model_service_tiers: {},
    capability_core_enabled: true,
    auto_capability_enabled: true,
    delivery_loop_enabled: false,
  };

  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
    vi.mocked(api.config).mockResolvedValue(enabledConfig as never);
    vi.mocked(api.listProjects).mockResolvedValue([
      { id: 1, name: 'test-project', git_url: '', has_remote: false, local_path: '/tmp/test', status: 'ready', show_in_selector: true, tags: [], sort_order: 0, badge_color: null, env_files: [], worker_id: null },
    ] as never);
  });

  it.each([
    ['Capability Core', { capability_core_enabled: false }],
    ['Auto Capability', { auto_capability_enabled: false }],
  ])('keeps controls hidden when the %s rollout gate is disabled', async (_name, override) => {
    vi.mocked(api.config).mockResolvedValue({
      ...enabledConfig,
      ...override,
    } as never);
    render(<TaskForm onCreated={vi.fn()} />);
    await openConfigPanel();

    expect(screen.queryByLabelText('Allow automatic Plan')).not.toBeInTheDocument();
    expect(screen.queryByLabelText('Allow automatic Code Review')).not.toBeInTheDocument();
  });

  it('fails closed when the server capability config cannot be loaded', async () => {
    vi.mocked(api.config).mockRejectedValueOnce(new Error('config unavailable'));
    render(<TaskForm onCreated={vi.fn()} />);
    await selectProject();
    await openConfigPanel();

    expect(screen.queryByLabelText('Allow automatic Plan')).not.toBeInTheDocument();
    await userEvent.type(
      screen.getByPlaceholderText(/Prompt \/ Description/),
      'Create without unconfirmed capability support',
    );
    await userEvent.click(screen.getByRole('button', { name: /create/i }));

    await waitFor(() => expect(api.createTask).toHaveBeenCalled());
    expect(vi.mocked(api.createTask).mock.calls[0][0]).not.toHaveProperty(
      'capability_policy',
    );
  });

  it('submits an explicit allowlist with total and per-capability budgets', async () => {
    render(<TaskForm onCreated={vi.fn()} />);
    await selectProject();
    await openConfigPanel();

    await userEvent.click(screen.getByLabelText('Allow automatic Plan'));
    await userEvent.selectOptions(
      screen.getByLabelText('Automatic Plan budget'),
      '2',
    );
    await userEvent.click(screen.getByLabelText('Allow automatic Code Review'));
    await waitFor(() => {
      expect(screen.getByLabelText('Automatic capability total limit')).toHaveValue('2');
    });
    await userEvent.selectOptions(
      screen.getByLabelText('Automatic capability total limit'),
      '3',
    );
    await userEvent.type(
      screen.getByPlaceholderText(/Prompt \/ Description/),
      'Implement with advisory help',
    );
    await userEvent.click(screen.getByRole('button', { name: /create/i }));

    await waitFor(() => expect(api.createTask).toHaveBeenCalledWith(
      expect.objectContaining({
        capability_policy: {
          version: 1,
          max_invocations: 3,
          capabilities: { plan: 2, code_review: 1 },
        },
      }),
    ));
  });

  it('keeps the maximum edge policy within every backend budget bound', async () => {
    render(<TaskForm onCreated={vi.fn()} />);
    await selectProject();
    await openConfigPanel();

    await userEvent.click(screen.getByLabelText('Allow automatic Plan'));
    await userEvent.selectOptions(
      screen.getByLabelText('Automatic Plan budget'),
      '8',
    );
    await userEvent.click(screen.getByLabelText('Allow automatic Code Review'));
    await userEvent.selectOptions(
      screen.getByLabelText('Automatic Code Review budget'),
      '8',
    );
    expect(screen.getByLabelText('Automatic capability total limit')).toHaveValue('8');

    await userEvent.type(
      screen.getByPlaceholderText(/Prompt \/ Description/),
      'Use bounded advisory help',
    );
    await userEvent.click(screen.getByRole('button', { name: /create/i }));

    await waitFor(() => expect(api.createTask).toHaveBeenCalledWith(
      expect.objectContaining({
        capability_policy: {
          version: 1,
          max_invocations: 8,
          capabilities: { plan: 8, code_review: 8 },
        },
      }),
    ));
  });

  it('clears a local opt-in and omits policy after switching to a Worker project', async () => {
    vi.mocked(api.listProjects).mockResolvedValue([
      { id: 1, name: 'test-project', git_url: '', has_remote: false, local_path: '/tmp/test', status: 'ready', show_in_selector: true, tags: [], sort_order: 0, badge_color: null, env_files: [], worker_id: null },
      { id: 2, name: 'worker-project', git_url: '', has_remote: false, local_path: '/tmp/worker', status: 'ready', show_in_selector: true, tags: [], sort_order: 0, badge_color: null, env_files: [], worker_id: 9 },
    ] as never);
    render(<TaskForm onCreated={vi.fn()} />);
    await selectProject();
    await openConfigPanel();

    await userEvent.click(screen.getByLabelText('Allow automatic Plan'));
    await userEvent.click(screen.getByText('test-project'));
    await userEvent.click(await screen.findByText('worker-project'));
    await openConfigPanel();

    expect(screen.queryByLabelText('Allow automatic Plan')).not.toBeInTheDocument();
    expect(screen.queryByLabelText('Allow automatic Code Review')).not.toBeInTheDocument();
    await userEvent.type(
      screen.getByPlaceholderText(/Prompt \/ Description/),
      'Run only on the Worker',
    );
    await userEvent.click(screen.getByRole('button', { name: /create/i }));

    await waitFor(() => expect(api.createTask).toHaveBeenCalled());
    expect(vi.mocked(api.createTask).mock.calls[0][0]).not.toHaveProperty(
      'capability_policy',
    );
  });
});

describe('legacy TaskForm Delivery Loop entry (moved to Delivery tab)', () => {
  const config = {
    default_provider: 'codex',
    provider_options: ['claude', 'codex'],
    default_model: 'claude-opus-4-6',
    model_options: ['claude-opus-4-6'],
    default_codex_model: 'gpt-5.6-sol',
    codex_model_options: ['gpt-5.6-sol'],
    default_effort: 'medium',
    effort_options: ['low', 'medium', 'high'],
    claude_model_efforts: {},
    claude_model_context_windows: {},
    codex_effort_options: ['low', 'medium', 'high', 'xhigh'],
    codex_model_efforts: {},
    codex_model_service_tiers: { 'gpt-5.6-sol': ['default', 'priority'] },
    capability_core_enabled: true,
    delivery_loop_enabled: true,
  };

  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
    vi.mocked(api.config).mockResolvedValue(config as never);
    vi.mocked(api.listProjects).mockResolvedValue([{
      id: 1,
      name: 'test-project',
      git_url: 'git@github.com:owner/repo.git',
      has_remote: true,
      local_path: '/srv/repo',
      default_branch: 'main',
      worker_id: null,
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
    }]);
    vi.mocked(api.getMonitoredRepos).mockResolvedValue([{
      id: 9,
      repo_full_name: 'owner/repo',
      project_id: 1,
      worker_id: null,
      enabled: true,
      auto_merge: true,
      auto_repair: true,
      max_repair_attempts: 3,
      provider: 'claude',
      review_model: null,
      review_effort: null,
      review_mode: 'panel',
      wait_for_ci: true,
      required_checks: [{ kind: 'check_run', name: 'test', app_slug: 'github-actions' }],
      merge_queue_mode: 'manual',
      default_branch: 'main',
      allowed_authors: [],
      status: 'active',
      error_message: null,
      created_at: '2026-08-05T00:00:00Z',
      updated_at: '2026-08-05T00:00:00Z',
    }]);
  });

  it('keeps global Delivery permission state out of the Task composer', async () => {
    vi.mocked(api.config).mockResolvedValue({
      ...config,
      agent_sandbox_unrestricted_enabled: true,
    } as never);

    render(<TaskForm onCreated={vi.fn()} />);

    await screen.findByPlaceholderText(/Prompt \/ Description/);
    expect(screen.queryByText('All Delivery permissions are ON', { exact: false })).not.toBeInTheDocument();
  });

  async function fillDeliveryForm(requirements: string) {
    await selectProject();
    await openConfigPanel();
    await userEvent.selectOptions(screen.getByDisplayValue('Auto'), 'delivery_loop');
    const repoSelect = await screen.findByLabelText('Delivery PR Monitor repository');
    await waitFor(() => expect(repoSelect).not.toBeDisabled());
    await userEvent.selectOptions(repoSelect, '9');
    fireEvent.change(
      screen.getByPlaceholderText('Delivery requirements (Plan → Code → Review → PR Monitor)'),
      { target: { value: requirements } },
    );
  }

  it('keeps the mode hidden when the server feature gate is off', async () => {
    vi.mocked(api.config).mockResolvedValue({
      ...config,
      delivery_loop_enabled: false,
    } as never);
    render(<TaskForm onCreated={vi.fn()} />);
    await openConfigPanel();

    expect(screen.queryByRole('option', { name: 'Delivery Loop' })).not.toBeInTheDocument();
  });

  it.skip('creates an atomic DeliveryRun instead of an ordinary Task', async () => {
    const onCreated = vi.fn();
    render(<TaskForm onCreated={onCreated} />);
    await selectProject();
    await openConfigPanel();
    await userEvent.selectOptions(screen.getByDisplayValue('Auto'), 'delivery_loop');
    const providerSelect = screen.getByLabelText('Task provider');
    expect(providerSelect).toHaveValue('codex');
    expect(providerSelect).not.toBeDisabled();
    expect(within(providerSelect).getByRole('option', { name: 'Claude' })).toBeInTheDocument();
    expect(within(providerSelect).getByRole('option', { name: 'Codex' })).toBeInTheDocument();

    expect(screen.queryByRole('button', { name: 'Attach files' })).not.toBeInTheDocument();
    expect(screen.queryByText('Priority')).not.toBeInTheDocument();
    expect(screen.queryByText('System Prompt')).not.toBeInTheDocument();
    expect(screen.queryByText(/Plugins/)).not.toBeInTheDocument();
    expect(screen.queryByText(/^Skills/)).not.toBeInTheDocument();

    const repoSelect = await screen.findByLabelText('Delivery PR Monitor repository');
    await waitFor(() => expect(repoSelect).not.toBeDisabled());
    await userEvent.selectOptions(repoSelect, '9');
    expect(screen.getByText(/Merge behavior is inherited from the selected PR Monitor/)).toBeInTheDocument();
    expect(screen.getByText(/PR Monitor Auto Merge is ON/)).toHaveTextContent(
      'CCM will finish only after GitHub confirms the merge',
    );
    await userEvent.type(
      screen.getByPlaceholderText('Delivery requirements (Plan → Code → Review → PR Monitor)'),
      'Fix exact-head loop\nwith complete regression tests',
    );
    await userEvent.click(screen.getByRole('button', { name: /create/i }));

    await waitFor(() => expect(api.createDeliveryRun).toHaveBeenCalledWith({
      idempotency_key: expect.any(String),
      project_id: 1,
      monitored_repo_id: 9,
      title: 'Fix exact-head loop',
      requirements: 'Fix exact-head loop\nwith complete regression tests',
      base_branch: 'main',
      provider: 'codex',
      model: 'gpt-5.6-sol',
      codex_service_tier: 'default',
    }));
    expect(api.createTask).not.toHaveBeenCalled();
    expect(onCreated).toHaveBeenCalledOnce();
  });

  it.skip('creates a Claude Delivery Run when Claude is the only configured provider', async () => {
    vi.mocked(api.config).mockResolvedValue({
      ...config,
      default_provider: 'claude',
      provider_options: ['claude'],
    } as never);
    render(<TaskForm onCreated={vi.fn()} />);
    await selectProject();
    await openConfigPanel();
    await userEvent.selectOptions(screen.getByDisplayValue('Auto'), 'delivery_loop');

    const providerSelect = screen.getByLabelText('Task provider');
    expect(providerSelect).toHaveValue('claude');
    expect(within(providerSelect).getAllByRole('option')).toHaveLength(1);
    expect(within(providerSelect).queryByRole('option', { name: 'Codex' })).not.toBeInTheDocument();
    expect(screen.queryByLabelText('Codex speed')).not.toBeInTheDocument();

    const repoSelect = await screen.findByLabelText('Delivery PR Monitor repository');
    await waitFor(() => expect(repoSelect).not.toBeDisabled());
    await userEvent.selectOptions(repoSelect, '9');
    await userEvent.type(
      screen.getByPlaceholderText('Delivery requirements (Plan → Code → Review → PR Monitor)'),
      'Ship with Claude only',
    );
    await userEvent.click(screen.getByRole('button', { name: /create/i }));

    await waitFor(() => expect(api.createDeliveryRun).toHaveBeenCalledOnce());
    const payload = vi.mocked(api.createDeliveryRun).mock.calls[0][0];
    expect(payload).toEqual(expect.objectContaining({
      project_id: 1,
      monitored_repo_id: 9,
      title: 'Ship with Claude only',
      requirements: 'Ship with Claude only',
      base_branch: 'main',
      provider: 'claude',
      model: 'claude-opus-4-6',
    }));
    expect(payload).not.toHaveProperty('codex_service_tier');
  });

  it.skip('keeps a failed admission key across remount and acknowledges it only after success', async () => {
    vi.mocked(api.createDeliveryRun)
      .mockRejectedValueOnce(new Error('response lost'))
      .mockResolvedValue({ id: 11, developer_task_id: 12 } as never);
    const firstRender = render(<TaskForm onCreated={vi.fn()} />);
    await fillDeliveryForm('Retry the exact same admission');

    await userEvent.click(screen.getByRole('button', { name: /create/i }));
    await screen.findByText('response lost');
    const firstKey = vi.mocked(api.createDeliveryRun).mock.calls[0][0].idempotency_key;
    firstRender.unmount();

    const replayRender = render(<TaskForm onCreated={vi.fn()} />);
    await fillDeliveryForm('Retry the exact same admission');
    await userEvent.click(screen.getByRole('button', { name: /create/i }));
    await waitFor(() => expect(api.createDeliveryRun).toHaveBeenCalledTimes(2));
    const secondKey = vi.mocked(api.createDeliveryRun).mock.calls[1][0].idempotency_key;
    expect(firstKey).toBeTruthy();
    expect(secondKey).toBe(firstKey);
    replayRender.unmount();

    render(<TaskForm onCreated={vi.fn()} />);
    await fillDeliveryForm('Retry the exact same admission');
    await userEvent.click(screen.getByRole('button', { name: /create/i }));
    await waitFor(() => expect(api.createDeliveryRun).toHaveBeenCalledTimes(3));
    expect(vi.mocked(api.createDeliveryRun).mock.calls[2][0].idempotency_key).not.toBe(firstKey);
  });

  it.skip('trims Delivery requirements and derives a capped title from the first non-empty line', async () => {
    const longTitle = 'T'.repeat(205);
    render(<TaskForm onCreated={vi.fn()} />);
    await fillDeliveryForm(`\n  \n${longTitle}\nRegression details\n`);

    await userEvent.click(screen.getByRole('button', { name: /create/i }));

    await waitFor(() => expect(api.createDeliveryRun).toHaveBeenCalledWith(
      expect.objectContaining({
        title: 'T'.repeat(200),
        requirements: `${longTitle}\nRegression details`,
      }),
    ));
  });

  it.skip('rejects whitespace-only Delivery requirements', async () => {
    render(<TaskForm onCreated={vi.fn()} />);
    await fillDeliveryForm('  \n\t  ');

    expect(screen.getByRole('button', { name: /create/i })).toBeDisabled();
    expect(api.createDeliveryRun).not.toHaveBeenCalled();
  });

  it.skip('ignores dropped and pasted files while Delivery Loop mode is active', async () => {
    render(<TaskForm onCreated={vi.fn()} />);
    await openConfigPanel();
    await userEvent.selectOptions(screen.getByDisplayValue('Auto'), 'delivery_loop');

    const prompt = screen.getByPlaceholderText(
      'Delivery requirements (Plan → Code → Review → PR Monitor)',
    );
    const form = prompt.closest('form');
    expect(form).not.toBeNull();

    const dropped = new File(['drop'], 'dropped.txt', { type: 'text/plain' });
    fireEvent.drop(form!, {
      dataTransfer: { files: [dropped] },
    });

    const pasted = new File(['paste'], 'pasted.txt', { type: 'text/plain' });
    fireEvent.paste(form!, {
      clipboardData: {
        items: [{ kind: 'file', getAsFile: () => pasted }],
      },
    });

    expect(api.uploadImages).not.toHaveBeenCalled();
  });

  it.skip('drops a previously selected SSH grant before Delivery admission', async () => {
    vi.mocked(api.listSSHProfiles).mockResolvedValueOnce([{
      id: 41,
      name: 'production-box',
      host: 'ssh.internal',
      port: 22,
      username: 'deploy',
      enabled: true,
      revision: 1,
      task_access_enabled: true,
      task_capabilities: ['read'],
      allowed_roots: ['/srv/app'],
      has_key: true,
      last_tested_at: null,
      last_test_ok: null,
      last_error_code: null,
      last_error_detail: null,
      created_at: '2026-08-06T00:00:00',
      updated_at: '2026-08-06T00:00:00',
    }]);
    render(<TaskForm onCreated={vi.fn()} />);
    await selectProject();
    await userEvent.click(screen.getByRole('button', { name: /^SSH access/ }));
    await userEvent.click(await screen.findByLabelText('Grant production-box'));

    await openConfigPanel();
    await userEvent.selectOptions(screen.getByDisplayValue('Auto'), 'delivery_loop');
    const repoSelect = await screen.findByLabelText('Delivery PR Monitor repository');
    await waitFor(() => expect(repoSelect).not.toBeDisabled());
    await userEvent.selectOptions(repoSelect, '9');
    fireEvent.change(
      screen.getByPlaceholderText('Delivery requirements (Plan → Code → Review → PR Monitor)'),
      { target: { value: 'Keep Delivery isolated from SSH grants' } },
    );
    await userEvent.click(screen.getByRole('button', { name: /create/i }));

    await waitFor(() => expect(api.createDeliveryRun).toHaveBeenCalledOnce());
    expect(screen.queryByText(/does not accept.*SSH grants/i)).not.toBeInTheDocument();
  });
});

describe('TaskForm SSH grants', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
    vi.mocked(api.listSSHProfiles).mockResolvedValue([{
      id: 41,
      name: 'production-box',
      host: 'ssh.internal',
      port: 22,
      username: 'deploy',
      key_path_hint: '…/id_ed25519',
      public_key_fingerprint: 'SHA256:client-key',
      host_key_type: 'ssh-ed25519',
      host_key_fingerprint: 'SHA256:server-key',
      revision: 1,
      enabled: true,
      task_access_enabled: true,
      task_capabilities: ['read', 'exec', 'write'],
      allowed_roots: ['/'],
      created_by: 1,
      last_tested_at: null,
      last_test_ok: null,
      last_error_code: null,
      last_error_detail: null,
      created_at: '2026-08-06T00:00:00',
      updated_at: '2026-08-06T00:00:00',
    }]);
  });

  it('submits the selected profile and least-privilege capabilities atomically', async () => {
    const user = userEvent.setup();
    render(<TaskForm onCreated={vi.fn()} />);
    await selectProject();
    await user.type(
      screen.getByPlaceholderText(/Prompt \/ Description/),
      'inspect production logs',
    );
    await user.click(screen.getByRole('button', { name: /^SSH access/ }));
    await user.click(await screen.findByLabelText('Grant production-box'));
    await user.click(screen.getByRole('button', { name: /create/i }));

    await waitFor(() => expect(api.createTask).toHaveBeenCalledWith(
      expect.objectContaining({
        ssh_grants: [{ profile_id: 41, capabilities: ['read'] }],
      }),
    ));
  });

  it('clears Manager-local SSH grants when switching to a Worker project', async () => {
    vi.mocked(api.listProjects).mockResolvedValueOnce([
      { id: 1, name: 'test-project', git_url: '', has_remote: false, local_path: '/tmp/test', status: 'ready', show_in_selector: true, tags: [], sort_order: 0, badge_color: null, env_files: [], worker_id: null },
      { id: 2, name: 'worker-project', worker_id: 9, git_url: '', has_remote: false, local_path: '/workspace/test', status: 'ready', show_in_selector: true, tags: [], sort_order: 0, badge_color: null, env_files: [] },
    ] as Awaited<ReturnType<typeof api.listProjects>>);
    const user = userEvent.setup();
    render(<TaskForm onCreated={vi.fn()} />);
    await selectProject();
    await user.click(screen.getByRole('button', { name: /^SSH access/ }));
    await user.click(await screen.findByLabelText('Grant production-box'));

    await user.click(screen.getByText('test-project'));
    await user.click(await screen.findByText('worker-project'));
    expect(screen.getByRole('button', { name: /^SSH access/ })).toBeDisabled();
    await user.type(screen.getByPlaceholderText(/Prompt \/ Description/), 'remote task');
    await user.click(screen.getByRole('button', { name: /create/i }));

    await waitFor(() => expect(api.createTask).toHaveBeenCalled());
    expect(vi.mocked(api.createTask).mock.calls.at(-1)?.[0]).not.toHaveProperty('ssh_grants');
  });
});
