import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor, act } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { TaskForm } from './TaskForm';

vi.mock('../../api/client', () => ({
  api: {
    listProjects: vi.fn().mockResolvedValue([
      { id: 1, name: 'test-project', git_url: '', has_remote: false, local_path: '/tmp/test', status: 'ready', show_in_selector: true, tags: [], sort_order: 0, badge_color: null, env_files: [] },
    ]),
    listTags: vi.fn().mockResolvedValue([]),
    listSecrets: vi.fn().mockResolvedValue([]),
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

  it('shows Monitor with ordinary/User Skills for local Codex', async () => {
    const cliSelect = await renderAndOpenConfig();
    expect(screen.queryByText(/本地 Codex Monitor 已启用/)).not.toBeInTheDocument();

    await userEvent.selectOptions(cliSelect, 'codex');
    expect(
      screen.getByText('本地 Codex Monitor 已启用'),
    ).toBeInTheDocument();
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
