import { cleanup, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { api } from '../api/client';
import { FALLBACK_PLAN_PIPELINE_CONFIG } from '../components/PlanReview/planPipelineDefaults';
import { SettingsPage } from './SettingsPage';

vi.mock('../api/client', () => ({
  api: {
    config: vi.fn(),
    getPlanPipelineSettings: vi.fn(),
    updatePlanPipelineSettings: vi.fn(),
    getCapacitySettings: vi.fn(),
    updateCapacitySettings: vi.fn(),
    getRuntimeSettings: vi.fn(),
    updateRuntimeSettings: vi.fn(),
    getFeishuStatus: vi.fn(),
    getFeishuAuthUrl: vi.fn(),
    unbindFeishu: vi.fn(),
    changePassword: vi.fn(),
  },
  clearToken: vi.fn(),
}));

const runtimeSettings = {
  use_pty_mode: false,
  pty_available: true,
  codex_app_server_enabled: true,
  codex_main_mcp_enabled: true,
  codex_monitor_enabled: true,
  auto_sort_on_access: true,
  context_compact_threshold: 0.8,
};

describe('SettingsPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.setItem('cc_user', JSON.stringify({
      id: 1,
      name: 'Test Admin',
      role: 'admin',
    }));
    vi.mocked(api.config).mockResolvedValue({
      default_provider: 'codex',
      provider_options: ['claude', 'codex'],
      default_model: 'claude-opus-4-6',
      model_options: ['claude-fable-5', 'claude-sonnet-5'],
      default_codex_model: 'gpt-5.6-sol',
      codex_model_options: ['gpt-5.6-sol', 'gpt-5.6-terra'],
      default_effort: 'medium',
      effort_options: ['low', 'medium', 'high'],
      claude_model_efforts: {},
      claude_model_context_windows: {},
      codex_effort_options: ['low', 'medium', 'high', 'xhigh'],
      codex_model_efforts: {},
      codex_model_service_tiers: {},
    });
    vi.mocked(api.getPlanPipelineSettings).mockResolvedValue(
      FALLBACK_PLAN_PIPELINE_CONFIG,
    );
    vi.mocked(api.updatePlanPipelineSettings).mockImplementation(
      async (value) => value,
    );
    vi.mocked(api.getCapacitySettings).mockResolvedValue({
      max_concurrent_instances: 8,
      configured_override: null,
      env_default: 8,
      min_idle_instances: 2,
      active_instances: 3,
      live_instances: 8,
      pending_tasks: 4,
    });
    vi.mocked(api.updateCapacitySettings).mockImplementation(async (value) => ({
      max_concurrent_instances: value ?? 8,
      configured_override: value,
      env_default: 8,
      min_idle_instances: 2,
      active_instances: 3,
      live_instances: 8,
      pending_tasks: 4,
    }));
    vi.mocked(api.getRuntimeSettings).mockResolvedValue(runtimeSettings);
    vi.mocked(api.updateRuntimeSettings).mockImplementation(async (value) => ({
      ...runtimeSettings,
      ...value,
    }));
    vi.mocked(api.getFeishuStatus).mockResolvedValue({ bound: false });
    vi.mocked(api.getFeishuAuthUrl).mockResolvedValue({ url: 'https://example.com/feishu' });
    vi.mocked(api.unbindFeishu).mockResolvedValue({ ok: true });
    vi.mocked(api.changePassword).mockResolvedValue({ ok: true });
  });

  afterEach(() => {
    cleanup();
    localStorage.clear();
    vi.restoreAllMocks();
  });

  it('contains every preference and account action moved out of the header menu', async () => {
    render(<SettingsPage />);

    expect(screen.getByRole('heading', { name: 'Appearance & locale' })).toBeInTheDocument();
    expect(screen.getByLabelText('时区')).toBeInTheDocument();
    expect(screen.getByLabelText('主题')).toBeInTheDocument();
    expect(await screen.findByRole('button', { name: 'PTY 模式' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: '访问置顶' })).toBeInTheDocument();
    expect(screen.getByLabelText('压缩阈值')).toBeInTheDocument();
    expect(screen.getByTestId('codex-main-mcp-status')).toHaveTextContent('已启用');
    expect(await screen.findByRole('button', { name: '绑定' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: '修改密码' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: '退出登录' })).toBeInTheDocument();
  });

  it('keeps personal settings available to members without loading admin configuration', async () => {
    localStorage.setItem('cc_user', JSON.stringify({
      id: 9,
      name: 'Member',
      role: 'member',
    }));

    render(<SettingsPage />);

    expect(screen.getByLabelText('时区')).toBeInTheDocument();
    expect(screen.getByLabelText('主题')).toBeInTheDocument();
    expect(await screen.findByRole('button', { name: '绑定' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: '修改密码' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: '退出登录' })).toBeInTheDocument();
    expect(screen.queryByText('Runtime & task behavior')).not.toBeInTheDocument();
    expect(screen.queryByText('Local task capacity')).not.toBeInTheDocument();
    expect(screen.queryByText('Plan Pipeline')).not.toBeInTheDocument();
    expect(api.getRuntimeSettings).not.toHaveBeenCalled();
    expect(api.getCapacitySettings).not.toHaveBeenCalled();
    expect(api.getPlanPipelineSettings).not.toHaveBeenCalled();
  });

  it('persists appearance preferences directly from the settings page', async () => {
    render(<SettingsPage />);

    await userEvent.selectOptions(screen.getByLabelText('主题'), 'light');
    await userEvent.selectOptions(screen.getByLabelText('时区'), 'UTC');

    expect(localStorage.getItem('cc_theme')).toBe('light');
    expect(localStorage.getItem('cc_timezone')).toBe('UTC');
  });

  it('persists the global maximum Plan rounds', async () => {
    render(<SettingsPage />);

    const rounds = await screen.findByLabelText('Plan maximum rounds');
    await userEvent.clear(rounds);
    await userEvent.type(rounds, '1');
    await userEvent.click(screen.getByRole('button', { name: 'Save settings' }));

    await waitFor(() => expect(api.updatePlanPipelineSettings).toHaveBeenCalledWith(
      expect.objectContaining({ max_revision_cycles: 1 }),
    ));
    expect(await screen.findByRole('button', { name: 'Saved' })).toBeInTheDocument();
  });

  it('applies a local concurrency override without restarting', async () => {
    render(<SettingsPage />);

    const capacity = await screen.findByLabelText('Maximum concurrent tasks');
    await userEvent.clear(capacity);
    await userEvent.type(capacity, '12');
    await userEvent.click(screen.getByRole('button', { name: 'Save capacity' }));

    await waitFor(() => expect(api.updateCapacitySettings).toHaveBeenCalledWith(12));
    expect(await screen.findByRole('button', { name: 'Saved' })).toBeInTheDocument();
  });

  it('restores the environment default after an override', async () => {
    vi.mocked(api.getCapacitySettings).mockResolvedValueOnce({
      max_concurrent_instances: 12,
      configured_override: 12,
      env_default: 8,
      min_idle_instances: 2,
      active_instances: 3,
      live_instances: 12,
      pending_tasks: 0,
    });
    render(<SettingsPage />);

    await userEvent.click(await screen.findByRole('button', {
      name: 'Restore environment default',
    }));

    await waitFor(() => expect(api.updateCapacitySettings).toHaveBeenCalledWith(null));
  });
});
