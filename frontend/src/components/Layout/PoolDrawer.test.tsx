import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { act, render, screen, within, waitFor, cleanup } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { PoolDrawer } from './PoolDrawer';

vi.mock('../../api/client', () => ({
  isApiRequestError: (error: unknown) => error instanceof Error
    && typeof (error as { status?: unknown }).status === 'number',
  api: {
    getPoolStatus: vi.fn(),
    getPoolUsage: vi.fn(),
    clearPoolCooldown: vi.fn(),
    setPoolPreferred: vi.fn(),
    poolRelogin: vi.fn(),
    poolReloginStatus: vi.fn(),
    poolAddAccount: vi.fn(),
    poolAddStatus: vi.fn(),
    poolDeleteAccount: vi.fn(),
    getCloudRouterAccounts: vi.fn(),
    createCloudRouterAccount: vi.fn(),
    refreshCloudRouterAccount: vi.fn(),
    deleteCloudRouterAccount: vi.fn(),
    getCodexPoolStatus: vi.fn(),
    getCodexPoolUsage: vi.fn(),
    getCodexPoolSettings: vi.fn(),
    putCodexPoolSettings: vi.fn(),
    clearCodexPoolCooldown: vi.fn(),
    setCodexPoolPreferred: vi.fn(),
    codexPoolDeleteAccount: vi.fn(),
    codexPoolRelogin: vi.fn(),
    codexPoolReloginStatus: vi.fn(),
    codexPoolAddAccount: vi.fn(),
    codexPoolAddStatus: vi.fn(),
    codexPoolSubmitOtp: vi.fn(),
    getCcSettings: vi.fn().mockResolvedValue({ settings: {} }),
    putCcSettings: vi.fn(),
  },
}));

import { api } from '../../api/client';

const mockPoolUsage = {
  total: 2,
  available: 2,
  preferred: null,
  last_selected: 'acc-1',
  accounts: [
    {
      id: 'acc-1',
      email: 'user1@example.com',
      available: true,
      enabled: true,
      subscription_type: 'pro',
      usage: {
        five_hour: { utilization: 30, resets_at: '2026-07-15T12:00:00Z' },
        seven_day: { utilization: 50, resets_at: '2026-07-20T00:00:00Z' },
        seven_day_opus: null,
      },
      usage_error: null,
    },
  ],
};

function enablePool() {
  vi.mocked(api.getPoolStatus).mockResolvedValue({ enabled: true } as never);
  vi.mocked(api.getPoolUsage).mockResolvedValue(mockPoolUsage as never);
  vi.mocked(api.getCodexPoolStatus).mockRejectedValue(new Error('Codex pool disabled'));
}

function enableCodexPool(usage: Record<string, unknown> = { total: 0, available: 0, preferred: null, accounts: [] }) {
  vi.mocked(api.getCodexPoolStatus).mockResolvedValue({ enabled: true } as never);
  vi.mocked(api.getCodexPoolUsage).mockResolvedValue(usage as never);
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

async function renderAndWaitForPro() {
  render(<PoolDrawer />);
  await waitFor(() => {
    expect(screen.getByText('Pro')).toBeInTheDocument();
  });
}

async function openDrawer(user: ReturnType<typeof userEvent.setup>) {
  await user.click(screen.getByText('Pro'));
  await waitFor(() => {
    expect(screen.getByText('Claude Pool')).toBeInTheDocument();
  });
}

describe('PoolDrawer', () => {
  beforeEach(() => {
    vi.mocked(api.getCloudRouterAccounts).mockResolvedValue([]);
    enablePool();
  });

  afterEach(() => {
    cleanup();
    vi.clearAllMocks();
    vi.restoreAllMocks();
  });

  it('renders the Pro trigger button when pool is enabled', async () => {
    await renderAndWaitForPro();
    expect(screen.getByText('Pro')).toBeInTheDocument();
  });

  it('distinguishes the Claude preferred account from its most recent route', async () => {
    vi.mocked(api.getPoolUsage).mockResolvedValue({
      ...mockPoolUsage,
      preferred: 'acc-1',
      last_selected: 'acc-1',
    } as never);
    vi.mocked(api.setPoolPreferred).mockResolvedValue({
      ok: true,
      preferred: null,
    } as never);
    const user = userEvent.setup();

    await renderAndWaitForPro();
    await openDrawer(user);

    const accountCard = screen
      .getByText('user1@example.com')
      .closest('.rounded-lg') as HTMLElement;
    expect(within(accountCard).getByText('优先账号')).toBeInTheDocument();
    expect(within(accountCard).getByText('最近使用')).toHaveAttribute(
      'title',
      '最近一次由路由器分配的账号',
    );
    expect(
      within(accountCard).getByRole('button', { name: '恢复自动' }),
    ).toHaveAttribute(
      'title',
      '取消全局优先；新会话优先兼容且可用的 API，已有对话继续使用绑定账号',
    );
  });

  it('does not render anything when pool is disabled', async () => {
    vi.mocked(api.getPoolStatus).mockResolvedValue({ enabled: false } as never);
    vi.mocked(api.getCloudRouterAccounts).mockRejectedValue(new Error('API accounts unavailable'));
    const { container } = render(<PoolDrawer />);
    await waitFor(() => {
      expect(container.innerHTML).toBe('');
    });
  });

  describe('z-index layering (portal)', () => {
    it('renders the drawer overlay via portal on document.body', async () => {
      const user = userEvent.setup();
      await renderAndWaitForPro();
      await openDrawer(user);

      const overlay = screen.getByText('Claude Pool').closest('[class*="fixed"]');
      expect(overlay).toBeTruthy();
      expect(overlay!.parentElement).toBe(document.body);
    });

    it('drawer overlay is NOT inside the component render container', async () => {
      const user = userEvent.setup();
      const { container } = render(<PoolDrawer />);
      await waitFor(() => {
        expect(screen.getByText('Pro')).toBeInTheDocument();
      });
      await openDrawer(user);

      const overlay = screen.getByText('Claude Pool').closest('[class*="fixed"]');
      expect(container.contains(overlay)).toBe(false);
    });

    it('drawer overlay uses z-[70], higher than z-50 used by page overlays', async () => {
      const user = userEvent.setup();
      await renderAndWaitForPro();
      await openDrawer(user);

      const overlay = screen.getByText('Claude Pool').closest('[class*="fixed"]');
      expect(overlay!.className).toContain('z-[70]');
      expect(overlay!.className).not.toMatch(/\bz-50\b/);
    });

    it('drawer overlay has fixed positioning with full viewport coverage', async () => {
      const user = userEvent.setup();
      await renderAndWaitForPro();
      await openDrawer(user);

      const overlay = screen.getByText('Claude Pool').closest('[class*="fixed"]');
      expect(overlay!.className).toContain('fixed');
      expect(overlay!.className).toContain('inset-0');
    });

    it('drawer panel has safe-area-inset-top padding for mobile notch/status bar', async () => {
      const user = userEvent.setup();
      await renderAndWaitForPro();
      await openDrawer(user);

      const panel = screen.getByText('Claude Pool').closest('[class*="max-w-sm"]');
      expect(panel).toBeTruthy();
      expect(panel!.className).toContain('pt-[env(safe-area-inset-top)]');
    });

    it('drawer portal escapes a header ancestor with position:relative', async () => {
      const user = userEvent.setup();

      const headerLike = document.createElement('header');
      headerLike.className = 'bg-gray-900 border-b';
      headerLike.style.position = 'relative';
      document.body.appendChild(headerLike);

      const innerDiv = document.createElement('div');
      headerLike.appendChild(innerDiv);

      render(<PoolDrawer />, { container: innerDiv });
      await waitFor(() => {
        expect(screen.getByText('Pro')).toBeInTheDocument();
      });
      await openDrawer(user);

      const overlay = screen.getByText('Claude Pool').closest('[class*="fixed"]');
      expect(overlay!.parentElement).toBe(document.body);
      expect(headerLike.contains(overlay!)).toBe(false);

      headerLike.remove();
    });

    it('drawer z-index (70) is numerically greater than ChatView z-index (50)', async () => {
      const user = userEvent.setup();
      await renderAndWaitForPro();
      await openDrawer(user);

      const drawerOverlay = screen.getByText('Claude Pool').closest('[class*="fixed"]') as HTMLElement;
      const match = drawerOverlay.className.match(/z-\[(\d+)\]/);
      expect(match).toBeTruthy();
      const drawerZ = parseInt(match![1], 10);
      expect(drawerZ).toBeGreaterThan(50);
    });
  });

  describe('drawer open/close behavior', () => {
    it('opens drawer on Pro button click', async () => {
      const user = userEvent.setup();
      await renderAndWaitForPro();

      expect(screen.queryByText('Claude Pool')).not.toBeInTheDocument();

      await user.click(screen.getByText('Pro'));

      await waitFor(() => {
        expect(screen.getByText('Claude Pool')).toBeInTheDocument();
      });
    });

    it('closes drawer on backdrop click', async () => {
      const user = userEvent.setup();
      await renderAndWaitForPro();
      await openDrawer(user);

      const overlay = screen.getByText('Claude Pool').closest('[class*="fixed"]');
      const backdrop = overlay!.querySelector('[class*="bg-black"]');
      expect(backdrop).toBeTruthy();

      await user.click(backdrop!);

      await waitFor(() => {
        expect(screen.queryByText('Claude Pool')).not.toBeInTheDocument();
      });
    });

    it('closes drawer on X button click', async () => {
      const user = userEvent.setup();
      await renderAndWaitForPro();
      await openDrawer(user);

      const overlay = screen.getByText('Claude Pool').closest('[class*="fixed"]') as HTMLElement;
      const headerBar = within(overlay).getByText('Claude Pool').closest('div[class*="border-b"]') as HTMLElement;
      const buttons = within(headerBar).getAllByRole('button');
      const closeButton = buttons[buttons.length - 1];

      await user.click(closeButton);

      await waitFor(() => {
        expect(screen.queryByText('Claude Pool')).not.toBeInTheDocument();
      });
    });

    it('removes portal element from body when drawer closes', async () => {
      const user = userEvent.setup();
      await renderAndWaitForPro();
      await openDrawer(user);

      let portalElements = Array.from(document.body.children).filter(
        (el) => el instanceof HTMLElement && el.className.includes('z-[70]')
      );
      expect(portalElements.length).toBe(1);

      const backdrop = screen.getByText('Claude Pool').closest('[class*="fixed"]')!.querySelector('[class*="bg-black"]');
      await user.click(backdrop!);

      await waitFor(() => {
        portalElements = Array.from(document.body.children).filter(
          (el) => el instanceof HTMLElement && el.className?.includes?.('z-[70]')
        );
        expect(portalElements.length).toBe(0);
      });
    });

    it('can reopen drawer after closing and portal still targets body', async () => {
      const user = userEvent.setup();
      await renderAndWaitForPro();
      await openDrawer(user);

      const backdrop = screen.getByText('Claude Pool').closest('[class*="fixed"]')!.querySelector('[class*="bg-black"]');
      await user.click(backdrop!);

      await waitFor(() => {
        expect(screen.queryByText('Claude Pool')).not.toBeInTheDocument();
      });

      await openDrawer(user);

      const overlay = screen.getByText('Claude Pool').closest('[class*="fixed"]');
      expect(overlay!.parentElement).toBe(document.body);
    });
  });

  describe('drawer content rendering', () => {
    it('displays account info after opening', async () => {
      const user = userEvent.setup();
      await renderAndWaitForPro();
      await openDrawer(user);

      expect(screen.getByText('acc-1')).toBeInTheDocument();
      expect(screen.getByText('user1@example.com')).toBeInTheDocument();
      expect(screen.getByText('2/2 可用')).toBeInTheDocument();
    });

    it('displays subscription badge', async () => {
      const user = userEvent.setup();
      await renderAndWaitForPro();
      await openDrawer(user);

      expect(screen.getByText('pro')).toBeInTheDocument();
    });

    it('shows loading state before data loads', async () => {
      vi.mocked(api.getPoolUsage).mockImplementation(() => new Promise(() => {}));

      const user = userEvent.setup();
      await renderAndWaitForPro();

      await user.click(screen.getByText('Pro'));

      await waitFor(() => {
        expect(screen.getByText('加载中…')).toBeInTheDocument();
      });
    });

    it('explains when the current live Codex quota cannot be confirmed', async () => {
      enableCodexPool({
        total: 1,
        available: 1,
        preferred: null,
        accounts: [{
          id: 'codex-1',
          email: 'codex@example.com',
          codex_home: '/tmp/codex-1',
          available: true,
          enabled: true,
          quota: null,
          quota_error: 'live_unavailable',
        }],
      });
      const user = userEvent.setup();
      await renderAndWaitForPro();
      await openDrawer(user);
      await user.click(screen.getByRole('button', { name: 'Codex' }));

      expect(await screen.findByText('实时额度查询失败，无法确认当前额度')).toBeInTheDocument();
    });
  });

  describe('CloudRouter API accounts', () => {
    const apiQuota = {
      state: 'active',
      mode: 'quota_limited',
      currency: 'USD',
      remaining: 42.5,
      quota: {
        used: 7.5,
        limit: 50,
        remaining: 42.5,
      },
      plan_name: 'CloudRouter Pro',
      expires_at: '2026-08-07T00:00:00Z',
      days_until_expiry: 14,
      windows: [{
        id: 'daily',
        label: '每日额度',
        used: 7.5,
        limit: 50,
        remaining: 42.5,
        reset_at: '2026-07-25T00:00:00Z',
      }],
    };
    const apiAccount = {
      id: 'cloudrouter-1',
      name: 'CloudRouter Claude',
      api_provider: 'cloudrouter' as const,
      auth_kind: 'cloudrouter_api' as const,
      enabled: true,
      retired: false,
      key_hint: 'cr-…1234',
      models: {
        claude: ['claude-sonnet-4-6'],
        codex: [],
      },
      providers: ['claude'],
      account_dir: '/tmp/cloudrouter-1',
      claude_config_dir: '/tmp/cloudrouter-1/claude',
      codex_home: '/tmp/cloudrouter-1/codex',
      supported_models: ['claude-sonnet-4-6'],
      endpoints: {
        models: 'https://console.cloudrouter.online/v1/models',
        usage: 'https://console.cloudrouter.online/v1/usage',
      },
    };

    it('allows adding the first API key when both native pools are disabled', async () => {
      vi.mocked(api.getPoolStatus).mockResolvedValue({ enabled: false } as never);
      vi.mocked(api.getCodexPoolStatus).mockRejectedValue(new Error('Codex pool disabled'));
      vi.mocked(api.createCloudRouterAccount).mockResolvedValue({
        ...apiAccount,
        api_quota: apiQuota,
      });
      vi.mocked(api.getPoolUsage).mockResolvedValue({
        enabled: true,
        total: 1,
        available: 1,
        preferred: null,
        accounts: [],
      } as never);
      const user = userEvent.setup();

      render(<PoolDrawer />);
      await user.click(await screen.findByText('API'));
      expect(screen.getByText('API 账号')).toBeInTheDocument();
      expect(screen.getByText(/还没有可用账号/)).toBeInTheDocument();
      expect(screen.queryByTitle('添加账号')).not.toBeInTheDocument();

      await user.click(screen.getByTitle('添加 API 账号'));
      await user.type(screen.getByLabelText('账号名称'), 'First API');
      await user.type(screen.getByLabelText('CloudRouter API Key'), 'cr-first-secret');
      await user.click(screen.getByRole('button', { name: '验证并添加' }));

      await waitFor(() => {
        expect(api.createCloudRouterAccount).toHaveBeenCalledWith({
          name: 'First API',
          api_key: 'cr-first-secret',
          api_provider: 'cloudrouter',
        });
      });
    });

    it('adds one key as a dedicated API account without exposing a task-level mode', async () => {
      vi.mocked(api.createCloudRouterAccount).mockResolvedValue({
        ...apiAccount,
        api_quota: apiQuota,
      });
      const user = userEvent.setup();

      await renderAndWaitForPro();
      await openDrawer(user);
      await user.click(screen.getByTitle('添加 API 账号'));

      expect(screen.getByText(/每把 Key 建立一个独立 API 账号目录/)).toBeInTheDocument();
      expect(screen.getByText(/0600 权限持久保存/)).toBeInTheDocument();
      expect(screen.getByText(/通常需要分别添加两把 Key/)).toBeInTheDocument();
      expect(screen.getByLabelText('API 渠道')).toHaveValue('cloudrouter');
      await user.type(screen.getByLabelText('账号名称'), 'CloudRouter Claude');
      await user.type(screen.getByLabelText('CloudRouter API Key'), 'cr-secret-value');
      await user.click(screen.getByRole('button', { name: '验证并添加' }));

      await waitFor(() => {
        expect(api.createCloudRouterAccount).toHaveBeenCalledWith({
          name: 'CloudRouter Claude',
          api_key: 'cr-secret-value',
          api_provider: 'cloudrouter',
        });
        expect(screen.queryByText('添加 API 账号')).not.toBeInTheDocument();
      });
      expect(api.getPoolUsage).toHaveBeenCalledWith(false);
      expect(api.getCodexPoolUsage).toHaveBeenCalledWith(false);
    });

    it('adds an ApexRouter key as a dual-provider API account', async () => {
      vi.mocked(api.createCloudRouterAccount).mockResolvedValue({
        ...apiAccount,
        id: 'apex-1',
        name: 'Apex',
        api_provider: 'apex',
        auth_kind: 'apex_api',
        models: {
          claude: ['claude-opus-4-8'],
          codex: ['gpt-5.4'],
        },
        providers: ['claude', 'codex'],
        supported_models: ['claude-opus-4-8', 'gpt-5.4'],
        api_quota: {
          state: 'active',
          mode: 'shared_group',
          unit: 'credits',
          known: true,
          key_name: 'test-key',
          group_name: 'test-group',
          key_usage: { requests_5h: 3 },
          windows: [{
            id: 'requests_5h',
            label: '5h 请求（分组共享）',
            scope: 'group',
            key_used: 3,
            used: 12,
            limit: 100,
            remaining: 88,
            currency: 'requests',
          }],
          concurrency: 3,
        },
      });
      const user = userEvent.setup();

      await renderAndWaitForPro();
      await openDrawer(user);
      await user.click(screen.getByTitle('添加 API 账号'));
      await user.selectOptions(screen.getByLabelText('API 渠道'), 'apex');

      expect(screen.getByLabelText('ApexRouter API Key')).toBeInTheDocument();
      expect(screen.getByText(/自动识别该 Key 可用于 Claude、Codex 或两者/)).toBeInTheDocument();
      expect(screen.getByText(/Anthropic Messages 与 OpenAI Responses/)).toBeInTheDocument();
      expect(screen.getByText(/额度通过 \/v1\/usage 获取/)).toBeInTheDocument();
      expect(screen.getByText(/剩余、上限与并发限制由同组 Key 共享/)).toBeInTheDocument();
      expect(screen.getByText(/当前不返回到期时间/)).toBeInTheDocument();

      await user.type(screen.getByLabelText('账号名称'), 'Apex');
      await user.type(screen.getByLabelText('ApexRouter API Key'), 'lck_test_only_not_real');
      await user.click(screen.getByRole('button', { name: '验证并添加' }));

      await waitFor(() => {
        expect(api.createCloudRouterAccount).toHaveBeenCalledWith({
          name: 'Apex',
          api_key: 'lck_test_only_not_real',
          api_provider: 'apex',
        });
      });
    });

    it('adds an APIBest key as a dual-provider API account', async () => {
      vi.mocked(api.createCloudRouterAccount).mockResolvedValue({
        ...apiAccount,
        id: 'apibest-1',
        name: 'APIBest',
        api_provider: 'apibest',
        auth_kind: 'apibest_api',
      });
      const user = userEvent.setup();

      await renderAndWaitForPro();
      await openDrawer(user);
      await user.click(screen.getByTitle('添加 API 账号'));
      await user.selectOptions(screen.getByLabelText('API 渠道'), 'apibest');

      expect(screen.getByLabelText('APIBest API Key')).toBeInTheDocument();
      expect(screen.getByText(/公开价格目录识别可用的 Claude 与 Codex 模型/)).toBeInTheDocument();
      await user.type(screen.getByLabelText('账号名称'), 'APIBest');
      await user.type(screen.getByLabelText('APIBest API Key'), 'sk-test-only');
      await user.click(screen.getByRole('button', { name: '验证并添加' }));

      await waitFor(() => {
        expect(api.createCloudRouterAccount).toHaveBeenCalledWith({
          name: 'APIBest',
          api_key: 'sk-test-only',
          api_provider: 'apibest',
        });
      });
    });

    it('deletes a Claude API projection by shared api_account_id and refreshes both pools', async () => {
      vi.mocked(api.getPoolUsage).mockResolvedValue({
        enabled: true,
        total: 1,
        available: 1,
        cooldown: 0,
        disabled: 0,
        preferred: null,
        accounts: [{
          id: 'cloudrouter:cloudrouter-1:claude',
          config_dir: '/tmp/cloudrouter-1/claude',
          email: '',
          role: 'api',
          enabled: true,
          available: true,
          cooldown_until: null,
          cooldown_remaining: 0,
          auth_kind: 'cloudrouter_api',
          display_name: 'CloudRouter Claude',
          api_account_id: 'cloudrouter-1',
          supported_models: ['claude-sonnet-4-6', 'claude-opus-4-8'],
          api_quota: apiQuota,
        }],
      });
      vi.mocked(api.deleteCloudRouterAccount).mockResolvedValue({
        ...apiAccount,
        ok: true,
        enabled: false,
        retired: true,
        cleanup_pending: false,
      });
      const confirm = vi.spyOn(window, 'confirm').mockReturnValue(true);
      const user = userEvent.setup();

      await renderAndWaitForPro();
      await openDrawer(user);

      const card = screen.getByText('CloudRouter Claude').closest('.rounded-lg') as HTMLElement;
      expect(card).toBeTruthy();
      expect(within(card).getByText('API')).toBeInTheDocument();
      expect(within(card).getByText('claude-sonnet-4-6')).toBeInTheDocument();
      expect(within(card).getByText('claude-opus-4-8')).toBeInTheDocument();
      expect(card).toHaveTextContent('CLAUDE_CONFIG_DIR: /tmp/cloudrouter-1/claude');
      const reveal = within(card).getByRole('button', { name: '查看额度与有效期' });
      expect(reveal).toHaveAttribute('aria-expanded', 'false');
      expect(within(card).queryByText('active')).not.toBeInTheDocument();
      expect(card).not.toHaveTextContent('14 天');

      await user.click(reveal);

      expect(within(card).getByText('active')).toBeInTheDocument();
      expect(within(card).getByText('quota_limited')).toBeInTheDocument();
      expect(within(card).getByText('CloudRouter Pro')).toBeInTheDocument();
      expect(within(card).getByText('总额度')).toBeInTheDocument();
      expect(within(card).getByText('14 天')).toBeInTheDocument();
      expect(within(card).getAllByText(/剩余 \$42\.5/).length).toBeGreaterThan(0);
      expect(card).not.toHaveTextContent('无法通过此 Key 获取');
      expect(card).not.toHaveTextContent('Key 无独立额度上限');
      expect(card).not.toHaveTextContent('等于所属账号额度上限');
      await user.click(within(card).getByRole('button', { name: '收起额度与有效期' }));
      expect(within(card).queryByText('active')).not.toBeInTheDocument();
      expect(card).not.toHaveTextContent('14 天');
      expect(within(card).queryByRole('button', { name: '重新登录' })).not.toBeInTheDocument();

      await user.click(within(card).getByRole('button', { name: '删除 API 账号' }));

      await waitFor(() => {
        expect(api.deleteCloudRouterAccount).toHaveBeenCalledWith('cloudrouter-1');
      });
      expect(api.deleteCloudRouterAccount).not.toHaveBeenCalledWith(
        'cloudrouter:cloudrouter-1:claude',
      );
      expect(confirm).toHaveBeenCalledWith(expect.stringContaining(
        '同时从 Claude 与 Codex 账号视图停用',
      ));
      expect(confirm).toHaveBeenCalledWith(expect.stringContaining(
        '永久删除 API Key 和账号配置，无法恢复',
      ));
      expect(confirm).toHaveBeenCalledWith(expect.stringContaining(
        'Claude projects 与 Codex sessions 会保留',
      ));
      expect(confirm).toHaveBeenCalledWith(expect.stringContaining(
        '活跃任务不会被强制终止',
      ));
      expect(api.getPoolUsage).toHaveBeenCalledWith(false);
      expect(api.getCodexPoolUsage).toHaveBeenCalledWith(false);
    });

    it('keeps a busy API tombstone visible with a safe cleanup retry action', async () => {
      vi.mocked(api.getCloudRouterAccounts).mockResolvedValue([{
        ...apiAccount,
        id: 'cloudrouter-pending',
        name: 'CloudRouter Pending',
        enabled: false,
        retired: true,
        cleanup_pending: true,
        cleanup_code: 'runtime_busy',
        cleanup_reason: 'Task #42 is still using this API account',
        cleanup_last_attempt_at: 1786802400,
        cleanup_last_error_at: 1786802401,
        models: {
          claude: ['claude-sonnet-4-6'],
          codex: ['gpt-5.5'],
        },
        providers: ['claude', 'codex'],
        supported_models: ['claude-sonnet-4-6', 'gpt-5.5'],
      }]);
      vi.mocked(api.getPoolUsage).mockResolvedValue({
        enabled: true,
        total: 1,
        available: 0,
        cooldown: 0,
        disabled: 1,
        preferred: 'cloudrouter:cloudrouter-pending:claude',
        last_selected: 'cloudrouter:cloudrouter-pending:claude',
        accounts: [{
          id: 'cloudrouter:cloudrouter-pending:claude',
          config_dir: '/tmp/cloudrouter-pending/claude',
          email: '',
          role: 'api',
          enabled: false,
          available: false,
          cooldown_until: null,
          cooldown_remaining: 0,
          auth_kind: 'cloudrouter_api',
          display_name: 'CloudRouter Pending',
          api_account_id: 'cloudrouter-pending',
          retired: true,
          cleanup_pending: true,
          supported_models: ['claude-sonnet-4-6'],
          api_quota: apiQuota,
        }],
      });
      enableCodexPool({
        enabled: true,
        total: 1,
        available: 0,
        cooldown: 0,
        disabled: 1,
        preferred: null,
        accounts: [{
          id: 'cloudrouter:cloudrouter-pending:codex',
          codex_home: '/tmp/cloudrouter-pending/codex',
          email: '',
          enabled: false,
          available: false,
          cooldown_until: null,
          cooldown_remaining: 0,
          auth_kind: 'cloudrouter_api',
          display_name: 'CloudRouter Pending',
          api_account_id: 'cloudrouter-pending',
          retired: true,
          cleanup_pending: true,
          supported_models: ['gpt-5.5'],
        }],
      });
      vi.mocked(api.deleteCloudRouterAccount).mockRejectedValue(
        Object.assign(new Error('Conflict'), {
          status: 409,
          detail: {
            message: 'API account is still in use',
            error: 'API account is still in use',
            code: 'runtime_busy',
            reason: 'Task #42 is still using this API account',
            cleanup_pending: true,
          },
        }),
      );
      const confirm = vi.spyOn(window, 'confirm').mockReturnValue(true);
      const alert = vi.spyOn(window, 'alert').mockImplementation(() => {});
      const user = userEvent.setup();

      await renderAndWaitForPro();
      await openDrawer(user);

      const card = screen.getByText('CloudRouter Pending').closest('.rounded-lg') as HTMLElement;
      expect(within(card).getByText('已停用')).toBeInTheDocument();
      expect(within(card).getByText('删除受阻')).toBeInTheDocument();
      expect(card).toHaveTextContent('账号已停用，新的 Claude/Codex 任务不会再使用它');
      expect(card).toHaveTextContent('API Key 与配置尚未删除');
      expect(card).toHaveTextContent('账号仍被运行中的任务或监控使用，请先停止相关运行再重试');
      expect(card).toHaveTextContent('后端详情：Task #42 is still using this API account');
      expect(card).toHaveTextContent('Task #42 is still using this API account');
      expect(card).toHaveTextContent('runtime_busy');
      expect(card).toHaveTextContent('最近失败：');
      expect(card).toHaveTextContent('Claude projects 与 Codex sessions 会保留');
      expect(card).toHaveTextContent('同一共享 API 账号会投影到 Claude 与 Codex 页签');
      expect(card).not.toHaveTextContent('待清理');
      expect(within(card).queryByText('优先账号')).not.toBeInTheDocument();
      expect(within(card).queryByText('最近使用')).not.toBeInTheDocument();
      expect(within(card).queryByRole('button', { name: '切换到此账号' })).not.toBeInTheDocument();
      expect(within(card).queryByRole('button', { name: '查看额度与有效期' })).not.toBeInTheDocument();

      await user.click(screen.getByRole('button', { name: 'Codex' }));
      const codexCard = (await screen.findByText('CloudRouter Pending')).closest('.rounded-lg') as HTMLElement;
      expect(codexCard).toHaveTextContent('Task #42 is still using this API account');
      expect(codexCard).toHaveTextContent('同一共享 API 账号会投影到 Claude 与 Codex 页签');
      expect(within(codexCard).getByRole('button', { name: '重试清理' })).toBeEnabled();

      await user.click(screen.getByRole('button', { name: 'Claude' }));
      const retryCard = (await screen.findByText('CloudRouter Pending')).closest('.rounded-lg') as HTMLElement;
      await user.click(within(retryCard).getByRole('button', { name: '重试清理' }));

      await waitFor(() => {
        expect(api.deleteCloudRouterAccount).toHaveBeenCalledWith('cloudrouter-pending');
        expect(alert).toHaveBeenCalledWith(expect.stringContaining(
          '账号已停用，但删除受阻：账号仍被运行中的任务或监控使用，请先停止相关运行再重试',
        ));
        expect(alert).toHaveBeenCalledWith(expect.stringContaining(
          '后端详情：Task #42 is still using this API account',
        ));
      });
      expect(confirm).toHaveBeenCalledWith(expect.stringContaining(
        '重试清理 API 账号“CloudRouter Pending”',
      ));
      expect(confirm).toHaveBeenCalledWith(expect.stringContaining(
        '若阻塞仍未解除，账号会继续显示“删除受阻”',
      ));
      expect(api.getPoolUsage).toHaveBeenCalledWith(false);
      expect(api.getCodexPoolUsage).toHaveBeenCalledWith(false);
      expect(within(retryCard).getByRole('button', { name: '重试清理' })).toBeEnabled();
    });

    it('renders the same API account in Codex without OAuth controls and refreshes its shared quota', async () => {
      enableCodexPool({
        enabled: true,
        total: 1,
        available: 1,
        cooldown: 0,
        disabled: 0,
        preferred: null,
        accounts: [{
          id: 'cloudrouter:cloudrouter-2:codex',
          codex_home: '/tmp/cloudrouter-2/codex',
          email: '',
          enabled: true,
          available: true,
          cooldown_until: null,
          cooldown_remaining: 0,
          auth_kind: 'cloudrouter_api',
          display_name: 'CloudRouter Codex',
          api_account_id: 'cloudrouter-2',
          supported_models: ['gpt-5.5'],
          api_quota: apiQuota,
        }],
      });
      vi.mocked(api.refreshCloudRouterAccount).mockResolvedValue({
        ...apiAccount,
        id: 'cloudrouter-2',
        name: 'CloudRouter Codex',
        models: {
          claude: [],
          codex: ['gpt-5.5'],
        },
        providers: ['codex'],
        supported_models: ['gpt-5.5'],
        api_quota: apiQuota,
      });
      const user = userEvent.setup();

      await renderAndWaitForPro();
      await openDrawer(user);
      await user.click(screen.getByRole('button', { name: 'Codex' }));

      const card = (await screen.findByText('CloudRouter Codex')).closest('.rounded-lg') as HTMLElement;
      expect(within(card).getByText('API')).toBeInTheDocument();
      expect(within(card).getByText('gpt-5.5')).toBeInTheDocument();
      expect(within(card).queryByRole('button', { name: '重新登录' })).not.toBeInTheDocument();
      expect(within(card).queryByLabelText('OpenAI 邮箱验证码')).not.toBeInTheDocument();

      await user.click(within(card).getByRole('button', { name: '查看额度与有效期' }));
      await user.click(within(card).getByRole('button', { name: '刷新额度' }));
      await waitFor(() => {
        expect(api.refreshCloudRouterAccount).toHaveBeenCalledWith('cloudrouter-2');
      });
      expect(api.getPoolUsage).toHaveBeenCalledWith(false);
      expect(api.getCodexPoolUsage).toHaveBeenCalledWith(false);
    });

    it('renders an Apex projection with separate Key and shared-group quota', async () => {
      enableCodexPool({
        enabled: true,
        total: 1,
        available: 1,
        cooldown: 0,
        disabled: 0,
        preferred: null,
        accounts: [{
          id: 'apex:apex-1:codex',
          codex_home: '/tmp/apex-1/codex',
          email: '',
          enabled: true,
          available: true,
          cooldown_until: null,
          cooldown_remaining: 0,
          auth_kind: 'apex_api',
          api_provider: 'apex',
          display_name: 'Apex Codex',
          api_account_id: 'apex-1',
          supported_models: ['gpt-5.4'],
          api_quota: {
            state: 'active',
            mode: 'shared_group',
            known: true,
            key_name: 'apex-test-key',
            group_name: 'apex-test-group',
            key_usage: { requests_5h: 3 },
            concurrency: 3,
            windows: [{
              id: 'requests_5h',
              label: '5h 请求（分组共享）',
              scope: 'group',
              key_used: 3,
              used: 12,
              limit: 100,
              remaining: 88,
              currency: 'requests',
            }],
          },
        }],
      });
      vi.mocked(api.refreshCloudRouterAccount).mockResolvedValue({
        ...apiAccount,
        id: 'apex-1',
        name: 'Apex Codex',
        api_provider: 'apex',
        auth_kind: 'apex_api',
        models: {
          claude: [],
          codex: ['gpt-5.4'],
        },
        providers: ['codex'],
        supported_models: ['gpt-5.4'],
      });
      const user = userEvent.setup();

      await renderAndWaitForPro();
      await openDrawer(user);
      await user.click(screen.getByRole('button', { name: 'Codex' }));

      const card = (await screen.findByText('Apex Codex')).closest('.rounded-lg') as HTMLElement;
      expect(within(card).getByText('APEXROUTER API')).toBeInTheDocument();
      expect(within(card).getByText('gpt-5.4')).toBeInTheDocument();
      expect(within(card).queryByRole('button', { name: '重新登录' })).not.toBeInTheDocument();

      await user.click(within(card).getByRole('button', { name: '查看额度与有效期' }));
      expect(card).toHaveTextContent('Key apex-test-key');
      expect(card).toHaveTextContent('分组 apex-test-group');
      expect(card).toHaveTextContent('本 Key 已用 3 requests');
      expect(card).toHaveTextContent('分组共享已用 12 requests');
      expect(card).toHaveTextContent('分组共享剩余 88 requests');
      expect(card).toHaveTextContent('分组并发上限 3');
      expect(card).toHaveTextContent('5h 请求（分组共享）');
      expect(card).not.toHaveTextContent('总额度：无法确认');
      expect(card).toHaveTextContent('到期时间：无法确认');
      expect(card).toHaveTextContent('剩余天数：无法确认');

      await user.click(within(card).getByRole('button', { name: '刷新额度' }));
      await waitFor(() => {
        expect(api.refreshCloudRouterAccount).toHaveBeenCalledWith('apex-1');
      });
    });

    it('renders Apex null quota windows as unlimited instead of unavailable', async () => {
      enableCodexPool({
        enabled: true,
        total: 1,
        available: 1,
        cooldown: 0,
        disabled: 0,
        preferred: null,
        accounts: [{
          id: 'apex:apex-1:codex',
          codex_home: '/tmp/apex-1/codex',
          email: '',
          enabled: true,
          available: true,
          cooldown_until: null,
          cooldown_remaining: 0,
          auth_kind: 'apex_api',
          api_provider: 'apex',
          display_name: 'Apex Unlimited',
          api_account_id: 'apex-1',
          supported_models: ['gpt-5.4'],
          api_quota: {
            state: 'active',
            mode: 'shared_group',
            known: true,
            key_name: 'apex-unlimited-key',
            group_name: 'apex-unlimited-group',
            key_usage: { requests_5h: 3 },
            concurrency: 3,
            windows: [{
              id: 'requests_5h',
              label: '5h 请求（分组共享）',
              scope: 'group',
              unlimited: true,
              key_used: 3,
              currency: 'requests',
            }],
          },
        }],
      });
      const user = userEvent.setup();

      await renderAndWaitForPro();
      await openDrawer(user);
      await user.click(screen.getByRole('button', { name: 'Codex' }));

      const card = (await screen.findByText('Apex Unlimited')).closest('.rounded-lg') as HTMLElement;
      await user.click(within(card).getByRole('button', { name: '查看额度与有效期' }));

      expect(card).toHaveTextContent('状态 active');
      expect(card).toHaveTextContent('分组不限额');
      expect(card).toHaveTextContent('本 Key 已用 3 requests');
      expect(card).not.toHaveTextContent('总额度：无法确认');
      expect(card).not.toHaveTextContent('分组共享上限');
      expect(card).not.toHaveTextContent('分组共享剩余');
      expect(card).not.toHaveTextContent('重置时间无法确认');
      expect(card).toHaveTextContent('到期时间：无法确认');
    });

    it('renders unrestricted CloudRouter usage without presenting informational zeroes as the Key balance', async () => {
      vi.mocked(api.getCloudRouterAccounts).mockResolvedValue([{
        ...apiAccount,
        id: 'cloudrouter-5',
        name: 'CloudRouter Unlimited',
        key_hint: 'cr-…9abc',
      }]);
      vi.mocked(api.getPoolUsage).mockResolvedValue({
        enabled: true,
        total: 1,
        available: 1,
        cooldown: 0,
        disabled: 0,
        preferred: null,
        accounts: [{
          id: 'cloudrouter-5',
          config_dir: '/tmp/cloudrouter-5/claude',
          email: '',
          role: 'api',
          enabled: true,
          available: true,
          cooldown_until: null,
          cooldown_remaining: 0,
          auth_kind: 'cloudrouter_api',
          api_provider: 'cloudrouter',
          display_name: 'CloudRouter Unlimited',
          api_account_id: 'cloudrouter-5',
          supported_models: ['claude-sonnet-4-6'],
          api_quota: {
            state: 'active',
            mode: 'unrestricted',
            known: true,
            stale: true,
            currency: 'USD',
            balance: 0,
            remaining: 0,
            fetched_at: '2026-07-28T08:30:00Z',
            refresh_failed_at: '2026-07-28T09:00:00Z',
            usage: {
              today: {
                actual_cost: 0.4662,
                requests: 2,
                total_tokens: 5967,
              },
              total: {
                actual_cost: 1.25,
                requests: 7,
                total_tokens: 12345,
              },
              daily_usage: [{
                date: '2026-07-28',
                actual_cost: 0.4662,
                requests: 2,
                total_tokens: 5967,
              }],
              model_stats: [{
                model: 'claude-sonnet-4-6',
                actual_cost: 0.4662,
                requests: 2,
                total_tokens: 5967,
              }],
            },
          },
        }],
      });
      const user = userEvent.setup();

      await renderAndWaitForPro();
      await openDrawer(user);

      const card = (await screen.findByText('CloudRouter Unlimited')).closest('.rounded-lg') as HTMLElement;
      expect(await within(card).findByText('Key 指纹：cr-…9abc')).toBeInTheDocument();
      await user.click(within(card).getByRole('button', { name: '查看额度与有效期' }));

      expect(card).toHaveTextContent('Key 无独立额度上限');
      expect(card).toHaveTextContent('Key 额度上限：等于所属账号额度上限');
      expect(card).toHaveTextContent('Key 剩余额度：等于所属账号剩余额度');
      expect(card).toHaveTextContent('Key 使用时间：不限制');
      expect(card).toHaveTextContent('此 Key 未设置独立额度，实际额度随所属账号');
      expect(card).toHaveTextContent('账号额度具体数值、充值和结算状态请前往 CloudRouter 控制台查看');
      expect(card).toHaveTextContent('到期时间：不限制');
      expect(card).toHaveTextContent('剩余天数：不限制');
      expect(card).not.toHaveTextContent('Key 总额度：不限');
      expect(card).not.toHaveTextContent('共享组织池');
      expect(card).not.toHaveTextContent('组织总额度：接口未提供');
      expect(card).not.toHaveTextContent('无法通过此 Key 获取');
      expect(card).not.toHaveTextContent('接口未返回');
      expect(card).not.toHaveTextContent('到期时间：无法确认');
      expect(card).toHaveTextContent('当前 Key 今日用量');
      expect(card).toHaveTextContent('费用 $0.4662');
      expect(card).toHaveTextContent('请求 2');
      expect(card).toHaveTextContent('Tokens 5,967');
      expect(card).toHaveTextContent('当前 Key 累计用量');
      expect(card).toHaveTextContent('费用 $1.25');
      expect(card).toHaveTextContent('缓存数据');
      expect(card).not.toHaveTextContent('剩余 $0');
      expect(card).not.toHaveTextContent('余额 $0');
      expect(card).not.toHaveTextContent('总额度：无法确认');
      expect(card).not.toHaveTextContent('数据时间：无法确认');
      expect(card).toHaveTextContent('刷新失败时间：');

      await user.click(within(card).getByText('逐日用量（最多显示 20 条）'));
      await user.click(within(card).getByText('逐模型用量（最多显示 20 条）'));
      expect(card).toHaveTextContent('2026-07-28');
      expect(card).toHaveTextContent('claude-sonnet-4-6');
    });

    it('does not trust a Key hint carried only by an ordinary Pool projection', async () => {
      vi.mocked(api.getCloudRouterAccounts).mockRejectedValue(new Error('admin access required'));
      vi.mocked(api.getPoolUsage).mockResolvedValue({
        enabled: true,
        total: 1,
        available: 1,
        cooldown: 0,
        disabled: 0,
        preferred: null,
        accounts: [{
          id: 'cloudrouter-6',
          config_dir: '/tmp/cloudrouter-6/claude',
          email: '',
          role: 'api',
          enabled: true,
          available: true,
          cooldown_until: null,
          cooldown_remaining: 0,
          auth_kind: 'cloudrouter_api',
          display_name: 'Member Projection',
          api_account_id: 'cloudrouter-6',
          key_hint: 'must-not-render',
          supported_models: ['claude-sonnet-4-6'],
          api_quota: null,
        }],
      } as never);
      const user = userEvent.setup();

      await renderAndWaitForPro();
      await openDrawer(user);

      const card = screen.getByText('Member Projection').closest('.rounded-lg') as HTMLElement;
      expect(card).not.toHaveTextContent('Key 指纹');
      expect(card).not.toHaveTextContent('must-not-render');
    });

    it('marks missing quota and expiry fields as unconfirmed instead of displaying zero', async () => {
      vi.mocked(api.getPoolUsage).mockResolvedValue({
        enabled: true,
        total: 1,
        available: 1,
        cooldown: 0,
        disabled: 0,
        preferred: null,
        accounts: [{
          id: 'cloudrouter:cloudrouter-3:claude',
          config_dir: '/tmp/cloudrouter-3/claude',
          email: '',
          role: 'api',
          enabled: true,
          available: true,
          cooldown_until: null,
          cooldown_remaining: 0,
          auth_kind: 'cloudrouter_api',
          display_name: 'CloudRouter Unknown',
          api_account_id: 'cloudrouter-3',
          supported_models: ['claude-sonnet-4-6'],
          api_quota: {
            state: 'unknown',
            mode: 'wallet',
            currency: 'USD',
            windows: [],
          },
        }],
      });
      const user = userEvent.setup();

      await renderAndWaitForPro();
      await openDrawer(user);

      const card = screen.getByText('CloudRouter Unknown').closest('.rounded-lg') as HTMLElement;
      expect(card).not.toHaveTextContent('总额度：无法确认');
      await user.click(within(card).getByRole('button', { name: '查看额度与有效期' }));
      expect(card).toHaveTextContent('总额度：无法确认');
      expect(card).toHaveTextContent('到期时间：无法确认');
      expect(card).toHaveTextContent('剩余天数：无法确认');
      expect(card).not.toHaveTextContent('0 天');
      expect(card).not.toHaveTextContent('$0');
    });

    it('does not infer unlimited duration from an active wallet with no expiry fields', async () => {
      vi.mocked(api.getPoolUsage).mockResolvedValue({
        enabled: true,
        total: 1,
        available: 1,
        cooldown: 0,
        disabled: 0,
        preferred: null,
        accounts: [{
          id: 'cloudrouter:cloudrouter-4:claude',
          config_dir: '/tmp/cloudrouter-4/claude',
          email: '',
          role: 'api',
          enabled: true,
          available: true,
          cooldown_until: null,
          cooldown_remaining: 0,
          auth_kind: 'cloudrouter_api',
          display_name: 'CloudRouter Wallet',
          api_account_id: 'cloudrouter-4',
          supported_models: ['claude-opus-4-8'],
          api_quota: {
            state: 'active',
            mode: 'wallet',
            currency: 'USD',
            known: true,
            remaining: 3000,
            balance: 3000,
            plan_name: '钱包余额',
            windows: [],
          },
        }],
      });
      const user = userEvent.setup();

      await renderAndWaitForPro();
      await openDrawer(user);

      const card = screen.getByText('CloudRouter Wallet').closest('.rounded-lg') as HTMLElement;
      expect(card).not.toHaveTextContent('剩余 $3,000');
      await user.click(within(card).getByRole('button', { name: '查看额度与有效期' }));
      expect(card).toHaveTextContent('剩余 $3,000');
      expect(card).toHaveTextContent('余额 $3,000');
      expect(card).toHaveTextContent('到期时间：无法确认');
      expect(card).toHaveTextContent('剩余天数：无法确认');
      expect(card).not.toHaveTextContent('到期时间：无期限');
      expect(card).not.toHaveTextContent('剩余天数：无期限');
    });

    it('renders CloudRouter remaining=-1 as unlimited instead of a negative balance', async () => {
      vi.mocked(api.getPoolUsage).mockResolvedValue({
        enabled: true,
        total: 1,
        available: 1,
        cooldown: 0,
        disabled: 0,
        preferred: null,
        accounts: [{
          id: 'cloudrouter:cloudrouter-4:claude',
          config_dir: '/tmp/cloudrouter-4/claude',
          email: '',
          role: 'api',
          enabled: true,
          available: true,
          cooldown_until: null,
          cooldown_remaining: 0,
          auth_kind: 'cloudrouter_api',
          display_name: 'CloudRouter Unlimited',
          api_account_id: 'cloudrouter-4',
          supported_models: ['claude-sonnet-4-6'],
          api_quota: {
            state: 'active',
            mode: 'wallet',
            currency: 'USD',
            remaining: -1,
            windows: [],
          },
        }],
      });
      const user = userEvent.setup();

      await renderAndWaitForPro();
      await openDrawer(user);

      const card = screen.getByText('CloudRouter Unlimited').closest('.rounded-lg') as HTMLElement;
      expect(card).not.toHaveTextContent('剩余 无限');
      await user.click(within(card).getByRole('button', { name: '查看额度与有效期' }));
      expect(card).toHaveTextContent('剩余 无限');
      expect(card).not.toHaveTextContent('-$1');
    });
  });

  describe('Codex pool settings', () => {
    it('edits and persists runtime settings', async () => {
      const settings = {
        enabled: true,
        cooldown_seconds: 300,
        quota_switch_threshold_percent: 90,
        routing_policy: 'api_first' as const,
        preferred_account_id: null,
      };
      enableCodexPool({
        enabled: true,
        total: 0,
        available: 0,
        cooldown: 0,
        disabled: 0,
        preferred: null,
        settings,
        accounts: [],
      });
      vi.mocked(api.putCodexPoolSettings).mockResolvedValue(settings);
      const user = userEvent.setup();

      await renderAndWaitForPro();
      await openDrawer(user);
      await user.click(screen.getByRole('button', { name: 'Codex' }));
      await user.click(await screen.findByTitle('Codex 号池设置'));
      await user.clear(screen.getByLabelText('撞限冷却时间（秒）'));
      await user.type(screen.getByLabelText('撞限冷却时间（秒）'), '600');
      await user.clear(screen.getByLabelText('主动换号阈值（%）'));
      await user.type(screen.getByLabelText('主动换号阈值（%）'), '80');
      await user.selectOptions(screen.getByLabelText('新会话路由顺序'), 'native_first');
      await user.click(screen.getByRole('button', { name: '保存并生效' }));

      await waitFor(() => expect(api.putCodexPoolSettings).toHaveBeenCalledWith({
        ...settings,
        cooldown_seconds: 600,
        quota_switch_threshold_percent: 80,
        routing_policy: 'native_first',
      }));
      expect(api.getCodexPoolUsage).toHaveBeenLastCalledWith(false);
    });
  });

  describe('Codex account login source', () => {
    it('allows password-only login and keeps mailbox token optional', async () => {
      enableCodexPool();
      vi.mocked(api.codexPoolAddAccount).mockResolvedValue({ ok: true, status: 'running' });
      const user = userEvent.setup();

      await renderAndWaitForPro();
      await openDrawer(user);
      await user.click(screen.getByRole('button', { name: 'Codex' }));
      await waitFor(() => expect(screen.getByText('Codex Pool')).toBeInTheDocument());
      await user.click(screen.getByTitle('添加账号'));

      await user.type(screen.getByLabelText('OpenAI 邮箱'), 'password-only@mail.com');
      const addButton = screen.getByRole('button', { name: '添加' });
      expect(addButton).toBeEnabled();

      await user.type(screen.getByLabelText('OpenAI 密码（可选）'), 'openai-password');
      await user.click(addButton);

      await waitFor(() => {
        expect(api.codexPoolAddAccount).toHaveBeenCalledWith({
          email: 'password-only@mail.com',
          token: undefined,
          password: 'openai-password',
          login_method: undefined,
        });
        expect(screen.getByLabelText('OpenAI 密码（可选）')).toHaveValue('');
      });
    });

    it('allows email-only login and leaves OTP entry to the active attempt', async () => {
      enableCodexPool();
      vi.mocked(api.codexPoolAddAccount).mockResolvedValue({ ok: true, status: 'running' });
      const user = userEvent.setup();

      await renderAndWaitForPro();
      await openDrawer(user);
      await user.click(screen.getByRole('button', { name: 'Codex' }));
      await waitFor(() => expect(screen.getByText('Codex Pool')).toBeInTheDocument());
      await user.click(screen.getByTitle('添加账号'));

      await user.type(screen.getByLabelText('OpenAI 邮箱'), 'email-only@163.com');
      await user.click(screen.getByRole('button', { name: '添加' }));

      await waitFor(() => {
        expect(api.codexPoolAddAccount).toHaveBeenCalledWith({
          email: 'email-only@163.com',
          token: undefined,
          password: undefined,
          login_method: undefined,
        });
      });
    });

    it('offers generic MailCatcher and sends it for a 163 mailbox', async () => {
      enableCodexPool();
      vi.mocked(api.codexPoolAddAccount).mockResolvedValue({ ok: true, status: 'running' });
      const user = userEvent.setup();

      await renderAndWaitForPro();
      await openDrawer(user);
      await user.click(screen.getByRole('button', { name: 'Codex' }));
      await waitFor(() => expect(screen.getByText('Codex Pool')).toBeInTheDocument());
      await user.click(screen.getByTitle('添加账号'));

      const methodSelect = screen.getByLabelText('验证码邮箱来源');
      expect(within(methodSelect).getByRole('option', { name: '171mail（API 接码）' })).toHaveValue('171mail');
      expect(within(methodSelect).getByRole('option', { name: 'MailCatcher（163 / mail.com / Onet / Gazeta 等）' })).toHaveValue('mailcatcher');
      expect(within(methodSelect).getByRole('option', { name: 'mail.com（MailCatcher 接码）' })).toHaveValue('mailcom');
      expect(within(methodSelect).getByRole('option', { name: 'Onet（MailCatcher 接码）' })).toHaveValue('onet');
      expect(within(methodSelect).getByRole('option', { name: 'Gazeta（MailCatcher 接码）' })).toHaveValue('gazeta');

      await user.type(screen.getByLabelText('OpenAI 邮箱'), 'test-user@163.com');
      await user.selectOptions(methodSelect, 'mailcatcher');
      await user.type(screen.getByLabelText('MailCatcher 查询 Token（可选）'), 'mail-query-token');
      await user.click(screen.getByRole('button', { name: '添加' }));

      await waitFor(() => {
        expect(api.codexPoolAddAccount).toHaveBeenCalledWith({
          email: 'test-user@163.com',
          token: 'mail-query-token',
          password: undefined,
          login_method: 'mailcatcher',
        });
        expect(screen.getByLabelText('MailCatcher 查询 Token（可选）')).toHaveValue('');
      });
    });

    it('pauses for a human email code and resumes the same login attempt', async () => {
      enableCodexPool();
      vi.mocked(api.codexPoolAddAccount).mockResolvedValue({
        ok: true,
        status: 'running',
        attempt_id: 'attempt-1',
      });
      vi.mocked(api.codexPoolAddStatus).mockResolvedValue({
        status: 'awaiting_otp',
        attempt_id: 'attempt-1',
        challenge_id: 'challenge-1',
        expires_at: Math.floor(Date.now() / 1000) + 600,
      });
      vi.mocked(api.codexPoolSubmitOtp).mockResolvedValue({
        ok: true,
        status: 'verifying_otp',
      });
      const user = userEvent.setup();

      await renderAndWaitForPro();
      await openDrawer(user);
      await user.click(screen.getByRole('button', { name: 'Codex' }));
      await waitFor(() => expect(screen.getByText('Codex Pool')).toBeInTheDocument());
      await user.click(screen.getByTitle('添加账号'));
      await user.type(screen.getByLabelText('OpenAI 邮箱'), 'human@example.com');
      await user.type(screen.getByLabelText('OpenAI 密码（可选）'), 'openai-password');
      await user.click(screen.getByRole('button', { name: '添加' }));

      const otpInput = await screen.findByLabelText('OpenAI 邮箱验证码', {}, { timeout: 3000 });
      await user.type(otpInput, '654321');
      await user.click(screen.getByRole('button', { name: '继续登录' }));

      await waitFor(() => {
        expect(api.codexPoolSubmitOtp).toHaveBeenCalledWith(
          'attempt-1',
          'challenge-1',
          '654321',
        );
      });
      expect(screen.getByText('验证码已提交，正在继续登录…')).toBeInTheDocument();
    });

    it('keeps polling an add attempt after a transient status failure', async () => {
      enableCodexPool();
      vi.mocked(api.codexPoolAddAccount).mockResolvedValue({
        ok: true,
        status: 'running',
        attempt_id: 'add-attempt-retry',
      });
      vi.mocked(api.codexPoolAddStatus)
        .mockRejectedValueOnce(new Error('temporary network error'))
        .mockResolvedValue({
          status: 'awaiting_otp',
          attempt_id: 'add-attempt-retry',
          challenge_id: 'add-challenge-retry',
          expires_at: Math.floor(Date.now() / 1000) + 600,
        });
      const user = userEvent.setup();

      await renderAndWaitForPro();
      await openDrawer(user);
      await user.click(screen.getByRole('button', { name: 'Codex' }));
      await waitFor(() => expect(screen.getByText('Codex Pool')).toBeInTheDocument());
      await user.click(screen.getByTitle('添加账号'));
      await user.type(screen.getByLabelText('OpenAI 邮箱'), 'retry@example.com');
      await user.type(screen.getByLabelText('OpenAI 密码（可选）'), 'openai-password');
      await user.click(screen.getByRole('button', { name: '添加' }));

      await waitFor(() => {
        expect(screen.getByText(/状态查询暂时失败，正在重试/)).toBeInTheDocument();
      }, { timeout: 2500 });
      await screen.findByLabelText(
        'OpenAI 邮箱验证码',
        {},
        { timeout: 5000 },
      );
      expect(api.codexPoolAddStatus).toHaveBeenCalledTimes(2);
    });

    it('keeps an add attempt active while the backend safely finalizes it', async () => {
      enableCodexPool();
      vi.mocked(api.codexPoolAddAccount).mockResolvedValue({
        ok: true,
        status: 'running',
        attempt_id: 'add-attempt-finalizing',
      });
      vi.mocked(api.codexPoolAddStatus)
        .mockResolvedValueOnce({
          status: 'finalizing',
          attempt_id: 'add-attempt-finalizing',
        })
        .mockResolvedValue({
          status: 'success',
          attempt_id: 'add-attempt-finalizing',
        });
      const user = userEvent.setup();

      await renderAndWaitForPro();
      await openDrawer(user);
      await user.click(screen.getByRole('button', { name: 'Codex' }));
      await waitFor(() => expect(screen.getByText('Codex Pool')).toBeInTheDocument());
      await user.click(screen.getByTitle('添加账号'));
      await user.type(screen.getByLabelText('OpenAI 邮箱'), 'finalizing@example.com');
      await user.type(screen.getByLabelText('OpenAI 密码（可选）'), 'openai-password');
      await user.click(screen.getByRole('button', { name: '添加' }));

      expect(await screen.findByText(
        '登录已完成，正在安全提交登录结果…',
        {},
        { timeout: 3000 },
      )).toBeInTheDocument();
      expect(screen.getByTitle('请等待登录完成')).toBeDisabled();

      await waitFor(() => {
        expect(api.codexPoolAddStatus).toHaveBeenCalledTimes(2);
        expect(screen.queryByText('登录已完成，正在安全提交登录结果…')).not.toBeInTheDocument();
      }, { timeout: 5000 });
    });
  });

  describe('Codex account controls', () => {
    const codexAccount = {
      id: 'codex-2',
      email: 'codex@example.com',
      codex_home: '/home/ubuntu/.codex-codex-2',
      enabled: true,
      available: true,
      cooldown_until: null,
      cooldown_remaining: 0,
      plan_type: 'pro',
      quota: null,
      quota_error: 'no_rollout_data',
    };

    async function openCodexTab(user: ReturnType<typeof userEvent.setup>) {
      await renderAndWaitForPro();
      await openDrawer(user);
      await user.click(screen.getByRole('button', { name: 'Codex' }));
      await waitFor(() => expect(screen.getByText('Codex Pool')).toBeInTheDocument());
    }

    it('loads live quota when the Codex tab is opened for the first time', async () => {
      enableCodexPool();
      const user = userEvent.setup();

      await openCodexTab(user);

      await waitFor(() => {
        expect(api.getCodexPoolUsage).toHaveBeenCalledTimes(1);
        expect(api.getCodexPoolUsage).toHaveBeenCalledWith(true);
      });
    });

    it('keeps account quota bound to its account and CODEX_HOME', async () => {
      enableCodexPool({
        enabled: true,
        total: 2,
        available: 2,
        cooldown: 0,
        disabled: 0,
        preferred: null,
        accounts: [
          {
            ...codexAccount,
            id: 'codex-1',
            email: 'one@example.com',
            codex_home: '/home/ubuntu/.codex',
            quota: {
              primary_used_percent: 100,
              primary_window_minutes: 10080,
              primary_resets_at: null,
              secondary_used_percent: null,
              secondary_window_minutes: null,
              secondary_resets_at: null,
              is_rate_limited: true,
              has_credits: false,
            },
            quota_error: null,
          },
          {
            ...codexAccount,
            id: 'codex-2',
            email: 'two@example.com',
            codex_home: '/home/ubuntu/.codex-codex-2',
            quota: {
              primary_used_percent: 14,
              primary_window_minutes: 10080,
              primary_resets_at: null,
              secondary_used_percent: null,
              secondary_window_minutes: null,
              secondary_resets_at: null,
              is_rate_limited: false,
              has_credits: false,
            },
            quota_error: null,
          },
        ],
      });
      const user = userEvent.setup();

      await openCodexTab(user);

      const accountOne = screen.getByText('codex-1').closest('.rounded-lg');
      const accountTwo = screen.getByText('codex-2').closest('.rounded-lg');
      expect(accountOne).toBeTruthy();
      expect(accountTwo).toBeTruthy();
      expect(within(accountOne as HTMLElement).getByText('CODEX_HOME: /home/ubuntu/.codex')).toBeInTheDocument();
      expect(within(accountOne as HTMLElement).getByText('已用 100.0%')).toBeInTheDocument();
      expect(within(accountTwo as HTMLElement).getByText('CODEX_HOME: /home/ubuntu/.codex-codex-2')).toBeInTheDocument();
      expect(within(accountTwo as HTMLElement).getByText('已用 14.0%')).toBeInTheDocument();
    });

    it('does not let an older Codex quota response overwrite a newer refresh', async () => {
      const staleRequest = deferred<Record<string, unknown>>();
      const latestUsage = {
        enabled: true,
        total: 1,
        available: 1,
        cooldown: 0,
        disabled: 0,
        preferred: null,
        accounts: [{ ...codexAccount, id: 'codex-new', email: 'latest@example.com' }],
      };
      vi.mocked(api.getCodexPoolStatus).mockResolvedValue({ enabled: true } as never);
      vi.mocked(api.getCodexPoolUsage)
        .mockImplementationOnce(() => staleRequest.promise as never)
        .mockResolvedValueOnce(latestUsage as never);
      const user = userEvent.setup();

      await renderAndWaitForPro();
      await openDrawer(user);
      await user.click(screen.getByRole('button', { name: 'Codex' }));
      await waitFor(() => expect(api.getCodexPoolUsage).toHaveBeenCalledTimes(1));

      await user.click(screen.getByRole('button', { name: 'Claude' }));
      await user.click(screen.getByRole('button', { name: 'Codex' }));
      expect(await screen.findByText('codex-new')).toBeInTheDocument();

      await act(async () => {
        staleRequest.resolve({
          ...latestUsage,
          accounts: [{ ...codexAccount, id: 'codex-stale', email: 'stale@example.com' }],
        });
        await staleRequest.promise;
      });
      expect(api.getCodexPoolUsage).toHaveBeenCalledTimes(2);
      expect(screen.getByText('codex-new')).toBeInTheDocument();
      expect(screen.queryByText('codex-stale')).not.toBeInTheDocument();
    });

    it('does not show a previous quota while live refresh is pending or after it fails', async () => {
      const refreshRequest = deferred<Record<string, unknown>>();
      enableCodexPool({
        enabled: true,
        total: 1,
        available: 1,
        cooldown: 0,
        disabled: 0,
        preferred: null,
        accounts: [{
          ...codexAccount,
          id: 'codex-previous',
          quota: {
            primary_used_percent: 3,
            primary_window_minutes: 10080,
            primary_resets_at: null,
            secondary_used_percent: null,
            secondary_window_minutes: null,
            secondary_resets_at: null,
            is_rate_limited: false,
            has_credits: false,
          },
          quota_error: null,
        }],
      });
      vi.mocked(api.getCodexPoolUsage)
        .mockResolvedValueOnce({
          enabled: true,
          total: 1,
          available: 1,
          cooldown: 0,
          disabled: 0,
          preferred: null,
          accounts: [{
            ...codexAccount,
            id: 'codex-previous',
            quota: {
              primary_used_percent: 3,
              primary_window_minutes: 10080,
              primary_resets_at: null,
              secondary_used_percent: null,
              secondary_window_minutes: null,
              secondary_resets_at: null,
              is_rate_limited: false,
              has_credits: false,
            },
            quota_error: null,
          }],
        } as never)
        .mockImplementationOnce(() => refreshRequest.promise as never);
      const user = userEvent.setup();

      await openCodexTab(user);
      expect(await screen.findByText('已用 3.0%')).toBeInTheDocument();

      await user.click(screen.getByTitle('刷新'));
      expect(screen.queryByText('已用 3.0%')).not.toBeInTheDocument();
      expect(screen.getByText('加载中…')).toBeInTheDocument();

      refreshRequest.reject(new Error('live quota unavailable'));
      expect(await screen.findByText('live quota unavailable')).toBeInTheDocument();
      expect(screen.queryByText('已用 3.0%')).not.toBeInTheDocument();
      expect(screen.queryByText('codex-previous')).not.toBeInTheDocument();
    });

    it('shows each account CODEX_HOME and can set it preferred', async () => {
      enableCodexPool({
        enabled: true,
        total: 1,
        available: 1,
        cooldown: 0,
        disabled: 0,
        preferred: null,
        accounts: [codexAccount],
      });
      vi.mocked(api.setCodexPoolPreferred).mockResolvedValue({ ok: true, preferred: 'codex-2' });
      const user = userEvent.setup();

      await openCodexTab(user);

      expect(screen.getByText(`CODEX_HOME: ${codexAccount.codex_home}`)).toBeInTheDocument();
      await user.click(screen.getByRole('button', { name: '切换到此账号' }));
      await waitFor(() => {
        expect(api.setCodexPoolPreferred).toHaveBeenCalledWith('codex-2');
      });
    });

    it('marks the preferred account and can restore automatic selection', async () => {
      enableCodexPool({
        enabled: true,
        total: 1,
        available: 1,
        cooldown: 0,
        disabled: 0,
        preferred: 'codex-2',
        accounts: [codexAccount],
      });
      vi.mocked(api.setCodexPoolPreferred).mockResolvedValue({ ok: true, preferred: null });
      const user = userEvent.setup();

      await openCodexTab(user);

      expect(screen.getByText('优先账号')).toBeInTheDocument();
      const restoreButton = screen.getByRole('button', { name: '恢复自动' });
      expect(restoreButton).toHaveAttribute(
        'title',
        '取消全局优先；新会话优先兼容且可用的 API，已有对话继续使用绑定账号',
      );
      await user.click(restoreButton);
      await waitFor(() => {
        expect(api.setCodexPoolPreferred).toHaveBeenCalledWith(null);
      });
    });

    it('marks only the most recently used Codex account', async () => {
      enableCodexPool({
        enabled: true,
        total: 2,
        available: 2,
        cooldown: 0,
        disabled: 0,
        preferred: null,
        last_selected: 'codex-2',
        accounts: [
          { ...codexAccount, id: 'codex-1', codex_home: '/home/ubuntu/.codex' },
          codexAccount,
        ],
      });
      const user = userEvent.setup();

      await openCodexTab(user);

      const firstAccount = screen.getByText('codex-1').closest('.rounded-lg') as HTMLElement;
      const lastAccount = screen.getByText('codex-2').closest('.rounded-lg') as HTMLElement;
      expect(within(firstAccount).queryByText('最近使用')).not.toBeInTheDocument();
      expect(within(lastAccount).getByText('最近使用')).toHaveAttribute(
        'title',
        '最近一次由路由器分配的账号',
      );
    });

    it('retries a transient relogin status failure and still accepts the OTP', async () => {
      enableCodexPool({
        enabled: true,
        total: 1,
        available: 1,
        cooldown: 0,
        disabled: 0,
        preferred: null,
        accounts: [codexAccount],
      });
      vi.mocked(api.codexPoolRelogin).mockResolvedValue({
        ok: true,
        status: 'running',
        attempt_id: 'relogin-attempt',
      });
      vi.mocked(api.codexPoolReloginStatus)
        .mockRejectedValueOnce(new Error('temporary network error'))
        .mockResolvedValue({
          status: 'awaiting_otp',
          attempt_id: 'relogin-attempt',
          challenge_id: 'relogin-challenge',
          expires_at: Math.floor(Date.now() / 1000) + 600,
        });
      vi.mocked(api.codexPoolSubmitOtp).mockResolvedValue({
        ok: true,
        status: 'verifying_otp',
      });
      const user = userEvent.setup();

      await openCodexTab(user);
      await user.click(screen.getByRole('button', { name: '重新登录' }));

      await waitFor(() => {
        expect(screen.getByText(/状态查询暂时失败，正在重试/)).toBeInTheDocument();
      }, { timeout: 2500 });
      const otpInput = await screen.findByLabelText(
        'OpenAI 邮箱验证码',
        {},
        { timeout: 5000 },
      );
      expect(api.codexPoolReloginStatus).toHaveBeenCalledTimes(2);

      await user.type(otpInput, '123456');
      await user.click(screen.getByRole('button', { name: '继续登录' }));
      await waitFor(() => {
        expect(api.codexPoolSubmitOtp).toHaveBeenCalledWith(
          'relogin-attempt',
          'relogin-challenge',
          '123456',
        );
      });
    });

    it('continues polling relogin through finalizing until committed success', async () => {
      enableCodexPool({
        enabled: true,
        total: 1,
        available: 1,
        cooldown: 0,
        disabled: 0,
        preferred: null,
        accounts: [codexAccount],
      });
      vi.mocked(api.codexPoolRelogin).mockResolvedValue({
        ok: true,
        status: 'running',
        attempt_id: 'relogin-finalizing',
      });
      vi.mocked(api.codexPoolReloginStatus)
        .mockResolvedValueOnce({
          status: 'finalizing',
          attempt_id: 'relogin-finalizing',
        })
        .mockResolvedValue({
          status: 'success',
          attempt_id: 'relogin-finalizing',
        });
      const user = userEvent.setup();

      await openCodexTab(user);
      await user.click(screen.getByRole('button', { name: '重新登录' }));

      expect(await screen.findByText(
        '登录已完成，正在安全提交登录结果…',
        {},
        { timeout: 3000 },
      )).toBeInTheDocument();
      expect(screen.getByRole('button', { name: '登录中…' })).toBeDisabled();

      expect(await screen.findByText('登录成功', {}, { timeout: 5000 })).toBeInTheDocument();
      expect(api.codexPoolReloginStatus).toHaveBeenCalledTimes(2);
    });
  });
});
