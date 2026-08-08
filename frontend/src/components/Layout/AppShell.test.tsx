import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { render, screen, waitFor, cleanup, act } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { AppShell } from './AppShell';
import { setTheme } from '../../config/theme';

vi.mock('../../api/client', () => ({
  api: {
    listWorkers: vi.fn().mockResolvedValue([]),
    countPlans: vi.fn().mockResolvedValue({ total: 0 }),
    getRuntimeSettings: vi.fn().mockResolvedValue({
      use_pty_mode: false,
      pty_available: false,
      codex_app_server_enabled: true,
      codex_main_mcp_enabled: true,
      auto_sort_on_access: true,
      context_compact_threshold: 0.8,
    }),
    getFeishuStatus: vi.fn().mockResolvedValue({ bound: false }),
    getPoolStatus: vi.fn().mockResolvedValue({ enabled: false }),
    getCodexPoolStatus: vi.fn().mockRejectedValue(new Error('disabled')),
    getCloudRouterAccounts: vi.fn().mockResolvedValue([]),
    startUpdate: vi.fn(),
    health: vi.fn(),
  },
  clearToken: vi.fn(),
  getToken: vi.fn().mockReturnValue('test-token'),
}));

vi.mock('../../hooks/useWebSocket', () => ({
  useWebSocket: vi.fn(),
}));

vi.mock('../../config/server', () => ({
  isCapacitor: vi.fn().mockReturnValue(false),
}));

import { api } from '../../api/client';

function renderShell(page = 'tasks', wide = false) {
  return render(
    <AppShell currentPage={page} onNavigate={() => {}} wide={wide}>
      <div data-testid="page-content">Page content</div>
    </AppShell>,
  );
}

describe('AppShell layout and z-index architecture', () => {
  beforeEach(() => {
    Object.defineProperty(window, 'matchMedia', {
      writable: true,
      value: vi.fn().mockImplementation((query: string) => ({
        matches: query === '(min-width: 1024px)',
        media: query,
        onchange: null,
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        addListener: vi.fn(),
        removeListener: vi.fn(),
        dispatchEvent: vi.fn(),
      })),
    });
    localStorage.setItem('cc_user', JSON.stringify({ name: 'Test', role: 'admin' }));
  });

  afterEach(() => {
    cleanup();
    vi.useRealTimers();
    vi.clearAllMocks();
    localStorage.clear();
  });

  it('polls the ACL-filtered Worker list for member navigation', async () => {
    vi.useFakeTimers();
    localStorage.setItem('cc_user', JSON.stringify({
      id: 9,
      name: 'Member',
      role: 'member',
    }));
    vi.mocked(api.listWorkers).mockResolvedValue([]);

    renderShell();
    await act(async () => {
      await Promise.resolve();
    });
    expect(api.listWorkers).toHaveBeenCalledTimes(1);

    await act(async () => {
      vi.advanceTimersByTime(30000);
      await Promise.resolve();
    });
    expect(api.listWorkers).toHaveBeenCalledTimes(2);
  });

  it('does not expose the process-wide update control to members', () => {
    localStorage.setItem('cc_user', JSON.stringify({
      id: 9,
      name: 'Member',
      role: 'member',
    }));

    renderShell();

    expect(screen.queryByTitle('更新并重启')).not.toBeInTheDocument();
  });

  it('does not advertise admin-only host files or global secrets to members', () => {
    localStorage.setItem('cc_user', JSON.stringify({
      id: 9,
      name: 'Member',
      role: 'member',
    }));

    renderShell();

    expect(screen.queryByRole('button', { name: 'Files' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Secrets' })).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Plans' })).toBeInTheDocument();
  });

  it('keeps the process-wide update control available to administrators', () => {
    renderShell();

    expect(screen.getByTitle('更新并重启')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Plans' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Files' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Secrets' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Browser Review' })).not.toBeInTheDocument();
  });

  it('marks Plans navigation when a Plan requires user action', async () => {
    vi.mocked(api.countPlans).mockResolvedValue({ total: 2 });
    renderShell();

    const marker = await screen.findByLabelText('Plans requiring action');
    expect(marker).toBeInTheDocument();
    expect(marker.parentElement).toHaveAttribute('data-nav-icon');
    expect(marker.className).toContain('absolute');
  });

  describe('header stacking context', () => {
    it('header is a solid surface with no backdrop-blur utility (7a1bc7c: 自定义主题的透明度由变量 alpha 承担)', async () => {
      renderShell();
      const header = document.querySelector('header');
      expect(header).toBeTruthy();
      expect(header!.className).toContain('bg-gray-900');
      // backdrop-filter 会为 fixed 后代创建 containing block——若重新引入
      // blur（如 per-theme CSS 覆盖），必须保证 header 内没有 fixed 元素（见下一个测试）
      expect(header!.className).not.toContain('backdrop-blur');
    });

    it('header uses sticky positioning with z-30', async () => {
      renderShell();
      const header = document.querySelector('header');
      expect(header!.className).toContain('sticky');
      expect(header!.className).toContain('z-30');
    });

    it('no fixed inset-0 elements are direct DOM children inside the header', async () => {
      renderShell();
      const header = document.querySelector('header');
      const fixedDescendants = header!.querySelectorAll('[class*="fixed"][class*="inset-0"]');
      expect(fixedDescendants.length).toBe(0);
    });
  });

  describe('mobile drawer', () => {
    it('mobile drawer is rendered OUTSIDE the header (sibling, not descendant)', async () => {
      const user = userEvent.setup();
      renderShell();

      const menuButton = screen.getByLabelText('打开导航');
      await user.click(menuButton);

      await waitFor(() => {
        const navButtons = screen.getAllByRole('button');
        const tasksButton = navButtons.find(b => b.textContent === 'Tasks');
        expect(tasksButton).toBeTruthy();
      });

      const header = document.querySelector('header');
      const mobileDrawer = document.querySelector('[class*="lg:hidden"][class*="fixed"][class*="inset-0"]');
      expect(mobileDrawer).toBeTruthy();
      expect(header!.contains(mobileDrawer!)).toBe(false);
    });

    it('mobile drawer uses z-50, higher than header z-30', async () => {
      const user = userEvent.setup();
      renderShell();

      await user.click(screen.getByLabelText('打开导航'));

      await waitFor(() => {
        const drawer = document.querySelector('[class*="lg:hidden"][class*="fixed"][class*="inset-0"]');
        expect(drawer).toBeTruthy();
        expect(drawer!.className).toContain('z-50');
      });
    });
  });

  describe('desktop sidebar', () => {
    it('collapses, persists the preference, and expands again from the header', async () => {
      const user = userEvent.setup();
      renderShell();

      expect(document.querySelector('aside[data-shell-sidebar]')).toBeTruthy();
      await user.click(screen.getByLabelText('收起导航'));

      expect(document.querySelector('aside[data-shell-sidebar]')).toBeNull();
      expect(localStorage.getItem('ccm-nav-collapsed')).toBe('true');

      await user.click(screen.getByLabelText('展开导航'));
      expect(document.querySelector('aside[data-shell-sidebar]')).toBeTruthy();
      expect(localStorage.getItem('ccm-nav-collapsed')).toBe('false');
    });

    it('sidebar is rendered OUTSIDE the header', () => {
      renderShell();
      const header = document.querySelector('header');
      const sidebar = document.querySelector('aside[class*="lg:flex"]');
      expect(sidebar).toBeTruthy();
      expect(header!.contains(sidebar!)).toBe(false);
    });

    it('sidebar uses z-40, between header z-30 and mobile drawer z-50', () => {
      renderShell();
      const sidebar = document.querySelector('aside[class*="lg:flex"]');
      expect(sidebar!.className).toContain('z-40');
    });

    it('switching theme swaps nav icon sets live (feishu → IconPark, apple → Ionicons, 其余 → Lucide)', async () => {
      renderShell();
      // 默认 dark：无 iconSet 包装（Lucide 直渲）
      expect(document.querySelector('[data-nav-item] [data-icon-set]')).toBeNull();

      await act(async () => { setTheme('feishu'); });
      for (const el of document.querySelectorAll('[data-nav-item]')) {
        expect(
          el.querySelector("[data-icon-set='feishu'] svg"),
          `feishu 集缺 "${el.textContent}" 的图标（回退成了 Lucide）`,
        ).toBeTruthy();
      }

      await act(async () => { setTheme('apple'); });
      for (const el of document.querySelectorAll('[data-nav-item]')) {
        expect(
          el.querySelector("[data-icon-set='sf'] svg"),
          `sf 集缺 "${el.textContent}" 的图标（回退成了 Lucide）`,
        ).toBeTruthy();
      }

      await act(async () => { setTheme('dark'); });
      expect(document.querySelector('[data-nav-item] [data-icon-set]')).toBeNull();
    });

    it('exposes theme-agnostic data hooks for per-theme structural CSS (feishu rail / apple squircles)', () => {
      // index.css 的结构级复刻层依赖这些钩子；改名/删除会让飞书与苹果主题
      // 的侧栏结构静默失效（回归为普通列表）
      renderShell();
      expect(document.querySelector('aside[data-shell-sidebar]')).toBeTruthy();
      expect(document.querySelector('[data-shell-main]')).toBeTruthy();
      expect(document.querySelector('[data-shell-brand-row]')).toBeTruthy();
      expect(document.querySelector('[data-shell-brand-text]')).toBeTruthy();
      const items = document.querySelectorAll('[data-nav-item]');
      expect(items.length).toBeGreaterThan(3);
      expect(document.querySelectorAll("[data-nav-item][data-active='true']").length).toBe(1);
    });
  });

  describe('page content area', () => {
    it('fills the available column in wide split mode so the task sidebar stays flush with navigation', () => {
      renderShell('tasks', true);
      const main = document.querySelector('main');
      expect(main!.className).toContain('max-w-none');
      expect(main!.className).not.toContain('mx-auto');
      expect(main!.className).not.toContain('max-w-[1400px]');
    });

    it('keeps regular pages centered and width-limited', () => {
      renderShell();
      const main = document.querySelector('main');
      expect(main!.className).toContain('mx-auto');
      expect(main!.className).toContain('max-w-6xl');
    });

    it('main content is rendered OUTSIDE the header', () => {
      renderShell();
      const header = document.querySelector('header');
      const content = screen.getByTestId('page-content');
      expect(header!.contains(content)).toBe(false);
    });

    it('main content is a sibling of header within the same column', () => {
      renderShell();
      const header = document.querySelector('header');
      const main = document.querySelector('main');
      expect(header!.parentElement).toBe(main!.parentElement);
    });
  });

  describe('z-index ordering invariants', () => {
    it('z-index hierarchy: header(30) < sidebar(40) < drawer/modals(50) < portaled overlays(70)', () => {
      renderShell();

      const header = document.querySelector('header');
      const sidebar = document.querySelector('aside[class*="lg:flex"]');

      const headerZ = parseInt(header!.className.match(/z-(\d+)/)?.[1] || '0', 10);
      const sidebarZ = parseInt(sidebar!.className.match(/z-(\d+)/)?.[1] || '0', 10);

      expect(headerZ).toBe(30);
      expect(sidebarZ).toBe(40);
      expect(sidebarZ).toBeGreaterThan(headerZ);
      // z-50 for modals/drawer > sidebar z-40
      expect(50).toBeGreaterThan(sidebarZ);
      // z-[70] for portaled overlays > regular modals z-50
      expect(70).toBeGreaterThan(50);
    });
  });
});
