import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { render, screen, waitFor, cleanup } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { PrefsMenu } from './PrefsMenu';

vi.mock('../../api/client', () => ({
  api: {
    getRuntimeSettings: vi.fn().mockResolvedValue({
      use_pty_mode: false,
      pty_available: true,
      codex_app_server_enabled: true,
      codex_main_mcp_enabled: true,
      auto_sort_on_access: true,
      context_compact_threshold: 0.8,
    }),
    updateRuntimeSettings: vi.fn(),
    getFeishuStatus: vi.fn().mockResolvedValue({ bound: false }),
    unbindFeishu: vi.fn(),
    getFeishuAuthUrl: vi.fn(),
  },
  clearToken: vi.fn(),
}));

describe('PrefsMenu', () => {
  beforeEach(() => {
    localStorage.setItem('cc_user', JSON.stringify({ id: 1, name: 'Test', role: 'admin' }));
  });

  afterEach(() => {
    cleanup();
    vi.clearAllMocks();
    localStorage.clear();
  });

  it('renders the settings trigger button', () => {
    render(<PrefsMenu isAdmin={true} />);
    expect(screen.getByTitle('偏好设置（时区 / 主题）')).toBeInTheDocument();
  });

  describe('dropdown positioning (not affected by backdrop-blur)', () => {
    it('dropdown uses absolute positioning, not fixed', async () => {
      const user = userEvent.setup();
      render(<PrefsMenu isAdmin={true} />);

      await user.click(screen.getByTitle('偏好设置（时区 / 主题）'));

      await waitFor(() => {
        expect(screen.getByText('时区')).toBeInTheDocument();
      });

      const dropdown = screen.getByText('时区').closest('[class*="absolute"]');
      expect(dropdown).toBeTruthy();
      expect(dropdown!.className).toContain('absolute');
      expect(dropdown!.className).not.toContain('fixed');
    });

    it('dropdown positions below trigger with top-full', async () => {
      const user = userEvent.setup();
      render(<PrefsMenu isAdmin={true} />);

      await user.click(screen.getByTitle('偏好设置（时区 / 主题）'));

      await waitFor(() => {
        const dropdown = screen.getByText('时区').closest('[class*="absolute"]');
        expect(dropdown!.className).toContain('top-full');
      });
    });

    it('dropdown is a DOM child of the relative container (not portaled)', async () => {
      const user = userEvent.setup();
      const { container } = render(<PrefsMenu isAdmin={true} />);

      await user.click(screen.getByTitle('偏好设置（时区 / 主题）'));

      await waitFor(() => {
        const dropdown = screen.getByText('时区').closest('[class*="absolute"]');
        expect(container.contains(dropdown)).toBe(true);
      });
    });

    it('wrapper div has position:relative for absolute dropdown anchoring', () => {
      render(<PrefsMenu isAdmin={true} />);
      const wrapper = screen.getByTitle('偏好设置（时区 / 主题）').parentElement;
      expect(wrapper!.className).toContain('relative');
    });
  });

  describe('dropdown open/close behavior', () => {
    it('opens dropdown on settings button click', async () => {
      const user = userEvent.setup();
      render(<PrefsMenu isAdmin={true} />);

      expect(screen.queryByText('时区')).not.toBeInTheDocument();

      await user.click(screen.getByTitle('偏好设置（时区 / 主题）'));

      await waitFor(() => {
        expect(screen.getByText('时区')).toBeInTheDocument();
      });
    });

    it('closes dropdown on outside click', async () => {
      const user = userEvent.setup();
      render(<PrefsMenu isAdmin={true} />);

      await user.click(screen.getByTitle('偏好设置（时区 / 主题）'));

      await waitFor(() => {
        expect(screen.getByText('时区')).toBeInTheDocument();
      });

      await user.click(document.body);

      await waitFor(() => {
        expect(screen.queryByText('时区')).not.toBeInTheDocument();
      });
    });
  });

  describe('dropdown content', () => {
    it('shows timezone selector', async () => {
      const user = userEvent.setup();
      render(<PrefsMenu isAdmin={true} />);

      await user.click(screen.getByTitle('偏好设置（时区 / 主题）'));

      await waitFor(() => {
        expect(screen.getByText('时区')).toBeInTheDocument();
      });
    });

    it('shows theme selector', async () => {
      const user = userEvent.setup();
      render(<PrefsMenu isAdmin={true} />);

      await user.click(screen.getByTitle('偏好设置（时区 / 主题）'));

      await waitFor(() => {
        expect(screen.getByText('主题')).toBeInTheDocument();
      });
    });

    it('shows PTY toggle for admin users', async () => {
      const user = userEvent.setup();
      render(<PrefsMenu isAdmin={true} />);

      await user.click(screen.getByTitle('偏好设置（时区 / 主题）'));

      await waitFor(() => {
        expect(screen.getByText('PTY 模式')).toBeInTheDocument();
      });
    });

    it('shows the read-only Codex main MCP runtime capability for admins', async () => {
      const user = userEvent.setup();
      render(<PrefsMenu isAdmin={true} />);

      await user.click(screen.getByRole('button'));

      const status = await screen.findByTestId('codex-main-mcp-status');
      expect(status).toHaveTextContent('Codex 主任务 MCP');
      expect(status).toHaveTextContent('已启用');
    });

    it('shows logout button', async () => {
      const user = userEvent.setup();
      render(<PrefsMenu isAdmin={true} />);

      await user.click(screen.getByTitle('偏好设置（时区 / 主题）'));

      await waitFor(() => {
        expect(screen.getByText('退出登录')).toBeInTheDocument();
      });
    });

    it('shows password change button for logged-in users', async () => {
      const user = userEvent.setup();
      render(<PrefsMenu isAdmin={true} />);

      await user.click(screen.getByTitle('偏好设置（时区 / 主题）'));

      await waitFor(() => {
        expect(screen.getByText('修改密码')).toBeInTheDocument();
      });
    });
  });
});
