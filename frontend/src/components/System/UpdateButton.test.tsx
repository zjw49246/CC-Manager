import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { render, screen, waitFor, cleanup, act } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { UpdateButton } from './UpdateButton';

vi.mock('../../api/client', () => ({
  api: {
    startUpdate: vi.fn(),
    getUpdateStatus: vi.fn(),
    reconcileUpdateState: vi.fn(),
    repairUpdate: vi.fn(),
    restartService: vi.fn(),
    rollbackUpdate: vi.fn(),
    health: vi.fn(),
  },
}));

vi.mock('../../hooks/useWebSocket', () => ({
  useWebSocket: vi.fn(),
}));

import { api } from '../../api/client';

const mockDryRun = {
  has_updates: true,
  needs_restart: false,
  commits_behind: 3,
  current_commit: 'abc1234',
  latest_commit: 'def5678',
  commit_messages: ['fix: bug', 'feat: new feature', 'chore: cleanup'],
  has_new_migrations: false,
  has_frontend_changes: true,
  has_package_changes: false,
  active_task_count: 0,
  active_tasks: [],
  update_blocked: false,
};

function findModalOverlay(): HTMLElement | null {
  return document.body.querySelector('[class*="fixed"][class*="z-[70]"]');
}

async function openAndCheck(user: ReturnType<typeof userEvent.setup>) {
  await user.click(screen.getByTitle('更新并重启'));
  await user.click(screen.getByRole('button', { name: '检查所选渠道' }));
}

describe('UpdateButton', () => {
  beforeEach(() => {
    sessionStorage.clear();
    vi.mocked(api.startUpdate).mockResolvedValue(mockDryRun as never);
    vi.mocked(api.getUpdateStatus).mockResolvedValue({ status: 'idle' } as never);
    vi.mocked(api.reconcileUpdateState).mockResolvedValue({
      update_blocked: false,
      active_task_count: 0,
      active_tasks: [],
      reconciled: true,
    } as never);
    vi.mocked(api.repairUpdate).mockResolvedValue({ update_id: 'repair-1' } as never);
    vi.mocked(api.restartService).mockResolvedValue({ status: 'restarting' } as never);
    vi.mocked(api.rollbackUpdate).mockResolvedValue({ status: 'rolling_back' } as never);
    localStorage.clear();
    Object.defineProperty(document, 'visibilityState', {
      value: 'visible',
      writable: true,
      configurable: true,
    });
  });

  afterEach(() => {
    cleanup();
    vi.clearAllMocks();
    vi.useRealTimers();
    localStorage.clear();
    sessionStorage.clear();
  });

  it('renders the update trigger button', () => {
    render(<UpdateButton />);
    expect(screen.getByTitle('更新并重启')).toBeInTheDocument();
  });

  it('shows both update channels before checking and does not auto-fallback', async () => {
    const user = userEvent.setup();
    vi.mocked(api.startUpdate).mockRejectedValue(new Error('仓库没有可用的正式版本 tag'));
    render(<UpdateButton />);

    await user.click(screen.getByTitle('更新并重启'));

    expect(screen.getByRole('radio', { name: /Stable 正式版/ })).toBeChecked();
    expect(screen.getByRole('radio', { name: /Main 测试版/ })).not.toBeChecked();
    expect(api.startUpdate).not.toHaveBeenCalled();

    await user.click(screen.getByRole('button', { name: '检查所选渠道' }));

    expect(await screen.findByText('仓库没有可用的正式版本 tag')).toBeInTheDocument();
    expect(api.startUpdate).toHaveBeenCalledWith({
      dry_run: true,
      force: true,
      channel: 'stable',
      branch: undefined,
    });
    expect(api.startUpdate).toHaveBeenCalledTimes(1);
  });

  it('checks Main only after the user explicitly selects it', async () => {
    const user = userEvent.setup();
    render(<UpdateButton />);

    await user.click(screen.getByTitle('更新并重启'));
    await user.click(screen.getByRole('radio', { name: /Main 测试版/ }));

    expect(screen.getByText('切换到测试版前请注意')).toBeInTheDocument();
    expect(screen.getByText(/如果 Main 包含正式版没有的数据库迁移/)).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: '检查所选渠道' }));

    await waitFor(() => {
      expect(api.startUpdate).toHaveBeenCalledWith({
        dry_run: true,
        force: true,
        channel: 'main',
        branch: undefined,
      });
    });
  });

  describe('modal portal rendering', () => {
    it('renders modal via portal on document.body when opened', async () => {
      const user = userEvent.setup();
      render(<UpdateButton />);

      await openAndCheck(user);

      await waitFor(() => {
        expect(findModalOverlay()).toBeTruthy();
      });

      const modal = findModalOverlay()!;
      expect(modal.parentElement).toBe(document.body);
    });

    it('modal is NOT inside the component render container', async () => {
      const user = userEvent.setup();
      const { container } = render(<UpdateButton />);

      await openAndCheck(user);

      await waitFor(() => {
        expect(findModalOverlay()).toBeTruthy();
      });

      expect(container.contains(findModalOverlay())).toBe(false);
    });

    it('modal uses z-[70], higher than z-50 page overlays', async () => {
      const user = userEvent.setup();
      render(<UpdateButton />);

      await openAndCheck(user);

      await waitFor(() => {
        expect(findModalOverlay()).toBeTruthy();
      });

      const modal = findModalOverlay()!;
      expect(modal.className).toContain('z-[70]');
      expect(modal.className).not.toMatch(/\bz-50\b/);
    });

    it('modal has fixed positioning with full viewport coverage and centering', async () => {
      const user = userEvent.setup();
      render(<UpdateButton />);

      await openAndCheck(user);

      await waitFor(() => {
        expect(findModalOverlay()).toBeTruthy();
      });

      const modal = findModalOverlay()!;
      expect(modal.className).toContain('fixed');
      expect(modal.className).toContain('inset-0');
      expect(modal.className).toContain('items-center');
      expect(modal.className).toContain('justify-center');
    });

    it('modal escapes a header ancestor with backdrop-blur (the root cause)', async () => {
      const user = userEvent.setup();

      const headerLike = document.createElement('header');
      headerLike.className = 'sticky top-0 z-30 bg-gray-900/85 backdrop-blur-md';
      document.body.appendChild(headerLike);

      const innerDiv = document.createElement('div');
      headerLike.appendChild(innerDiv);

      render(<UpdateButton />, { container: innerDiv });

      await openAndCheck(user);

      await waitFor(() => {
        expect(findModalOverlay()).toBeTruthy();
      });

      const modal = findModalOverlay()!;
      expect(modal.parentElement).toBe(document.body);
      expect(headerLike.contains(modal)).toBe(false);

      headerLike.remove();
    });

    it('modal z-index (70) is numerically greater than page overlay z-index (50)', async () => {
      const user = userEvent.setup();
      render(<UpdateButton />);

      await openAndCheck(user);

      await waitFor(() => {
        expect(findModalOverlay()).toBeTruthy();
      });

      const modal = findModalOverlay()!;
      const match = modal.className.match(/z-\[(\d+)\]/);
      expect(match).toBeTruthy();
      expect(parseInt(match![1], 10)).toBeGreaterThan(50);
    });
  });

  describe('modal open/close behavior', () => {
    it('opens modal on update button click', async () => {
      const user = userEvent.setup();
      render(<UpdateButton />);

      expect(findModalOverlay()).toBeNull();

      await openAndCheck(user);

      await waitFor(() => {
        expect(findModalOverlay()).toBeTruthy();
      });
    });

    it('closes modal on cancel button click', async () => {
      const user = userEvent.setup();
      render(<UpdateButton />);

      await openAndCheck(user);

      await waitFor(() => {
        expect(findModalOverlay()).toBeTruthy();
      });

      await user.click(screen.getByText('取消'));

      await waitFor(() => {
        expect(findModalOverlay()).toBeNull();
      });
    });

    it('removes portal element from body when modal closes', async () => {
      const user = userEvent.setup();
      render(<UpdateButton />);

      await openAndCheck(user);

      await waitFor(() => {
        const portals = document.body.querySelectorAll('[class*="z-[70]"]');
        expect(portals.length).toBe(1);
      });

      await user.click(screen.getByText('取消'));

      await waitFor(() => {
        const portals = document.body.querySelectorAll('[class*="z-[70]"]');
        expect(portals.length).toBe(0);
      });
    });

    it('shows "已是最新版本" when no updates available', async () => {
      vi.mocked(api.startUpdate).mockResolvedValue({
        has_updates: false,
        needs_restart: false,
      } as never);

      const user = userEvent.setup();
      render(<UpdateButton />);

      await openAndCheck(user);

      await waitFor(() => {
        expect(screen.getByText('已是最新版本，无需更新。')).toBeInTheDocument();
      });
    });

    it('forces a fresh dry-run when the user checks manually', async () => {
      const user = userEvent.setup();
      render(<UpdateButton />);

      await openAndCheck(user);

      await waitFor(() => {
        expect(api.startUpdate).toHaveBeenCalledWith({
          dry_run: true,
          force: true,
          channel: 'stable',
          branch: undefined,
        });
      });
    });

    it('allows a safe redeploy of locally pulled code when the remote check failed', async () => {
      vi.mocked(api.startUpdate).mockResolvedValue({
        has_updates: false,
        needs_restart: true,
        manual_update_detected: true,
        current_commit: 'def5678',
        running_commit: 'abc1234',
        error: 'network unavailable',
      } as never);

      const user = userEvent.setup();
      render(<UpdateButton />);
      await openAndCheck(user);

      await waitFor(() => {
        expect(screen.getByText(/磁盘代码尚未完整部署/)).toBeInTheDocument();
      });
      expect(screen.getByText(/远端更新检查失败/)).toBeInTheDocument();
      expect(screen.getByRole('button', { name: '修复并重新部署' })).toBeEnabled();
      expect(screen.queryByText('检查更新失败')).not.toBeInTheDocument();
    });
  });

  describe('modal content rendering', () => {
    it('repeats the Stable rollback warning when Main contains migrations', async () => {
      vi.mocked(api.startUpdate).mockResolvedValue({
        ...mockDryRun,
        channel: 'main',
        has_new_migrations: true,
        migration_count: 1,
      } as never);

      const user = userEvent.setup();
      render(<UpdateButton />);
      await user.click(screen.getByTitle('更新并重启'));
      await user.click(screen.getByRole('radio', { name: /Main 测试版/ }));
      await user.click(screen.getByRole('button', { name: '检查所选渠道' }));

      expect(await screen.findByText(/更新后可能无法一键切回 Stable/)).toBeInTheDocument();
    });

    it('labels a Main-to-Stable change as a channel switch', async () => {
      vi.mocked(api.startUpdate).mockResolvedValue({
        ...mockDryRun,
        channel: 'stable',
        latest_version: 'v1.0.0',
        update_kind: 'stable_switch',
        is_stable_downgrade: true,
        commits_behind: 0,
      } as never);

      const user = userEvent.setup();
      render(<UpdateButton />);
      await openAndCheck(user);

      expect(await screen.findByText(/将从测试版切换回正式版/)).toBeInTheDocument();
      expect(screen.getByRole('button', { name: '切换到正式版' })).toBeEnabled();
    });

    it('displays commit count when updates available', async () => {
      const user = userEvent.setup();
      render(<UpdateButton />);

      await openAndCheck(user);

      await waitFor(() => {
        expect(screen.getByText('3')).toBeInTheDocument();
      });
    });

    it('displays commit hashes', async () => {
      const user = userEvent.setup();
      render(<UpdateButton />);

      await openAndCheck(user);

      await waitFor(() => {
        expect(screen.getByText(/abc1234/)).toBeInTheDocument();
        expect(screen.getByText(/def5678/)).toBeInTheDocument();
      });
    });

    it('displays commit messages', async () => {
      const user = userEvent.setup();
      render(<UpdateButton />);

      await openAndCheck(user);

      await waitFor(() => {
        expect(screen.getByText('fix: bug')).toBeInTheDocument();
        expect(screen.getByText('feat: new feature')).toBeInTheDocument();
      });
    });

    it('displays frontend changes badge', async () => {
      const user = userEvent.setup();
      render(<UpdateButton />);

      await openAndCheck(user);

      await waitFor(() => {
        expect(screen.getByText('前端变更')).toBeInTheDocument();
      });
    });

    it('blocks confirmation while tasks are active', async () => {
      vi.mocked(api.startUpdate).mockResolvedValue({
        ...mockDryRun,
        active_task_count: 1,
        active_tasks: [{ id: 42, title: '正在写代码', status: 'executing' }],
        update_blocked: true,
      } as never);

      const user = userEvent.setup();
      render(<UpdateButton />);
      await openAndCheck(user);

      await waitFor(() => {
        expect(screen.getByText(/当前有 1 个任务正在执行/)).toBeInTheDocument();
      });
      expect(screen.getByText(/#42 正在写代码/)).toBeInTheDocument();
      expect(screen.getByRole('button', { name: '重新核对运行状态' })).toBeEnabled();
      expect(screen.getByRole('button', { name: '等待任务完成' })).toBeDisabled();
      expect(api.startUpdate).toHaveBeenCalledTimes(1);
    });

    it('reconciles stale blockers and refreshes the dry-run before enabling update', async () => {
      const blockedResult = {
        ...mockDryRun,
        active_task_count: 1,
        active_tasks: [{ id: 42, title: '已退出的任务', status: 'executing' }],
        update_blocked: true,
      };
      vi.mocked(api.startUpdate)
        .mockResolvedValueOnce(blockedResult as never)
        .mockResolvedValueOnce(mockDryRun as never);

      const user = userEvent.setup();
      render(<UpdateButton />);
      await openAndCheck(user);
      await user.click(await screen.findByRole('button', { name: '重新核对运行状态' }));

      await waitFor(() => {
        expect(screen.queryByText(/当前有 1 个任务正在执行/)).not.toBeInTheDocument();
      });
      expect(api.reconcileUpdateState).toHaveBeenCalledTimes(1);
      expect(api.startUpdate).toHaveBeenLastCalledWith({
        dry_run: true,
        force: true,
        channel: 'stable',
        branch: undefined,
      });
      expect(vi.mocked(api.reconcileUpdateState).mock.invocationCallOrder[0])
        .toBeLessThan(vi.mocked(api.startUpdate).mock.invocationCallOrder[1]);
      expect(screen.getByText('运行状态已重新核对，可以继续更新或重启。')).toBeInTheDocument();
      expect(screen.getByRole('button', { name: '确认更新' })).toBeEnabled();
    });

    it('keeps real blockers visible after reconciliation and dry-run refresh', async () => {
      const initialBlocker = {
        ...mockDryRun,
        active_task_count: 1,
        active_tasks: [{ id: 42, title: '旧任务记录', status: 'executing' }],
        update_blocked: true,
      };
      const liveBlocker = {
        ...mockDryRun,
        active_task_count: 1,
        active_tasks: [{ id: 77, title: '真实运行任务', status: 'executing' }],
        update_blocked: true,
      };
      vi.mocked(api.startUpdate)
        .mockResolvedValueOnce(initialBlocker as never)
        .mockResolvedValueOnce(liveBlocker as never);
      vi.mocked(api.reconcileUpdateState).mockResolvedValue({
        update_blocked: false,
        active_task_count: 0,
        active_tasks: [],
        reconciled: true,
      } as never);

      const user = userEvent.setup();
      render(<UpdateButton />);
      await openAndCheck(user);
      await user.click(await screen.findByRole('button', { name: '重新核对运行状态' }));

      expect(await screen.findByText(/#77 真实运行任务/)).toBeInTheDocument();
      expect(screen.queryByText(/#42 旧任务记录/)).not.toBeInTheDocument();
      expect(screen.getByText(/以上运行阻断项仍存在/)).toBeInTheDocument();
      expect(screen.getByRole('button', { name: '等待任务完成' })).toBeDisabled();
      expect(api.startUpdate).toHaveBeenCalledTimes(2);
    });

    it('labels unresolved instance evidence as a blocker instead of a user task', async () => {
      vi.mocked(api.startUpdate).mockResolvedValue({
        ...mockDryRun,
        active_task_count: 1,
        active_tasks: [{
          id: 13,
          instance_id: 13,
          title: '实例 worker-13（仍有未解除运行证据）',
          status: 'quarantined_process_evidence',
          kind: 'instance',
        }],
        update_blocked: true,
      } as never);

      const user = userEvent.setup();
      render(<UpdateButton />);
      await openAndCheck(user);

      expect(await screen.findByText(/当前有 1 个运行阻断项/)).toBeInTheDocument();
      expect(screen.getByText(/实例 worker-13（仍有未解除运行证据）/)).toBeInTheDocument();
      expect(screen.queryByText(/#13 实例 worker-13/)).not.toBeInTheDocument();
    });

    it('labels a live auxiliary generation as a runtime blocker', async () => {
      vi.mocked(api.startUpdate).mockResolvedValue({
        ...mockDryRun,
        active_task_count: 1,
        active_tasks: [{
          id: 7,
          title: '监控子 Agent #7',
          status: 'running_auxiliary',
          kind: 'monitor',
        }],
        update_blocked: true,
      } as never);

      const user = userEvent.setup();
      render(<UpdateButton />);
      await openAndCheck(user);

      expect(await screen.findByText(/当前有 1 个运行阻断项/)).toBeInTheDocument();
      expect(screen.getByText(/监控子 Agent #7（running_auxiliary）/)).toBeInTheDocument();
      expect(screen.queryByText(/^#7 监控子 Agent/)).not.toBeInTheDocument();
    });

    it('keeps the original blocker when the post-reconcile dry-run fails', async () => {
      const initialBlocker = {
        ...mockDryRun,
        active_task_count: 1,
        active_tasks: [{ id: 42, title: '尚未确认的任务', status: 'executing' }],
        update_blocked: true,
      };
      vi.mocked(api.startUpdate)
        .mockResolvedValueOnce(initialBlocker as never)
        .mockRejectedValueOnce(new Error('无法刷新部署状态'));

      const user = userEvent.setup();
      render(<UpdateButton />);
      await openAndCheck(user);
      await user.click(await screen.findByRole('button', { name: '重新核对运行状态' }));

      expect(await screen.findByText(
        '运行状态已核对，但刷新更新信息失败：无法刷新部署状态'
      )).toBeInTheDocument();
      expect(screen.getByText(/#42 尚未确认的任务/)).toBeInTheDocument();
      expect(screen.getByRole('button', { name: '等待任务完成' })).toBeDisabled();
      expect(screen.getByRole('button', { name: '重新核对运行状态' })).toBeEnabled();
      expect(api.startUpdate).toHaveBeenCalledTimes(2);
    });

    it('keeps the blocker and shows a clear error when reconciliation fails', async () => {
      vi.mocked(api.startUpdate).mockResolvedValue({
        ...mockDryRun,
        active_task_count: 1,
        active_tasks: [{ id: 42, title: '待核对任务', status: 'executing' }],
        update_blocked: true,
      } as never);
      vi.mocked(api.reconcileUpdateState).mockRejectedValue(new Error('维护锁正忙'));

      const user = userEvent.setup();
      render(<UpdateButton />);
      await openAndCheck(user);
      await user.click(await screen.findByRole('button', { name: '重新核对运行状态' }));

      expect(await screen.findByText('重新核对运行状态失败：维护锁正忙')).toBeInTheDocument();
      expect(screen.getByText(/#42 待核对任务/)).toBeInTheDocument();
      expect(screen.getByRole('button', { name: '等待任务完成' })).toBeDisabled();
      expect(api.startUpdate).toHaveBeenCalledTimes(1);
    });
  });

  describe('deployment state actions', () => {
    it('shows running, disk, and database revisions', async () => {
      vi.mocked(api.startUpdate).mockResolvedValue({
        has_updates: false,
        needs_restart: false,
        repair_required: false,
        running_commit: '1111111abcdef',
        disk_commit: '1111111fedcba',
        db_current_revision: 'rev_a',
        db_head_revision: 'rev_a',
      } as never);

      const user = userEvent.setup();
      render(<UpdateButton />);
      await openAndCheck(user);

      await waitFor(() => {
        expect(screen.getByText('运行版本')).toBeInTheDocument();
        expect(screen.getByText('磁盘版本')).toBeInTheDocument();
        expect(screen.getByText('数据库')).toBeInTheDocument();
        expect(screen.getByText('已同步')).toBeInTheDocument();
      });
    });

    it('uses a full repair when the running service is stale and restart safety is unproven', async () => {
      vi.mocked(api.startUpdate).mockResolvedValue({
        has_updates: false,
        needs_restart: true,
        repair_required: false,
        running_commit: 'old1111',
        disk_commit: 'new2222',
      } as never);

      const user = userEvent.setup();
      render(<UpdateButton />);
      await openAndCheck(user);

      const repairButton = await screen.findByRole('button', { name: '修复并重新部署' });
      await user.click(repairButton);

      expect(api.repairUpdate).toHaveBeenCalledTimes(1);
      expect(api.restartService).not.toHaveBeenCalled();
      expect(api.startUpdate).toHaveBeenCalledTimes(1);
      expect(screen.getByText('更新中...')).toBeInTheDocument();
    });

    it('uses lightweight restart only with an explicit backend safety proof', async () => {
      vi.mocked(api.startUpdate).mockResolvedValue({
        has_updates: false,
        needs_restart: true,
        restart_only_safe: true,
        repair_required: false,
        running_commit: 'same111',
        disk_commit: 'same111',
      } as never);

      const user = userEvent.setup();
      render(<UpdateButton />);
      await openAndCheck(user);

      await user.click(await screen.findByRole('button', { name: '重启服务' }));

      expect(api.restartService).toHaveBeenCalledTimes(1);
      expect(api.repairUpdate).not.toHaveBeenCalled();
    });

    it('offers manual restart even when code, service, and database are aligned', async () => {
      vi.mocked(api.startUpdate).mockResolvedValue({
        has_updates: false,
        needs_restart: false,
        repair_required: false,
        running_commit: 'same111',
        disk_commit: 'same111',
        db_current_revision: 'rev_a',
        db_head_revision: 'rev_a',
      } as never);

      const user = userEvent.setup();
      render(<UpdateButton />);
      await openAndCheck(user);

      const restartButton = await screen.findByRole('button', { name: '手动重启' });
      await user.click(restartButton);

      expect(api.restartService).toHaveBeenCalledTimes(1);
      expect(screen.getByText('重启中...')).toBeInTheDocument();
    });

    it('uses repair instead of restart when deployment is incomplete', async () => {
      vi.mocked(api.startUpdate).mockResolvedValue({
        has_updates: false,
        needs_restart: true,
        repair_required: true,
        repair_reasons: ['数据库版本落后于代码'],
        running_commit: 'old1111',
        disk_commit: 'new2222',
        db_current_revision: null,
        db_head_revision: 'rev_b',
        db_up_to_date: false,
      } as never);

      const user = userEvent.setup();
      render(<UpdateButton />);
      await openAndCheck(user);

      expect(await screen.findByText('数据库版本落后于代码')).toBeInTheDocument();
      expect(screen.getByText('待迁移')).toBeInTheDocument();
      expect(screen.queryByRole('button', { name: '重启服务' })).not.toBeInTheDocument();
      await user.click(screen.getByRole('button', { name: '修复并重新部署' }));

      expect(api.repairUpdate).toHaveBeenCalledWith();
      expect(api.restartService).not.toHaveBeenCalled();
      expect(screen.getByText('更新中...')).toBeInTheDocument();
      expect(screen.queryByText('跳过前端构建（仅后端更新）')).not.toBeInTheDocument();
    });

    it('requires an explicit data-loss confirmation before rolling back a migrated database', async () => {
      sessionStorage.setItem('ccm-update-active', '1');
      vi.mocked(api.getUpdateStatus).mockResolvedValue({
        status: 'completed',
        old_commit: 'old1111',
        new_commit: 'new2222',
        database_migration_applied: true,
      } as never);
      const confirmSpy = vi.spyOn(window, 'confirm')
        .mockReturnValueOnce(true)
        .mockReturnValueOnce(true);

      const user = userEvent.setup();
      render(<UpdateButton />);
      await screen.findByText('更新完成');
      await user.click(screen.getByRole('button', { name: '回滚' }));

      await waitFor(() => {
        expect(api.rollbackUpdate).toHaveBeenCalledWith({
          confirm_database_restore: true,
        });
      });
      expect(confirmSpy).toHaveBeenCalledTimes(2);
      expect(confirmSpy.mock.calls[1][0]).toContain('丢失更新完成后产生的数据');
      confirmSpy.mockRestore();
    });

    it('does not restore the database when the completed update did not migrate it', async () => {
      sessionStorage.setItem('ccm-update-active', '1');
      vi.mocked(api.getUpdateStatus).mockResolvedValue({
        status: 'completed',
        old_commit: 'old1111',
        new_commit: 'new2222',
        database_migration_applied: false,
      } as never);
      const confirmSpy = vi.spyOn(window, 'confirm').mockReturnValue(true);

      const user = userEvent.setup();
      render(<UpdateButton />);
      await screen.findByText('更新完成');
      await user.click(screen.getByRole('button', { name: '回滚' }));

      await waitFor(() => {
        expect(api.rollbackUpdate).toHaveBeenCalledWith({
          confirm_database_restore: false,
        });
      });
      expect(confirmSpy).toHaveBeenCalledTimes(1);
      confirmSpy.mockRestore();
    });

    it('treats an interrupted migration with unknown outcome as requiring database restore', async () => {
      sessionStorage.setItem('ccm-update-active', '1');
      vi.mocked(api.getUpdateStatus).mockResolvedValue({
        status: 'failed',
        old_commit: 'old1111',
        new_commit: 'new2222',
        database_migration_required: true,
        database_migration_applied: null,
      } as never);
      const confirmSpy = vi.spyOn(window, 'confirm')
        .mockReturnValueOnce(true)
        .mockReturnValueOnce(true);

      const user = userEvent.setup();
      render(<UpdateButton />);
      await screen.findByText('更新失败');
      await user.click(screen.getByRole('button', { name: '回滚' }));

      await waitFor(() => {
        expect(api.rollbackUpdate).toHaveBeenCalledWith({
          confirm_database_restore: true,
        });
      });
      expect(confirmSpy.mock.calls[1][0]).toContain('可能已经开始执行');
      confirmSpy.mockRestore();
    });

    it('conservatively confirms database restore for legacy status without migration metadata', async () => {
      sessionStorage.setItem('ccm-update-active', '1');
      vi.mocked(api.getUpdateStatus).mockResolvedValue({
        status: 'failed',
        old_commit: 'old1111',
        new_commit: 'new2222',
      } as never);
      const confirmSpy = vi.spyOn(window, 'confirm')
        .mockReturnValueOnce(true)
        .mockReturnValueOnce(true);

      const user = userEvent.setup();
      render(<UpdateButton />);
      await screen.findByText('更新失败');
      await user.click(screen.getByRole('button', { name: '回滚' }));

      await waitFor(() => {
        expect(api.rollbackUpdate).toHaveBeenCalledWith({
          confirm_database_restore: true,
        });
      });
      expect(confirmSpy).toHaveBeenCalledTimes(2);
      confirmSpy.mockRestore();
    });

    it('does not offer a second rollback after an automatic rollback completed', async () => {
      sessionStorage.setItem('ccm-update-active', '1');
      vi.mocked(api.getUpdateStatus).mockResolvedValue({
        status: 'rolled_back',
        old_commit: 'old1111',
        new_commit: 'new2222',
        error: '迁移失败，已自动回滚',
      } as never);

      render(<UpdateButton />);

      expect(await screen.findByText('迁移失败，已自动回滚')).toBeInTheDocument();
      expect(screen.queryByRole('button', { name: '回滚' })).not.toBeInTheDocument();
    });

    it('does not offer rollback after a same-commit manual restart', async () => {
      sessionStorage.setItem('ccm-update-active', '1');
      vi.mocked(api.getUpdateStatus).mockResolvedValue({
        status: 'completed',
        operation: 'restart',
        old_commit: 'same1111',
        new_commit: 'same1111',
      } as never);

      render(<UpdateButton />);

      expect(await screen.findByText('更新完成')).toBeInTheDocument();
      expect(screen.queryByRole('button', { name: '回滚' })).not.toBeInTheDocument();
    });

    it('surfaces a dry-run error instead of reporting that code is current', async () => {
      vi.mocked(api.startUpdate).mockResolvedValue({
        has_updates: false,
        error: '无法拉取 origin/main',
      } as never);

      const user = userEvent.setup();
      render(<UpdateButton />);
      await openAndCheck(user);

      expect(await screen.findByText('无法拉取 origin/main')).toBeInTheDocument();
      expect(screen.getByText('更新失败')).toBeInTheDocument();
      expect(screen.queryByText('已是最新版本，无需更新。')).not.toBeInTheDocument();
    });

    it('recovers an active update after a page refresh', async () => {
      sessionStorage.setItem('ccm-update-active', '1');
      vi.mocked(api.getUpdateStatus).mockResolvedValue({
        status: 'running',
        update_id: 'u-recovered',
        steps: [{ name: 'uv_sync', status: 'running' }],
      } as never);

      render(<UpdateButton />);

      expect(await screen.findByText('更新中...')).toBeInTheDocument();
      expect(screen.getByText('Python 依赖')).toBeInTheDocument();
      expect(api.getUpdateStatus).toHaveBeenCalledTimes(1);
    });

    it('polls a refreshed running pipeline through handoff to completion', async () => {
      vi.useFakeTimers();
      sessionStorage.setItem('ccm-update-active', '1');
      vi.mocked(api.getUpdateStatus)
        .mockResolvedValueOnce({ status: 'running' } as never)
        .mockResolvedValueOnce({
          status: 'restarting',
          repair_required: true,
          deployment_incomplete: true,
        } as never)
        .mockResolvedValueOnce({ status: 'completed' } as never);
      vi.mocked(api.health).mockResolvedValue({ status: 'ok', commit: 'new2222' });

      render(<UpdateButton />);
      await act(async () => {
        await Promise.resolve();
        await Promise.resolve();
      });
      expect(screen.getByText('更新中...')).toBeInTheDocument();

      await act(async () => {
        await vi.advanceTimersByTimeAsync(2000);
      });
      expect(screen.getByText('重启中...')).toBeInTheDocument();
      expect(screen.queryByText('更新失败')).not.toBeInTheDocument();

      await act(async () => {
        await vi.advanceTimersByTimeAsync(2000);
      });
      expect(screen.getByText('更新完成')).toBeInTheDocument();
      expect(api.getUpdateStatus).toHaveBeenCalledTimes(3);
    });

    it('accepts a terminal recovered status when restart was too fast to observe downtime', async () => {
      vi.useFakeTimers();
      sessionStorage.setItem('ccm-update-active', '1');
      vi.mocked(api.getUpdateStatus)
        .mockResolvedValueOnce({ status: 'restarting' } as never)
        .mockResolvedValueOnce({ status: 'completed' } as never);
      vi.mocked(api.health).mockResolvedValue({ status: 'ok', commit: 'new2222' });

      render(<UpdateButton />);
      await act(async () => {
        await Promise.resolve();
        await Promise.resolve();
      });
      expect(screen.getByText('重启中...')).toBeInTheDocument();

      await act(async () => {
        await vi.advanceTimersByTimeAsync(2000);
      });

      expect(screen.getByText('更新完成')).toBeInTheDocument();
      expect(api.health).toHaveBeenCalled();
      expect(api.getUpdateStatus).toHaveBeenCalledTimes(2);
    });

    it('does not treat the active handoff repair fence as a terminal failure', async () => {
      vi.useFakeTimers();
      sessionStorage.setItem('ccm-update-active', '1');
      vi.mocked(api.getUpdateStatus)
        .mockResolvedValueOnce({
          status: 'restarting',
          repair_required: true,
          deployment_incomplete: true,
        } as never)
        .mockResolvedValueOnce({
          status: 'starting',
          repair_required: true,
          deployment_incomplete: true,
        } as never)
        .mockResolvedValueOnce({ status: 'completed' } as never);
      vi.mocked(api.health).mockResolvedValue({ status: 'ok', commit: 'new2222' });

      render(<UpdateButton />);
      await act(async () => {
        await Promise.resolve();
        await Promise.resolve();
      });

      await act(async () => {
        await vi.advanceTimersByTimeAsync(2000);
      });
      expect(screen.getByText('重启中...')).toBeInTheDocument();
      expect(screen.queryByText('更新失败')).not.toBeInTheDocument();

      await act(async () => {
        await vi.advanceTimersByTimeAsync(2000);
      });
      expect(screen.getByText('更新完成')).toBeInTheDocument();
      expect(api.getUpdateStatus).toHaveBeenCalledTimes(3);
    });
  });

  describe('update checks', () => {
    it('does not check for or prompt about updates automatically', async () => {
      vi.useFakeTimers();
      render(<UpdateButton />);

      await act(async () => {
        await vi.advanceTimersByTimeAsync(2 * 60 * 60_000);
      });

      expect(api.getUpdateStatus).not.toHaveBeenCalled();
      expect(api.startUpdate).not.toHaveBeenCalled();
      expect(findModalOverlay()).toBeNull();
      expect(screen.queryByText('发现可用更新')).not.toBeInTheDocument();
    });
  });

  describe('visibilitychange recovery', () => {
    function simulateVisibilityChange(state: 'visible' | 'hidden') {
      Object.defineProperty(document, 'visibilityState', {
        value: state,
        writable: true,
        configurable: true,
      });
      document.dispatchEvent(new Event('visibilitychange'));
    }

    it('polls update status when page becomes visible during running phase', async () => {
      vi.mocked(api.startUpdate)
        .mockResolvedValueOnce(mockDryRun as never)
        .mockResolvedValueOnce({ update_id: 'u1', old_commit: 'abc' } as never);

      const mockStatus = {
        status: 'completed',
        old_commit: 'abc1234',
        new_commit: 'def5678',
        steps: [{ name: 'git_pull', status: 'completed', duration_ms: 500 }],
      };
      vi.mocked(api.getUpdateStatus).mockResolvedValue(mockStatus as never);

      const user = userEvent.setup();
      render(<UpdateButton />);

      await openAndCheck(user);
      await waitFor(() => expect(findModalOverlay()).toBeTruthy());

      const confirmBtn = screen.getAllByText('确认更新').find(el => el.tagName === 'BUTTON');
      await user.click(confirmBtn!);

      await waitFor(() => {
        expect(screen.getByText('更新中...')).toBeInTheDocument();
      });

      simulateVisibilityChange('hidden');
      simulateVisibilityChange('visible');

      await waitFor(() => {
        expect(api.getUpdateStatus).toHaveBeenCalled();
      });

      await waitFor(() => {
        expect(screen.getByText('更新完成')).toBeInTheDocument();
      });
    });

    it('does NOT poll when page becomes visible during idle phase', async () => {
      render(<UpdateButton />);

      simulateVisibilityChange('hidden');
      simulateVisibilityChange('visible');

      await new Promise(r => setTimeout(r, 50));
      expect(api.getUpdateStatus).not.toHaveBeenCalled();
    });

    it('does NOT poll when page becomes visible during confirming phase', async () => {
      const user = userEvent.setup();
      render(<UpdateButton />);

      await openAndCheck(user);
      await waitFor(() => expect(findModalOverlay()).toBeTruthy());

      simulateVisibilityChange('hidden');
      simulateVisibilityChange('visible');

      await new Promise(r => setTimeout(r, 50));
      expect(api.getUpdateStatus).not.toHaveBeenCalled();
    });

    it('handles failed status on visibility recovery', async () => {
      vi.mocked(api.startUpdate)
        .mockResolvedValueOnce(mockDryRun as never)
        .mockResolvedValueOnce({ update_id: 'u1', old_commit: 'abc' } as never);

      vi.mocked(api.getUpdateStatus).mockResolvedValue({
        status: 'failed',
        error: '迁移出错',
        steps: [{ name: 'alembic_upgrade', status: 'failed' }],
      } as never);

      const user = userEvent.setup();
      render(<UpdateButton />);

      await openAndCheck(user);
      await waitFor(() => expect(findModalOverlay()).toBeTruthy());

      const confirmBtn = screen.getAllByText('确认更新').find(el => el.tagName === 'BUTTON');
      await user.click(confirmBtn!);

      await waitFor(() => {
        expect(screen.getByText('更新中...')).toBeInTheDocument();
      });

      simulateVisibilityChange('visible');

      await waitFor(() => {
        expect(screen.getByText('更新失败')).toBeInTheDocument();
        expect(screen.getByText('迁移出错')).toBeInTheDocument();
      });
    });

    it('keeps polling when visibility recovery sees an active incomplete handoff', async () => {
      vi.mocked(api.startUpdate)
        .mockResolvedValueOnce(mockDryRun as never)
        .mockResolvedValueOnce({ update_id: 'u1', old_commit: 'abc' } as never);
      vi.mocked(api.getUpdateStatus).mockResolvedValue({
        status: 'restarting',
        repair_required: true,
        deployment_incomplete: true,
      } as never);

      const user = userEvent.setup();
      render(<UpdateButton />);
      await openAndCheck(user);
      await waitFor(() => expect(findModalOverlay()).toBeTruthy());
      const confirmBtn = screen.getAllByText('确认更新').find(el => el.tagName === 'BUTTON');
      await user.click(confirmBtn!);

      simulateVisibilityChange('visible');

      await waitFor(() => {
        expect(screen.getByText('重启中...')).toBeInTheDocument();
      });
      expect(screen.queryByText('更新失败')).not.toBeInTheDocument();
    });

    it('keeps current phase if getUpdateStatus fails (server still restarting)', async () => {
      vi.mocked(api.startUpdate)
        .mockResolvedValueOnce(mockDryRun as never)
        .mockResolvedValueOnce({ update_id: 'u1', old_commit: 'abc' } as never);

      vi.mocked(api.getUpdateStatus).mockRejectedValue(new Error('connection refused'));

      const user = userEvent.setup();
      render(<UpdateButton />);

      await openAndCheck(user);
      await waitFor(() => expect(findModalOverlay()).toBeTruthy());

      const confirmBtn = screen.getAllByText('确认更新').find(el => el.tagName === 'BUTTON');
      await user.click(confirmBtn!);

      await waitFor(() => {
        expect(screen.getByText('更新中...')).toBeInTheDocument();
      });

      simulateVisibilityChange('visible');

      await new Promise(r => setTimeout(r, 100));
      expect(screen.getByText('更新中...')).toBeInTheDocument();
    });

    it('does NOT trigger on hidden event (only on visible)', async () => {
      vi.mocked(api.startUpdate)
        .mockResolvedValueOnce(mockDryRun as never)
        .mockResolvedValueOnce({ update_id: 'u1', old_commit: 'abc' } as never);

      const user = userEvent.setup();
      render(<UpdateButton />);

      await openAndCheck(user);
      await waitFor(() => expect(findModalOverlay()).toBeTruthy());

      const confirmBtn = screen.getAllByText('确认更新').find(el => el.tagName === 'BUTTON');
      await user.click(confirmBtn!);

      await waitFor(() => {
        expect(screen.getByText('更新中...')).toBeInTheDocument();
      });

      simulateVisibilityChange('hidden');

      await new Promise(r => setTimeout(r, 50));
      expect(api.getUpdateStatus).not.toHaveBeenCalled();
    });
  });
});
