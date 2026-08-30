import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { cleanup, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import App from './App';

vi.mock('./api/client', () => ({
  getToken: vi.fn(() => 'member-token'),
}));
vi.mock('./config/server', () => ({
  isCapacitor: vi.fn(() => false),
  getServerUrl: vi.fn(() => ''),
  getApiBase: vi.fn(() => ''),
}));
vi.mock('./components/Layout/AppShell', () => ({
  AppShell: ({ children, onNavigate }: { children: React.ReactNode; onNavigate: (page: string) => void }) => (
    <div
      data-testid="app-shell"
      data-role={JSON.parse(localStorage.getItem('cc_user') || '{}').role || ''}
    >
      <button type="button" onClick={() => onNavigate('plans')}>Navigate to Plans</button>
      <button type="button" onClick={() => onNavigate('settings')}>Navigate to Settings</button>
      <button type="button" onClick={() => onNavigate('pr-monitor')}>Navigate to PR Monitor</button>
      {children}
    </div>
  ),
}));
vi.mock('./pages/TasksPage', () => ({
  TasksPage: ({ chatTaskId }: { chatTaskId: number | null }) => <>
    <div>Tasks screen</div>
    {chatTaskId != null && <div>Task chat {chatTaskId}</div>}
  </>,
}));
vi.mock('./pages/PlansPage', () => ({
  PlansPage: ({ selectedPlanId, onNavigateTask, onNavigateSettings }: { selectedPlanId: number | null; onNavigateTask: (taskId: number) => void; onNavigateSettings: () => void }) => (
    <div>
      Plans screen {selectedPlanId ?? 'none'}
      <button type="button" onClick={() => onNavigateTask(200)}>Open related Task #200</button>
      <button type="button" onClick={onNavigateSettings}>Open Plan settings</button>
    </div>
  ),
}));
vi.mock('./pages/SettingsPage', () => ({
  SettingsPage: () => <div>Settings screen</div>,
}));
vi.mock('./pages/PRMonitorPage', () => ({
  PRMonitorPage: () => <div>PR Monitor screen</div>,
}));
vi.mock('./pages/LoginPage', () => ({
  LoginPage: () => <div>Login screen</div>,
}));

describe('App authentication probe', () => {
  beforeEach(() => {
    window.location.hash = '#/tasks';
    localStorage.clear();
  });

  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
  });

  it('uses auth/me instead of the Instance admin API', async () => {
    localStorage.setItem('cc_user', JSON.stringify({
      id: 4,
      name: 'Stale Admin',
      role: 'admin',
    }));
    const fetchMock = vi.fn()
      .mockResolvedValueOnce({ ok: true })
      .mockResolvedValueOnce({
        ok: true,
        json: vi.fn().mockResolvedValue({
          ok: true,
          user: { id: 4, name: 'Member', role: 'member' },
        }),
      });
    vi.stubGlobal('fetch', fetchMock);

    render(<App />);

    expect(await screen.findByText('Tasks screen')).toBeInTheDocument();
    expect(screen.getByTestId('app-shell')).toHaveAttribute(
      'data-role',
      'member',
    );
    const urls = fetchMock.mock.calls.map(([url]) => String(url));
    expect(urls).toEqual(['/api/system/health', '/api/auth/me']);
    expect(urls.some((url) => url.includes('/api/instances'))).toBe(false);
    await waitFor(() => {
      expect(JSON.parse(localStorage.getItem('cc_user') || '{}'))
        .toMatchObject({ name: 'Member' });
    });
  });

  it('shows login when auth/me rejects the current credentials', async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce({ ok: true })
      .mockResolvedValueOnce({ ok: false, status: 401 });
    vi.stubGlobal('fetch', fetchMock);

    render(<App />);

    expect(await screen.findByText('Login screen')).toBeInTheDocument();
  });

  it('replaces a stale cached identity in no-auth mode', async () => {
    localStorage.setItem('cc_user', JSON.stringify({
      name: 'Old member',
      role: 'member',
    }));
    const fetchMock = vi.fn()
      .mockResolvedValueOnce({ ok: true })
      .mockResolvedValueOnce({
        ok: true,
        json: vi.fn().mockResolvedValue({
          ok: true,
          auth_type: 'none',
          role: 'super_admin',
        }),
      });
    vi.stubGlobal('fetch', fetchMock);

    render(<App />);

    expect(await screen.findByText('Tasks screen')).toBeInTheDocument();
    await waitFor(() => {
      expect(JSON.parse(localStorage.getItem('cc_user') || '{}')).toEqual({
        name: 'Local Admin',
        role: 'super_admin',
      });
    });
  });

  it('restores a first-class Plan deep link', async () => {
    window.location.hash = '#/plans/14';
    const fetchMock = vi.fn()
      .mockResolvedValueOnce({ ok: true })
      .mockResolvedValueOnce({
        ok: true,
        json: vi.fn().mockResolvedValue({ ok: true, role: 'super_admin' }),
      });
    vi.stubGlobal('fetch', fetchMock);

    render(<App />);

    expect(await screen.findByText('Plans screen 14')).toBeInTheDocument();
  });

  it('recognizes and preserves an exact PR Monitor review deep link', async () => {
    window.location.hash = '#/pr-monitor?repo=5&review=113';
    const fetchMock = vi.fn()
      .mockResolvedValueOnce({ ok: true })
      .mockResolvedValueOnce({
        ok: true,
        json: vi.fn().mockResolvedValue({ ok: true, role: 'super_admin' }),
      });
    vi.stubGlobal('fetch', fetchMock);

    render(<App />);

    expect(await screen.findByText('PR Monitor screen')).toBeInTheDocument();
    await waitFor(() => {
      expect(window.location.hash).toBe('#/pr-monitor?repo=5&review=113');
    });
  });

  it('clears a PR Monitor deep link when its current navigation item is selected', async () => {
    window.location.hash = '#/pr-monitor?repo=5&review=113';
    const fetchMock = vi.fn()
      .mockResolvedValueOnce({ ok: true })
      .mockResolvedValueOnce({
        ok: true,
        json: vi.fn().mockResolvedValue({ ok: true, role: 'super_admin' }),
      });
    vi.stubGlobal('fetch', fetchMock);
    const hashChange = vi.fn();
    window.addEventListener('hashchange', hashChange);

    render(<App />);
    await userEvent.click(await screen.findByRole('button', { name: 'Navigate to PR Monitor' }));

    expect(window.location.hash).toBe('#/pr-monitor');
    expect(hashChange).toHaveBeenCalled();
    window.removeEventListener('hashchange', hashChange);
  });

  it('navigates atomically from a related Plan to its Task chat', async () => {
    window.location.hash = '#/plans/14';
    const fetchMock = vi.fn()
      .mockResolvedValueOnce({ ok: true })
      .mockResolvedValueOnce({
        ok: true,
        json: vi.fn().mockResolvedValue({ ok: true, role: 'super_admin' }),
      });
    vi.stubGlobal('fetch', fetchMock);

    render(<App />);
    await userEvent.click(await screen.findByRole('button', { name: 'Open related Task #200' }));

    expect(await screen.findByText('Task chat 200')).toBeInTheDocument();
    await waitFor(() => expect(window.location.hash).toBe('#/tasks/chat/200'));
  });

  it('returns from Settings to the plain Plans URL instead of an older Plan deep link', async () => {
    window.history.replaceState(null, '', '#/plans/34');
    window.history.pushState(null, '', '#/plans');
    const fetchMock = vi.fn()
      .mockResolvedValueOnce({ ok: true })
      .mockResolvedValueOnce({
        ok: true,
        json: vi.fn().mockResolvedValue({ ok: true, role: 'super_admin' }),
      });
    vi.stubGlobal('fetch', fetchMock);

    render(<App />);
    expect(await screen.findByText('Plans screen none')).toBeInTheDocument();

    await userEvent.click(screen.getByRole('button', { name: 'Open Plan settings' }));
    expect(await screen.findByText('Settings screen')).toBeInTheDocument();
    expect(window.location.hash).toBe('#/settings');

    window.history.back();

    expect(await screen.findByText('Plans screen none')).toBeInTheDocument();
    await waitFor(() => expect(window.location.hash).toBe('#/plans'));
  });
});
