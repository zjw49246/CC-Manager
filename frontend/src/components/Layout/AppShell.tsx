import { useCallback, useEffect, useRef, useState } from 'react';
import type { ReactNode } from 'react';
import {
  Bot, Menu, X, PanelLeftClose, PanelLeftOpen, LayoutDashboard, ListTodo, FolderGit2, KeyRound,
  FolderOpen, MessagesSquare, GitPullRequest, Server, Sparkles, Users, Globe,
  ScrollText, Settings, GitBranch,
} from '../icons';
import type { ComponentType } from 'react';
import { api } from '../../api/client';
import { isCapacitor } from '../../config/server';
import { useWebSocket } from '../../hooks/useWebSocket';
import { useTheme } from '../../hooks/useTheme';
import { getThemeOption } from '../../config/theme';
import { getNavIcon } from '../../config/iconSets';
import { PoolDrawer } from './PoolDrawer';
import { UpdateButton } from '../System/UpdateButton';

interface AppShellProps {
  currentPage: string;
  onNavigate: (page: string) => void;
  /** 分屏聊天等需要更大内容宽度的页面 */
  wide?: boolean;
  children: ReactNode;
}

interface NavItem {
  key: string;
  label: string;
  /** 中央图标模块（components/icons）的主题化组件；Lucide 兼容 props */
  icon: ComponentType<{ size?: number | string; className?: string }>;
  show: boolean;
}

/** App 壳：桌面端左侧固定侧栏，移动端顶栏 + 抽屉；顶栏收纳全局控件。
 * 页面主体走文档流滚动（sticky 顶栏），分屏视图的 100vh 计算依赖顶栏
 * 高度 = h-12 (3rem) + 底边框。 */
export function AppShell({ currentPage, onNavigate, wide, children }: AppShellProps) {
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [navCollapsed, setNavCollapsed] = useState(
    () => localStorage.getItem('ccm-nav-collapsed') === 'true',
  );

  const [navWidth, setNavWidth] = useState(() => {
    const saved = localStorage.getItem('ccm-nav-width');
    return saved ? Math.max(180, Math.min(400, Number(saved))) : 240;
  });
  const navDragging = useRef(false);
  const navDragStartX = useRef(0);
  const navDragStartW = useRef(240);

  const handleNavDragStart = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    navDragging.current = true;
    navDragStartX.current = e.clientX;
    navDragStartW.current = navWidth;
    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';

    const onMove = (ev: MouseEvent) => {
      if (!navDragging.current) return;
      const w = Math.max(180, Math.min(400, navDragStartW.current + ev.clientX - navDragStartX.current));
      setNavWidth(w);
    };
    const onUp = () => {
      navDragging.current = false;
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
      document.removeEventListener('mousemove', onMove);
      document.removeEventListener('mouseup', onUp);
      setNavWidth(w => { localStorage.setItem('ccm-nav-width', String(w)); return w; });
    };
    document.addEventListener('mousemove', onMove);
    document.addEventListener('mouseup', onUp);
  }, [navWidth]);

  // 主题图标集：feishu → IconPark two-tone / apple → Ionicons；其余 Lucide
  const theme = useTheme();
  const iconSet = getThemeOption(theme).iconSet;
  const navResizable = theme !== 'feishu' && theme !== 'apple';

  const [isLg, setIsLg] = useState(() => window.innerWidth >= 1024);
  useEffect(() => {
    const mq = window.matchMedia('(min-width: 1024px)');
    const h = (e: MediaQueryListEvent) => setIsLg(e.matches);
    mq.addEventListener('change', h);
    return () => mq.removeEventListener('change', h);
  }, []);

  const ccUser = JSON.parse(localStorage.getItem('cc_user') || '{}');
  const isAdmin = ccUser.role === 'admin' || ccUser.role === 'super_admin' || !ccUser.id;
  const [hasWorker, setHasWorker] = useState(isAdmin);
  const [hasPlanActions, setHasPlanActions] = useState(false);
  const [hasDeliveryActions, setHasDeliveryActions] = useState(false);

  const refreshWorkerStatus = useCallback(() => {
    if (!isAdmin) {
      api.listWorkers().then(w => setHasWorker(w.length > 0)).catch(() => {});
    }
  }, [isAdmin]);

  useEffect(() => {
    refreshWorkerStatus();
    if (isAdmin) return;
    // Members cannot subscribe to the cross-owner global workers channel.
    // Poll the already ACL-filtered list endpoint so new/revoked assignments
    // update navigation without requiring a page reload.
    const timer = window.setInterval(refreshWorkerStatus, 30000);
    return () => window.clearInterval(timer);
  }, [isAdmin, refreshWorkerStatus]);

  // Refresh nav when worker assignments change
  useWebSocket(['workers'], () => { refreshWorkerStatus(); });

  const refreshPlanAttention = useCallback(() => {
    void api.countPlans({ display_state: 'waiting_user,awaiting_review,approved' })
      .then((result) => setHasPlanActions(result.total > 0))
      .catch(() => {});
  }, []);

  useEffect(() => {
    refreshPlanAttention();
    const timer = window.setInterval(refreshPlanAttention, 30000);
    return () => window.clearInterval(timer);
  }, [refreshPlanAttention]);
  useWebSocket(['plans'], refreshPlanAttention);

  const refreshDeliveryAttention = useCallback(() => {
    void api.countDeliveryAttention()
      .then((result) => setHasDeliveryActions(result.total > 0))
      .catch(() => {});
  }, []);

  useEffect(() => {
    refreshDeliveryAttention();
    const timer = window.setInterval(refreshDeliveryAttention, 30000);
    return () => window.clearInterval(timer);
  }, [refreshDeliveryAttention]);
  useWebSocket(['deliveries'], refreshDeliveryAttention);

  const allPages: NavItem[] = [
    { key: 'dashboard', label: 'Dashboard', icon: LayoutDashboard, show: isAdmin },
    { key: 'tasks', label: 'Tasks', icon: ListTodo, show: true },
    { key: 'plans', label: 'Plans', icon: ScrollText, show: true },
    { key: 'delivery', label: 'Delivery', icon: GitBranch, show: true },
    { key: 'projects', label: 'Projects', icon: FolderGit2, show: true },
    // These pages expose Manager-host data rather than per-tenant resources.
    // Their backend routers are admin-only, so do not advertise dead links to
    // members (task attachments continue to use the scoped uploads API).
    { key: 'secrets', label: 'Secrets', icon: KeyRound, show: isAdmin },
    { key: 'files', label: 'Files', icon: FolderOpen, show: isAdmin },
    { key: 'discussions', label: 'Discussions', icon: MessagesSquare, show: true },
    { key: 'pr-monitor', label: 'PR Monitor', icon: GitPullRequest, show: isAdmin || hasWorker },
    { key: 'workers', label: 'Workers', icon: Server, show: isAdmin || hasWorker },
    { key: 'skills', label: 'Skills', icon: Sparkles, show: true },
    { key: 'team', label: 'Team', icon: Users, show: true },
    { key: 'settings', label: 'Settings', icon: Settings, show: true },
    ...(isCapacitor() ? [{ key: 'server', label: 'Server', icon: Globe, show: true }] : []),
  ];
  const pages = allPages.filter(p => p.show);
  const current = pages.find(p => p.key === currentPage);

  const navigate = (key: string) => {
    onNavigate(key);
    setDrawerOpen(false);
  };

  const toggleNavCollapsed = () => {
    setNavCollapsed(collapsed => {
      const next = !collapsed;
      localStorage.setItem('ccm-nav-collapsed', String(next));
      return next;
    });
  };

  const brand = (
    <div className="flex items-center gap-2.5 min-w-0">
      <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-lg bg-gradient-to-br from-indigo-500 to-indigo-700 text-white shadow-md shadow-indigo-600/25">
        <Bot size={18} />
      </div>
      <span data-shell-brand-text className="text-sm font-semibold tracking-tight text-foreground truncate">Claude Manager</span>
    </div>
  );

  const navList = (
    <nav className="flex-1 overflow-y-auto px-3 py-3 space-y-0.5">
      {pages.map((p) => {
        const active = currentPage === p.key;
        const Icon = p.icon;
        return (
          <button
            key={p.key}
            data-nav-item
            data-active={active}
            onClick={() => navigate(p.key)}
            className={`w-full flex items-center gap-2.5 px-3 py-2 rounded-lg text-sm font-medium transition-colors text-left ${
              active
                ? 'bg-indigo-600/15 text-indigo-300'
                : 'text-gray-400 hover:text-gray-200 hover:bg-gray-800/70'
            }`}
          >
            <span data-nav-icon className="relative inline-flex shrink-0 items-center justify-center">
              {(() => {
                const themed = getNavIcon(iconSet, p.key);
                return themed ? (
                  <span data-icon-set={iconSet} className="contents">
                    {themed({ size: 16, active })}
                  </span>
                ) : (
                  <Icon size={16} className={active ? 'text-indigo-400' : 'text-gray-500'} />
                );
              })()}
              {p.key === 'plans' && hasPlanActions && (
                <span
                  aria-label="Plans requiring action"
                  className="absolute -right-1 -top-1 h-2 w-2 rounded-full bg-red-500 ring-2 ring-red-500/20"
                />
              )}
              {p.key === 'delivery' && hasDeliveryActions && (
                <span
                  aria-label="Deliveries requiring action"
                  className="absolute -right-1 -top-1 h-2 w-2 rounded-full bg-amber-400 ring-2 ring-amber-400/20"
                />
              )}
            </span>
            {p.label}
          </button>
        );
      })}
    </nav>
  );

  const userFooter = ccUser.name ? (
    <div data-shell-user-footer className="border-t border-gray-800 px-4 py-3 flex items-center gap-2.5">
      <div className="flex h-7 w-7 shrink-0 items-center justify-center rounded-full bg-gray-800 text-xs font-semibold text-gray-300 uppercase">
        {String(ccUser.name).slice(0, 1)}
      </div>
      <div data-shell-user-meta className="min-w-0">
        <p className="text-xs font-medium text-gray-300 truncate">{ccUser.name}</p>
        {ccUser.role && <p className="text-[10px] text-gray-500 truncate">{ccUser.role}</p>}
      </div>
    </div>
  ) : null;

  const collapseButton = (
    <button
      type="button"
      onClick={toggleNavCollapsed}
      className="hidden lg:inline-flex shrink-0 p-1.5 rounded-md text-gray-500 hover:text-gray-200 hover:bg-gray-800 transition-colors"
      aria-label="收起导航"
      title="收起导航"
    >
      <PanelLeftClose size={17} />
    </button>
  );

  return (
    <div className="min-h-screen bg-gray-900 text-foreground overflow-x-clip">
      {/* 桌面侧栏 */}
      {!navCollapsed && (navResizable ? (
        <>
          <aside data-shell-sidebar className="hidden lg:flex fixed inset-y-0 left-0 z-40 flex-col bg-gray-950 border-r border-gray-800" style={{ width: navWidth }}>
            <div data-shell-brand-row className="h-14 shrink-0 flex items-center justify-between gap-2 px-4 border-b border-gray-800/70">
              {brand}
              {collapseButton}
            </div>
            {navList}
            {userFooter}
          </aside>
          <div
            onMouseDown={handleNavDragStart}
            className="hidden lg:block fixed inset-y-0 z-40 w-1 cursor-col-resize bg-transparent hover:bg-indigo-500/40 active:bg-indigo-500/60 transition-colors"
            style={{ left: navWidth }}
          />
        </>
      ) : (
        <aside data-shell-sidebar className="hidden lg:flex fixed inset-y-0 left-0 z-40 w-60 flex-col bg-gray-950 border-r border-gray-800">
          <div data-shell-brand-row className="h-14 shrink-0 flex items-center justify-between gap-2 px-4 border-b border-gray-800/70">
            {brand}
            {collapseButton}
          </div>
          {navList}
          {userFooter}
        </aside>
      ))}

      {/* 移动端抽屉 */}
      {drawerOpen && (
        <div className="lg:hidden fixed inset-0 z-50">
          <div className="absolute inset-0 bg-black/60 backdrop-blur-sm animate-fade-in" onClick={() => setDrawerOpen(false)} />
          <aside className="absolute inset-y-0 left-0 w-72 max-w-[85vw] flex flex-col bg-gray-950 border-r border-gray-800 pt-[env(safe-area-inset-top)] animate-slide-in-left">
            <div className="h-14 shrink-0 flex items-center justify-between pl-4 pr-2 border-b border-gray-800/70">
              {brand}
              <button
                onClick={() => setDrawerOpen(false)}
                className="p-2 rounded-lg text-gray-400 hover:text-foreground hover:bg-gray-800 transition-colors"
              >
                <X size={18} />
              </button>
            </div>
            {navList}
            {userFooter}
          </aside>
        </div>
      )}

      {/* 右侧主列：sticky 顶栏 + 页面内容 */}
      <div data-shell-main className={`flex flex-col min-h-screen ${!navCollapsed && !navResizable ? 'lg:pl-60' : ''}`} style={!navCollapsed && navResizable && isLg ? { paddingLeft: navWidth } : undefined}>
        <header className="sticky top-0 z-30 bg-gray-900 border-b border-gray-800 pt-[env(safe-area-inset-top)]">
          <div className="h-12 flex items-center gap-2 px-3 sm:px-4">
            <button
              onClick={() => setDrawerOpen(true)}
              className="lg:hidden p-2 -ml-1 rounded-lg text-gray-400 hover:text-foreground hover:bg-gray-800 transition-colors"
              aria-label="打开导航"
            >
              <Menu size={18} />
            </button>
            <div className="lg:hidden flex items-center gap-2 min-w-0">
              <span className="text-sm font-semibold tracking-tight text-foreground truncate">
                {current?.label ?? 'Claude Manager'}
              </span>
            </div>
            {navCollapsed && (
              <button
                type="button"
                onClick={toggleNavCollapsed}
                className="hidden lg:inline-flex p-1.5 -ml-1 rounded-md text-gray-400 hover:text-foreground hover:bg-gray-800 transition-colors"
                aria-label="展开导航"
                title="展开导航"
              >
                <PanelLeftOpen size={18} />
              </button>
            )}
            <span className="hidden lg:block text-sm font-semibold tracking-tight text-foreground">
              {current?.label ?? ''}
            </span>
            <div className="ml-auto flex items-center gap-1">
              {ccUser.name && (
                <span className="text-xs text-gray-400 mr-1 hidden sm:inline">{ccUser.name}</span>
              )}
              {isAdmin && <UpdateButton />}
              {isAdmin && <PoolDrawer />}
            </div>
          </div>
        </header>
        <main className={`flex-1 w-full ${wide ? 'max-w-none' : 'mx-auto max-w-6xl'} p-4`}>
          {children}
        </main>
      </div>
    </div>
  );
}
