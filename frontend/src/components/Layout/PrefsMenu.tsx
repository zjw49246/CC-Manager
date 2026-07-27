import { useCallback, useEffect, useRef, useState } from 'react';
import { Palette, Globe, Settings, LogOut, KeyRound, Image as ImageIcon } from '../icons';
import { api, clearToken } from '../../api/client';
import type { RuntimeSettings } from '../../api/client';
import { getTheme, setTheme as persistTheme, THEME_OPTIONS, type Theme } from '../../config/theme';
import { getCustomColors, setCustomColors, hasBgImage, getBgVisible, setBgVisible } from '../../config/customTheme';
import { importBgImage, clearBgImage } from '../../config/customBg';
import { getTimezone, setTimezone, TIMEZONE_OPTIONS } from '../../config/timezone';

/** 顶栏齿轮下拉：时区 / 主题 / PTY / 访问置顶 / 压缩阈值 / 飞书 / 密码 / 退出。
 * 低频设置集中收纳，保持顶栏精简。 */
export function PrefsMenu({ isAdmin }: { isAdmin: boolean }) {
  const [theme, setTheme] = useState(getTheme());
  const [custom, setCustom] = useState(getCustomColors());
  const [bgOn, setBgOn] = useState(hasBgImage());
  const [bgBusy, setBgBusy] = useState(false);
  const [bgVisible, setBgVisibleState] = useState(getBgVisible());
  const bgInputRef = useRef<HTMLInputElement>(null);
  const [tz, setTz] = useState(getTimezone());
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  const [runtime, setRuntime] = useState<RuntimeSettings | null>(null);
  const [switching, setSwitching] = useState(false);
  const [feishuStatus, setFeishuStatus] = useState<{ bound: boolean; name?: string; avatar_url?: string } | null>(null);

  const ccUser = JSON.parse(localStorage.getItem('cc_user') || '{}');

  useEffect(() => {
    if (!open) return;
    const handle = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener('mousedown', handle);
    return () => document.removeEventListener('mousedown', handle);
  }, [open]);

  useEffect(() => {
    api.getRuntimeSettings().then(setRuntime).catch(() => setRuntime(null));
    api.getFeishuStatus().then(setFeishuStatus).catch(() => {});
  }, []);

  const togglePtyMode = useCallback(async () => {
    if (!runtime || switching || !runtime.pty_available) return;
    if (runtime.use_pty_mode) {
      const ok = window.confirm('关闭 PTY 模式将回退到 claude -p 一次性进程，新任务不再复用会话。确定关闭？');
      if (!ok) return;
    }
    setSwitching(true);
    try {
      const updated = await api.updateRuntimeSettings({ use_pty_mode: !runtime.use_pty_mode });
      setRuntime(updated);
    } catch {
      // keep previous state on failure
    } finally {
      setSwitching(false);
    }
  }, [runtime, switching]);

  const toggleAutoSort = useCallback(async () => {
    if (!runtime || switching) return;
    setSwitching(true);
    try {
      const updated = await api.updateRuntimeSettings({ auto_sort_on_access: !runtime.auto_sort_on_access });
      setRuntime(updated);
    } catch { /* keep previous state */ } finally {
      setSwitching(false);
    }
  }, [runtime, switching]);

  const changeCompactThreshold = useCallback(async (value: number) => {
    if (!runtime || switching) return;
    setSwitching(true);
    try {
      const updated = await api.updateRuntimeSettings({ context_compact_threshold: value });
      setRuntime(updated);
    } catch { /* keep previous state */ } finally {
      setSwitching(false);
    }
  }, [runtime, switching]);

  const handleThemeChange = (next: Theme) => {
    persistTheme(next);
    setTheme(next);
  };

  const applyCustom = (bg: string, brand: string) => {
    setCustom({ bg, brand });
    setCustomColors(bg, brand);
    persistTheme('custom');  // 重算色阶并落盘
    setTheme('custom');
  };

  const handleCustomColor = (key: 'bg' | 'brand', value: string) =>
    applyCustom(key === 'bg' ? value : custom.bg, key === 'brand' ? value : custom.brand);

  /** 上传背景图：缩放存 IDB + 取色回填两个取色器（之后仍可手动微调）。 */
  const handleBgUpload = async (file: File) => {
    setBgBusy(true);
    try {
      const { bg, brand } = await importBgImage(file);
      setBgOn(true);
      applyCustom(bg, brand);
    } catch {
      alert('图片读取失败，请换一张试试');
    } finally {
      setBgBusy(false);
      if (bgInputRef.current) bgInputRef.current.value = '';  // 允许重传同一文件
    }
  };

  const handleBgClear = async () => {
    await clearBgImage();
    setBgOn(false);
    persistTheme('custom');  // 重算：去掉表面档位的 alpha
  };

  const handleBgVisible = (v: number) => {
    setBgVisibleState(v);
    setBgVisible(v);
    persistTheme('custom');  // 重算表面档位 alpha（实时预览）
  };

  const selectCls = 'bg-gray-700 text-gray-200 text-xs rounded-md px-2 py-1 border border-gray-600 focus:outline-none focus:ring-1 focus:ring-indigo-500 cursor-pointer';

  const toggleCls = (on: boolean) =>
    `relative inline-flex h-4 w-8 items-center rounded-full transition-colors disabled:opacity-50 ${on ? 'bg-green-500' : 'bg-gray-600'}`;
  const knobCls = (on: boolean) =>
    `inline-block h-3 w-3 transform rounded-full bg-white transition-transform ${on ? 'translate-x-4' : 'translate-x-1'}`;

  return (
    <div className="relative shrink-0" ref={ref}>
      <button
        onClick={() => setOpen(!open)}
        className={`p-2 rounded-lg transition-colors ${open ? 'text-foreground bg-gray-800' : 'text-gray-400 hover:text-foreground hover:bg-gray-800'}`}
        title="偏好设置（时区 / 主题）"
      >
        <Settings size={18} />
      </button>
      {open && (
        <div className="absolute top-full right-0 mt-2 bg-gray-800 border border-gray-700 rounded-xl shadow-2xl shadow-black/20 z-30 p-3 min-w-[230px] space-y-3">
          {isAdmin && runtime && (
            <div
              className="flex items-center justify-between gap-3"
              title={
                !runtime.pty_available
                  ? 'claude_pty 未安装，PTY 模式不可用'
                  : runtime.use_pty_mode
                    ? 'PTY 常驻会话模式：开（多轮免冷启动；切换仅影响新任务）'
                    : 'PTY 常驻会话模式：关（使用 claude -p 一次性进程）'
              }
            >
              <span className={`text-xs flex items-center gap-1.5 ${runtime.use_pty_mode ? 'text-green-400' : 'text-gray-400'}`}>
                PTY 模式
              </span>
              <button
                onClick={togglePtyMode}
                disabled={!runtime.pty_available || switching}
                className={toggleCls(runtime.use_pty_mode)}
              >
                <span className={knobCls(runtime.use_pty_mode)} />
              </button>
            </div>
          )}
          {isAdmin && runtime && (
            <div
              data-testid="codex-main-mcp-status"
              className="flex items-center justify-between gap-3"
              title="只读运行时 capability；可用 CODEX_MAIN_MCP_ENABLED=false 紧急关闭"
            >
              <span className="text-xs text-gray-400">Codex 主任务 MCP</span>
              <span className={`text-xs font-medium ${runtime.codex_main_mcp_enabled ? 'text-green-400' : 'text-gray-500'}`}>
                {runtime.codex_main_mcp_enabled ? '已启用' : '已关闭'}
              </span>
            </div>
          )}
          <div className="flex items-center justify-between gap-3">
            <span className="text-xs text-gray-400 flex items-center gap-1.5"><Globe size={13} /> 时区</span>
            <select
              value={tz}
              onChange={(e) => { setTimezone(e.target.value); setTz(e.target.value); }}
              className={selectCls}
            >
              {TIMEZONE_OPTIONS.map((opt) => (
                <option key={opt.value} value={opt.value}>{opt.label}</option>
              ))}
            </select>
          </div>
          <div className="flex items-center justify-between gap-3">
            <span className="text-xs text-gray-400 flex items-center gap-1.5"><Palette size={13} /> 主题</span>
            <select
              value={theme}
              onChange={(e) => handleThemeChange(e.target.value as Theme)}
              className={selectCls}
            >
              <optgroup label="现代">
                {THEME_OPTIONS.filter((o) => o.group === 'modern').map((opt) => (
                  <option key={opt.value} value={opt.value}>{opt.label}</option>
                ))}
              </optgroup>
              <optgroup label="Legacy">
                {THEME_OPTIONS.filter((o) => o.group === 'legacy').map((opt) => (
                  <option key={opt.value} value={opt.value}>{opt.label}</option>
                ))}
              </optgroup>
              <optgroup label="自定义">
                {THEME_OPTIONS.filter((o) => o.group === 'custom').map((opt) => (
                  <option key={opt.value} value={opt.value}>{opt.label}</option>
                ))}
              </optgroup>
            </select>
          </div>
          {theme === 'custom' && (
            <>
              <div className="flex items-center justify-between gap-3 pl-4">
                <span className="text-xs text-gray-500">背景 / 品牌色</span>
                <div className="flex items-center gap-1.5">
                  <input
                    type="color"
                    value={custom.bg}
                    onChange={(e) => handleCustomColor('bg', e.target.value)}
                    className="h-6 w-8 rounded border border-gray-600 bg-transparent cursor-pointer"
                    title="背景色：决定整套中性色与明暗"
                  />
                  <input
                    type="color"
                    value={custom.brand}
                    onChange={(e) => handleCustomColor('brand', e.target.value)}
                    className="h-6 w-8 rounded border border-gray-600 bg-transparent cursor-pointer"
                    title="品牌色：决定按钮/选中态/链接色"
                  />
                </div>
              </div>
              <div className="flex items-center justify-between gap-3 pl-4">
                <span className="text-xs text-gray-500 flex items-center gap-1.5">
                  <ImageIcon size={12} /> 背景图
                </span>
                <div className="flex items-center gap-1.5">
                  <input
                    ref={bgInputRef}
                    type="file"
                    accept="image/*"
                    onChange={(e) => { const f = e.target.files?.[0]; if (f) void handleBgUpload(f); }}
                    className="hidden"
                  />
                  <button
                    onClick={() => bgInputRef.current?.click()}
                    disabled={bgBusy}
                    className="text-xs px-2 py-1 rounded-md bg-gray-700 text-gray-200 border border-gray-600 hover:bg-gray-600 disabled:opacity-50 transition-colors"
                    title="上传后自动从图中取色，界面会半透明透出背景图"
                  >
                    {bgBusy ? '处理中…' : bgOn ? '更换' : '上传'}
                  </button>
                  {bgOn && (
                    <button
                      onClick={() => void handleBgClear()}
                      className="text-xs px-2 py-1 rounded-md text-gray-400 hover:text-red-400 transition-colors"
                      title="移除背景图"
                    >
                      移除
                    </button>
                  )}
                </div>
              </div>
              {bgOn && (
                <div className="flex items-center justify-between gap-3 pl-4">
                  <span className="text-xs text-gray-500">背景图强度</span>
                  <div className="flex items-center gap-2">
                    <input
                      type="range"
                      min={0}
                      max={100}
                      value={bgVisible}
                      onChange={(e) => handleBgVisible(Number(e.target.value))}
                      className="w-28 accent-indigo-500 cursor-pointer"
                      title="向右：背景图更明显；向左：界面更实"
                    />
                    <span className="text-[10px] text-gray-500 tabular-nums w-8 text-right">{bgVisible}%</span>
                  </div>
                </div>
              )}
            </>
          )}
          {runtime && (
            <div className="flex items-center justify-between gap-3">
              <span className="text-xs text-gray-400">访问置顶</span>
              <button
                onClick={toggleAutoSort}
                disabled={switching}
                className={toggleCls(runtime.auto_sort_on_access)}
                title={runtime.auto_sort_on_access ? '开启：打开聊天自动置顶任务' : '关闭：打开聊天不改变排序'}
              >
                <span className={knobCls(runtime.auto_sort_on_access)} />
              </button>
            </div>
          )}
          {runtime && (
            <div className="flex items-center justify-between gap-3">
              <span
                className="text-xs text-gray-400"
                title="会话上下文利用率达到该比例时自动压缩摘要并换新 session。过高会让超大 context 的请求在服务端易挂起"
              >
                压缩阈值
              </span>
              <select
                value={Math.round(runtime.context_compact_threshold * 100)}
                onChange={(e) => changeCompactThreshold(Number(e.target.value) / 100)}
                disabled={switching}
                className={`${selectCls} disabled:opacity-50`}
              >
                {Array.from(new Set([60, 70, 75, 80, 85, 90, Math.round(runtime.context_compact_threshold * 100)]))
                  .sort((a, b) => a - b)
                  .map((pct) => (
                    <option key={pct} value={pct}>{pct}%</option>
                  ))}
              </select>
            </div>
          )}
          {/* Feishu binding */}
          <div className="border-t border-gray-700 pt-2 mt-1">
            <div className="flex items-center justify-between gap-3">
              <span className="text-xs text-gray-400">飞书</span>
              {feishuStatus?.bound ? (
                <div className="flex items-center gap-1.5">
                  {feishuStatus.avatar_url && <img src={feishuStatus.avatar_url} className="w-4 h-4 rounded-full" alt="" />}
                  <span className="text-xs text-gray-300">{feishuStatus.name}</span>
                  <button
                    onClick={async () => { if (confirm('解绑飞书？')) { await api.unbindFeishu(); setFeishuStatus({ bound: false }); } }}
                    className="text-xs text-red-400 hover:text-red-300 ml-1"
                  >解绑</button>
                </div>
              ) : feishuStatus !== null ? (
                <button
                  onClick={async () => { const { url } = await api.getFeishuAuthUrl(); window.location.href = url; }}
                  className="text-xs px-2 py-0.5 rounded-md bg-blue-600/20 text-blue-300 hover:bg-blue-600/30"
                >绑定</button>
              ) : null}
            </div>
          </div>
          {/* Change password */}
          {ccUser.id && (
            <div className="border-t border-gray-700 pt-2 mt-1">
              <button
                onClick={() => {
                  const oldPwd = prompt('当前密码：');
                  if (!oldPwd) return;
                  const newPwd = prompt('新密码：');
                  if (!newPwd) return;
                  fetch('/api/auth/me/password', {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${localStorage.getItem('cc_token')}` },
                    body: JSON.stringify({ old_password: oldPwd, new_password: newPwd }),
                  }).then(r => {
                    if (r.ok) alert('密码修改成功');
                    else r.json().then(d => alert(d.detail || '修改失败'));
                  }).catch(() => alert('修改失败'));
                }}
                className="flex items-center gap-1.5 text-xs text-gray-400 hover:text-gray-200 transition-colors w-full"
              >
                <KeyRound size={12} /> 修改密码
              </button>
            </div>
          )}
          {/* Logout */}
          <div className="border-t border-gray-700 pt-2 mt-1">
            <button
              onClick={() => { clearToken(); localStorage.removeItem('cc_user'); window.location.reload(); }}
              className="flex items-center gap-1.5 text-xs text-red-400 hover:text-red-300 transition-colors w-full"
            >
              <LogOut size={12} /> 退出登录
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
