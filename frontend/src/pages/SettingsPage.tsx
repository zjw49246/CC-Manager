import { useCallback, useEffect, useRef, useState } from 'react';

import { api, clearToken } from '../api/client';
import type {
  CapacitySettings,
  PlanPipelineConfig,
  RuntimeSettings,
  SystemConfig,
} from '../api/client';
import {
  Check,
  Globe,
  Image as ImageIcon,
  KeyRound,
  Loader2,
  LogOut,
  Palette,
  Settings,
} from '../components/icons';
import { PlanPipelineFields } from '../components/PlanReview/PlanPipelineFields';
import { FALLBACK_PLAN_PIPELINE_CONFIG } from '../components/PlanReview/planPipelineDefaults';
import { clearBgImage, importBgImage } from '../config/customBg';
import {
  getBgVisible,
  getCustomColors,
  hasBgImage,
  setBgVisible,
  setCustomColors,
} from '../config/customTheme';
import { getTheme, setTheme as persistTheme, THEME_OPTIONS, type Theme } from '../config/theme';
import { getTimezone, setTimezone, TIMEZONE_OPTIONS } from '../config/timezone';

const selectClassName = 'rounded-lg border border-gray-700 bg-gray-950 px-3 py-2 text-sm text-gray-100 outline-none focus:border-indigo-500 disabled:opacity-50';

const toggleClassName = (on: boolean) =>
  `relative inline-flex h-5 w-10 items-center rounded-full transition-colors disabled:opacity-50 ${on ? 'bg-green-500' : 'bg-gray-700'}`;

const toggleKnobClassName = (on: boolean) =>
  `inline-block h-4 w-4 transform rounded-full bg-white transition-transform ${on ? 'translate-x-5' : 'translate-x-1'}`;

export function SettingsPage() {
  const ccUser = JSON.parse(localStorage.getItem('cc_user') || '{}');
  const isAdmin = ccUser.role === 'admin' || ccUser.role === 'super_admin' || !ccUser.id;

  const [theme, setTheme] = useState(getTheme());
  const [custom, setCustom] = useState(getCustomColors());
  const [bgOn, setBgOn] = useState(hasBgImage());
  const [bgBusy, setBgBusy] = useState(false);
  const [bgVisible, setBgVisibleState] = useState(getBgVisible());
  const bgInputRef = useRef<HTMLInputElement>(null);
  const [timezone, setTimezoneValue] = useState(getTimezone());
  const [feishuStatus, setFeishuStatus] = useState<{
    bound: boolean;
    name?: string;
    avatar_url?: string;
  } | null>(null);

  const [runtime, setRuntime] = useState<RuntimeSettings | null>(null);
  const [runtimeLoading, setRuntimeLoading] = useState(isAdmin);
  const [runtimeSaving, setRuntimeSaving] = useState(false);
  const [runtimeError, setRuntimeError] = useState<string | null>(null);

  const [systemConfig, setSystemConfig] = useState<SystemConfig | null>(null);
  const [pipeline, setPipeline] = useState<PlanPipelineConfig>(
    FALLBACK_PLAN_PIPELINE_CONFIG,
  );
  const [loading, setLoading] = useState(isAdmin);
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [capacity, setCapacity] = useState<CapacitySettings | null>(null);
  const [capacityInput, setCapacityInput] = useState('');
  const [capacitySaving, setCapacitySaving] = useState(false);
  const [capacitySaved, setCapacitySaved] = useState(false);
  const [capacityError, setCapacityError] = useState<string | null>(null);

  useEffect(() => {
    api.getFeishuStatus().then(setFeishuStatus).catch(() => setFeishuStatus({ bound: false }));
  }, []);

  useEffect(() => {
    if (!isAdmin) return;
    api.getRuntimeSettings()
      .then(setRuntime)
      .catch((reason) => {
        setRuntimeError(reason instanceof Error ? reason.message : String(reason));
      })
      .finally(() => setRuntimeLoading(false));
  }, [isAdmin]);

  useEffect(() => {
    if (!isAdmin) return;
    Promise.all([
      api.config(),
      api.getPlanPipelineSettings(),
      api.getCapacitySettings(),
    ])
      .then(([config, persisted, currentCapacity]) => {
        setSystemConfig(config);
        setPipeline(persisted);
        setCapacity(currentCapacity);
        setCapacityInput(String(currentCapacity.max_concurrent_instances));
      })
      .catch((reason) => {
        setError(reason instanceof Error ? reason.message : String(reason));
      })
      .finally(() => setLoading(false));
  }, [isAdmin]);

  const updateRuntime = useCallback(async (
    update: Partial<Pick<
      RuntimeSettings,
      'use_pty_mode' | 'auto_sort_on_access' | 'context_compact_threshold'
    >>,
  ) => {
    if (!runtime || runtimeSaving) return;
    setRuntimeSaving(true);
    setRuntimeError(null);
    try {
      setRuntime(await api.updateRuntimeSettings(update));
    } catch (reason) {
      setRuntimeError(reason instanceof Error ? reason.message : String(reason));
    } finally {
      setRuntimeSaving(false);
    }
  }, [runtime, runtimeSaving]);

  const togglePtyMode = useCallback(async () => {
    if (!runtime || runtimeSaving || !runtime.pty_available) return;
    if (runtime.use_pty_mode) {
      const confirmed = window.confirm('关闭 PTY 模式将回退到 claude -p 一次性进程，新任务不再复用会话。确定关闭？');
      if (!confirmed) return;
    }
    await updateRuntime({ use_pty_mode: !runtime.use_pty_mode });
  }, [runtime, runtimeSaving, updateRuntime]);

  const handleThemeChange = (next: Theme) => {
    persistTheme(next);
    setTheme(next);
  };

  const applyCustom = (background: string, brand: string) => {
    setCustom({ bg: background, brand });
    setCustomColors(background, brand);
    persistTheme('custom');
    setTheme('custom');
  };

  const handleCustomColor = (key: 'bg' | 'brand', value: string) => {
    applyCustom(key === 'bg' ? value : custom.bg, key === 'brand' ? value : custom.brand);
  };

  const handleBgUpload = async (file: File) => {
    setBgBusy(true);
    try {
      const colors = await importBgImage(file);
      setBgOn(true);
      applyCustom(colors.bg, colors.brand);
    } catch {
      window.alert('图片读取失败，请换一张试试');
    } finally {
      setBgBusy(false);
      if (bgInputRef.current) bgInputRef.current.value = '';
    }
  };

  const handleBgClear = async () => {
    await clearBgImage();
    setBgOn(false);
    persistTheme('custom');
  };

  const handleBgVisible = (value: number) => {
    setBgVisibleState(value);
    setBgVisible(value);
    persistTheme('custom');
  };

  const save = async () => {
    setSaving(true);
    setSaved(false);
    setError(null);
    try {
      const persisted = await api.updatePlanPipelineSettings(pipeline);
      setPipeline(persisted);
      setSaved(true);
      window.setTimeout(() => setSaved(false), 2000);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : String(reason));
    } finally {
      setSaving(false);
    }
  };

  const saveCapacity = async (restoreDefault = false) => {
    const parsed = Number(capacityInput);
    if (!restoreDefault && (!Number.isInteger(parsed) || parsed < 1 || parsed > 64)) {
      setCapacityError('Concurrency must be a whole number between 1 and 64.');
      return;
    }
    if (
      !restoreDefault
      && capacity
      && parsed < capacity.active_instances
      && !window.confirm(
        `There are ${capacity.active_instances} active tasks. They will keep running, and new work will wait until usage falls below ${parsed}. Continue?`,
      )
    ) return;

    setCapacitySaving(true);
    setCapacitySaved(false);
    setCapacityError(null);
    try {
      const updated = await api.updateCapacitySettings(restoreDefault ? null : parsed);
      setCapacity(updated);
      setCapacityInput(String(updated.max_concurrent_instances));
      setCapacitySaved(true);
      window.setTimeout(() => setCapacitySaved(false), 2000);
    } catch (reason) {
      setCapacityError(reason instanceof Error ? reason.message : String(reason));
    } finally {
      setCapacitySaving(false);
    }
  };

  return (
    <div className="mx-auto max-w-4xl space-y-5">
      <div className="flex items-center gap-2">
        <Settings size={20} className="text-indigo-400" />
        <div>
          <h2 className="text-lg font-semibold text-foreground">Settings</h2>
          <p className="text-xs text-gray-500">Manage appearance, account, and Manager behavior in one place.</p>
        </div>
      </div>

      <section className="rounded-xl border border-gray-800 bg-gray-900/70 p-5 shadow-sm">
        <div className="mb-4">
          <h3 className="flex items-center gap-2 text-sm font-semibold text-gray-100">
            <Palette size={15} className="text-indigo-400" /> Appearance &amp; locale
          </h3>
          <p className="mt-1 text-xs leading-5 text-gray-500">
            These preferences are stored in this browser and apply immediately.
          </p>
        </div>

        <div className="grid gap-4 sm:grid-cols-2">
          <label>
            <span className="mb-1.5 flex items-center gap-1.5 text-xs font-medium text-gray-300">
              <Globe size={13} /> 时区
            </span>
            <select
              aria-label="时区"
              value={timezone}
              onChange={(event) => {
                setTimezone(event.target.value);
                setTimezoneValue(event.target.value);
              }}
              className={`${selectClassName} w-full`}
            >
              {TIMEZONE_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>{option.label}</option>
              ))}
            </select>
          </label>
          <label>
            <span className="mb-1.5 flex items-center gap-1.5 text-xs font-medium text-gray-300">
              <Palette size={13} /> 主题
            </span>
            <select
              aria-label="主题"
              value={theme}
              onChange={(event) => handleThemeChange(event.target.value as Theme)}
              className={`${selectClassName} w-full`}
            >
              <optgroup label="现代">
                {THEME_OPTIONS.filter((option) => option.group === 'modern').map((option) => (
                  <option key={option.value} value={option.value}>{option.label}</option>
                ))}
              </optgroup>
              <optgroup label="Legacy">
                {THEME_OPTIONS.filter((option) => option.group === 'legacy').map((option) => (
                  <option key={option.value} value={option.value}>{option.label}</option>
                ))}
              </optgroup>
              <optgroup label="自定义">
                {THEME_OPTIONS.filter((option) => option.group === 'custom').map((option) => (
                  <option key={option.value} value={option.value}>{option.label}</option>
                ))}
              </optgroup>
            </select>
          </label>
        </div>

        {theme === 'custom' && (
          <div className="mt-4 space-y-4 rounded-lg border border-gray-800 bg-gray-950/50 p-4">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <span className="text-xs font-medium text-gray-300">背景 / 品牌色</span>
              <div className="flex items-center gap-2">
                <label className="flex items-center gap-1.5 text-xs text-gray-500">
                  背景
                  <input
                    aria-label="背景色"
                    type="color"
                    value={custom.bg}
                    onChange={(event) => handleCustomColor('bg', event.target.value)}
                    className="h-8 w-10 cursor-pointer rounded border border-gray-700 bg-transparent"
                  />
                </label>
                <label className="flex items-center gap-1.5 text-xs text-gray-500">
                  品牌
                  <input
                    aria-label="品牌色"
                    type="color"
                    value={custom.brand}
                    onChange={(event) => handleCustomColor('brand', event.target.value)}
                    className="h-8 w-10 cursor-pointer rounded border border-gray-700 bg-transparent"
                  />
                </label>
              </div>
            </div>
            <div className="flex flex-wrap items-center justify-between gap-3">
              <span className="flex items-center gap-1.5 text-xs font-medium text-gray-300">
                <ImageIcon size={13} /> 背景图
              </span>
              <div className="flex items-center gap-2">
                <input
                  ref={bgInputRef}
                  aria-label="上传背景图"
                  type="file"
                  accept="image/*"
                  onChange={(event) => {
                    const file = event.target.files?.[0];
                    if (file) void handleBgUpload(file);
                  }}
                  className="hidden"
                />
                <button
                  type="button"
                  onClick={() => bgInputRef.current?.click()}
                  disabled={bgBusy}
                  className="rounded-lg border border-gray-700 bg-gray-800 px-3 py-1.5 text-xs text-gray-200 hover:bg-gray-700 disabled:opacity-50"
                >
                  {bgBusy ? '处理中…' : bgOn ? '更换' : '上传'}
                </button>
                {bgOn && (
                  <button
                    type="button"
                    onClick={() => void handleBgClear()}
                    className="rounded-lg px-3 py-1.5 text-xs text-gray-400 hover:bg-red-500/10 hover:text-red-300"
                  >
                    移除
                  </button>
                )}
              </div>
            </div>
            {bgOn && (
              <label className="flex flex-wrap items-center justify-between gap-3">
                <span className="text-xs font-medium text-gray-300">背景图强度</span>
                <div className="flex items-center gap-2">
                  <input
                    aria-label="背景图强度"
                    type="range"
                    min={0}
                    max={100}
                    value={bgVisible}
                    onChange={(event) => handleBgVisible(Number(event.target.value))}
                    className="w-40 cursor-pointer accent-indigo-500"
                  />
                  <span className="w-9 text-right text-xs tabular-nums text-gray-500">{bgVisible}%</span>
                </div>
              </label>
            )}
          </div>
        )}
      </section>

      {isAdmin && (
        <section className="rounded-xl border border-gray-800 bg-gray-900/70 p-5 shadow-sm">
          <div className="mb-4">
            <h3 className="text-sm font-semibold text-gray-100">Runtime &amp; task behavior</h3>
            <p className="mt-1 text-xs leading-5 text-gray-500">
              Process-wide settings for new task turns on this Manager.
            </p>
          </div>

          {runtimeLoading ? (
            <div className="flex items-center gap-2 py-6 text-sm text-gray-500">
              <Loader2 size={15} className="animate-spin" /> Loading runtime settings…
            </div>
          ) : runtime ? (
            <div className="space-y-4">
              <div
                className="flex items-center justify-between gap-4 rounded-lg border border-gray-800 bg-gray-950/50 px-4 py-3"
                title={!runtime.pty_available
                  ? 'claude_pty 未安装，PTY 模式不可用'
                  : '切换只影响之后启动的新任务'}
              >
                <div>
                  <p className="text-sm font-medium text-gray-200">PTY 模式</p>
                  <p className="mt-0.5 text-xs text-gray-500">复用常驻 Claude 会话，减少多轮任务冷启动。</p>
                </div>
                <button
                  type="button"
                  aria-label="PTY 模式"
                  aria-pressed={runtime.use_pty_mode}
                  onClick={() => void togglePtyMode()}
                  disabled={!runtime.pty_available || runtimeSaving}
                  className={toggleClassName(runtime.use_pty_mode)}
                >
                  <span className={toggleKnobClassName(runtime.use_pty_mode)} />
                </button>
              </div>

              <div className="grid gap-4 sm:grid-cols-2">
                <div
                  data-testid="codex-main-mcp-status"
                  className="rounded-lg border border-gray-800 bg-gray-950/50 px-4 py-3"
                  title="只读运行时 capability；可用 CODEX_MAIN_MCP_ENABLED=false 紧急关闭"
                >
                  <p className="text-xs text-gray-500">Codex 主任务 MCP</p>
                  <p className={`mt-1 text-sm font-medium ${runtime.codex_main_mcp_enabled ? 'text-green-400' : 'text-gray-500'}`}>
                    {runtime.codex_main_mcp_enabled ? '已启用' : '已关闭'}
                  </p>
                </div>
                <div className="flex items-center justify-between gap-4 rounded-lg border border-gray-800 bg-gray-950/50 px-4 py-3">
                  <div>
                    <p className="text-sm font-medium text-gray-200">访问置顶</p>
                    <p className="mt-0.5 text-xs text-gray-500">打开聊天时自动将任务移到顶部。</p>
                  </div>
                  <button
                    type="button"
                    aria-label="访问置顶"
                    aria-pressed={runtime.auto_sort_on_access}
                    onClick={() => void updateRuntime({ auto_sort_on_access: !runtime.auto_sort_on_access })}
                    disabled={runtimeSaving}
                    className={toggleClassName(runtime.auto_sort_on_access)}
                  >
                    <span className={toggleKnobClassName(runtime.auto_sort_on_access)} />
                  </button>
                </div>
              </div>

              <label className="block rounded-lg border border-gray-800 bg-gray-950/50 px-4 py-3">
                <span className="mb-1.5 block text-sm font-medium text-gray-200">压缩阈值</span>
                <span className="mb-3 block text-xs leading-5 text-gray-500">
                  会话上下文利用率达到该比例时自动压缩摘要并换新 session。
                </span>
                <select
                  aria-label="压缩阈值"
                  value={Math.round(runtime.context_compact_threshold * 100)}
                  onChange={(event) => void updateRuntime({
                    context_compact_threshold: Number(event.target.value) / 100,
                  })}
                  disabled={runtimeSaving}
                  className={selectClassName}
                >
                  {Array.from(new Set([
                    60,
                    70,
                    75,
                    80,
                    85,
                    90,
                    Math.round(runtime.context_compact_threshold * 100),
                  ])).sort((left, right) => left - right).map((percentage) => (
                    <option key={percentage} value={percentage}>{percentage}%</option>
                  ))}
                </select>
              </label>
            </div>
          ) : null}

          {runtimeError && (
            <p className="mt-3 rounded border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs text-red-400">
              {runtimeError}
            </p>
          )}
        </section>
      )}

      {isAdmin && (
        <section className="rounded-xl border border-gray-800 bg-gray-900/70 p-5 shadow-sm">
          <div className="mb-4">
            <h3 className="text-sm font-semibold text-gray-100">Local task capacity</h3>
            <p className="mt-1 text-xs leading-5 text-gray-500">
              Limits concurrent Tasks and Plans on this Manager. Changes apply immediately
              without interrupting work already running. Remote Workers are configured separately.
            </p>
          </div>

          {loading || !capacity ? (
            <div className="flex items-center gap-2 py-6 text-sm text-gray-500">
              <Loader2 size={15} className="animate-spin" />
              Loading capacity…
            </div>
          ) : (
            <>
              <div className="grid gap-3 sm:grid-cols-3">
                <label className="sm:col-span-1">
                  <span className="mb-1 block text-xs font-medium text-gray-300">
                    Maximum concurrent tasks
                  </span>
                  <input
                    aria-label="Maximum concurrent tasks"
                    type="number"
                    min={1}
                    max={64}
                    step={1}
                    value={capacityInput}
                    onChange={(event) => setCapacityInput(event.target.value)}
                    className="w-full rounded-lg border border-gray-700 bg-gray-950 px-3 py-2 text-sm text-gray-100 outline-none focus:border-indigo-500"
                  />
                </label>
                <div className="rounded-lg border border-gray-800 bg-gray-950/60 px-3 py-2">
                  <p className="text-[11px] uppercase tracking-wide text-gray-500">Active now</p>
                  <p className="mt-1 text-lg font-semibold text-gray-100">{capacity.active_instances}</p>
                </div>
                <div className="rounded-lg border border-gray-800 bg-gray-950/60 px-3 py-2">
                  <p className="text-[11px] uppercase tracking-wide text-gray-500">Waiting</p>
                  <p className="mt-1 text-lg font-semibold text-gray-100">{capacity.pending_tasks}</p>
                </div>
              </div>
              <p className="mt-2 text-xs text-gray-500">
                Environment default: {capacity.env_default} · Minimum idle slots: {capacity.min_idle_instances}
                {capacity.configured_override !== null ? ' · Runtime override active' : ' · Following environment default'}
              </p>
            </>
          )}

          {capacityError && (
            <p className="mt-3 rounded border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs text-red-400">
              {capacityError}
            </p>
          )}

          <div className="mt-4 flex justify-end gap-2">
            <button
              type="button"
              onClick={() => void saveCapacity(true)}
              disabled={loading || capacitySaving || !capacity || capacity.configured_override === null}
              className="rounded-lg border border-gray-700 px-3 py-2 text-sm text-gray-300 hover:border-gray-600 hover:text-white disabled:opacity-40"
            >
              Restore environment default
            </button>
            <button
              type="button"
              onClick={() => void saveCapacity(false)}
              disabled={loading || capacitySaving || !capacity}
              className="inline-flex items-center gap-1.5 rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-500 disabled:opacity-50"
            >
              {capacitySaving
                ? <Loader2 size={14} className="animate-spin" />
                : capacitySaved ? <Check size={14} /> : null}
              {capacitySaving ? 'Saving…' : capacitySaved ? 'Saved' : 'Save capacity'}
            </button>
          </div>
        </section>
      )}

      {isAdmin && (
        <section className="rounded-xl border border-gray-800 bg-gray-900/70 p-5 shadow-sm">
          <div className="mb-4">
            <h3 className="text-sm font-semibold text-gray-100">Plan Pipeline</h3>
            <p className="mt-1 text-xs leading-5 text-gray-500">
              These routes are snapshotted when a new Plan is created. Existing
              Plans keep the configuration they started with.
            </p>
          </div>

          {loading ? (
            <div className="flex items-center gap-2 py-8 text-sm text-gray-500">
              <Loader2 size={15} className="animate-spin" />
              Loading settings…
            </div>
          ) : (
            <PlanPipelineFields
              value={pipeline}
              onChange={setPipeline}
              systemConfig={systemConfig}
            />
          )}

          {error && (
            <p className="mt-3 rounded border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs text-red-400">
              {error}
            </p>
          )}

          <div className="mt-4 flex justify-end">
            <button
              type="button"
              onClick={() => void save()}
              disabled={loading || saving}
              className="inline-flex items-center gap-1.5 rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-500 disabled:opacity-50"
            >
              {saving
                ? <Loader2 size={14} className="animate-spin" />
                : saved ? <Check size={14} /> : null}
              {saving ? 'Saving…' : saved ? 'Saved' : 'Save settings'}
            </button>
          </div>
        </section>
      )}

      <section className="rounded-xl border border-gray-800 bg-gray-900/70 p-5 shadow-sm">
        <div className="mb-4">
          <h3 className="text-sm font-semibold text-gray-100">Account</h3>
          <p className="mt-1 text-xs leading-5 text-gray-500">Connected accounts and login security.</p>
        </div>

        <div className="divide-y divide-gray-800 rounded-lg border border-gray-800 bg-gray-950/50">
          <div className="flex min-h-14 flex-wrap items-center justify-between gap-3 px-4 py-3">
            <span className="text-sm text-gray-300">飞书</span>
            {feishuStatus?.bound ? (
              <div className="flex items-center gap-2">
                {feishuStatus.avatar_url && (
                  <img src={feishuStatus.avatar_url} className="h-6 w-6 rounded-full" alt="" />
                )}
                <span className="text-sm text-gray-300">{feishuStatus.name}</span>
                <button
                  type="button"
                  onClick={async () => {
                    if (!window.confirm('解绑飞书？')) return;
                    await api.unbindFeishu();
                    setFeishuStatus({ bound: false });
                  }}
                  className="rounded px-2 py-1 text-xs text-red-400 hover:bg-red-500/10 hover:text-red-300"
                >
                  解绑
                </button>
              </div>
            ) : feishuStatus ? (
              <button
                type="button"
                onClick={async () => {
                  const { url } = await api.getFeishuAuthUrl();
                  window.location.href = url;
                }}
                className="rounded-lg bg-blue-600/20 px-3 py-1.5 text-xs font-medium text-blue-300 hover:bg-blue-600/30"
              >
                绑定
              </button>
            ) : (
              <Loader2 size={14} className="animate-spin text-gray-600" />
            )}
          </div>

          {ccUser.id && (
            <div className="flex min-h-14 flex-wrap items-center justify-between gap-3 px-4 py-3">
              <div>
                <p className="text-sm text-gray-300">登录密码</p>
                <p className="mt-0.5 text-xs text-gray-500">更新当前 CCM 账号的密码。</p>
              </div>
              <button
                type="button"
                onClick={() => {
                  const oldPassword = window.prompt('当前密码：');
                  if (!oldPassword) return;
                  const newPassword = window.prompt('新密码：');
                  if (!newPassword) return;
                  api.changePassword(oldPassword, newPassword)
                    .then(() => window.alert('密码修改成功'))
                    .catch((reason) => window.alert(reason instanceof Error ? reason.message : '修改失败'));
                }}
                className="inline-flex items-center gap-1.5 rounded-lg border border-gray-700 px-3 py-1.5 text-xs text-gray-300 hover:border-gray-600 hover:bg-gray-800 hover:text-gray-100"
              >
                <KeyRound size={13} /> 修改密码
              </button>
            </div>
          )}

          <div className="flex min-h-14 flex-wrap items-center justify-between gap-3 px-4 py-3">
            <div>
              <p className="text-sm text-gray-300">当前会话</p>
              {ccUser.name && <p className="mt-0.5 text-xs text-gray-500">{ccUser.name}</p>}
            </div>
            <button
              type="button"
              onClick={() => {
                clearToken();
                localStorage.removeItem('cc_user');
                window.location.reload();
              }}
              className="inline-flex items-center gap-1.5 rounded-lg border border-red-500/25 bg-red-500/10 px-3 py-1.5 text-xs font-medium text-red-300 hover:bg-red-500/15"
            >
              <LogOut size={13} /> 退出登录
            </button>
          </div>
        </div>
      </section>
    </div>
  );
}
