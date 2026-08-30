import { useState, useEffect, useCallback } from 'react';
import { Wrench, Users, Settings } from '../icons';
import { api } from '../../api/client';
import type { CodexServiceTier, Task, SubAgentSummary } from '../../api/client';
import { skillSupportedByProvider } from '../../config/skillCapabilities';

const ACTIVE_PLAN_STATUSES = new Set(['in_progress', 'executing']);

function routeDescription(
  label: string,
  route: { provider: string; model: string; effort: string | null },
): string {
  return `${label}: ${route.provider} / ${route.model}${route.effort ? ` / ${route.effort}` : ''}`;
}

/** Read-only Plan routing badge. A running Plan shows the concrete current
 * route (including fallback); an idle Plan shows its frozen pipeline. */
export function PlanPipelineBadge({ task }: { task: Task }) {
  if (task.mode !== 'plan') return null;

  const config = task.plan_pipeline_config;
  const active = ACTIVE_PLAN_STATUSES.has(task.status);
  const reviewing = task.plan_stage === 'reviewing';
  const configuredRoute = reviewing
    ? config?.reviewer.primary
    : config?.planner.primary;
  const provider = task.plan_stage_provider || configuredRoute?.provider;
  const model = task.plan_stage_model || configuredRoute?.model;
  const effort = task.plan_stage_effort ?? configuredRoute?.effort;
  const routeSlot = task.plan_stage_route_slot;

  if (active && provider && model) {
    const stage = reviewing ? 'Reviewer' : 'Planner';
    const round = Math.max(1, task.plan_stage_round || 1);
    const fallback = routeSlot === 'fallback';
    return (
      <span
        data-testid="plan-pipeline-badge"
        className={`flex max-w-[280px] items-center gap-1 rounded px-1.5 text-xs font-medium ${
          fallback
            ? 'bg-amber-600/25 text-amber-300'
            : provider === 'codex'
              ? 'bg-green-600/25 text-green-300'
              : 'bg-blue-600/25 text-blue-300'
        }`}
        title={`${stage}, round ${round}: ${provider} / ${model}${effort ? ` / ${effort}` : ''}${fallback ? ' (fallback)' : ''}`}
      >
        <span className="truncate">{stage} · {model}</span>
        {fallback && <span className="shrink-0 text-[10px] uppercase">fallback</span>}
      </span>
    );
  }

  if (!config) {
    return (
      <span
        data-testid="plan-pipeline-badge"
        className="max-w-[240px] truncate rounded bg-purple-600/20 px-1.5 text-xs font-medium text-purple-300"
        title={`Planner: ${task.provider} / ${task.model || 'default'}`}
      >
        Planner · {task.model || 'default'}
      </span>
    );
  }

  const summary = config.reviewer.enabled
    ? `${config.planner.primary.model} → ${config.reviewer.primary.model}`
    : config.planner.primary.model;
  const details = [
    routeDescription('Planner primary', config.planner.primary),
    routeDescription('Planner fallback', config.planner.fallback),
    ...(config.reviewer.enabled
      ? [
          routeDescription('Reviewer primary', config.reviewer.primary),
          routeDescription('Reviewer fallback', config.reviewer.fallback),
        ]
      : ['Reviewer: disabled']),
    `Maximum rounds: ${Math.max(1, config.max_revision_cycles)}`,
  ].join('\n');

  return (
    <span
      data-testid="plan-pipeline-badge"
      className="max-w-[280px] truncate rounded bg-purple-600/20 px-1.5 text-xs font-medium text-purple-300"
      title={details}
    >
      {summary}
    </span>
  );
}

/** Immutable Plan revision lineage. Middle versions can show both links. */
export function PlanRevisionBadge({ task }: { task: Task }) {
  if (task.mode !== 'plan') return null;
  const predecessorId = task.supersedes_plan_task_id;
  const successorId = task.metadata_?.plan_superseded_by_task_id;
  if (!predecessorId && !successorId) return null;

  return (
    <>
      {predecessorId && (
        <span
          className="rounded bg-indigo-600/15 px-1.5 text-[10px] font-medium text-indigo-300"
          title={`This Plan is a revision of Plan #${predecessorId}`}
        >
          Revision of #{predecessorId}
        </span>
      )}
      {successorId && (
        <span
          className="rounded bg-gray-700 px-1.5 text-[10px] font-medium text-gray-400"
          title={`A newer revision is available as Plan #${successorId}`}
        >
          Superseded by #{successorId}
        </span>
      )}
    </>
  );
}

// Plugins (SKILL.md-based) loaded from API at page load, cached globally
let _pluginsCache: { key: string; label: string }[] | null = null;
interface CodexTaskSkillsCapability {
  mainMcpEnabled: boolean;
  monitorEnabled: boolean;
}
let _codexTaskSkillsCapability:
  | Promise<CodexTaskSkillsCapability>
  | null = null;

export async function loadPlugins(): Promise<{ key: string; label: string }[]> {
  if (_pluginsCache) return _pluginsCache;
  try {
    const skills = await api.listSkills();
    _pluginsCache = skills.map((s: { key: string; label: string }) => ({ key: s.key, label: s.label }));
    return _pluginsCache;
  } catch {
    return [{ key: 'monitor', label: 'Monitor' }];
  }
}

async function loadCodexTaskSkillsCapability(): Promise<CodexTaskSkillsCapability> {
  if (!_codexTaskSkillsCapability) {
    _codexTaskSkillsCapability = api.getRuntimeSettings()
      .then((runtime) => ({
        mainMcpEnabled: runtime.codex_main_mcp_enabled !== false,
        monitorEnabled: runtime.codex_monitor_enabled === true,
      }))
      .catch((error) => {
        // A transient settings failure is capability uncertainty, not a
        // page-lifetime proof that ordinary Codex Skills are disabled.
        _codexTaskSkillsCapability = null;
        throw error;
      });
  }
  return _codexTaskSkillsCapability;
}

/** Wrench badge with a dropdown to toggle per-task tools (shared by the
 * task list and the split-mode sidebar). */
export function PluginsBadge({ task, onRefresh }: { task: Task; onRefresh: () => void }) {
  const [open, setOpen] = useState(false);
  const [tools, setTools] = useState<{ key: string; label: string }[]>([]);
  const remoteTaskScope = task.worker_id != null
    || task.shared_from_id != null
    || task.is_worker_managed;

  useEffect(() => {
    Promise.all([loadPlugins(), loadCodexTaskSkillsCapability()])
      .then(([plugins, capability]) => setTools(
        plugins.filter((plugin) => (
          skillSupportedByProvider(
            task.provider,
            plugin.key,
            capability.mainMcpEnabled,
            capability.monitorEnabled,
            remoteTaskScope,
          )
        )),
      ))
      .catch(() => {
        loadPlugins()
          .then((plugins) => setTools(
            plugins.filter((plugin) => (
              skillSupportedByProvider(
                task.provider,
                plugin.key,
                false,
                false,
                remoteTaskScope,
              )
            )),
          ))
          .catch(() => {});
      });
  }, [task.provider, remoteTaskScope]);

  useEffect(() => {
    if (!open) return;
    const handle = (e: MouseEvent) => {
      if (!(e.target as HTMLElement).closest('[data-plugins-dropdown]')) setOpen(false);
    };
    document.addEventListener('mousedown', handle);
    return () => document.removeEventListener('mousedown', handle);
  }, [open]);

  return (
    <div className="relative" data-plugins-dropdown>
      <button
        onClick={(e) => { e.stopPropagation(); setOpen(!open); }}
        className="text-xs bg-amber-600/30 text-amber-300 px-1.5 rounded cursor-pointer hover:bg-amber-600/40 flex items-center gap-0.5"
        title="Plugins"
      >
        <Wrench size={12} />
        {task.enabled_skills ? Object.entries(task.enabled_skills).filter(([k, v]) => v && tools.some(t => t.key === k)).length : 0}
      </button>
      {open && (
        <div className="absolute top-full mt-1 left-0 bg-gray-800 border border-gray-600 rounded shadow-lg z-20 min-w-[160px] py-1">
          {tools.map((tool) => {
            const enabled = !!(task.enabled_skills && task.enabled_skills[tool.key]);
            return (
              <button
                key={tool.key}
                onClick={async (e) => {
                  e.stopPropagation();
                  const nextSkills = {
                    ...(task.enabled_skills || {}),
                    [tool.key]: !enabled,
                  };
                  try {
                    // ``tools`` is only the currently visible capability
                    // subset. Preserve persisted keys that are hidden because
                    // capability discovery is unavailable or provider-filtered.
                    await api.updateTask(task.id, { enabled_skills: nextSkills });
                    onRefresh();
                  } catch { /* keep current state */ }
                }}
                className="w-full px-3 py-1.5 text-xs text-left flex items-center gap-2 hover:bg-gray-700 transition-colors"
              >
                <span className={`w-3.5 h-3.5 rounded border flex items-center justify-center text-[9px] ${
                  enabled ? 'bg-green-600 border-green-500 text-white' : 'border-gray-600'
                }`}>
                  {enabled && '✓'}
                </span>
                <span className={enabled ? 'text-gray-200' : 'text-gray-400'}>{tool.label}</span>
              </button>
            );
          })}
        </div>
      )}
    </div>
  );
}

/** Sub-agents badge with a summary dropdown (shared by the task list and
 * the split-mode sidebar). */
export function SubAgentsBadge({ task }: { task: Task }) {
  const [open, setOpen] = useState(false);
  const [summary, setSummary] = useState<SubAgentSummary | null>(null);

  useEffect(() => {
    if (!open) return;
    const handle = (e: MouseEvent) => {
      if (!(e.target as HTMLElement).closest('[data-subagents-dropdown]')) setOpen(false);
    };
    document.addEventListener('mousedown', handle);
    return () => document.removeEventListener('mousedown', handle);
  }, [open]);

  const toggle = useCallback(async (e: React.MouseEvent) => {
    e.stopPropagation();
    if (open) {
      setOpen(false);
      setSummary(null);
      return;
    }
    setSummary(null);
    try {
      setSummary(await api.getSubAgentSummary(task.id));
    } catch {
      setSummary({ by_type: {} });
    }
    setOpen(true);
  }, [open, task.id]);

  return (
    <div className="relative" data-subagents-dropdown>
      <button
        onClick={toggle}
        className={`text-xs bg-teal-600/30 text-teal-300 px-1.5 rounded cursor-pointer hover:bg-teal-600/40 flex items-center gap-0.5${task.active_sub_agents > 0 ? ' animate-pulse' : ''}`}
        title="Sub-agents"
      >
        <Users size={12} />
        {task.active_sub_agents}
      </button>
      {open && (
        <div className="absolute top-full mt-1 left-0 bg-gray-800 border border-gray-600 rounded shadow-lg z-20 min-w-[140px] py-1">
          {summary && Object.keys(summary.by_type).length > 0 ? (
            Object.entries(summary.by_type).map(([type, counts]) => (
              <div key={type} className="px-3 py-1 text-xs text-gray-300 flex items-center justify-between gap-3">
                <span>{type.charAt(0).toUpperCase() + type.slice(1)}</span>
                <span className={counts.running > 0 ? 'text-green-400' : 'text-gray-500'}>{counts.running} running</span>
              </div>
            ))
          ) : (
            <div className="px-3 py-1 text-xs text-gray-500">No sub-agents</div>
          )}
        </div>
      )}
    </div>
  );
}

// Config options cache (fetched once per page load)
interface ConfigOptions {
  claude: string[]; codex: string[];
  effort: string[]; codexEffort: string[];
  codexModelEfforts: Record<string, string[]>;
  defaultCodexModel: string;
  codexModelServiceTiers: Record<string, CodexServiceTier[]>;
}
let _configOptionsCache: ConfigOptions | null = null;
async function fetchConfigOptions(): Promise<ConfigOptions> {
  if (_configOptionsCache) return _configOptionsCache;
  const c = await api.config();
  _configOptionsCache = {
    claude: c.model_options.filter((m) => m !== 'default'),
    codex: c.codex_model_options.filter((m) => m !== 'default'),
    effort: c.effort_options,
    codexEffort: c.codex_effort_options,
    codexModelEfforts: c.codex_model_efforts || {},
    defaultCodexModel: c.default_codex_model,
    codexModelServiceTiers: c.codex_model_service_tiers || {},
  };
  return _configOptionsCache;
}
const fetchModelOptions = fetchConfigOptions;

/** Visible Fast request marker. The backend refuses to start a turn unless
 * priority was confirmed, so it cannot silently execute this task as Standard. */
export function FastModeBadge({ task }: { task: Task }) {
  if (task.provider !== 'codex' || task.codex_service_tier !== 'priority') return null;
  return (
    <span
      data-testid="codex-fast-badge"
      className="text-xs bg-amber-500/20 text-amber-300 px-1.5 rounded font-medium whitespace-nowrap"
      title="Codex Fast（priority service tier）"
    >
      Fast
    </span>
  );
}

/** Clickable model badge: dropdown to switch the task's model (persisted). */
export function ModelBadge({ task, onRefresh, compact }: { task: Task; onRefresh: () => void; compact?: boolean }) {
  const [open, setOpen] = useState(false);
  const [config, setConfig] = useState<ConfigOptions | null>(null);

  useEffect(() => {
    if (!open) return;
    fetchModelOptions().then(setConfig).catch(() => {});
    const handle = (e: MouseEvent) => {
      if (!(e.target as HTMLElement).closest('[data-model-dropdown]')) setOpen(false);
    };
    document.addEventListener('mousedown', handle);
    return () => document.removeEventListener('mousedown', handle);
  }, [open, task.provider]);

  const label = task.model || 'default';
  const options = config ? (task.provider === 'codex' ? config.codex : config.claude) : [];

  return (
    <div className="relative" data-model-dropdown>
      <button
        onClick={(e) => { e.stopPropagation(); setOpen(!open); }}
        className={`text-xs bg-gray-700 text-gray-300 px-1.5 rounded cursor-pointer hover:bg-gray-600 hover:text-gray-100 ${compact ? 'max-w-[120px] truncate' : ''}`}
        title="切换模型（持久化到该任务）"
      >
        {label}
      </button>
      {open && (
        <div className="absolute top-full mt-1 left-0 bg-gray-800 border border-gray-600 rounded shadow-lg z-20 min-w-[180px] py-1 max-h-60 overflow-y-auto">
          {options.length === 0 && (
            <div className="px-3 py-1.5 text-xs text-gray-500">Loading…</div>
          )}
          {options.map((m) => (
            <button
              key={m}
              onClick={async (e) => {
                e.stopPropagation();
                setOpen(false);
                if (m === task.model) return;
                try {
                  const shouldClearFast = task.provider === 'codex'
                    && task.codex_service_tier === 'priority'
                    && !(config?.codexModelServiceTiers[m] || []).includes('priority');
                  await api.updateTask(task.id, {
                    model: m,
                    ...(shouldClearFast ? { codex_service_tier: 'default' as const } : {}),
                  });
                  onRefresh();
                } catch {
                  // A lost response does not prove the update was rolled back.
                  // Reconcile the badge/model with the authoritative Task.
                  onRefresh();
                }
              }}
              className={`w-full px-3 py-1.5 text-xs text-left transition-colors hover:bg-gray-700 ${
                m === task.model ? 'text-indigo-300 bg-indigo-600/20' : 'text-gray-300'
              }`}
            >
              {m}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}


const TIMEOUT_OPTIONS: { value: string; label: string }[] = [
  { value: '', label: 'default' },
  { value: '0.5', label: '30 min' },
  { value: '1', label: '1 hour' },
  { value: '2', label: '2 hours' },
  { value: '4', label: '4 hours' },
  { value: '8', label: '8 hours' },
  { value: '12', label: '12 hours' },
  { value: '24', label: '24 hours' },
  { value: '0', label: 'No limit' },
];

const THINKING_OPTIONS: { value: string; label: string }[] = [
  { value: '', label: 'default' },
  { value: '4096', label: '4k' },
  { value: '8192', label: '8k' },
  { value: '16384', label: '16k' },
  { value: '32768', label: '32k' },
  { value: '65536', label: '64k' },
  { value: '131072', label: '128k' },
];

/** Per-task Config: gear button opening a panel to edit Model / Effort /
 * Timeout / Thinking in place (each change persists via updateTask).
 * Shared by the task list, the sidebar, and the chat header. */
export function TaskConfigBadge({ task, onRefresh, openUp, align }: { task: Task; onRefresh: () => void; openUp?: boolean; align?: 'left' | 'right' }) {
  const [open, setOpen] = useState(false);
  const [opts, setOpts] = useState<ConfigOptions | null>(null);
  const [updateError, setUpdateError] = useState('');
  // workers state removed — Run on moved to Project level
  // migrating state removed — Run on moved to Project level

  useEffect(() => {
    if (!open) return;
    // workers list removed — Run on moved to Project level
  }, [open]);

  useEffect(() => {
    if (!open) return;
    fetchConfigOptions().then(setOpts).catch(() => {});
    const handle = (e: MouseEvent) => {
      if (!(e.target as HTMLElement).closest('[data-task-config]')) setOpen(false);
    };
    document.addEventListener('mousedown', handle);
    return () => document.removeEventListener('mousedown', handle);
  }, [open]);

  const isShared = !!task.shared_from_id;
  const update = async (data: Parameters<typeof api.updateTask>[1]) => {
    if (isShared) return;
    try {
      await api.updateTask(task.id, data);
      setUpdateError('');
      onRefresh();
    } catch (error) {
      setUpdateError(
        error instanceof Error
          ? error.message
          : 'Task 配置保存失败，请稍后重试',
      );
      // The response can be lost after the server has already committed the
      // authoritative routing tuple. Re-read it even on failure so a stale
      // Fast badge can never be treated as proof that the next turn is Fast.
      onRefresh();
    }
  };

  const isCodex = task.provider === 'codex';
  const models = opts ? (isCodex ? opts.codex : opts.claude) : [];
  // GPT-5.6 系列按模型区分档位（sol/terra 到 ultra，luna 到 max）
  const efforts = opts
    ? (isCodex ? (task.model && opts.codexModelEfforts[task.model]) || opts.codexEffort : opts.effort)
    : [];
  const resolvedCodexModel = (
    !task.model || task.model === 'default'
      ? opts?.defaultCodexModel
      : task.model
  ) || '';
  const codexModelSupportsFast = !!opts
    && (opts.codexModelServiceTiers[resolvedCodexModel] || []).includes('priority');

  return (
    <div className="relative" data-task-config>
      <button
        onClick={(e) => { e.stopPropagation(); setOpen(!open); }}
        className="text-xs bg-gray-700 text-gray-300 px-1.5 rounded cursor-pointer hover:bg-gray-600 hover:text-gray-100 flex items-center gap-0.5"
        title={`Config（model: ${task.model || 'default'}）`}
      >
        <Settings size={12} />
        <span className="hidden sm:inline">Config</span>
      </button>
      {open && (
        <div
          data-task-config
          className={`absolute ${openUp ? 'bottom-full mb-1' : 'top-full mt-1'} ${align === 'right' ? 'right-0' : 'left-0'} bg-gray-800 border border-gray-600 rounded shadow-lg z-20 p-3 min-w-[250px] max-w-[calc(100vw-1rem)] max-h-[80vh] overflow-y-auto`}
          onClick={(e) => e.stopPropagation()}
        >
          {isShared && <p className="text-xs text-orange-400 mb-2">Shared task — read only</p>}
          {updateError && (
            <p
              role="alert"
              className="text-xs text-red-300 bg-red-950/50 border border-red-800/60 rounded px-2 py-1.5 mb-2"
            >
              配置保存失败：{updateError}
            </p>
          )}
          <div className={`grid grid-cols-[auto_1fr] gap-x-3 gap-y-2 items-center text-xs ${isShared ? 'pointer-events-none opacity-60' : ''}`}>
            <span className="text-gray-400">Model</span>
            <select
              className="bg-gray-700 text-foreground rounded px-2 py-1 text-xs"
              value={task.model || ''}
              onChange={(e) => {
                const nextModel = e.target.value;
                const shouldClearFast = isCodex
                  && task.codex_service_tier === 'priority'
                  && !(opts?.codexModelServiceTiers[nextModel] || []).includes('priority');
                update({
                  model: nextModel,
                  ...(shouldClearFast ? { codex_service_tier: 'default' as const } : {}),
                });
              }}
            >
              {task.model && !models.includes(task.model) && (
                <option value={task.model}>{task.model}</option>
              )}
              {models.map((m) => <option key={m} value={m}>{m}</option>)}
            </select>

            <span className="text-gray-400">Effort</span>
            <select
              className="bg-gray-700 text-foreground rounded px-2 py-1 text-xs"
              value={task.effort_level || ''}
              onChange={(e) => update({ effort_level: e.target.value })}
            >
              {!task.effort_level && <option value="">default</option>}
              {efforts.map((m) => <option key={m} value={m}>{m}</option>)}
            </select>

            {isCodex && (
              <>
                <span className="text-gray-400">Speed</span>
                <select
                  aria-label="Codex speed"
                  className="bg-gray-700 text-foreground rounded px-2 py-1 text-xs"
                  value={task.codex_service_tier || 'default'}
                  onChange={(e) => update({ codex_service_tier: e.target.value as CodexServiceTier })}
                >
                  <option value="default">Standard</option>
                  <option value="priority" disabled={!codexModelSupportsFast}>Fast</option>
                </select>
              </>
            )}

            <span className="text-gray-400">Timeout</span>
            <select
              className="bg-gray-700 text-foreground rounded px-2 py-1 text-xs"
              value={task.timeout_hours == null ? '' : String(task.timeout_hours)}
              onChange={(e) => update({ timeout_hours: e.target.value === '' ? null : Number(e.target.value) })}
            >
              {TIMEOUT_OPTIONS.map((o) => <option key={o.value} value={o.value}>{o.label}</option>)}
            </select>

            <span className="text-gray-400">Thinking</span>
            <select
              className="bg-gray-700 text-foreground rounded px-2 py-1 text-xs"
              value={task.thinking_budget == null ? '' : String(task.thinking_budget)}
              onChange={(e) => update({ thinking_budget: e.target.value === '' ? null : Number(e.target.value) })}
            >
              {THINKING_OPTIONS.map((o) => <option key={o.value} value={o.value}>{o.label}</option>)}
            </select>

            <span className="text-gray-400">System Prompt</span>
            <select
              className="bg-gray-700 text-foreground rounded px-2 py-1 text-xs"
              value={task.system_prompt_mode || ''}
              onChange={(e) => update({ system_prompt_mode: e.target.value === '' ? 'off' : e.target.value })}
            >
              <option value="">Off</option>
              <option value="append">Fable 5 (Append)</option>
              <option value="replace">Fable 5 (Replace)</option>
            </select>
          </div>
          {isCodex && (
            <div className={`mt-2 text-[10px] ${codexModelSupportsFast ? 'text-amber-400/80' : 'text-gray-500'}`}>
              {codexModelSupportsFast
                ? 'Fast 约 1.5×，会消耗更多额度；实际计价取决于账号来源'
                : `${resolvedCodexModel || '当前模型'} 不支持 Fast`}
            </div>
          )}
          <div className="mt-2 text-[10px] text-gray-500">修改在下一轮对话生效</div>
        </div>
      )}
    </div>
  );
}
