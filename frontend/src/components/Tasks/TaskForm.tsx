import { useState, useEffect, useRef } from 'react';
import { api } from '../../api/client';
import type {
  CodexServiceTier,
  Project,
  TagItem,
  Task,
} from '../../api/client';
import { Plus, Paperclip, X, Star, Wrench, Settings, Loader2, AlertCircle, Pin } from '../icons';
import { ProjectSelect } from '../ProjectSelect';
import { VoiceButton } from '../Voice/VoiceButton';
import { SecretPicker } from '../Secrets/SecretPicker';
import { useFileDrop } from '../../hooks/useFileDrop';
import { useFileUpload } from '../../hooks/useFileUpload';
import { skillSupportedByProvider } from '../../config/skillCapabilities';

interface TaskFormProps {
  onCreated: () => void;
}

const NEW_PROJECT_VALUE = '__new__';
const STORAGE_KEY = 'cc_default_task_config';
interface StoredTaskDefaults {
  priority?: number;
  mode?: string;
  provider?: string;
  model?: string;
  effort?: string;
  codexServiceTier?: CodexServiceTier;
  thinkingBudget?: string;
  timeoutHours?: string;
  systemPromptMode?: string;
}

function readStoredTaskDefaults(): StoredTaskDefaults | null {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return null;
    const parsed: unknown = JSON.parse(raw);
    return parsed && typeof parsed === 'object'
      ? parsed as StoredTaskDefaults
      : null;
  } catch {
    return null;
  }
}

export function TaskForm({ onCreated }: TaskFormProps) {
  const ccUser = JSON.parse(localStorage.getItem('cc_user') || '{}');
  const isAdmin = ccUser.role === 'admin' || ccUser.role === 'super_admin' || !ccUser.id;
  const [description, setDescription] = useState('');
  const [projectId, setProjectId] = useState<number | ''>('');
  const [isNewProject, setIsNewProject] = useState(false);
  const [newProjectName, setNewProjectName] = useState('');
  const [newProjectUrl, setNewProjectUrl] = useState('');
  const [priority, setPriority] = useState(0);
  const [mode, setMode] = useState('auto');
  const [provider, setProvider] = useState('codex');
  // 分布式 Worker：执行位置（'' = 本机）
  const [workerId, setWorkerId] = useState('');
  // workers state removed — Run on moved to Project level
  const [model, setModel] = useState('');
  const [providerOptions, setProviderOptions] = useState<string[]>(['claude', 'codex']);
  const [effort, setEffort] = useState('');
  const [defaultProvider, setDefaultProvider] = useState('codex');
  const [defaultModel, setDefaultModel] = useState('claude-opus-4-6');
  const [modelOptions, setModelOptions] = useState<string[]>([]);
  const [defaultCodexModel, setDefaultCodexModel] = useState('gpt-5.6-sol');
  const [codexModelOptions, setCodexModelOptions] = useState<string[]>([]);
  const [effortOptions, setEffortOptions] = useState<string[]>([]);
  const [codexEffortOptions, setCodexEffortOptions] = useState<string[]>([]);
  // GPT-5.6 系列按模型区分档位（sol/terra 到 ultra，luna 到 max），未列出的模型用 codexEffortOptions
  const [codexModelEfforts, setCodexModelEfforts] = useState<Record<string, string[]>>({});
  const [codexModelServiceTiers, setCodexModelServiceTiers] = useState<Record<string, CodexServiceTier[]>>({});
  const [codexCapabilitiesLoaded, setCodexCapabilitiesLoaded] = useState(false);
  const [codexServiceTier, setCodexServiceTier] = useState<CodexServiceTier>('default');
  const [defaultEffort, setDefaultEffort] = useState('medium');
  const [todoFilePath, setTodoFilePath] = useState('');
  const [maxIterations, setMaxIterations] = useState('50');
  const [mustComplete, setMustComplete] = useState(false);
  const [goalCondition, setGoalCondition] = useState('');
  const [goalMaxTurns, setGoalMaxTurns] = useState('30');
  const [thinkingBudget, setThinkingBudget] = useState('');
  const [timeoutHours, setTimeoutHours] = useState('');
  const [systemPromptMode, setSystemPromptMode] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [projects, setProjects] = useState<Project[]>([]);
  const [hasWorker, setHasWorker] = useState(isAdmin);
  const [tagItems, setTagItems] = useState<TagItem[]>([]);
  const fileUpload = useFileUpload();
  const [selectedSecretIds, setSelectedSecretIds] = useState<number[]>([]);
  const [dropError, setDropError] = useState('');
  const [enabledPlugins, setEnabledPlugins] = useState<Record<string, boolean>>({});
  const [codexTaskSkillsEnabled, setCodexTaskSkillsEnabled] = useState(false);
  const [codexMonitorEnabled, setCodexMonitorEnabled] = useState(false);
  const [showPluginsDropdown, setShowPluginsDropdown] = useState(false);
  const [showConfigPanel, setShowConfigPanel] = useState(false);
  const [starOnCreate, setStarOnCreate] = useState(false);
  const pluginsRef = useRef<HTMLDivElement>(null);
  const configRef = useRef<HTMLDivElement>(null);
  const [cloneFromTaskId, setCloneFromTaskId] = useState<number | ''>('');
  const [contextTasks, setContextTasks] = useState<Task[]>([]);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const formRef = useRef<HTMLFormElement>(null);

  const applyStoredDefaults = (
    stored: StoredTaskDefaults | null,
    fallbackProvider: string,
  ) => {
    setPriority(stored?.priority ?? 0);
    // Plan creation now lives on the first-class Plans page. Normalize the
    // legacy saved value so an old browser preference cannot recreate the
    // removed Task-form mode invisibly.
    setMode(stored?.mode === 'plan' ? 'auto' : stored?.mode || 'auto');
    setProvider(stored?.provider || fallbackProvider);
    setModel(stored?.model || '');
    setEffort(stored?.effort || '');
    setCodexServiceTier(stored?.codexServiceTier === 'priority' ? 'priority' : 'default');
    setThinkingBudget(stored?.thinkingBudget || '');
    setTimeoutHours(stored?.timeoutHours || '');
    setSystemPromptMode(stored?.systemPromptMode || '');
  };

  const loadProjects = () => {
    api.listProjects().then(setProjects).catch(() => {});
    api.listTags().then(setTagItems).catch(() => {});
  };

  useEffect(() => {
    loadProjects();
    if (!isAdmin) api.listWorkers().then(w => setHasWorker(w.length > 0)).catch(() => {});
    // Restore persisted user choices independently of the server request.
    // A slow or unavailable backend must not make local defaults disappear.
    applyStoredDefaults(readStoredTaskDefaults(), 'codex');
    api.config().then((c) => {
      const configuredProvider = c.default_provider || 'codex';
      setDefaultProvider(configuredProvider);
      setProviderOptions(c.provider_options.length ? c.provider_options : ['claude', 'codex']);
      setDefaultModel(c.default_model);
      setModelOptions(c.model_options.filter((m) => m !== 'default'));
      setDefaultCodexModel(c.default_codex_model);
      setCodexModelOptions(c.codex_model_options.filter((m) => m !== 'default'));
      setDefaultEffort(c.default_effort);
      setEffortOptions(c.effort_options);
      setCodexEffortOptions(c.codex_effort_options || ['low', 'medium', 'high', 'xhigh']);
      setCodexModelEfforts(c.codex_model_efforts || {});
      setCodexModelServiceTiers(c.codex_model_service_tiers || {});
      setCodexCapabilitiesLoaded(true);
      // Re-read after the async request so a default saved while it was in
      // flight still wins over the server defaults.
      applyStoredDefaults(readStoredTaskDefaults(), configuredProvider);
    }).catch(() => {
      setCodexModelServiceTiers({});
      setCodexCapabilitiesLoaded(true);
      applyStoredDefaults(readStoredTaskDefaults(), 'codex');
    });
    api.getRuntimeSettings()
      .then((runtime) => {
        setCodexTaskSkillsEnabled(
          runtime.codex_main_mcp_enabled !== false,
        );
        setCodexMonitorEnabled(runtime.codex_monitor_enabled === true);
      })
      .catch(() => {});
  }, []);

  useEffect(() => {
    if (!projectId) {
      setContextTasks([]);
      setCloneFromTaskId('');
      return;
    }
    api.listTasks(undefined, true, projectId as number, undefined, 100)
      .then((tasks) => setContextTasks(tasks.filter((t) => t.session_id)))
      .catch(() => setContextTasks([]));
  }, [projectId]);

  const [availableSkills, setAvailableSkills] = useState<{ key: string; label: string; description: string }[]>([]);
  const selectedProject = projectId
    ? projects.find((project) => project.id === projectId)
    : undefined;
  const remoteTaskScope = Boolean(workerId)
    || selectedProject?.worker_id != null;
  useEffect(() => {
    api.listSkillsCached()
      .then((skills) => setAvailableSkills(
        skills
          .filter((skill) => skillSupportedByProvider(
            provider,
            skill.key,
            codexTaskSkillsEnabled,
            codexMonitorEnabled,
            remoteTaskScope,
          ))
          .map((s) => ({ key: s.key, label: s.label, description: s.description })),
      ))
      .catch(() => setAvailableSkills(
        provider === 'codex'
          ? [{ key: 'sub-agent', label: 'Sub-Agent', description: 'Parallel one-shot sub-agents' }]
          : [{ key: 'monitor', label: 'Monitor', description: 'Background monitoring sub-agents' }],
      ));
  }, [
    provider,
    codexTaskSkillsEnabled,
    codexMonitorEnabled,
    remoteTaskScope,
  ]);
  const AVAILABLE_PLUGINS = availableSkills;
  const enabledPluginCount = AVAILABLE_PLUGINS.filter(
    (skill) => enabledPlugins[skill.key],
  ).length;

  const [userSkills, setUserSkills] = useState<{ id: number; name: string; description: string }[]>([]);
  const [enabledUserSkills, setEnabledUserSkills] = useState<Record<number, boolean>>({});
  const [showSkillsDropdown, setShowSkillsDropdown] = useState(false);
  const skillsRef = useRef<HTMLDivElement>(null);
  const [skillsDefaultSaved, setSkillsDefaultSaved] = useState(false);
  useEffect(() => {
    api.listUserSkillsCached().then((list: any[]) => setUserSkills(list.map((s) => ({ id: s.id, name: s.name, description: s.description })))).catch(() => {});
  }, []);
  const enabledUserSkillCount = Object.values(enabledUserSkills).filter(Boolean).length;

  // Load default skills/plugins from server on mount
  useEffect(() => {
    api.getDefaultSkills().then((defaults) => {
      if (defaults.default_enabled_plugins) {
        setEnabledPlugins(defaults.default_enabled_plugins);
      }
      if (defaults.default_enabled_user_skills) {
        const map: Record<number, boolean> = {};
        for (const id of defaults.default_enabled_user_skills) map[id] = true;
        setEnabledUserSkills(map);
      }
    }).catch(() => {});
  }, []);

  const saveSkillsAsDefault = () => {
    const pluginSelection = Object.entries(enabledPlugins)
      .filter(([key, enabled]) => (
        enabled && skillSupportedByProvider(
          provider,
          key,
          codexTaskSkillsEnabled,
          codexMonitorEnabled,
          remoteTaskScope,
        )
      ))
      .reduce((acc, [k]) => ({ ...acc, [k]: true }), {} as Record<string, boolean>);
    const userSkillIds = Object.entries(enabledUserSkills)
      .filter(([, v]) => (
        v && (provider !== 'codex' || codexTaskSkillsEnabled)
      ))
      .map(([k]) => Number(k));
    api.setDefaultSkills(
      Object.keys(pluginSelection).length > 0 ? pluginSelection : null,
      userSkillIds.length > 0 ? userSkillIds : null,
    ).then(() => {
      setSkillsDefaultSaved(true);
      setTimeout(() => setSkillsDefaultSaved(false), 2000);
    }).catch(() => {});
  };

  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (pluginsRef.current && !pluginsRef.current.contains(e.target as Node)) {
        setShowPluginsDropdown(false);
      }
      if (configRef.current && !configRef.current.contains(e.target as Node)) {
        setShowConfigPanel(false);
      }
      if (skillsRef.current && !skillsRef.current.contains(e.target as Node)) {
        setShowSkillsDropdown(false);
      }
    };
    if (showPluginsDropdown || showConfigPanel || showSkillsDropdown) document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, [showPluginsDropdown, showConfigPanel, showSkillsDropdown]);

  const saveAsDefault = () => {
    const cfg = {
      priority,
      mode,
      provider,
      model,
      effort,
      codexServiceTier,
      thinkingBudget,
      timeoutHours,
      systemPromptMode,
    };
    localStorage.setItem(STORAGE_KEY, JSON.stringify(cfg));
    setDefaultSaved(true);
    setTimeout(() => setDefaultSaved(false), 2000);
  };

  const clearDefault = () => {
    localStorage.removeItem(STORAGE_KEY);
    setDefaultSaved(false);
  };

  const [defaultSaved, setDefaultSaved] = useState(false);
  const hasStoredDefault = !!localStorage.getItem(STORAGE_KEY);

  const hasNonDefaultConfig = priority !== 0 || mode !== 'auto' || provider !== defaultProvider || model !== '' || effort !== '' || codexServiceTier !== 'default' || thinkingBudget !== '' || timeoutHours !== '';

  const activeDefaultModel = provider === 'codex' ? defaultCodexModel : defaultModel;
  const activeModelOptions = provider === 'codex' ? codexModelOptions : modelOptions;
  const activeEffortOptions = provider === 'codex'
    ? (codexModelEfforts[model || defaultCodexModel] ?? codexEffortOptions)
    : effortOptions;
  const activeCodexModel = model || defaultCodexModel;
  const codexModelSupportsFast = (candidate: string) =>
    (codexModelServiceTiers[candidate] || []).includes('priority');
  const activeCodexModelSupportsFast = codexModelSupportsFast(activeCodexModel);

  // 切换 codex 模型后，已选档位可能不再受支持（如 max 换到 gpt-5.5）——回落默认
  useEffect(() => {
    if (provider !== 'codex' || !effort) return;
    const supported = codexModelEfforts[model || defaultCodexModel] ?? codexEffortOptions;
    if (supported.length && !supported.includes(effort)) setEffort('');
  }, [provider, model, effort, codexModelEfforts, codexEffortOptions, defaultCodexModel]);

  // Fast is Codex-only and model-gated. Wait for the capability response before
  // normalizing persisted defaults so a slow config request cannot erase them.
  useEffect(() => {
    if (provider !== 'codex') {
      if (codexServiceTier !== 'default') setCodexServiceTier('default');
      return;
    }
    if (codexCapabilitiesLoaded && codexServiceTier === 'priority' && !activeCodexModelSupportsFast) {
      setCodexServiceTier('default');
    }
  }, [provider, codexServiceTier, codexCapabilitiesLoaded, activeCodexModelSupportsFast]);

  const handleProjectChange = (val: string) => {
    if (val === NEW_PROJECT_VALUE) {
      setIsNewProject(true);
      setProjectId('');
    } else {
      setIsNewProject(false);
      setNewProjectName('');
      setNewProjectUrl('');
      setProjectId(val ? Number(val) : '');
    }
  };

  useFileDrop({
    targetRef: formRef,
    onDrop: (files) => fileUpload.addFiles(files, (msg) => setDropError(msg)),
    disabled: false,
  });

  useEffect(() => {
    const handlePaste = (e: ClipboardEvent) => {
      const items = e.clipboardData?.items;
      if (!items) return;
      const files: File[] = [];
      for (const item of items) {
        if (item.kind === 'file') {
          const f = item.getAsFile();
          if (f) files.push(f);
        }
      }
      if (files.length > 0) {
        e.preventDefault();
        fileUpload.addFiles(files, (msg) => setDropError(msg));
      }
    };
    const form = formRef.current;
    if (form) {
      form.addEventListener('paste', handlePaste);
      return () => form.removeEventListener('paste', handlePaste);
    }
  }, [fileUpload.addFiles]);

  useEffect(() => {
    if (dropError) {
      const t = setTimeout(() => setDropError(''), 2000);
      return () => clearTimeout(t);
    }
  }, [dropError]);

  const handleFileSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = Array.from(e.target.files || []);
    if (!files.length) return;
    fileUpload.addFiles(files, (msg) => setDropError(msg));
    e.target.value = '';
  };

  const canSubmit =
    (description || mode === 'loop') &&
    (mode !== 'loop' || todoFilePath) &&
    (mode !== 'goal' || goalCondition) &&
    (projectId || (isNewProject && newProjectName)) &&
    !fileUpload.isUploading &&
    !fileUpload.hasFailed;

  useEffect(() => {
    // workers list removed — Run on moved to Project level
  }, []);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!canSubmit) return;
    setLoading(true);
    setError('');
    try {
      let pid = projectId || undefined;

      // Create new project first if needed
      if (isNewProject && newProjectName) {
        const project = await api.createProject({
          name: newProjectName,
          git_url: newProjectUrl || undefined,
        });
        pid = project.id;
        // Refresh project list and reset new project fields
        loadProjects();
        setIsNewProject(false);
        setNewProjectName('');
        setNewProjectUrl('');
        setProjectId(project.id);
      }

      const uploadedPaths = fileUpload.uploadedResults.map(r => r.path);
      const attachments = fileUpload.uploadedResults.map(r => ({
        url: r.url,
        name: r.filename || r.url.split('/').pop() || 'file',
        is_image: r.is_image,
      }));

      await api.createTask({
        description: description || undefined,
        project_id: pid as number,
        priority,
        mode,
        ...(mode === 'loop' ? { todo_file_path: todoFilePath, max_iterations: parseInt(maxIterations) || 50, must_complete: mustComplete } : {}),
        ...(mode === 'goal' ? { goal_condition: goalCondition, goal_max_turns: parseInt(goalMaxTurns) || 30 } : {}),
        ...(uploadedPaths.length > 0 ? { file_paths: uploadedPaths } : {}),
        ...(attachments.length > 0 ? { attachments } : {}),
        ...(selectedSecretIds.length > 0 ? { secret_ids: selectedSecretIds } : {}),
        ...(workerId ? { worker_id: parseInt(workerId) } : {}),
        provider,
        model: model || activeDefaultModel,
        ...(effort ? { effort_level: effort } : {}),
        ...(provider === 'codex'
          ? { codex_service_tier: codexServiceTier }
          : {}),
        ...(thinkingBudget ? { thinking_budget: parseInt(thinkingBudget) || null } : {}),
        ...(systemPromptMode ? { system_prompt_mode: systemPromptMode } : {}),
        ...(timeoutHours !== '' ? { timeout_hours: Number(timeoutHours) } : {}),
        enabled_skills: (() => {
          const skills = Object.entries(enabledPlugins)
            .filter(([key, enabled]) => (
              enabled && skillSupportedByProvider(
                provider,
                key,
                codexTaskSkillsEnabled,
                codexMonitorEnabled,
                remoteTaskScope,
              )
            ))
            .reduce((acc, [k]) => ({ ...acc, [k]: true }), {} as Record<string, boolean>);
          return Object.keys(skills).length > 0 ? skills : undefined;
        })(),
        ...(enabledUserSkillCount > 0
          && (provider !== 'codex' || codexTaskSkillsEnabled) ? {
          selected_user_skills: Object.entries(enabledUserSkills)
            .filter(([, v]) => v)
            .map(([k]) => Number(k)),
        } : {}),
        ...(starOnCreate ? { starred: true } : {}),
        ...(provider === 'claude' && cloneFromTaskId
          ? { clone_from_task_id: cloneFromTaskId as number }
          : {}),
      });
      setDescription('');
      fileUpload.clear();
      setSelectedSecretIds([]);
      setCloneFromTaskId('');
      setGoalCondition('');
      setGoalMaxTurns('30');
      // Restore localStorage defaults (or fall back to server defaults)
      applyStoredDefaults(readStoredTaskDefaults(), defaultProvider);
      onCreated();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to create task');
    } finally {
      setLoading(false);
    }
  };

  return (
    <form ref={formRef} onSubmit={handleSubmit} className="bg-gray-800 border border-gray-700/60 rounded-xl p-4 space-y-3 overflow-visible shadow-sm">
      <h3 className="text-sm font-semibold text-gray-300">New Task</h3>
      {dropError && (
        <div className="bg-yellow-900/50 border border-yellow-700 text-yellow-300 text-xs rounded px-3 py-2 flex items-center justify-between">
          <span>{dropError}</span>
          <button type="button" onClick={() => setDropError('')} className="text-yellow-400 hover:text-yellow-200 ml-2">
            <X size={14} />
          </button>
        </div>
      )}
      {error && (
        <div className="bg-red-900/50 border border-red-700 text-red-300 text-xs rounded px-3 py-2 flex items-center justify-between">
          <span>{error}</span>
          <button type="button" onClick={() => setError('')} className="text-red-400 hover:text-red-200 ml-2">
            <X size={14} />
          </button>
        </div>
      )}
      <div className="flex gap-2">
        <textarea
          className="flex-1 bg-gray-700 text-foreground rounded-lg px-3 py-2 text-sm h-24 resize-none border border-gray-600/50 focus:outline-none focus:border-indigo-500 focus:ring-2 focus:ring-indigo-500/25 transition-colors"
          placeholder={mode === 'loop' ? 'Background / context (optional)' : `Prompt / Description (this will be sent to ${provider === 'codex' ? 'Codex' : 'Claude Code'})`}
          value={description}
          onChange={(e) => setDescription(e.target.value)}
          required={mode !== 'loop'}
        />
        <VoiceButton onTranscribed={(text) => setDescription((prev) => prev ? prev + ' ' + text : text)} />
      </div>
      {/* Image attachments */}
      <div className="flex items-center gap-2 flex-wrap">
        <input
          ref={fileInputRef}
          type="file"
          multiple
          className="hidden"
          onChange={handleFileSelect}
        />
        <SecretPicker selectedIds={selectedSecretIds} onChange={setSelectedSecretIds} />
        {fileUpload.uploads.map((upload) => (
          <div key={upload.id} className="relative rounded overflow-hidden border border-gray-600">
            {upload.preview ? (
              <div className="w-12 h-12">
                <img src={upload.preview} alt="" className="w-full h-full object-cover" />
              </div>
            ) : (
              <div className="flex items-center gap-1.5 px-2.5 py-1.5 bg-gray-800 text-xs text-gray-300 max-w-[120px]">
                <Paperclip size={12} className="shrink-0" />
                <span className="truncate">
                  {upload.file?.name || upload.result?.filename || 'attachment'}
                </span>
              </div>
            )}
            {upload.status === 'uploading' && (
              <div className="absolute inset-0 bg-black/50 flex items-center justify-center">
                <Loader2 size={16} className="animate-spin text-white" />
              </div>
            )}
            {upload.status === 'failed' && (
              <div className="absolute inset-0 bg-red-900/50 flex items-center justify-center cursor-pointer" onClick={() => fileUpload.retryFile(upload.id)} title="Click to retry">
                <AlertCircle size={16} className="text-red-400" />
              </div>
            )}
            <button
              type="button"
              onClick={() => fileUpload.removeFile(upload.id)}
              className="absolute top-0 right-0 bg-gray-900/80 rounded-bl p-0.5 text-gray-300 hover:text-foreground"
            >
              <X size={10} />
            </button>
          </div>
        ))}
      </div>
      <div className="space-y-2">
        <ProjectSelect
          projects={projects.filter((p) => p.show_in_selector)}
          value={isNewProject ? NEW_PROJECT_VALUE : projectId || undefined}
          onChange={handleProjectChange}
          placeholder="Select project..."
          extraOptions={hasWorker ? [{ value: NEW_PROJECT_VALUE, label: '+ New project' }] : []}
          className="w-full"
          showStatus
          tagColorMap={Object.fromEntries(tagItems.map((t) => [t.name, t.color]))}
        />
        {isNewProject && (
          <div className="flex flex-col sm:flex-row gap-2">
            <input
              className="flex-1 bg-gray-700 text-foreground rounded px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500"
              placeholder="Project name (required)"
              value={newProjectName}
              onChange={(e) => setNewProjectName(e.target.value)}
              required
            />
            <input
              className="flex-1 min-w-0 bg-gray-700 text-foreground rounded px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500"
              placeholder="Remote repo URL (optional)"
              value={newProjectUrl}
              onChange={(e) => setNewProjectUrl(e.target.value)}
            />
          </div>
        )}
      </div>
      {provider === 'claude' && contextTasks.length > 0 && (
        <div className="flex items-center gap-2 min-w-0">
          <label className="text-sm text-gray-400 whitespace-nowrap shrink-0">Copy context from:</label>
          <select
            className="flex-1 min-w-0 bg-gray-700 text-foreground rounded px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500"
            value={cloneFromTaskId}
            onChange={(e) => setCloneFromTaskId(e.target.value ? Number(e.target.value) : '')}
          >
            <option value="">None (start fresh)</option>
            {contextTasks.map((t) => (
              <option key={t.id} value={t.id}>
                #{t.id} {t.description ? t.description.slice(0, 60) : t.title || '(no description)'}
                {t.description && t.description.length > 60 ? '…' : ''}
              </option>
            ))}
          </select>
        </div>
      )}
      {/* Mode-specific inputs (loop/goal) */}
      {mode === 'loop' && (
        <div className="flex items-center gap-2 flex-wrap">
          <input
            className="flex-1 min-w-0 bg-gray-700 text-foreground rounded px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500"
            placeholder="Todo file path (e.g. TODO.md)"
            value={todoFilePath}
            onChange={(e) => setTodoFilePath(e.target.value)}
            required
          />
          <label className="text-xs text-gray-400 whitespace-nowrap">Max iter:</label>
          <input
            type="text"
            inputMode="numeric"
            className="w-16 bg-gray-700 text-foreground rounded px-2 py-1.5 text-sm"
            value={maxIterations}
            onChange={(e) => setMaxIterations(e.target.value.replace(/[^0-9]/g, ''))}
            onBlur={() => {
              const n = parseInt(maxIterations);
              setMaxIterations(String((!n || n < 1) ? 1 : n));
            }}
          />
          <label className="flex items-center gap-1 text-xs text-gray-400 whitespace-nowrap cursor-pointer">
            <input
              type="checkbox"
              checked={mustComplete}
              onChange={(e) => setMustComplete(e.target.checked)}
              className="accent-indigo-500"
            />
            Must complete
          </label>
        </div>
      )}
      {mode === 'goal' && (
        <div className="flex items-center gap-2 flex-wrap">
          <input
            className="flex-1 min-w-0 bg-gray-700 text-foreground rounded px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500"
            placeholder="Goal condition (e.g. all tests pass and lint is clean)"
            value={goalCondition}
            onChange={(e) => setGoalCondition(e.target.value)}
            required
          />
          <label className="text-xs text-gray-400 whitespace-nowrap">Max turns:</label>
          <input
            type="text"
            inputMode="numeric"
            className="w-16 bg-gray-700 text-foreground rounded px-2 py-1.5 text-sm"
            value={goalMaxTurns}
            onChange={(e) => setGoalMaxTurns(e.target.value.replace(/[^0-9]/g, ''))}
            onBlur={() => {
              const n = parseInt(goalMaxTurns);
              setGoalMaxTurns(String((!n || n < 1) ? 1 : n));
            }}
          />
        </div>
      )}
      {/* Bottom action row */}
      <div className="flex items-center gap-2 flex-wrap">
        {/* Attach files */}
        <button
          type="button"
          onClick={() => fileInputRef.current?.click()}
          disabled={fileUpload.uploads.length >= 10}
          className="flex items-center gap-1 text-xs px-2 py-1.5 rounded border transition-colors bg-gray-700 text-gray-400 border-gray-600 hover:bg-gray-600 hover:text-gray-300 disabled:opacity-40"
        >
          <Paperclip size={13} />
          <span className="hidden sm:inline">{fileUpload.uploads.length > 0 ? `${fileUpload.uploads.length}/10 files` : 'Attach files'}</span>
          {fileUpload.uploads.length > 0 && <span className="sm:hidden">{fileUpload.uploads.length}</span>}
        </button>
        {/* Config dropdown */}
        <div ref={configRef} className="relative">
          <button
            type="button"
            onClick={() => setShowConfigPanel(!showConfigPanel)}
            className={`flex items-center gap-1 text-xs px-2 py-1.5 rounded border transition-colors ${
              hasNonDefaultConfig
                ? 'bg-amber-600/30 text-amber-300 border-amber-500/50 hover:bg-amber-600/40'
                : 'bg-gray-700 text-gray-400 border-gray-600 hover:bg-gray-600 hover:text-gray-300'
            }`}
          >
            <Settings size={13} />
            <span className="hidden sm:inline">Config</span>
          </button>
          {showConfigPanel && (
            <div className="absolute top-full mt-1 left-0 bg-gray-800 border border-gray-600 rounded shadow-lg z-20 p-3 min-w-[280px]">
              <div className="grid grid-cols-[auto_1fr] gap-x-3 gap-y-2 items-center text-xs">
                <span className="text-gray-400">Priority</span>
                <select
                  className="bg-gray-700 text-foreground rounded px-2 py-1 text-xs"
                  value={priority}
                  onChange={(e) => setPriority(Number(e.target.value))}
                >
                  {[0, 1, 2, 3, 4, 5, 6, 7, 8, 9].map((p) => (
                    <option key={p} value={p}>{p}</option>
                  ))}
                </select>

                <span className="text-gray-400">Mode</span>
                <select
                  className="bg-gray-700 text-foreground rounded px-2 py-1 text-xs"
                  value={mode}
                  onChange={(e) => {
                    const nextMode = e.target.value;
                    setMode(nextMode);
                  }}
                >
                  <option value="auto">Auto</option>
                  <option value="loop">Loop</option>
                  <option value="goal">Goal</option>
                </select>

                {false && (
                  <>
                    {/* Run on removed — Task inherits from Project */}
                    <span className="text-gray-400">Run on</span>
                    <select className="hidden" value={workerId} onChange={(e) => setWorkerId(e.target.value)}>
                    </select>
                  </>
                )}

                <span className="text-gray-400">CLI</span>
                <select
                  className="bg-gray-700 text-foreground rounded px-2 py-1 text-xs"
                  value={provider}
                  onChange={(e) => {
                    const nextProvider = e.target.value;
                    setProvider(nextProvider);
                    setModel('');
                    setEffort('');
                    if (nextProvider !== 'codex') setCodexServiceTier('default');
                  }}
                >
                  {providerOptions.map((p) => (
                    <option key={p} value={p}>{p === 'claude' ? 'Claude' : p === 'codex' ? 'Codex' : p}</option>
                  ))}
                </select>

                <span className="text-gray-400">Model</span>
                <select
                  className="bg-gray-700 text-foreground rounded px-2 py-1 text-xs"
                  value={model}
                  onChange={(e) => {
                    const nextModel = e.target.value;
                    setModel(nextModel);
                    const resolvedModel = nextModel || defaultCodexModel;
                    if (
                      provider === 'codex'
                      && codexCapabilitiesLoaded
                      && codexServiceTier === 'priority'
                      && !codexModelSupportsFast(resolvedModel)
                    ) {
                      setCodexServiceTier('default');
                    }
                  }}
                >
                  <option value="">{activeDefaultModel} (default)</option>
                  {activeModelOptions.map((m) => (
                    <option key={m} value={m}>{m}</option>
                  ))}
                </select>

                <span className="text-gray-400">Effort</span>
                <select
                  className="bg-gray-700 text-foreground rounded px-2 py-1 text-xs"
                  value={effort}
                  onChange={(e) => setEffort(e.target.value)}
                >
                  <option value="">{defaultEffort} (default)</option>
                  {activeEffortOptions.filter((e) => e !== defaultEffort).map((e) => (
                    <option key={e} value={e}>{e}</option>
                  ))}
                </select>

                {provider === 'codex' && (
                  <>
                    <span className="text-gray-400">Speed</span>
                    <select
                      aria-label="Codex speed"
                      className="bg-gray-700 text-foreground rounded px-2 py-1 text-xs"
                      value={codexServiceTier}
                      onChange={(e) => setCodexServiceTier(e.target.value as CodexServiceTier)}
                    >
                      <option value="default">Standard</option>
                      <option value="priority" disabled={!activeCodexModelSupportsFast}>Fast</option>
                    </select>
                    <span
                      className={`col-span-2 text-[10px] ${
                        activeCodexModelSupportsFast ? 'text-amber-400/80' : 'text-gray-500'
                      }`}
                    >
                      {activeCodexModelSupportsFast
                        ? 'Fast 约 1.5×，会消耗更多额度；实际计价取决于账号来源'
                        : `${activeCodexModel} 不支持 Fast`}
                    </span>
                  </>
                )}

                {/* Thinking 预算走 MAX_THINKING_TOKENS，claude 专属
                    （codex 的推理强度就是上面的 Effort）——codex 下隐藏幽灵选项 */}
                {provider === 'claude' && (
                  <>
                    <span className="text-gray-400">Thinking</span>
                    <select
                      className="bg-gray-700 text-foreground rounded px-2 py-1 text-xs"
                      value={thinkingBudget}
                      onChange={(e) => setThinkingBudget(e.target.value)}
                    >
                      <option value="">default</option>
                      <option value="4096">4k</option>
                      <option value="8192">8k</option>
                      <option value="16384">16k</option>
                      <option value="32768">32k</option>
                      <option value="65536">64k</option>
                      <option value="131072">128k</option>
                    </select>
                  </>
                )}

                <span className="text-gray-400">Timeout</span>
                <select
                  className="bg-gray-700 text-foreground rounded px-2 py-1 text-xs"
                  value={timeoutHours}
                  onChange={(e) => setTimeoutHours(e.target.value)}
                >
                  <option value="">default</option>
                  <option value="0.5">30 min</option>
                  <option value="1">1 hour</option>
                  <option value="2">2 hours</option>
                  <option value="4">4 hours</option>
                  <option value="8">8 hours</option>
                  <option value="12">12 hours</option>
                  <option value="24">24 hours</option>
                  <option value="0">No limit</option>
                </select>

                <span className="text-gray-400">System Prompt</span>
                <select
                  className="bg-gray-700 text-foreground rounded px-2 py-1 text-xs"
                  value={systemPromptMode}
                  onChange={(e) => setSystemPromptMode(e.target.value)}
                >
                  <option value="">Off</option>
                  <option value="append">Fable 5 (Append)</option>
                  <option value="replace">Fable 5 (Replace)</option>
                </select>
              </div>
              <div className="flex items-center gap-2 mt-3 pt-2 border-t border-gray-700">
                <button
                  type="button"
                  onClick={saveAsDefault}
                  className="text-xs px-2 py-1 rounded bg-indigo-600/30 text-indigo-300 hover:bg-indigo-600/50 transition-colors"
                >
                  {defaultSaved ? '✓ 已保存' : '设为默认配置'}
                </button>
                {hasStoredDefault && !defaultSaved && (
                  <button
                    type="button"
                    onClick={clearDefault}
                    className="text-xs px-2 py-1 rounded text-gray-500 hover:text-gray-300 transition-colors"
                  >
                    清除默认
                  </button>
                )}
              </div>
            </div>
          )}
        </div>
        {/* Star */}
        <button
          type="button"
          onClick={() => setStarOnCreate(!starOnCreate)}
          className={`flex items-center gap-1 text-xs px-2 py-1.5 rounded border transition-colors ${
            starOnCreate
              ? 'bg-yellow-600/30 text-yellow-300 border-yellow-500/50 hover:bg-yellow-600/40'
              : 'bg-gray-700 text-gray-400 border-gray-600 hover:bg-gray-600 hover:text-gray-300'
          }`}
        >
          <Star size={13} fill={starOnCreate ? 'currentColor' : 'none'} />
        </button>
        {(provider !== 'codex' || codexTaskSkillsEnabled) && (
          <div ref={skillsRef} className="relative">
            <button
              type="button"
              onClick={() => setShowSkillsDropdown(!showSkillsDropdown)}
              className={`flex items-center gap-1 text-xs px-2 py-1.5 rounded border transition-colors ${
                enabledUserSkillCount > 0
                  ? 'bg-green-600/30 text-green-300 border-green-500/50 hover:bg-green-600/40'
                  : 'bg-gray-700 text-gray-400 border-gray-600 hover:bg-gray-600 hover:text-gray-300'
              }`}
            >
              <span className="text-xs">📝</span>
              <span className="hidden sm:inline">Skills{enabledUserSkillCount > 0 ? ` (${enabledUserSkillCount})` : ''}</span>
              {enabledUserSkillCount > 0 && <span className="sm:hidden">{enabledUserSkillCount}</span>}
            </button>
            {showSkillsDropdown && (
              <div className="absolute top-full mt-1 left-0 bg-gray-800 border border-gray-600 rounded shadow-lg z-20 min-w-[180px]">
                {userSkills.length === 0 ? (
                  <div className="px-3 py-2 text-xs text-gray-500">暂无 Skill，去 Skills 页面创建</div>
                ) : userSkills.map((skill) => (
                  <label
                    key={skill.id}
                    className="flex items-center gap-2 px-3 py-2 text-xs text-gray-300 hover:bg-gray-700 cursor-pointer"
                    title={skill.description}
                  >
                    <input
                      type="checkbox"
                      checked={!!enabledUserSkills[skill.id]}
                      onChange={(e) => setEnabledUserSkills((prev) => ({ ...prev, [skill.id]: e.target.checked }))}
                      className="accent-green-500"
                    />
                    {skill.name}
                  </label>
                ))}
                <div className="border-t border-gray-700 px-3 py-1.5">
                  <button
                    type="button"
                    onClick={saveSkillsAsDefault}
                    className="flex items-center gap-1 text-xs text-gray-400 hover:text-indigo-300 transition-colors"
                    title="Save current Skills & Plugins selection as default for new tasks"
                  >
                    <Pin size={11} />
                    {skillsDefaultSaved ? '已保存为默认' : '设为默认'}
                  </button>
                </div>
              </div>
            )}
          </div>
        )}
        {provider === 'codex' && (
          <span
            className="text-xs text-gray-500 px-1 py-1.5 whitespace-nowrap"
            title={!codexTaskSkillsEnabled
              ? 'Codex 主任务 MCP 已关闭；仅 Sub-Agent 可用'
              : remoteTaskScope
                ? 'Codex Monitor 当前仅支持本地、非共享任务'
                : codexMonitorEnabled
                  ? 'Codex Monitor 已对本地任务开放'
                  : '当前后端尚未声明 Codex Monitor capability'}
          >
            {!codexTaskSkillsEnabled
              ? '主任务 MCP 已关闭 · 仅 Sub-Agent 可用'
              : remoteTaskScope
                ? 'Monitor 仅支持本地 Codex'
                : codexMonitorEnabled
                  ? '本地 Codex Monitor 已启用'
                  : 'Codex Monitor capability 未知'}
          </span>
        )}
        {/* Plugins dropdown */}
        {AVAILABLE_PLUGINS.length > 0 && (
          <div ref={pluginsRef} className="relative">
            <button
              type="button"
              onClick={() => setShowPluginsDropdown(!showPluginsDropdown)}
              className={`flex items-center gap-1 text-xs px-2 py-1.5 rounded border transition-colors ${
                enabledPluginCount > 0
                  ? 'bg-indigo-600/30 text-indigo-300 border-indigo-500/50 hover:bg-indigo-600/40'
                  : 'bg-gray-700 text-gray-400 border-gray-600 hover:bg-gray-600 hover:text-gray-300'
              }`}
            >
              <Wrench size={13} />
              <span className="hidden sm:inline">Plugins{enabledPluginCount > 0 ? ` (${enabledPluginCount})` : ''}</span>
              {enabledPluginCount > 0 && <span className="sm:hidden">{enabledPluginCount}</span>}
            </button>
            {showPluginsDropdown && (
              <div className="absolute top-full mt-1 left-0 bg-gray-800 border border-gray-600 rounded shadow-lg z-20 min-w-[180px]">
                {AVAILABLE_PLUGINS.map((tool) => {
                  const locked = false;
                  return (
                    <label
                      key={tool.key}
                      className={`flex items-center gap-2 px-3 py-2 text-xs transition-colors ${locked ? 'text-gray-500 cursor-default' : 'text-gray-300 hover:bg-gray-700 cursor-pointer'}`}
                      title={tool.description}
                    >
                      <input
                        type="checkbox"
                        checked={!!enabledPlugins[tool.key]}
                        onChange={(e) => !locked && setEnabledPlugins((prev) => ({ ...prev, [tool.key]: e.target.checked }))}
                        disabled={locked}
                        className="accent-indigo-500"
                      />
                      {tool.label}
                    </label>
                  );
                })}
                <div className="border-t border-gray-700 px-3 py-1.5">
                  <button
                    type="button"
                    onClick={saveSkillsAsDefault}
                    className="flex items-center gap-1 text-xs text-gray-400 hover:text-indigo-300 transition-colors"
                    title="Save current Skills & Plugins selection as default for new tasks"
                  >
                    <Pin size={11} />
                    {skillsDefaultSaved ? '已保存为默认' : '设为默认'}
                  </button>
                </div>
              </div>
            )}
          </div>
        )}
        {/* Create */}
        <button
          type="submit"
          disabled={loading || !canSubmit}
          className="flex items-center gap-1 bg-indigo-600 hover:bg-indigo-700 text-white px-4 py-1.5 rounded text-xs font-medium disabled:opacity-50 ml-auto"
        >
          <Plus size={14} />
          <span className="hidden sm:inline">{loading ? 'Creating...' : 'Create'}</span>
        </button>
      </div>
    </form>
  );
}
