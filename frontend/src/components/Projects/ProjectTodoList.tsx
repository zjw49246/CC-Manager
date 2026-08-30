import { useCallback, useEffect, useMemo, useState } from 'react';
import type { FormEvent, ReactNode } from 'react';
import {
  Archive,
  CheckCircle2,
  ChevronDown,
  ChevronRight,
  Circle,
  Pencil,
  Play,
  Plus,
  RotateCcw,
  Save,
  Trash2,
  X,
} from '../icons';
import { api } from '../../api/client';
import type {
  DeliveryRunCreate,
  MonitoredRepo,
  Project,
  ProjectTodo,
} from '../../api/client';
import {
  acknowledgeDeliveryAdmission,
  deliveryProviderOptions,
  prepareDeliveryAdmission,
  resolveDeliveryProvider,
} from '../Tasks/deliveryAdmission';
import { filterDeliveryRepos } from '../Tasks/deliveryCompatibility';

interface ProjectTodoListProps {
  projectId: number;
  project?: Project;
}

interface TodoDraft {
  title: string;
  prompt: string;
}

const emptyDraft: TodoDraft = { title: '', prompt: '' };

function deliveryAdmissionScope(projectId: number, todoId: number): string {
  return `project-todo:${projectId}:${todoId}`;
}

export function ProjectTodoList({ projectId, project }: ProjectTodoListProps) {
  const [expanded, setExpanded] = useState(false);
  const [todos, setTodos] = useState<ProjectTodo[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [showCreate, setShowCreate] = useState(false);
  const [showArchived, setShowArchived] = useState(false);
  const [hasLoaded, setHasLoaded] = useState(false);
  const [createDraft, setCreateDraft] = useState<TodoDraft>(emptyDraft);
  const [editingId, setEditingId] = useState<number | null>(null);
  const [editDraft, setEditDraft] = useState<TodoDraft>(emptyDraft);
  const [taskTodo, setTaskTodo] = useState<ProjectTodo | null>(null);
  const [taskDraft, setTaskDraft] = useState<TodoDraft>(emptyDraft);
  const [taskProvider, setTaskProvider] = useState('codex');
  const [providerOptions, setProviderOptions] = useState<string[]>(['claude', 'codex']);
  const [providerConfigLoaded, setProviderConfigLoaded] = useState(false);
  const [taskMode, setTaskMode] = useState<'auto' | 'delivery_loop'>('auto');
  const [deliveryLoopEnabled, setDeliveryLoopEnabled] = useState(false);
  const [monitoredRepos, setMonitoredRepos] = useState<MonitoredRepo[]>([]);
  const [deliveryRepoId, setDeliveryRepoId] = useState<number | ''>('');
  const [saving, setSaving] = useState(false);
  const [running, setRunning] = useState(false);
  const [updatingIds, setUpdatingIds] = useState<Set<number>>(() => new Set());

  const openCount = useMemo(() => todos.filter((todo) => todo.status === 'open').length, [todos]);
  const deliveryProviders = useMemo(
    () => deliveryProviderOptions(providerOptions),
    [providerOptions],
  );
  const compatibleDeliveryRepos = useMemo(
    () => filterDeliveryRepos(
      project?.id === projectId ? project : undefined,
      monitoredRepos,
      providerConfigLoaded ? deliveryProviders : [],
    ),
    [deliveryProviders, monitoredRepos, project, projectId, providerConfigLoaded],
  );
  const selectedDeliveryProvider = resolveDeliveryProvider(
    taskProvider,
    undefined,
    deliveryProviders,
  );
  const selectedDeliveryRepo = compatibleDeliveryRepos.find(
    (repo) => repo.id === deliveryRepoId,
  );

  // Track in-flight mutations per row. A single scalar would let one row's
  // completion clear another row's busy flag mid-flight and allow double-submits.
  const startBusy = (id: number) => setUpdatingIds((prev) => new Set(prev).add(id));
  const endBusy = (id: number) =>
    setUpdatingIds((prev) => {
      const next = new Set(prev);
      next.delete(id);
      return next;
    });

  const loadTodos = useCallback(async () => {
    setLoading(true);
    setError('');
    try {
      setTodos(await api.listProjectTodos(projectId, showArchived));
      setHasLoaded(true);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }, [projectId, showArchived]);

  useEffect(() => {
    if (expanded) {
      loadTodos();
    }
  }, [expanded, loadTodos]);

  const openCreateModal = () => {
    setError('');
    setCreateDraft(emptyDraft);
    setExpanded(true);
    setShowCreate(true);
  };

  const createTodo = async (event: FormEvent) => {
    event.preventDefault();
    setSaving(true);
    setError('');
    try {
      await api.createProjectTodo(projectId, createDraft);
      setShowCreate(false);
      setCreateDraft(emptyDraft);
      await loadTodos();
    } catch (e) {
      setError(String(e));
    } finally {
      setSaving(false);
    }
  };

  const startEdit = (todo: ProjectTodo) => {
    setEditingId(todo.id);
    setEditDraft({ title: todo.title, prompt: todo.prompt });
  };

  const saveEdit = async (todoId: number) => {
    if (!editDraft.title.trim() || !editDraft.prompt.trim()) {
      setError('Title and prompt are required.');
      return;
    }
    startBusy(todoId);
    setError('');
    try {
      const updated = await api.updateProjectTodo(projectId, todoId, editDraft);
      setTodos((prev) => prev.map((todo) => (todo.id === todoId ? updated : todo)));
      setEditingId(null);
    } catch (e) {
      setError(String(e));
    } finally {
      endBusy(todoId);
    }
  };

  const setStatus = async (todo: ProjectTodo, status: ProjectTodo['status']) => {
    startBusy(todo.id);
    setError('');
    try {
      const updated = await api.updateProjectTodo(projectId, todo.id, { status });
      // If the new status falls outside the current view, drop it; else update in place.
      setTodos((prev) =>
        status === 'archived' && !showArchived
          ? prev.filter((item) => item.id !== todo.id)
          : prev.map((item) => (item.id === todo.id ? updated : item)),
      );
    } catch (e) {
      setError(String(e));
    } finally {
      endBusy(todo.id);
    }
  };

  const toggleDone = (todo: ProjectTodo) => setStatus(todo, todo.status === 'done' ? 'open' : 'done');

  const archiveTodo = (todo: ProjectTodo) => {
    if (!confirm('Archive this todo? You can restore it from "Show archived".')) return;
    setStatus(todo, 'archived');
  };

  const restoreTodo = (todo: ProjectTodo) => setStatus(todo, 'open');

  const openCreatedTask = (todo: ProjectTodo) => {
    if (todo.created_task_id == null) return;
    window.location.hash = `#/tasks/chat/${todo.created_task_id}`;
  };

  const deleteTodo = async (todo: ProjectTodo) => {
    if (!confirm('Permanently delete this todo? This cannot be undone.')) return;
    startBusy(todo.id);
    setError('');
    try {
      await api.deleteProjectTodo(projectId, todo.id);
      setTodos((prev) => prev.filter((item) => item.id !== todo.id));
    } catch (e) {
      setError(String(e));
    } finally {
      endBusy(todo.id);
    }
  };

  const openTaskModal = (todo: ProjectTodo) => {
    if (todo.created_task_id != null) {
      openCreatedTask(todo);
      return;
    }
    setError('');
    setTaskTodo(todo);
    setTaskDraft({ title: todo.title, prompt: todo.prompt });
    setTaskMode('auto');
    setDeliveryRepoId('');
    setProviderConfigLoaded(false);
    setDeliveryLoopEnabled(false);
    setMonitoredRepos([]);
    api.config().then((c) => {
      const configuredProviders = c.provider_options?.length
        ? c.provider_options
        : ['claude', 'codex'];
      setProviderOptions(configuredProviders);
      setProviderConfigLoaded(true);
      setTaskProvider((current) => (
        resolveDeliveryProvider(c.default_provider, current, configuredProviders) ?? current
      ));
      const enabled = c.delivery_loop_enabled === true;
      setDeliveryLoopEnabled(enabled);
      if (enabled) {
        api.getMonitoredRepos().then(setMonitoredRepos).catch(() => setMonitoredRepos([]));
      } else {
        setMonitoredRepos([]);
      }
    }).catch(() => {
      setProviderConfigLoaded(true);
    });
  };

  const createTask = async (event: FormEvent) => {
    event.preventDefault();
    if (!taskTodo || running) return;
    setRunning(true);
    setError('');
    let deliveryRequest: DeliveryRunCreate | null = null;
    let admissionScope: string | null = null;
    try {
      let taskId: number | null = null;
      let todoClaimedAtomically = false;
      if (taskMode === 'delivery_loop') {
        const repo = compatibleDeliveryRepos.find((item) => item.id === deliveryRepoId);
        if (!repo) throw new Error('Select a compatible PR Monitor repository.');
        if (!selectedDeliveryProvider) {
          throw new Error('Select a supported Delivery provider.');
        }
        admissionScope = deliveryAdmissionScope(projectId, taskTodo.id);
        deliveryRequest = prepareDeliveryAdmission(admissionScope, {
          project_id: projectId,
          monitored_repo_id: repo.id,
          source_todo_id: taskTodo.id,
          title: taskDraft.title,
          requirements: taskDraft.prompt,
          provider: selectedDeliveryProvider,
        });
        const run = await api.createDeliveryRun(deliveryRequest);
        taskId = run.developer_task_id;
        todoClaimedAtomically = true;
      } else {
        const task = await api.createTaskFromProjectTodo(projectId, taskTodo.id, {
          title: taskDraft.title,
          prompt: taskDraft.prompt,
          provider: taskProvider === 'claude' ? 'claude' : 'codex',
        });
        taskId = task?.id || null;
        todoClaimedAtomically = true;
      }
      if (!taskId) {
        throw new Error('Task was created but returned no id');
      }
      if (!todoClaimedAtomically) throw new Error('Todo admission was not atomic');
      if (deliveryRequest && admissionScope) {
        acknowledgeDeliveryAdmission(admissionScope, deliveryRequest);
      }
      window.location.hash = `#/tasks/chat/${taskId}`;
    } catch (caught) {
      // The create response may have been lost after the backend atomically
      // claimed the Todo. Re-read before inviting a retry: a durable claim is
      // authoritative and gives us the Task to navigate to without duplicating
      // a Delivery Run.
      try {
        const refreshed = await api.listProjectTodos(projectId, showArchived);
        setTodos(refreshed);
        const claimed = refreshed.find((todo) => todo.id === taskTodo.id);
        if (claimed?.created_task_id != null) {
          if (deliveryRequest && admissionScope) {
            acknowledgeDeliveryAdmission(admissionScope, deliveryRequest);
          }
          window.location.hash = `#/tasks/chat/${claimed.created_task_id}`;
          return;
        }
      } catch {
        // Preserve the original admission error and durable key when recovery
        // cannot be confirmed.
      }
      setError(String(caught));
      setRunning(false);
    }
  };

  return (
    <div className="mt-3 border-t border-gray-700 pt-3">
      <div className="flex items-center justify-between gap-2">
        <button
          type="button"
          onClick={() => setExpanded((value) => !value)}
          className="flex h-8 min-w-0 items-center gap-2 rounded px-2 text-sm text-gray-300 hover:bg-gray-700 hover:text-foreground"
          title={expanded ? 'Collapse todos' : 'Expand todos'}
        >
          {expanded ? <ChevronDown size={16} /> : <ChevronRight size={16} />}
          <span className="font-medium">To-dos</span>
          {hasLoaded && (
            <span className="rounded bg-gray-700 px-1.5 py-0.5 text-xs text-gray-300">{openCount}</span>
          )}
        </button>
        <div className="flex items-center gap-2">
          {expanded && (
            <button
              type="button"
              onClick={() => setShowArchived((value) => !value)}
              className={`flex h-8 items-center gap-1.5 rounded px-2.5 text-xs ${
                showArchived ? 'bg-gray-700 text-gray-300' : 'text-gray-500 hover:bg-gray-700 hover:text-gray-300'
              }`}
              title={showArchived ? 'Hide archived todos' : 'Show archived todos'}
            >
              <Archive size={13} /> {showArchived ? 'Hide archived' : 'Show archived'}
            </button>
          )}
          <button
            type="button"
            onClick={openCreateModal}
            className="flex h-8 items-center gap-1.5 rounded bg-gray-700 px-2.5 text-sm text-gray-300 hover:bg-gray-700 hover:text-foreground"
            title="Add todo"
          >
            <Plus size={14} /> Add
          </button>
        </div>
      </div>

      {expanded && (
        <div className="mt-2 space-y-2">
          {error && <div className="rounded bg-red-500/15 px-3 py-2 text-xs text-red-400">{error}</div>}

          {loading ? (
            <div className="px-2 py-3 text-sm text-gray-500">Loading...</div>
          ) : todos.length === 0 ? (
            !error && <div className="px-2 py-3 text-sm text-gray-500">No to-dos.</div>
          ) : (
            <div className="space-y-1.5">
              {todos.map((todo) => {
                const isClaimed = todo.created_task_id != null;
                const isEditing = !isClaimed && editingId === todo.id;
                const isBusy = updatingIds.has(todo.id);

                if (todo.status === 'archived') {
                  return (
                    <div
                      key={todo.id}
                      className="flex items-center gap-2 rounded border border-gray-700 bg-gray-900/30 px-2.5 py-1.5"
                    >
                      <span className="min-w-0 flex-1 truncate text-xs text-gray-500 line-through">{todo.title}</span>
                      <span className="shrink-0 rounded bg-gray-700 px-1.5 py-0.5 text-[10px] text-gray-500">
                        archived
                      </span>
                      {isClaimed ? (
                        <button
                          type="button"
                          onClick={() => openCreatedTask(todo)}
                          className="h-7 w-7 rounded text-gray-500 hover:bg-gray-700 hover:text-green-400"
                          title={`Open created task #${todo.created_task_id}`}
                        >
                          <Play size={14} />
                        </button>
                      ) : (
                        <>
                          <button
                            type="button"
                            onClick={() => restoreTodo(todo)}
                            disabled={isBusy}
                            className="h-7 w-7 rounded text-gray-500 hover:bg-gray-700 hover:text-blue-400 disabled:opacity-60"
                            title="Restore todo"
                          >
                            <RotateCcw size={14} />
                          </button>
                          <button
                            type="button"
                            onClick={() => deleteTodo(todo)}
                            disabled={isBusy}
                            className="h-7 w-7 rounded text-gray-500 hover:bg-gray-700 hover:text-red-400 disabled:opacity-60"
                            title="Delete permanently"
                          >
                            <Trash2 size={14} />
                          </button>
                        </>
                      )}
                    </div>
                  );
                }

                return (
                  <div key={todo.id} className="rounded border border-gray-700 bg-gray-900/40 px-2.5 py-2">
                    {isEditing ? (
                      <div className="space-y-2">
                        <input
                          value={editDraft.title}
                          onChange={(e) => setEditDraft((prev) => ({ ...prev, title: e.target.value }))}
                          className="w-full rounded border border-gray-600 bg-gray-700 px-2 py-1.5 text-sm text-foreground outline-none focus:border-indigo-500"
                          placeholder="Title"
                        />
                        <textarea
                          value={editDraft.prompt}
                          onChange={(e) => setEditDraft((prev) => ({ ...prev, prompt: e.target.value }))}
                          className="min-h-24 w-full resize-y rounded border border-gray-600 bg-gray-700 px-2 py-1.5 text-sm text-foreground outline-none focus:border-indigo-500"
                          placeholder="Prompt"
                        />
                        <div className="flex justify-end gap-2">
                          <button
                            type="button"
                            onClick={() => setEditingId(null)}
                            className="flex h-8 items-center gap-1.5 rounded px-2.5 text-sm text-gray-300 hover:bg-gray-700"
                          >
                            <X size={14} /> Cancel
                          </button>
                          <button
                            type="button"
                            onClick={() => saveEdit(todo.id)}
                            disabled={isBusy}
                            className="flex h-8 items-center gap-1.5 rounded bg-indigo-600 px-2.5 text-sm text-white hover:bg-indigo-500 disabled:opacity-60"
                          >
                            <Save size={14} /> Save
                          </button>
                        </div>
                      </div>
                    ) : (
                      <div className="flex items-start gap-2">
                        {isClaimed ? (
                          <span className="mt-0.5 flex h-7 w-7 shrink-0 items-center justify-center text-gray-600">
                            {todo.status === 'done' ? <CheckCircle2 size={17} /> : <Circle size={17} />}
                          </span>
                        ) : (
                          <button
                            type="button"
                            onClick={() => toggleDone(todo)}
                            disabled={isBusy}
                            className="mt-0.5 h-7 w-7 shrink-0 rounded text-gray-500 hover:bg-gray-700 hover:text-green-400 disabled:opacity-60"
                            title={todo.status === 'done' ? 'Mark open' : 'Mark done'}
                          >
                          {todo.status === 'done' ? <CheckCircle2 size={17} /> : <Circle size={17} />}
                          </button>
                        )}
                        <div className="min-w-0 flex-1">
                          <div className={`truncate text-sm font-medium ${todo.status === 'done' ? 'text-gray-500 line-through' : 'text-foreground'}`}>
                            {todo.title}
                          </div>
                          <div className="mt-0.5 line-clamp-2 whitespace-pre-wrap text-xs text-gray-500">{todo.prompt}</div>
                        </div>
                        <div className="flex shrink-0 items-center gap-1">
                          {isClaimed ? (
                            <button
                              type="button"
                              onClick={() => openCreatedTask(todo)}
                              className="h-8 w-8 rounded text-gray-500 hover:bg-gray-700 hover:text-green-400"
                              title={`Open created task #${todo.created_task_id}`}
                            >
                              <Play size={15} />
                            </button>
                          ) : (
                            <>
                              <button
                                type="button"
                                onClick={() => openTaskModal(todo)}
                                className="h-8 w-8 rounded text-gray-500 hover:bg-gray-700 hover:text-green-400"
                                title="Create task"
                              >
                                <Play size={15} />
                              </button>
                              <button
                                type="button"
                                onClick={() => startEdit(todo)}
                                className="h-8 w-8 rounded text-gray-500 hover:bg-gray-700 hover:text-blue-400"
                                title="Edit todo"
                              >
                                <Pencil size={15} />
                              </button>
                              <button
                                type="button"
                                onClick={() => archiveTodo(todo)}
                                disabled={isBusy}
                                className="h-8 w-8 rounded text-gray-500 hover:bg-gray-700 hover:text-yellow-400 disabled:opacity-60"
                                title="Archive todo"
                              >
                                <Archive size={15} />
                              </button>
                            </>
                          )}
                        </div>
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          )}
        </div>
      )}

      {showCreate && (
        <TodoModal
          title="New todo"
          draft={createDraft}
          setDraft={setCreateDraft}
          submitLabel="Create todo"
          saving={saving}
          error={error}
          onClose={() => setShowCreate(false)}
          onSubmit={createTodo}
        />
      )}

      {taskTodo && (
        <TodoModal
          title="Create task"
          draft={taskDraft}
          setDraft={setTaskDraft}
          submitLabel="Create task"
          saving={running}
          error={error}
          onClose={() => setTaskTodo(null)}
          onSubmit={createTask}
          extraFields={
            <>
              <label className="block space-y-1.5">
                <span className="text-sm text-gray-300">Mode</span>
                <select
                  aria-label="Task mode"
                  value={taskMode}
                  onChange={(e) => {
                    const nextMode = e.target.value === 'delivery_loop' ? 'delivery_loop' : 'auto';
                    setTaskMode(nextMode);
                    if (nextMode === 'delivery_loop') {
                      const nextProvider = resolveDeliveryProvider(
                        taskProvider,
                        undefined,
                        deliveryProviders,
                      );
                      if (nextProvider) setTaskProvider(nextProvider);
                    }
                    setDeliveryRepoId('');
                  }}
                  className="w-full rounded border border-gray-600 bg-gray-700 px-3 py-2 text-sm text-foreground outline-none focus:border-indigo-500"
                >
                  <option value="auto">Auto</option>
                  {deliveryLoopEnabled && deliveryProviders.length > 0 && (
                    <option value="delivery_loop">Delivery Loop</option>
                  )}
                </select>
              </label>
              <label className="block space-y-1.5">
                <span className="text-sm text-gray-300">Provider</span>
                <select
                  aria-label="Task provider"
                  value={taskProvider}
                  onChange={(e) => setTaskProvider(e.target.value)}
                  className="w-full rounded border border-gray-600 bg-gray-700 px-3 py-2 text-sm text-foreground outline-none focus:border-indigo-500"
                >
                  {(taskMode === 'delivery_loop' ? deliveryProviders : providerOptions).map((p) => (
                    <option key={p} value={p}>{p === 'codex' ? 'Codex' : 'Claude Code'}</option>
                  ))}
                </select>
              </label>
              {taskMode === 'delivery_loop' && (
                <label className="block space-y-1.5">
                  <span className="text-sm text-gray-300">PR Monitor repository</span>
                  <select
                    aria-label="Delivery PR Monitor repository"
                    value={deliveryRepoId}
                    onChange={(e) => setDeliveryRepoId(e.target.value ? Number(e.target.value) : '')}
                    className="w-full rounded border border-gray-600 bg-gray-700 px-3 py-2 text-sm text-foreground outline-none focus:border-indigo-500"
                    required
                  >
                    <option value="">Select a compatible repository…</option>
                    {compatibleDeliveryRepos.map((repo) => (
                      <option key={repo.id} value={repo.id}>
                        {repo.repo_full_name} · {repo.auto_merge ? 'auto merge' : 'ready to merge only'}
                      </option>
                    ))}
                  </select>
                  {selectedDeliveryRepo && (
                    <span className="block text-xs text-gray-400">
                      PR Monitor Auto Merge is {selectedDeliveryRepo.auto_merge ? 'ON; completion waits for GitHub to confirm the merge.' : 'OFF; completion stops when the PR is ready to merge.'}
                    </span>
                  )}
                  {compatibleDeliveryRepos.length === 0 && (
                    <span className="block text-xs text-amber-300">
                      This project has no compatible panel-review PR Monitor with required CI checks.
                    </span>
                  )}
                </label>
              )}
            </>
          }
        />
      )}
    </div>
  );
}

function TodoModal({
  title,
  draft,
  setDraft,
  submitLabel,
  saving,
  error,
  onClose,
  onSubmit,
  extraFields,
}: {
  title: string;
  draft: TodoDraft;
  setDraft: (draft: TodoDraft) => void;
  submitLabel: string;
  saving: boolean;
  error?: string;
  onClose: () => void;
  onSubmit: (event: FormEvent) => void;
  extraFields?: ReactNode;
}) {
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
  }, [onClose]);

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4"
      onClick={onClose}
    >
      <form
        role="dialog"
        aria-modal="true"
        aria-label={title}
        onSubmit={onSubmit}
        onClick={(e) => e.stopPropagation()}
        className="flex max-h-[90vh] w-full max-w-2xl flex-col overflow-hidden rounded-xl bg-gray-800 shadow-2xl"
      >
        <div className="flex items-center justify-between border-b border-gray-700 px-5 py-4">
          <h3 className="font-semibold text-foreground">{title}</h3>
          <button type="button" onClick={onClose} className="text-gray-500 hover:text-foreground">
            <X size={18} />
          </button>
        </div>
        <div className="space-y-4 overflow-y-auto p-5">
          <label className="block space-y-1.5">
            <span className="text-sm text-gray-300">Title</span>
            <input
              value={draft.title}
              onChange={(e) => setDraft({ ...draft, title: e.target.value })}
              className="w-full rounded border border-gray-600 bg-gray-700 px-3 py-2 text-sm text-foreground outline-none focus:border-indigo-500"
              autoFocus
              required
            />
          </label>
          <label className="block space-y-1.5">
            <span className="text-sm text-gray-300">Prompt</span>
            <textarea
              value={draft.prompt}
              onChange={(e) => setDraft({ ...draft, prompt: e.target.value })}
              className="min-h-44 w-full resize-y rounded border border-gray-600 bg-gray-700 px-3 py-2 text-sm text-foreground outline-none focus:border-indigo-500"
              required
            />
          </label>
          {extraFields}
          {error && (
            <div className="rounded bg-red-500/15 px-3 py-2 text-xs text-red-400">{error}</div>
          )}
        </div>
        <div className="flex justify-end gap-2 border-t border-gray-700 px-5 py-4">
          <button
            type="button"
            onClick={onClose}
            className="rounded px-3 py-2 text-sm text-gray-300 hover:bg-gray-700"
          >
            Cancel
          </button>
          <button
            type="submit"
            disabled={saving}
            className="rounded bg-indigo-600 px-3 py-2 text-sm text-white hover:bg-indigo-500 disabled:opacity-60"
          >
            {saving ? 'Saving...' : submitLabel}
          </button>
        </div>
      </form>
    </div>
  );
}
