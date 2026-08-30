import { useState, useEffect } from 'react';
import { api } from '../../api/client';
import type { Instance } from '../../api/client';
import { Square, Trash2, Plus, Zap, ZapOff, Eraser } from '../icons';

interface InstanceGridProps {
  instances: Instance[];
  onRefresh: () => void;
  onViewLogs: (id: number) => void;
}

const statusColors: Record<string, string> = {
  idle: 'bg-gray-500',
  running: 'bg-green-500 animate-pulse',
  error: 'bg-red-500',
  stopped: 'bg-yellow-500',
};

export function InstanceGrid({ instances, onRefresh, onViewLogs }: InstanceGridProps) {
  const [newName, setNewName] = useState('');
  const [dispatcherRunning, setDispatcherRunning] = useState<boolean | null>(null);
  const [pendingAction, setPendingAction] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);

  useEffect(() => {
    api.dispatcherStatus()
      .then((s) => setDispatcherRunning(s.running))
      .catch((error) => setActionError(error instanceof Error ? error.message : String(error)));
  }, []);

  const runAction = async (name: string, action: () => Promise<void>) => {
    if (pendingAction) return;
    setPendingAction(name);
    setActionError(null);
    try {
      await action();
    } catch (error) {
      setActionError(error instanceof Error ? error.message : String(error));
    } finally {
      setPendingAction(null);
    }
  };

  const defaultWorkerName = () => {
    const used = new Set(instances.map((instance) => instance.name));
    let suffix = 1;
    while (used.has(`worker-${suffix}`)) suffix += 1;
    return `worker-${suffix}`;
  };

  const handleCreate = async () => {
    const name = newName.trim() || defaultWorkerName();
    await runAction('create', async () => {
      await api.createInstance({ name });
      setNewName('');
      await onRefresh();
    });
  };

  const handleDelete = async (instance: Instance) => {
    if (instance.status === 'running' || instance.pid || instance.current_task_id) return;
    if (!window.confirm(`Delete instance "${instance.name}"? This cannot be undone.`)) return;
    await runAction(`delete:${instance.id}`, async () => {
      await api.deleteInstance(instance.id);
      await onRefresh();
    });
  };

  const handleStop = async (instance: Instance) => {
    if (!instance.current_task_id) {
      setActionError('Instance has no current task owner; refresh before stopping.');
      return;
    }
    if (instance.current_task_turn_generation == null) {
      setActionError('Instance task turn cannot be verified; refresh before stopping.');
      return;
    }
    const expectedTaskId = instance.current_task_id;
    const expectedTaskTurnGeneration = instance.current_task_turn_generation;
    if (!window.confirm(
      `Stop instance "${instance.name}"? Its current task will be interrupted and returned to the queue when safe.`,
    )) return;
    await runAction(`stop:${instance.id}`, async () => {
      await api.stopInstance(
        instance.id,
        expectedTaskId,
        expectedTaskTurnGeneration,
        instance.pid,
        instance.started_at,
      );
      await onRefresh();
    });
  };

  const handleCleanup = async () => {
    const deadCount = instances.filter((i) => i.status === 'error' || i.status === 'stopped').length;
    if (deadCount === 0) return;
    if (!window.confirm(`Remove ${deadCount} error/stopped instance(s)?`)) return;
    await runAction('cleanup', async () => {
      const result = await api.cleanupInstances();
      if (result.skipped_running.length > 0) {
        setActionError(
          `Skipped ${result.skipped_running.length} instance(s) that still have active or orphaned process metadata.`,
        );
      }
      await onRefresh();
    });
  };

  const toggleDispatcher = async () => {
    if (dispatcherRunning === null) return;
    if (dispatcherRunning) {
      if (!window.confirm(
        'Pause the Dispatcher? Running tasks will continue; only new task claims will stop.',
      )) return;
      await runAction('dispatcher', async () => {
        await api.stopDispatcher();
        const status = await api.dispatcherStatus();
        setDispatcherRunning(status.running);
        await onRefresh();
      });
    } else {
      await runAction('dispatcher', async () => {
        await api.startDispatcher();
        const status = await api.dispatcherStatus();
        setDispatcherRunning(status.running);
        await onRefresh();
      });
    }
  };

  return (
    <div className="space-y-3">
      {actionError && (
        <div role="alert" className="bg-red-500/20 text-red-400 px-3 py-2 rounded text-sm">
          {actionError}
        </div>
      )}
      <div className="flex gap-2 flex-wrap">
        <input
          className="flex-1 min-w-[120px] bg-gray-700 text-foreground rounded px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500"
          placeholder="Instance name (optional)"
          value={newName}
          onChange={(e) => setNewName(e.target.value)}
        />
        <button
          onClick={handleCreate}
          disabled={pendingAction !== null}
          className="flex items-center gap-1 bg-indigo-600 hover:bg-indigo-700 text-white px-3 py-2 rounded text-sm font-medium whitespace-nowrap disabled:opacity-50 disabled:cursor-not-allowed"
        >
          <Plus size={16} /> {pendingAction === 'create' ? 'Adding…' : 'Add'}
        </button>
        <button
          onClick={toggleDispatcher}
          disabled={dispatcherRunning === null || pendingAction !== null}
          title={dispatcherRunning ? 'Pause new task claims; running tasks continue' : 'Allow the Dispatcher to claim queued tasks'}
          className={`flex items-center gap-1 px-3 py-2 rounded text-sm font-medium whitespace-nowrap ${
            dispatcherRunning
              ? 'bg-yellow-600/20 text-yellow-400 hover:bg-yellow-600/30'
              : 'bg-green-600/20 text-green-400 hover:bg-green-600/30'
          } disabled:opacity-50 disabled:cursor-not-allowed`}
        >
          {dispatcherRunning ? <ZapOff size={16} /> : <Zap size={16} />}
          {pendingAction === 'dispatcher'
            ? 'Updating…'
            : dispatcherRunning === null
              ? 'Loading Dispatcher…'
              : dispatcherRunning
                ? 'Pause Dispatcher'
                : 'Start Dispatcher'}
        </button>
        <button
          onClick={handleCleanup}
          disabled={pendingAction !== null || !instances.some((i) => i.status === 'error' || i.status === 'stopped')}
          className="flex items-center gap-1 px-3 py-2 rounded text-sm font-medium whitespace-nowrap bg-red-600/20 text-red-400 hover:bg-red-600/30 disabled:opacity-30 disabled:cursor-not-allowed"
        >
          <Eraser size={16} /> {pendingAction === 'cleanup' ? 'Cleaning…' : 'Clean Up'}
        </button>
      </div>
      <p className="text-xs text-gray-500">
        Pausing the Dispatcher stops new task claims. Tasks that are already running continue normally.
      </p>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
        {instances.map((inst) => (
          <div key={inst.id} className="bg-gray-800 rounded-lg p-4 space-y-2">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <span className={`w-2.5 h-2.5 rounded-full ${statusColors[inst.status] || 'bg-gray-500'}`} />
                <span className="text-foreground font-medium text-sm">{inst.name}</span>
              </div>
              <span className="text-xs text-gray-500">#{inst.id}</span>
            </div>

            <div className="text-xs text-gray-400 space-y-0.5">
              <p>Status: <span className="text-gray-300">{inst.status}</span></p>
              <p>Completed: <span className="text-gray-300">{inst.total_tasks_completed}</span></p>
              {inst.current_task_id && <p>Task: <span className="text-indigo-400">#{inst.current_task_id}</span></p>}
              {inst.pid && <p>PID: <span className="text-gray-300">{inst.pid}</span></p>}
            </div>

            <div className="flex gap-1 pt-1">
              <button
                onClick={() => onViewLogs(inst.id)}
                className="px-2 py-1 rounded text-xs font-medium bg-gray-700 text-gray-300 hover:bg-gray-600"
              >
                Logs
              </button>
              {inst.status === 'running' && (
                <button
                  onClick={() => handleStop(inst)}
                  disabled={pendingAction !== null || !inst.current_task_id || inst.current_task_turn_generation == null}
                  className="p-1 text-gray-400 hover:text-yellow-400 disabled:opacity-40 disabled:cursor-not-allowed"
                  title="Stop current task"
                  aria-label={`Stop ${inst.name}`}
                >
                  <Square size={14} />
                </button>
              )}
              <button
                onClick={() => handleDelete(inst)}
                disabled={pendingAction !== null || inst.status === 'running' || !!inst.pid || !!inst.current_task_id}
                className="p-1 text-gray-400 hover:text-red-400 ml-auto disabled:opacity-30 disabled:cursor-not-allowed disabled:hover:text-gray-400"
                title={(inst.status === 'running' || inst.pid || inst.current_task_id)
                  ? 'Resolve or stop this instance before deleting it'
                  : 'Delete instance'}
                aria-label={`Delete ${inst.name}`}
              >
                <Trash2 size={14} />
              </button>
            </div>
          </div>
        ))}
      </div>

      {instances.length === 0 && (
        <p className="text-gray-500 text-sm text-center py-8">No instances. Dispatcher will auto-create workers on start.</p>
      )}
    </div>
  );
}
