import { useState, useCallback, useRef, useEffect } from 'react';
import { createPortal } from 'react-dom';
import { ArrowUpCircle, RefreshCw } from '../icons';
import { api } from '../../api/client';
import { useWebSocket } from '../../hooks/useWebSocket';

interface StepInfo {
  name: string;
  status: string;
  duration_ms?: number | null;
  message?: string | null;
  result?: Record<string, unknown> | null;
}

interface UpdateStatusData {
  update_id?: string;
  status: string;
  steps?: StepInfo[];
  old_commit?: string;
  new_commit?: string;
  error?: string;
  current_step?: number;
  total_steps?: number;
  repair_required?: boolean;
  database_migration_required?: boolean;
  database_migration_applied?: boolean | null;
}

interface DeploymentCheck {
  has_updates?: boolean;
  needs_restart?: boolean;
  restart_only_safe?: boolean;
  repair_required?: boolean;
  repair_reasons?: string[];
  error?: string;
  commits_behind?: number;
  current_commit?: string;
  latest_commit?: string;
  disk_commit?: string;
  running_commit?: string;
  db_current_revision?: string | null;
  db_head_revision?: string | null;
  db_up_to_date?: boolean | null;
  db_in_sync?: boolean | null;
  commit_messages?: string[];
  has_new_migrations?: boolean;
  migration_count?: number;
  has_frontend_changes?: boolean;
  has_package_changes?: boolean;
  active_task_count?: number;
  active_tasks?: ActiveTaskSummary[];
  update_blocked?: boolean;
  remote?: string;
  branch?: string;
  channel?: 'stable' | 'main';
  latest_version?: string;
  version?: string;
  update_kind?: 'stable_upgrade' | 'stable_switch';
  is_stable_downgrade?: boolean;
  stable_switch_blocked?: boolean;
  [key: string]: unknown;
}

interface ActiveTaskSummary {
  id: number;
  title: string;
  status: string;
  kind?: 'task' | 'instance' | 'monitor' | 'sub_agent';
  instance_id?: number;
  instance_claim_count?: number;
}

type Phase = 'idle' | 'selecting' | 'checking' | 'confirming' | 'running' | 'restarting' | 'completed' | 'failed';
type DeploymentAction = 'update' | 'repair' | 'restart' | 'none';

const ACTIVE_UPDATE_KEY = 'ccm-update-active';
const ACTIVE_DEPLOYMENT_STATUSES = new Set([
  'claimed',
  'running',
  'backing_up',
  'restarting',
  'starting',
  'stopping',
  'migrating',
  'rolling_back',
]);

const STEP_LABELS: Record<string, string> = {
  git_pull: '拉取代码',
  detect_changes: '检测变更',
  backup_database: '备份数据库',
  uv_sync: 'Python 依赖',
  refresh_pty: 'PTY 依赖',
  npm_install: '前端依赖',
  frontend_build: '构建前端',
  stop_service: '停止服务',
  alembic_upgrade: '数据库迁移',
  start_service: '启动服务',
};

const STATUS_ICON: Record<string, string> = {
  pending: '○',
  running: '⏳',
  completed: '✅',
  failed: '❌',
  skipped: '⏭',
};

function errorMessage(error: unknown, fallback: string): string {
  return error instanceof Error && error.message ? error.message : fallback;
}

function setUpdateActive(active: boolean) {
  try {
    if (active) {
      sessionStorage.setItem(ACTIVE_UPDATE_KEY, '1');
    } else {
      sessionStorage.removeItem(ACTIVE_UPDATE_KEY);
    }
  } catch {
    // Storage can be unavailable in hardened/private browser contexts.
  }
}

function hasActiveUpdate() {
  try {
    return sessionStorage.getItem(ACTIVE_UPDATE_KEY) === '1';
  } catch {
    return false;
  }
}

function isActiveDeploymentStatus(status: string | undefined) {
  return Boolean(status && ACTIVE_DEPLOYMENT_STATUSES.has(status));
}

function deploymentAction(result: DeploymentCheck): DeploymentAction {
  if (result.repair_required) return 'repair';
  if (result.has_updates) return 'update';
  // A stale running commit may also have stale dependencies/frontend output.
  // Only use the lightweight endpoint when the backend explicitly proves that
  // restart alone is safe; old/partial responses conservatively redeploy.
  if (result.needs_restart) {
    return result.restart_only_safe ? 'restart' : 'repair';
  }
  return 'none';
}

function shortRevision(value: string | null | undefined) {
  if (!value) return '无法确认';
  return value.length > 7 ? value.slice(0, 7) : value;
}

export function UpdateButton() {
  const [phase, setPhase] = useState<Phase>('idle');
  const [steps, setSteps] = useState<StepInfo[]>([]);
  const [logs, setLogs] = useState<string[]>([]);
  const [dryRunResult, setDryRunResult] = useState<DeploymentCheck | null>(null);
  const [skipFrontend, setSkipFrontend] = useState(false);
  const [branch, setBranch] = useState('');
  const [channel, setChannel] = useState<'stable' | 'main'>('stable');
  const [error, setError] = useState('');
  const [oldCommit, setOldCommit] = useState('');
  const [newCommit, setNewCommit] = useState('');
  const [reconnectCount, setReconnectCount] = useState(0);
  const [reconnectSlow, setReconnectSlow] = useState(false);
  const [reconciling, setReconciling] = useState(false);
  const [reconcileError, setReconcileError] = useState('');
  const [reconcileNotice, setReconcileNotice] = useState('');
  const reconnectTimer = useRef<ReturnType<typeof setInterval> | null>(null);
  const logsEndRef = useRef<HTMLDivElement>(null);
  const phaseRef = useRef(phase);
  phaseRef.current = phase;

  const onWsMessage = useCallback((msg: Record<string, unknown>) => {
    const data = msg.data as Record<string, unknown> | undefined;
    if (!data || (msg.channel !== 'system_update')) return;

    const event = data.event as string;

    if (event === 'step_update') {
      setSteps(prev => {
        const stepName = data.step as string;
        const exists = prev.find(s => s.name === stepName);
        if (exists) {
          return prev.map(s =>
            s.name === stepName
              ? { ...s, status: data.status as string, duration_ms: data.duration_ms as number | undefined, message: data.message as string | undefined, result: data.result as Record<string, unknown> | undefined }
              : s
          );
        }
        return prev;
      });
    }

    if (event === 'log_line') {
      const log = data.log as string;
      if (log) setLogs(prev => [...prev.slice(-200), log]);
    }

    if (event === 'update_complete') {
      setUpdateActive(false);
      setPhase('completed');
    }

    if (event === 'update_failed') {
      setUpdateActive(false);
      setError(data.message as string || '更新失败');
      setPhase('failed');
    }

    if (event === 'restarting') {
      setUpdateActive(true);
      setPhase('restarting');
      startReconnectPolling();
    }
  }, []);

  useWebSocket(['system_update'], onWsMessage);

  useEffect(() => {
    if (logsEndRef.current) {
      logsEndRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, [logs]);

  useEffect(() => {
    return () => {
      if (reconnectTimer.current) clearInterval(reconnectTimer.current);
    };
  }, []);

  useEffect(() => {
    if (typeof api.getUpdateChannel !== 'function') return;
    void api.getUpdateChannel().then(result => {
      if (result?.update_channel === 'stable' || result?.update_channel === 'main') {
        setChannel(result.update_channel);
      }
    }).catch(() => {
      // Older Managers do not expose channel settings; keep the safe default.
    });
  }, []);

  useEffect(() => {
    if (!hasActiveUpdate()) return;
    let cancelled = false;

    const recoverActiveUpdate = async () => {
      try {
        const status = await api.getUpdateStatus() as UpdateStatusData;
        if (cancelled) return;
        if (status.old_commit) setOldCommit(status.old_commit);
        if (status.new_commit) setNewCommit(status.new_commit);
        if (status.steps) setSteps(status.steps);

        if (status.status === 'running') {
          setPhase('running');
          startReconnectPolling();
        } else if (isActiveDeploymentStatus(status.status)) {
          setPhase('restarting');
          startReconnectPolling();
        } else if (status.status === 'rolled_back') {
          setOldCommit('');
          setUpdateActive(false);
          setError(status.error || '更新未完成，系统已经自动回滚到上一个版本');
          setPhase('failed');
        } else if (
          status.status === 'failed'
          || status.status === 'rollback_failed'
          || status.repair_required
        ) {
          setUpdateActive(false);
          setError(status.error || '上次部署未完成，请检查状态后重新修复');
          setPhase('failed');
        } else {
          setUpdateActive(false);
          if (status.status === 'completed') setPhase('completed');
        }
      } catch {
        // The service can be temporarily unavailable while it restarts. Keep
        // the marker so a later visibility change can recover the operation.
        setPhase('restarting');
        startReconnectPolling();
      }
    };

    void recoverActiveUpdate();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    const onVisible = async () => {
      if (document.visibilityState !== 'visible') return;
      const p = phaseRef.current;
      if (p !== 'running' && p !== 'restarting') return;
      try {
        const status = await api.getUpdateStatus() as UpdateStatusData;
        if (status.old_commit) setOldCommit(status.old_commit);
        if (status.new_commit) setNewCommit(status.new_commit);
        if (status.steps) setSteps(status.steps);
        if (status.status === 'completed') {
          setUpdateActive(false);
          setPhase('completed');
        } else if (status.status === 'rolled_back') {
          setOldCommit('');
          setUpdateActive(false);
          setError(status.error || '更新未完成，系统已经自动回滚到上一个版本');
          setPhase('failed');
        } else if (isActiveDeploymentStatus(status.status)) {
          // ``deployment_incomplete`` is deliberately true throughout the
          // external handoff. It is a safety fence, not a terminal failure.
          if (status.status !== 'running') {
            setPhase('restarting');
            startReconnectPolling();
          }
        } else if (
          status.status === 'failed'
          || status.status === 'rollback_failed'
          || status.repair_required
        ) {
          setUpdateActive(false);
          setError(status.error || '更新失败');
          setPhase('failed');
        }
      } catch {
        // Server may still be restarting — keep current phase
      }
    };
    document.addEventListener('visibilitychange', onVisible);
    return () => document.removeEventListener('visibilitychange', onVisible);
  }, []);

  const startReconnectPolling = () => {
    if (reconnectTimer.current) clearInterval(reconnectTimer.current);
    let attempts = 0;
    setReconnectSlow(false);

    const slowDownPolling = () => {
      if (attempts !== 60) return;
      setReconnectSlow(true);
      if (reconnectTimer.current) clearInterval(reconnectTimer.current);
      reconnectTimer.current = setInterval(poll, 5000);
    };

    const poll = async () => {
      attempts++;
      setReconnectCount(attempts);
      try {
        await api.health();
        try {
          const status = await api.getUpdateStatus() as UpdateStatusData;
          if (status.old_commit) setOldCommit(status.old_commit);
          if (status.new_commit) setNewCommit(status.new_commit);
          if (status.steps) setSteps(status.steps);
          if (status.status === 'rolled_back') {
            setOldCommit('');
            setUpdateActive(false);
            setError(status.error || '更新未完成，系统已经自动回滚到上一个版本');
            setPhase('failed');
          } else if (isActiveDeploymentStatus(status.status)) {
            // The old process may still answer while the deployment worker
            // holds an incomplete lease. Keep polling until the worker writes
            // a terminal, token-matched result.
            if (status.status !== 'running') setPhase('restarting');
            slowDownPolling();
            return;
          } else if (
            status.status === 'failed'
            || status.status === 'rollback_failed'
            || status.repair_required
          ) {
            setUpdateActive(false);
            setError(status.error || '迁移失败，已自动回滚');
            setPhase('failed');
          } else if (status.status === 'completed') {
            setUpdateActive(false);
            setPhase('completed');
            setTimeout(() => window.location.reload(), 1500);
          } else {
            // The old process can still answer health while the delayed
            // restart command is pending. Only a terminal status proves that
            // the new process recovered.
            slowDownPolling();
            return;
          }
        } catch {
          // Health and status must both be available before declaring the
          // deployment complete.
          slowDownPolling();
          return;
        }
        if (reconnectTimer.current) clearInterval(reconnectTimer.current);
        reconnectTimer.current = null;
        setReconnectSlow(false);
      } catch {
        // After 120s (60 fast polls), switch to slow polling instead of giving up
        slowDownPolling();
      }
    };

    reconnectTimer.current = setInterval(poll, 2000);
  };

  const handleCheck = async () => {
    setPhase('checking');
    setError('');
    setReconcileError('');
    setReconcileNotice('');
    try {
      const result = await api.startUpdate({
        dry_run: true,
        force: true,
        channel,
        branch: channel === 'main' ? (branch || undefined) : undefined,
      }) as DeploymentCheck;
      if (result.error && !result.needs_restart && !result.repair_required) {
        throw new Error(result.error);
      }
      setDryRunResult(result);
      setPhase('confirming');
    } catch (e: unknown) {
      setError(errorMessage(e, '检查更新失败'));
      setPhase('failed');
    }
  };

  const handleOpen = () => {
    setDryRunResult(null);
    setError('');
    setPhase('selecting');
  };

  const handleChannelChange = async (next: 'stable' | 'main') => {
    setChannel(next);
    setDryRunResult(null);
    setError('');
    if (next === 'stable') setBranch('');
    if (typeof api.updateUpdateChannel === 'function') {
      try {
        await api.updateUpdateChannel(next);
      } catch {
        // The visible selection remains usable for this check even if the
        // preference could not be persisted.
      }
    }
  };

  const handleReconcile = async () => {
    setReconciling(true);
    setReconcileError('');
    setReconcileNotice('');
    let reconciliationCompleted = false;

    try {
      const reconciliation = await api.reconcileUpdateState();
      reconciliationCompleted = true;

      // Keep the previous blocker visible until a fresh dry-run confirms the
      // complete deployment state. A task can start after maintenance resumes,
      // and a failed refresh must never make an uncertain action available.
      const result = await api.startUpdate({
        dry_run: true,
        force: true,
        channel,
        branch: channel === 'main' ? (branch || undefined) : undefined,
      }) as DeploymentCheck;
      if (result.error && !result.needs_restart && !result.repair_required) {
        throw new Error(result.error);
      }

      const refreshedResult: DeploymentCheck = {
        ...result,
        update_blocked: result.update_blocked ?? reconciliation.update_blocked,
        active_task_count: result.active_task_count ?? reconciliation.active_task_count,
        active_tasks: result.active_tasks ?? reconciliation.active_tasks,
      };
      setDryRunResult(refreshedResult);

      const stillBlocked = Boolean(
        refreshedResult.update_blocked
        || refreshedResult.active_task_count
        || refreshedResult.active_tasks?.length
      );
      setReconcileNotice(
        stillBlocked
          ? '核对完成：以上运行阻断项仍存在，更新和重启将继续保持禁用。'
          : '运行状态已重新核对，可以继续更新或重启。'
      );
    } catch (e: unknown) {
      const detail = errorMessage(e, '未知错误');
      setReconcileError(
        reconciliationCompleted
          ? `运行状态已核对，但刷新更新信息失败：${detail}`
          : `重新核对运行状态失败：${detail}`
      );
    } finally {
      setReconciling(false);
    }
  };

  const prepareRunning = () => {
    setPhase('running');
    setLogs([]);
    setError('');

    const defaultSteps: StepInfo[] = [
      'git_pull', 'detect_changes', 'backup_database', 'uv_sync',
      'refresh_pty', 'npm_install', 'frontend_build',
      'stop_service', 'alembic_upgrade', 'start_service',
    ].map(name => ({ name, status: 'pending' }));
    setSteps(defaultSteps);
    setUpdateActive(true);
  };

  const handleConfirm = async () => {
    prepareRunning();
    try {
      const result = await api.startUpdate({
        skip_frontend_build: skipFrontend,
        channel,
        branch: channel === 'main' ? (branch || undefined) : undefined,
      });
      if (result.update_id) {
        setOldCommit(result.old_commit || '');
      }
    } catch (e: unknown) {
      setUpdateActive(false);
      setError(errorMessage(e, '启动更新失败'));
      setPhase('failed');
    }
  };

  const handleRepair = async () => {
    prepareRunning();
    try {
      const result = await api.repairUpdate();
      if (result.old_commit) setOldCommit(result.old_commit);
    } catch (e: unknown) {
      setUpdateActive(false);
      setError(errorMessage(e, '启动部署修复失败'));
      setPhase('failed');
    }
  };

  const handleRestart = async () => {
    setError('');
    setUpdateActive(true);
    try {
      await api.restartService();
      setPhase('restarting');
      startReconnectPolling();
    } catch (e: unknown) {
      setUpdateActive(false);
      setError(errorMessage(e, '启动服务重启失败'));
      setPhase('failed');
    }
  };

  const handleRollback = async () => {
    try {
      const status = await api.getUpdateStatus() as UpdateStatusData;
      // Legacy/interrupted records may not have migration metadata. Match the
      // backend's conservative rule: only an explicit false proves that a
      // database restore is unnecessary.
      const restoreDatabase =
        status.database_migration_applied !== false;
      if (!confirm('确定要回滚到上一个版本吗？')) return;
      if (
        restoreDatabase
        && !confirm(
          `${status.database_migration_applied === true ? '这次更新已经执行' : '这次更新可能已经开始执行'}数据库迁移。`
          + '继续回滚会恢复更新前的数据库备份，并丢失更新完成后产生的数据。确定仍要继续吗？'
        )
      ) {
        return;
      }
      setUpdateActive(true);
      await api.rollbackUpdate({ confirm_database_restore: restoreDatabase });
      startReconnectPolling();
      setPhase('restarting');
    } catch (e: unknown) {
      setUpdateActive(false);
      setError(errorMessage(e, '回滚失败'));
      setPhase('failed');
    }
  };

  const handleClose = () => {
    setPhase('idle');
    setSteps([]);
    setLogs([]);
    setError('');
    setDryRunResult(null);
    setSkipFrontend(false);
    setBranch('');
    setOldCommit('');
    setNewCommit('');
    setReconnectCount(0);
    setReconnectSlow(false);
    setUpdateActive(false);
    setReconciling(false);
    setReconcileError('');
    setReconcileNotice('');
  };

  const isModalOpen = phase !== 'idle';
  const activeTasks = (dryRunResult?.active_tasks || []) as ActiveTaskSummary[];
  const activeTaskCount = Number(dryRunResult?.active_task_count || activeTasks.length || 0);
  const updateBlocked = Boolean(dryRunResult?.update_blocked || activeTaskCount > 0);
  const actionDisabled = updateBlocked || reconciling || Boolean(reconcileError);
  const hasRuntimeBlockers = activeTasks.some(
    task => task.kind && task.kind !== 'task'
  );
  const action = dryRunResult ? deploymentAction(dryRunResult) : 'none';
  const canRollback = Boolean(
    oldCommit && newCommit && oldCommit !== newCommit
  );
  const dbInSync = dryRunResult?.db_up_to_date ?? dryRunResult?.db_in_sync ?? (
    dryRunResult?.db_current_revision && dryRunResult?.db_head_revision
      ? dryRunResult.db_current_revision === dryRunResult.db_head_revision
      : null
  );

  return (
    <>
      <button
        onClick={handleOpen}
        className="relative p-2 rounded text-gray-400 hover:text-foreground hover:bg-gray-800 transition-colors"
        title="更新并重启"
      >
        <ArrowUpCircle size={18} />
      </button>

      {isModalOpen && createPortal(
        <div className="fixed inset-0 z-[70] flex items-center justify-center bg-black/60">
          <div className="bg-gray-900 border border-gray-700 rounded-xl shadow-2xl w-full max-w-lg mx-4 max-h-[85vh] flex flex-col">
            {/* Header */}
            <div className="flex items-center justify-between px-4 py-3 border-b border-gray-700">
              <h3 className="text-sm font-semibold text-foreground">
                {phase === 'selecting' && '选择更新渠道'}
                {phase === 'checking' && '检查更新...'}
                {phase === 'confirming' && '确认更新'}
                {phase === 'running' && '更新中...'}
                {phase === 'restarting' && '重启中...'}
                {phase === 'completed' && '更新完成'}
                {phase === 'failed' && '更新失败'}
              </h3>
              {(phase === 'selecting' || phase === 'completed' || phase === 'failed' || phase === 'confirming') && (
                <button onClick={handleClose} className="text-gray-400 hover:text-foreground text-lg">✕</button>
              )}
            </div>

            {/* Body */}
            <div className="flex-1 overflow-y-auto p-4 space-y-3">
              {/* Channel selection always precedes a remote update check. */}
              {phase === 'selecting' && (
                <div className="space-y-3">
                  <p className="text-sm text-gray-300">请选择本次要检查的更新渠道。</p>
                  <div className="grid gap-2">
                    <label className={`cursor-pointer rounded-lg border p-3 transition-colors ${channel === 'stable' ? 'border-indigo-500 bg-indigo-950/30' : 'border-gray-700 bg-gray-800/40 hover:border-gray-600'}`}>
                      <span className="flex items-center gap-3">
                        <input
                          type="radio"
                          name="update-channel"
                          value="stable"
                          checked={channel === 'stable'}
                          onChange={() => void handleChannelChange('stable')}
                        />
                        <span>
                          <span className="block text-sm font-medium text-foreground">Stable 正式版</span>
                          <span className="block text-xs text-gray-500">只接收已发布的正式版本 tag</span>
                        </span>
                      </span>
                    </label>
                    <label className={`cursor-pointer rounded-lg border p-3 transition-colors ${channel === 'main' ? 'border-amber-500 bg-amber-950/20' : 'border-gray-700 bg-gray-800/40 hover:border-gray-600'}`}>
                      <span className="flex items-center gap-3">
                        <input
                          type="radio"
                          name="update-channel"
                          value="main"
                          checked={channel === 'main'}
                          onChange={() => void handleChannelChange('main')}
                        />
                        <span>
                          <span className="block text-sm font-medium text-foreground">Main 测试版</span>
                          <span className="block text-xs text-amber-400">跟随 main 最新代码，可能包含未验证变更</span>
                        </span>
                      </span>
                    </label>
                  </div>
                  {channel === 'main' && (
                    <>
                      <div className="rounded-lg border border-amber-700/60 bg-amber-950/30 p-3 text-xs text-amber-200" role="alert">
                        <p className="font-medium">切换到测试版前请注意</p>
                        <p className="mt-1 text-amber-300/90">
                          如果 Main 包含正式版没有的数据库迁移，更新后将无法一键切回 Stable，需先备份并制定数据库降级方案。
                        </p>
                      </div>
                      <div className="flex items-center gap-2">
                        <label htmlFor="update-branch" className="text-xs text-gray-400 shrink-0">分支:</label>
                        <input
                          id="update-branch"
                          value={branch}
                          onChange={e => setBranch(e.target.value)}
                          placeholder="main"
                          className="flex-1 bg-gray-800 text-foreground text-xs rounded px-2 py-1.5 border border-gray-700 focus:outline-none focus:border-indigo-500"
                        />
                      </div>
                    </>
                  )}
                </div>
              )}

              {/* Checking phase */}
              {phase === 'checking' && (
                <div className="flex items-center gap-2 text-gray-400 text-sm">
                  <RefreshCw size={14} className="animate-spin" />
                  正在检查是否有新版本...
                </div>
              )}

              {/* Confirm phase */}
              {phase === 'confirming' && dryRunResult && (
                <div className="space-y-3">
                  {/* Update channel selector */}
                  <div className="flex items-center gap-2">
                    <label htmlFor="update-channel" className="text-xs text-gray-400 shrink-0">更新渠道:</label>
                    <select
                      id="update-channel"
                      value={channel}
                      onChange={e => void handleChannelChange(e.target.value as 'stable' | 'main')}
                      className="bg-gray-800 text-foreground text-xs rounded px-2 py-1 border border-gray-700 focus:outline-none focus:border-indigo-500"
                    >
                      <option value="stable">Stable 正式版</option>
                      <option value="main">Main 测试版</option>
                    </select>
                    <button
                      onClick={handleCheck}
                      className="px-2 py-1 text-xs rounded bg-gray-800 text-gray-300 hover:bg-gray-700 shrink-0"
                    >
                      重新检查
                    </button>
                  </div>
                  {channel === 'main' && (
                    <div className="flex items-center gap-2">
                      <label className="text-xs text-gray-400 shrink-0">分支:</label>
                      <input
                        value={branch}
                        onChange={e => setBranch(e.target.value)}
                        placeholder="main"
                        className="flex-1 bg-gray-800 text-foreground text-xs rounded px-2 py-1 border border-gray-700 focus:outline-none focus:border-indigo-500"
                      />
                    </div>
                  )}
                  {channel === 'main' && (
                    <p className="text-xs text-amber-400">测试渠道会跟随 main 最新代码，可能包含未验证变更。</p>
                  )}
                  {branch && branch !== 'main' && (
                    <p className="text-xs text-amber-400">⚠️ 将从分支 <span className="font-mono">{branch}</span> 更新（非 main）</p>
                  )}

                  {updateBlocked && (
                    <div className="rounded border border-amber-700/60 bg-amber-950/30 p-3 text-xs text-amber-200" role="alert">
                      <p className="font-medium">
                        当前有 {activeTaskCount} 个{hasRuntimeBlockers ? '运行阻断项' : '任务正在执行'}，暂不能更新或重启。
                      </p>
                      <p className="mt-1 text-amber-300/80">请等待任务完成，或重新核对实际运行状态。系统不会中断仍在运行的任务。</p>
                      {activeTasks.length > 0 && (
                        <ul className="mt-2 space-y-1 text-amber-300/80">
                          {activeTasks.slice(0, 5).map(task => (
                            <li key={`${task.kind || 'task'}:${task.id}`}>
                              {task.kind && task.kind !== 'task'
                                ? `${task.title || `实例 #${task.instance_id || task.id}`}（${task.status}）`
                                : `#${task.id} ${task.title || '未命名任务'}（${task.status}）`}
                            </li>
                          ))}
                        </ul>
                      )}
                      <button
                        type="button"
                        onClick={handleReconcile}
                        disabled={reconciling}
                        aria-busy={reconciling}
                        className="mt-3 inline-flex items-center gap-1.5 rounded border border-amber-600/60 bg-amber-900/40 px-2.5 py-1.5 text-amber-100 hover:bg-amber-900/60 disabled:cursor-wait disabled:opacity-60"
                      >
                        <RefreshCw size={13} className={reconciling ? 'animate-spin' : ''} />
                        {reconciling ? '正在核对运行状态...' : '重新核对运行状态'}
                      </button>
                    </div>
                  )}
                  {reconcileError && (
                    <p className="rounded border border-red-800/60 bg-red-950/30 px-3 py-2 text-xs text-red-300" role="alert">
                      {reconcileError}
                    </p>
                  )}
                  {reconcileNotice && (
                    <p className="rounded border border-sky-800/60 bg-sky-950/30 px-3 py-2 text-xs text-sky-300" role="status">
                      {reconcileNotice}
                    </p>
                  )}

                  {(dryRunResult.running_commit || dryRunResult.disk_commit || dryRunResult.db_current_revision || dryRunResult.db_head_revision) && (
                    <div className="grid grid-cols-[auto_1fr_auto] gap-x-3 gap-y-1 rounded border border-gray-800 bg-gray-950/60 p-2 text-xs">
                      <span className="text-gray-500">运行版本</span>
                      <span className="font-mono text-gray-300">{shortRevision(dryRunResult.running_commit)}</span>
                      <span className={dryRunResult.needs_restart ? 'text-yellow-300' : 'text-green-400'}>
                        {dryRunResult.needs_restart ? '待重启' : '已加载'}
                      </span>
                      <span className="text-gray-500">磁盘版本</span>
                      <span className="font-mono text-gray-300">{shortRevision(dryRunResult.disk_commit || dryRunResult.current_commit)}</span>
                      <span className="text-gray-500">已拉取</span>
                      <span className="text-gray-500">数据库</span>
                      <span className="font-mono text-gray-300">
                        {shortRevision(dryRunResult.db_current_revision)} → {shortRevision(dryRunResult.db_head_revision)}
                      </span>
                      <span className={dbInSync === false ? 'text-red-300' : dbInSync === true ? 'text-green-400' : 'text-gray-500'}>
                        {dbInSync === false ? '待迁移' : dbInSync === true ? '已同步' : '无法确认'}
                      </span>
                    </div>
                  )}

                  {(dryRunResult.latest_version || dryRunResult.version) && (
                    <p className="text-xs text-gray-400">
                      正式版本：<span className="font-mono text-indigo-300">{dryRunResult.latest_version || dryRunResult.version}</span>
                    </p>
                  )}

                  {action === 'none' ? (
                    <div className="space-y-1">
                      <p className="text-sm text-gray-300">
                        {dryRunResult.channel === 'stable' && dryRunResult.error
                          ? 'Stable 当前不可切换。'
                          : '已是最新版本，无需更新。'}
                      </p>
                      {dryRunResult.error && (
                        <p className="text-xs text-red-300">{dryRunResult.error}</p>
                      )}
                    </div>
                  ) : action === 'restart' ? (
                    <div className="space-y-1 text-sm text-yellow-300">
                      <p>代码已是最新，但服务正在运行旧版本，需要重启。</p>
                      {dryRunResult.error && (
                        <p className="text-xs text-yellow-400/80">远端更新检查失败，但不影响重启并加载磁盘上的代码。</p>
                      )}
                    </div>
                  ) : action === 'repair' ? (
                    <div className="rounded border border-red-800/50 bg-red-900/20 p-3 text-sm text-red-200">
                      <p>
                        {dryRunResult.needs_restart && !dryRunResult.repair_required
                          ? '磁盘代码尚未完整部署，需要同步依赖、重建前端并确认数据库后再重启。'
                          : '当前部署未完整完成，需要修复后再重启服务。'}
                      </p>
                      {(dryRunResult.repair_reasons || []).length > 0 && (
                        <ul className="mt-2 list-disc space-y-1 pl-4 text-xs text-red-300">
                          {(dryRunResult.repair_reasons || []).map((reason, index) => (
                            <li key={index}>{reason}</li>
                          ))}
                        </ul>
                      )}
                      {dryRunResult.error && (
                        <p className="mt-2 break-words text-xs text-red-400">
                          {dryRunResult.needs_restart && !dryRunResult.repair_required
                            ? '远端更新检查失败：'
                            : '检查详情：'}
                          {dryRunResult.error}
                        </p>
                      )}
                    </div>
                  ) : (
                    <>
                      <div className="text-sm text-gray-300 space-y-1">
                        <p>
                          {dryRunResult.is_stable_downgrade
                            ? <>将从测试版切换回正式版 <span className="text-indigo-400 font-medium">{dryRunResult.latest_version || 'Stable'}</span></>
                            : <>发现 <span className="text-indigo-400 font-medium">{dryRunResult.commits_behind}</span> 个新提交</>}
                        </p>
                        <p className="text-xs text-gray-500">{dryRunResult.current_commit} → {dryRunResult.latest_commit}</p>
                        {dryRunResult.remote && (
                          <p className="text-xs text-gray-600">来源：{dryRunResult.remote}/{dryRunResult.branch || 'main'}</p>
                        )}
                      </div>

                      {dryRunResult.needs_restart && (
                        <p className="text-xs text-yellow-400">⚠️ 磁盘上还有尚未加载的手动更新，本次会一并完成部署。</p>
                      )}

                      {(dryRunResult.commit_messages || []).length > 0 && (
                        <div className="bg-gray-800 rounded p-2 max-h-32 overflow-y-auto">
                          {(dryRunResult.commit_messages || []).map((msg, i) => (
                            <p key={i} className="text-xs text-gray-400 py-0.5">{msg}</p>
                          ))}
                        </div>
                      )}

                      <div className="flex flex-wrap gap-2 text-xs">
                        {dryRunResult.has_new_migrations && (
                          <span className="px-2 py-0.5 rounded bg-yellow-900/50 text-yellow-300">
                            {dryRunResult.migration_count} 个迁移
                          </span>
                        )}
                        {dryRunResult.has_frontend_changes && (
                          <span className="px-2 py-0.5 rounded bg-blue-900/50 text-blue-300">前端变更</span>
                        )}
                        {dryRunResult.has_package_changes && (
                          <span className="px-2 py-0.5 rounded bg-purple-900/50 text-purple-300">依赖变更</span>
                        )}
                      </div>

                  {dryRunResult.has_new_migrations && (
                        <div className="rounded border border-yellow-700/60 bg-yellow-950/30 p-2 text-xs text-yellow-300" role="alert">
                          <p>⚠️ 包含数据库迁移，更新时会短暂停服并自动备份数据库。</p>
                          {channel === 'main' && (
                            <p className="mt-1 font-medium text-amber-300">
                              更新后可能无法一键切回 Stable；切回前需要数据库降级方案。
                            </p>
                          )}
                        </div>
                      )}
                    </>
                  )}

                  {action === 'update' && (
                    <label className="flex items-center gap-2 text-xs text-gray-400 cursor-pointer">
                      <input
                        type="checkbox"
                        checked={skipFrontend}
                        onChange={e => setSkipFrontend(e.target.checked)}
                        className="rounded border-gray-600"
                      />
                      跳过前端构建（仅后端更新）
                    </label>
                  )}
                </div>
              )}

              {/* Running / completed / failed — show steps */}
              {(phase === 'running' || phase === 'completed' || phase === 'failed') && steps.length > 0 && (
                <div className="space-y-1">
                  {steps.map(step => (
                    <div key={step.name} className="flex items-center gap-2 text-xs">
                      <span className="w-4 text-center">{STATUS_ICON[step.status] || '○'}</span>
                      <span className={`flex-1 ${step.status === 'running' ? 'text-foreground' : step.status === 'completed' ? 'text-gray-400' : step.status === 'failed' ? 'text-red-400' : 'text-gray-600'}`}>
                        {STEP_LABELS[step.name] || step.name}
                        {step.message && step.status !== 'running' && (
                          <span className="text-gray-600 ml-1">— {step.message}</span>
                        )}
                      </span>
                      {step.duration_ms != null && (
                        <span className="text-gray-600 text-[10px]">{(step.duration_ms / 1000).toFixed(1)}s</span>
                      )}
                    </div>
                  ))}
                </div>
              )}

              {/* Restarting phase */}
              {phase === 'restarting' && (
                <div className="flex flex-col items-center gap-3 py-6">
                  <RefreshCw size={24} className="animate-spin text-indigo-400" />
                  <p className="text-sm text-gray-300">服务正在重启...</p>
                  <p className="text-xs text-gray-500">
                    {reconnectSlow
                      ? `等待服务恢复（每 5 秒检测，已等待 ${Math.round((60 * 2 + (reconnectCount - 60) * 5))}秒）`
                      : `每 2 秒检测一次（${reconnectCount}/60）`
                    }
                  </p>
                  {reconnectSlow && (
                    <>
                      <p className="text-xs text-yellow-400">重启时间超过预期，可能正在执行数据库迁移...</p>
                      <button
                        onClick={() => window.location.reload()}
                        className="px-3 py-1.5 text-xs rounded bg-gray-800 text-gray-300 hover:bg-gray-700"
                      >
                        手动刷新页面
                      </button>
                    </>
                  )}
                </div>
              )}

              {/* Completed */}
              {phase === 'completed' && (
                <div className="bg-green-900/20 border border-green-800/50 rounded p-3 text-sm text-green-300">
                  ✅ 更新完成
                  {oldCommit && newCommit && oldCommit !== newCommit && (
                    <span className="text-xs text-gray-500 ml-2">{oldCommit.slice(0, 7)} → {newCommit.slice(0, 7)}</span>
                  )}
                </div>
              )}

              {/* Failed */}
              {phase === 'failed' && error && (
                <div className="bg-red-900/20 border border-red-800/50 rounded p-3 text-sm text-red-300">
                  {error}
                </div>
              )}

              {/* Logs */}
              {logs.length > 0 && (phase === 'running' || phase === 'failed') && (
                <div className="bg-gray-950 rounded border border-gray-800 p-2 max-h-40 overflow-y-auto font-mono text-[11px] text-gray-500">
                  {logs.map((line, i) => (
                    <div key={i}>{line}</div>
                  ))}
                  <div ref={logsEndRef} />
                </div>
              )}
            </div>

            {/* Footer */}
            <div className="flex justify-end gap-2 px-4 py-3 border-t border-gray-700">
              {phase === 'selecting' && (
                <>
                  <button onClick={handleClose} className="px-3 py-1.5 text-xs rounded bg-gray-800 text-gray-300 hover:bg-gray-700">取消</button>
                  <button onClick={handleCheck} className="px-3 py-1.5 text-xs rounded bg-indigo-600 text-white hover:bg-indigo-500">
                    检查所选渠道
                  </button>
                </>
              )}
              {phase === 'confirming' && dryRunResult && action === 'update' && (
                <>
                  <button onClick={handleClose} className="px-3 py-1.5 text-xs rounded bg-gray-800 text-gray-300 hover:bg-gray-700">取消</button>
                  <button
                    onClick={handleConfirm}
                    disabled={actionDisabled}
                    className="px-3 py-1.5 text-xs rounded bg-indigo-600 text-white hover:bg-indigo-500 disabled:cursor-not-allowed disabled:opacity-50"
                  >
                    {updateBlocked
                      ? '等待任务完成'
                      : dryRunResult.is_stable_downgrade ? '切换到正式版' : '确认更新'}
                  </button>
                </>
              )}
              {phase === 'confirming' && dryRunResult && action === 'repair' && (
                <>
                  <button onClick={handleClose} className="px-3 py-1.5 text-xs rounded bg-gray-800 text-gray-300 hover:bg-gray-700">取消</button>
                  <button
                    onClick={handleRepair}
                    disabled={actionDisabled}
                    className="px-3 py-1.5 text-xs rounded bg-red-700 text-white hover:bg-red-600 disabled:cursor-not-allowed disabled:opacity-50"
                  >
                    {updateBlocked ? '等待任务完成' : '修复并重新部署'}
                  </button>
                </>
              )}
              {phase === 'confirming' && dryRunResult && action === 'restart' && (
                <>
                  <button onClick={handleClose} className="px-3 py-1.5 text-xs rounded bg-gray-800 text-gray-300 hover:bg-gray-700">取消</button>
                  <button
                    onClick={handleRestart}
                    disabled={actionDisabled}
                    className="px-3 py-1.5 text-xs rounded bg-indigo-600 text-white hover:bg-indigo-500 disabled:cursor-not-allowed disabled:opacity-50"
                  >
                    {updateBlocked ? '等待任务完成' : '重启服务'}
                  </button>
                </>
              )}
              {phase === 'confirming' && dryRunResult && action === 'none' && (
                <>
                  <button onClick={handleClose} className="px-3 py-1.5 text-xs rounded bg-gray-800 text-gray-300 hover:bg-gray-700">关闭</button>
                  <button
                    onClick={handleRestart}
                    disabled={actionDisabled}
                    className="px-3 py-1.5 text-xs rounded border border-gray-700 bg-gray-800 text-gray-300 hover:bg-gray-700 disabled:cursor-not-allowed disabled:opacity-50"
                  >
                    {updateBlocked ? '等待任务完成' : '手动重启'}
                  </button>
                </>
              )}
              {phase === 'completed' && (
                <>
                  {canRollback && <button onClick={handleRollback} className="px-3 py-1.5 text-xs rounded bg-gray-800 text-gray-300 hover:bg-gray-700">回滚</button>}
                  <button onClick={() => window.location.reload()} className="px-3 py-1.5 text-xs rounded bg-indigo-600 text-white hover:bg-indigo-500">刷新页面</button>
                </>
              )}
              {phase === 'failed' && (
                <>
                  {canRollback && <button onClick={handleRollback} className="px-3 py-1.5 text-xs rounded bg-red-900/50 text-red-300 hover:bg-red-900/70">回滚</button>}
                  <button onClick={handleClose} className="px-3 py-1.5 text-xs rounded bg-gray-800 text-gray-300 hover:bg-gray-700">关闭</button>
                </>
              )}
            </div>
          </div>
        </div>,
        document.body,
      )}
    </>
  );
}
