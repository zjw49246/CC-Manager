import { useState, useEffect, useCallback } from 'react';
import { api } from '../../api/client';
import type { MonitorSession, MonitorCheck } from '../../api/client';
import { X, StopCircle, ChevronDown, ChevronRight, Activity, AlertCircle, CheckCircle2 } from '../icons';

interface MonitorPanelProps {
  taskId: number;
  sessions: MonitorSession[];
  onSessionsChange: (sessions: MonitorSession[]) => void;
  onClose: () => void;
  /** Task provider 与精确 Monitor capability；未知/不支持时面板显式标注。 */
  provider?: string;
  monitorSupported?: boolean;
}

function StatusBadge({ status }: { status: string }) {
  const styles: Record<string, string> = {
    running: 'bg-emerald-900/50 text-emerald-400 border-emerald-700',
    completed: 'bg-blue-900/50 text-blue-400 border-blue-700',
    failed: 'bg-red-900/50 text-red-400 border-red-700',
    cancelled: 'bg-gray-700/50 text-gray-400 border-gray-600',
  };
  return (
    <span className={`px-1.5 py-0.5 text-xs rounded border ${styles[status] || styles.cancelled}`}>
      {status}
    </span>
  );
}

/** 类别小徽章：monitor / sub_agent / native-agent / native-monitor */
function TypeChip({ agentType, source }: { agentType: string; source: string }) {
  const isNative = source === 'native';
  const isSubAgent = agentType === 'sub_agent';
  return (
    <span
      className={`px-1 py-0.5 text-[10px] rounded border shrink-0 ${
        isNative
          ? 'bg-purple-900/40 text-purple-300 border-purple-700'
          : isSubAgent
            ? 'bg-amber-900/40 text-amber-300 border-amber-700'
            : 'bg-teal-900/40 text-teal-300 border-teal-700'
      }`}
      title={
        isNative ? '模型原生子 agent（Provider 生命周期观测）'
          : isSubAgent ? 'CCM Sub-Agent（一次性任务）'
            : 'CCM $monitor 子 agent'
      }
    >
      {agentType === 'sub_agent' ? 'sub-agent' : agentType}
    </span>
  );
}

function MonitorSessionRow({ session, taskId, onStopped }: { session: MonitorSession; taskId: number; onStopped: () => void }) {
  const [expanded, setExpanded] = useState(false);
  const [checks, setChecks] = useState<MonitorCheck[]>([]);
  const [stopping, setStopping] = useState(false);
  const isNative = session.source === 'native';
  const cleanupPending = (
    session.provider === 'codex' && session.codex_cleanup_pending
  );

  const loadChecks = useCallback(() => {
    api.getSubAgentReports(taskId, session.id).then(setChecks).catch(() => {});
  }, [taskId, session.id]);

  useEffect(() => {
    if (expanded) loadChecks();
  }, [expanded, loadChecks, session.checks_done]);

  const handleStop = async () => {
    setStopping(true);
    try {
      if (session.agent_type === 'sub_agent') {
        await api.deleteSubAgentSession(taskId, session.id);
      } else {
        await api.deleteMonitorSession(taskId, session.id);
      }
      onStopped();
    } catch {
      // Terminal cleanup can fail after the backend has already persisted the
      // cancelled row. Refresh so the durable retry state and error become
      // visible instead of leaving a stale "running" row on screen.
      onStopped();
    } finally {
      setStopping(false);
    }
  };

  return (
    <div className="border border-gray-700 rounded">
      <div className="flex items-center gap-2 px-3 py-2">
        <button
          className="text-gray-400 hover:text-gray-200"
          onClick={() => setExpanded(!expanded)}
        >
          {expanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
        </button>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <TypeChip agentType={session.agent_type} source={session.source} />
            <span className="text-sm text-gray-200 truncate">{session.description}</span>
            <StatusBadge status={session.status} />
          </div>
          <div className="text-xs text-gray-500 mt-0.5">
            {isNative
              ? (session.checks_done > 0 ? `${session.checks_done} updates` : null)
              : session.agent_type === 'sub_agent'
                ? (session.checks_done > 0 ? `${session.checks_done} progress updates` : 'running...')
                : `${session.checks_done}/${session.max_checks} checks`}
            {session.last_summary && (
              <span className="ml-2 text-gray-400">— {session.last_summary}</span>
            )}
          </div>
          {cleanupPending && (
            <div className="mt-1 text-xs text-amber-300">
              <span>Codex runtime cleanup pending</span>
              {session.codex_cleanup_error && (
                <span
                  className="ml-1 text-amber-400"
                  title={session.codex_cleanup_error}
                >
                  — {session.codex_cleanup_error}
                </span>
              )}
            </div>
          )}
        </div>
        {(session.status === 'running' || cleanupPending) && !isNative && (
          <button
            className="text-gray-400 hover:text-red-400 p-1 disabled:opacity-50"
            onClick={handleStop}
            disabled={stopping}
            title={cleanupPending
              ? 'Retry Codex cleanup'
              : session.agent_type === 'sub_agent'
                ? 'Stop sub-agent'
                : 'Stop monitor'}
          >
            <StopCircle size={16} />
          </button>
        )}
      </div>

      {expanded && checks.length > 0 && (
        <div className="border-t border-gray-700 px-3 py-2 space-y-1.5 max-h-48 overflow-y-auto">
          {checks.map((check) => (
            <div key={check.id} className="flex items-start gap-2 text-xs">
              {check.status === 'running' ? (
                <Activity size={12} className="text-cyan-400 mt-0.5 shrink-0" />
              ) : check.status === 'success' || check.status === 'completed' ? (
                <CheckCircle2 size={12} className="text-emerald-500 mt-0.5 shrink-0" />
              ) : (
                <AlertCircle size={12} className="text-red-500 mt-0.5 shrink-0" />
              )}
              <div className="min-w-0">
                <span className="text-gray-400">#{check.check_number}</span>
                {check.summary && <span className="text-gray-300 ml-1.5">{check.summary}</span>}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

export function MonitorPanel({
  taskId,
  sessions,
  onSessionsChange,
  onClose,
  provider,
  monitorSupported,
}: MonitorPanelProps) {
  const refresh = useCallback(() => {
    api.listAllSubAgentSessions(taskId).then(onSessionsChange).catch(() => {});
  }, [taskId, onSessionsChange]);

  useEffect(() => {
    refresh();
  }, [refresh]);

  return (
    <div className="bg-gray-800 border border-gray-700 rounded-lg">
      <div className="flex items-center justify-between px-3 py-2 border-b border-gray-700">
        <div className="flex items-center gap-2 text-sm font-medium text-gray-300">
          <Activity size={14} className="text-emerald-400" />
          Sub-Agents
          <span className="text-xs text-gray-500">({sessions.length})</span>
        </div>
        <button className="text-gray-400 hover:text-gray-200" onClick={onClose}>
          <X size={16} />
        </button>
      </div>
      {provider === 'codex' && monitorSupported !== true && (
        <div className="mx-2 mt-2 rounded border border-amber-700/50 bg-amber-900/20 px-2 py-1.5 text-xs text-amber-300">
          Codex Monitor 当前仅支持 capability 已确认的本地、非共享任务
        </div>
      )}
      <div className="p-2 space-y-2 max-h-64 overflow-y-auto">
        {sessions.length === 0 ? (
          <div className="text-xs text-gray-500 text-center py-3">No sub-agents</div>
        ) : (
          sessions.map((s) => (
            <MonitorSessionRow
              key={s.id}
              session={s}
              taskId={taskId}
              onStopped={refresh}
            />
          ))
        )}
      </div>
    </div>
  );
}
