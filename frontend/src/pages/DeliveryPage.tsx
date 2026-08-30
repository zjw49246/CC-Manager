import { useCallback, useEffect, useMemo, useState } from 'react';

import { api, type DeliveryRun, type MonitoredRepo, type Project, type SystemConfig } from '../api/client';
import { DeliveryCreateForm } from '../components/Delivery/DeliveryCreateForm';
import { DeliveryRunDialog } from '../components/Delivery/DeliveryRunDialog';
import { CheckCircle2, Circle, GitPullRequest, Loader2, Play, RefreshCw, X } from '../components/icons';
import { useWebSocket } from '../hooks/useWebSocket';

interface Props {
  selectedRunId: number | null;
  onSelectedRunChange: (runId: number | null) => void;
  onNavigate: (page: string) => void;
  onNavigateTask: (taskId: number) => void;
  onNavigatePlan: (planId: number) => void;
}

function titleCase(value: string): string { return value.split('_').map((part) => part.charAt(0).toUpperCase() + part.slice(1)).join(' '); }
function statusLabel(run: DeliveryRun): string { if (run.activity === 'terminal') return run.outcome === 'success' ? (run.terminal === 'merged' ? 'Merged' : 'Ready to Merge') : titleCase(run.outcome || 'done'); return `${titleCase(run.phase)} · ${titleCase(run.activity)}`; }

export function DeliveryPage({ selectedRunId, onSelectedRunChange, onNavigate, onNavigateTask, onNavigatePlan }: Props) {
  const [runs, setRuns] = useState<DeliveryRun[]>([]);
  const [projects, setProjects] = useState<Project[]>([]);
  const [repos, setRepos] = useState<MonitoredRepo[]>([]);
  const [config, setConfig] = useState<SystemConfig | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [createOpen, setCreateOpen] = useState(false);
  const refresh = useCallback(async (initial = false) => {
    if (initial) setLoading(true);
    try {
      const [runRows, projectRows, repoRows, systemConfig] = await Promise.all([api.listDeliveryRuns(), api.listProjects(), api.getMonitoredRepos(), api.config()]);
      setRuns(runRows); setProjects(projectRows); setRepos(repoRows); setConfig(systemConfig); setError('');
    } catch (reason) { setError(reason instanceof Error ? reason.message : String(reason)); }
    finally { setLoading(false); }
  }, []);
  useEffect(() => { void refresh(true); const timer = window.setInterval(() => void refresh(), 15000); return () => window.clearInterval(timer); }, [refresh]);
  useWebSocket(['deliveries'], () => { void refresh(); }, () => { void refresh(); }, () => { void refresh(); });
  const projectMap = useMemo(() => Object.fromEntries(projects.map((project) => [project.id, project])), [projects]);
  const active = runs.filter((run) => run.activity !== 'terminal');
  const completed = runs.filter((run) => run.activity === 'terminal');

  const selectedRun = selectedRunId == null
    ? undefined
    : runs.find((run) => run.id === selectedRunId);

  return (
    <div className="space-y-4">
      {selectedRunId == null && (
        <header className="flex flex-wrap items-center justify-between gap-3 border-b border-gray-800 pb-3">
          <span className="text-xs text-gray-600">{active.length} active · {completed.length} completed</span>
          <div className="flex items-center gap-2">
            <button type="button" onClick={() => void refresh()} className="rounded p-2 text-gray-500 hover:bg-gray-800 hover:text-gray-200" title="Refresh Deliveries"><RefreshCw size={15} /></button>
            <button
              type="button"
              onClick={() => setCreateOpen((value) => !value)}
              className="inline-flex items-center gap-1.5 rounded-lg bg-indigo-600 px-3 py-2 text-xs font-medium text-white hover:bg-indigo-500"
            >
              {createOpen ? <X size={14} /> : <Play size={14} />}
              {createOpen ? 'Close' : 'New Delivery'}
            </button>
          </div>
        </header>
      )}

      {selectedRunId != null ? (
        <DeliveryRunDialog
          embedded
          runId={selectedRunId}
          project={selectedRun ? projectMap[selectedRun.project_id] : undefined}
          onClose={() => onSelectedRunChange(null)}
          onOpenTask={onNavigateTask}
          onOpenPlan={onNavigatePlan}
          onOpenPRMonitor={() => onNavigate('pr-monitor')}
        />
      ) : (
        <>
          {createOpen && (
            <DeliveryCreateForm
              projects={projects}
              repos={repos}
              config={config}
              onCreated={() => { setCreateOpen(false); void refresh(); }}
              onNavigateProjects={() => onNavigate('projects')}
              onNavigatePRMonitor={() => onNavigate('pr-monitor')}
            />
          )}
          <section aria-label="Delivery Runs" className="space-y-3">
            {error && <div className="rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs text-red-300">{error}</div>}
            {loading ? (
              <div className="flex justify-center gap-2 py-12 text-sm text-gray-500"><Loader2 size={16} className="animate-spin" /> Loading Deliveries…</div>
            ) : runs.length === 0 ? (
              <div className="rounded-lg border border-dashed border-gray-800 px-4 py-12 text-center text-sm text-gray-500">No Delivery Runs yet.</div>
            ) : (
              <div className="space-y-5">
                {active.length > 0 && <RunGroup title="Active" runs={active} projectMap={projectMap} onOpen={onSelectedRunChange} />}
                {completed.length > 0 && <RunGroup title="Completed" runs={completed} projectMap={projectMap} onOpen={onSelectedRunChange} />}
              </div>
            )}
          </section>
        </>
      )}
    </div>
  );
}

function RunGroup({ title, runs, projectMap, onOpen }: { title: string; runs: DeliveryRun[]; projectMap: Record<number, Project>; onOpen: (id: number) => void }) { return <div className="space-y-2"><h3 className="text-xs font-medium uppercase tracking-wide text-gray-600">{title}</h3>{runs.map((run) => <button key={run.id} type="button" onClick={() => onOpen(run.id)} className="w-full rounded-xl border border-gray-800 bg-gray-900/70 p-4 text-left transition-colors hover:border-indigo-500/40 hover:bg-gray-800/80"><div className="flex items-start gap-3">{run.activity === 'terminal' && run.outcome === 'success' ? <CheckCircle2 size={19} className="mt-0.5 shrink-0 text-emerald-400" /> : <Circle size={19} className="mt-0.5 shrink-0 fill-indigo-500/20 text-indigo-400" />}<div className="min-w-0 flex-1"><div className="flex flex-wrap items-center gap-2"><span className="text-xs text-gray-500">DLV-{run.id}</span><span className="rounded bg-indigo-500/15 px-1.5 py-0.5 text-[10px] text-indigo-300">{statusLabel(run)}</span><span className="rounded border border-indigo-400/30 bg-indigo-500/10 px-1.5 py-0.5 text-[10px] font-semibold text-indigo-200">Round {run.cycle_count} of {run.max_cycles}</span>{run.wait_reason && <span className="rounded bg-amber-500/15 px-1.5 py-0.5 text-[10px] text-amber-300">Needs attention: {titleCase(run.wait_reason)}</span>}</div><div className="mt-1 truncate text-sm font-semibold text-gray-100">{run.title}</div><div className="mt-1 flex flex-wrap gap-x-4 gap-y-1 text-xs text-gray-500"><span>{projectMap[run.project_id]?.name || `Project #${run.project_id}`}</span><span>{run.turn_count} turn{run.turn_count === 1 ? '' : 's'}</span>{run.pr_number && <span className="inline-flex items-center gap-1"><GitPullRequest size={12} /> PR #{run.pr_number}</span>}</div></div></div></button>)}</div>; }
