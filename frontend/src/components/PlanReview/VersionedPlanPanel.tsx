import { useCallback, useEffect, useState } from 'react';

import { api, type PlanResource } from '../../api/client';
import { useDialogA11y } from '../../hooks/useDialogA11y';
import { ChevronRight } from '../icons';
import { PlanDetail } from './PlanDetail';
import { usePlanEvents } from './usePlanEvents';

interface Props {
  onVisibilityChange?: (visible: boolean) => void;
  onNavigateTask: (taskId: number) => void;
}

export function VersionedPlanPanel({ onVisibilityChange, onNavigateTask }: Props) {
  const [plans, setPlans] = useState<PlanResource[]>([]);
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [expanded, setExpanded] = useState(false);
  const close = useCallback(() => { setSelectedId(null); setExpanded(false); }, []);
  const dialogRef = useDialogA11y(selectedId != null, close);

  const refresh = useCallback(async () => {
    try {
      const rows = (await api.listPlans()).filter((plan) => (
        !plan.read_only
        && (
          plan.display_state === 'awaiting_review'
          || (plan.display_state === 'approved' && plan.target_task_id == null)
        )
      ));
      setPlans(rows);
      setError(null);
      onVisibilityChange?.(rows.length > 0);
      setSelectedId((current) => current != null && rows.some((plan) => plan.id === current) ? current : null);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : String(reason));
      onVisibilityChange?.(true);
    }
  }, [onVisibilityChange]);

  useEffect(() => {
    const initial = window.setTimeout(() => void refresh(), 0);
    const timer = window.setInterval(() => void refresh(), 15000);
    return () => {
      window.clearTimeout(initial);
      window.clearInterval(timer);
    };
  }, [refresh]);
  usePlanEvents(plans, refresh);
  const selected = plans.find((plan) => plan.id === selectedId) || null;
  if (plans.length === 0 && !error) return null;

  return <section className="space-y-3" aria-label="Plans requiring review or execution">
    <div className="flex items-center gap-2"><h3 className="text-sm font-semibold text-gray-300">Review or execute</h3><span className="rounded-full bg-indigo-600 px-2 py-0.5 text-xs font-bold text-white">{plans.length}</span></div>
    {error && <div className="rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs text-red-400">{error}</div>}
    {plans.map((plan) => <article key={plan.id} className="flex items-center gap-3 rounded-xl border border-gray-700/70 bg-gray-800 px-4 py-3.5">
      <div className="min-w-0 flex-1"><div className="flex flex-wrap items-center gap-2"><h3 className="truncate text-sm font-semibold text-gray-100">#{plan.id} {plan.title}</h3>{plan.current_version && <span className="rounded-full bg-indigo-500/15 px-2 py-0.5 text-[10px] font-semibold text-indigo-300">v{plan.current_version.version_number}</span>}<span className="rounded-full border border-gray-600 px-2 py-0.5 text-[10px] text-gray-400">{plan.target_task_id ? `Task #${plan.target_task_id}` : plan.display_state === 'approved' ? 'Ready to execute' : 'Standalone'}</span></div></div>
      <button type="button" onClick={() => setSelectedId(plan.id)} className="flex items-center gap-1 rounded-lg bg-indigo-600 px-3 py-2 text-xs font-medium text-white hover:bg-indigo-500">Open <ChevronRight size={13} /></button>
    </article>)}
    {selected && <div className="fixed inset-0 z-[80] flex items-end justify-center bg-black/65 sm:items-center sm:p-5" onMouseDown={(event) => event.target === event.currentTarget && close()}><div ref={dialogRef} role="dialog" aria-modal="true" aria-label={`Plan #${selected.id}`} className={`w-full overflow-hidden border border-gray-700 bg-gray-900 shadow-2xl sm:h-[min(88vh,860px)] sm:max-w-5xl sm:rounded-2xl ${expanded ? 'h-[100dvh]' : 'h-[70dvh]'}`}><button type="button" onClick={() => setExpanded((value) => !value)} className="absolute left-1/2 top-2 z-10 h-1.5 w-12 -translate-x-1/2 rounded-full bg-gray-600 transition-colors hover:bg-gray-500 sm:hidden" aria-label={expanded ? 'Collapse Plan detail' : 'Expand Plan detail'} /><PlanDetail key={selected.id} plan={selected} onRefresh={refresh} onClose={close} onNavigateTask={onNavigateTask} /></div></div>}
  </section>;
}
