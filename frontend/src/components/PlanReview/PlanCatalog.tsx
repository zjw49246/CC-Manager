import type { PlanResource, Project } from '../../api/client';
import { useState } from 'react';
import { Archive, ArchiveRestore, ChevronRight, ListTodo } from '../icons';
import { planDisplayStateClassName, planDisplayStateLabel } from './planResourceStatus';

interface Props {
  plans: PlanResource[];
  projects: Project[];
  selectedPlanId: number | null;
  onSelectPlan: (planId: number) => void;
  onNavigateTask: (taskId: number) => void;
  onNavigateDelivery?: (runId: number) => void;
  onSetArchived: (plan: PlanResource, archived: boolean) => Promise<void>;
}

export function PlanCatalog({ plans, projects, selectedPlanId, onSelectPlan, onNavigateTask, onNavigateDelivery, onSetArchived }: Props) {
  const [updatingPlanId, setUpdatingPlanId] = useState<number | null>(null);
  if (plans.length === 0) {
    return <div className="rounded-xl border border-gray-800 bg-gray-900/50 px-4 py-10 text-center text-sm text-gray-500">No Plans match this filter.</div>;
  }

  return <div className="space-y-2">
      {plans.map((plan) => {
        const project = projects.find((item) => item.id === plan.project_id);
        const selected = plan.id === selectedPlanId;
        const appliedOlder = Boolean(
          plan.current_version
          && !plan.current_version.applied
          && plan.applications.some((item) => item.plan_version_id !== plan.current_version!.id),
        );
        const archived = plan.archived_at != null;
        const updating = updatingPlanId === plan.id;
        const archiveLabel = archived ? `Restore Plan #${plan.id}` : `Archive Plan #${plan.id}`;
        const setArchived = async () => {
          setUpdatingPlanId(plan.id);
          try {
            await onSetArchived(plan, !archived);
          } catch {
            // PlansPage owns the shared catalog error and authoritative refresh.
          } finally {
            setUpdatingPlanId(null);
          }
        };
        return <div key={plan.id} className={`flex w-full items-stretch rounded-xl border text-left transition-colors ${selected ? 'border-indigo-500/70 bg-indigo-500/15 ring-1 ring-inset ring-indigo-400/30' : 'border-gray-800 bg-gray-900/70 hover:border-gray-700 hover:bg-gray-800/70'}`}>
          <button type="button" onClick={() => onSelectPlan(plan.id)} aria-current={selected ? 'true' : undefined} className="flex min-w-0 flex-1 items-center gap-3 rounded-l-xl px-4 py-3 text-left">
          <div className="min-w-0 flex-1">
            <div className="flex flex-wrap items-center gap-2">
              <span className="text-xs text-gray-500">#{plan.id}</span>
              {plan.current_version && <span className="rounded-full bg-indigo-500/15 px-2 py-0.5 text-[10px] text-indigo-300">v{plan.current_version.version_number}</span>}
              <span className={`rounded-full border px-2 py-0.5 text-[10px] ${planDisplayStateClassName(plan.display_state)}`}>{planDisplayStateLabel(plan.display_state)}</span>
              {plan.read_only && <span className="rounded-full border border-indigo-500/40 bg-indigo-500/10 px-2 py-0.5 text-[10px] text-indigo-300">Capability · read-only</span>}
              {plan.delivery_run_id != null && <button type="button" onClick={(event) => { event.stopPropagation(); onNavigateDelivery?.(plan.delivery_run_id!); }} className="rounded-full border border-indigo-500/40 bg-indigo-500/10 px-2 py-0.5 text-[10px] text-indigo-300 hover:bg-indigo-500/20">DLV-{plan.delivery_run_id}</button>}
              {appliedOlder && <span className="rounded-full bg-teal-500/15 px-2 py-0.5 text-[10px] text-teal-300">earlier Version applied</span>}
            </div>
            <div className="mt-1 truncate text-sm font-semibold text-gray-100">{plan.title}</div>
            <div className="mt-1 truncate text-xs text-gray-500">{plan.target_task_id != null ? `Related to Task #${plan.target_task_id}` : 'Standalone Plan'}{project ? ` · ${project.name}` : ''}</div>
          </div>
          </button>
          <div className="flex shrink-0 items-center gap-1 pr-3">
            {plan.target_task_id != null && <button type="button" onClick={() => plan.delivery_run_id != null ? onNavigateDelivery?.(plan.delivery_run_id) : onNavigateTask(plan.target_task_id!)} aria-label={plan.delivery_run_id != null ? `Open Delivery DLV-${plan.delivery_run_id}` : `Open related Task #${plan.target_task_id}`} title={plan.delivery_run_id != null ? `Open Delivery DLV-${plan.delivery_run_id}` : `Open related Task #${plan.target_task_id}`} className="inline-flex items-center gap-1.5 rounded-lg px-2.5 py-2 text-xs text-gray-500 transition-colors hover:bg-indigo-500/15 hover:text-indigo-300"><ListTodo size={15} /><span className="hidden lg:inline">{plan.delivery_run_id != null ? `DLV-${plan.delivery_run_id}` : `Task #${plan.target_task_id}`}</span></button>}
            {!plan.read_only && plan.active_run_id == null && <button type="button" onClick={() => void setArchived()} disabled={updating} aria-label={archiveLabel} title={archived ? 'Restore' : 'Archive'} className="inline-flex items-center gap-1.5 rounded-lg px-2.5 py-2 text-xs text-gray-500 transition-colors hover:bg-gray-700/70 hover:text-gray-200 disabled:pointer-events-none disabled:opacity-40">{archived ? <ArchiveRestore size={15} /> : <Archive size={15} />}<span className="hidden lg:inline">{archived ? 'Restore' : 'Archive'}</span></button>}
            <ChevronRight size={15} aria-hidden="true" className={`shrink-0 ${selected ? 'text-indigo-300' : 'text-gray-600'}`} />
          </div>
        </div>;
      })}
    </div>;
}
