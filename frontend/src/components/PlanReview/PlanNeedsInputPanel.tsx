import { useCallback, useRef, useState } from 'react';

import { api, type PlanResource } from '../../api/client';
import { useDialogA11y } from '../../hooks/useDialogA11y';
import { ChevronRight, MessageCircle, X } from '../icons';
import { PlanInputForm } from './PlanInputForm';
import { usePlanEvents } from './usePlanEvents';
import { useVisibilityAwareInterval } from '../../hooks/useVisibilityAwareInterval';

interface Props {
  onVisibilityChange?: (visible: boolean) => void;
}

export function PlanNeedsInputPanel({ onVisibilityChange }: Props = {}) {
  const [plans, setPlans] = useState<PlanResource[]>([]);
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);
  const refreshRequest = useRef(0);
  const close = useCallback(() => setSelectedId(null), []);
  const dialogRef = useDialogA11y(selectedId != null, close);

  const refresh = useCallback(async () => {
    const requestId = ++refreshRequest.current;
    try {
      const rows = await api.listPlans({ display_state: 'waiting_user' });
      if (requestId !== refreshRequest.current) return;
      setPlans(rows);
      setError(null);
      onVisibilityChange?.(rows.length > 0);
      setSelectedId((current) => current != null && rows.some((plan) => plan.id === current) ? current : null);
    } catch (fetchError) {
      if (requestId !== refreshRequest.current) return;
      setError(fetchError instanceof Error ? fetchError.message : String(fetchError));
      onVisibilityChange?.(true);
    }
  }, [onVisibilityChange]);

  useVisibilityAwareInterval(() => refresh(), 15000);
  usePlanEvents(plans, refresh);

  const selected = plans.find((plan) => plan.id === selectedId) || null;
  if (plans.length === 0 && !error) return null;

  return (
    <section className="space-y-3" aria-label="Plans needing your input">
      <div className="flex items-center gap-2">
        <MessageCircle size={17} className="text-amber-400" />
        <h3 className="text-sm font-semibold text-gray-300">Input needed</h3>
        <span className="rounded-full bg-amber-500 px-2 py-0.5 text-xs font-bold text-gray-950">{plans.length}</span>
      </div>
      {error && <div className="rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs text-red-400">{error}</div>}
      <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
        {plans.map((plan) => (
          <button
            key={plan.id}
            type="button"
            onClick={() => setSelectedId(plan.id)}
            className="flex items-center gap-3 rounded-xl border border-amber-500/25 bg-amber-500/5 px-4 py-3 text-left hover:border-amber-400/50"
          >
            <div className="min-w-0 flex-1">
              <div className="truncate text-sm font-semibold text-gray-100">#{plan.id} {plan.title}</div>
              <div className="mt-1 text-xs text-gray-400">
                {plan.open_input_request?.questions.length || 0} questions · {plan.target_task_id ? `Task #${plan.target_task_id}` : 'Standalone'}
              </div>
            </div>
            <ChevronRight size={15} className="shrink-0 text-amber-300" />
          </button>
        ))}
      </div>

      {selected?.active_run && selected.open_input_request && (
        <div className="fixed inset-0 z-[80] flex items-end justify-center bg-black/65 sm:items-center sm:p-5" onMouseDown={(event) => event.target === event.currentTarget && close()}>
          <div ref={dialogRef} role="dialog" aria-modal="true" aria-label={`Input for Plan #${selected.id}`} className="max-h-[70dvh] w-full overflow-y-auto border border-gray-700 bg-gray-900 p-4 shadow-2xl sm:max-h-[88vh] sm:max-w-2xl sm:rounded-2xl sm:p-5">
            <div className="mb-4 flex items-start justify-between gap-3">
              <div>
                <div className="text-sm font-semibold text-gray-100">Plan #{selected.id} · {selected.title}</div>
                <div className="mt-1 text-xs text-gray-500">Run #{selected.active_run.id} · round {selected.active_run.round}</div>
              </div>
              <button type="button" onClick={close} className="rounded-lg p-1.5 text-gray-500 hover:bg-gray-800 hover:text-gray-200" aria-label="Close"><X size={16} /></button>
            </div>
            <PlanInputForm
              key={selected.open_input_request.id}
              run={selected.active_run}
              request={selected.open_input_request}
              compact
              onAnswered={refresh}
            />
          </div>
        </div>
      )}
    </section>
  );
}
