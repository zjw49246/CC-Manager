import { useCallback, useDeferredValue, useEffect, useMemo, useRef, useState } from 'react';

import { api, type PlanResource, type Project } from '../api/client';
import { PlanCatalog } from '../components/PlanReview/PlanCatalog';
import { PlanCreateForm } from '../components/PlanReview/PlanCreateForm';
import { PlanDetail } from '../components/PlanReview/PlanDetail';
import { PlanNeedsInputPanel } from '../components/PlanReview/PlanNeedsInputPanel';
import { usePlanEvents } from '../components/PlanReview/usePlanEvents';
import { VersionedPlanPanel } from '../components/PlanReview/VersionedPlanPanel';
import { ProjectSelect } from '../components/ProjectSelect';
import { Archive, ChevronLeft, ChevronRight, Search, X } from '../components/icons';

const PAGE_SIZE = 20;
type KindFilter = 'all' | 'standalone' | 'related';
type StatusFilter = 'all' | 'waiting_user' | 'awaiting_review' | 'running' | 'approved' | 'applied' | 'failed' | 'rejected_cancelled';

const STATUS_OPTIONS: { value: StatusFilter; label: string }[] = [
  { value: 'all', label: 'All' },
  { value: 'waiting_user', label: 'Input' },
  { value: 'awaiting_review', label: 'Needs approval' },
  { value: 'running', label: 'Running' },
  { value: 'approved', label: 'Approved' },
  { value: 'applied', label: 'Applied' },
  { value: 'failed', label: 'Failed' },
  { value: 'rejected_cancelled', label: 'Rejected / Cancelled' },
];

const DISPLAY_STATE_QUERY: Record<StatusFilter, string | undefined> = {
  all: undefined,
  waiting_user: 'waiting_user',
  awaiting_review: 'awaiting_review',
  running: 'planner,reviewer,queued,running,cancelling',
  approved: 'approved',
  applied: 'applied',
  failed: 'failed',
  rejected_cancelled: 'rejected,cancelled',
};

const EMPTY_STATUS_COUNTS: Record<StatusFilter, number> = {
  all: 0,
  waiting_user: 0,
  awaiting_review: 0,
  running: 0,
  approved: 0,
  applied: 0,
  failed: 0,
  rejected_cancelled: 0,
};

interface Props {
  selectedPlanId: number | null;
  onSelectedPlanChange: (planId: number | null) => void;
  onNavigateTask: (taskId: number) => void;
  onNavigateDelivery?: (runId: number) => void;
  onNavigateSettings: () => void;
}

export function PlansPage({ selectedPlanId, onSelectedPlanChange, onNavigateTask, onNavigateDelivery, onNavigateSettings }: Props) {
  const [plans, setPlans] = useState<PlanResource[]>([]);
  const [selectedPlan, setSelectedPlan] = useState<PlanResource | null>(null);
  const [projects, setProjects] = useState<Project[]>([]);
  const [kind, setKind] = useState<KindFilter>('all');
  const [status, setStatus] = useState<StatusFilter>('all');
  const [projectId, setProjectId] = useState<number | undefined>();
  const [archivedOnly, setArchivedOnly] = useState(false);
  const [search, setSearch] = useState('');
  const deferredSearch = useDeferredValue(search);
  const [page, setPage] = useState(1);
  const [total, setTotal] = useState(0);
  const [statusCounts, setStatusCounts] = useState(EMPTY_STATUS_COUNTS);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [needsInputVisible, setNeedsInputVisible] = useState(false);
  const [reviewVisible, setReviewVisible] = useState(false);
  const loadedOnceRef = useRef(false);
  const refreshRequestRef = useRef(0);
  const close = useCallback(() => {
    refreshRequestRef.current += 1;
    setSelectedPlan(null);
    onSelectedPlanChange(null);
  }, [onSelectedPlanChange]);

  const baseQuery = useMemo(() => ({
    ...(kind !== 'all' ? { kind } : {}),
    ...(projectId != null ? { project_id: projectId } : {}),
    ...(archivedOnly ? { archived_only: true } : {}),
    ...(deferredSearch.trim() ? { q: deferredSearch.trim() } : {}),
  }), [archivedOnly, deferredSearch, kind, projectId]);
  const query = useMemo(() => ({
    ...baseQuery,
    ...(DISPLAY_STATE_QUERY[status] ? { display_state: DISPLAY_STATE_QUERY[status] } : {}),
  }), [baseQuery, status]);

  const refresh = useCallback(async (showLoading = false) => {
    const requestId = ++refreshRequestRef.current;
    const showInitialLoading = showLoading && !loadedOnceRef.current;
    if (showInitialLoading) setLoading(true);
    try {
      const offset = (page - 1) * PAGE_SIZE;
      const [rows, count, projectRows, detail, ...counts] = await Promise.all([
        api.listPlans({ ...query, limit: PAGE_SIZE, offset }),
        api.countPlans(query),
        api.listProjects(),
        selectedPlanId != null ? api.getPlan(selectedPlanId) : Promise.resolve(null),
        ...STATUS_OPTIONS.map((option) => api.countPlans({
          ...baseQuery,
          ...(DISPLAY_STATE_QUERY[option.value] ? { display_state: DISPLAY_STATE_QUERY[option.value] } : {}),
        })),
      ]);
      if (requestId !== refreshRequestRef.current) return;
      setPlans(rows);
      setTotal(count.total);
      setStatusCounts(Object.fromEntries(STATUS_OPTIONS.map((option, index) => [option.value, counts[index].total])) as Record<StatusFilter, number>);
      setProjects(projectRows);
      setSelectedPlan(detail);
      setError(null);
    } catch (reason) {
      if (requestId !== refreshRequestRef.current) return;
      setError(reason instanceof Error ? reason.message : String(reason));
    } finally {
      if (requestId === refreshRequestRef.current) {
        loadedOnceRef.current = true;
        setLoading(false);
      }
    }
  }, [baseQuery, page, query, selectedPlanId]);

  useEffect(() => {
    void refresh(true);
    const timer = window.setInterval(() => void refresh(), 15000);
    return () => window.clearInterval(timer);
  }, [refresh]);
  useEffect(() => setPage(1), [query]);
  usePlanEvents(selectedPlan && !plans.some((plan) => plan.id === selectedPlan.id) ? [...plans, selectedPlan] : plans, refresh);

  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE));
  const selectPlan = (planId: number) => {
    const local = plans.find((plan) => plan.id === planId);
    if (local) setSelectedPlan(local);
    onSelectedPlanChange(planId);
  };
  const created = (createdPlan: PlanResource) => {
    const requestedStates = DISPLAY_STATE_QUERY[status]?.split(',');
    const normalizedSearch = deferredSearch.trim().toLowerCase();
    const matchesCurrentCatalog = (
      (kind === 'all' || (kind === 'standalone') === (createdPlan.target_task_id == null))
      && (projectId == null || createdPlan.project_id === projectId)
      && (archivedOnly ? createdPlan.archived_at != null : createdPlan.archived_at == null)
      && (!requestedStates || requestedStates.includes(createdPlan.display_state))
      && (!normalizedSearch || `${createdPlan.title}\n${createdPlan.initial_request}`.toLowerCase().includes(normalizedSearch))
    );
    if (page === 1 && matchesCurrentCatalog) {
      setPlans((current) => [createdPlan, ...current.filter((item) => item.id !== createdPlan.id)]);
      setTotal((current) => current + 1);
      void refresh();
    } else if (matchesCurrentCatalog) {
      setPage(1);
    } else {
      void refresh();
    }
  };
  const setArchived = async (plan: PlanResource, archived: boolean) => {
    try {
      await api.updatePlan(plan.id, { archived, expected_lock_version: plan.lock_version });
    } catch (reason) {
      await refresh();
      setError(reason instanceof Error ? reason.message : String(reason));
      return;
    }
    await refresh();
  };

  return <div className="space-y-6">
    {selectedPlan ? (
      <section aria-label={`Plan #${selectedPlan.id}`} className="min-h-[calc(100dvh-8rem)] overflow-hidden rounded-lg border border-gray-800 bg-gray-900/60">
        <PlanDetail key={selectedPlan.id} plan={selectedPlan} onRefresh={() => refresh()} onClose={close} onNavigateTask={onNavigateTask} onNavigateDelivery={onNavigateDelivery} embedded />
      </section>
    ) : <>
    <PlanCreateForm onCreated={created} onNavigateSettings={onNavigateSettings} />

    <section className={needsInputVisible || reviewVisible ? 'space-y-4' : ''} aria-label={needsInputVisible || reviewVisible ? 'Plans requiring action' : undefined}>
      {(needsInputVisible || reviewVisible) && <h2 className="text-base font-semibold text-gray-200">Plans requiring action</h2>}
      <PlanNeedsInputPanel onVisibilityChange={setNeedsInputVisible} />
      <VersionedPlanPanel onVisibilityChange={setReviewVisible} onNavigateTask={onNavigateTask} />
    </section>

    <section className="space-y-3" aria-label="All Plans">
      <div className="flex flex-wrap items-center gap-2">
        <select aria-label="Plan kind" value={kind} onChange={(event) => setKind(event.target.value as KindFilter)} className="h-9 rounded-lg border border-gray-700 bg-gray-800 px-3 text-xs text-gray-300">
          <option value="all">All Plans</option>
          <option value="standalone">Standalone</option>
          <option value="related">Related</option>
        </select>
        <ProjectSelect projects={projects.filter((project) => project.show_in_selector)} value={projectId} onChange={(value) => setProjectId(value ? Number(value) : undefined)} placeholder="All Projects" className="[&>button]:h-9 [&>button]:rounded-lg" />
        <div className="relative h-9 min-w-[180px] flex-1 sm:max-w-sm"><Search size={13} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-500" /><input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Search Plans" className="h-9 w-full rounded-lg border border-gray-700 bg-gray-800 pl-8 pr-8 text-xs text-gray-200 outline-none focus:border-indigo-500" />{search && <button type="button" onClick={() => setSearch('')} className="absolute right-2 top-1/2 -translate-y-1/2 text-gray-500" aria-label="Clear Plan search"><X size={13} /></button>}</div>
        <button type="button" onClick={() => setArchivedOnly((value) => !value)} aria-pressed={archivedOnly} className={`flex h-9 items-center gap-1.5 rounded-lg border px-3 text-xs ${archivedOnly ? 'border-amber-500/50 bg-amber-500/15 text-amber-300' : 'border-gray-700 bg-gray-800 text-gray-400 hover:text-gray-200'}`}><Archive size={13} /> Archived only</button>
      </div>
      <div className="flex gap-1 overflow-x-auto pb-1">{STATUS_OPTIONS.map((option) => <button key={option.value} type="button" onClick={() => setStatus(option.value)} aria-pressed={status === option.value} className={`whitespace-nowrap rounded-full px-3 py-1.5 text-xs ${status === option.value ? 'bg-indigo-500/20 text-indigo-300' : 'text-gray-500 hover:bg-gray-800 hover:text-gray-300'}`}>{option.label} <span className="tabular-nums">{statusCounts[option.value]}</span></button>)}</div>
      {error && <div className="rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs text-red-300">{error}</div>}
      {loading ? <div className="py-12 text-center text-sm text-gray-500">Loading Plans…</div> : <PlanCatalog plans={plans} projects={projects} selectedPlanId={selectedPlanId} onSelectPlan={selectPlan} onNavigateTask={onNavigateTask} onNavigateDelivery={onNavigateDelivery} onSetArchived={setArchived} />}
      {totalPages > 1 && <div className="flex items-center justify-center gap-3 py-2"><button type="button" onClick={() => setPage((value) => Math.max(1, value - 1))} disabled={page <= 1} className="rounded p-1.5 text-gray-400 disabled:opacity-30" aria-label="Previous Plans page"><ChevronLeft size={17} /></button><span className="text-xs text-gray-500">{page} / {totalPages} · {total} Plans</span><button type="button" onClick={() => setPage((value) => Math.min(totalPages, value + 1))} disabled={page >= totalPages} className="rounded p-1.5 text-gray-400 disabled:opacity-30" aria-label="Next Plans page"><ChevronRight size={17} /></button></div>}
    </section>

    </>}
  </div>;
}
