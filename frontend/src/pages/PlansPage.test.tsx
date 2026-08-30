import { useState } from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { api, type PlanResource } from '../api/client';
import { PlansPage } from './PlansPage';

vi.mock('../api/client', () => ({
  api: {
    listPlans: vi.fn(),
    countPlans: vi.fn(),
    listProjects: vi.fn(),
    getPlan: vi.fn(),
    updatePlan: vi.fn(),
  },
}));

vi.mock('../components/PlanReview/PlanCreateForm', () => ({
  PlanCreateForm: ({ onCreated, onNavigateSettings }: { onCreated: (plan: PlanResource) => void; onNavigateSettings: () => void }) => <div>
    <button type="button" onClick={() => onCreated(createdPlan)}>Create standalone Plan</button>
    <button type="button" onClick={onNavigateSettings}>Plan settings</button>
  </div>,
}));
vi.mock('../components/PlanReview/PlanNeedsInputPanel', () => ({
  PlanNeedsInputPanel: ({ onVisibilityChange }: { onVisibilityChange?: (visible: boolean) => void }) => (
    <button type="button" onClick={() => onVisibilityChange?.(true)}>Show input actions</button>
  ),
}));
vi.mock('../components/PlanReview/VersionedPlanPanel', () => ({
  VersionedPlanPanel: () => <div>Review panel</div>,
}));
vi.mock('../components/PlanReview/PlanCatalog', () => ({
  PlanCatalog: ({
    plans,
    selectedPlanId,
    onSelectPlan,
    onNavigateTask,
    onSetArchived,
  }: {
    plans: PlanResource[];
    selectedPlanId: number | null;
    onSelectPlan: (id: number) => void;
    onNavigateTask: (taskId: number) => void;
    onSetArchived: (plan: PlanResource, archived: boolean) => Promise<void>;
  }) => <div>{plans.map((item) => (
    <div key={item.id}>
      <button type="button" aria-pressed={selectedPlanId === item.id} onClick={() => onSelectPlan(item.id)}>{item.title}</button>
      {item.target_task_id != null && <button type="button" onClick={() => onNavigateTask(item.target_task_id!)}>Open related Task #{item.target_task_id}</button>}
      {item.active_run_id == null && <button type="button" onClick={() => void onSetArchived(item, item.archived_at == null)}>{item.archived_at ? `Restore Plan #${item.id}` : `Archive Plan #${item.id}`}</button>}
    </div>
  ))}</div>,
}));
vi.mock('../components/PlanReview/PlanDetail', () => ({
  PlanDetail: ({ plan, onClose, onNavigateTask }: { plan: PlanResource; onClose?: () => void; onNavigateTask: (taskId: number) => void }) => <div>
    <span>Detail for {plan.title}</span>
    {plan.target_task_id != null && <button type="button" onClick={() => { onNavigateTask(plan.target_task_id!); onClose?.(); }}>Open related Task #{plan.target_task_id}</button>}
    <button type="button" onClick={onClose}>Close detail</button>
  </div>,
}));
vi.mock('../components/PlanReview/usePlanEvents', () => ({
  usePlanEvents: vi.fn(),
}));
vi.mock('../components/ProjectSelect', () => ({
  ProjectSelect: ({ onChange }: { onChange: (value: string) => void }) => <button type="button" onClick={() => onChange('3')}>Select project</button>,
}));
vi.mock('../hooks/useDialogA11y', () => ({
  useDialogA11y: () => ({ current: null }),
}));

const plan = {
  id: 14,
  title: 'Standalone architecture',
  initial_request: 'Design it',
  initial_attachments: null,
  target_task_id: null,
  project_id: 3,
  target_repo: '/repo',
  target_branch: 'main',
  worker_id: null,
  priority: 0,
  timeout_hours: null,
  created_by: 1,
  current_version_id: null,
  active_run_id: null,
  forked_from_version_id: null,
  archived_at: null,
  closed_at: null,
  lock_version: 0,
  created_at: '2026-08-03T00:00:00Z',
  updated_at: '2026-08-03T00:00:00Z',
  display_state: 'queued',
  legacy: false,
  latest_run_status: 'queued',
  latest_run_error: null,
  pipeline_config: {},
  application: null,
  applications: [],
  current_version: null,
  active_run: null,
  open_input_request: null,
} as PlanResource;

const createdPlan = {
  ...plan,
  id: 15,
  title: 'Newly created plan',
  initial_request: 'Plan the next iteration',
} as PlanResource;

function StatefulPlansPage() {
  const [selectedPlanId, setSelectedPlanId] = useState<number | null>(null);
  return <PlansPage selectedPlanId={selectedPlanId} onSelectedPlanChange={setSelectedPlanId} onNavigateTask={vi.fn()} onNavigateSettings={vi.fn()} />;
}

describe('PlansPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(api.listPlans).mockResolvedValue([plan]);
    vi.mocked(api.countPlans).mockResolvedValue({ total: 1 });
    vi.mocked(api.listProjects).mockResolvedValue([]);
    vi.mocked(api.getPlan).mockResolvedValue(plan);
    vi.mocked(api.updatePlan).mockResolvedValue(plan);
  });

  it('owns the Plan catalog, hides an empty action heading, and supports deep-link selection', async () => {
    const onSelectedPlanChange = vi.fn();
    render(<PlansPage selectedPlanId={null} onSelectedPlanChange={onSelectedPlanChange} onNavigateTask={vi.fn()} onNavigateSettings={vi.fn()} />);

    expect(await screen.findByRole('button', { name: plan.title })).toBeInTheDocument();
    expect(screen.queryByRole('region', { name: 'Plans requiring action' })).not.toBeInTheDocument();
    expect(screen.queryByRole('heading', { name: 'Plans requiring action' })).not.toBeInTheDocument();
    expect(api.listPlans).toHaveBeenCalledWith(expect.objectContaining({ limit: 20, offset: 0 }));

    await userEvent.click(screen.getByRole('button', { name: 'Show input actions' }));
    expect(screen.getByRole('region', { name: 'Plans requiring action' })).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: 'Plans requiring action' })).toBeInTheDocument();

    await userEvent.click(screen.getByRole('button', { name: plan.title }));
    expect(onSelectedPlanChange).toHaveBeenCalledWith(plan.id);
    expect(screen.getByText(`Detail for ${plan.title}`)).toBeInTheDocument();
  });

  it('uses canonical Plan search/archive filters and keeps a newly created Plan in the catalog', async () => {
    const onSelectedPlanChange = vi.fn();
    render(<PlansPage selectedPlanId={null} onSelectedPlanChange={onSelectedPlanChange} onNavigateTask={vi.fn()} onNavigateSettings={vi.fn()} />);

    await screen.findByRole('button', { name: plan.title });
    await userEvent.type(screen.getByPlaceholderText('Search Plans'), 'architecture');
    await userEvent.click(screen.getByRole('button', { name: 'Archived only' }));

    await waitFor(() => expect(api.listPlans).toHaveBeenCalledWith(expect.objectContaining({
      archived_only: true,
      q: 'architecture',
    })));

    await userEvent.click(screen.getByRole('button', { name: 'Create standalone Plan' }));
    expect(onSelectedPlanChange).not.toHaveBeenCalled();
    expect(screen.queryByText(`Detail for ${plan.title}`)).not.toBeInTheDocument();
  });

  it('shows status counts and applies base filters to every mapped count query', async () => {
    const totals: Record<string, number> = {
      all: 70,
      waiting_user: 4,
      awaiting_review: 5,
      'planner,reviewer,queued,running,cancelling': 6,
      approved: 7,
      applied: 8,
      failed: 9,
      'rejected,cancelled': 10,
    };
    vi.mocked(api.countPlans).mockImplementation(async (params = {}) => ({ total: totals[params.display_state || 'all'] }));
    render(<StatefulPlansPage />);

    expect(await screen.findByRole('button', { name: 'All 70' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Input 4' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Needs approval 5' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Running 6' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Approved 7' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Applied 8' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Failed 9' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Rejected / Cancelled 10' })).toBeInTheDocument();

    await userEvent.selectOptions(screen.getByRole('combobox', { name: 'Plan kind' }), 'standalone');
    await userEvent.click(screen.getByRole('button', { name: 'Select project' }));
    await userEvent.type(screen.getByPlaceholderText('Search Plans'), 'architecture');
    await userEvent.click(screen.getByRole('button', { name: 'Archived only' }));

    const base = { kind: 'standalone', project_id: 3, q: 'architecture', archived_only: true };
    await waitFor(() => {
      expect(api.countPlans).toHaveBeenCalledWith(base);
      for (const display_state of ['waiting_user', 'awaiting_review', 'planner,reviewer,queued,running,cancelling', 'approved', 'applied', 'failed', 'rejected,cancelled']) {
        expect(api.countPlans).toHaveBeenCalledWith({ ...base, display_state });
      }
    });
  });

  it('uses the selected status for the catalog and pagination total without changing preview counts', async () => {
    vi.mocked(api.countPlans).mockImplementation(async (params = {}) => ({
      total: params.display_state === 'planner,reviewer,queued,running,cancelling' ? 23 : 61,
    }));
    render(<StatefulPlansPage />);
    await screen.findByRole('button', { name: 'Running 23' });

    await userEvent.click(screen.getByRole('button', { name: 'Running 23' }));
    await waitFor(() => expect(api.listPlans).toHaveBeenCalledWith(expect.objectContaining({
      display_state: 'planner,reviewer,queued,running,cancelling',
      limit: 20,
      offset: 0,
    })));
    expect(screen.getByText('1 / 2 · 23 Plans')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'All 61' })).toBeInTheDocument();
  });

  it('uses a page-native Plan detail and restores the catalog when closed', async () => {
    let resolveRefresh!: (rows: PlanResource[]) => void;
    const pendingRefresh = new Promise<PlanResource[]>((resolve) => { resolveRefresh = resolve; });
    vi.mocked(api.listPlans)
      .mockResolvedValueOnce([plan])
      .mockReturnValueOnce(pendingRefresh)
      .mockResolvedValue([plan]);

    render(<StatefulPlansPage />);
    await screen.findByRole('button', { name: plan.title });

    await userEvent.click(screen.getByRole('button', { name: plan.title }));
    expect(screen.getByText(`Detail for ${plan.title}`)).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: plan.title })).not.toBeInTheDocument();
    expect(screen.getByRole('region', { name: `Plan #${plan.id}` })).toBeInTheDocument();
    expect(screen.queryByText('Loading Plans…')).not.toBeInTheDocument();

    resolveRefresh([plan]);
    await waitFor(() => expect(api.listPlans).toHaveBeenCalledTimes(2));
    await userEvent.click(screen.getByRole('button', { name: 'Close detail' }));
    expect(screen.queryByText(`Detail for ${plan.title}`)).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: plan.title })).toBeInTheDocument();
    expect(screen.queryByText('Loading Plans…')).not.toBeInTheDocument();
  });

  it('inserts a newly created Plan without blanking the catalog during reconciliation', async () => {
    let resolveRefresh!: (rows: PlanResource[]) => void;
    const pendingRefresh = new Promise<PlanResource[]>((resolve) => { resolveRefresh = resolve; });
    vi.mocked(api.listPlans)
      .mockResolvedValueOnce([plan])
      .mockReturnValueOnce(pendingRefresh);

    render(<StatefulPlansPage />);
    await screen.findByRole('button', { name: plan.title });

    await userEvent.click(screen.getByRole('button', { name: 'Create standalone Plan' }));
    expect(screen.getByRole('button', { name: createdPlan.title })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: plan.title })).toBeInTheDocument();
    expect(screen.queryByText('Loading Plans…')).not.toBeInTheDocument();

    resolveRefresh([createdPlan, plan]);
    await waitFor(() => expect(api.listPlans).toHaveBeenCalledTimes(2));
    expect(api.countPlans).toHaveBeenCalledTimes(18);
  });

  it('archives through the optimistic-lock API and refreshes the catalog and counts', async () => {
    vi.mocked(api.listPlans).mockResolvedValueOnce([plan]).mockResolvedValue([]);
    render(<StatefulPlansPage />);
    await screen.findByRole('button', { name: plan.title });
    const countCallsBefore = vi.mocked(api.countPlans).mock.calls.length;

    await userEvent.click(screen.getByRole('button', { name: `Archive Plan #${plan.id}` }));

    expect(api.updatePlan).toHaveBeenCalledWith(plan.id, { archived: true, expected_lock_version: plan.lock_version });
    await waitFor(() => expect(screen.queryByRole('button', { name: plan.title })).not.toBeInTheDocument());
    expect(vi.mocked(api.countPlans).mock.calls.length).toBeGreaterThanOrEqual(countCallsBefore + 9);
  });

  it('keeps the Plan and reports the API error when archiving fails', async () => {
    vi.mocked(api.updatePlan).mockRejectedValueOnce(new Error('Plan was changed elsewhere'));
    render(<StatefulPlansPage />);
    await screen.findByRole('button', { name: plan.title });

    await userEvent.click(screen.getByRole('button', { name: `Archive Plan #${plan.id}` }));

    expect(await screen.findByText('Plan was changed elsewhere')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: plan.title })).toBeInTheDocument();
  });

  it('uses the app navigation callback for a related Plan Task', async () => {
    const relatedPlan = { ...plan, target_task_id: 200 };
    vi.mocked(api.listPlans).mockResolvedValue([relatedPlan]);
    vi.mocked(api.getPlan).mockResolvedValue(relatedPlan);
    const onSelectedPlanChange = vi.fn();
    const onNavigateTask = vi.fn();

    render(<PlansPage selectedPlanId={null} onSelectedPlanChange={onSelectedPlanChange} onNavigateTask={onNavigateTask} onNavigateSettings={vi.fn()} />);
    await userEvent.click(await screen.findByRole('button', { name: 'Open related Task #200' }));

    expect(onNavigateTask).toHaveBeenCalledWith(200);
    expect(onSelectedPlanChange).not.toHaveBeenCalled();
  });

  it('provides an in-app shortcut to the Plan settings page', async () => {
    const onNavigateSettings = vi.fn();
    render(<PlansPage selectedPlanId={null} onSelectedPlanChange={vi.fn()} onNavigateTask={vi.fn()} onNavigateSettings={onNavigateSettings} />);

    await userEvent.click(screen.getByRole('button', { name: 'Plan settings' }));
    expect(onNavigateSettings).toHaveBeenCalledOnce();
  });
});
