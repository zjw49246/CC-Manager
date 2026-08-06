import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { api, type PlanResource, type PlanRun, type PlanVersion } from '../../api/client';
import { PlanDetail } from './PlanDetail';

vi.mock('../../api/client', () => ({
  isApiRequestError: () => false,
  api: {
    listPlanVersions: vi.fn(),
    listPlanResourceRuns: vi.fn().mockResolvedValue([]),
    createPlanRun: vi.fn(),
    answerPlanInput: vi.fn(),
    updatePlan: vi.fn(),
    resolvePlanApplicationDelivery: vi.fn(),
    getPlanVersionStaleness: vi.fn().mockResolvedValue({
      stale: false,
      hard_conflict: false,
      reasons: [],
      hard_conflicts: [],
      can_confirm: false,
    }),
  },
}));

function version(overrides: Partial<PlanVersion>): PlanVersion {
  return {
    id: 12,
    plan_id: 4,
    version_number: 2,
    parent_version_id: 11,
    produced_by_run_id: 22,
    produced_by_step_id: 32,
    content: '# Current proposal',
    context_session_id: 'session-1',
    context_log_id: 41,
    repo_revision: { head: 'planner-head' },
    reviewer_repo_revision: { head: 'reviewer-head' },
    review_verdict: 'approve',
    review_feedback: null,
    reviewed_by_step_id: 33,
    review_exhausted: false,
    reviewed_at: '2026-08-02T08:00:00Z',
    human_decision: 'pending',
    decided_at: null,
    decided_by: null,
    superseded_by_version_id: null,
    applied: false,
    display_state: 'awaiting_review',
    created_at: '2026-08-02T08:00:00Z',
    ...overrides,
  };
}

function plan(current: PlanVersion, prior: PlanVersion): PlanResource {
  return {
    id: 4,
    title: 'Version history',
    initial_request: 'Design the migration',
    initial_attachments: null,
    target_task_id: null,
    project_id: 7,
    target_repo: '/repo',
    target_branch: 'main',
    worker_id: null,
    priority: 0,
    timeout_hours: null,
    created_by: 1,
    current_version_id: current.id,
    active_run_id: null,
    forked_from_version_id: null,
    archived_at: null,
    closed_at: null,
    lock_version: 3,
    created_at: '2026-08-02T08:00:00Z',
    updated_at: '2026-08-02T09:00:00Z',
    display_state: 'awaiting_review',
    legacy: false,
    latest_run_status: 'completed',
    latest_run_error: null,
    pipeline_config: {
      version: 1,
      planner: {
        primary: { provider: 'codex', model: 'gpt-5.6-sol', effort: 'ultra' },
        fallback: { provider: 'claude', model: 'claude-opus-4-6', effort: 'high' },
      },
      reviewer: {
        enabled: true,
        primary: { provider: 'claude', model: 'claude-opus-4-6', effort: 'high' },
        fallback: { provider: 'codex', model: 'gpt-5.6-terra', effort: 'high' },
      },
      max_revision_cycles: 2,
      max_interactions: 5,
    },
    application: {
      id: 51,
      plan_id: 4,
      plan_version_id: prior.id,
      application_type: 'execution_task',
      target_task_id: null,
      target_session_id: null,
      user_log_id: null,
      execution_task_id: 91,
      execution_task_available: true,
      created_at: '2026-08-02T08:30:00Z',
    },
    applications: [{
      id: 51,
      plan_id: 4,
      plan_version_id: prior.id,
      application_type: 'execution_task',
      target_task_id: null,
      target_session_id: null,
      user_log_id: null,
      execution_task_id: 91,
      execution_task_available: true,
      created_at: '2026-08-02T08:30:00Z',
    }],
    application_attempts: [],
    current_version: current,
    active_run: null,
    open_input_request: null,
  };
}

describe('PlanDetail', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.setItem('cc_user', JSON.stringify({ id: 1, role: 'admin' }));
  });

  it('clears revision state when the Plan identity changes', async () => {
    const priorA = version({ id: 11, plan_id: 4, version_number: 1 });
    const currentA = version({ id: 12, plan_id: 4, version_number: 2 });
    const priorB = version({ id: 21, plan_id: 5, version_number: 1 });
    const currentB = version({ id: 22, plan_id: 5, version_number: 2 });
    const planA = plan(currentA, priorA);
    const planB = { ...plan(currentB, priorB), id: 5, title: 'Second Plan' };
    vi.mocked(api.listPlanVersions).mockImplementation(async (planId) => (
      planId === 4 ? [currentA, priorA] : [currentB, priorB]
    ));
    vi.mocked(api.listPlanResourceRuns).mockResolvedValue([]);

    const { rerender } = render(
      <PlanDetail plan={planA} onRefresh={vi.fn()} />,
    );
    const revision = await screen.findByPlaceholderText('Revise from v2…');
    await userEvent.type(revision, 'draft for Plan A');

    rerender(<PlanDetail plan={planB} onRefresh={vi.fn()} />);

    await waitFor(() => expect(
      screen.getByPlaceholderText('Revise from v2…'),
    ).toHaveValue(''));
  });

  it('keeps an applied older Version, current review actions, routes, and execution link visible', async () => {
    const prior = version({
      id: 11,
      version_number: 1,
      parent_version_id: null,
      content: '# Applied proposal',
      human_decision: 'approved',
      applied: true,
      display_state: 'applied',
    });
    const current = version({});
    vi.mocked(api.listPlanVersions).mockResolvedValue([current, prior]);
    const navigate = vi.fn();

    render(<PlanDetail plan={plan(current, prior)} onRefresh={vi.fn()} onNavigateTask={navigate} />);

    expect(await screen.findByText(/v1 applied/)).toBeInTheDocument();
    expect(screen.getByRole('option', { name: 'v2 · Awaiting approval · Current' })).toBeInTheDocument();
    expect(screen.getByRole('option', { name: 'v1 · Applied' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Approve v2 & create execution Task' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Approve v2 only' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Reject v2' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Refresh contexts and regenerate Plan' }))
      .not.toBeInTheDocument();
    expect(screen.getByText(/Input pauses: 5/)).toBeInTheDocument();
    expect(screen.getByText(/Reviewer: claude \/ claude-opus-4-6 \/ high/).parentElement)
      .toHaveTextContent('Fallback: codex / gpt-5.6-terra / high');
    expect(screen.queryByText(/Application history/)).not.toBeInTheDocument();
    expect(within(screen.getByText('Debug information').closest('details')!)
      .getByText('Applications (1)')).toBeInTheDocument();

    await userEvent.click(screen.getByRole('button', { name: 'Open v1 execution Task #91' }));
    expect(navigate).toHaveBeenCalledWith(91);
    await userEvent.selectOptions(screen.getByRole('combobox', { name: 'Plan Version' }), '11');
    expect(await screen.findByRole('heading', { level: 1, name: 'Applied proposal' })).toBeInTheDocument();
    expect(screen.getByText(/Historical Version/)).toBeInTheDocument();
  });

  it("offers a one-click revision only for the current exhausted review and carries its feedback", async () => {
    const prior = version({ id: 11, version_number: 1 });
    const current = version({
      review_verdict: 'exhausted',
      review_feedback: 'Clarify rollback boundaries and add an exact concurrency test.',
      review_exhausted: true,
    });
    vi.mocked(api.listPlanVersions).mockResolvedValue([current, prior]);
    vi.mocked(api.createPlanRun).mockResolvedValue({} as PlanRun);

    render(<PlanDetail plan={plan(current, prior)} onRefresh={vi.fn()} />);

    const button = await screen.findByRole('button', {
      name: "Revise with Reviewer's latest feedback",
    });
    expect(button).toBeEnabled();
    expect(screen.getByText(/feedback is included automatically/)).toBeInTheDocument();

    await userEvent.click(button);

    await waitFor(() => expect(api.createPlanRun).toHaveBeenCalledWith(4, {
      run_type: 'user_revision',
      request: [
        "Continue iterating Plan v2. Resolve every item in the Reviewer's latest feedback. Preserve sound existing decisions unless a change is required by that feedback.",
        "## Reviewer's latest feedback\nClarify rollback boundaries and add an exact concurrency test.",
      ].join('\n\n'),
      base_version_id: 12,
      expected_current_version_id: 12,
    }));
  });

  it("does not offer the latest-feedback revision for a Reviewer-approved Version", async () => {
    const prior = version({
      id: 11,
      version_number: 1,
      review_verdict: 'exhausted',
      review_feedback: 'Older unresolved feedback.',
      review_exhausted: true,
      superseded_by_version_id: 12,
      display_state: 'superseded',
    });
    const current = version({
      review_verdict: 'approve',
      review_feedback: 'Ready to implement.',
      review_exhausted: false,
    });
    vi.mocked(api.listPlanVersions).mockResolvedValue([current, prior]);

    render(<PlanDetail plan={plan(current, prior)} onRefresh={vi.fn()} />);

    await screen.findByText('Ready to implement.');
    expect(screen.queryByRole('button', {
      name: "Revise with Reviewer's latest feedback",
    })).not.toBeInTheDocument();

    await userEvent.selectOptions(screen.getByRole('combobox', { name: 'Plan Version' }), '11');
    expect(await screen.findByText('Older unresolved feedback.')).toBeInTheDocument();
    expect(screen.queryByRole('button', {
      name: "Revise with Reviewer's latest feedback",
    })).not.toBeInTheDocument();
  });

  it('labels an undecided historical Version as superseded and disables a missing execution Task link', async () => {
    const prior = version({
      id: 11,
      version_number: 1,
      parent_version_id: null,
      superseded_by_version_id: 12,
      display_state: 'superseded',
    });
    const current = version({
      human_decision: 'approved',
      applied: true,
      display_state: 'applied',
    });
    const resource = plan(current, prior);
    resource.application = {
      ...resource.applications[0],
      plan_version_id: current.id,
      execution_task_available: false,
    };
    resource.applications = [resource.application];
    vi.mocked(api.listPlanVersions).mockResolvedValue([current, prior]);

    render(<PlanDetail plan={resource} onRefresh={vi.fn()} onNavigateTask={vi.fn()} />);

    expect(await screen.findByRole('option', { name: 'v1 · Superseded (not decided)' })).toBeInTheDocument();
    expect(screen.getByRole('option', { name: 'v2 · Applied · Current' })).toBeInTheDocument();
    expect(screen.getByText('v2 applied · execution Task #91 unavailable')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /Open v2 execution Task/ })).not.toBeInTheDocument();
  });

  it('shows uncertain delivery evidence and lets an administrator resolve it', async () => {
    const prior = version({
      id: 11,
      version_number: 1,
      human_decision: 'approved',
      applied: true,
      display_state: 'applied',
    });
    const current = version({});
    const resource = plan(current, prior);
    resource.application = {
      ...resource.applications[0],
      application_type: 'chat_message',
      execution_task_id: null,
      execution_task_available: null,
      target_task_id: 8,
      user_log_id: 44,
      application_receipt_key: 'receipt-uncertain',
      delivery_status: 'uncertain',
      delivery_error: 'Automatic replay was blocked',
      launch_evidence: { task_id: 8, instance_id: 3, retry_count: 2 },
    };
    resource.applications = [resource.application];
    vi.mocked(api.listPlanVersions).mockResolvedValue([current, prior]);
    vi.mocked(api.resolvePlanApplicationDelivery).mockResolvedValue({
      receipt_key: 'receipt-uncertain',
      action: 'release_for_retry',
      plan_ids: [4],
      target_task_id: 8,
    });
    vi.spyOn(window, 'confirm').mockReturnValue(true);
    vi.spyOn(window, 'prompt').mockReturnValue('No matching native turn exists');

    render(<PlanDetail plan={resource} onRefresh={vi.fn()} />);

    expect(await screen.findByText('Plan delivery needs reconciliation')).toBeInTheDocument();
    expect(screen.getAllByText(/automatic replay was blocked/i)).toHaveLength(2);
    await userEvent.click(screen.getByRole('button', { name: 'Confirm no turn · release Version' }));

    await waitFor(() => expect(api.resolvePlanApplicationDelivery).toHaveBeenCalledWith(
      4,
      'receipt-uncertain',
      'release_for_retry',
      'No matching native turn exists',
    ));
  });

  it('keeps a released delivery resolution discoverable in audit history', async () => {
    const prior = version({ id: 11, version_number: 1, human_decision: 'approved' });
    const current = version({});
    const resource = plan(current, prior);
    resource.application = null;
    resource.applications = [];
    resource.application_attempts = [{
      id: 71,
      plan_id: 4,
      plan_version_id: prior.id,
      application_receipt_key: 'receipt-released',
      application_type: 'chat_message',
      target_task_id: 8,
      target_session_id: 'session-8',
      user_log_id: 44,
      execution_task_id: null,
      applied_by: 1,
      application_created_at: '2026-08-02T08:30:00Z',
      released_at: '2026-08-02T08:35:00Z',
      delivery_status: 'cancelled',
      delivery_error: 'Administrator confirmed that the delivery did not launch',
      launch_evidence: { task_id: 8, retry_count: 2 },
      delivery_resolution: {
        action: 'release_for_retry',
        note: 'No exact native turn exists',
        resolved_by: 1,
      },
    }];
    vi.mocked(api.listPlanVersions).mockResolvedValue([current, prior]);

    render(<PlanDetail plan={resource} onRefresh={vi.fn()} />);

    await userEvent.click(await screen.findByText('Debug information'));
    expect(screen.getByText('Delivery history (1)')).toBeInTheDocument();
    expect(screen.getByText(/receipt-released.*release_for_retry/)).toBeInTheDocument();
    expect(screen.getByText('Resolution note: No exact native turn exists')).toBeInTheDocument();
  });

  it('shows a confirmable warning for a migrated Version without blocking decisions', async () => {
    const current = version({ repo_revision: null, reviewer_repo_revision: null });
    const prior = version({ id: 11, version_number: 1 });
    vi.mocked(api.listPlanVersions).mockResolvedValue([current, prior]);
    vi.mocked(api.getPlanVersionStaleness).mockResolvedValueOnce({
      stale: true,
      reasons: ['captured_repository_state_missing'],
      hard_conflict: false,
      hard_conflicts: [],
      can_confirm: true,
      current_log_id: null,
      current_repo_revision: { available: true, head: 'current' },
    });

    render(<PlanDetail plan={plan(current, prior)} onRefresh={vi.fn()} />);

    expect(await screen.findByText(/no historical repository snapshot/)).toBeInTheDocument();
    expect(screen.getByText(/Confirm to continue, or regenerate first/)).toBeInTheDocument();
    expect(screen.getByRole('alert')).toHaveClass('text-gray-200', 'bg-amber-500/15');
    expect(screen.getByText('Context changed')).toHaveClass('text-amber-300');
    expect(screen.queryByText(/This action is blocked/)).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Approve v2 & create execution Task' })).toBeEnabled();
    expect(screen.getByRole('button', { name: 'Reject v2' })).toBeEnabled();
    expect(screen.getByRole('button', { name: 'Refresh contexts and regenerate Plan' }))
      .toBeInTheDocument();
  });

  it('shows live planning feedback and keeps internal Run identifiers inside Debug information', async () => {
    const prior = version({ id: 11, version_number: 1 });
    const current = version({});
    const activeRun = {
      id: 15,
      plan_id: 4,
      run_type: 'initial',
      status: 'running',
      current_stage: 'planner',
      base_version_id: null,
      source_run_id: null,
      result_version_id: null,
      request_text: 'Design the migration',
      round: 1,
      generation: 1,
      instance_id: 2,
      worker_id: null,
      open_input_request_id: null,
      interaction_count: 0,
      max_interactions: 5,
      execution_seconds: 8,
      last_execution_started_at: '2026-08-03T08:00:00Z',
      review_verdict: null,
      review_feedback: null,
      review_exhausted: false,
      error: null,
      created_at: '2026-08-03T08:00:00Z',
      updated_at: '2026-08-03T08:00:08Z',
      finished_at: null,
      steps: [],
      input_requests: [],
    } satisfies PlanRun;
    const resource = plan(current, prior);
    resource.current_version_id = null;
    resource.current_version = null;
    resource.active_run_id = activeRun.id;
    resource.active_run = activeRun;
    resource.display_state = 'planner';
    resource.latest_run_status = 'running';
    vi.mocked(api.listPlanVersions).mockResolvedValue([]);
    vi.mocked(api.listPlanResourceRuns).mockResolvedValue([activeRun]);

    render(<PlanDetail plan={resource} onRefresh={vi.fn()} />);

    expect(await screen.findByRole('status')).toHaveTextContent('Creating v1 draft');
    expect(screen.queryByRole('region', { name: 'Plan activity' })).not.toBeInTheDocument();

    const debug = screen.getByText('Debug information').closest('details');
    expect(debug).not.toHaveAttribute('open');
    expect(within(debug!).getByText(/Run #15 · initial · running · round 1/))
      .toBeInTheDocument();
  });

  it('immediately resumes the draft state and shows submitted input history', async () => {
    const prior = version({ id: 11, version_number: 1 });
    const current = version({});
    const openRequest = {
      id: 41,
      plan_id: 4,
      run_id: 21,
      source_step_id: 30,
      requested_by: 'planner' as const,
      reason: 'Choose the rollout window',
      questions: [{
        id: 'window',
        header: 'Window',
        question: 'Which rollout window?',
        response_type: 'text' as const,
        options: [],
        required: true,
      }],
      status: 'open' as const,
      answers: null,
      response_text: null,
      attachments: null,
      answered_by: null,
      opened_at: '2026-08-03T11:06:50Z',
      answered_at: null,
      created_at: '2026-08-03T11:06:50Z',
    };
    const waitingRun = {
      id: 21,
      plan_id: 4,
      run_type: 'initial',
      status: 'waiting_user',
      current_stage: 'planner',
      base_version_id: null,
      source_run_id: null,
      result_version_id: null,
      request_text: 'Design the migration',
      round: 1,
      generation: 3,
      instance_id: null,
      worker_id: null,
      open_input_request_id: openRequest.id,
      interaction_count: 1,
      max_interactions: 5,
      execution_seconds: 8,
      last_execution_started_at: null,
      review_verdict: null,
      review_feedback: null,
      review_exhausted: false,
      error: null,
      created_at: '2026-08-03T11:06:48Z',
      updated_at: '2026-08-03T11:07:00Z',
      finished_at: null,
      steps: [],
      input_requests: [openRequest],
    } satisfies PlanRun;
    const answeredRequest = {
      ...openRequest,
      status: 'answered' as const,
      answers: [{ question_id: 'window', value: 'Sunday 02:00 UTC' }],
      answered_at: '2026-08-03T11:08:00Z',
    };
    const queuedRun = {
      ...waitingRun,
      status: 'queued' as const,
      generation: 4,
      open_input_request_id: null,
      input_requests: [answeredRequest],
    } satisfies PlanRun;
    const resource = plan(current, prior);
    resource.current_version_id = null;
    resource.current_version = null;
    resource.active_run_id = waitingRun.id;
    resource.active_run = waitingRun;
    resource.open_input_request = openRequest;
    resource.display_state = 'waiting_user';
    resource.latest_run_status = 'waiting_user';
    vi.mocked(api.listPlanVersions).mockResolvedValue([]);
    vi.mocked(api.listPlanResourceRuns)
      .mockResolvedValueOnce([waitingRun])
      .mockResolvedValue([queuedRun]);
    vi.mocked(api.answerPlanInput).mockResolvedValue(answeredRequest);

    render(<PlanDetail plan={resource} onRefresh={vi.fn()} />);
    await userEvent.type((await screen.findAllByRole('textbox'))[0], 'Sunday 02:00 UTC');
    await userEvent.click(screen.getByRole('button', { name: 'Submit answers' }));

    expect(await screen.findByRole('status')).toHaveTextContent('v1 generation queued');
    expect(screen.getByText('v1 input history (1)')).toBeInTheDocument();
    expect(screen.getByText('Sunday 02:00 UTC')).toBeVisible();
  });

  it('hides stale warnings after the selected Version has already been applied', async () => {
    const current = version({ human_decision: 'approved', applied: true, display_state: 'applied' });
    const prior = version({ id: 11, version_number: 1 });
    const resource = plan(current, prior);
    resource.display_state = 'applied';
    vi.mocked(api.listPlanVersions).mockResolvedValue([current, prior]);
    vi.mocked(api.getPlanVersionStaleness).mockResolvedValueOnce({
      stale: true,
      reasons: ['conversation_advanced'],
      hard_conflict: false,
      hard_conflicts: [],
      can_confirm: true,
      current_log_id: 99,
      current_repo_revision: null,
    });

    render(<PlanDetail plan={resource} onRefresh={vi.fn()} />);

    await screen.findByRole('option', { name: 'v2 · Applied · Current' });
    expect(screen.queryByText('Context changed')).not.toBeInTheDocument();
    expect(screen.queryByText('stale context')).not.toBeInTheDocument();
  });

  it('shows in-progress feedback and keeps the modal open after archiving succeeds', async () => {
    const current = version({});
    const prior = version({ id: 11, version_number: 1 });
    const resource = plan(current, prior);
    let finishArchive!: (value: PlanResource) => void;
    vi.mocked(api.listPlanVersions).mockResolvedValue([current, prior]);
    vi.mocked(api.updatePlan).mockReturnValue(new Promise<PlanResource>((resolve) => { finishArchive = resolve; }));
    const onClose = vi.fn();

    render(<PlanDetail plan={resource} onRefresh={vi.fn()} onClose={onClose} />);
    await userEvent.click(await screen.findByRole('button', { name: 'Archive' }));

    expect(screen.getByRole('status')).toHaveTextContent('Archiving Plan…');
    expect(onClose).not.toHaveBeenCalled();
    finishArchive({ ...resource, archived_at: '2026-08-04T12:00:00Z' });
    await waitFor(() => expect(screen.queryByText('Archiving Plan…')).not.toBeInTheDocument());
    expect(onClose).not.toHaveBeenCalled();
  });

  it('opens the Task associated with a related Plan', async () => {
    const current = version({});
    const prior = version({ id: 11, version_number: 1 });
    const resource = plan(current, prior);
    resource.target_task_id = 200;
    vi.mocked(api.listPlanVersions).mockResolvedValue([current, prior]);
    const navigate = vi.fn();
    const onClose = vi.fn();

    render(<PlanDetail plan={resource} onRefresh={vi.fn()} onNavigateTask={navigate} onClose={onClose} />);
    await userEvent.click(await screen.findByRole('button', { name: 'Open related Task #200' }));

    expect(navigate).toHaveBeenCalledWith(200);
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it('keeps failed Run details in Debug and offers an in-place retry', async () => {
    const current = version({});
    const prior = version({ id: 11, version_number: 1 });
    const rawError = 'Claude Plan Agent exited with 1: API Error: 400 tools.3.custom.input_schema';
    const failedRun = {
      id: 16,
      plan_id: 4,
      run_type: 'initial',
      status: 'failed',
      current_stage: 'failed',
      base_version_id: null,
      source_run_id: null,
      result_version_id: null,
      request_text: 'Design the migration',
      round: 1,
      generation: 1,
      instance_id: null,
      worker_id: null,
      open_input_request_id: null,
      interaction_count: 0,
      max_interactions: 5,
      execution_seconds: 2,
      last_execution_started_at: null,
      review_verdict: null,
      review_feedback: null,
      review_exhausted: false,
      error: rawError,
      created_at: '2026-08-03T10:27:22Z',
      updated_at: '2026-08-03T10:27:26Z',
      finished_at: '2026-08-03T10:27:26Z',
      steps: [{
        id: 51,
        step_type: 'reviewer',
        round: 1,
        provider: 'codex',
        model: 'gpt-5.6-sol',
        effort: 'xhigh',
        route_slot: 'primary',
        status: 'failed',
        output: null,
        error: 'Codex stream stalled after 90s without a delta',
        last_delta_at: '2026-08-03T10:25:56Z',
        streamed_output_chars: 381,
        last_event_type: 'item.agent_message.delta',
        started_at: '2026-08-03T10:24:22Z',
        finished_at: '2026-08-03T10:27:26Z',
      }],
      input_requests: [],
    } satisfies PlanRun;
    const resource = plan(current, prior);
    resource.current_version_id = null;
    resource.current_version = null;
    resource.display_state = 'failed';
    resource.latest_run_status = 'failed';
    resource.latest_run_error = rawError;
    vi.mocked(api.listPlanVersions).mockResolvedValue([]);
    vi.mocked(api.listPlanResourceRuns).mockResolvedValue([failedRun]);
    vi.mocked(api.createPlanRun).mockResolvedValue(failedRun);

    render(<PlanDetail plan={resource} onRefresh={vi.fn()} />);

    const alert = await screen.findByRole('alert');
    expect(alert).toHaveTextContent('Latest planning attempt failed');
    expect(alert).not.toHaveTextContent(rawError);
    expect(screen.queryByRole('region', { name: 'Plan activity' })).not.toBeInTheDocument();
    const debug = screen.getByText('Debug information').closest('details');
    expect(within(debug!).getByText(rawError)).toBeInTheDocument();
    expect(within(debug!).getByText(/streamed chars: 381/)).toBeInTheDocument();
    expect(within(debug!).getByText(/last event: item\.agent_message\.delta/))
      .toBeInTheDocument();
    expect(within(debug!).getByText(/Codex stream stalled after 90s/))
      .toBeInTheDocument();

    await userEvent.click(screen.getByRole('button', { name: 'Retry planning' }));
    expect(api.createPlanRun).toHaveBeenCalledWith(4, {
      run_type: 'retry',
      request: 'Design the migration',
      base_version_id: undefined,
      expected_current_version_id: undefined,
      source_run_id: 16,
    });
  });

  it('shows the planner draft read-only while the reviewer is running', async () => {
    const prior = version({ id: 11, version_number: 1 });
    const current = version({ review_verdict: null, reviewed_at: null });
    const reviewerRun = {
      id: 20,
      plan_id: 4,
      run_type: 'user_revision',
      status: 'running',
      current_stage: 'reviewer',
      base_version_id: prior.id,
      source_run_id: null,
      result_version_id: null,
      draft_content: '# Candidate proposal',
      draft_step_id: 31,
      draft_repo_revision: { head: 'candidate-head' },
      request_text: 'Tighten the rollout plan',
      round: 1,
      generation: 2,
      instance_id: 2,
      worker_id: null,
      open_input_request_id: null,
      interaction_count: 1,
      max_interactions: 5,
      execution_seconds: 9,
      last_execution_started_at: '2026-08-03T11:07:57Z',
      review_verdict: null,
      review_feedback: null,
      review_exhausted: false,
      error: null,
      created_at: '2026-08-03T11:06:48Z',
      updated_at: '2026-08-03T11:07:57Z',
      finished_at: null,
      steps: [],
      input_requests: [{
        id: 41,
        plan_id: 4,
        run_id: 20,
        source_step_id: 30,
        requested_by: 'planner',
        reason: 'Choose the rollout window',
        questions: [{
          id: 'window',
          header: 'Window',
          question: 'Which rollout window?',
          response_type: 'text',
          options: [],
          required: true,
        }],
        status: 'answered',
        answers: [{ question_id: 'window', value: 'Sunday 02:00 UTC' }],
        response_text: null,
        attachments: null,
        answered_by: 1,
        opened_at: '2026-08-03T11:06:50Z',
        answered_at: '2026-08-03T11:07:00Z',
        created_at: '2026-08-03T11:06:50Z',
      }],
    } satisfies PlanRun;
    const resource = plan(current, prior);
    resource.active_run_id = reviewerRun.id;
    resource.active_run = reviewerRun;
    resource.display_state = 'reviewer';
    resource.latest_run_status = 'running';
    vi.mocked(api.listPlanVersions).mockResolvedValue([current, prior]);
    vi.mocked(api.listPlanResourceRuns).mockResolvedValue([reviewerRun]);

    render(<PlanDetail plan={resource} onRefresh={vi.fn()} />);

    expect(await screen.findByRole('status')).toHaveTextContent(
      'Reviewing v3 candidate · actions unlock when review finishes',
    );
    expect(await screen.findByRole('heading', { level: 1, name: 'Candidate proposal' }))
      .toBeInTheDocument();
    expect(screen.getByText('v3 candidate · not a Version yet')).toBeInTheDocument();
    expect(screen.getByText('v3 revision & input history (2)')).toBeInTheDocument();
    expect(screen.getByText('Tighten the rollout plan')).toBeVisible();
    expect(screen.getByText('Sunday 02:00 UTC')).toBeVisible();
    expect(screen.queryByPlaceholderText('Revise from v2…')).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Refresh context' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Fork' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /Approve v2/ })).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Cancel planning' })).toBeInTheDocument();
  });
});
