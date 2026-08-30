import { afterEach, describe, expect, it, vi } from 'vitest';
import { cleanup, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { api } from '../../api/client';
import { DeliveryRunDialog } from './DeliveryRunDialog';

vi.mock('../../api/client', async (importOriginal) => { const actual = await importOriginal<typeof import('../../api/client')>(); return { ...actual, api: { ...actual.api, getDeliveryRun: vi.fn(), getDeliveryProgress: vi.fn(), getTask: vi.fn(), getPlan: vi.fn(), getPlanVersion: vi.fn(), listPlanVersions: vi.fn(), listPlanResourceRuns: vi.fn(), getPRMonitorRun: vi.fn(), getTestRun: vi.fn(), getTestRunEvidence: vi.fn() } }; });
vi.mock('../../hooks/useWebSocket', () => ({ useWebSocket: vi.fn() }));
vi.mock('../Tasks/DeliveryRunPanel', () => ({ DeliveryRunPanel: ({ runId, compact }: { runId: number; compact?: boolean }) => <div>{compact ? 'Actions' : 'Full controls'} for {runId}</div> }));
vi.mock('../PlanReview/PlanInputForm', () => ({ PlanInputForm: ({ request }: { request: { questions: Array<{ question: string }> } }) => <div>Inline plan input: {request.questions[0]?.question}</div> }));

describe('DeliveryRunDialog', () => {
  afterEach(() => { cleanup(); vi.clearAllMocks(); });
  it('expands authoritative Plan and Task links without duplicating their data', async () => {
    vi.mocked(api.getDeliveryRun).mockResolvedValue({ id: 7, title: 'Ship', requirements: 'Ship the actual Delivery request', phase: 'coding', activity: 'waiting', outcome: null, terminal: 'ready_to_merge', developer_task_id: 12, pr_monitor_run_id: null, cycles: [{ id: 1, cycle_number: 1, plan_version_id: 31 }], turns: [{ id: 2, generation: 1, status: 'completed', attempts: 1, last_error: null }], delivery_branch: 'ccm/delivery/7', turn_count: 1, head_sha: 'a'.repeat(40), wait_reason: null } as never);
    vi.mocked(api.getDeliveryProgress).mockResolvedValue({
      run_id: 7,
      state_version: 3,
      phase: 'coding',
      activity: 'waiting',
      headline: 'Waiting for the developer',
      detail: null,
      attention_required: false,
      attention_kind: null,
      last_activity_at: null,
      stages: [
        { key: 'planning', label: 'Plan', state: 'completed', summary: '', started_at: null, completed_at: null },
        { key: 'coding', label: 'Development', state: 'waiting', summary: '', started_at: null, completed_at: null },
        { key: 'pre_review', label: 'Code review', state: 'pending', summary: '', started_at: null, completed_at: null },
        { key: 'frontend_review', label: 'Frontend review', state: 'pending', summary: '', started_at: null, completed_at: null },
        { key: 'publishing', label: 'Publish PR', state: 'pending', summary: '', started_at: null, completed_at: null },
        { key: 'monitoring', label: 'CI & PR review', state: 'pending', summary: '', started_at: null, completed_at: null },
      ],
      active_agent: { role: 'planner', provider: 'claude', model: 'claude-opus-4-6', effort: 'high', service_tier: null, status: 'running', activity_kind: 'planning', headline: 'Planner is reviewing the repository', detail: 'Reading the fixed revision and preparing the first draft.', started_at: null, first_output_at: null, last_activity_at: '2026-08-17T03:00:00Z', output_chars: 128 },
      events: [],
      plan_input: null,
      frontend_review: { policy: 'auto', run_id: null, status: null, stage: null, verdict: null, report: null, error: null, cleanup_status: null, evidence_archive_state: null, finding_count: 0, evidence_count: 0, skip_reason: null },
    });
    vi.mocked(api.getTask).mockResolvedValue({ id: 12 } as never);
    vi.mocked(api.getPlanVersion).mockResolvedValue({ id: 31, plan_id: 21, version_number: 2, content: '# Real Plan' } as never);
    vi.mocked(api.getPlan).mockResolvedValue({ id: 21, title: 'Delivery Plan', initial_request: 'Ship the change', display_state: 'approved', current_version_id: 31, active_run_id: null, current_version: { id: 31, plan_id: 21, version_number: 2, content: '# Real Plan', display_state: 'approved', human_decision: 'approved', applied: false }, active_run: null, read_only: true, ownership: 'capability', applications: [], application_attempts: [], latest_run_status: 'completed', latest_run_error: null, pipeline_config: { planner: { primary: {}, fallback: {} }, reviewer: { enabled: true, primary: {}, fallback: {} } } } as never);
    vi.mocked(api.listPlanVersions).mockResolvedValue([{ id: 31, plan_id: 21, version_number: 2, content: '# Real Plan', display_state: 'approved', human_decision: 'approved', applied: false }] as never);
    vi.mocked(api.listPlanResourceRuns).mockResolvedValue([]);
    const onOpenTask = vi.fn(); const onOpenPlan = vi.fn();
    render(<DeliveryRunDialog runId={7} onClose={() => {}} onOpenTask={onOpenTask} onOpenPlan={onOpenPlan} onOpenPRMonitor={() => {}} />);
    expect(await screen.findByText('Actions for 7')).toBeInTheDocument();
    expect(screen.queryByRole('region', { name: 'Delivery rounds' })).not.toBeInTheDocument();
    expect(screen.queryByText('Full controls for 7')).not.toBeInTheDocument();
    await userEvent.click(await screen.findByRole('button', { name: /Plan/ }));
    expect(await screen.findByText('Real Plan')).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: /Open Plan conversation/ }));
    expect(await screen.findByRole('region', { name: 'Plan conversation' })).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: 'Delivery #7' })).toBeInTheDocument();
    expect(screen.getByText('Ship the actual Delivery request')).toBeInTheDocument();
    expect(screen.queryByText('Ship the change')).not.toBeInTheDocument();
    expect(onOpenPlan).not.toHaveBeenCalled();
    await userEvent.click(screen.getByRole('button', { name: 'Back to Delivery #7' }));
    await userEvent.click(screen.getByRole('button', { name: /Development/ }));
    await userEvent.click(screen.getByRole('button', { name: /Open real Task Chat/ }));
    expect(onOpenTask).toHaveBeenCalledWith(12);
  });

  it('surfaces a Plan choice inline and selects the Plan tab automatically', async () => {
    vi.mocked(api.getDeliveryRun).mockResolvedValue({ id: 8, title: 'Choose scope', phase: 'planning', activity: 'waiting', outcome: null, terminal: 'ready_to_merge', developer_task_id: null, pr_monitor_run_id: null, cycles: [{ id: 2, cycle_number: 1, plan_version_id: null }], turns: [], transitions: [], delivery_branch: 'ccm/delivery/8', turn_count: 0, head_sha: null, wait_reason: 'plan_capability' } as never);
    vi.mocked(api.getDeliveryProgress).mockResolvedValue({
      run_id: 8, state_version: 4, phase: 'planning', activity: 'waiting', headline: 'Plan needs your decision', detail: 'Scope changes the implementation.', attention_required: true, attention_kind: 'plan_input', last_activity_at: null,
      stages: [
        { key: 'planning', label: 'Plan', state: 'waiting', summary: '', started_at: null, completed_at: null },
        { key: 'coding', label: 'Development', state: 'pending', summary: '', started_at: null, completed_at: null },
        { key: 'pre_review', label: 'Code review', state: 'pending', summary: '', started_at: null, completed_at: null },
        { key: 'frontend_review', label: 'Frontend review', state: 'pending', summary: '', started_at: null, completed_at: null },
        { key: 'publishing', label: 'Publish PR', state: 'pending', summary: '', started_at: null, completed_at: null },
        { key: 'monitoring', label: 'CI & PR review', state: 'pending', summary: '', started_at: null, completed_at: null },
      ],
      active_agent: null, events: [], plan_id: 44,
      plan_input: {
        plan_id: 44,
        run: { id: 55, generation: 2 },
        request: { id: 66, requested_by: 'planner', reason: 'Scope changes the implementation.', questions: [{ id: 'scope', header: 'Scope', question: 'Which rollout scope?', response_type: 'text', options: [], required: true }] },
      },
      frontend_review: { policy: 'auto', run_id: null, status: null, stage: null, verdict: null, report: null, error: null, cleanup_status: null, evidence_archive_state: null, finding_count: 0, evidence_count: 0, skip_reason: null },
    } as never);

    render(<DeliveryRunDialog runId={8} onClose={() => {}} onOpenTask={() => {}} onOpenPlan={() => {}} onOpenPRMonitor={() => {}} />);

    expect(await screen.findByText('The Loop needs your choice')).toBeInTheDocument();
    expect(screen.getByText('Inline plan input: Which rollout scope?')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /^Plan: / })).toHaveAttribute('aria-pressed', 'true');
  });

  it('links the Plan aggregate while its first Version is still being prepared', async () => {
    vi.mocked(api.getDeliveryRun).mockResolvedValue({ id: 18, title: 'Prepare rollout', phase: 'planning', activity: 'waiting', outcome: null, terminal: 'ready_to_merge', developer_task_id: 1005, pr_monitor_run_id: null, current_cycle_id: 12, cycles: [{ id: 12, cycle_number: 1, plan_version_id: null }], turns: [], transitions: [], delivery_branch: 'ccm/delivery/18', turn_count: 0, head_sha: null, wait_reason: 'plan_capability' } as never);
    vi.mocked(api.getDeliveryProgress).mockResolvedValue({
      run_id: 18, state_version: 2, phase: 'planning', activity: 'waiting', headline: 'Planning started', detail: null, attention_required: false, attention_kind: null, last_activity_at: null, plan_id: 2,
      stages: [
        { key: 'planning', label: 'Plan', state: 'running', summary: '', started_at: null, completed_at: null },
        { key: 'coding', label: 'Development', state: 'pending', summary: '', started_at: null, completed_at: null },
        { key: 'pre_review', label: 'Code review', state: 'pending', summary: '', started_at: null, completed_at: null },
        { key: 'frontend_review', label: 'Frontend review', state: 'pending', summary: '', started_at: null, completed_at: null },
        { key: 'publishing', label: 'Publish PR', state: 'pending', summary: '', started_at: null, completed_at: null },
        { key: 'monitoring', label: 'CI & PR review', state: 'pending', summary: '', started_at: null, completed_at: null },
      ], active_agent: null, events: [], plan_input: null,
      frontend_review: { policy: 'auto', run_id: null, status: null, stage: null, verdict: null, report: null, error: null, cleanup_status: null, evidence_archive_state: null, finding_count: 0, evidence_count: 0, skip_reason: null },
    } as never);
    vi.mocked(api.getTask).mockResolvedValue({ id: 1005 } as never);
    vi.mocked(api.getPlan).mockResolvedValue({ id: 2, title: 'Preparing Plan', initial_request: 'Prepare rollout', display_state: 'planner', current_version_id: null, active_run_id: null, current_version: null, active_run: null, read_only: true, ownership: 'capability', applications: [], application_attempts: [], latest_run_status: 'running', latest_run_error: null, pipeline_config: { planner: { primary: {}, fallback: {} }, reviewer: { enabled: true, primary: {}, fallback: {} } } } as never);
    vi.mocked(api.listPlanVersions).mockResolvedValue([]);
    vi.mocked(api.listPlanResourceRuns).mockResolvedValue([]);
    const onOpenPlan = vi.fn();

    render(<DeliveryRunDialog runId={18} onClose={() => {}} onOpenTask={() => {}} onOpenPlan={onOpenPlan} onOpenPRMonitor={() => {}} />);

    expect(await screen.findByText('Plan #2 is being prepared')).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Open Plan #2' }));
    expect(await screen.findByRole('region', { name: 'Plan conversation' })).toBeInTheDocument();
    expect(onOpenPlan).not.toHaveBeenCalled();
  });

  it('fails closed when a Plan choice has an unknown requester', async () => {
    vi.mocked(api.getDeliveryRun).mockResolvedValue({ id: 8, title: 'Choose scope', phase: 'planning', activity: 'waiting', outcome: null, terminal: 'ready_to_merge', developer_task_id: null, pr_monitor_run_id: null, cycles: [{ id: 2, cycle_number: 1, plan_version_id: null }], turns: [], transitions: [], delivery_branch: 'ccm/delivery/8', turn_count: 0, head_sha: null, wait_reason: 'plan_capability' } as never);
    vi.mocked(api.getDeliveryProgress).mockResolvedValue({
      run_id: 8, state_version: 4, phase: 'planning', activity: 'waiting', headline: 'Plan needs your decision', detail: null, attention_required: true, attention_kind: 'plan_input', last_activity_at: null,
      stages: [
        { key: 'planning', label: 'Plan', state: 'waiting', summary: '', started_at: null, completed_at: null },
        { key: 'coding', label: 'Development', state: 'pending', summary: '', started_at: null, completed_at: null },
        { key: 'pre_review', label: 'Code review', state: 'pending', summary: '', started_at: null, completed_at: null },
        { key: 'frontend_review', label: 'Frontend review', state: 'pending', summary: '', started_at: null, completed_at: null },
        { key: 'publishing', label: 'Publish PR', state: 'pending', summary: '', started_at: null, completed_at: null },
        { key: 'monitoring', label: 'CI & PR review', state: 'pending', summary: '', started_at: null, completed_at: null },
      ],
      active_agent: null, events: [],
      plan_input: {
        plan_id: 44,
        run: { id: 55, generation: 2 },
        request: { id: 66, requested_by: 'operator', reason: null, questions: [{ id: 'scope', header: 'Scope', question: 'Which rollout scope?', response_type: 'text', options: [], required: true }] },
      },
      frontend_review: { policy: 'auto', run_id: null, status: null, stage: null, verdict: null, report: null, error: null, cleanup_status: null, evidence_archive_state: null, finding_count: 0, evidence_count: 0, skip_reason: null },
    } as never);

    render(<DeliveryRunDialog runId={8} onClose={() => {}} onOpenTask={() => {}} onOpenPlan={() => {}} onOpenPRMonitor={() => {}} />);

    expect(await screen.findByText(/invalid Plan input requester/)).toBeInTheDocument();
    expect(screen.queryByText(/Inline plan input:/)).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Open Plan conversation' })).toBeInTheDocument();
  });

  it('shows Browser Agent findings and archived report in the frontend stage', async () => {
    vi.mocked(api.getDeliveryRun).mockResolvedValue({ id: 9, title: 'Review UI', phase: 'frontend_review', activity: 'waiting', outcome: null, terminal: 'ready_to_merge', developer_task_id: 14, pr_monitor_run_id: null, cycles: [{ id: 3, cycle_number: 1, plan_version_id: null, frontend_review_run_id: 'f'.repeat(32) }], turns: [], transitions: [], delivery_branch: 'ccm/delivery/9', turn_count: 1, head_sha: 'b'.repeat(40), wait_reason: 'frontend_review' } as never);
    vi.mocked(api.getDeliveryProgress).mockResolvedValue({
      run_id: 9, state_version: 8, phase: 'frontend_review', activity: 'waiting', headline: 'Browser reviewer is validating the user-visible flow', detail: 'Reviewing save flow', attention_required: false, attention_kind: null, last_activity_at: null,
      stages: [
        { key: 'planning', label: 'Plan', state: 'completed', summary: '', started_at: null, completed_at: null },
        { key: 'coding', label: 'Development', state: 'completed', summary: '', started_at: null, completed_at: null },
        { key: 'pre_review', label: 'Code review', state: 'completed', summary: '', started_at: null, completed_at: null },
        { key: 'frontend_review', label: 'Frontend review', state: 'waiting', summary: '', started_at: null, completed_at: null },
        { key: 'publishing', label: 'Publish PR', state: 'pending', summary: '', started_at: null, completed_at: null },
        { key: 'monitoring', label: 'CI & PR review', state: 'pending', summary: '', started_at: null, completed_at: null },
      ],
      active_agent: null, events: [], plan_input: null,
      frontend_review: { policy: 'required', run_id: 'f'.repeat(32), status: 'completed', stage: 'completed', verdict: 'failed', report: 'The Save flow failed.', error: null, cleanup_status: 'completed', evidence_archive_state: 'complete', finding_count: 1, evidence_count: 2, skip_reason: null },
    } as never);
    vi.mocked(api.getTask).mockResolvedValue({ id: 14 } as never);
    vi.mocked(api.getTestRun).mockResolvedValue({ id: 'f'.repeat(32), stage: 'completed', cleanup_status: 'completed', findings: [{ id: 'finding-1', severity: 'high', title: 'Save action does not complete', actual: 'The page reports an error' }], report: 'The Save flow failed.', evidence: [{ id: 'evidence-1', name: 'save-flow.png', kind: 'screenshot' }] } as never);

    render(<DeliveryRunDialog runId={9} onClose={() => {}} onOpenTask={() => {}} onOpenPlan={() => {}} onOpenPRMonitor={() => {}} />);

    expect(await screen.findByText(/Save action does not complete/)).toBeInTheDocument();
    expect(screen.getByText('The Save flow failed.')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /save-flow\.png/ })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Frontend review/ })).toHaveAttribute('aria-pressed', 'true');
  });

  it('shows the live Browser Agent stage, latest action, counters, and Task panel entry', async () => {
    const harnessRunId = 'e'.repeat(32);
    vi.mocked(api.getDeliveryRun).mockResolvedValue({
      id: 11,
      title: 'Watch UI review',
      phase: 'frontend_review',
      activity: 'waiting',
      outcome: null,
      terminal: 'ready_to_merge',
      developer_task_id: 14,
      pr_monitor_run_id: null,
      cycles: [{ id: 4, cycle_number: 1, plan_version_id: null, frontend_review_run_id: harnessRunId }],
      turns: [],
      transitions: [],
      delivery_branch: 'ccm/delivery/11',
      turn_count: 1,
      head_sha: 'c'.repeat(40),
      wait_reason: 'frontend_review',
    } as never);
    vi.mocked(api.getDeliveryProgress).mockResolvedValue({
      run_id: 11,
      state_version: 9,
      phase: 'frontend_review',
      activity: 'waiting',
      headline: 'Browser reviewer is exercising the save flow',
      detail: 'The latest action is visible below.',
      attention_required: false,
      attention_kind: null,
      last_activity_at: '2026-08-13T00:00:03Z',
      stages: [
        { key: 'planning', label: 'Plan', state: 'completed', summary: '', started_at: null, completed_at: null },
        { key: 'coding', label: 'Development', state: 'completed', summary: '', started_at: null, completed_at: null },
        { key: 'pre_review', label: 'Code review', state: 'completed', summary: '', started_at: null, completed_at: null },
        { key: 'frontend_review', label: 'Frontend review', state: 'waiting', summary: '', started_at: null, completed_at: null },
        { key: 'publishing', label: 'Publish PR', state: 'pending', summary: '', started_at: null, completed_at: null },
        { key: 'monitoring', label: 'CI & PR review', state: 'pending', summary: '', started_at: null, completed_at: null },
      ],
      active_agent: null,
      events: [],
      plan_input: null,
      frontend_review: {
        policy: 'required',
        run_id: harnessRunId,
        status: 'running',
        stage: 'executing_actions',
        verdict: null,
        report: null,
        error: null,
        cleanup_status: 'pending',
        evidence_archive_state: 'staging',
        finding_count: 0,
        evidence_count: 1,
        skip_reason: null,
      },
    } as never);
    vi.mocked(api.getTask).mockResolvedValue({ id: 14 } as never);
    vi.mocked(api.getTestRun).mockResolvedValue({
      id: harnessRunId,
      status: 'running',
      stage: 'executing_actions',
      cleanup_status: 'pending',
      events: [
        { id: 1, sequence: 1, event_type: 'browser_ready', stage: 'browser_ready', title: '页面已打开', detail: null, data: {}, created_at: '2026-08-13T00:00:01Z' },
        { id: 2, sequence: 2, event_type: 'tool', stage: 'executing_actions', title: '点击保存按钮', detail: '正在验证保存后的成功状态', data: {}, created_at: '2026-08-13T00:00:03Z' },
      ],
      findings: [],
      evidence: [],
      report: null,
      browser_review: {
        status: 'running',
        stage: 'executing_actions',
        steps: 3,
        max_steps: 12,
        actions: 2,
        latest_screenshot: null,
      },
    } as never);
    const onOpenTask = vi.fn();

    render(<DeliveryRunDialog runId={11} onClose={() => {}} onOpenTask={onOpenTask} onOpenPlan={() => {}} onOpenPRMonitor={() => {}} />);

    const live = await screen.findByTestId('delivery-frontend-agent-live');
    expect(live).toHaveTextContent('前端 Browser Agent 正在执行');
    expect(live).toHaveTextContent('点击保存按钮');
    expect(live).toHaveTextContent('步骤 3/12');
    expect(screen.getByTestId('delivery-frontend-live-jump')).toHaveTextContent('Frontend Browser Agent');
    expect(screen.getByTestId('delivery-frontend-live-jump')).toHaveTextContent('Live');
    expect(screen.getByTestId('delivery-frontend-live-jump')).toHaveTextContent('步骤 3/12');
    expect(screen.getByRole('button', { name: /Frontend review/ })).toHaveAttribute('aria-pressed', 'true');
    expect(screen.getByTestId('delivery-frontend-agent-events')).toHaveTextContent('正在验证保存后的成功状态');

    await userEvent.click(screen.getByRole('button', { name: '打开完整实时测试面板' }));
    expect(onOpenTask).toHaveBeenCalledWith(14);
  });

  it('makes the current round and every repair reason explicit and browsable', async () => {
    vi.mocked(api.getDeliveryRun).mockResolvedValue({
      id: 10,
      title: 'Repair PR findings',
      phase: 'planning',
      activity: 'waiting',
      outcome: null,
      terminal: 'ready_to_merge',
      current_cycle_id: 103,
      cycle_count: 3,
      max_cycles: 10,
      developer_task_id: null,
      pr_monitor_run_id: null,
      cycles: [
        {
          id: 101,
          cycle_number: 1,
          status: 'completed',
          trigger_kind: 'initial_request',
          trigger_payload: {},
          plan_version_id: null,
          created_at: '2026-08-13T00:00:00Z',
          completed_at: '2026-08-13T00:02:00Z',
        },
        {
          id: 102,
          cycle_number: 2,
          status: 'completed',
          trigger_kind: 'pre_review_changes_requested',
          trigger_payload: {
            summary: 'The exact code review found a cleanup regression.',
            findings: [{ title: 'Temporary directory cleanup can fail' }],
          },
          plan_version_id: null,
          created_at: '2026-08-13T00:03:00Z',
          completed_at: '2026-08-13T00:05:30Z',
        },
        {
          id: 103,
          cycle_number: 3,
          status: 'planning',
          trigger_kind: 'pr_monitor_blocked',
          trigger_payload: {
            evidence: {
              findings: [
                { title: 'Runtime bypasses shared config loading' },
                { title: 'Malformed config is silently replaced' },
                { title: 'Scheduler XML references an unknown principal' },
              ],
            },
          },
          plan_version_id: null,
          created_at: '2026-08-13T00:06:00Z',
          completed_at: null,
        },
      ],
      turns: [],
      transitions: [],
      delivery_branch: 'ccm/delivery/10',
      turn_count: 0,
      head_sha: null,
      wait_reason: 'plan_capability',
    } as never);
    vi.mocked(api.getDeliveryProgress).mockResolvedValue({
      run_id: 10,
      state_version: 12,
      phase: 'planning',
      activity: 'waiting',
      headline: 'Plan reviewer is checking the proposal',
      detail: null,
      attention_required: false,
      attention_kind: null,
      last_activity_at: '2026-08-13T00:06:01Z',
      stages: [
        { key: 'planning', label: 'Plan', state: 'waiting', summary: '', started_at: null, completed_at: null },
        { key: 'coding', label: 'Development', state: 'pending', summary: '', started_at: null, completed_at: null },
        { key: 'pre_review', label: 'Code review', state: 'pending', summary: '', started_at: null, completed_at: null },
        { key: 'frontend_review', label: 'Frontend review', state: 'pending', summary: '', started_at: null, completed_at: null },
        { key: 'publishing', label: 'Publish PR', state: 'pending', summary: '', started_at: null, completed_at: null },
        { key: 'monitoring', label: 'CI & PR review', state: 'pending', summary: '', started_at: null, completed_at: null },
      ],
      active_agent: null,
      events: [
        { id: 'transition:20', stage: 'pre_review', kind: 'review_changes_requested', source: 'delivery', title: 'Code review requested changes', detail: null, status: 'ready', created_at: '2026-08-13T00:05:00Z' },
        { id: 'transition:21', stage: 'planning', kind: 'plan_requested', source: 'delivery', title: 'Planning started', detail: 'plan_capability', status: 'waiting', created_at: '2026-08-13T00:06:01Z' },
      ],
      plan_input: null,
      frontend_review: { policy: 'auto', run_id: null, status: null, stage: null, verdict: null, report: null, error: null, cleanup_status: null, evidence_archive_state: null, finding_count: 0, evidence_count: 0, skip_reason: null },
    } as never);

    render(<DeliveryRunDialog runId={10} onClose={() => {}} onOpenTask={() => {}} onOpenPlan={() => {}} onOpenPRMonitor={() => {}} />);

    expect(await screen.findByRole('tablist', { name: 'Delivery round history' })).toBeInTheDocument();
    expect(screen.getByRole('tab', { name: 'View round 3: PR review requested fixes' })).toHaveAttribute('aria-current', 'step');

    await userEvent.click(screen.getByRole('tab', { name: 'View round 2: Code review requested fixes' }));

    expect(screen.queryByRole('heading', { name: 'Activity' })).not.toBeInTheDocument();
    expect(screen.getByRole('tab', { name: 'View round 2: Code review requested fixes' })).toHaveAttribute('aria-selected', 'true');
  });

  it('keeps a successful terminal run compact and moves completed Browser evidence into the stage detail', async () => {
    const harnessRunId = 'a'.repeat(32);
    vi.mocked(api.getDeliveryRun).mockResolvedValue({
      id: 12,
      title: 'Completed delivery',
      phase: 'done',
      activity: 'terminal',
      outcome: 'success',
      terminal: 'ready_to_merge',
      current_cycle_id: 202,
      cycle_count: 2,
      max_cycles: 5,
      developer_task_id: 14,
      pr_monitor_run_id: null,
      cycles: [
        { id: 201, cycle_number: 1, status: 'completed', trigger_kind: 'initial_request', trigger_payload: {}, plan_version_id: null, created_at: '2026-08-13T00:00:00Z', completed_at: '2026-08-13T00:05:00Z' },
        { id: 202, cycle_number: 2, status: 'completed', trigger_kind: 'pr_monitor_blocked', trigger_payload: { evidence: { findings: [{ title: 'Exercise the production interaction' }] } }, plan_version_id: null, frontend_review_run_id: harnessRunId, created_at: '2026-08-13T00:06:00Z', completed_at: '2026-08-13T00:12:00Z' },
      ],
      turns: [
        { id: 1, cycle_id: 201, generation: 1, status: 'completed', attempts: 1, last_error: null },
        { id: 2, cycle_id: 202, generation: 2, status: 'completed', attempts: 1, last_error: null },
      ],
      transitions: [],
      delivery_branch: 'ccm/delivery/12',
      turn_count: 2,
      head_sha: 'd'.repeat(40),
      pr_number: 8,
      pr_url: 'https://github.com/example/repo/pull/8',
      wait_reason: null,
      allowed_actions: [],
    } as never);
    vi.mocked(api.getDeliveryProgress).mockResolvedValue({
      run_id: 12,
      state_version: 22,
      phase: 'done',
      activity: 'terminal',
      headline: 'Delivery completed',
      detail: 'https://github.com/example/repo/pull/8',
      attention_required: false,
      attention_kind: null,
      last_activity_at: '2026-08-13T00:12:00Z',
      stages: [
        { key: 'planning', label: 'Plan', state: 'completed', summary: '', started_at: null, completed_at: null },
        { key: 'coding', label: 'Development', state: 'completed', summary: '', started_at: null, completed_at: null },
        { key: 'pre_review', label: 'Code review', state: 'completed', summary: '', started_at: null, completed_at: null },
        { key: 'frontend_review', label: 'Frontend review', state: 'completed', summary: '', started_at: null, completed_at: null },
        { key: 'publishing', label: 'Publish PR', state: 'completed', summary: '', started_at: null, completed_at: null },
        { key: 'monitoring', label: 'CI & PR review', state: 'completed', summary: '', started_at: null, completed_at: null },
      ],
      active_agent: null,
      events: [],
      plan_input: null,
      frontend_review: { policy: 'required', run_id: harnessRunId, status: 'completed', stage: 'completed', verdict: 'passed', report: 'No defects found.', error: null, cleanup_status: 'completed', evidence_archive_state: 'complete', finding_count: 0, evidence_count: 8, skip_reason: null },
    } as never);
    vi.mocked(api.getTask).mockResolvedValue({ id: 14 } as never);
    vi.mocked(api.getTestRun).mockResolvedValue({
      id: harnessRunId,
      status: 'completed',
      stage: 'completed',
      verdict: 'passed',
      cleanup_status: 'completed',
      events: Array.from({ length: 32 }, (_, index) => ({ id: index + 1, sequence: index + 1, event_type: 'tool', stage: 'executing_actions', title: `Event ${index + 1}`, detail: null, data: {}, created_at: '2026-08-13T00:10:00Z' })),
      findings: [],
      evidence: Array.from({ length: 8 }, (_, index) => ({ id: `evidence-${index}`, name: `evidence-${index}.png`, kind: 'screenshot' })),
      report: 'No defects found.',
      browser_review: { status: 'completed', stage: 'completed', steps: 3, max_steps: 24, actions: 3, latest_screenshot: null },
    } as never);

    render(<DeliveryRunDialog runId={12} onClose={() => {}} onOpenTask={() => {}} onOpenPlan={() => {}} onOpenPRMonitor={() => {}} />);

    const outcome = await screen.findByTestId('delivery-outcome-summary');
    expect(outcome).toHaveTextContent('Ready to merge');
    expect(outcome).toHaveTextContent('2 delivery rounds');
    expect(screen.getByRole('link', { name: /Open PR #8/ })).toHaveAttribute('href', 'https://github.com/example/repo/pull/8');
    expect(screen.getByRole('tablist', { name: 'Delivery round history' })).toBeInTheDocument();
    expect(screen.queryByText('Live')).not.toBeInTheDocument();
    expect(screen.queryByText(/controls for 12/i)).not.toBeInTheDocument();

    expect(screen.queryByTestId('delivery-frontend-live-jump')).not.toBeInTheDocument();

    const frontendTab = screen.getByRole('button', { name: /Frontend review/ });
    await userEvent.click(frontendTab);
    expect(frontendTab).toHaveAttribute('aria-pressed', 'true');
    expect(screen.getByText(`Harness ${harnessRunId.slice(0, 8)}`)).toBeInTheDocument();
    expect(screen.getByText('No defects found.')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /evidence-0\.png/ })).toBeInTheDocument();
  });

  it('labels a successful no-change run as a completed report instead of ready to merge', async () => {
    const sha = 'e'.repeat(40);
    vi.mocked(api.getDeliveryRun).mockResolvedValue({
      id: 19, title: 'Inspect stress tests', phase: 'done', activity: 'terminal', outcome: 'success', terminal: 'ready_to_merge',
      developer_task_id: 1005, pr_monitor_run_id: null, cycles: [{ id: 19, cycle_number: 1, plan_version_id: 41 }],
      turns: [{ id: 19, generation: 1, status: 'completed', attempts: 1, last_error: null }], transitions: [],
      delivery_branch: 'ccm/delivery/19', cycle_count: 1, max_cycles: 10, turn_count: 1, base_sha: sha, head_sha: sha,
      pr_number: null, pr_url: null, wait_reason: null,
    } as never);
    vi.mocked(api.getDeliveryProgress).mockResolvedValue({
      run_id: 19, state_version: 6, phase: 'done', activity: 'terminal', headline: 'Delivery completed', detail: null,
      attention_required: false, attention_kind: null, last_activity_at: '2026-08-15T16:12:00Z',
      stages: [
        { key: 'planning', label: 'Plan', state: 'completed', summary: '', started_at: null, completed_at: null },
        { key: 'coding', label: 'Development', state: 'completed', summary: '', started_at: null, completed_at: null },
        { key: 'pre_review', label: 'Code review', state: 'skipped', summary: '', started_at: null, completed_at: null },
        { key: 'frontend_review', label: 'Frontend review', state: 'skipped', summary: '', started_at: null, completed_at: null },
        { key: 'publishing', label: 'Publish PR', state: 'skipped', summary: '', started_at: null, completed_at: null },
        { key: 'monitoring', label: 'CI & PR review', state: 'skipped', summary: '', started_at: null, completed_at: null },
      ],
      active_agent: null, events: [], plan_input: null,
      frontend_review: { policy: 'off', run_id: null, status: null, stage: null, verdict: null, report: null, error: null, cleanup_status: null, evidence_archive_state: null, finding_count: 0, evidence_count: 0, skip_reason: 'Not applicable to report-only Delivery' },
    } as never);
    vi.mocked(api.getTask).mockResolvedValue({ id: 1005 } as never);
    vi.mocked(api.getPlanVersion).mockResolvedValue({ id: 41, plan_id: 31, version_number: 1, content: '# Read-only plan' } as never);

    render(<DeliveryRunDialog runId={19} onClose={() => {}} onOpenTask={() => {}} onOpenPlan={() => {}} onOpenPRMonitor={() => {}} />);

    const outcome = await screen.findByTestId('delivery-outcome-summary');
    expect(outcome).toHaveTextContent('Report completed');
    expect(outcome).toHaveTextContent('without repository changes or a pull request');
    expect(outcome).toHaveTextContent('code and PR gates not applicable');
    expect(outcome).not.toHaveTextContent('Ready to merge');
    expect(screen.queryByRole('link', { name: /Open PR/ })).not.toBeInTheDocument();
    await waitFor(() => {
      expect(screen.getByRole('button', { name: 'Development: Completed' })).toHaveAttribute('aria-pressed', 'true');
    });
    expect(screen.getByRole('button', { name: 'CI & PR review: Skipped' })).not.toHaveAttribute('aria-pressed', 'true');
  });
});
