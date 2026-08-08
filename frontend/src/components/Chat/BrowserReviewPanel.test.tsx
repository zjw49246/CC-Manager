import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';

import type {
  BrowserReviewJob,
  TestHarnessCapabilities,
  TestHarnessRun,
  TestHarnessRuntimeConfig,
  WorkspaceReviewRun,
} from '../../api/client';
import { BrowserReviewPanel } from './BrowserReviewPanel';

vi.mock('../../api/client', () => ({
  api: {
    listTestRuns: vi.fn(),
    getTestRunEvidence: vi.fn(),
    cancelTestRun: vi.fn(),
    repeatTestRun: vi.fn(),
    startTestRun: vi.fn(),
    getTestHarnessCapabilities: vi.fn(),
    getTestHarnessRuntimeConfig: vi.fn(),
    updateTestHarnessRuntimeConfig: vi.fn(),
  },
}));

import { api } from '../../api/client';

const defaultRuntimeConfig: TestHarnessRuntimeConfig = {
  inherit_task: true,
  provider: 'codex',
  model: 'gpt-5.6-sol',
  reasoning_effort: 'high',
  codex_service_tier: 'priority',
  source: 'task',
  task_runtime: {
    provider: 'codex',
    model: 'gpt-5.6-sol',
    reasoning_effort: 'high',
    codex_service_tier: 'priority',
  },
  default_provider: 'codex',
  providers: ['claude', 'codex'],
  default_models: {
    claude: 'claude-opus-4-6',
    codex: 'gpt-5.6-sol',
  },
  models_by_provider: {
    claude: ['claude-opus-4-6', 'claude-opus-5'],
    codex: ['gpt-5.6-sol', 'gpt-5.6-terra'],
  },
  default_effort: 'medium',
  effort_options: {
    claude: ['low', 'medium', 'high', 'xhigh', 'max'],
    codex: ['low', 'medium', 'high', 'xhigh'],
  },
  model_efforts: {
    claude: { 'claude-opus-5': ['low', 'medium', 'high', 'xhigh', 'max'] },
    codex: {
      'gpt-5.6-sol': ['low', 'medium', 'high', 'xhigh', 'max', 'ultra'],
      'gpt-5.6-terra': ['low', 'medium', 'high', 'xhigh', 'max', 'ultra'],
    },
  },
  codex_service_tiers: ['default', 'priority'],
  codex_model_service_tiers: {
    'gpt-5.6-sol': ['default', 'priority'],
    'gpt-5.6-terra': ['default', 'priority'],
  },
};

const defaultCapabilities: TestHarnessCapabilities = {
  contract_version: 1,
  available: true,
  reason: null,
  provider: 'codex',
  task_provider: 'codex',
  provider_browser_capability: true,
  runtime_configurable: true,
  runtime: defaultRuntimeConfig,
  context_policy: 'isolated_black_box_v1',
  targets: {
    current_workspace: true,
    fixed_url: true,
    pull_request: true,
    git_ref: true,
  },
  target_reasons: {
    pull_request: null,
    git_ref: null,
  },
  sandbox: {
    available: true,
    backend: 'docker',
    reason: null,
    image: 'ccm-test-harness-sandbox:local',
    image_id: `sha256:${'1'.repeat(64)}`,
  },
  preview: {
    available: true,
    reason: null,
    repo_path: '/repo',
    configured: true,
    config: null,
    suggested_config: null,
  },
  supports_repeat: true,
  supports_compare: true,
};

const completedJob: BrowserReviewJob = {
  id: 'inline-review-1',
  task_id: 73,
  owner_task_id: 73,
  harness_run_id: 'harness-review-1',
  inline_tool: true,
  status: 'completed',
  stage: 'completed',
  url: 'http://127.0.0.1:5173',
  network_policy: 'managed_preview',
  goal: '检查 Task 页面布局和运行错误',
  provider: 'claude',
  model: 'claude-opus-4-6',
  reasoning_effort: 'medium',
  codex_service_tier: 'default',
  allow_actions: false,
  capture_only: false,
  browser_channel: 'chrome',
  viewport_width: 1440,
  viewport_height: 900,
  max_steps: 20,
  max_actions: 60,
  created_at: '2026-08-05T00:00:00Z',
  started_at: '2026-08-05T00:00:01Z',
  completed_at: '2026-08-05T00:00:05Z',
  error: null,
  response_id: null,
  steps: 3,
  actions: 2,
  latest_screenshot: null,
  telemetry: { page_errors: [{ message: 'render exploded' }] },
  action_batches: [],
  trace: [
    {
      id: 10,
      kind: 'decision',
      title: '模型观察与决策',
      detail: '首屏存在横向溢出，继续检查错误面板。',
      timestamp: '2026-08-05T00:00:02Z',
    },
    {
      id: 11,
      kind: 'tool',
      title: '滚动查看页面',
      detail: '{"delta_y": 600}',
      tool_name: 'browser_scroll',
      timestamp: '2026-08-05T00:00:03Z',
    },
  ],
  verdict: 'failed',
  findings: [],
  coverage: { scenarios: ['primary-flow'] },
  artifacts: ['report.md'],
  report: '# 审查结论\n\n发现一个布局问题。',
};

function makeWorkspaceRun(overrides: Partial<WorkspaceReviewRun> = {}): WorkspaceReviewRun {
  return {
    id: 'workspace-run-default',
    task_id: 73,
    project_id: 4,
    agent_task_id: null,
    browser_review_job_id: null,
    mode: 'review_only',
    profile: 'standard',
    goal: '验证当前分支页面',
    status: 'completed',
    stage: 'completed',
    workspace_path: '/repo',
    git_head: '1234567890abcdef1234567890abcdef12345678',
    workspace_fingerprint: 'abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890',
    preview_config: {
      version: 1,
      name: 'Vite preview',
      setup: [],
      processes: [],
      url: 'http://127.0.0.1:{preview_port}/',
      health_url: 'http://127.0.0.1:{preview_port}/',
      startup_timeout_seconds: 90,
    },
    preview_url: null,
    stale: false,
    report: '# 完成',
    error: null,
    cleanup_status: 'completed',
    cleanup_error: null,
    created_at: '2026-08-06T00:00:00Z',
    started_at: '2026-08-06T00:00:01Z',
    completed_at: '2026-08-06T00:00:05Z',
    ...overrides,
  };
}

function makeHarnessRun(overrides: Partial<TestHarnessRun> = {}): TestHarnessRun {
  const workspace = overrides.workspace_review ?? null;
  const browser = overrides.browser_review ?? null;
  const objective = String(overrides.test_plan?.objective || workspace?.goal || browser?.goal || '验证前端页面');
  return {
    id: 'harness-run-default',
    task_id: 73,
    project_id: workspace?.project_id ?? null,
    workspace_review_run_id: workspace?.id ?? null,
    browser_review_job_id: browser?.id ?? null,
    agent_task_id: workspace?.agent_task_id ?? browser?.task_id ?? null,
    target_kind: workspace ? 'current_workspace' : 'fixed_url',
    target: workspace ? {} : { url: browser?.url || 'http://127.0.0.1:5173' },
    resolved_target: null,
    test_plan: { version: 1, objective, scenarios: [] },
    runtime: { context_policy: 'isolated_black_box_v1' },
    request_fingerprint: 'f'.repeat(64),
    parent_run_id: null,
    root_run_id: 'harness-run-default',
    attempt_number: 1,
    status: browser?.status ?? (workspace?.status === 'preparing' ? 'preparing_environment' : workspace?.status ?? 'completed'),
    stage: browser?.stage ?? workspace?.stage ?? 'completed',
    verdict: browser?.verdict ?? null,
    source_git_head: workspace?.git_head ?? null,
    source_fingerprint: workspace?.workspace_fingerprint ?? null,
    stale: workspace?.stale ?? false,
    report: browser?.report ?? workspace?.report ?? null,
    error: browser?.error ?? workspace?.error ?? null,
    cleanup_status: workspace?.cleanup_status ?? (browser && browser.status === 'completed' ? 'completed' : 'pending'),
    cleanup_error: workspace?.cleanup_error ?? null,
    evidence_archive_state: (browser?.status === 'completed' || workspace?.status === 'completed') ? 'complete' : 'staging',
    evidence_archive_error: null,
    created_at: workspace?.created_at ?? browser?.created_at ?? '2026-08-06T00:00:00Z',
    started_at: workspace?.started_at ?? browser?.started_at ?? null,
    completed_at: workspace?.completed_at ?? browser?.completed_at ?? null,
    attempts: [],
    events: (browser?.trace || []).map((event, index) => ({
      id: event.id,
      sequence: index + 1,
      event_type: event.kind,
      stage: browser?.stage ?? null,
      title: event.title,
      detail: event.detail,
      data: { tool_name: event.tool_name },
      created_at: event.timestamp || '2026-08-06T00:00:00Z',
    })),
    evidence: (browser?.artifacts || []).map((name, index) => ({
      id: `evidence-${index}`,
      kind: name.endsWith('.png') ? 'screenshot' : 'report',
      name,
      content_type: name.endsWith('.png') ? 'image/png' : 'text/markdown',
      sha256: 'a'.repeat(64),
      byte_size: 10,
      metadata: {},
      created_at: '2026-08-06T00:00:00Z',
    })),
    findings: browser?.findings || [],
    workspace_review: workspace,
    browser_review: browser,
    ...overrides,
  };
}

beforeEach(() => {
  vi.mocked(api.listTestRuns).mockResolvedValue([]);
  vi.mocked(api.getTestHarnessCapabilities).mockResolvedValue(defaultCapabilities);
  vi.mocked(api.getTestHarnessRuntimeConfig).mockResolvedValue(defaultRuntimeConfig);
  vi.mocked(api.updateTestHarnessRuntimeConfig).mockImplementation(async (_taskId, update) => ({
    ...defaultRuntimeConfig,
    inherit_task: update.inherit_task,
    source: update.inherit_task ? 'task' : 'browser_review_config',
    provider: update.provider || defaultRuntimeConfig.task_runtime.provider,
    model: update.model || defaultRuntimeConfig.task_runtime.model,
    reasoning_effort: update.reasoning_effort || defaultRuntimeConfig.task_runtime.reasoning_effort,
    codex_service_tier: update.codex_service_tier || defaultRuntimeConfig.task_runtime.codex_service_tier,
  }));
});

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
  vi.mocked(api.listTestRuns).mockResolvedValue([]);
  localStorage.clear();
  delete document.documentElement.dataset.theme;
});

describe('BrowserReviewPanel', () => {
  it('shows durable workspace preparation, fingerprint, and cancellation before the browser job exists', async () => {
    const workspaceRun: WorkspaceReviewRun = {
      id: 'workspace-run-1',
      task_id: 73,
      project_id: 4,
      agent_task_id: null,
      browser_review_job_id: null,
      mode: 'review_only',
      profile: 'standard',
      goal: '验证当前分支的设置页',
      status: 'preparing',
      stage: 'starting_preview',
      workspace_path: '/repo',
      git_head: '1234567890abcdef1234567890abcdef12345678',
      workspace_fingerprint: 'abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890',
      preview_config: {
        version: 1,
        name: 'Vite preview',
        setup: [],
        processes: [],
        url: 'http://127.0.0.1:{preview_port}/',
        health_url: 'http://127.0.0.1:{preview_port}/',
        startup_timeout_seconds: 90,
      },
      preview_url: null,
      stale: false,
      report: null,
      error: null,
      cleanup_status: 'pending',
      cleanup_error: null,
      created_at: '2026-08-06T00:00:00Z',
      started_at: '2026-08-06T00:00:01Z',
      completed_at: null,
    };
    const harnessRun = makeHarnessRun({
      id: 'harness-workspace-1',
      root_run_id: 'harness-workspace-1',
      status: 'preparing_environment',
      stage: 'starting_preview',
      workspace_review: workspaceRun,
    });
    const cancelled = makeHarnessRun({
      ...harnessRun,
      status: 'cancelled',
      stage: 'cancelled',
      cleanup_status: 'completed',
      workspace_review: { ...workspaceRun, status: 'cancelled', stage: 'cancelled', cleanup_status: 'completed' },
    });
    vi.mocked(api.listTestRuns)
      .mockResolvedValueOnce([harnessRun])
      .mockResolvedValue([cancelled]);
    vi.mocked(api.cancelTestRun).mockResolvedValue(cancelled);

    render(
      <BrowserReviewPanel
        taskId={73}
        taskActive={false}
        open
        displayMode="docked"
        onAvailableChange={vi.fn()}
        onClose={vi.fn()}
        onDisplayModeChange={vi.fn()}
        onNewReview={vi.fn()}
      />,
    );

    expect(await screen.findByText('Test Harness · 当前工作区')).toBeInTheDocument();
    expect(screen.getAllByText('验证当前分支的设置页')).toHaveLength(1);
    expect(screen.getByText(/工作区指纹 abcdef1234/)).toBeInTheDocument();
    expect(screen.getAllByText('正在启动隔离预览').length).toBeGreaterThan(0);
    expect(screen.getAllByText(/HEAD 1234567890/).length).toBeGreaterThan(0);
    fireEvent.click(screen.getByRole('button', { name: 'Stop test run' }));
    await waitFor(() => expect(api.cancelTestRun).toHaveBeenCalledWith(73, harnessRun.id));
  });

  it('shows same-Task progress, trace, telemetry, and report', async () => {
    vi.mocked(api.listTestRuns).mockResolvedValue([
      makeHarnessRun({ id: 'harness-review-1', root_run_id: 'harness-review-1', browser_review: completedJob }),
    ]);
    const onAvailableChange = vi.fn();

    render(
      <BrowserReviewPanel
        taskId={73}
        taskActive={false}
        open
        displayMode="docked"
        onAvailableChange={onAvailableChange}
        onClose={vi.fn()}
        onDisplayModeChange={vi.fn()}
        onNewReview={vi.fn()}
        goalProgress={{ turn: 1, maxTurns: 5, lastReason: '还需要复查窄屏页面', active: true }}
      />,
    );

    expect(await screen.findByText('前端运行审查')).toBeInTheDocument();
    expect(screen.getByText('模型观察与操作轨迹')).toBeInTheDocument();
    expect(screen.getByText(/首屏存在横向溢出/)).toBeInTheDocument();
    expect(screen.getByText('page errors: 1')).toBeInTheDocument();
    expect(screen.getByText('审查结论')).toBeInTheDocument();
    expect(screen.getByText('截图与报告已完成哈希校验和持久化归档')).toBeInTheDocument();
    expect(screen.getByText('Goal 循环审查 · 模型自动判断')).toBeInTheDocument();
    expect(screen.getByText(/还需要复查窄屏页面/)).toBeInTheDocument();
    const harnessSummary = screen.getByTestId('test-harness-progress');
    const goalSummary = screen.getByTestId('frontend-review-goal-progress');
    expect(harnessSummary.compareDocumentPosition(goalSummary) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    await waitFor(() => expect(onAvailableChange).toHaveBeenCalledWith(true));
  });

  it('shows a persistent evidence archive failure instead of a false success', async () => {
    vi.mocked(api.listTestRuns).mockResolvedValue([
      makeHarnessRun({
        id: 'harness-archive-failed',
        root_run_id: 'harness-archive-failed',
        status: 'failed',
        stage: 'evidence_incomplete',
        evidence_archive_state: 'retryable_error',
        evidence_archive_error: 'Expected evidence final.png is missing',
        browser_review: completedJob,
      }),
    ]);

    render(
      <BrowserReviewPanel
        taskId={73}
        taskActive={false}
        open
        displayMode="docked"
        onAvailableChange={vi.fn()}
        onClose={vi.fn()}
        onDisplayModeChange={vi.fn()}
        onNewReview={vi.fn()}
      />,
    );

    const state = await screen.findByTestId('evidence-archive-state');
    expect(state).toHaveTextContent(
      '证据归档未完成：Expected evidence final.png is missing',
    );
    expect(state).toHaveClass('text-red-600');
  });

  it('shows a standby page before the task has started a test', async () => {
    vi.mocked(api.listTestRuns).mockResolvedValue([]);
    const onAvailableChange = vi.fn();

    render(
      <BrowserReviewPanel
        taskId={73}
        taskActive={false}
        open
        displayMode="docked"
        onAvailableChange={onAvailableChange}
        onClose={vi.fn()}
        onDisplayModeChange={vi.fn()}
        onNewReview={vi.fn()}
      />,
    );

    await waitFor(() => expect(onAvailableChange).toHaveBeenCalledWith(false));
    expect(await screen.findByTestId('frontend-test-idle')).toBeInTheDocument();
    expect(screen.getByText('尚未启动前端测试')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: '配置并启动测试' }));
    expect(screen.getByTestId('frontend-test-settings')).toBeInTheDocument();
    expect(screen.getByLabelText('待检测网站')).toBeInTheDocument();
  });

  it('starts a fixed URL Harness run from the right-panel configuration', async () => {
    vi.mocked(api.listTestRuns).mockResolvedValue([]);
    const startedRun = makeHarnessRun({
      id: 'harness-configured-url',
      root_run_id: 'harness-configured-url',
      status: 'queued',
      stage: 'waiting_for_browser',
      target_kind: 'fixed_url',
      target: { url: 'http://127.0.0.1:5173' },
      test_plan: { version: 1, objective: '检查设置保存流程', scenarios: [] },
      browser_review: null,
      report: null,
      verdict: null,
      completed_at: null,
      cleanup_status: 'pending',
    });
    vi.mocked(api.startTestRun).mockResolvedValue(startedRun);

    render(
      <BrowserReviewPanel
        taskId={73}
        taskActive={false}
        taskProvider="codex"
        taskModel="gpt-5.6-sol"
        taskEffort="high"
        taskServiceTier="priority"
        canStartConfiguredReview
        open
        displayMode="docked"
        onAvailableChange={vi.fn()}
        onClose={vi.fn()}
        onDisplayModeChange={vi.fn()}
        onNewReview={vi.fn()}
      />,
    );

    fireEvent.click(await screen.findByRole('button', { name: '配置并启动测试' }));
    expect(await screen.findByText(/Codex · gpt-5.6-sol · effort high · Fast/)).toBeInTheDocument();
    fireEvent.change(screen.getByLabelText('待检测网站'), {
      target: { value: 'http://127.0.0.1:5173' },
    });
    fireEvent.change(screen.getByLabelText('测试目标'), {
      target: { value: '检查设置保存流程' },
    });
    fireEvent.change(screen.getByLabelText('测试深度'), { target: { value: 'exhaustive' } });
    fireEvent.change(screen.getByLabelText('视口'), { target: { value: '390x844' } });
    expect(screen.getByLabelText('浏览器')).toHaveValue('chromium');
    expect(screen.getByRole('option', { name: '系统 Chrome' })).toBeInTheDocument();
    fireEvent.click(screen.getByRole('checkbox', { name: /允许安全的点击和输入/ }));
    fireEvent.click(screen.getByRole('button', { name: '开始网站测试' }));

    await waitFor(() => expect(api.updateTestHarnessRuntimeConfig).toHaveBeenCalledWith(73, {
      inherit_task: true,
    }));
    await waitFor(() => expect(api.startTestRun).toHaveBeenCalledWith(73, {
      target_kind: 'fixed_url',
      target: { url: 'http://127.0.0.1:5173' },
      goal: '检查设置保存流程',
      profile: 'exhaustive',
      allow_actions: true,
      browser_channel: 'chromium',
      viewport_width: 390,
      viewport_height: 844,
      max_steps: 20,
      max_actions: 60,
    }));
    expect(await screen.findByText('Test Harness · 固定 URL')).toBeInTheDocument();
    expect(screen.queryByTestId('frontend-test-settings')).not.toBeInTheDocument();
  });

  it('starts an exact GitHub PR sandbox run and shows its frozen target', async () => {
    const startedRun = makeHarnessRun({
      id: 'harness-pr-99',
      root_run_id: 'harness-pr-99',
      project_id: 4,
      target_kind: 'pull_request',
      target: { remote: 'origin', pr_number: 99 },
      resolved_target: {
        kind: 'pull_request',
        repository: 'zjw49246/CC-Manager',
        base_sha: 'a'.repeat(40),
        head_sha: 'b'.repeat(40),
        pr_number: 99,
        source_ref: 'feature/browser-review',
        changed_files: [
          { path: 'frontend/src/App.tsx', status: 'modified' },
          { path: 'frontend/src/styles.css', status: 'added' },
        ],
      },
      source_git_head: 'b'.repeat(40),
      status: 'preparing_environment',
      stage: 'target_resolved',
      test_plan: { version: 1, objective: '验收 PR #99 前端改动', scenarios: [] },
      browser_review: null,
      report: null,
      verdict: null,
      completed_at: null,
      cleanup_status: 'pending',
    });
    vi.mocked(api.startTestRun).mockResolvedValue(startedRun);

    render(
      <BrowserReviewPanel
        taskId={73}
        taskActive={false}
        canStartConfiguredReview
        open
        displayMode="docked"
        onAvailableChange={vi.fn()}
        onClose={vi.fn()}
        onDisplayModeChange={vi.fn()}
        onNewReview={vi.fn()}
      />,
    );

    fireEvent.click(await screen.findByRole('button', { name: '配置并启动测试' }));
    fireEvent.change(screen.getByLabelText('测试目标类型'), {
      target: { value: 'pull_request' },
    });
    expect(await screen.findByText(/Sandbox 已就绪/)).toBeInTheDocument();
    fireEvent.change(screen.getByLabelText('Pull Request 编号'), {
      target: { value: '99' },
    });
    fireEvent.change(screen.getByLabelText('测试目标'), {
      target: { value: '验收 PR #99 前端改动' },
    });
    fireEvent.click(screen.getByRole('button', { name: '开始 GitHub PR 测试' }));

    await waitFor(() => expect(api.startTestRun).toHaveBeenCalledWith(73, expect.objectContaining({
      target_kind: 'pull_request',
      target: { remote: 'origin', pr_number: 99 },
      goal: '验收 PR #99 前端改动',
    })));
    expect(await screen.findByText('Test Harness · GitHub PR')).toBeInTheDocument();
    expect(screen.getByText('zjw49246/CC-Manager')).toBeInTheDocument();
    expect(screen.getByText('PR #99')).toBeInTheDocument();
    expect(screen.getByText('frontend/src/App.tsx')).toBeInTheDocument();
    expect(screen.getByText('变更文件 2')).toBeInTheDocument();
  });

  it('shows the exact sandbox admission reason before a PR run can start', async () => {
    vi.mocked(api.getTestHarnessCapabilities).mockResolvedValue({
      ...defaultCapabilities,
      targets: {
        ...defaultCapabilities.targets,
        pull_request: false,
        git_ref: false,
      },
      target_reasons: {
        pull_request: 'Docker sandbox image is not installed',
        git_ref: 'Docker sandbox image is not installed',
      },
      sandbox: {
        ...defaultCapabilities.sandbox,
        available: false,
        reason: 'Docker sandbox image is not installed',
      },
    });

    render(
      <BrowserReviewPanel
        taskId={73}
        taskActive={false}
        canStartConfiguredReview
        open
        displayMode="docked"
        onAvailableChange={vi.fn()}
        onClose={vi.fn()}
        onDisplayModeChange={vi.fn()}
        onNewReview={vi.fn()}
      />,
    );

    fireEvent.click(await screen.findByRole('button', { name: '配置并启动测试' }));
    fireEvent.change(screen.getByLabelText('测试目标类型'), {
      target: { value: 'pull_request' },
    });
    expect(await screen.findByText('Docker sandbox image is not installed')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: '开始 GitHub PR 测试' })).toBeDisabled();
    expect(api.startTestRun).not.toHaveBeenCalled();
  });

  it('shows the workspace Preview admission reason for the workspace target', async () => {
    vi.mocked(api.getTestHarnessCapabilities).mockResolvedValue({
      ...defaultCapabilities,
      available: false,
      reason: 'Project Preview configuration has not been confirmed',
      targets: {
        ...defaultCapabilities.targets,
        current_workspace: false,
      },
      target_reasons: {
        ...defaultCapabilities.target_reasons,
        current_workspace: 'Project Preview configuration has not been confirmed',
      },
      preview: {
        ...defaultCapabilities.preview,
        available: false,
        configured: false,
        reason: 'Project Preview configuration has not been confirmed',
      },
    });

    render(
      <BrowserReviewPanel
        taskId={73}
        taskActive={false}
        canStartConfiguredReview
        open
        displayMode="docked"
        onAvailableChange={vi.fn()}
        onClose={vi.fn()}
        onDisplayModeChange={vi.fn()}
        onNewReview={vi.fn()}
      />,
    );

    fireEvent.click(await screen.findByRole('button', { name: '配置并启动测试' }));
    fireEvent.change(screen.getByLabelText('测试目标类型'), {
      target: { value: 'current_workspace' },
    });
    expect(await screen.findByText('Project Preview configuration has not been confirmed')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: '开始 当前工作区 测试' })).toBeDisabled();
  });

  it('runs the Browser Agent with a model and effort independent from the Task', async () => {
    const startedRun = makeHarnessRun({
      id: 'harness-independent-runtime',
      root_run_id: 'harness-independent-runtime',
      status: 'queued',
      stage: 'waiting_for_browser',
      runtime: {
        provider: 'claude',
        model: 'claude-opus-5',
        reasoning_effort: 'max',
        codex_service_tier: 'default',
        selection_source: 'run_override',
      },
      report: null,
      completed_at: null,
    });
    vi.mocked(api.startTestRun).mockResolvedValue(startedRun);

    render(
      <BrowserReviewPanel
        taskId={73}
        taskActive={false}
        taskProvider="codex"
        taskModel="gpt-5.6-sol"
        taskEffort="high"
        taskServiceTier="priority"
        canStartConfiguredReview
        open
        displayMode="docked"
        onAvailableChange={vi.fn()}
        onClose={vi.fn()}
        onDisplayModeChange={vi.fn()}
        onNewReview={vi.fn()}
      />,
    );

    fireEvent.click(await screen.findByRole('button', { name: '配置并启动测试' }));
    await screen.findByText('Browser Agent 独立运行配置');
    fireEvent.click(screen.getByRole('checkbox', { name: /跟随当前 Task/ }));
    fireEvent.change(screen.getByLabelText('审查 Provider'), { target: { value: 'claude' } });
    fireEvent.change(screen.getByLabelText('审查模型'), { target: { value: 'claude-opus-5' } });
    fireEvent.change(screen.getByLabelText('推理强度'), { target: { value: 'max' } });
    fireEvent.change(screen.getByLabelText('待检测网站'), {
      target: { value: 'http://127.0.0.1:5173' },
    });
    fireEvent.change(screen.getByLabelText('测试目标'), {
      target: { value: '使用独立 Claude 模型审查页面' },
    });
    fireEvent.click(screen.getByRole('button', { name: '开始网站测试' }));

    await waitFor(() => expect(api.updateTestHarnessRuntimeConfig).toHaveBeenCalledWith(73, {
      inherit_task: false,
      provider: 'claude',
      model: 'claude-opus-5',
      reasoning_effort: 'max',
      codex_service_tier: 'default',
    }));
    await waitFor(() => expect(api.startTestRun).toHaveBeenCalledWith(73, expect.objectContaining({
      provider: 'claude',
      model: 'claude-opus-5',
      reasoning_effort: 'max',
      codex_service_tier: 'default',
    })));
    expect(await screen.findByText(/Browser Agent · Claude · claude-opus-5 · effort max/)).toBeInTheDocument();
  });

  it('supports floating, minimizing, restoring, and docking the review window', async () => {
    const runningJob: BrowserReviewJob = {
      ...completedJob,
      status: 'running',
      stage: 'executing_actions',
      completed_at: null,
      latest_screenshot: 'step-02.png',
      report: null,
    };
    const runningRun = makeHarnessRun({
      id: 'harness-running',
      root_run_id: 'harness-running',
      status: 'running',
      stage: 'executing_actions',
      browser_review: runningJob,
      evidence: [{
        id: 'evidence-screenshot',
        kind: 'screenshot',
        name: 'step-02.png',
        content_type: 'image/png',
        sha256: 'a'.repeat(64),
        byte_size: 10,
        metadata: {},
        created_at: '2026-08-05T00:00:02Z',
      }],
    });
    vi.mocked(api.listTestRuns).mockResolvedValue([runningRun]);
    vi.mocked(api.getTestRunEvidence).mockResolvedValue(new Blob(['screenshot'], { type: 'image/png' }));
    Object.defineProperty(URL, 'createObjectURL', {
      configurable: true,
      value: vi.fn(() => 'blob:frontend-review-screenshot'),
    });
    Object.defineProperty(URL, 'revokeObjectURL', {
      configurable: true,
      value: vi.fn(),
    });
    const onDisplayModeChange = vi.fn();

    render(
      <BrowserReviewPanel
        taskId={73}
        taskActive={false}
        open
        displayMode="floating"
        onAvailableChange={vi.fn()}
        onClose={vi.fn()}
        onDisplayModeChange={onDisplayModeChange}
        onNewReview={vi.fn()}
      />,
    );

    const panel = await screen.findByLabelText('Frontend Review progress');
    expect(panel).toHaveAttribute('data-display-mode', 'floating');
    expect(screen.getAllByText('正在验证页面状态').length).toBeGreaterThan(0);
    expect(await screen.findByAltText('Latest frontend review screenshot')).toHaveAttribute(
      'src',
      'blob:frontend-review-screenshot',
    );
    const screenshotHeading = screen.getByText('最新浏览器画面');
    const traceHeading = screen.getByText('模型观察与操作轨迹');
    expect(screenshotHeading.compareDocumentPosition(traceHeading)).toBe(
      Node.DOCUMENT_POSITION_FOLLOWING,
    );

    const dragHandle = panel.querySelector('[data-floating-drag-handle="true"]');
    expect(dragHandle).not.toBeNull();
    vi.spyOn(panel, 'getBoundingClientRect').mockReturnValue({
      x: 100,
      y: 100,
      left: 100,
      top: 100,
      right: 530,
      bottom: 700,
      width: 430,
      height: 600,
      toJSON: () => ({}),
    });
    fireEvent.pointerDown(dragHandle!, { button: 0, clientX: 120, clientY: 120 });
    fireEvent.pointerMove(window, { clientX: 300, clientY: 260 });
    fireEvent.pointerUp(window);
    await waitFor(() => {
      expect(panel).toHaveStyle({ left: '280px', top: '240px' });
    });

    fireEvent.click(screen.getByRole('button', { name: 'Minimize Frontend Review window' }));
    expect(screen.queryByText('模型观察与操作轨迹')).not.toBeInTheDocument();
    expect(screen.getByText('前端运行审查')).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Restore Frontend Review window' }));
    expect(screen.getByText('模型观察与操作轨迹')).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Dock Frontend Review panel' }));
    expect(onDisplayModeChange).toHaveBeenCalledWith('docked');
  });

  it('stops an active review from the Task progress panel', async () => {
    const runningJob: BrowserReviewJob = {
      ...completedJob,
      status: 'running',
      stage: 'executing_actions',
      completed_at: null,
      report: null,
    };
    const runningRun = makeHarnessRun({
      id: 'harness-running-stop',
      root_run_id: 'harness-running-stop',
      status: 'running',
      stage: 'executing_actions',
      browser_review: runningJob,
    });
    const cancelledRun = makeHarnessRun({
      ...runningRun,
      status: 'cancelled',
      stage: 'cancelled',
      completed_at: '2026-08-05T00:00:06Z',
      browser_review: { ...runningJob, status: 'cancelled', stage: 'cancelled', completed_at: '2026-08-05T00:00:06Z' },
    });
    vi.mocked(api.listTestRuns).mockResolvedValue([runningRun]);
    vi.mocked(api.cancelTestRun).mockResolvedValue(cancelledRun);

    render(
      <BrowserReviewPanel
        taskId={73}
        taskActive
        open
        displayMode="docked"
        onAvailableChange={vi.fn()}
        onClose={vi.fn()}
        onDisplayModeChange={vi.fn()}
        onNewReview={vi.fn()}
      />,
    );

    fireEvent.click(await screen.findByRole('button', { name: 'Stop test run' }));

    await waitFor(() => expect(api.cancelTestRun).toHaveBeenCalledWith(73, runningRun.id));
    expect((await screen.findAllByText('已停止')).length).toBeGreaterThan(0);
    expect(screen.queryByRole('button', { name: 'Stop test run' })).not.toBeInTheDocument();
  });

  it('repeats a terminal harness run and switches to the new run', async () => {
    const completedRun = makeHarnessRun({
      id: 'harness-repeat-source',
      root_run_id: 'harness-repeat-source',
      browser_review: completedJob,
    });
    const repeatedRun = makeHarnessRun({
      id: 'harness-repeat-next',
      root_run_id: 'harness-repeat-source',
      parent_run_id: completedRun.id,
      attempt_number: 2,
      status: 'queued',
      stage: 'waiting_for_browser',
      test_plan: { version: 1, objective: '第二轮前端复查', scenarios: [] },
      browser_review: null,
    });
    vi.mocked(api.listTestRuns).mockResolvedValue([completedRun]);
    vi.mocked(api.repeatTestRun).mockResolvedValue(repeatedRun);
    const onNewReview = vi.fn();

    render(
      <BrowserReviewPanel
        taskId={73}
        taskActive={false}
        open
        displayMode="docked"
        onAvailableChange={vi.fn()}
        onClose={vi.fn()}
        onDisplayModeChange={vi.fn()}
        onNewReview={onNewReview}
      />,
    );

    fireEvent.click(await screen.findByRole('button', { name: 'Repeat test run' }));

    await waitFor(() => expect(api.repeatTestRun).toHaveBeenCalledWith(73, completedRun.id));
    expect(await screen.findByText('第二轮前端复查')).toBeInTheDocument();
    expect(onNewReview).toHaveBeenCalledTimes(1);
  });

  it('automatically switches to a newly started review in the same task', async () => {
    const olderJob: BrowserReviewJob = {
      ...completedJob,
      id: 'inline-review-older',
      goal: '历史审查页面',
      created_at: '2026-08-04T23:00:00Z',
    };
    const newJob: BrowserReviewJob = {
      ...completedJob,
      id: 'inline-review-new',
      status: 'running',
      stage: 'browser_ready',
      goal: '新的审查页面',
      created_at: '2026-08-05T01:00:00Z',
      completed_at: null,
      report: null,
      trace: [],
    };
    const completedRun = makeHarnessRun({ id: 'harness-completed', root_run_id: 'harness-completed', browser_review: completedJob });
    const olderRun = makeHarnessRun({ id: 'harness-older', root_run_id: 'harness-older', browser_review: olderJob });
    const newRun = makeHarnessRun({ id: 'harness-new', root_run_id: 'harness-new', status: 'running', stage: 'browser_ready', browser_review: newJob });
    vi.mocked(api.listTestRuns)
      .mockResolvedValueOnce([completedRun, olderRun])
      .mockResolvedValueOnce([newRun, completedRun, olderRun]);
    const onNewReview = vi.fn();

    render(
      <BrowserReviewPanel
        taskId={73}
        taskActive={false}
        open
        displayMode="docked"
        onAvailableChange={vi.fn()}
        onClose={vi.fn()}
        onDisplayModeChange={vi.fn()}
        onNewReview={onNewReview}
      />,
    );

    const reviewPicker = await screen.findByRole('combobox');
    fireEvent.change(reviewPicker, { target: { value: olderRun.id } });
    expect(screen.getAllByText('历史审查页面').length).toBeGreaterThan(0);

    fireEvent.click(screen.getByTitle('刷新审查进度'));
    expect((await screen.findAllByText('新的审查页面')).length).toBeGreaterThan(0);
    expect(reviewPicker).toHaveValue(newRun.id);
    expect(onNewReview).toHaveBeenCalledTimes(1);
  });

  it('switches to an older run with an explicit control and preserves it on refresh', async () => {
    const newerRun = makeHarnessRun({
      id: 'harness-newer',
      root_run_id: 'harness-newer',
      test_plan: { version: 1, objective: '最新审查', scenarios: [] },
    });
    const olderRun = makeHarnessRun({
      id: 'harness-older',
      root_run_id: 'harness-older',
      test_plan: { version: 1, objective: '历史终态审查', scenarios: [] },
    });
    vi.mocked(api.listTestRuns).mockResolvedValue([newerRun, olderRun]);

    render(
      <BrowserReviewPanel
        taskId={73}
        taskActive={false}
        open
        displayMode="docked"
        onAvailableChange={vi.fn()}
        onClose={vi.fn()}
        onDisplayModeChange={vi.fn()}
        onNewReview={vi.fn()}
      />,
    );

    const picker = await screen.findByRole('combobox', { name: 'Select test run' });
    expect(picker).toHaveValue(newerRun.id);
    fireEvent.click(screen.getByRole('button', { name: 'Select older test run' }));
    expect(picker).toHaveValue(olderRun.id);
    expect(screen.getAllByText('历史终态审查').length).toBeGreaterThan(0);

    fireEvent.click(screen.getByTitle('刷新审查进度'));
    await waitFor(() => expect(api.listTestRuns).toHaveBeenCalledTimes(2));
    expect(picker).toHaveValue(olderRun.id);
  });

  it('shows ordinary-chat expectation and switches from its exact baseline to the new workspace run', async () => {
    const oldRun = makeWorkspaceRun({
      id: 'workspace-run-old',
      goal: '历史前端验收',
    });
    const newRun = makeWorkspaceRun({
      id: 'workspace-run-new',
      goal: '本轮 PR99 前端验收',
      status: 'preparing',
      stage: 'starting_preview',
      report: null,
      cleanup_status: 'pending',
      created_at: '2026-08-06T01:00:00Z',
      completed_at: null,
    });
    const oldHarnessRun = makeHarnessRun({ id: 'harness-old', root_run_id: 'harness-old', workspace_review: oldRun });
    const newHarnessRun = makeHarnessRun({
      id: 'harness-new-pr99',
      root_run_id: 'harness-new-pr99',
      status: 'preparing_environment',
      stage: 'starting_preview',
      workspace_review: newRun,
    });
    vi.mocked(api.listTestRuns)
      .mockResolvedValueOnce([oldHarnessRun])
      .mockResolvedValueOnce([oldHarnessRun])
      .mockResolvedValue([newHarnessRun, oldHarnessRun]);
    const onExpectedWorkspaceReviewFound = vi.fn();
    const onNewReview = vi.fn();

    render(
      <BrowserReviewPanel
        taskId={73}
        taskActive
        open
        displayMode="docked"
        onAvailableChange={vi.fn()}
        onClose={vi.fn()}
        onDisplayModeChange={vi.fn()}
        onNewReview={onNewReview}
        expectedWorkspaceReviewBaseline={oldHarnessRun.id}
        onExpectedWorkspaceReviewFound={onExpectedWorkspaceReviewFound}
      />,
    );

    expect(await screen.findByText('正在创建新的前端测试')).toBeInTheDocument();
    expect(screen.getByText('上一轮测试仍保留在历史记录中，本页不会继续展示其内容。')).toBeInTheDocument();
    expect(screen.queryByText('历史前端验收')).not.toBeInTheDocument();
    expect(screen.queryByTestId('test-harness-progress')).not.toBeInTheDocument();
    expect(screen.queryByTestId('workspace-review-progress')).not.toBeInTheDocument();
    fireEvent.click(screen.getByTitle('刷新审查进度'));

    expect((await screen.findAllByText('本轮 PR99 前端验收')).length).toBeGreaterThan(0);
    expect(onExpectedWorkspaceReviewFound).toHaveBeenCalledTimes(1);
    expect(onNewReview).toHaveBeenCalledTimes(1);
  });

  it('replaces the previous run with a dedicated page for every Goal browser review request', async () => {
    const firstWorkspaceRun = makeWorkspaceRun({
      id: 'workspace-goal-first',
      goal: '第一轮基线审查',
    });
    const firstHarnessRun = makeHarnessRun({
      id: 'harness-goal-first',
      root_run_id: 'harness-goal-first',
      workspace_review: firstWorkspaceRun,
    });
    const secondWorkspaceRun = makeWorkspaceRun({
      id: 'workspace-goal-second',
      goal: '修改后的第一次复查',
      status: 'preparing',
      stage: 'starting_preview',
      report: null,
      cleanup_status: 'pending',
      created_at: '2026-08-06T01:00:00Z',
      completed_at: null,
    });
    const secondHarnessRun = makeHarnessRun({
      id: 'harness-goal-second',
      root_run_id: 'harness-goal-second',
      status: 'preparing_environment',
      stage: 'starting_preview',
      workspace_review: secondWorkspaceRun,
    });
    const thirdWorkspaceRun = makeWorkspaceRun({
      id: 'workspace-goal-third',
      goal: '修改后的第二次复查',
      status: 'preparing',
      stage: 'starting_preview',
      report: null,
      cleanup_status: 'pending',
      created_at: '2026-08-06T02:00:00Z',
      completed_at: null,
    });
    const thirdHarnessRun = makeHarnessRun({
      id: 'harness-goal-third',
      root_run_id: 'harness-goal-third',
      status: 'preparing_environment',
      stage: 'starting_preview',
      workspace_review: thirdWorkspaceRun,
    });
    let listedRuns = [firstHarnessRun];
    vi.mocked(api.listTestRuns).mockImplementation(async () => listedRuns);
    const onGoalReviewFound = vi.fn();
    const commonProps = {
      taskId: 73,
      taskActive: true,
      open: true,
      displayMode: 'docked' as const,
      onAvailableChange: vi.fn(),
      onClose: vi.fn(),
      onDisplayModeChange: vi.fn(),
      onNewReview: vi.fn(),
      onGoalReviewFound,
      goalProgress: { turn: 0, maxTurns: 5, lastReason: null, active: true },
    };
    const { rerender } = render(<BrowserReviewPanel {...commonProps} />);

    expect((await screen.findAllByText('第一轮基线审查')).length).toBeGreaterThan(0);
    rerender(
      <BrowserReviewPanel
        {...commonProps}
        goalStart={{
          requestId: 1,
          prompt: '修改后重新检查弹窗',
          maxTurns: 5,
          phase: 'starting_review',
        }}
      />,
    );

    expect(await screen.findByText('正在创建本轮浏览器复查')).toBeInTheDocument();
    expect(screen.getByText('修改后重新检查弹窗')).toBeInTheDocument();
    expect(screen.queryByText('第一轮基线审查')).not.toBeInTheDocument();

    listedRuns = [secondHarnessRun, firstHarnessRun];
    fireEvent.click(screen.getByTitle('刷新审查进度'));
    await waitFor(() => expect(onGoalReviewFound).toHaveBeenCalledTimes(1));
    rerender(<BrowserReviewPanel {...commonProps} />);
    expect((await screen.findAllByText('修改后的第一次复查')).length).toBeGreaterThan(0);

    rerender(
      <BrowserReviewPanel
        {...commonProps}
        goalStart={{
          requestId: 2,
          prompt: '再次复查键盘焦点',
          maxTurns: 5,
          phase: 'starting_review',
        }}
      />,
    );
    expect(await screen.findByText('正在创建本轮浏览器复查')).toBeInTheDocument();
    expect(screen.getByText('再次复查键盘焦点')).toBeInTheDocument();
    expect(screen.queryByText('修改后的第一次复查')).not.toBeInTheDocument();

    listedRuns = [thirdHarnessRun, secondHarnessRun, firstHarnessRun];
    fireEvent.click(screen.getByTitle('刷新审查进度'));
    await waitFor(() => expect(onGoalReviewFound).toHaveBeenCalledTimes(2));
    rerender(<BrowserReviewPanel {...commonProps} />);
    expect((await screen.findAllByText('修改后的第二次复查')).length).toBeGreaterThan(0);
  });

  it('uses high-contrast theme tokens for the light review panel', async () => {
    document.documentElement.dataset.theme = 'light';
    vi.mocked(api.listTestRuns).mockResolvedValue([
      makeHarnessRun({
        id: 'harness-light-theme',
        root_run_id: 'harness-light-theme',
        browser_review: completedJob,
      }),
    ]);
    const { container } = render(
      <BrowserReviewPanel
        taskId={73}
        taskActive={false}
        open
        displayMode="docked"
        onAvailableChange={vi.fn()}
        onClose={vi.fn()}
        onDisplayModeChange={vi.fn()}
        onNewReview={vi.fn()}
      />,
    );

    const panel = await screen.findByLabelText('Frontend Review progress');
    expect(panel).toHaveClass('border-gray-600/60');
    expect(screen.getByText(/Test Harness/)).toHaveClass('text-indigo-300');
    expect(container.querySelectorAll('[class~="border-gray-800"]')).toHaveLength(0);
    expect(screen.getByText('审查结论').closest('.prose')).toHaveClass('prose-p:text-gray-300');
  });
});
