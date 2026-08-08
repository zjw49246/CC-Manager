import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor, act, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { ChatView } from './ChatView';
import type { ChatMessage, PlanResource, PlanVersion, Project, Task, TestHarnessRun, UploadResult, WorkspaceReviewRun } from '../../api/client';

// Mock dependencies
vi.mock('../../api/client', () => ({
  isApiRequestError: (error: unknown) => (
    error instanceof Error
    && typeof (error as { status?: unknown }).status === 'number'
  ),
  api: {
    getTaskChatHistory: vi.fn().mockResolvedValue([]),
    sendTaskChat: vi.fn().mockResolvedValue({}),
    startFrontendReviewGoal: vi.fn().mockResolvedValue({
      status: 'pending',
      goal_max_turns: 5,
    }),
    getFrontendReviewGoalCapabilities: vi.fn().mockResolvedValue({
      available: true,
      reason: null,
      repo_path: '/repo',
    }),
    getWorkspaceReviewCapabilities: vi.fn().mockResolvedValue({
      available: true,
      reason: null,
      repo_path: '/repo',
      configured: true,
      config: {},
      suggested_config: null,
    }),
    approveWorkspacePreviewConfig: vi.fn().mockResolvedValue({
      available: true,
      reason: null,
      repo_path: '/repo',
      configured: true,
      config: {},
      suggested_config: null,
    }),
    startTestRun: vi.fn().mockResolvedValue({ id: 'test-harness-run-1' }),
    getTestHarnessRuntimeConfig: vi.fn().mockResolvedValue({
      inherit_task: true,
      provider: 'codex',
      model: 'gpt-5.6-sol',
      reasoning_effort: 'medium',
      codex_service_tier: 'default',
      source: 'task',
      task_runtime: {
        provider: 'codex',
        model: 'gpt-5.6-sol',
        reasoning_effort: 'medium',
        codex_service_tier: 'default',
      },
      default_provider: 'codex',
      providers: ['claude', 'codex'],
      default_models: { claude: 'claude-opus-4-6', codex: 'gpt-5.6-sol' },
      models_by_provider: { claude: ['claude-opus-4-6'], codex: ['gpt-5.6-sol'] },
      default_effort: 'medium',
      effort_options: { claude: ['medium'], codex: ['medium'] },
      model_efforts: { claude: {}, codex: {} },
      codex_service_tiers: ['default', 'priority'],
      codex_model_service_tiers: { 'gpt-5.6-sol': ['default', 'priority'] },
    }),
    updateTestHarnessRuntimeConfig: vi.fn(),
    listTestRuns: vi.fn().mockResolvedValue([]),
    getTestRunEvidence: vi.fn().mockResolvedValue(new Blob()),
    cancelTestRun: vi.fn().mockResolvedValue({}),
    listWorkspaceReviews: vi.fn().mockResolvedValue([]),
    cancelWorkspaceReview: vi.fn().mockResolvedValue({}),
    updateTask: vi.fn().mockResolvedValue({}),
    stopTaskSession: vi.fn().mockResolvedValue({
      ok: true,
      stopped: true,
      task_status: 'completed',
      background_active: false,
    }),
    listForkAnchors: vi.fn().mockResolvedValue([]),
    forkTask: vi.fn().mockResolvedValue({}),
    uploadImages: vi.fn().mockResolvedValue([]),
    listMonitorSessions: vi.fn().mockResolvedValue([]),
    listTaskBrowserReviews: vi.fn().mockResolvedValue([]),
    getTaskBrowserReviewArtifact: vi.fn().mockResolvedValue(new Blob()),
    getAskUserPending: vi.fn().mockResolvedValue({ pending: [] }),
    getRuntimeSettings: vi.fn().mockResolvedValue({
      use_pty_mode: false,
      pty_available: false,
      codex_app_server_enabled: true,
      codex_main_mcp_enabled: true,
      codex_monitor_enabled: true,
    }),
    getWorkerRuntimeSettings: vi.fn().mockResolvedValue({
      use_pty_mode: false,
      pty_available: false,
      codex_app_server_enabled: true,
      codex_main_mcp_enabled: true,
      codex_monitor_enabled: true,
    }),
    config: vi.fn().mockResolvedValue({ model_options: ['claude-opus-4-6'], codex_model_options: [] }),
    getInjectCapabilities: vi.fn().mockResolvedValue({
      attachment_protocol: 1,
      codex_native_inputs: true,
    }),
    injectTaskMessage: vi.fn().mockResolvedValue({ ok: true, injected: true }),
    listQuickPhrases: vi.fn().mockResolvedValue([]),
    createQuickPhrase: vi.fn().mockResolvedValue({}),
    updateQuickPhrase: vi.fn().mockResolvedValue({}),
    deleteQuickPhrase: vi.fn().mockResolvedValue({}),
    getMessageDetail: vi.fn().mockResolvedValue({}),
    getMonitorChecks: vi.fn().mockResolvedValue([]),
    deleteMonitorSession: vi.fn().mockResolvedValue({}),
    resolvePermission: vi.fn().mockResolvedValue({}),
    submitAskUser: vi.fn().mockResolvedValue({}),
    starTask: vi.fn().mockResolvedValue({}),
    distillTask: vi.fn().mockResolvedValue({}),
    saveDistilledSkill: vi.fn().mockResolvedValue({}),
    cancelTask: vi.fn().mockResolvedValue({}),
    listPlans: vi.fn().mockResolvedValue([]),
    createPlan: vi.fn().mockResolvedValue({}),
    listPlanVersions: vi.fn().mockResolvedValue([]),
    listPlanResourceRuns: vi.fn().mockResolvedValue([]),
    createPlanRun: vi.fn().mockResolvedValue({}),
    approvePlanVersion: vi.fn().mockResolvedValue({}),
    rejectPlanVersion: vi.fn().mockResolvedValue({}),
    forkPlan: vi.fn().mockResolvedValue({}),
    cancelPlanRun: vi.fn().mockResolvedValue({}),
    answerPlanInput: vi.fn().mockResolvedValue({}),
    getPlanVersionStaleness: vi.fn().mockResolvedValue({
      stale: false,
      hard_conflict: false,
      reasons: [],
      hard_conflicts: [],
      can_confirm: false,
    }),
    createVersionExecutionTask: vi.fn().mockResolvedValue({ execution_task_id: 99 }),
    updatePlan: vi.fn().mockResolvedValue({}),
    downloadTaskArtifact: vi.fn().mockResolvedValue({
      blob: new Blob(['artifact']),
      filename: '汇报稿.md',
    }),
  },
}));

// Store the onMessage/onReconnect callbacks so tests can trigger them
let capturedOnReconnect: (() => void) | undefined;
let capturedOnMessage: ((msg: Record<string, unknown>) => void) | undefined;
let capturedOnSubscribed: ((channels: string[]) => void) | undefined;
vi.mock('../../hooks/useWebSocket', () => ({
  useWebSocket: vi.fn((
    _channels: string[],
    onMessage?: unknown,
    onReconnect?: () => void,
    onSubscribed?: (channels: string[]) => void,
  ) => {
    capturedOnMessage = onMessage as typeof capturedOnMessage;
    capturedOnReconnect = onReconnect;
    capturedOnSubscribed = onSubscribed;
    return { lastMessage: null, isConnected: true };
  }),
}));

vi.mock('../Secrets/SecretPicker', () => ({
  SecretPicker: () => null,
}));

vi.mock('./SubAgentIndicator', () => ({
  SubAgentIndicator: ({ onNavigate }: { onNavigate?: () => void }) => (
    <button onClick={onNavigate}>Open monitors</button>
  ),
}));

import { api } from '../../api/client';

function makeTask(overrides: Partial<Task> = {}): Task {
  return {
    id: 1,
    title: '',
    description: 'Initial task prompt here',
    status: 'pending',
    priority: 0,
    project_id: null,
    target_repo: null,
    target_branch: 'main',
    result_branch: null,
    merge_status: 'pending',
    instance_id: null,
    retry_count: 0,
    max_retries: 3,
    mode: 'auto',
    todo_file_path: null,
    loop_progress: null,
    max_iterations: 50,
    must_complete: false,
    plan_content: null,
    plan_approved: null,
    starred: false,
    archived: false,
    has_unread: false,
    session_id: 'session-123',
    error_message: null,
    provider: 'claude',
    model: null,
    codex_service_tier: 'default',
    tags: null,
    context_window_usage: null,
    created_at: '2024-01-01T00:00:00Z',
    started_at: null,
    completed_at: null,
    ...overrides,
  };
}

function makePlanVersion(overrides: Partial<PlanVersion> = {}): PlanVersion {
  return {
    id: 501,
    plan_id: 80,
    version_number: 1,
    parent_version_id: null,
    produced_by_run_id: 700,
    produced_by_step_id: 701,
    content: '# Candidate Plan',
    context_session_id: 'session-123',
    context_log_id: 1,
    repo_revision: null,
    review_verdict: 'approve',
    review_feedback: null,
    reviewed_by_step_id: 702,
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

function makePlan(overrides: Partial<PlanResource> = {}): PlanResource {
  const version = makePlanVersion();
  return {
    id: 80,
    title: 'Versioned Plan',
    initial_request: 'Design the change',
    initial_attachments: null,
    target_task_id: 1,
    project_id: null,
    target_repo: '/tmp',
    target_branch: 'main',
    worker_id: null,
    priority: 0,
    timeout_hours: null,
    created_by: null,
    current_version_id: version.id,
    active_run_id: null,
    forked_from_version_id: null,
    archived_at: null,
    closed_at: null,
    lock_version: 0,
    created_at: '2026-08-02T08:00:00Z',
    updated_at: '2026-08-02T08:00:00Z',
    display_state: 'awaiting_review',
    pipeline_config: {
      version: 1,
      planner: {
        primary: { provider: 'claude', model: 'claude-opus-4-6', effort: 'high' },
        fallback: { provider: 'claude', model: 'claude-sonnet-4-6', effort: 'high' },
      },
      reviewer: {
        enabled: true,
        primary: { provider: 'claude', model: 'claude-opus-4-6', effort: 'high' },
        fallback: { provider: 'claude', model: 'claude-sonnet-4-6', effort: 'high' },
      },
      max_revision_cycles: 2,
      max_interactions: 3,
    },
    legacy: false,
    latest_run_status: 'completed',
    latest_run_error: null,
    current_version: version,
    active_run: null,
    open_input_request: null,
    application: null,
    applications: [],
    ...overrides,
  };
}

function makeUpload(id: string, filename = `${id}.txt`): UploadResult {
  return {
    id,
    filename,
    path: `/srv/uploads/${filename}`,
    url: `/api/uploads/${filename}`,
    is_image: false,
  };
}

describe('ChatView', () => {
  const projects: Project[] = [];
  const onBack = vi.fn();
  const onTaskUpdated = vi.fn();

  beforeEach(() => {
    localStorage.clear();
    vi.clearAllMocks();
    (api.getTaskChatHistory as ReturnType<typeof vi.fn>).mockResolvedValue([]);
    (api.getAskUserPending as ReturnType<typeof vi.fn>).mockResolvedValue({ pending: [] });
    (api.listForkAnchors as ReturnType<typeof vi.fn>).mockResolvedValue([]);
    (api.listPlans as ReturnType<typeof vi.fn>).mockResolvedValue([]);
    (api.listPlanVersions as ReturnType<typeof vi.fn>).mockResolvedValue([]);
    (api.getRuntimeSettings as ReturnType<typeof vi.fn>).mockResolvedValue({
      use_pty_mode: false,
      pty_available: false,
      codex_app_server_enabled: true,
      codex_main_mcp_enabled: true,
      codex_monitor_enabled: true,
    });
    (api.getWorkerRuntimeSettings as ReturnType<typeof vi.fn>).mockResolvedValue({
      use_pty_mode: false,
      pty_available: false,
      codex_app_server_enabled: true,
      codex_main_mcp_enabled: true,
      codex_monitor_enabled: true,
    });
    (api.uploadImages as ReturnType<typeof vi.fn>).mockResolvedValue([]);
    (api.getInjectCapabilities as ReturnType<typeof vi.fn>).mockResolvedValue({
      attachment_protocol: 1,
      codex_native_inputs: true,
    });
    (api.injectTaskMessage as ReturnType<typeof vi.fn>).mockResolvedValue({
      ok: true,
      injected: true,
    });
    (api.sendTaskChat as ReturnType<typeof vi.fn>).mockResolvedValue({});
    (api.stopTaskSession as ReturnType<typeof vi.fn>).mockResolvedValue({
      ok: true,
      stopped: true,
      task_status: 'completed',
      background_active: false,
    });
    (api.startFrontendReviewGoal as ReturnType<typeof vi.fn>).mockResolvedValue({
      status: 'pending',
      goal_max_turns: 5,
    });
    (api.getFrontendReviewGoalCapabilities as ReturnType<typeof vi.fn>).mockResolvedValue({
      available: true,
      reason: null,
      repo_path: '/repo',
    });
  });

  it('fits full-screen chat to the iOS visual viewport but leaves inline chat alone', () => {
    const originalViewport = Object.getOwnPropertyDescriptor(window, 'visualViewport');
    const viewport = Object.assign(new EventTarget(), {
      height: 486,
      offsetTop: 47,
    });
    Object.defineProperty(window, 'visualViewport', {
      configurable: true,
      value: viewport,
    });

    try {
      const fullScreen = render(
        <ChatView task={makeTask()} projects={projects} onBack={onBack} />,
      );
      expect(fullScreen.container.firstElementChild).toHaveStyle({
        height: '486px',
        top: '47px',
        bottom: 'auto',
      });
      fullScreen.unmount();

      const inline = render(
        <ChatView task={makeTask()} projects={projects} onBack={onBack} inline />,
      );
      expect((inline.container.firstElementChild as HTMLElement).style.height).toBe('');
      expect((inline.container.firstElementChild as HTMLElement).style.top).toBe('');
      expect((inline.container.firstElementChild as HTMLElement).style.bottom).toBe('');
      inline.unmount();
    } finally {
      if (originalViewport) {
        Object.defineProperty(window, 'visualViewport', originalViewport);
      } else {
        Reflect.deleteProperty(window, 'visualViewport');
      }
    }
  });

  describe('chat conflict state', () => {
    it('does not show Interrupt for a non-busy 409 rejection', async () => {
      const rejection = Object.assign(
        new Error('PR review workflow is not terminal'),
        { status: 409, detail: 'PR review workflow is not terminal' },
      );
      vi.mocked(api.sendTaskChat).mockRejectedValueOnce(rejection);
      render(
        <ChatView
          task={makeTask({ status: 'completed', tags: ['pr-review'] })}
          projects={projects}
          onBack={onBack}
        />,
      );

      await userEvent.type(screen.getByRole('textbox'), 'follow up');
      await userEvent.click(screen.getByTitle('Send (Ctrl+Enter)'));

      await screen.findByText(/PR review workflow is not terminal/);
      expect(screen.queryByTitle('Interrupt session')).not.toBeInTheDocument();
    });

    it('still shows Interrupt for a genuine busy conflict', async () => {
      const rejection = Object.assign(
        new Error('The preceding Codex turn is still running'),
        { status: 409, detail: 'The preceding Codex turn is still running' },
      );
      vi.mocked(api.sendTaskChat).mockRejectedValueOnce(rejection);
      render(
        <ChatView
          task={makeTask({ status: 'completed' })}
          projects={projects}
          onBack={onBack}
        />,
      );

      await userEvent.type(screen.getByRole('textbox'), 'follow up');
      await userEvent.click(screen.getByTitle('Send (Ctrl+Enter)'));

      expect(await screen.findByTitle('Interrupt session')).toBeInTheDocument();
    });
  });

  describe('optimistic user-message reconciliation', () => {
    it('removes the optimistic bubble when an id-less WS echo precedes HTTP failure', async () => {
      let rejectSend!: (reason: Error) => void;
      vi.mocked(api.sendTaskChat).mockReturnValueOnce(
        new Promise((_resolve, reject) => { rejectSend = reject; }),
      );
      render(
        <ChatView
          task={makeTask({ id: 17, status: 'completed' })}
          projects={projects}
          onBack={onBack}
          onTaskUpdated={onTaskUpdated}
        />,
      );

      await userEvent.type(screen.getByRole('textbox'), 'pending legacy echo');
      await userEvent.click(screen.getByTitle('Send (Ctrl+Enter)'));
      await waitFor(() => expect(api.sendTaskChat).toHaveBeenCalled());
      expect(screen.getByText('pending legacy echo')).toBeInTheDocument();

      act(() => {
        capturedOnMessage?.({
          channel: 'task:17',
          data: {
            event_type: 'user_message',
            role: 'user',
            content: '[Legacy] pending legacy echo',
            raw_content: 'pending legacy echo',
          },
        });
      });
      expect(screen.getAllByText('[Legacy] pending legacy echo')).toHaveLength(1);

      await act(async () => {
        rejectSend(new Error('send failed after live echo'));
      });

      expect(await screen.findByText(/send failed after live echo/)).toBeInTheDocument();
      await waitFor(() => {
        expect(screen.queryByText('[Legacy] pending legacy echo')).not.toBeInTheDocument();
      });
      expect(screen.getByRole('textbox')).toHaveValue('pending legacy echo');
    });
  });

  describe('legacy Codex completion compatibility', () => {
    it('hides only raw-classified collab item completions from history', async () => {
      const legacyNoise: ChatMessage = {
        id: 901,
        role: 'system',
        event_type: 'system_event',
        content: 'completed',
        tool_name: null,
        tool_input: null,
        tool_output: null,
        is_error: false,
        loop_iteration: null,
        timestamp: null,
        image_urls: null,
        attachments: null,
        native_item_type: 'collabAgentToolCall',
        native_item_status: 'completed',
      };
      const legitimateSystemMessage: ChatMessage = {
        ...legacyNoise,
        id: 902,
        native_item_type: null,
        native_item_status: null,
      };
      const visibleReply: ChatMessage = {
        ...legacyNoise,
        id: 903,
        role: 'assistant',
        event_type: 'message',
        content: 'Reply after agent wait',
        native_item_type: null,
        native_item_status: null,
      };
      (api.getTaskChatHistory as ReturnType<typeof vi.fn>).mockResolvedValue([
        legacyNoise,
        legitimateSystemMessage,
        visibleReply,
      ]);

      render(
        <ChatView
          task={makeTask({ provider: 'codex', status: 'completed' })}
          projects={projects}
          onBack={onBack}
        />,
      );

      expect(await screen.findByText('Reply after agent wait')).toBeInTheDocument();
      expect(screen.getAllByText('— completed —')).toHaveLength(1);
    });
  });

  describe('Codex main MCP capability', () => {
    it('shows the enabled runtime capability on Codex tasks', async () => {
      render(
        <ChatView
          task={makeTask({ provider: 'codex' })}
          projects={projects}
          onBack={onBack}
          onTaskUpdated={onTaskUpdated}
        />,
      );

      expect(await screen.findByTitle('Codex 主任务 MCP 已启用')).toHaveTextContent('MCP 已启用');
    });

    it('shows the Fast badge for a Codex priority task', async () => {
      render(
        <ChatView
          task={makeTask({
            provider: 'codex',
            codex_service_tier: 'priority',
          })}
          projects={projects}
          onBack={onBack}
          onTaskUpdated={onTaskUpdated}
        />,
      );

      expect(screen.getByTestId('codex-fast-badge')).toHaveTextContent('Fast');
    });

    it('blocks an unsupported one-shot model while the task is Fast', async () => {
      vi.mocked(api.config).mockResolvedValue({
        default_provider: 'codex',
        provider_options: ['claude', 'codex'],
        default_model: 'claude-opus-4-6',
        model_options: ['claude-opus-4-6'],
        default_codex_model: 'gpt-5.5',
        codex_model_options: ['gpt-5.5', 'gpt-5.4-mini'],
        default_effort: 'medium',
        effort_options: ['low', 'medium', 'high'],
        claude_model_efforts: {},
        claude_model_context_windows: {},
        codex_effort_options: ['low', 'medium', 'high', 'xhigh'],
        codex_model_efforts: {},
        codex_model_service_tiers: {
          'gpt-5.5': ['default', 'priority'],
          'gpt-5.4-mini': ['default'],
        },
      });
      render(
        <ChatView
          task={makeTask({
            provider: 'codex',
            model: 'gpt-5.5',
            codex_service_tier: 'priority',
          })}
          projects={projects}
          onBack={onBack}
          onTaskUpdated={onTaskUpdated}
        />,
      );

      await userEvent.click(screen.getByTitle('临时切换模型（仅下一条消息）'));
      const unsupported = await screen.findByRole('button', { name: /gpt-5\.4-mini/ });
      expect(unsupported).toBeDisabled();
      expect(unsupported).toHaveAttribute(
        'title',
        'gpt-5.4-mini 不支持 Fast；先在 Task Config 中切换为 Standard',
      );
    });

    it('shows the emergency opt-out state returned by the backend', async () => {
      (api.getRuntimeSettings as ReturnType<typeof vi.fn>).mockResolvedValueOnce({
        use_pty_mode: false,
        pty_available: false,
        codex_app_server_enabled: true,
        codex_main_mcp_enabled: false,
      });

      render(
        <ChatView
          task={makeTask({ provider: 'codex' })}
          projects={projects}
          onBack={onBack}
          onTaskUpdated={onTaskUpdated}
        />,
      );

      expect(await screen.findByTitle('Codex 主任务 MCP 已关闭')).toHaveTextContent('MCP 已关闭');
    });

    it('refreshes the capability badge from runtime settings broadcasts', async () => {
      render(
        <ChatView
          task={makeTask({ provider: 'codex' })}
          projects={projects}
          onBack={onBack}
          onTaskUpdated={onTaskUpdated}
        />,
      );
      await screen.findByTitle('Codex 主任务 MCP 已启用');

      act(() => {
        capturedOnMessage?.({
          channel: 'system',
          data: {
            event: 'runtime_settings_changed',
            use_pty_mode: false,
            codex_app_server_enabled: true,
            codex_main_mcp_enabled: false,
          },
        });
      });

      expect(screen.getByTitle('Codex 主任务 MCP 已关闭')).toHaveTextContent('MCP 已关闭');
    });

    it('shows the proxied Worker capability for Worker tasks', async () => {
      (api.getWorkerRuntimeSettings as ReturnType<typeof vi.fn>).mockResolvedValueOnce({
        use_pty_mode: false,
        pty_available: false,
        codex_app_server_enabled: true,
        codex_main_mcp_enabled: false,
      });

      render(
        <ChatView
          task={makeTask({ provider: 'codex', worker_id: 7 })}
          projects={projects}
          onBack={onBack}
          onTaskUpdated={onTaskUpdated}
        />,
      );

      await waitFor(() => expect(api.getWorkerRuntimeSettings).toHaveBeenCalledWith(7));
      expect(api.getRuntimeSettings).not.toHaveBeenCalled();
      expect(screen.getByTitle('Codex 主任务 MCP 已关闭')).toHaveTextContent('MCP 已关闭');

      act(() => {
        capturedOnMessage?.({
          channel: 'system',
          data: {
            event: 'runtime_settings_changed',
            use_pty_mode: false,
            codex_app_server_enabled: true,
            codex_main_mcp_enabled: true,
          },
        });
      });
      expect(screen.getByTitle('Codex 主任务 MCP 已关闭')).toBeInTheDocument();
    });

    it('does not report an old Worker capability as disabled when it is absent', async () => {
      (api.getWorkerRuntimeSettings as ReturnType<typeof vi.fn>).mockResolvedValueOnce({
        use_pty_mode: false,
        pty_available: false,
        codex_app_server_enabled: true,
      });

      render(
        <ChatView
          task={makeTask({ provider: 'codex', worker_id: 7 })}
          projects={projects}
          onBack={onBack}
          onTaskUpdated={onTaskUpdated}
        />,
      );

      await waitFor(() => expect(api.getWorkerRuntimeSettings).toHaveBeenCalledWith(7));
      expect(screen.queryByTestId('codex-main-mcp-status')).not.toBeInTheDocument();
    });

    it('opens the Monitor panel without a warning for local Codex capability', async () => {
      render(
        <ChatView
          task={makeTask({
            provider: 'codex',
            worker_id: null,
            shared_from_id: null,
          })}
          projects={projects}
          onBack={onBack}
          onTaskUpdated={onTaskUpdated}
        />,
      );

      await waitFor(() => expect(api.getRuntimeSettings).toHaveBeenCalled());
      await userEvent.click(screen.getByRole('button', { name: 'Open monitors' }));
      expect(screen.getByText('Sub-Agents')).toBeInTheDocument();
      expect(
        screen.queryByText(/Codex Monitor 当前仅支持 capability/),
      ).not.toBeInTheDocument();
    });

    it('keeps the Monitor warning for a Codex Worker task', async () => {
      render(
        <ChatView
          task={makeTask({ provider: 'codex', worker_id: 7 })}
          projects={projects}
          onBack={onBack}
          onTaskUpdated={onTaskUpdated}
        />,
      );

      await waitFor(() => {
        expect(api.getWorkerRuntimeSettings).toHaveBeenCalledWith(7);
      });
      await userEvent.click(screen.getByRole('button', { name: 'Open monitors' }));
      expect(
        screen.getByText(/Codex Monitor 当前仅支持 capability/),
      ).toBeInTheDocument();
    });
  });

  describe('PTY background activity', () => {
    it.each([
      ['claude', 'executing', 'Claude'],
      ['codex', 'in_progress', 'Codex'],
    ] as const)(
      'restores the %s thinking indicator when an already-running task is opened',
      (provider, status, label) => {
        render(
          <ChatView
            task={makeTask({ id: provider === 'claude' ? 301 : 302, provider, status })}
            projects={projects}
            onBack={onBack}
          />,
        );

        expect(screen.getByText(`${label} is thinking...`)).toBeInTheDocument();
        expect(screen.getByTitle('Interrupt session')).toBeInTheDocument();
      },
    );

    it('shows the background badge while the foreground status is still executing', () => {
      const task = makeTask({ id: 31, status: 'executing', background_active: false });
      render(<ChatView task={task} projects={projects} onBack={onBack} />);

      act(() => {
        capturedOnMessage?.({
          channel: 'tasks',
          data: {
            event: 'status_change',
            task_id: 31,
            new_status: 'executing',
            background_active: true,
          },
        });
      });

      expect(screen.getByText('后台运行中')).toBeInTheDocument();
    });

    it('consumes only matching strict-boolean global background events', () => {
      const task = makeTask({ id: 32, background_active: false });
      render(<ChatView task={task} projects={projects} onBack={onBack} />);

      act(() => {
        capturedOnMessage?.({
          channel: 'tasks',
          data: {
            event: 'background_activity',
            task_id: 32,
            background_active: true,
          },
        });
      });
      expect(screen.getByText('后台运行中')).toBeInTheDocument();

      act(() => {
        capturedOnMessage?.({
          channel: 'tasks',
          data: {
            event: 'background_activity',
            task_id: 32,
            background_active: 'false',
          },
        });
        capturedOnMessage?.({
          channel: 'tasks',
          data: {
            event: 'background_activity',
            task_id: 999,
            background_active: false,
          },
        });
      });
      expect(screen.getByText('后台运行中')).toBeInTheDocument();

      act(() => {
        capturedOnMessage?.({
          channel: 'tasks',
          data: {
            event: 'background_activity',
            task_id: 32,
            background_active: false,
          },
        });
      });
      expect(screen.queryByText('后台运行中')).not.toBeInTheDocument();
    });

    it('does not coerce a malformed task-channel marker to false', () => {
      const task = makeTask({ id: 33, background_active: false });
      render(<ChatView task={task} projects={projects} onBack={onBack} />);

      act(() => {
        capturedOnMessage?.({
          channel: 'task:33',
          data: {
            event_type: 'background_activity',
            background_active: true,
          },
        });
      });
      expect(screen.getByText('后台运行中')).toBeInTheDocument();

      act(() => {
        capturedOnMessage?.({
          channel: 'task:33',
          data: {
            event_type: 'background_activity',
          },
        });
      });
      expect(screen.getByText('后台运行中')).toBeInTheDocument();
    });

    it('keeps a WS false marker across a stale polling prop that rebounds true', () => {
      const task = makeTask({
        id: 36,
        status: 'completed',
        background_active: false,
      });
      const { rerender } = render(
        <ChatView task={task} projects={projects} onBack={onBack} />,
      );

      act(() => {
        capturedOnMessage?.({
          channel: 'tasks',
          data: {
            event: 'background_activity',
            task_id: 36,
            background_active: false,
          },
        });
      });

      // Models a request started before the WS false event and resolved after
      // it with the old durable marker.
      rerender(
        <ChatView
          task={{ ...task, background_active: true }}
          projects={projects}
          onBack={onBack}
        />,
      );

      expect(screen.queryByText('后台运行中')).not.toBeInTheDocument();
      expect(screen.queryByTitle('Interrupt session')).not.toBeInTheDocument();
    });

    it('expires a stale WS status even when the polled status never changes', () => {
      vi.useFakeTimers();
      try {
        const task = makeTask({
          id: 37,
          status: 'completed',
          background_active: false,
        });
        render(<ChatView task={task} projects={projects} onBack={onBack} />);

        act(() => {
          capturedOnMessage?.({
            channel: 'tasks',
            data: {
              event: 'status_change',
              task_id: 37,
              new_status: 'executing',
              background_active: false,
            },
          });
        });
        expect(screen.getByTitle('Interrupt session')).toBeInTheDocument();

        act(() => {
          vi.advanceTimersByTime(7001);
        });
        expect(screen.queryByTitle('Interrupt session')).not.toBeInTheDocument();
      } finally {
        vi.useRealTimers();
      }
    });

    it('treats an ownerless background tail as processing and keeps Interrupt usable', async () => {
      const task = makeTask({
        id: 34,
        status: 'completed',
        background_active: true,
      });
      const { rerender } = render(
        <ChatView task={task} projects={projects} onBack={onBack} />,
      );

      const textarea = screen.getByPlaceholderText(/Type next message to queue/i);
      fireEvent.change(textarea, { target: { value: 'wait behind background work' } });
      expect(screen.getByTitle(/Add to queue/)).toBeInTheDocument();

      const interrupt = screen.getByTitle('Interrupt session');
      expect(interrupt).toBeEnabled();
      await userEvent.click(interrupt);
      await waitFor(() => {
        expect(api.stopTaskSession).toHaveBeenCalledWith(34);
      });

      rerender(
        <ChatView
          task={{ ...task, background_active: false }}
          projects={projects}
          onBack={onBack}
        />,
      );
      expect(screen.queryByTitle('Interrupt session')).not.toBeInTheDocument();
      expect(screen.getByTitle(/Send \(Ctrl\+Enter\)/)).toBeInTheDocument();
    });

    it('clears thinking immediately after an authoritative Interrupt response', async () => {
      const task = makeTask({ id: 38, status: 'executing' });
      render(
        <ChatView
          task={task}
          projects={projects}
          onBack={onBack}
          onTaskUpdated={onTaskUpdated}
        />,
      );

      expect(screen.getByText('Claude is thinking...')).toBeInTheDocument();
      await userEvent.click(screen.getByTitle('Interrupt session'));

      await waitFor(() => {
        expect(screen.queryByText('Claude is thinking...')).not.toBeInTheDocument();
      });
      expect(screen.queryByTitle('Interrupt session')).not.toBeInTheDocument();
      expect(onTaskUpdated).toHaveBeenCalled();
    });

    it('keeps thinking and exposes the backend reason when Interrupt fails', async () => {
      vi.mocked(api.stopTaskSession).mockRejectedValueOnce(
        new Error('Task process cleanup could not be confirmed for instance(s): 9, 10'),
      );
      const task = makeTask({ id: 39, status: 'executing' });
      render(<ChatView task={task} projects={projects} onBack={onBack} />);

      await userEvent.click(screen.getByTitle('Interrupt session'));

      expect(await screen.findByText(
        'Interrupt failed: Task process cleanup could not be confirmed for instance(s): 9, 10',
      )).toBeInTheDocument();
      expect(screen.getByText('Claude is thinking...')).toBeInTheDocument();
      expect(screen.getByTitle('Interrupt session')).toBeInTheDocument();
    });

    it('treats an already-finished stop race as a resolved Interrupt', async () => {
      const sendConflict = Object.assign(new Error('currently being processed'), {
        status: 409,
        detail: 'Task is currently being processed',
      });
      const noSession = Object.assign(new Error('No running session found for this task'), {
        status: 400,
        detail: 'No running session found for this task',
      });
      (api.sendTaskChat as ReturnType<typeof vi.fn>).mockRejectedValueOnce(sendConflict);
      (api.stopTaskSession as ReturnType<typeof vi.fn>).mockRejectedValueOnce(noSession);
      const task = makeTask({ id: 39, status: 'completed' });
      render(<ChatView task={task} projects={projects} onBack={onBack} />);

      await userEvent.type(screen.getByRole('textbox'), 'follow up');
      await userEvent.click(screen.getByTitle(/Send \(Ctrl\+Enter\)/));
      await userEvent.click(await screen.findByTitle('Interrupt session'));

      expect(await screen.findByText(
        'Interrupt: the session had already finished before the stop request arrived.',
      )).toBeInTheDocument();
      expect(screen.queryByTitle('Interrupt session')).not.toBeInTheDocument();
    });
    it('does not finish or dequeue at terminal/process_exit until the marker clears', async () => {
      const task = makeTask({
        id: 35,
        status: 'executing',
        background_active: false,
      });
      render(<ChatView task={task} projects={projects} onBack={onBack} />);

      act(() => {
        capturedOnMessage?.({
          channel: 'task:35',
          data: {
            event_type: 'user_message',
            content: 'foreground request',
          },
        });
      });
      expect(screen.getByText('Claude is thinking...')).toBeInTheDocument();

      const textarea = screen.getByPlaceholderText(/Type next message to queue/i);
      fireEvent.change(textarea, { target: { value: 'queued follow-up' } });
      await userEvent.click(screen.getByTitle(/Add to queue/));
      expect(screen.getByText('Queued messages (1)')).toBeInTheDocument();

      act(() => {
        capturedOnMessage?.({
          channel: 'tasks',
          data: {
            event: 'status_change',
            task_id: 35,
            new_status: 'completed',
            background_active: true,
          },
        });
      });
      expect(screen.getByText('后台运行中')).toBeInTheDocument();
      expect(screen.getByText('Claude is thinking...')).toBeInTheDocument();
      expect(screen.getByText('Queued messages (1)')).toBeInTheDocument();
      expect(api.sendTaskChat).not.toHaveBeenCalled();

      act(() => {
        capturedOnMessage?.({
          channel: 'task:35',
          data: { event_type: 'process_exit', exit_code: 0 },
        });
      });
      await act(async () => {
        await new Promise((resolve) => setTimeout(resolve, 650));
      });
      expect(screen.getByText('Claude is thinking...')).toBeInTheDocument();
      expect(screen.getByText('Queued messages (1)')).toBeInTheDocument();
      expect(api.sendTaskChat).not.toHaveBeenCalled();

      act(() => {
        capturedOnMessage?.({
          channel: 'tasks',
          data: {
            event: 'background_activity',
            task_id: 35,
            background_active: false,
          },
        });
      });

      await waitFor(
        () => expect(api.sendTaskChat).toHaveBeenCalled(),
        { timeout: 2000 },
      );
      expect(
        (api.sendTaskChat as ReturnType<typeof vi.fn>).mock.calls[0][1],
      ).toBe('queued follow-up');
      await waitFor(() => {
        expect(screen.queryByText('Queued messages (1)')).not.toBeInTheDocument();
      });
    });
  });

  describe('follow-up 循环审查模式', () => {
    it('keeps the right-side test entry visible and opens the standby page without runs', async () => {
      const task = makeTask({
        id: 406,
        status: 'completed',
        provider: 'codex',
        model: 'gpt-5.6-sol',
      });
      vi.mocked(api.listTestRuns).mockResolvedValue([]);
      render(
        <ChatView
          task={task}
          projects={projects}
          onBack={onBack}
          onTaskUpdated={onTaskUpdated}
        />,
      );

      const testPanelButton = screen.getByRole('button', { name: 'Toggle Frontend Review panel' });
      expect(testPanelButton).toBeInTheDocument();
      await userEvent.click(testPanelButton);
      expect(await screen.findByText('尚未启动前端测试')).toBeInTheDocument();
    });

    it('普通对话识别为前端验收后立即打开右栏等待新的浏览器 run', async () => {
      const task = makeTask({
        id: 407,
        status: 'completed',
        provider: 'codex',
        model: 'gpt-5.6-sol',
      });
      vi.mocked(api.sendTaskChat).mockResolvedValueOnce({
        ok: true,
        queued: true,
        session_id: task.session_id!,
        workspace_review_expected: true,
        workspace_review_baseline_run_id: 'older-workspace-run',
      });
      render(
        <ChatView
          task={task}
          projects={projects}
          onBack={onBack}
          onTaskUpdated={onTaskUpdated}
        />,
      );

      const input = screen.getByPlaceholderText(/follow-up message/i);
      await userEvent.type(input, '审查一下pr99分支的前端内容是否实现');
      await userEvent.click(screen.getByTitle('Send (Ctrl+Enter)'));

      await waitFor(() => expect(api.sendTaskChat).toHaveBeenCalled());
      expect(await screen.findByText('正在创建新的前端测试')).toBeInTheDocument();
      expect(screen.getByLabelText('Frontend Review progress')).toBeInTheDocument();
    });

    it('从已完成 Task 单次黑盒审查当前分支，不发送普通 follow-up', async () => {
      const task = makeTask({
        id: 408,
        status: 'completed',
        provider: 'codex',
        model: 'gpt-5.6-sol',
      });
      const workspaceRun: WorkspaceReviewRun = {
        id: 'workspace-review-1',
        task_id: 408,
        project_id: 1,
        agent_task_id: null,
        browser_review_job_id: null,
        mode: 'review_only',
        profile: 'standard',
        goal: '验证设置页保存和窄屏布局',
        status: 'preparing',
        stage: 'validating_workspace',
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
        evidence_archive_state: 'staging',
        evidence_archive_error: null,
        created_at: '2026-08-06T00:00:00Z',
        started_at: null,
        completed_at: null,
      };
      const startedRun: TestHarnessRun = {
        id: 'test-harness-run-1',
        task_id: 408,
        project_id: 1,
        workspace_review_run_id: workspaceRun.id,
        browser_review_job_id: null,
        agent_task_id: null,
        target_kind: 'current_workspace',
        target: {},
        test_plan: { objective: workspaceRun.goal },
        runtime: { context_policy: 'isolated_black_box_v1' },
        request_fingerprint: 'f'.repeat(64),
        parent_run_id: null,
        root_run_id: 'test-harness-run-1',
        attempt_number: 1,
        status: 'preparing_environment',
        stage: 'validating_workspace',
        verdict: null,
        source_git_head: workspaceRun.git_head,
        source_fingerprint: workspaceRun.workspace_fingerprint,
        stale: false,
        report: null,
        error: null,
        cleanup_status: 'pending',
        cleanup_error: null,
        created_at: workspaceRun.created_at,
        started_at: null,
        completed_at: null,
        attempts: [],
        events: [],
        evidence: [],
        findings: [],
        workspace_review: workspaceRun,
        browser_review: null,
      };
      vi.mocked(api.startTestRun).mockResolvedValueOnce(startedRun);
      render(
        <ChatView
          task={task}
          projects={projects}
          onBack={onBack}
          onTaskUpdated={onTaskUpdated}
        />,
      );

      const modeButton = screen.getByRole('button', { name: '单次审查当前分支' });
      await waitFor(() => expect(modeButton).toBeEnabled());
      await userEvent.click(modeButton);
      const input = screen.getByPlaceholderText(/无需提供 URL/);
      await userEvent.type(input, '验证设置页保存和窄屏布局');
      await userEvent.click(screen.getByTitle('启动单次审查 (Ctrl+Enter)'));

      await waitFor(() => expect(api.startTestRun).toHaveBeenCalledWith(
        408,
        {
          target_kind: 'current_workspace',
          target: {},
          goal: '验证设置页保存和窄屏布局',
          profile: 'standard',
          allow_actions: true,
          browser_channel: 'chromium',
          viewport_width: 1440,
          viewport_height: 900,
        },
      ));
      expect(api.sendTaskChat).not.toHaveBeenCalled();
      expect(api.startFrontendReviewGoal).not.toHaveBeenCalled();
      expect(await screen.findByText('Test Harness · 当前工作区')).toBeInTheDocument();
      expect(screen.getAllByText('正在校验本地仓库').length).toBeGreaterThan(0);
    });

    it('从已完成 Task 的输入工具栏启动同 session Goal，而不发送普通 follow-up', async () => {
      const task = makeTask({
        id: 409,
        status: 'completed',
        provider: 'codex',
        model: 'gpt-5.6-sol',
      });
      let resolveGoalStart!: (task: Task) => void;
      vi.mocked(api.startFrontendReviewGoal).mockReturnValueOnce(new Promise<Task>((resolve) => {
        resolveGoalStart = resolve;
      }));
      render(
        <ChatView
          task={task}
          projects={projects}
          onBack={onBack}
          onTaskUpdated={onTaskUpdated}
        />,
      );

      const modeButton = screen.getByRole('button', { name: '循环审查' });
      await waitFor(() => expect(modeButton).toBeEnabled());
      await userEvent.click(modeButton);
      expect(modeButton).toHaveAttribute('aria-pressed', 'true');
      expect(screen.getByText(/当前 Task\/session/)).toBeInTheDocument();

      const input = screen.getByPlaceholderText(/描述这次要循环审查/);
      await userEvent.type(input, '审查设置页桌面和窄屏，修复后重新验证');
      await userEvent.click(screen.getByTitle('启动循环审查 (Ctrl+Enter)'));

      expect(await screen.findByText('Goal 循环审查正在启动')).toBeInTheDocument();
      expect(screen.getAllByText('审查设置页桌面和窄屏，修复后重新验证').length).toBeGreaterThan(0);
      expect(screen.getByText(/审查报告不会结束本轮/)).toBeInTheDocument();
      await waitFor(() => expect(api.startFrontendReviewGoal).toHaveBeenCalledWith(
        409,
        {
          message: '审查设置页桌面和窄屏，修复后重新验证',
          file_paths: undefined,
          secret_ids: undefined,
          profile: 'standard',
          max_iterations: 5,
          expected_routing: {
            provider: 'codex',
            model: 'gpt-5.6-sol',
            codex_service_tier: 'default',
          },
        },
      ));
      expect(api.sendTaskChat).not.toHaveBeenCalled();
      await act(async () => {
        resolveGoalStart(makeTask({
          ...task,
          status: 'pending',
          mode: 'goal',
          goal_max_turns: 5,
          metadata_: {
            frontend_review: {
              mode: 'goal',
              profile: 'standard',
              max_iterations: 5,
            },
          },
        }));
      });
      await waitFor(() => expect(onTaskUpdated).toHaveBeenCalled());
      await waitFor(() => {
        expect(screen.getByRole('button', { name: '循环审查' }))
          .toHaveAttribute('aria-pressed', 'false');
      });

      act(() => {
        capturedOnMessage?.({
          channel: 'task:409',
          data: {
            event_type: 'tool_use',
            role: 'assistant',
            tool_name: 'ccm_workspace_review.test_current_changes',
            tool_input: JSON.stringify({ goal: '修改后重新检查设置页键盘流程' }),
            item_id: 'workspace-review-start-1',
            timestamp: '2026-08-06T02:00:00Z',
          },
        });
      });
      expect(await screen.findByText('正在创建本轮浏览器复查')).toBeInTheDocument();
      expect(screen.getByText('修改后重新检查设置页键盘流程')).toBeInTheDocument();
    });

    it('Goal 已终态时不再显示运行提示，后续输入保持普通对话', async () => {
      const task = makeTask({
        id: 411,
        status: 'completed',
        mode: 'goal',
        goal_turns_used: 2,
        goal_max_turns: 5,
        metadata_: {
          frontend_review: {
            mode: 'goal',
            profile: 'standard',
            max_iterations: 5,
          },
          frontend_review_activation: {
            message: '旧版残留的循环审查',
          },
        },
      });
      render(
        <ChatView
          task={task}
          projects={projects}
          onBack={onBack}
        />,
      );

      expect(screen.queryByText(/Goal 审查 · 第/)).not.toBeInTheDocument();
      expect(screen.queryByText(/Goal Agent 第/)).not.toBeInTheDocument();
      const input = screen.getByPlaceholderText(/follow-up message/i);
      await userEvent.type(input, 'Goal 已结束后的普通问题');
      await userEvent.click(screen.getByTitle('Send (Ctrl+Enter)'));
      await waitFor(() => expect(api.sendTaskChat).toHaveBeenCalled());
      expect(api.startFrontendReviewGoal).not.toHaveBeenCalled();
    });

    it('终态 status_change 不会被随后到达的 process_exit 恢复成 thinking', async () => {
      render(
        <ChatView
          task={makeTask({
            id: 412,
            status: 'executing',
            mode: 'goal',
            metadata_: {
              frontend_review: {
                mode: 'goal',
                profile: 'standard',
                max_iterations: 5,
              },
            },
          })}
          projects={projects}
          onBack={onBack}
        />,
      );
      expect(screen.getByText(/Goal Agent 第 1 轮正在执行/)).toBeInTheDocument();

      act(() => {
        capturedOnMessage?.({
          channel: 'tasks',
          data: {
            event: 'status_change',
            task_id: 412,
            new_status: 'completed',
            background_active: false,
          },
        });
      });
      expect(screen.queryByText(/Goal Agent 第/)).not.toBeInTheDocument();

      act(() => {
        capturedOnMessage?.({
          channel: 'task:412',
          data: { event_type: 'process_exit', exit_code: 0 },
        });
      });
      await act(async () => {
        await new Promise((resolve) => setTimeout(resolve, 650));
      });
      expect(screen.queryByText(/Goal Agent 第/)).not.toBeInTheDocument();
      expect(screen.queryByText(/is thinking/)).not.toBeInTheDocument();
    });

    it('Task 运行时显示但禁用循环审查按钮', () => {
      render(
        <ChatView
          task={makeTask({ id: 410, status: 'executing' })}
          projects={projects}
          onBack={onBack}
        />,
      );

      expect(screen.getByRole('button', { name: '循环审查' })).toBeDisabled();
    });

    it('未确认本地 Git 仓库时禁用并显示后端原因', async () => {
      (api.getFrontendReviewGoalCapabilities as ReturnType<typeof vi.fn>).mockResolvedValueOnce({
        available: false,
        reason: 'Task 工作目录不是有效的 Git 仓库或 worktree',
        repo_path: null,
      });
      render(
        <ChatView
          task={makeTask({ id: 411, status: 'completed' })}
          projects={projects}
          onBack={onBack}
        />,
      );

      const modeButton = screen.getByRole('button', { name: '循环审查' });
      await waitFor(() => expect(modeButton).toBeDisabled());
      await waitFor(() => expect(modeButton).toHaveAttribute(
        'title',
        'Task 工作目录不是有效的 Git 仓库或 worktree',
      ));
      expect(api.startFrontendReviewGoal).not.toHaveBeenCalled();
    });
  });

  describe('queued message attachment editing', () => {
    beforeEach(() => {
      localStorage.clear();
      vi.mocked(api.sendTaskChat).mockResolvedValue({});
    });

    it('moves merged queue attachments into the composer and sends their existing path', async () => {
      const task = makeTask({ id: 410, status: 'executing' });
      const attachment = makeUpload('merge-report', 'report.md');
      localStorage.setItem(
        `ccm-chat-queue-${task.id}`,
        JSON.stringify([{ text: 'review the report', uploadResults: [attachment] }]),
      );
      const { rerender } = render(
        <ChatView task={task} projects={projects} onBack={onBack} />,
      );

      await userEvent.click(screen.getByRole('button', { name: 'Merge' }));

      expect(screen.queryByText('Queued messages (1)')).not.toBeInTheDocument();
      expect(screen.getByRole('textbox')).toHaveValue('review the report');
      expect(screen.getByText('report.md')).toBeInTheDocument();
      await waitFor(() => {
        expect(JSON.parse(
          localStorage.getItem(`ccm-chat-draft-uploads-${task.id}`) || '[]',
        )).toEqual([attachment]);
      });

      rerender(
        <ChatView
          task={{ ...task, status: 'completed' }}
          projects={projects}
          onBack={onBack}
        />,
      );
      await userEvent.click(await screen.findByTitle(/Send \(Ctrl\+Enter\)/));

      await waitFor(() => {
        expect(api.sendTaskChat).toHaveBeenCalledWith(
          task.id,
          'review the report',
          [attachment.path],
          undefined,
          null,
          {
            provider: 'claude',
            model: null,
            codex_service_tier: 'default',
          },
        );
      });
    });

    it('persists merged attachments with the text draft across a remount', async () => {
      const task = makeTask({ id: 411, status: 'executing' });
      const attachment = makeUpload('draft-notes', 'notes.txt');
      localStorage.setItem(
        `ccm-chat-queue-${task.id}`,
        JSON.stringify([{ text: 'continue later', uploadResults: [attachment] }]),
      );
      const first = render(
        <ChatView task={task} projects={projects} onBack={onBack} />,
      );

      await userEvent.click(screen.getByRole('button', { name: 'Merge' }));
      await waitFor(() => {
        expect(localStorage.getItem(`ccm-chat-draft-uploads-${task.id}`)).not.toBeNull();
      });
      first.unmount();

      render(<ChatView task={task} projects={projects} onBack={onBack} />);

      expect(screen.getByRole('textbox')).toHaveValue('continue later');
      expect(screen.getByText('notes.txt')).toBeInTheDocument();
      expect(screen.queryByText('Queued messages (1)')).not.toBeInTheDocument();
    });

    it('restores attachments when editing one queued message', async () => {
      const task = makeTask({ id: 412, status: 'executing' });
      const attachment = makeUpload('edit-evidence', 'evidence.txt');
      localStorage.setItem(
        `ccm-chat-queue-${task.id}`,
        JSON.stringify([{ text: 'edit this', uploadResults: [attachment] }]),
      );
      render(<ChatView task={task} projects={projects} onBack={onBack} />);

      await userEvent.click(screen.getByTitle('Edit in input'));

      expect(screen.getByRole('textbox')).toHaveValue('edit this');
      expect(screen.getByText('evidence.txt')).toBeInTheDocument();
      expect(screen.queryByText('Queued messages (1)')).not.toBeInTheDocument();
    });

    it('keeps merged attachments when the edited message is queued again', async () => {
      const task = makeTask({ id: 417, status: 'executing' });
      const attachment = makeUpload('requeue-upload', 'requeue.txt');
      localStorage.setItem(
        `ccm-chat-queue-${task.id}`,
        JSON.stringify([{ text: 'before edit', uploadResults: [attachment] }]),
      );
      render(<ChatView task={task} projects={projects} onBack={onBack} />);

      await userEvent.click(screen.getByRole('button', { name: 'Merge' }));
      const textbox = screen.getByRole('textbox');
      await userEvent.clear(textbox);
      await userEvent.type(textbox, 'after edit');
      await userEvent.click(screen.getByTitle(/Add to queue/));

      expect(screen.getByText('Queued messages (1)')).toBeInTheDocument();
      expect(screen.getByText('after edit')).toBeInTheDocument();
      await waitFor(() => {
        expect(JSON.parse(
          localStorage.getItem(`ccm-chat-queue-${task.id}`) || '[]',
        )).toEqual([{
          text: 'after edit',
          uploadResults: [attachment],
        }]);
      });
    });

    it('deduplicates attachments while preserving queued message order', async () => {
      const task = makeTask({ id: 413, status: 'executing' });
      const repeated = makeUpload('same-upload', 'same.txt');
      const other = makeUpload('other-upload', 'other.txt');
      localStorage.setItem(
        `ccm-chat-queue-${task.id}`,
        JSON.stringify([
          { text: 'first', uploadResults: [repeated] },
          { text: 'second', uploadResults: [repeated, other] },
        ]),
      );
      render(<ChatView task={task} projects={projects} onBack={onBack} />);

      await userEvent.click(screen.getByRole('button', { name: 'Merge' }));

      expect(screen.getByRole('textbox')).toHaveValue('first\n\nsecond');
      expect(screen.getAllByText('same.txt')).toHaveLength(1);
      expect(screen.getAllByText('other.txt')).toHaveLength(1);
    });

    it('rejects an over-limit merge atomically and keeps the queue intact', async () => {
      const task = makeTask({ id: 414, status: 'executing' });
      const attachments = Array.from(
        { length: 11 },
        (_, index) => makeUpload(`limit-${index}`, `limit-${index}.txt`),
      );
      localStorage.setItem(
        `ccm-chat-queue-${task.id}`,
        JSON.stringify([
          { text: 'first batch', uploadResults: attachments.slice(0, 6) },
          { text: 'second batch', uploadResults: attachments.slice(6) },
        ]),
      );
      render(<ChatView task={task} projects={projects} onBack={onBack} />);

      await userEvent.click(screen.getByRole('button', { name: 'Merge' }));

      expect(screen.getByText(/合并后将有 11 个附件/)).toBeInTheDocument();
      expect(screen.getByText('Queued messages (2)')).toBeInTheDocument();
      expect(screen.getByRole('textbox')).toHaveValue('');
      expect(screen.queryByText('limit-0.txt')).not.toBeInTheDocument();
    });

    it('restores a merged attachment when sending fails', async () => {
      const task = makeTask({ id: 415, status: 'executing' });
      const attachment = makeUpload('retry-upload', 'retry.txt');
      localStorage.setItem(
        `ccm-chat-queue-${task.id}`,
        JSON.stringify([{ text: 'retry this', uploadResults: [attachment] }]),
      );
      vi.mocked(api.sendTaskChat).mockRejectedValueOnce(new Error('send failed'));
      const { rerender } = render(
        <ChatView task={task} projects={projects} onBack={onBack} />,
      );

      await userEvent.click(screen.getByRole('button', { name: 'Merge' }));
      rerender(
        <ChatView
          task={{ ...task, status: 'completed' }}
          projects={projects}
          onBack={onBack}
        />,
      );
      await userEvent.click(await screen.findByTitle(/Send \(Ctrl\+Enter\)/));

      expect(await screen.findByText(/send failed/)).toBeInTheDocument();
      expect(screen.getByRole('textbox')).toHaveValue('retry this');
      expect(screen.getByText('retry.txt')).toBeInTheDocument();
      await waitFor(() => {
        expect(localStorage.getItem(`ccm-chat-draft-uploads-${task.id}`)).not.toBeNull();
      });
    });

    it('keeps the existing text-only merge behavior', async () => {
      const task = makeTask({ id: 416, status: 'executing' });
      localStorage.setItem(
        `ccm-chat-queue-${task.id}`,
        JSON.stringify([{ text: 'plain follow-up' }]),
      );
      render(<ChatView task={task} projects={projects} onBack={onBack} />);

      await userEvent.click(screen.getByRole('button', { name: 'Merge' }));

      expect(screen.getByRole('textbox')).toHaveValue('plain follow-up');
      expect(screen.queryByText('Queued messages (1)')).not.toBeInTheDocument();
    });
  });

  describe('Live turn injection', () => {
    it('steers an executing Codex turn even when Claude PTY is off', async () => {
      const task = makeTask({
        provider: 'codex',
        status: 'executing',
        worker_id: null,
        shared_from_id: null,
      });
      render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);

      const toggle = await screen.findByTitle(/Codex turn\/steer.*插入运行中的 turn/);
      await userEvent.click(toggle);
      await userEvent.type(screen.getByRole('textbox'), 'change direction');
      await userEvent.click(screen.getByTitle('注入到运行中的 turn (Ctrl+Enter)'));

      await waitFor(() => {
        expect(api.injectTaskMessage).toHaveBeenCalledWith(
          task.id,
          'change direction',
          {
            provider: 'codex',
            model: null,
            codex_service_tier: 'default',
          },
          undefined,
        );
      });
      expect(api.sendTaskChat).not.toHaveBeenCalled();
    });

    it('uploads images and files and injects their exact server metadata into the active turn', async () => {
      const task = makeTask({
        provider: 'codex',
        status: 'executing',
        worker_id: null,
        shared_from_id: null,
      });
      vi.mocked(api.uploadImages).mockImplementation(async ([file]) => [{
        id: `upload-${file.name}`,
        filename: file.name,
        path: `/srv/uploads/${file.name}`,
        url: `/api/uploads/${file.name}`,
        is_image: file.type.startsWith('image/'),
      }]);
      vi.mocked(api.injectTaskMessage).mockResolvedValue({
        ok: true,
        injected: true,
        attachment_count: 2,
      });
      const { container } = render(
        <ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />,
      );

      await userEvent.click(await screen.findByTitle(/Codex turn\/steer.*插入运行中的 turn/));
      const picker = container.querySelector<HTMLInputElement>('input[type="file"]')!;
      await userEvent.upload(picker, [
        new File(['png'], 'diagram.png', { type: 'image/png' }),
        new File(['notes'], 'notes.txt', { type: 'text/plain' }),
      ]);
      await waitFor(() => expect(api.uploadImages).toHaveBeenCalledTimes(2));
      await screen.findByText('notes.txt');
      await userEvent.click(screen.getByTitle('注入到运行中的 turn (Ctrl+Enter)'));

      await waitFor(() => {
        expect(api.getInjectCapabilities).toHaveBeenCalledWith(task.id);
        expect(api.injectTaskMessage).toHaveBeenCalledWith(
          task.id,
          '(files attached)',
          {
            provider: 'codex',
            model: null,
            codex_service_tier: 'default',
          },
          {
            file_paths: [
              '/srv/uploads/diagram.png',
              '/srv/uploads/notes.txt',
            ],
            image_paths: ['/srv/uploads/diagram.png'],
            attachments: [
              {
                url: '/api/uploads/diagram.png',
                name: 'diagram.png',
                is_image: true,
              },
              {
                url: '/api/uploads/notes.txt',
                name: 'notes.txt',
                is_image: false,
              },
            ],
          },
        );
      });
      expect(api.sendTaskChat).not.toHaveBeenCalled();
      await waitFor(() => {
        expect(screen.queryByText('notes.txt')).not.toBeInTheDocument();
      });
    });

    it('does not contact an old inject endpoint before attachment capability is confirmed', async () => {
      const task = makeTask({
        provider: 'codex',
        status: 'executing',
        worker_id: null,
        shared_from_id: null,
      });
      vi.mocked(api.uploadImages).mockResolvedValue([{
        id: 'upload-evidence',
        filename: 'evidence.txt',
        path: '/srv/uploads/evidence.txt',
        url: '/api/uploads/evidence.txt',
        is_image: false,
      }]);
      vi.mocked(api.getInjectCapabilities).mockRejectedValue(
        new Error('HTTP 404'),
      );
      const { container } = render(
        <ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />,
      );

      await userEvent.click(await screen.findByTitle(/Codex turn\/steer.*插入运行中的 turn/));
      await userEvent.upload(
        container.querySelector<HTMLInputElement>('input[type="file"]')!,
        new File(['evidence'], 'evidence.txt', { type: 'text/plain' }),
      );
      await screen.findByText('evidence.txt');
      await userEvent.click(screen.getByTitle('注入到运行中的 turn (Ctrl+Enter)'));

      expect(await screen.findByText(/HTTP 404/)).toHaveTextContent(
        '消息和附件已保留',
      );
      expect(api.injectTaskMessage).not.toHaveBeenCalled();
      expect(screen.getByText('evidence.txt')).toBeInTheDocument();
    });

    it('keeps attachments unless the server confirms the exact attachment count', async () => {
      const task = makeTask({
        provider: 'codex',
        status: 'executing',
        worker_id: null,
        shared_from_id: null,
      });
      vi.mocked(api.uploadImages).mockResolvedValue([{
        id: 'upload-evidence',
        filename: 'evidence.txt',
        path: '/srv/uploads/evidence.txt',
        url: '/api/uploads/evidence.txt',
        is_image: false,
      }]);
      vi.mocked(api.injectTaskMessage).mockResolvedValue({
        ok: true,
        injected: true,
      });
      const { container } = render(
        <ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />,
      );

      await userEvent.click(await screen.findByTitle(/Codex turn\/steer.*插入运行中的 turn/));
      await userEvent.upload(
        container.querySelector<HTMLInputElement>('input[type="file"]')!,
        new File(['evidence'], 'evidence.txt', { type: 'text/plain' }),
      );
      await screen.findByText('evidence.txt');
      await userEvent.click(screen.getByTitle('注入到运行中的 turn (Ctrl+Enter)'));

      expect(await screen.findByText(/没有确认全部附件均已注入/)).toHaveTextContent(
        '消息和附件已保留',
      );
      expect(screen.getByText('evidence.txt')).toBeInTheDocument();
    });

    it('freezes composer edits while an injection request is in flight', async () => {
      const task = makeTask({
        provider: 'codex',
        status: 'executing',
        worker_id: null,
        shared_from_id: null,
      });
      let resolveInjection!: (value: {
        ok: boolean;
        injected: boolean;
      }) => void;
      vi.mocked(api.injectTaskMessage).mockImplementation(
        () => new Promise((resolve) => {
          resolveInjection = resolve;
        }),
      );
      render(
        <ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />,
      );

      await userEvent.click(await screen.findByTitle(/Codex turn\/steer.*插入运行中的 turn/));
      const textbox = screen.getByRole('textbox');
      await userEvent.type(textbox, 'snapshot');
      await userEvent.click(screen.getByTitle('注入到运行中的 turn (Ctrl+Enter)'));
      await waitFor(() => expect(api.injectTaskMessage).toHaveBeenCalled());

      expect(textbox).toBeDisabled();
      expect(screen.getByTitle('Attach files')).toBeDisabled();
      expect(screen.getByTitle(/注入模式已开启/)).toBeDisabled();

      await act(async () => {
        resolveInjection({ ok: true, injected: true });
      });
      await waitFor(() => expect(textbox).not.toBeDisabled());
      expect(textbox).toHaveValue('');
    });

    it('keeps the text and uploaded attachment when the server does not confirm injection', async () => {
      const task = makeTask({
        provider: 'codex',
        status: 'executing',
        worker_id: null,
        shared_from_id: null,
      });
      vi.mocked(api.uploadImages).mockResolvedValue([{
        id: 'upload-evidence',
        filename: 'evidence.txt',
        path: '/srv/uploads/evidence.txt',
        url: '/api/uploads/evidence.txt',
        is_image: false,
      }]);
      vi.mocked(api.injectTaskMessage).mockResolvedValue({
        ok: true,
        injected: false,
      });
      const { container } = render(
        <ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />,
      );

      await userEvent.click(await screen.findByTitle(/Codex turn\/steer.*插入运行中的 turn/));
      await userEvent.upload(
        container.querySelector<HTMLInputElement>('input[type="file"]')!,
        new File(['evidence'], 'evidence.txt', { type: 'text/plain' }),
      );
      await screen.findByText('evidence.txt');
      await userEvent.type(screen.getByRole('textbox'), '请结合附件继续');
      await userEvent.click(screen.getByTitle('注入到运行中的 turn (Ctrl+Enter)'));

      expect(await screen.findByText(/服务器没有确认消息已注入/)).toHaveTextContent(
        '消息和附件已保留',
      );
      expect(screen.getByRole('textbox')).toHaveValue('请结合附件继续');
      expect(screen.getByText('evidence.txt')).toBeInTheDocument();
      expect(api.sendTaskChat).not.toHaveBeenCalled();
    });

    it('does not silently drop a failed upload when injecting text', async () => {
      const task = makeTask({
        provider: 'codex',
        status: 'executing',
        worker_id: null,
        shared_from_id: null,
      });
      vi.mocked(api.uploadImages).mockRejectedValue(new Error('upload rejected'));
      const { container } = render(
        <ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />,
      );

      await userEvent.click(await screen.findByTitle(/Codex turn\/steer.*插入运行中的 turn/));
      await userEvent.upload(
        container.querySelector<HTMLInputElement>('input[type="file"]')!,
        new File(['bad'], 'broken.txt', { type: 'text/plain' }),
      );
      await screen.findByTitle('Click to retry');
      await userEvent.type(screen.getByRole('textbox'), '不要漏掉附件');
      fireEvent.keyDown(screen.getByRole('textbox'), { key: 'Enter', ctrlKey: true });

      expect(await screen.findByText('Retry or remove failed attachments before sending.')).toBeInTheDocument();
      expect(api.injectTaskMessage).not.toHaveBeenCalled();
      expect(screen.getByText('broken.txt')).toBeInTheDocument();
    });

    it('does not offer local injection for worker tasks', async () => {
      const task = makeTask({
        provider: 'codex',
        status: 'executing',
        worker_id: 7,
        shared_from_id: null,
      });
      render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);

      await waitFor(() => expect(api.getWorkerRuntimeSettings).toHaveBeenCalledWith(7));
      expect(screen.queryByTitle(/Codex turn\/steer/)).not.toBeInTheDocument();
    });
  });

  describe('Initial prompt bubble', () => {
    it('renders initial prompt as first bubble', async () => {
      const task = makeTask({ title: 'Has Title', description: 'Build a login page' });
      render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);

      expect(screen.getByText('— Initial Prompt —')).toBeInTheDocument();
      // Description appears in the initial prompt bubble (header shows title instead)
      expect(screen.getByText('Build a login page')).toBeInTheDocument();
    });

    it('shows timestamp on initial prompt bubble from task.created_at', async () => {
      const task = makeTask({ description: 'Hello', created_at: '2024-06-15T10:30:00Z' });
      const { container } = render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);

      // The initial prompt bubble should contain a MessageTimestamp span
      const initialPromptDiv = container.querySelector('[data-user-msg]')!;
      expect(initialPromptDiv).toBeInTheDocument();
      // Look for the timestamp span (text-[10px] is the MessageTimestamp class)
      const timestampSpan = initialPromptDiv.querySelector('span.select-none');
      expect(timestampSpan).toBeInTheDocument();
      expect(timestampSpan!.textContent).toBeTruthy();
    });

    it('does not show timestamp on initial prompt when created_at is missing', async () => {
      const task = makeTask({ description: 'Hello', created_at: '' });
      const { container } = render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);

      const initialPromptDiv = container.querySelector('[data-user-msg]')!;
      expect(initialPromptDiv).toBeInTheDocument();
      // No timestamp span should appear
      const timestampSpan = initialPromptDiv.querySelector('span.select-none');
      expect(timestampSpan).not.toBeInTheDocument();
    });

    it('does not render initial prompt bubble when description is null', async () => {
      const task = makeTask({ description: null });
      render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);

      expect(screen.queryByText('— Initial Prompt —')).not.toBeInTheDocument();
    });

    it('opens one toolbar picker and forks before a selected user message', async () => {
      const task = makeTask({ id: 12, provider: 'codex', status: 'completed' });
      const anchor = {
        type: 'user_message' as const,
        id: 456,
        content: 'Try a different implementation',
        timestamp: '2024-01-01T00:01:00Z',
        attachments: [],
      };
      const forked = makeTask({
        id: 99,
        provider: 'codex',
        status: 'completed',
        session_id: 'fork-thread',
        metadata_: {
          fork_seed_message: anchor.content,
          fork_seed_log_id: anchor.id,
        },
      });
      const onTaskForked = vi.fn();
      (api.listForkAnchors as ReturnType<typeof vi.fn>).mockResolvedValue([anchor]);
      (api.forkTask as ReturnType<typeof vi.fn>).mockResolvedValue(forked);
      render(
        <ChatView
          task={task}
          projects={projects}
          onBack={onBack}
          onTaskForked={onTaskForked}
        />,
      );

      const forkButton = screen.getByLabelText('Fork Codex session');
      expect(forkButton).toHaveClass('h-8', 'w-8');
      expect(forkButton).toHaveClass('text-gray-500', 'hover:text-indigo-400');
      expect(forkButton).not.toHaveClass('text-indigo-400');
      await userEvent.click(forkButton);
      expect(await screen.findByText(anchor.content)).toBeInTheDocument();
      expect(api.listForkAnchors).toHaveBeenCalledWith(task.id);
      await userEvent.click(screen.getByText(anchor.content));
      await userEvent.click(screen.getByRole('button', { name: 'Create fork' }));

      await waitFor(() => {
        expect(api.forkTask).toHaveBeenCalledWith(
          12,
          { type: 'user_message', id: anchor.id },
          '',
        );
        expect(onTaskForked).toHaveBeenCalledWith(forked);
      });
    });

    it('can copy the complete latest Codex context without a seed message', async () => {
      const task = makeTask({ id: 14, provider: 'codex', status: 'completed' });
      const latest = {
        type: 'latest' as const,
        id: null,
        content: '完整复制当前上下文',
        timestamp: '2024-01-01T00:02:00Z',
        attachments: [],
      };
      const forked = makeTask({
        id: 100,
        provider: 'codex',
        session_id: 'full-copy-thread',
        metadata_: { fork_mode: 'full_copy' },
      });
      (api.listForkAnchors as ReturnType<typeof vi.fn>).mockResolvedValue([latest]);
      (api.forkTask as ReturnType<typeof vi.fn>).mockResolvedValue(forked);

      render(
        <ChatView
          task={task}
          projects={projects}
          onBack={onBack}
          onTaskForked={vi.fn()}
        />,
      );

      await userEvent.click(screen.getByLabelText('Fork Codex session'));
      await userEvent.click(await screen.findByText('完整复制当前上下文'));
      await userEvent.click(screen.getByRole('button', { name: 'Create fork' }));

      await waitFor(() => {
        expect(api.forkTask).toHaveBeenCalledWith(
          task.id,
          { type: 'latest' },
          '',
        );
      });
    });

    it('does not offer native fork actions for Claude sessions', () => {
      const task = makeTask({ provider: 'claude', status: 'completed' });
      render(<ChatView task={task} projects={projects} onBack={onBack} />);

      expect(screen.queryByLabelText('Fork Codex session')).not.toBeInTheDocument();
    });

    it('does not add fork actions to assistant messages', async () => {
      const task = makeTask({
        id: 13,
        description: null,
        provider: 'codex',
        status: 'completed',
      });
      const message: ChatMessage = {
        id: 456,
        task_id: task.id,
        event_type: 'message',
        role: 'assistant',
        content: 'Forkable answer',
        is_error: false,
        timestamp: '2024-01-01T00:01:00Z',
      };
      (api.getTaskChatHistory as ReturnType<typeof vi.fn>).mockResolvedValue([message]);
      render(<ChatView task={task} projects={projects} onBack={onBack} />);

      expect(await screen.findByText('Forkable answer')).toBeInTheDocument();
      expect(screen.getAllByLabelText('Fork Codex session')).toHaveLength(1);
    });

    it('prefills a fork seed only once when the child task opens', () => {
      const task = makeTask({
        id: 101,
        provider: 'codex',
        metadata_: {
          fork_seed_message: 'editable fork prompt',
          fork_seed_log_id: 456,
        },
      });
      const first = render(<ChatView task={task} projects={projects} onBack={onBack} />);
      expect(screen.getByRole('textbox')).toHaveValue('editable fork prompt');
      first.unmount();
      localStorage.removeItem(`ccm-chat-draft-${task.id}`);

      render(<ChatView task={task} projects={projects} onBack={onBack} />);
      expect(screen.getByRole('textbox')).toHaveValue('');
    });

    it('shows and sends fork seed attachments with the editable message', async () => {
      const task = makeTask({
        id: 102,
        provider: 'codex',
        session_id: 'fork-thread',
        metadata_: {
          fork_seed_message: 'inspect this file',
          fork_seed_log_id: 456,
          fork_seed_uploads: [{
            id: 'fork-seed-0',
            filename: 'evidence.txt',
            path: '/repo/uploads/evidence.txt',
            url: '/api/uploads/evidence.txt',
            is_image: false,
          }],
        },
      });
      render(<ChatView task={task} projects={projects} onBack={onBack} />);

      expect(screen.getByText('evidence.txt')).toBeInTheDocument();
      await userEvent.click(screen.getByTitle('Send (Ctrl+Enter)'));

      await waitFor(() => expect(api.sendTaskChat).toHaveBeenCalledWith(
        task.id,
        'inspect this file',
        ['/repo/uploads/evidence.txt'],
        undefined,
        null,
        {
          provider: 'codex',
          model: null,
          codex_service_tier: 'default',
        },
      ));
    });

    it('allows removing a fork seed attachment before sending', async () => {
      const task = makeTask({
        id: 103,
        provider: 'codex',
        session_id: 'fork-thread',
        metadata_: {
          fork_seed_message: 'text only now',
          fork_seed_uploads: [{
            id: 'fork-seed-0',
            filename: 'remove.txt',
            path: '/repo/uploads/remove.txt',
            url: '/api/uploads/remove.txt',
            is_image: false,
          }],
        },
      });
      render(<ChatView task={task} projects={projects} onBack={onBack} />);

      await userEvent.click(screen.getByRole('button', { name: 'Remove remove.txt' }));
      await userEvent.click(screen.getByTitle('Send (Ctrl+Enter)'));

      await waitFor(() => expect(api.sendTaskChat).toHaveBeenCalledWith(
        task.id,
        'text only now',
        undefined,
        undefined,
        null,
        {
          provider: 'codex',
          model: null,
          codex_service_tier: 'default',
        },
      ));
    });
  });

  describe('Title display', () => {
    it('shows title when set', () => {
      const task = makeTask({ title: 'Custom Title', description: 'Some prompt' });
      render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);

      expect(screen.getByText('Custom Title')).toBeInTheDocument();
    });

    it('falls back to description when title is empty', () => {
      const task = makeTask({ title: '', description: 'The prompt' });
      render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);

      // Description appears both in header (as title fallback) and in initial prompt bubble
      const matches = screen.getAllByText('The prompt');
      expect(matches.length).toBeGreaterThanOrEqual(2);
    });

    it('shows Untitled when both title and description are empty', () => {
      const task = makeTask({ title: '', description: null });
      render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);

      expect(screen.getByText('Untitled')).toBeInTheDocument();
    });
  });

  describe('Attention tag', () => {
    it('shows an existing attention tag in the chat header', () => {
      render(
        <ChatView
          task={makeTask({ attention_tag: '等待模型结束' })}
          projects={projects}
          onBack={onBack}
        />,
      );

      expect(screen.getByText('等待模型结束')).toBeInTheDocument();
      expect(screen.getByTitle('Edit attention tag')).toBeInTheDocument();
    });

    it('adds an attention tag from the chat header and refreshes the task', async () => {
      vi.mocked(api.updateTask).mockResolvedValueOnce(
        makeTask({ attention_tag: '需要人工确认' }),
      );
      render(
        <ChatView
          task={makeTask({ attention_tag: null })}
          projects={projects}
          onBack={onBack}
          onTaskUpdated={onTaskUpdated}
        />,
      );

      await userEvent.click(screen.getByTitle('Add attention tag'));
      await userEvent.type(screen.getByLabelText('Attention tag'), '需要人工确认');
      await userEvent.click(screen.getByTitle('Save attention tag'));

      await waitFor(() => {
        expect(api.updateTask).toHaveBeenCalledWith(1, {
          attention_tag: '需要人工确认',
        });
      });
      expect(onTaskUpdated).toHaveBeenCalledWith(
        expect.objectContaining({ attention_tag: '需要人工确认' }),
      );
    });
  });

  describe('Title editing', () => {
    it('enters edit mode on pencil click', async () => {
      const task = makeTask({ title: 'My Title' });
      render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);

      const editBtn = screen.getByTitle('Edit title');
      await userEvent.click(editBtn);

      expect(screen.getByPlaceholderText('Enter title...')).toBeInTheDocument();
    });

    it('saves title on Enter and calls onTaskUpdated', async () => {
      const task = makeTask({ id: 5, title: 'Old Title' });
      render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);

      await userEvent.click(screen.getByTitle('Edit title'));
      const input = screen.getByPlaceholderText('Enter title...');
      await userEvent.clear(input);
      await userEvent.type(input, 'New Title{Enter}');

      expect(api.updateTask).toHaveBeenCalledWith(5, { title: 'New Title' });
      expect(onTaskUpdated).toHaveBeenCalled();
    });

    it('cancels editing on Escape without saving', async () => {
      const task = makeTask({ title: 'Keep' });
      render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);

      await userEvent.click(screen.getByTitle('Edit title'));
      const input = screen.getByPlaceholderText('Enter title...');
      await userEvent.type(input, 'Nope');
      await userEvent.keyboard('{Escape}');

      expect(api.updateTask).not.toHaveBeenCalled();
    });
  });

  describe('Scroll container', () => {
    it('message container has min-h-0 for proper flex scrolling', () => {
      const task = makeTask();
      const { container } = render(<ChatView task={task} projects={projects} onBack={onBack} />);

      const messageContainer = container.querySelector('.overflow-y-auto.min-h-0');
      expect(messageContainer).toBeInTheDocument();
    });
  });

  describe('Textarea auto-resize', () => {
    it('textarea has ref and auto-resize classes', () => {
      const task = makeTask();
      const { container } = render(<ChatView task={task} projects={projects} onBack={onBack} />);

      const textarea = container.querySelector('textarea');
      expect(textarea).toBeInTheDocument();
      expect(textarea?.className).toContain('max-h-48');
      expect(textarea?.className).toContain('overflow-y-auto');
      expect(textarea?.className).toContain('resize-none');
    });

    it('adjusts height when input changes', async () => {
      const task = makeTask();
      const { container } = render(<ChatView task={task} projects={projects} onBack={onBack} />);

      const textarea = container.querySelector('textarea')!;
      // Mock scrollHeight
      Object.defineProperty(textarea, 'scrollHeight', { value: 80, configurable: true });

      await userEvent.type(textarea, 'Line 1\nLine 2\nLine 3');

      expect(textarea.style.height).toBe('80px');
    });

    it('resets height when input is cleared', async () => {
      const task = makeTask();
      const { container } = render(<ChatView task={task} projects={projects} onBack={onBack} />);

      const textarea = container.querySelector('textarea')!;
      Object.defineProperty(textarea, 'scrollHeight', { value: 80, configurable: true });

      await userEvent.type(textarea, 'Hello');
      expect(textarea.style.height).toBe('80px');

      // Simulate clearing input and smaller scrollHeight
      Object.defineProperty(textarea, 'scrollHeight', { value: 40, configurable: true });
      await userEvent.clear(textarea);
      expect(textarea.style.height).toBe('40px');
    });
  });

  describe('Back button', () => {
    it('calls onBack when back button clicked', async () => {
      const task = makeTask();
      render(<ChatView task={task} projects={projects} onBack={onBack} />);

      const backButtons = screen.getAllByRole('button');
      // First button is the back arrow
      await userEvent.click(backButtons[0]);
      expect(onBack).toHaveBeenCalled();
    });
  });

  describe('Chat history loading', () => {
    it('loads chat history on mount', async () => {
      const task = makeTask({ id: 42 });
      render(<ChatView task={task} projects={projects} onBack={onBack} />);

      await waitFor(() => {
        // compact=true, limit=HISTORY_PAGE_SIZE (paginated initial load)
        expect(api.getTaskChatHistory).toHaveBeenCalledWith(42, true, 200, 0, true);
      });
    });

    it('re-fetches chat history on WebSocket reconnect', async () => {
      const msgs: ChatMessage[] = [
        { id: 1, role: 'assistant', event_type: 'message', content: 'Hello', tool_name: null, tool_input: null, tool_output: null, is_error: false, loop_iteration: null, timestamp: '2024-01-01T00:00:00Z' },
      ];
      (api.getTaskChatHistory as ReturnType<typeof vi.fn>).mockResolvedValue(msgs);
      const task = makeTask({ id: 10 });
      render(<ChatView task={task} projects={projects} onBack={onBack} />);

      // Wait for initial load
      await waitFor(() => {
        expect(api.getTaskChatHistory).toHaveBeenCalledTimes(1);
      });

      // Simulate WebSocket reconnection
      capturedOnReconnect?.();

      await waitFor(() => {
        expect(api.getTaskChatHistory).toHaveBeenCalledTimes(2);
      });
    });

    it('passes onReconnect callback to useWebSocket', () => {
      const task = makeTask();
      render(<ChatView task={task} projects={projects} onBack={onBack} />);

      expect(capturedOnReconnect).toBeDefined();
      expect(typeof capturedOnReconnect).toBe('function');
      expect(capturedOnSubscribed).toBeDefined();
      expect(typeof capturedOnSubscribed).toBe('function');
    });

    it('keeps a live message when an older history snapshot finishes later', async () => {
      let resolveHistory!: (messages: ChatMessage[]) => void;
      (api.getTaskChatHistory as ReturnType<typeof vi.fn>).mockReturnValueOnce(
        new Promise<ChatMessage[]>((resolve) => { resolveHistory = resolve; }),
      );
      const task = makeTask({ id: 12, description: null });
      render(<ChatView task={task} projects={projects} onBack={onBack} />);

      act(() => {
        capturedOnMessage?.({
          channel: 'task:12',
          data: {
            event_type: 'message',
            role: 'assistant',
            content: 'live while snapshot is in flight',
            is_error: false,
          },
        });
      });
      expect(screen.getByText('live while snapshot is in flight')).toBeInTheDocument();

      await act(async () => { resolveHistory([]); });
      expect(screen.getByText('live while snapshot is in flight')).toBeInTheDocument();
    });

    it('pages from the HTTP boundary when an older persisted WS row is retained', async () => {
      const latest = Array.from({ length: 200 }, (_, index): ChatMessage => ({
        id: 210 + index,
        role: 'assistant',
        event_type: 'message',
        content: `latest-${index}`,
        tool_name: null,
        tool_input: null,
        tool_output: null,
        is_error: false,
        loop_iteration: null,
        timestamp: `2026-07-30T10:${String(index % 60).padStart(2, '0')}:00Z`,
        image_urls: null,
        attachments: null,
      }));
      const injected: ChatMessage = {
        id: 10,
        role: 'user',
        event_type: 'user_message',
        content: '[Admin] early steer',
        raw_content: 'early steer',
        tool_name: null,
        tool_input: null,
        tool_output: null,
        is_error: false,
        loop_iteration: null,
        timestamp: '2026-07-30T09:00:00Z',
        image_urls: null,
        attachments: null,
        source: 'inject',
      };
      vi.mocked(api.getTaskChatHistory)
        .mockResolvedValueOnce(latest)
        .mockResolvedValueOnce([injected]);

      render(
        <ChatView
          task={makeTask({ id: 16, description: null })}
          projects={projects}
          onBack={onBack}
        />,
      );
      await screen.findByText('latest-199');

      act(() => {
        capturedOnMessage?.({
          channel: 'task:16',
          data: {
            ...injected,
            task_id: 16,
          },
        });
      });
      expect(screen.getAllByText('[Admin] early steer')).toHaveLength(1);

      await userEvent.click(
        screen.getByRole('button', { name: 'Load older messages' }),
      );

      await waitFor(() => {
        expect(api.getTaskChatHistory).toHaveBeenNthCalledWith(
          2,
          16,
          true,
          200,
          210,
        );
      });
      await waitFor(() => {
        expect(screen.getAllByText('[Admin] early steer')).toHaveLength(1);
      });
      expect(api.injectTaskMessage).not.toHaveBeenCalled();
      expect(api.sendTaskChat).not.toHaveBeenCalled();
    }, 10_000);

    it('backfills again after the task-channel subscription is acknowledged', async () => {
      const task = makeTask({ id: 13 });
      render(<ChatView task={task} projects={projects} onBack={onBack} />);
      await waitFor(() => expect(api.getTaskChatHistory).toHaveBeenCalledTimes(1));

      act(() => { capturedOnSubscribed?.(['task:13']); });
      await waitFor(() => expect(api.getTaskChatHistory).toHaveBeenCalledTimes(2));
    });

    it('does not revive an answered ask-user card from a stale pending snapshot', async () => {
      let resolvePending!: (value: {
        pending: { request_id: string; questions: {
          question: string;
          options: { label: string }[];
        }[] }[];
      }) => void;
      (api.getAskUserPending as ReturnType<typeof vi.fn>).mockReturnValueOnce(
        new Promise((resolve) => { resolvePending = resolve; }),
      );
      render(
        <ChatView
          task={makeTask({ id: 14, description: null })}
          projects={projects}
          onBack={onBack}
        />,
      );

      act(() => {
        capturedOnMessage?.({
          channel: 'task:14',
          data: {
            event_type: 'ask_user_question',
            request_id: 'ask-1',
            questions: [{
              question: 'Proceed?',
              options: [{ label: 'Yes' }, { label: 'No' }],
            }],
          },
        });
      });
      expect(screen.getByText('Proceed?')).toBeInTheDocument();

      act(() => {
        capturedOnMessage?.({
          channel: 'task:14',
          data: {
            event_type: 'ask_user_resolved',
            request_id: 'ask-1',
            timed_out: false,
          },
        });
      });
      expect(screen.getByText('✓ 已回答')).toBeInTheDocument();

      await act(async () => {
        resolvePending({
          pending: [{
            request_id: 'ask-1',
            questions: [{
              question: 'Proceed?',
              options: [{ label: 'Yes' }, { label: 'No' }],
            }],
          }],
        });
      });

      expect(screen.getAllByText('Proceed?')).toHaveLength(1);
      expect(screen.getByText('✓ 已回答')).toBeInTheDocument();
      expect(screen.queryByPlaceholderText('或自定义回答…')).not.toBeInTheDocument();
    });

    it('tombstones a locally submitted answer before a stale pending snapshot returns', async () => {
      let resolvePending!: (value: {
        pending: { request_id: string; questions: {
          question: string;
          options: { label: string }[];
        }[] }[];
      }) => void;
      (api.getAskUserPending as ReturnType<typeof vi.fn>).mockReturnValueOnce(
        new Promise((resolve) => { resolvePending = resolve; }),
      );
      render(
        <ChatView
          task={makeTask({ id: 15, description: null })}
          projects={projects}
          onBack={onBack}
        />,
      );

      act(() => {
        capturedOnMessage?.({
          channel: 'task:15',
          data: {
            event_type: 'ask_user_question',
            request_id: 'ask-local',
            questions: [{
              question: 'Ship it?',
              options: [{ label: 'Yes' }, { label: 'No' }],
            }],
          },
        });
      });
      await userEvent.click(screen.getByRole('button', { name: /Yes/ }));
      await userEvent.click(screen.getByRole('button', { name: '提交' }));
      await waitFor(() => expect(api.submitAskUser).toHaveBeenCalled());
      expect(screen.getByText('✓ 已回答')).toBeInTheDocument();

      await act(async () => {
        resolvePending({
          pending: [{
            request_id: 'ask-local',
            questions: [{
              question: 'Ship it?',
              options: [{ label: 'Yes' }, { label: 'No' }],
            }],
          }],
        });
      });

      expect(screen.getAllByText('Ship it?')).toHaveLength(1);
      expect(screen.getByText('✓ 已回答')).toBeInTheDocument();
      expect(screen.queryByPlaceholderText('或自定义回答…')).not.toBeInTheDocument();
    });

    it('copies a user message without its sender prefix', async () => {
      const writeText = vi.fn().mockResolvedValue(undefined);
      Object.assign(navigator, { clipboard: { writeText } });
      const msgs: ChatMessage[] = [{
        id: 1,
        role: 'user',
        event_type: 'user_message',
        content: '[Admin] 现在进度怎么样了',
        tool_name: null,
        tool_input: null,
        tool_output: null,
        is_error: false,
        loop_iteration: null,
        timestamp: '2024-01-01T00:00:00Z',
        image_urls: null,
        attachments: null,
      }];
      (api.getTaskChatHistory as ReturnType<typeof vi.fn>).mockResolvedValue(msgs);

      render(<ChatView task={makeTask({ description: null })} projects={projects} onBack={onBack} />);

      await userEvent.click(await screen.findByTitle('Copy message'));

      expect(writeText).toHaveBeenCalledWith('现在进度怎么样了');
    });

    it('preserves a real bracket tag when raw user content is available', async () => {
      const writeText = vi.fn().mockResolvedValue(undefined);
      Object.assign(navigator, { clipboard: { writeText } });
      const msgs: ChatMessage[] = [{
        id: 1,
        role: 'user',
        event_type: 'user_message',
        content: '[Admin] [BUG] preserve this tag',
        raw_content: '[BUG] preserve this tag',
        tool_name: null,
        tool_input: null,
        tool_output: null,
        is_error: false,
        loop_iteration: null,
        timestamp: '2024-01-01T00:00:00Z',
        image_urls: null,
        attachments: null,
      }];
      (api.getTaskChatHistory as ReturnType<typeof vi.fn>).mockResolvedValue(msgs);

      render(<ChatView task={makeTask({ description: null })} projects={projects} onBack={onBack} />);

      await userEvent.click(await screen.findByTitle('Copy message'));

      expect(writeText).toHaveBeenCalledWith('[BUG] preserve this tag');
    });
  });

  describe('User message navigation', () => {
    function makeChatMessages(count: number): ChatMessage[] {
      const msgs: ChatMessage[] = [];
      for (let i = 0; i < count; i++) {
        msgs.push({
          id: i * 2 + 1,
          role: 'user',
          event_type: 'user_message',
          content: `User message ${i + 1}`,
          tool_name: null,
          tool_input: null,
          tool_output: null,
          is_error: false,
          loop_iteration: null,
          timestamp: '2024-01-01T00:00:00Z',
          image_urls: null,
          attachments: null,
        });
        msgs.push({
          id: i * 2 + 2,
          role: 'assistant',
          event_type: 'message',
          content: `Assistant response ${i + 1}`,
          tool_name: null,
          tool_input: null,
          tool_output: null,
          is_error: false,
          loop_iteration: null,
          timestamp: '2024-01-01T00:01:00Z',
          image_urls: null,
          attachments: null,
        });
      }
      return msgs;
    }

    it('shows navigation buttons even with fewer than 2 user messages (always visible since e049d57)', async () => {
      const msgs = makeChatMessages(0);
      (api.getTaskChatHistory as ReturnType<typeof vi.fn>).mockResolvedValue(msgs);
      const task = makeTask({ description: 'Only one user msg' });
      render(<ChatView task={task} projects={projects} onBack={onBack} />);

      await waitFor(() => {
        expect(api.getTaskChatHistory).toHaveBeenCalled();
      });

      // e049d57 起导航按钮常驻工具栏右侧（不再按消息数量显隐）
      expect(screen.getByTitle('Previous user message')).toBeInTheDocument();
      expect(screen.getByTitle('Next user message')).toBeInTheDocument();
    });

    it('shows navigation buttons when 2+ user messages exist (description + 1 chat msg)', async () => {
      const msgs = makeChatMessages(1);
      (api.getTaskChatHistory as ReturnType<typeof vi.fn>).mockResolvedValue(msgs);
      const task = makeTask({ description: 'Initial prompt' });
      render(<ChatView task={task} projects={projects} onBack={onBack} />);

      await waitFor(() => {
        expect(screen.getByTitle('Previous user message')).toBeInTheDocument();
      });
      expect(screen.getByTitle('Next user message')).toBeInTheDocument();
    });

    it('shows navigation buttons when 2+ chat user messages exist (no description)', async () => {
      const msgs = makeChatMessages(2);
      (api.getTaskChatHistory as ReturnType<typeof vi.fn>).mockResolvedValue(msgs);
      const task = makeTask({ description: null });
      render(<ChatView task={task} projects={projects} onBack={onBack} />);

      await waitFor(() => {
        expect(screen.getByTitle('Previous user message')).toBeInTheDocument();
      });
      expect(screen.getByTitle('Next user message')).toBeInTheDocument();
    });

    it('marks initial prompt with data-user-msg attribute', async () => {
      (api.getTaskChatHistory as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      const task = makeTask({ description: 'Initial prompt text' });
      const { container } = render(<ChatView task={task} projects={projects} onBack={onBack} />);

      await waitFor(() => {
        expect(api.getTaskChatHistory).toHaveBeenCalled();
      });

      const userMsgNodes = container.querySelectorAll('[data-user-msg]');
      expect(userMsgNodes.length).toBe(1);
    });

    it('marks user chat messages with data-user-msg attribute', async () => {
      const msgs = makeChatMessages(3);
      (api.getTaskChatHistory as ReturnType<typeof vi.fn>).mockResolvedValue(msgs);
      const task = makeTask({ description: 'Prompt' });
      const { container } = render(<ChatView task={task} projects={projects} onBack={onBack} />);

      await waitFor(() => {
        expect(container.querySelectorAll('[data-user-msg]').length).toBe(4);
      });
    });

    it('does not mark assistant messages with data-user-msg attribute', async () => {
      const msgs = makeChatMessages(2);
      (api.getTaskChatHistory as ReturnType<typeof vi.fn>).mockResolvedValue(msgs);
      const task = makeTask({ description: null });
      const { container } = render(<ChatView task={task} projects={projects} onBack={onBack} />);

      await waitFor(() => {
        expect(container.querySelectorAll('[data-user-msg]').length).toBe(2);
      });

      const allMsgDivs = container.querySelectorAll('.items-start');
      allMsgDivs.forEach((div) => {
        expect(div).not.toHaveAttribute('data-user-msg');
      });
    });

    it('clicking "Previous user message" scrolls the container to the user message', async () => {
      const msgs = makeChatMessages(3);
      (api.getTaskChatHistory as ReturnType<typeof vi.fn>).mockResolvedValue(msgs);
      const task = makeTask({ description: 'Prompt' });
      const { container } = render(<ChatView task={task} projects={projects} onBack={onBack} />);

      await waitFor(() => {
        expect(screen.getByTitle('Previous user message')).toBeInTheDocument();
      });

      const scrollContainer = container.querySelector('.overflow-y-auto')!;
      const userMsgNodes = scrollContainer.querySelectorAll('[data-user-msg]');

      // Navigation is getBoundingClientRect-based: container top = 100;
      // nodes below except nodes[2], which sits above the viewport (top = 0)
      (scrollContainer as HTMLElement).getBoundingClientRect = () => ({ top: 100 } as DOMRect);
      const scrollToMock = vi.fn();
      (scrollContainer as HTMLElement).scrollTo = scrollToMock;
      userMsgNodes.forEach((node, i) => {
        Object.defineProperty(node, 'offsetTop', { value: i * 140, configurable: true });
        (node as HTMLElement).getBoundingClientRect = () => ({ top: i === 2 ? 0 : 200 } as DOMRect);
      });

      await userEvent.click(screen.getByTitle('Previous user message'));

      expect(scrollToMock).toHaveBeenCalledWith({ top: 280, behavior: 'smooth' });
    });

    it('clicking "Next user message" scrolls the container to the next user message', async () => {
      const msgs = makeChatMessages(3);
      (api.getTaskChatHistory as ReturnType<typeof vi.fn>).mockResolvedValue(msgs);
      const task = makeTask({ description: 'Prompt' });
      const { container } = render(<ChatView task={task} projects={projects} onBack={onBack} />);

      await waitFor(() => {
        expect(screen.getByTitle('Next user message')).toBeInTheDocument();
      });

      const scrollContainer = container.querySelector('.overflow-y-auto')!;
      const userMsgNodes = scrollContainer.querySelectorAll('[data-user-msg]');

      // Container top = 100; all nodes below the viewport top (top = 200)
      // → "down" navigates to the first node past container top + threshold
      (scrollContainer as HTMLElement).getBoundingClientRect = () => ({ top: 100 } as DOMRect);
      const scrollToMock = vi.fn();
      (scrollContainer as HTMLElement).scrollTo = scrollToMock;
      userMsgNodes.forEach((node, i) => {
        Object.defineProperty(node, 'offsetTop', { value: i * 140 + 40, configurable: true });
        (node as HTMLElement).getBoundingClientRect = () => ({ top: 200 } as DOMRect);
      });

      await userEvent.click(screen.getByTitle('Next user message'));

      expect(scrollToMock).toHaveBeenCalledWith({ top: 40, behavior: 'smooth' });
    });

    it('does nothing when already at the top and clicking up', async () => {
      const msgs = makeChatMessages(2);
      (api.getTaskChatHistory as ReturnType<typeof vi.fn>).mockResolvedValue(msgs);
      const task = makeTask({ description: 'Prompt' });
      const { container } = render(<ChatView task={task} projects={projects} onBack={onBack} />);

      await waitFor(() => {
        expect(screen.getByTitle('Previous user message')).toBeInTheDocument();
      });

      const scrollContainer = container.querySelector('.overflow-y-auto')!;
      const userMsgNodes = scrollContainer.querySelectorAll('[data-user-msg]');

      const scrollToMock = vi.fn();
      (scrollContainer as HTMLElement).scrollTo = scrollToMock;
      userMsgNodes.forEach((node, i) => {
        Object.defineProperty(node, 'offsetTop', { value: i * 300 + 100, configurable: true });
      });

      Object.defineProperty(scrollContainer, 'scrollTop', { value: 0, configurable: true, writable: true });

      await userEvent.click(screen.getByTitle('Previous user message'));

      expect(scrollToMock).not.toHaveBeenCalled();
    });

    it('does nothing when already at the last user message and clicking down', async () => {
      const msgs = makeChatMessages(2);
      (api.getTaskChatHistory as ReturnType<typeof vi.fn>).mockResolvedValue(msgs);
      const task = makeTask({ description: 'Prompt' });
      const { container } = render(<ChatView task={task} projects={projects} onBack={onBack} />);

      await waitFor(() => {
        expect(screen.getByTitle('Next user message')).toBeInTheDocument();
      });

      const scrollContainer = container.querySelector('.overflow-y-auto')!;
      const userMsgNodes = scrollContainer.querySelectorAll('[data-user-msg]');

      const scrollToMock = vi.fn();
      (scrollContainer as HTMLElement).scrollTo = scrollToMock;
      userMsgNodes.forEach((node, i) => {
        Object.defineProperty(node, 'offsetTop', { value: i * 100, configurable: true });
      });

      Object.defineProperty(scrollContainer, 'scrollTop', { value: 9999, configurable: true, writable: true });

      await userEvent.click(screen.getByTitle('Next user message'));

      expect(scrollToMock).not.toHaveBeenCalled();
    });
  });

  describe('Draft buffering (localStorage)', () => {
    const draftKey = (id: number) => `ccm-chat-draft-${id}`;

    beforeEach(() => {
      localStorage.clear();
    });

    it('persists typed input to localStorage', async () => {
      const task = makeTask({ id: 7 });
      render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);

      const textarea = screen.getByPlaceholderText(/follow-up message/i);
      fireEvent.change(textarea, { target: { value: 'unsent draft' } });

      await waitFor(() => {
        expect(localStorage.getItem(draftKey(7))).toBe('unsent draft');
      });
    });

    it('restores the draft when re-entering the chat', async () => {
      localStorage.setItem(draftKey(7), 'restored draft');
      const task = makeTask({ id: 7 });
      render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);

      const textarea = screen.getByPlaceholderText(/follow-up message/i) as HTMLTextAreaElement;
      expect(textarea.value).toBe('restored draft');
    });

    it('does not leak drafts between tasks', async () => {
      localStorage.setItem(draftKey(7), 'task seven draft');
      const task = makeTask({ id: 8 });
      render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);

      const textarea = screen.getByPlaceholderText(/follow-up message/i) as HTMLTextAreaElement;
      expect(textarea.value).toBe('');
    });

    it('clears the draft after sending', async () => {
      const task = makeTask({ id: 7 });
      render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);

      const textarea = screen.getByPlaceholderText(/follow-up message/i);
      fireEvent.change(textarea, { target: { value: 'about to send' } });
      await waitFor(() => expect(localStorage.getItem(draftKey(7))).toBe('about to send'));

      fireEvent.keyDown(textarea, { key: 'Enter', code: 'Enter', ctrlKey: true });

      await waitFor(() => {
        expect(localStorage.getItem(draftKey(7))).toBeNull();
      });
    });
  });
});

describe('聊天图片附件展示（2026-07-16 用户反馈：发图后图片不显示）', () => {
  const projects: Project[] = [];
  const onBack = vi.fn();
  const onTaskUpdated = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
    delete (window as Record<string, unknown>).Capacitor;
  });

  function wsUserMessage(taskId: number, data: Record<string, unknown>) {
    capturedOnMessage!({
      channel: `task:${taskId}`,
      data: { event_type: 'user_message', ...data },
    });
  }

  it('WS user_message 与已展示消息内容重复时，附件必须合并进已展示消息（去重不能吃掉图片）', async () => {
    const task = makeTask({ id: 11 });
    render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);
    await waitFor(() => expect(api.getTaskChatHistory).toHaveBeenCalled());

    // 第一条：乐观回显场景 —— 只有文字、无附件
    await act(async () => {
      wsUserMessage(11, { content: '看下这张截图', image_urls: null, attachments: null });
    });
    expect(screen.getByText('看下这张截图')).toBeInTheDocument();

    // 第二条：服务端广播 —— 同样内容但带图片附件（真实发送时后端会广播这条）
    await act(async () => {
      wsUserMessage(11, {
        content: '看下这张截图',
        image_urls: ['/api/uploads/shot.png'],
        attachments: [{ url: '/api/uploads/shot.png', name: 'shot.png', is_image: true }],
      });
    });

    // 不应产生重复文本消息
    expect(screen.getAllByText('看下这张截图')).toHaveLength(1);
    // 但图片必须显示出来（去重时合并附件，而不是整条丢弃）
    const imgs = document.querySelectorAll('img[src*="/api/uploads/shot.png"]');
    expect(imgs.length).toBeGreaterThan(0);
  });

  it('用带用户名的服务端消息替换无前缀乐观消息，不显示两个气泡', async () => {
    const task = makeTask({ id: 12 });
    render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);
    await waitFor(() => expect(api.getTaskChatHistory).toHaveBeenCalled());

    await act(async () => {
      wsUserMessage(12, {
        content: '检查训练状态',
        raw_content: '检查训练状态',
      });
    });
    expect(screen.getByText('检查训练状态')).toBeInTheDocument();

    await act(async () => {
      wsUserMessage(12, {
        id: 991,
        content: '[Admin] 检查训练状态',
        raw_content: '检查训练状态',
      });
    });

    expect(screen.getByText('[Admin] 检查训练状态')).toBeInTheDocument();
    expect(screen.queryByText('检查训练状态')).not.toBeInTheDocument();
  });

  it('权威用户消息即使不是最后一条，也会替换此前的实时气泡', async () => {
    const task = makeTask({ id: 120, description: null });
    render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);
    await waitFor(() => expect(api.getTaskChatHistory).toHaveBeenCalled());

    await act(async () => {
      wsUserMessage(120, {
        content: '检查训练状态',
        raw_content: '检查训练状态',
      });
      capturedOnMessage!({
        channel: 'task:120',
        data: {
          id: 700,
          event_type: 'message',
          role: 'assistant',
          content: '正在检查',
          timestamp: '2026-07-30T10:00:01Z',
        },
      });
      wsUserMessage(120, {
        id: 699,
        content: '[Admin] 检查训练状态',
        raw_content: '检查训练状态',
        timestamp: '2026-07-30T10:00:00Z',
      });
    });

    expect(screen.getByText('[Admin] 检查训练状态')).toBeInTheDocument();
    expect(screen.queryByText('检查训练状态')).not.toBeInTheDocument();
    expect(screen.getByText('正在检查')).toBeInTheDocument();
  });

  it('同一个持久化用户消息重放时按 id 原位更新而不追加', async () => {
    const task = makeTask({ id: 121, description: null });
    render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);
    await waitFor(() => expect(api.getTaskChatHistory).toHaveBeenCalled());

    await act(async () => {
      wsUserMessage(121, {
        id: 801,
        content: '[Admin] 只显示一次',
        raw_content: '只显示一次',
      });
      capturedOnMessage!({
        channel: 'task:121',
        data: {
          id: 802,
          event_type: 'message',
          role: 'assistant',
          content: '中间输出',
        },
      });
      wsUserMessage(121, {
        id: 801,
        content: '[Admin] 只显示一次',
        raw_content: '只显示一次',
      });
    });

    expect(screen.getAllByText('[Admin] 只显示一次')).toHaveLength(1);
    expect(screen.getByText('中间输出')).toBeInTheDocument();
  });

  it('Capacitor（手机 App）下附件相对 URL 必须拼上远程服务器地址', async () => {
    (window as Record<string, unknown>).Capacitor = {};
    localStorage.setItem('cc_server_url', 'https://ccm.example.com');
    const task = makeTask({ id: 12 });
    render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);
    await waitFor(() => expect(api.getTaskChatHistory).toHaveBeenCalled());

    await act(async () => {
      wsUserMessage(12, {
        content: '手机发图',
        image_urls: ['/api/uploads/phone.png'],
        attachments: [
          { url: '/api/uploads/phone.png', name: 'phone.png', is_image: true },
          { url: '/api/uploads/doc.pdf', name: 'doc.pdf', is_image: false },
        ],
      });
    });

    const img = document.querySelector('img[src="https://ccm.example.com/api/uploads/phone.png"]');
    expect(img).not.toBeNull();
    const link = document.querySelector('a[href="https://ccm.example.com/api/uploads/doc.pdf"]');
    expect(link).not.toBeNull();
  });

  it('初始 Prompt 气泡渲染 task.metadata_.attachments 里的图片', () => {
    const task = makeTask({
      id: 13,
      description: '看图建任务',
      metadata_: {
        attachments: [{ url: '/api/uploads/init.png', name: 'init.png', is_image: true }],
      },
    } as Partial<Task>);
    render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);
    const img = document.querySelector('img[src*="/api/uploads/init.png"]');
    expect(img).not.toBeNull();
  });

  it('助手消息里的任务文件链接一键下载，外部链接保持原行为', async () => {
    const clickSpy = vi.spyOn(HTMLAnchorElement.prototype, 'click')
      .mockImplementation(() => {});
    const createObjectUrl = vi.fn().mockReturnValue('blob:artifact');
    const revokeObjectUrl = vi.fn();
    const NativeURL = URL;
    class MockURL extends NativeURL {
      static createObjectURL = createObjectUrl;
      static revokeObjectURL = revokeObjectUrl;
    }
    vi.stubGlobal('URL', MockURL);
    vi.mocked(api.getTaskChatHistory).mockResolvedValue([
      {
        id: 501,
        role: 'assistant',
        event_type: 'message',
        content: '[汇报稿.md](输出/汇报稿.md) [官网](https://example.com)',
        tool_name: null,
        tool_input: null,
        tool_output: null,
        is_error: false,
        loop_iteration: null,
        timestamp: null,
        image_urls: null,
        attachments: null,
      },
    ]);
    const task = makeTask({ id: 88 });

    render(<ChatView task={task} projects={projects} onBack={onBack} />);
    const artifactLink = await screen.findByRole('link', { name: /汇报稿\.md/ });
    const externalLink = screen.getByRole('link', { name: '官网' });

    expect(externalLink).toHaveAttribute('href', 'https://example.com');
    expect(externalLink).toHaveAttribute('target', '_blank');
    fireEvent.click(artifactLink);

    await waitFor(() => {
      expect(api.downloadTaskArtifact).toHaveBeenCalledWith(
        88,
        '%E8%BE%93%E5%87%BA/%E6%B1%87%E6%8A%A5%E7%A8%BF.md',
      );
    });
    expect(createObjectUrl).toHaveBeenCalled();
    expect(clickSpy).toHaveBeenCalled();
    clickSpy.mockRestore();
    vi.unstubAllGlobals();
  });

  it('普通文件名链接不误判为产物，显式标记仍可下载', async () => {
    vi.mocked(api.getTaskChatHistory).mockResolvedValue([
      {
        id: 504,
        role: 'assistant',
        event_type: 'message',
        content: '[DEPLOYMENT.md](DEPLOYMENT.md) [下载报告](report.pdf "ccm-task-artifact")',
        tool_name: null,
        tool_input: null,
        tool_output: null,
        is_error: false,
        loop_iteration: null,
        timestamp: null,
        image_urls: null,
        attachments: null,
      },
    ]);

    render(<ChatView task={makeTask({ id: 91 })} projects={projects} onBack={onBack} />);

    const sourceLink = await screen.findByRole('link', { name: 'DEPLOYMENT.md' });
    const artifactLink = screen.getByRole('link', { name: /下载报告/ });
    expect(sourceLink).toHaveAttribute('href', 'DEPLOYMENT.md');
    expect(sourceLink).not.toHaveAttribute('title', '下载任务文件');
    expect(artifactLink).toHaveAttribute('title', '下载任务文件');
  });

  it.each(['claude', 'codex'] as const)(
    '%s 助手返回裸绝对文件路径时自动显示任务下载链接',
    async (provider) => {
      const artifactPath = '/home/ubuntu/Projects/调研coding agent/test-download.txt';
      vi.mocked(api.getTaskChatHistory).mockResolvedValue([
        {
          id: 502,
          role: 'assistant',
          event_type: 'message',
          content: `测试下载文件已生成：${artifactPath}\n\n文件约 1 KB。`,
          tool_name: null,
          tool_input: null,
          tool_output: null,
          is_error: false,
          loop_iteration: null,
          timestamp: null,
          image_urls: null,
          attachments: null,
        },
      ]);

      render(
        <ChatView
          task={makeTask({ id: 89, provider })}
          projects={projects}
          onBack={onBack}
        />,
      );

      const link = await screen.findByRole('link', { name: /test-download\.txt/ });
      expect(decodeURI(link.getAttribute('href') || '')).toBe(artifactPath);
      expect(link).toHaveAttribute('title', '下载任务文件');
    },
  );

  it('不把代码中的绝对文件路径兜底改写为下载链接', async () => {
    vi.mocked(api.getTaskChatHistory).mockResolvedValue([
      {
        id: 503,
        role: 'assistant',
        event_type: 'message',
        content: '示例：`/home/ubuntu/report.txt`\n\n```text\n/home/ubuntu/output.txt\n```',
        tool_name: null,
        tool_input: null,
        tool_output: null,
        is_error: false,
        loop_iteration: null,
        timestamp: null,
        image_urls: null,
        attachments: null,
      },
    ]);

    render(<ChatView task={makeTask({ id: 90 })} projects={projects} onBack={onBack} />);

    expect(await screen.findByText('/home/ubuntu/report.txt')).toBeInTheDocument();
    expect(screen.queryByRole('link', { name: 'report.txt' })).not.toBeInTheDocument();
    expect(screen.queryByRole('link', { name: 'output.txt' })).not.toBeInTheDocument();
  });
});

describe('Codex app-server 增量消息', () => {
  const projects: Project[] = [];
  const onBack = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
    (api.getTaskChatHistory as ReturnType<typeof vi.fn>).mockResolvedValue([]);
  });

  it('按 item_id 合并 delta，并用最终消息原位替换而不重复', async () => {
    const task = makeTask({ id: 21, provider: 'codex' });
    render(<ChatView task={task} projects={projects} onBack={onBack} />);
    await waitFor(() => expect(api.getTaskChatHistory).toHaveBeenCalled());

    await act(async () => {
      capturedOnMessage!({
        channel: 'task:21',
        data: { event_type: 'message_delta', item_id: 'msg-1', content: 'Hel' },
      });
      capturedOnMessage!({
        channel: 'task:21',
        data: { event_type: 'message_delta', item_id: 'msg-1', content: 'lo' },
      });
    });
    expect(screen.getAllByText('Hello')).toHaveLength(1);

    await act(async () => {
      capturedOnMessage!({
        channel: 'task:21',
        data: {
          event_type: 'message', item_id: 'msg-1', role: 'assistant',
          content: 'Hello', is_error: false,
        },
      });
    });
    expect(screen.getAllByText('Hello')).toHaveLength(1);
  });

  it('切走再切回运行中的 task 后保留并继续合并未完成 thinking', async () => {
    const task = makeTask({ id: 2101, provider: 'codex', status: 'executing' });
    const first = render(<ChatView task={task} projects={projects} onBack={onBack} />);
    await waitFor(() => expect(api.getTaskChatHistory).toHaveBeenCalled());

    act(() => {
      capturedOnMessage!({
        channel: 'task:2101',
        data: {
          event_type: 'thinking_delta',
          item_id: 'reasoning-1',
          content: 'first half',
        },
      });
    });
    expect(screen.getByText('first half')).toBeInTheDocument();
    first.unmount();

    const second = render(<ChatView task={task} projects={projects} onBack={onBack} />);
    expect(screen.getByText('first half')).toBeInTheDocument();

    act(() => {
      capturedOnMessage!({
        channel: 'task:2101',
        data: {
          event_type: 'thinking_delta',
          item_id: 'reasoning-1',
          content: ' second half',
        },
      });
    });
    expect(screen.getByText('first half second half')).toBeInTheDocument();

    act(() => {
      capturedOnMessage!({
        channel: 'task:2101',
        data: {
          id: 991,
          event_type: 'thinking',
          item_id: 'reasoning-1',
          role: 'assistant',
          content: 'first half second half',
        },
      });
    });
    expect(screen.getAllByText('first half second half')).toHaveLength(1);
    second.unmount();

    render(<ChatView task={task} projects={projects} onBack={onBack} />);
    expect(screen.queryByText('first half second half')).not.toBeInTheDocument();
  });
});

describe('failed attachment sending', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    (api.getTaskChatHistory as ReturnType<typeof vi.fn>).mockResolvedValue([]);
    (api.getAskUserPending as ReturnType<typeof vi.fn>).mockResolvedValue({ pending: [] });
  });

  it('keeps Send disabled instead of posting an empty attachment placeholder', async () => {
    (api.uploadImages as ReturnType<typeof vi.fn>)
      .mockRejectedValueOnce(new Error('upload failed'));
    render(<ChatView task={makeTask()} projects={[]} onBack={vi.fn()} />);

    const input = document.querySelector('input[type="file"]') as HTMLInputElement;
    fireEvent.change(input, {
      target: { files: [new File(['evidence'], 'evidence.txt', { type: 'text/plain' })] },
    });
    await screen.findByTitle('Click to retry');

    const send = screen.getByTitle('Retry or remove failed attachments before sending');
    expect(send).toBeDisabled();
    expect(api.sendTaskChat).not.toHaveBeenCalled();
  });
});

describe('independent Plan attachments', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.removeItem('ccm-plan-dismissed-1');
    (api.getTaskChatHistory as ReturnType<typeof vi.fn>).mockResolvedValue([]);
    (api.getAskUserPending as ReturnType<typeof vi.fn>).mockResolvedValue({ pending: [] });
    (api.listPlans as ReturnType<typeof vi.fn>).mockResolvedValue([]);
    (api.listPlanVersions as ReturnType<typeof vi.fn>).mockResolvedValue([]);
    (api.getRuntimeSettings as ReturnType<typeof vi.fn>).mockResolvedValue({
      use_pty_mode: false,
      pty_available: false,
      codex_app_server_enabled: true,
      codex_main_mcp_enabled: true,
    });
    (api.sendTaskChat as ReturnType<typeof vi.fn>).mockResolvedValue({
      ok: true,
      applied_plan_task_ids: [81],
    });
  });

  it('creates an associated Plan using the global pipeline settings', async () => {
    (api.createPlan as ReturnType<typeof vi.fn>).mockResolvedValue(makePlan());
    render(<ChatView task={makeTask({ id: 1 })} projects={[]} onBack={vi.fn()} />);

    await userEvent.click(screen.getByRole('button', { name: 'Plans' }));
    await userEvent.type(
      await screen.findByPlaceholderText(
        'Create an independent Plan…',
      ),
      'Design the migration',
    );
    await userEvent.click(screen.getByRole('button', { name: 'Create Plan' }));

    await waitFor(() => expect(api.createPlan).toHaveBeenCalledWith({
      input: 'Design the migration',
      target_task_id: 1,
    }));
    expect(screen.queryByRole('button', { name: 'Models' })).not.toBeInTheDocument();
  });

  it('keeps the Plan catalog open after creating an associated Plan', async () => {
    const created = makePlan({ id: 86, title: 'New background Plan' });
    let refreshes = 0;
    (api.createPlan as ReturnType<typeof vi.fn>).mockResolvedValue(created);
    (api.listPlans as ReturnType<typeof vi.fn>).mockImplementation(async () => {
      refreshes += 1;
      return refreshes === 1 ? [] : [created];
    });
    render(<ChatView task={makeTask({ id: 1 })} projects={[]} onBack={vi.fn()} />);

    await userEvent.click(screen.getByRole('button', { name: 'Plans' }));
    await userEvent.type(
      await screen.findByPlaceholderText('Create an independent Plan…'),
      'Plan without opening details',
    );
    await userEvent.click(screen.getByRole('button', { name: 'Create Plan' }));

    expect(await screen.findByRole('button', { name: /#86 New background Plan/ }))
      .not.toHaveAttribute('aria-current');
    expect(screen.getByText('Select or create a Plan')).toBeInTheDocument();
  });

  it('highlights the selected Plan in the modal list', async () => {
    const selectedPlan = makePlan({ id: 80, title: 'Selected Plan' });
    const otherPlan = makePlan({ id: 81, title: 'Other Plan' });
    (api.listPlans as ReturnType<typeof vi.fn>).mockResolvedValue([
      selectedPlan,
      otherPlan,
    ]);

    render(<ChatView task={makeTask({ id: 1 })} projects={[]} onBack={vi.fn()} />);
    await userEvent.click(screen.getByRole('button', { name: 'Plans' }));

    const selectedButton = await screen.findByRole('button', {
      name: /#80 Selected Plan/,
    });
    const otherButton = screen.getByRole('button', { name: /#81 Other Plan/ });
    expect(selectedButton).not.toHaveAttribute('aria-current');

    await userEvent.click(selectedButton);

    expect(selectedButton).toHaveAttribute('aria-current', 'true');
    expect(selectedButton).toHaveClass('border-indigo-500/70', 'bg-indigo-500/15');
    expect(within(selectedButton).getByText('Needs approval'))
      .toHaveClass('text-indigo-300', 'ring-indigo-500/30');
    expect(otherButton).not.toHaveAttribute('aria-current');
  });

  it('shows the exact applied Plan snapshot on its persisted user message', async () => {
    (api.getTaskChatHistory as ReturnType<typeof vi.fn>).mockResolvedValue([{
      id: 901,
      role: 'user',
      event_type: 'user_message',
      content: 'Implement the approved plan',
      tool_name: null,
      tool_input: null,
      tool_output: null,
      is_error: false,
      loop_iteration: null,
      timestamp: '2026-08-02T08:36:21Z',
      image_urls: null,
      attachments: null,
      raw_content: 'Implement the approved plan',
      applied_plans: [{
        id: 81,
        title: 'Schema migration',
        content: '# Migration\n\nUse the **safe path**.',
      }],
    } satisfies ChatMessage]);

    render(<ChatView task={makeTask({ id: 1 })} projects={[]} onBack={vi.fn()} />);

    const applied = await screen.findByText(
      'Applied Plan #81: Schema migration',
    );
    expect(applied.closest('.applied-plan-message')).toBeInTheDocument();
    expect(applied.closest('details')).toHaveClass('bg-black/15', 'border-white/25');
    await userEvent.click(applied);
    expect(document.querySelector('.applied-plan-content')).toHaveClass('bg-transparent');
    expect(document.querySelector('.applied-plan-content')).not.toHaveClass('bg-black/20');
    expect(screen.getByRole('heading', { level: 1, name: 'Migration' }))
      .toBeInTheDocument();
    expect(screen.getByText('safe path').tagName).toBe('STRONG');
  });

  it('reconciles a Worker persisted message without dropping optimistic attachments or Plans', async () => {
    localStorage.clear();
    const version = makePlanVersion({
      id: 505,
      plan_id: 85,
      human_decision: 'approved',
      display_state: 'approved',
    });
    const plan = makePlan({
      id: 85,
      title: 'Worker rollout',
      display_state: 'approved',
      current_version_id: version.id,
      current_version: version,
    });
    const upload = makeUpload('worker-notes', 'worker-notes.txt');
    (api.listPlans as ReturnType<typeof vi.fn>).mockResolvedValue([plan]);
    (api.listPlanVersions as ReturnType<typeof vi.fn>).mockResolvedValue([version]);
    (api.uploadImages as ReturnType<typeof vi.fn>).mockResolvedValueOnce([upload]);

    const { container } = render(
      <ChatView
        task={makeTask({ id: 1, worker_id: 7 })}
        projects={[]}
        onBack={vi.fn()}
      />,
    );

    await userEvent.click(screen.getByRole('button', { name: 'Plans' }));
    await userEvent.click(await screen.findByRole('button', { name: /#85 Worker rollout/ }));
    await userEvent.click(await screen.findByRole('button', { name: 'Attach to next message' }));
    expect(await screen.findByText('Plan #85 · v1 · Worker rollout')).toBeInTheDocument();

    await userEvent.upload(
      container.querySelector<HTMLInputElement>('input[type="file"]')!,
      new File(['worker evidence'], upload.filename, { type: 'text/plain' }),
    );
    await screen.findByText(upload.filename);
    await userEvent.type(
      screen.getByPlaceholderText('Type a follow-up message...'),
      'Ship the Worker rollout',
    );
    await userEvent.click(screen.getByTitle('Send (Ctrl+Enter)'));

    await waitFor(() => expect(api.sendTaskChat).toHaveBeenCalled());
    await waitFor(() => {
      expect(screen.queryByText('Plan #85 · v1 · Worker rollout')).not.toBeInTheDocument();
    });

    // The Manager's initial Worker broadcast has a durable id/raw_content but
    // does not yet contain attachment or applied-Plan metadata.
    act(() => {
      capturedOnMessage?.({
        channel: 'task:1',
        data: {
          id: 1901,
          event_type: 'user_message',
          role: 'user',
          content: '[Admin] Ship the Worker rollout',
          raw_content: 'Ship the Worker rollout',
          timestamp: '2026-08-05T10:00:00Z',
          image_urls: null,
          attachments: null,
          applied_plans: null,
        },
      });
    });

    expect(screen.getAllByText('[Admin] Ship the Worker rollout')).toHaveLength(1);
    expect(screen.queryByText('Ship the Worker rollout')).not.toBeInTheDocument();
    expect(screen.getByRole('link', { name: upload.filename })).toHaveAttribute(
      'href',
      upload.url,
    );
    expect(screen.getByText('Applied Plan #85: Worker rollout')).toBeInTheDocument();
  });

  it('creates an associated Plan with uploaded files from the modal composer', async () => {
    (api.createPlan as ReturnType<typeof vi.fn>).mockResolvedValue(makePlan());
    (api.uploadImages as ReturnType<typeof vi.fn>).mockResolvedValueOnce([{
      id: 'plan-upload',
      filename: 'design-notes.txt',
      path: '/srv/uploads/design-notes.txt',
      url: '/api/uploads/design-notes.txt',
      is_image: false,
    }]);
    render(<ChatView task={makeTask({ id: 1 })} projects={[]} onBack={vi.fn()} />);

    await userEvent.click(screen.getByRole('button', { name: 'Plans' }));
    const dialog = await screen.findByRole('dialog', { name: 'Plans for Task #1' });
    const picker = dialog.querySelector('input[type="file"]') as HTMLInputElement;
    await userEvent.upload(
      picker,
      new File(['notes'], 'design-notes.txt', { type: 'text/plain' }),
    );
    await screen.findByText('design-notes.txt');
    await userEvent.type(
      screen.getByPlaceholderText('Create an independent Plan…'),
      'Use the attached design notes',
    );
    await userEvent.click(screen.getByRole('button', { name: 'Create Plan' }));

    await waitFor(() => expect(api.createPlan).toHaveBeenCalledWith({
      input: 'Use the attached design notes',
      target_task_id: 1,
      file_paths: ['/srv/uploads/design-notes.txt'],
      image_paths: [],
      attachments: [{
        url: '/api/uploads/design-notes.txt',
        name: 'design-notes.txt',
        is_image: false,
      }],
    }));
    await waitFor(() => {
      expect(screen.getByPlaceholderText('Create an independent Plan…')).toHaveValue('');
      expect(screen.queryByText('design-notes.txt')).not.toBeInTheDocument();
    });
  });

  it('routes pasted files to the Plan composer instead of the main chat composer', async () => {
    const pasted = new File(['pasted notes'], 'pasted-notes.txt', { type: 'text/plain' });
    (api.createPlan as ReturnType<typeof vi.fn>).mockResolvedValue(makePlan());
    (api.uploadImages as ReturnType<typeof vi.fn>).mockResolvedValueOnce([{
      id: 'pasted-plan-upload',
      filename: pasted.name,
      path: '/srv/uploads/pasted-notes.txt',
      url: '/api/uploads/pasted-notes.txt',
      is_image: false,
    }]);
    render(<ChatView task={makeTask({ id: 1 })} projects={[]} onBack={vi.fn()} />);

    await userEvent.click(screen.getByRole('button', { name: 'Plans' }));
    const planInput = await screen.findByPlaceholderText('Create an independent Plan…');
    fireEvent.paste(planInput, {
      clipboardData: {
        items: [{ kind: 'file', getAsFile: () => pasted }],
      },
    });

    await waitFor(() => expect(api.uploadImages).toHaveBeenCalledTimes(1));
    expect(api.uploadImages).toHaveBeenCalledWith([pasted]);
    expect(await screen.findByText(pasted.name)).toBeInTheDocument();

    await userEvent.type(planInput, 'Use the pasted notes');
    await userEvent.click(screen.getByRole('button', { name: 'Create Plan' }));
    await waitFor(() => expect(api.createPlan).toHaveBeenCalledWith({
      input: 'Use the pasted notes',
      target_task_id: 1,
      file_paths: ['/srv/uploads/pasted-notes.txt'],
      image_paths: [],
      attachments: [{
        url: '/api/uploads/pasted-notes.txt',
        name: 'pasted-notes.txt',
        is_image: false,
      }],
    }));
  });

  it('updates an associated Plan stage and ready state in real time', async () => {
    const activeRun = {
      id: 700, plan_id: 80, run_type: 'initial', status: 'running',
      current_stage: 'planner', base_version_id: null, result_version_id: null,
      request_text: 'Design', round: 1, generation: 1, instance_id: 2,
      worker_id: null, open_input_request_id: null, interaction_count: 0,
      max_interactions: 3, execution_seconds: 0, last_execution_started_at: null,
      review_verdict: null,
      review_feedback: null, review_exhausted: false, error: null,
      created_at: '2026-08-02T08:00:00Z', updated_at: '2026-08-02T08:00:00Z',
      finished_at: null, steps: [], input_requests: [],
    };
    const running = makePlan({
      title: 'Live Plan', display_state: 'planner', current_version_id: null,
      current_version: null, active_run_id: 700, active_run: activeRun,
    });
    const ready = makePlan({ title: 'Live Plan' });
    (api.listPlans as ReturnType<typeof vi.fn>).mockResolvedValue([running]);
    (api.listPlanVersions as ReturnType<typeof vi.fn>).mockResolvedValue([ready.current_version]);

    render(<ChatView task={makeTask({ id: 1 })} projects={[]} onBack={vi.fn()} />);
    await userEvent.click(screen.getByRole('button', { name: 'Plans' }));
    expect((await screen.findAllByText('Planning')).length).toBeGreaterThan(0);
    await userEvent.click(await screen.findByRole('button', { name: /#80 Live Plan/ }));

    (api.listPlans as ReturnType<typeof vi.fn>).mockResolvedValue([ready]);
    act(() => {
      capturedOnMessage?.({
        channel: 'plans',
        data: {
          event: 'plan_run_status_changed',
          plan_id: 80,
          run_id: 700,
          status: 'completed',
        },
      });
    });
    expect(await screen.findByRole('button', { name: /Approve & attach v1/ })).toBeInTheDocument();
    expect(api.listPlans).toHaveBeenCalledWith({ target_task_id: 1 });
  });

  it('approves a Plan without sending a chat turn', async () => {
    const version = makePlanVersion({ id: 502, plan_id: 82 });
    const plan = makePlan({ id: 82, title: 'Review me', current_version_id: 502, current_version: version });
    const approved = makePlan({ ...plan, display_state: 'approved', current_version: { ...version, human_decision: 'approved' } });
    let isApproved = false;
    (api.listPlans as ReturnType<typeof vi.fn>).mockImplementation(async () => [isApproved ? approved : plan]);
    (api.listPlanVersions as ReturnType<typeof vi.fn>).mockResolvedValue([version]);
    (api.approvePlanVersion as ReturnType<typeof vi.fn>).mockImplementation(async () => {
      isApproved = true;
      return approved.current_version;
    });

    render(<ChatView task={makeTask({ id: 1 })} projects={[]} onBack={vi.fn()} />);
    await userEvent.click(screen.getByRole('button', { name: 'Plans' }));
    await userEvent.click(await screen.findByRole('button', { name: /#82 Review me/ }));
    await userEvent.click(await screen.findByRole('button', { name: 'Approve v1 only' }));

    await waitFor(() => expect(api.approvePlanVersion).toHaveBeenCalledWith(502, 502, false));
    expect(api.sendTaskChat).not.toHaveBeenCalled();
  });

  it('approves and explicitly attaches a Plan to the next message', async () => {
    const existingVersion = makePlanVersion({
      id: 503,
      plan_id: 83,
      human_decision: 'approved',
      display_state: 'approved',
    });
    const existing = makePlan({
      id: 83,
      title: 'Already attached',
      display_state: 'approved',
      current_version_id: 503,
      current_version: existingVersion,
    });
    const version = makePlanVersion({ id: 504, plan_id: 84 });
    const plan = makePlan({ id: 84, title: 'Attach me', current_version_id: 504, current_version: version });
    const approved = makePlan({ ...plan, display_state: 'approved', current_version: { ...version, human_decision: 'approved' } });
    let isApproved = false;
    (api.listPlans as ReturnType<typeof vi.fn>).mockImplementation(async () => [existing, isApproved ? approved : plan]);
    (api.listPlanVersions as ReturnType<typeof vi.fn>).mockImplementation(async (planId: number) => (
      planId === existing.id ? [existingVersion] : [version]
    ));
    (api.approvePlanVersion as ReturnType<typeof vi.fn>).mockImplementation(async () => {
      isApproved = true;
      return approved.current_version;
    });

    render(<ChatView task={makeTask({ id: 1 })} projects={[]} onBack={vi.fn()} />);
    await userEvent.click(screen.getByRole('button', { name: 'Plans' }));
    await userEvent.click(await screen.findByRole('button', { name: /#83 Already attached/ }));
    await userEvent.click(await screen.findByRole('button', { name: 'Attach to next message' }));
    expect(await screen.findByText('Plan #83 · v1 · Already attached')).toBeInTheDocument();

    await userEvent.click(screen.getByRole('button', { name: 'Plans' }));
    await userEvent.click(await screen.findByRole('button', { name: /#84 Attach me/ }));
    await userEvent.click(await screen.findByRole('button', { name: /Approve & attach v1/ }));

    expect(await screen.findByText('Plan #83 · v1 · Already attached')).toBeInTheDocument();
    expect(await screen.findByText('Plan #84 · v1 · Attach me')).toBeInTheDocument();
    expect(screen.queryByRole('dialog', { name: 'Plans for Task #1' })).not.toBeInTheDocument();

    await userEvent.click(screen.getByRole('button', { name: 'Plans' }));
    expect(await screen.findByRole('button', { name: /#84 Attach me/ })).toBeInTheDocument();
    expect(screen.queryByRole('status', { name: 'Loading Plans' })).not.toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Close Plans' }));

    await userEvent.type(
      screen.getByPlaceholderText('Type a follow-up message...'),
      'Implement the approved Version',
    );
    await userEvent.click(screen.getByTitle('Send (Ctrl+Enter)'));
    await waitFor(() => expect(api.sendTaskChat).toHaveBeenCalledWith(
      1,
      'Implement the approved Version',
      undefined,
      undefined,
      null,
      {
        provider: 'claude',
        model: null,
        codex_service_tier: 'default',
      },
      undefined,
      undefined,
      [503, 504],
      [],
    ));
  }, 15_000);

  it('creates a new related Plan when the user requests a revision', async () => {
    const version = makePlanVersion({ id: 503, plan_id: 83 });
    const plan = makePlan({ id: 83, title: 'Revise me', current_version_id: 503, current_version: version });
    (api.listPlans as ReturnType<typeof vi.fn>).mockResolvedValue([plan]);
    (api.listPlanVersions as ReturnType<typeof vi.fn>).mockResolvedValue([version]);
    (api.createPlanRun as ReturnType<typeof vi.fn>).mockResolvedValue({});
    render(<ChatView task={makeTask({ id: 1 })} projects={[]} onBack={vi.fn()} />);
    await userEvent.click(screen.getByRole('button', { name: 'Plans' }));
    await userEvent.click(await screen.findByRole('button', { name: /#83 Revise me/ }));
    await userEvent.type(
      await screen.findByPlaceholderText('Revise from v1…', {}, { timeout: 5_000 }),
      'Preserve backwards compatibility',
    );
    await userEvent.click(await screen.findByRole('button', { name: 'Revise from v1' }));

    await waitFor(() => expect(api.createPlanRun).toHaveBeenCalledWith(83, {
      run_type: 'user_revision',
      request: 'Preserve backwards compatibility',
      base_version_id: 503,
      expected_current_version_id: 503,
    }));
  }, 10_000);
});
