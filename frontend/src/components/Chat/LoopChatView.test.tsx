import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor, act, fireEvent } from '@testing-library/react';
import { LoopChatView } from './LoopChatView';
import type { Task, ChatMessage } from '../../api/client';

// Capture the onMessage callback so tests can inject WebSocket messages
let capturedOnMessage: ((msg: Record<string, unknown>) => void) | undefined;
let capturedChannels: string[] = [];
vi.mock('../../hooks/useWebSocket', () => ({
  useWebSocket: vi.fn((
    channels: string[],
    onMessage?: (msg: Record<string, unknown>) => void,
  ) => {
    capturedChannels = channels;
    capturedOnMessage = onMessage;
    return { lastMessage: null, isConnected: true };
  }),
}));

vi.mock('../../api/client', () => ({
  api: {
    getTaskChatHistory: vi.fn().mockResolvedValue([]),
    cancelTask: vi.fn().mockResolvedValue({}),
    downloadTaskArtifact: vi.fn().mockResolvedValue({
      blob: new Blob(['loop artifact']),
      filename: 'loop-report.md',
    }),
  },
}));

import { api } from '../../api/client';

function makeTask(overrides: Partial<Task> = {}): Task {
  return {
    id: 42,
    title: '',
    description: 'Loop test task',
    status: 'executing',
    priority: 0,
    project_id: 1,
    target_repo: '/tmp/repo',
    target_branch: 'main',
    result_branch: null,
    merge_status: 'pending',
    instance_id: 1,
    retry_count: 0,
    turn_generation: 0,
    max_retries: 2,
    mode: 'loop',
    todo_file_path: 'TODO.md',
    loop_progress: '3/10',
    max_iterations: 10,
    must_complete: true,
    plan_content: null,
    plan_approved: null,
    starred: false,
    archived: false,
    has_unread: false,
    session_id: 'sess-1',
    error_message: null,
    model: null,
    tags: null,
    context_window_usage: null,
    created_at: '2024-01-01T00:00:00Z',
    started_at: '2024-01-01T00:01:00Z',
    completed_at: null,
    ...overrides,
  };
}

function makeMsg(overrides: Partial<ChatMessage> = {}): ChatMessage {
  return {
    id: Math.random() * 100000,
    role: 'assistant',
    event_type: 'message',
    content: 'some content',
    tool_name: null,
    tool_input: null,
    tool_output: null,
    is_error: false,
    loop_iteration: 0,
    timestamp: '2024-01-01T00:02:00Z',
    image_urls: null,
    attachments: null,
    ...overrides,
  };
}

function sendWs(data: Record<string, unknown>, channel = 'task:42') {
  capturedOnMessage?.({ channel, data });
}

describe('LoopChatView', () => {
  const onBack = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(api.getTaskChatHistory).mockReset().mockResolvedValue([]);
    capturedOnMessage = undefined;
    capturedChannels = [];
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  describe('History loading', () => {
    it('loads all history without a limit', async () => {
      const task = makeTask();
      render(<LoopChatView task={task} onBack={onBack} />);
      await waitFor(() => {
        // limit=0 表示不限量拉全量历史（compact=true, beforeId=0, touch=true）
        expect(api.getTaskChatHistory).toHaveBeenCalledWith(task.id, true, 0, 0, true);
      });
    });

    it('hides only raw-classified legacy Codex collab completions', async () => {
      vi.mocked(api.getTaskChatHistory).mockResolvedValue([
        makeMsg({
          id: 901,
          role: 'system',
          event_type: 'system_event',
          content: 'completed',
          native_item_type: 'collab_agent_tool_call',
          native_item_status: 'completed',
        }),
        makeMsg({
          id: 902,
          role: 'system',
          event_type: 'system_event',
          content: 'completed',
          native_item_type: null,
          native_item_status: null,
        }),
        makeMsg({
          id: 903,
          content: 'Loop reply after agent wait',
        }),
      ]);

      render(<LoopChatView task={makeTask()} onBack={onBack} />);

      expect(
        await screen.findByText('Loop reply after agent wait'),
      ).toBeInTheDocument();
      expect(screen.getAllByText('— completed —')).toHaveLength(1);
    });

    it('downloads task file links with the loop task context', async () => {
      const clickSpy = vi.spyOn(HTMLAnchorElement.prototype, 'click')
        .mockImplementation(() => {});
      const createObjectUrl = vi.fn().mockReturnValue('blob:loop-artifact');
      const NativeURL = URL;
      class MockURL extends NativeURL {
        static createObjectURL = createObjectUrl;
        static revokeObjectURL = vi.fn();
      }
      vi.stubGlobal('URL', MockURL);
      vi.mocked(api.getTaskChatHistory).mockResolvedValue([
        makeMsg({
          id: 904,
          content: '[loop-report.md](reports/loop-report.md)',
        }),
      ]);

      render(<LoopChatView task={makeTask({ id: 42 })} onBack={onBack} />);
      const link = await screen.findByRole('link', { name: /loop-report\.md/ });
      fireEvent.click(link);

      await waitFor(() => {
        expect(api.downloadTaskArtifact).toHaveBeenCalledWith(
          42,
          'reports/loop-report.md',
        );
      });
      expect(createObjectUrl).toHaveBeenCalled();
      clickSpy.mockRestore();
      vi.unstubAllGlobals();
    });

    it('renders a bare absolute artifact path as a task download link', async () => {
      const artifactPath = '/home/ubuntu/loop output/final-report.md';
      vi.mocked(api.getTaskChatHistory).mockResolvedValue([
        makeMsg({
          id: 905,
          content: `产物已生成：${artifactPath}`,
        }),
      ]);

      render(<LoopChatView task={makeTask({ id: 42 })} onBack={onBack} />);

      const link = await screen.findByRole('link', { name: /final-report\.md/ });
      expect(decodeURI(link.getAttribute('href') || '')).toBe(artifactPath);
      expect(link).toHaveAttribute('title', '下载任务文件');
    });

    it('honors the explicit artifact marker for a bare filename', async () => {
      vi.mocked(api.getTaskChatHistory).mockResolvedValue([
        makeMsg({
          id: 906,
          content: '[下载结果](result.txt "ccm-task-artifact")',
        }),
      ]);

      render(<LoopChatView task={makeTask({ id: 42 })} onBack={onBack} />);

      const link = await screen.findByRole('link', { name: /下载结果/ });
      expect(link).toHaveAttribute('href', 'result.txt');
      expect(link).toHaveAttribute('title', '下载任务文件');
    });
  });

  describe('WebSocket message uses loop_iteration from backend', () => {
    it('uses loop_iteration from the WebSocket message data', async () => {
      const task = makeTask();
      render(<LoopChatView task={task} onBack={onBack} />);
      await waitFor(() => expect(api.getTaskChatHistory).toHaveBeenCalled());

      act(() => {
        sendWs({
          event_type: 'message',
          role: 'assistant',
          content: 'Working on iteration 3',
          loop_iteration: 3,
          is_error: false,
        });
      });

      await waitFor(() => {
        expect(screen.getByText('Iteration 4')).toBeInTheDocument();
      });
    });

    it('falls back to 0 when loop_iteration is missing from WS data', async () => {
      const task = makeTask();
      render(<LoopChatView task={task} onBack={onBack} />);
      await waitFor(() => expect(api.getTaskChatHistory).toHaveBeenCalled());

      act(() => {
        sendWs({
          event_type: 'message',
          role: 'assistant',
          content: 'No iteration field',
          is_error: false,
        });
      });

      await waitFor(() => {
        expect(screen.getByText('Iteration 1')).toBeInTheDocument();
      });
    });
  });

  describe('Race condition: WS messages during initial history load', () => {
    it('does not lose WS messages that arrive before history finishes loading', async () => {
      const historyMsgs: ChatMessage[] = [
        makeMsg({ id: 100, content: 'History msg', loop_iteration: 0 }),
      ];
      let resolveHistory!: (msgs: ChatMessage[]) => void;
      const historyPromise = new Promise<ChatMessage[]>((r) => { resolveHistory = r; });
      vi.mocked(api.getTaskChatHistory).mockReturnValue(historyPromise);

      const task = makeTask();
      render(<LoopChatView task={task} onBack={onBack} />);

      // WS message arrives BEFORE history resolves
      act(() => {
        sendWs({
          event_type: 'message',
          role: 'assistant',
          content: 'Realtime msg',
          loop_iteration: 1,
          is_error: false,
        });
      });

      // Now resolve history
      await act(async () => {
        resolveHistory(historyMsgs);
      });

      // Both iterations should have panels — iteration 0 (history) may be collapsed,
      // but iteration 2 (realtime) should be expanded as the active one
      await waitFor(() => {
        expect(screen.getByText('Iteration 1')).toBeInTheDocument();
        expect(screen.getByText('Iteration 2')).toBeInTheDocument();
        expect(screen.getByText('Realtime msg')).toBeInTheDocument();
      });
    });

    it('does not duplicate messages that arrive in both history and WS', async () => {
      let resolveHistory!: (msgs: ChatMessage[]) => void;
      const historyPromise = new Promise<ChatMessage[]>((r) => { resolveHistory = r; });
      vi.mocked(api.getTaskChatHistory).mockReturnValue(historyPromise);

      const task = makeTask();
      render(<LoopChatView task={task} onBack={onBack} />);

      // WS message arrives with a generated id (Date.now()-based, always larger than DB ids)
      act(() => {
        sendWs({
          event_type: 'message',
          role: 'assistant',
          content: 'Will be in history too',
          loop_iteration: 0,
          is_error: false,
        });
      });

      // History arrives with the same message (DB id is smaller than the WS id)
      const historyMsgs: ChatMessage[] = [
        makeMsg({
          id: 50,
          content: 'Will be in history too',
          loop_iteration: 0,
          task_retry_count: task.retry_count,
          task_turn_generation: task.turn_generation,
        }),
      ];

      await act(async () => {
        resolveHistory(historyMsgs);
      });

      await waitFor(() => {
        expect(screen.getAllByText('Will be in history too')).toHaveLength(1);
      });
    });

    it('keeps buffered WS messages across a status refresh and ignores the stale request', async () => {
      const resolvers: Array<(messages: ChatMessage[]) => void> = [];
      vi.mocked(api.getTaskChatHistory)
        .mockImplementationOnce(
          () => new Promise<ChatMessage[]>((resolve) => resolvers.push(resolve)),
        )
        .mockImplementationOnce(
          () => new Promise<ChatMessage[]>((resolve) => resolvers.push(resolve)),
        );

      const task = makeTask({ status: 'executing' });
      const { rerender } = render(<LoopChatView task={task} onBack={onBack} />);
      await waitFor(() => expect(resolvers).toHaveLength(1));

      act(() => {
        sendWs({
          event_type: 'message',
          role: 'assistant',
          content: 'Buffered across status change',
          loop_iteration: 0,
          is_error: false,
        });
      });

      rerender(
        <LoopChatView
          task={{ ...task, status: 'completed' }}
          onBack={onBack}
        />,
      );
      await waitFor(() => expect(resolvers).toHaveLength(2));

      await act(async () => {
        resolvers[1]([]);
      });
      await waitFor(() => {
        expect(screen.getByText('Buffered across status change')).toBeInTheDocument();
      });

      await act(async () => {
        resolvers[0]([
          makeMsg({ id: 999, content: 'Stale history response', loop_iteration: 0 }),
        ]);
      });
      expect(screen.queryByText('Stale history response')).not.toBeInTheDocument();
      expect(screen.queryByText('Claude is working...')).not.toBeInTheDocument();
    });
  });

  describe('loop_iteration_end event', () => {
    it('updates iteration metadata and advances activeIteration', async () => {
      const task = makeTask();
      render(<LoopChatView task={task} onBack={onBack} />);
      await waitFor(() => expect(api.getTaskChatHistory).toHaveBeenCalled());

      // Add messages for iteration 0
      act(() => {
        sendWs({
          event_type: 'message',
          role: 'assistant',
          content: 'Iter 0 work',
          loop_iteration: 0,
          is_error: false,
        });
      });

      // Send loop_iteration_end for iteration 0
      act(() => {
        sendWs({
          event: 'loop_iteration_end',
          iteration: 0,
          action: 'continue',
          reason: 'Completed phase 1',
          progress: '3/10',
        });
      });

      await waitFor(() => {
        expect(screen.getByText('Iteration 1')).toBeInTheDocument();
        expect(screen.getByText('Completed phase 1')).toBeInTheDocument();
      });

      // New message should go to iteration 1 (from backend loop_iteration)
      act(() => {
        sendWs({
          event_type: 'message',
          role: 'assistant',
          content: 'Iter 1 work',
          loop_iteration: 1,
          is_error: false,
        });
      });

      await waitFor(() => {
        expect(screen.getByText('Iteration 2')).toBeInTheDocument();
        expect(screen.getByText('Iter 1 work')).toBeInTheDocument();
      });
    });

    it('shows done metadata when loop finishes', async () => {
      const task = makeTask({ status: 'completed' });
      render(<LoopChatView task={task} onBack={onBack} />);
      await waitFor(() => expect(api.getTaskChatHistory).toHaveBeenCalled());

      act(() => {
        sendWs({
          event_type: 'message',
          role: 'assistant',
          content: 'Final work',
          loop_iteration: 0,
          is_error: false,
        });
      });

      act(() => {
        sendWs({
          event: 'loop_iteration_end',
          iteration: 0,
          action: 'done',
          reason: 'All items completed',
          progress: '10/10',
        });
      });

      await waitFor(() => {
        expect(screen.getByText('All items completed')).toBeInTheDocument();
        expect(screen.getByText(/done/)).toBeInTheDocument();
        // No "running" indicator since task.status is completed
        expect(screen.queryByText('Claude is working...')).not.toBeInTheDocument();
      });
    });
  });

  describe('History load sets activeIteration correctly', () => {
    it('sets activeIteration to max loop_iteration from history', async () => {
      const historyMsgs: ChatMessage[] = [
        makeMsg({ id: 1, content: 'Iter 0', loop_iteration: 0 }),
        makeMsg({ id: 2, content: 'Iter 1', loop_iteration: 1 }),
        makeMsg({ id: 3, content: 'Iter 2 msg', loop_iteration: 2 }),
      ];
      vi.mocked(api.getTaskChatHistory).mockResolvedValue(historyMsgs);

      const task = makeTask({ status: 'executing' });
      render(<LoopChatView task={task} onBack={onBack} />);

      await waitFor(() => {
        // Iteration 3 header should show (0-indexed iteration 2)
        expect(screen.getByText('Iteration 3')).toBeInTheDocument();
        expect(screen.getByText('Iter 2 msg')).toBeInTheDocument();
      });
    });

    it('ignores an older completed lifecycle after reconnecting to a later iteration', async () => {
      const task = makeTask({
        provider: 'codex',
        status: 'executing',
        retry_count: 5,
        turn_generation: 13,
      });
      vi.mocked(api.getTaskChatHistory).mockResolvedValue([
        makeMsg({
          id: 20,
          content: 'Iteration one answer',
          loop_iteration: 0,
          task_retry_count: 5,
          task_turn_generation: 13,
        }),
        makeMsg({
          id: 21,
          role: 'system',
          event_type: 'background_lifecycle',
          content: null,
          loop_iteration: 0,
          task_retry_count: 5,
          task_turn_generation: 13,
          background_lifecycle: {
            state: 'completed',
            reason: 'descendants_completed',
            active_count: 0,
            active_thread_ids: [],
            started_at: '2026-08-18T12:00:00Z',
            last_activity_at: '2026-08-18T12:00:01Z',
          },
        }),
        makeMsg({
          id: 22,
          content: 'Iteration two resumed',
          loop_iteration: 1,
          task_retry_count: 5,
          task_turn_generation: 13,
        }),
      ]);

      render(<LoopChatView task={task} onBack={onBack} />);

      await waitFor(() => {
        expect(screen.getByText('Iteration two resumed')).toBeInTheDocument();
        expect(screen.getByText('Claude is working...')).toBeInTheDocument();
        expect(screen.getByRole('button', { name: /Cancel Loop/i })).toBeInTheDocument();
      });
      expect(screen.queryByText('Main loop response finished; background agents are still running')).not.toBeInTheDocument();
    });
  });

  describe('Detached background activity', () => {
    it('uses the Codex background lifecycle without keeping the loop iteration active', async () => {
      const task = makeTask({
        provider: 'codex',
        status: 'executing',
        background_active: false,
        retry_count: 2,
        turn_generation: 7,
      });
      vi.mocked(api.getTaskChatHistory).mockResolvedValue([
        makeMsg({
          id: 10,
          content: 'Foreground loop reply',
          task_retry_count: 2,
          task_turn_generation: 7,
        }),
      ]);

      render(<LoopChatView task={task} onBack={onBack} />);
      expect(await screen.findByText('Claude is working...')).toBeInTheDocument();

      act(() => {
        sendWs({
          event_type: 'background_lifecycle',
          background_state: 'running',
          background_reason: 'waiting_for_descendants',
          background_active_count: 1,
          background_active_thread_ids: ['child-loop-42'],
          background_started_at: new Date().toISOString(),
          background_last_activity_at: new Date().toISOString(),
          task_retry_count: 2,
          task_turn_generation: 7,
        });
      });

      await waitFor(() => {
        expect(screen.getByText('Main loop response finished; background agents are still running')).toBeInTheDocument();
        expect(screen.queryByText('Claude is working...')).not.toBeInTheDocument();
        expect(screen.queryByText('running', { selector: 'span' })).not.toBeInTheDocument();
        expect(screen.getByRole('button', { name: /Cancel Loop/i })).toBeInTheDocument();
      });
    });

    it('keeps a terminal task active and cancellable from its initial marker', async () => {
      vi.mocked(api.getTaskChatHistory).mockResolvedValue([
        makeMsg({ id: 1, content: 'Native child is still working' }),
      ]);

      render(
        <LoopChatView
          task={makeTask({ status: 'completed', background_active: true })}
          onBack={onBack}
        />,
      );

      await waitFor(() => {
        expect(screen.getByText('Main loop response finished; background agents are still running')).toBeInTheDocument();
        expect(screen.queryByText('Claude is working...')).not.toBeInTheDocument();
        expect(screen.getByRole('button', { name: /Cancel Loop/i })).toBeInTheDocument();
      });
      expect(capturedChannels).toEqual(['task:42', 'tasks']);

      fireEvent.click(screen.getByRole('button', { name: /Cancel Loop/i }));
      await waitFor(() => {
        expect(api.cancelTask).toHaveBeenCalledWith(42);
      });
    });

    it.each([
      {
        label: 'global tasks channel',
        channel: 'tasks',
        data: {
          event: 'background_activity',
          task_id: 42,
          background_active: true,
        },
      },
      {
        label: 'task channel',
        channel: 'task:42',
        data: {
          event_type: 'background_activity',
          background_active: true,
        },
      },
    ])('consumes background activity from the $label', async ({ channel, data }) => {
      vi.mocked(api.getTaskChatHistory).mockResolvedValue([
        makeMsg({ id: 2, content: 'Foreground answer' }),
      ]);
      render(
        <LoopChatView
          task={makeTask({ status: 'completed', background_active: false })}
          onBack={onBack}
        />,
      );
      await waitFor(() => {
        expect(screen.getByText('Foreground answer')).toBeInTheDocument();
      });
      expect(screen.queryByText('Claude is working...')).not.toBeInTheDocument();

      act(() => {
        sendWs(data, channel);
      });

      await waitFor(() => {
        expect(screen.getByText('Main loop response finished; background agents are still running')).toBeInTheDocument();
        expect(screen.queryByText('Claude is working...')).not.toBeInTheDocument();
        expect(screen.getByRole('button', { name: /Cancel Loop/i })).toBeInTheDocument();
      });
    });

    it('projects a current Codex lifecycle as background-only work', async () => {
      const task = makeTask({
        provider: 'codex',
        status: 'executing',
        background_active: false,
        retry_count: 2,
        turn_generation: 9,
      });
      vi.mocked(api.getTaskChatHistory).mockResolvedValue([
        makeMsg({ id: 4, content: 'Foreground answer', loop_iteration: 1 }),
      ]);
      render(<LoopChatView task={task} onBack={onBack} />);
      await screen.findByText('Foreground answer');
      expect(screen.getByText('Claude is working...')).toBeInTheDocument();

      act(() => {
        sendWs({
          event_type: 'background_lifecycle',
          background_state: 'running',
          background_reason: 'waiting_for_descendants',
          background_active_count: 1,
          background_active_thread_ids: ['codex-child'],
          background_started_at: '2026-08-18T12:00:00Z',
          background_last_activity_at: '2026-08-18T12:00:01Z',
          task_retry_count: 2,
          task_turn_generation: 9,
        });
      });

      await waitFor(() => {
        expect(screen.getByText('Main loop response finished; background agents are still running')).toBeInTheDocument();
        expect(screen.queryByText('Claude is working...')).not.toBeInTheDocument();
        expect(screen.getByRole('button', { name: /Cancel Loop/i })).toBeInTheDocument();
      });

      act(() => {
        sendWs({
          event_type: 'background_lifecycle',
          background_state: 'completed',
          background_reason: 'descendants_completed',
          background_active_count: 0,
          background_active_thread_ids: [],
          background_started_at: '2026-08-18T12:00:00Z',
          background_last_activity_at: '2026-08-18T12:00:02Z',
          task_retry_count: 2,
          task_turn_generation: 9,
        });
      });

      await waitFor(() => {
        expect(screen.queryByText('Main loop response finished; background agents are still running')).not.toBeInTheDocument();
        expect(screen.queryByText('Claude is working...')).not.toBeInTheDocument();
        expect(screen.queryByRole('button', { name: /Cancel Loop/i })).not.toBeInTheDocument();
      });
    });

    it('treats a later loop iteration as foreground after completion', async () => {
      const task = makeTask({
        provider: 'codex',
        status: 'executing',
        background_active: false,
        retry_count: 4,
        turn_generation: 12,
      });
      vi.mocked(api.getTaskChatHistory).mockResolvedValue([
        makeMsg({
          id: 6,
          content: 'Iteration one answer',
          loop_iteration: 0,
          task_retry_count: 4,
          task_turn_generation: 12,
        }),
      ]);
      render(<LoopChatView task={task} onBack={onBack} />);
      await screen.findByText('Iteration one answer');

      act(() => {
        sendWs({
          event_type: 'background_lifecycle',
          background_state: 'completed',
          background_reason: 'descendants_completed',
          background_active_count: 0,
          background_active_thread_ids: [],
          background_started_at: '2026-08-18T12:00:00Z',
          background_last_activity_at: '2026-08-18T12:00:02Z',
          loop_iteration: 0,
          task_retry_count: 4,
          task_turn_generation: 12,
        });
      });
      await waitFor(() => {
        expect(screen.queryByText('Claude is working...')).not.toBeInTheDocument();
      });

      act(() => {
        sendWs({
          event_type: 'message',
          role: 'assistant',
          content: 'Iteration two is active',
          loop_iteration: 1,
          task_retry_count: 4,
          task_turn_generation: 12,
        });
      });

      await waitFor(() => {
        expect(screen.getByText('Iteration two is active')).toBeInTheDocument();
        expect(screen.getByText('Claude is working...')).toBeInTheDocument();
        expect(screen.getByRole('button', { name: /Cancel Loop/i })).toBeInTheDocument();
      });
    });

    it('ignores a Codex lifecycle from a stale loop turn', async () => {
      const task = makeTask({
        provider: 'codex',
        status: 'executing',
        background_active: false,
        retry_count: 3,
        turn_generation: 10,
      });
      vi.mocked(api.getTaskChatHistory).mockResolvedValue([
        makeMsg({ id: 5, content: 'Current foreground', loop_iteration: 2 }),
      ]);
      render(<LoopChatView task={task} onBack={onBack} />);
      await screen.findByText('Current foreground');

      act(() => {
        sendWs({
          event_type: 'background_lifecycle',
          background_state: 'running',
          background_reason: 'waiting_for_descendants',
          background_active_count: 1,
          background_active_thread_ids: ['stale-child'],
          background_started_at: '2026-08-18T12:00:00Z',
          background_last_activity_at: '2026-08-18T12:00:01Z',
          task_retry_count: 3,
          task_turn_generation: 9,
        });
      });

      expect(screen.getByText('Claude is working...')).toBeInTheDocument();
      expect(screen.queryByText('Main loop response finished; background agents are still running')).not.toBeInTheDocument();
    });

    it('keeps the background hint and cancel action after terminal status without marking an iteration active', async () => {
      vi.mocked(api.getTaskChatHistory).mockResolvedValue([
        makeMsg({ id: 3, content: 'Foreground and child output' }),
      ]);
      render(<LoopChatView task={makeTask()} onBack={onBack} />);
      await waitFor(() => {
        expect(screen.getByText('Claude is working...')).toBeInTheDocument();
      });

      act(() => {
        sendWs({
          event: 'status_change',
          task_id: 42,
          new_status: 'completed',
          background_active: true,
        }, 'tasks');
      });
      expect(screen.getByText('Main loop response finished; background agents are still running')).toBeInTheDocument();
      expect(screen.queryByText('Claude is working...')).not.toBeInTheDocument();
      expect(screen.getByRole('button', { name: /Cancel Loop/i })).toBeInTheDocument();

      act(() => {
        sendWs({
          event: 'background_activity',
          task_id: 42,
          background_active: false,
        }, 'tasks');
      });
      await waitFor(() => {
        expect(screen.queryByText('Claude is working...')).not.toBeInTheDocument();
        expect(screen.queryByRole('button', { name: /Cancel Loop/i })).not.toBeInTheDocument();
      });
    });

    it('expires a WS terminal override after seven seconds without another prop change', () => {
      vi.useFakeTimers();
      const task = makeTask({
        status: 'in_progress',
        background_active: false,
      });
      const { rerender } = render(
        <LoopChatView task={task} onBack={onBack} />,
      );
      expect(
        screen.getByRole('button', { name: /Cancel Loop/i }),
      ).toBeInTheDocument();

      act(() => {
        sendWs({
          event: 'status_change',
          task_id: 42,
          new_status: 'completed',
          background_active: false,
        }, 'tasks');
      });
      expect(
        screen.queryByRole('button', { name: /Cancel Loop/i }),
      ).not.toBeInTheDocument();

      act(() => {
        vi.advanceTimersByTime(5000);
      });
      rerender(
        <LoopChatView
          task={{ ...task, status: 'executing' }}
          onBack={onBack}
        />,
      );
      // The stale poll must not override a fresher WS terminal event early.
      expect(
        screen.queryByRole('button', { name: /Cancel Loop/i }),
      ).not.toBeInTheDocument();

      act(() => {
        vi.advanceTimersByTime(1999);
      });
      expect(
        screen.queryByRole('button', { name: /Cancel Loop/i }),
      ).not.toBeInTheDocument();

      act(() => {
        vi.advanceTimersByTime(1);
      });
      // No further prop transition is needed: the local status expires on its
      // own and the still-executing durable snapshot becomes authoritative.
      expect(
        screen.getByRole('button', { name: /Cancel Loop/i }),
      ).toBeInTheDocument();
    });
  });
});
