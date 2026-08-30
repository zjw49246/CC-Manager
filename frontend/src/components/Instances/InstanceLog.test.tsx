import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';
import { InstanceLog } from './InstanceLog';
import type { LogEntry } from '../../api/client';

const apiMock = vi.hoisted(() => ({
  getInstanceLogs: vi.fn(),
}));

let messageHandler: ((message: Record<string, unknown>) => void) | undefined;
let subscribedHandler: (() => void) | undefined;

vi.mock('../../api/client', () => ({ api: apiMock }));
vi.mock('../../hooks/useWebSocket', () => ({
  useWebSocket: vi.fn((
    _channels: string[],
    onMessage?: (message: Record<string, unknown>) => void,
    _onReconnect?: () => void,
    onSubscribed?: () => void,
  ) => {
    messageHandler = onMessage;
    subscribedHandler = onSubscribed;
    return { lastMessage: null, isConnected: true };
  }),
}));

function log(id: number, content: string, overrides: Partial<LogEntry> = {}): LogEntry {
  return {
    id,
    instance_id: 7,
    task_id: 10,
    task_retry_count: 0,
    task_turn_generation: 1,
    native_turn_id: null,
    event_type: 'message',
    role: 'assistant',
    content,
    tool_name: null,
    tool_input: null,
    tool_output: null,
    is_error: false,
    timestamp: new Date(Date.UTC(2026, 6, 23) + id * 1000).toISOString(),
    ...overrides,
  };
}

describe('InstanceLog live/history merge', () => {
  beforeEach(() => {
    messageHandler = undefined;
    subscribedHandler = undefined;
  });

  afterEach(() => {
    cleanup();
    vi.clearAllMocks();
  });

  it('keeps every rapid WS event and does not let late history overwrite or duplicate it', async () => {
    let resolveHistory!: (entries: LogEntry[]) => void;
    apiMock.getInstanceLogs.mockReturnValueOnce(new Promise<LogEntry[]>((resolve) => {
      resolveHistory = resolve;
    }));
    render(<InstanceLog instanceId={7} onClose={vi.fn()} />);

    act(() => {
      messageHandler?.({
        channel: 'instance:7',
        data: { id: 2, instance_id: 7, task_id: 10, event_type: 'message', content: 'second', timestamp: '2026-07-23T00:00:02.000Z' },
      });
      // No server id: the local monotonic key must still retain both events
      // even when they arrive in the same millisecond/React batch.
      messageHandler?.({ channel: 'instance:7', data: { event_type: 'message', content: 'third-a' } });
      messageHandler?.({ channel: 'instance:7', data: { event_type: 'message', content: 'third-b' } });
    });

    expect(screen.getByText('second')).toBeInTheDocument();
    expect(screen.getByText('third-a')).toBeInTheDocument();
    expect(screen.getByText('third-b')).toBeInTheDocument();
    expect(screen.getAllByTestId('instance-log-entry').map((entry) => entry.textContent)).toEqual([
      expect.stringContaining('second'),
      expect.stringContaining('third-a'),
      expect.stringContaining('third-b'),
    ]);

    await act(async () => {
      // API order is newest-first. id=2 was already received over WS.
      resolveHistory([log(2, 'second'), log(1, 'first')]);
    });

    await waitFor(() => expect(screen.getAllByTestId('instance-log-entry')).toHaveLength(4));
    expect(screen.getAllByText('second')).toHaveLength(1);
    expect(screen.getByText('first')).toBeInTheDocument();
  });

  it('renders tool input and output returned by the history API', async () => {
    apiMock.getInstanceLogs.mockResolvedValueOnce([
      log(1, '', {
        event_type: 'tool_result',
        tool_name: 'Bash',
        tool_input: '{"command":"pwd"}',
        tool_output: '/workspace',
      }),
    ]);
    render(<InstanceLog instanceId={7} onClose={vi.fn()} />);

    expect(await screen.findByText('{"command":"pwd"}')).toBeInTheDocument();
    expect(screen.getByText('/workspace')).toBeInTheDocument();
    expect(screen.getByText('Bash')).toBeInTheDocument();
  });

  it('backfills history after a WebSocket reconnect', async () => {
    apiMock.getInstanceLogs.mockResolvedValue([]);
    render(<InstanceLog instanceId={7} onClose={vi.fn()} />);
    await waitFor(() => expect(apiMock.getInstanceLogs).toHaveBeenCalledTimes(1));

    act(() => subscribedHandler?.());
    await waitFor(() => expect(apiMock.getInstanceLogs).toHaveBeenCalledTimes(2));
    expect(apiMock.getInstanceLogs).toHaveBeenLastCalledWith(7, 200, 0);
  });

  it('waits for the bounded initial snapshot when subscribe ACK arrives first', async () => {
    let resolveInitial!: (entries: LogEntry[]) => void;
    apiMock.getInstanceLogs
      .mockReturnValueOnce(new Promise<LogEntry[]>((resolve) => {
        resolveInitial = resolve;
      }))
      .mockResolvedValueOnce([log(2, 'between-snapshot-and-ack')]);

    render(<InstanceLog instanceId={7} onClose={vi.fn()} />);
    act(() => subscribedHandler?.());
    await act(async () => Promise.resolve());

    // The ACK must not issue an unbounded after_id=0 request while the
    // latest-200 baseline is still unresolved.
    expect(apiMock.getInstanceLogs).toHaveBeenCalledTimes(1);
    await act(async () => resolveInitial([log(1, 'snapshot')]));

    expect(await screen.findByText('snapshot')).toBeInTheDocument();
    expect(await screen.findByText('between-snapshot-and-ack')).toBeInTheDocument();
    expect(apiMock.getInstanceLogs.mock.calls).toEqual([
      [7, 200],
      [7, 200, 1],
    ]);
  });

  it('does not let a high live id skip persisted rows before it', async () => {
    apiMock.getInstanceLogs
      .mockResolvedValueOnce([log(90, 'snapshot-90')])
      .mockResolvedValueOnce([
        log(91, 'gap-91'),
        log(92, 'gap-92'),
        log(100, 'live-100'),
      ]);

    render(<InstanceLog instanceId={7} onClose={vi.fn()} />);
    expect(await screen.findByText('snapshot-90')).toBeInTheDocument();

    act(() => {
      messageHandler?.({
        channel: 'instance:7',
        data: {
          id: 100,
          instance_id: 7,
          task_id: 10,
          event_type: 'message',
          content: 'live-100',
          timestamp: '2026-07-23T00:01:40.000Z',
        },
      });
      subscribedHandler?.();
    });

    expect(await screen.findByText('gap-91')).toBeInTheDocument();
    expect(screen.getByText('gap-92')).toBeInTheDocument();
    expect(screen.getAllByText('live-100')).toHaveLength(1);
    expect(apiMock.getInstanceLogs).toHaveBeenLastCalledWith(7, 200, 90);
  });

  it('queues a new cursor pass for every subscribe ACK instead of coalescing it into an older pass', async () => {
    let resolveFirstAck!: (entries: LogEntry[]) => void;
    apiMock.getInstanceLogs
      .mockResolvedValueOnce([log(1, 'snapshot')])
      .mockReturnValueOnce(new Promise<LogEntry[]>((resolve) => {
        resolveFirstAck = resolve;
      }))
      .mockResolvedValueOnce([log(2, 'second-ack-recovery')]);

    render(<InstanceLog instanceId={7} onClose={vi.fn()} />);
    expect(await screen.findByText('snapshot')).toBeInTheDocument();

    act(() => subscribedHandler?.());
    await waitFor(() => expect(apiMock.getInstanceLogs).toHaveBeenCalledTimes(2));
    // Another reconnect/subscribe ACK can arrive while the previous cursor
    // request still represents an older server snapshot.
    act(() => subscribedHandler?.());
    expect(apiMock.getInstanceLogs).toHaveBeenCalledTimes(2);

    await act(async () => resolveFirstAck([]));

    expect(await screen.findByText('second-ack-recovery')).toBeInTheDocument();
    expect(apiMock.getInstanceLogs.mock.calls).toEqual([
      [7, 200],
      [7, 200, 1],
      [7, 200, 1],
    ]);
  });

  it('loops through cursor pages after reconnect so gaps larger than one page are complete', async () => {
    const firstPage = Array.from(
      { length: 200 },
      (_, index) => log(index + 11, `recovered-${index + 11}`),
    );
    const finalPage = [
      log(211, 'recovered-211'),
      log(212, 'recovered-212'),
    ];
    apiMock.getInstanceLogs
      .mockResolvedValueOnce([log(10, 'before-disconnect')])
      .mockResolvedValueOnce(firstPage)
      .mockResolvedValueOnce(finalPage);

    render(<InstanceLog instanceId={7} onClose={vi.fn()} />);
    expect(await screen.findByText('before-disconnect')).toBeInTheDocument();

    act(() => subscribedHandler?.());
    await waitFor(() => expect(apiMock.getInstanceLogs).toHaveBeenCalledTimes(3));
    expect(apiMock.getInstanceLogs.mock.calls).toEqual([
      [7, 200],
      [7, 200, 10],
      [7, 200, 210],
    ]);
    expect(await screen.findByText('recovered-11')).toBeInTheDocument();
    expect(screen.getByText('recovered-212')).toBeInTheDocument();
    expect(screen.getAllByTestId('instance-log-entry')).toHaveLength(203);
  });

  it('aggregates Codex deltas by item id and replaces them with the persisted final item', async () => {
    apiMock.getInstanceLogs.mockResolvedValueOnce([]);
    render(<InstanceLog instanceId={7} onClose={vi.fn()} />);
    await waitFor(() => expect(apiMock.getInstanceLogs).toHaveBeenCalled());

    act(() => {
      messageHandler?.({
        channel: 'instance:7',
        data: {
          event_type: 'message_delta',
          item_id: 'msg-1',
          content: 'Hel',
          task_id: 10,
          task_retry_count: 0,
          task_turn_generation: 1,
        },
      });
      messageHandler?.({
        channel: 'instance:7',
        data: {
          event_type: 'message_delta',
          item_id: 'msg-1',
          content: 'lo',
          task_id: 10,
          task_retry_count: 0,
          task_turn_generation: 1,
        },
      });
    });

    expect(screen.getAllByText('Hello')).toHaveLength(1);
    expect(screen.getAllByTestId('instance-log-entry')).toHaveLength(1);
    expect(screen.getByText('[message_delta]')).toBeInTheDocument();

    act(() => {
      messageHandler?.({
        channel: 'instance:7',
        data: {
          id: 42,
          instance_id: 7,
          task_id: 10,
          event_type: 'message',
          role: 'assistant',
          item_id: 'msg-1',
          content: 'Hello',
          task_retry_count: 0,
          task_turn_generation: 1,
          timestamp: '2026-07-23T00:01:00.000Z',
        },
      });
    });

    expect(screen.getAllByText('Hello')).toHaveLength(1);
    expect(screen.getAllByTestId('instance-log-entry')).toHaveLength(1);
    expect(screen.getByText('[message]')).toBeInTheDocument();
    expect(screen.queryByText('[message_delta]')).not.toBeInTheDocument();
  });

  it('drops deltas that do not carry an exact task retry and turn identity', async () => {
    apiMock.getInstanceLogs.mockResolvedValueOnce([]);
    render(<InstanceLog instanceId={7} onClose={vi.fn()} />);
    await waitFor(() => expect(apiMock.getInstanceLogs).toHaveBeenCalled());

    act(() => {
      messageHandler?.({
        channel: 'instance:7',
        data: {
          event_type: 'message_delta', item_id: 'missing-retry',
          task_id: 10, task_turn_generation: 1, content: 'missing retry',
        },
      });
      messageHandler?.({
        channel: 'instance:7',
        data: {
          event_type: 'thinking_delta', item_id: 'missing-turn',
          task_id: 10, task_retry_count: 0, content: 'missing turn',
        },
      });
    });

    expect(screen.queryByText('missing retry')).not.toBeInTheDocument();
    expect(screen.queryByText('missing turn')).not.toBeInTheDocument();
    expect(screen.getByText('No logs yet')).toBeInTheDocument();
  });

  it('does not merge a reused item id across task turns or accept a late old delta', async () => {
    apiMock.getInstanceLogs.mockResolvedValueOnce([]);
    render(<InstanceLog instanceId={7} onClose={vi.fn()} />);
    await waitFor(() => expect(apiMock.getInstanceLogs).toHaveBeenCalled());

    act(() => {
      messageHandler?.({
        channel: 'instance:7',
        data: {
          event_type: 'message_delta', item_id: 'same-item', content: 'turn one partial',
          task_id: 10, task_retry_count: 5, task_turn_generation: 30,
        },
      });
      messageHandler?.({
        channel: 'instance:7',
        data: {
          event_type: 'message_delta', item_id: 'same-item', content: 'turn two partial',
          task_id: 10, task_retry_count: 5, task_turn_generation: 31,
        },
      });
      messageHandler?.({
        channel: 'instance:7',
        data: {
          event_type: 'message_delta', item_id: 'same-item', content: ' late turn one',
          task_id: 10, task_retry_count: 5, task_turn_generation: 30,
        },
      });
    });

    expect(screen.queryByText('turn one partial')).not.toBeInTheDocument();
    expect(screen.queryByText(/late turn one/)).not.toBeInTheDocument();
    expect(screen.getByText('turn two partial')).toBeInTheDocument();

    act(() => {
      messageHandler?.({
        channel: 'instance:7',
        data: {
          id: 80, instance_id: 7, task_id: 10,
          task_retry_count: 5, task_turn_generation: 30,
          event_type: 'message', role: 'assistant', item_id: 'same-item',
          content: 'turn one persisted', timestamp: '2026-07-23T00:02:00.000Z',
        },
      });
    });
    expect(screen.getByText('turn one persisted')).toBeInTheDocument();
    expect(screen.getByText('turn two partial')).toBeInTheDocument();

    act(() => {
      messageHandler?.({
        channel: 'instance:7',
        data: {
          id: 81, instance_id: 7, task_id: 10,
          task_retry_count: 5, task_turn_generation: 31,
          event_type: 'message', role: 'assistant', item_id: 'same-item',
          content: 'turn two persisted', timestamp: '2026-07-23T00:02:01.000Z',
        },
      });
    });
    expect(screen.queryByText('turn two partial')).not.toBeInTheDocument();
    expect(screen.getByText('turn two persisted')).toBeInTheDocument();
    expect(screen.getByText('turn one persisted')).toBeInTheDocument();
  });

  it('aggregates thinking deltas independently by their item id', async () => {
    apiMock.getInstanceLogs.mockResolvedValueOnce([]);
    render(<InstanceLog instanceId={7} onClose={vi.fn()} />);
    await waitFor(() => expect(apiMock.getInstanceLogs).toHaveBeenCalled());

    act(() => {
      messageHandler?.({
        channel: 'instance:7',
        data: {
          event_type: 'thinking_delta',
          item_id: 'reasoning-1',
          content: 'Check ',
          task_id: 10,
          task_retry_count: 0,
          task_turn_generation: 1,
        },
      });
      messageHandler?.({
        channel: 'instance:7',
        data: {
          event_type: 'thinking_delta',
          item_id: 'reasoning-1',
          content: 'state',
          task_id: 10,
          task_retry_count: 0,
          task_turn_generation: 1,
        },
      });
    });
    expect(screen.getAllByText('Check state')).toHaveLength(1);

    act(() => {
      messageHandler?.({
        channel: 'instance:7',
        data: {
          id: 43,
          instance_id: 7,
          task_id: 10,
          event_type: 'thinking',
          role: 'assistant',
          item_id: 'reasoning-1',
          content: 'Check state',
          task_retry_count: 0,
          task_turn_generation: 1,
        },
      });
    });
    expect(screen.getAllByText('Check state')).toHaveLength(1);
    expect(screen.getByText('[thinking]')).toBeInTheDocument();
    expect(screen.queryByText('[thinking_delta]')).not.toBeInTheDocument();
  });

  it('uses a cursor-backfilled final item to replace a delta left by disconnect', async () => {
    apiMock.getInstanceLogs.mockResolvedValueOnce([]);
    render(<InstanceLog instanceId={7} onClose={vi.fn()} />);
    await waitFor(() => expect(apiMock.getInstanceLogs).toHaveBeenCalledTimes(1));

    act(() => {
      messageHandler?.({
        channel: 'instance:7',
        data: {
          event_type: 'message_delta',
          item_id: 'disconnected-message',
          content: 'part',
          task_id: 10,
          task_retry_count: 0,
          task_turn_generation: 1,
        },
      });
    });
    expect(screen.getByText('part')).toBeInTheDocument();
    expect(screen.getByText('[message_delta]')).toBeInTheDocument();

    apiMock.getInstanceLogs.mockResolvedValueOnce([
      log(52, 'complete', { item_id: 'disconnected-message' }),
    ]);
    act(() => subscribedHandler?.());

    expect(await screen.findByText('complete')).toBeInTheDocument();
    await waitFor(() => {
      expect(screen.getAllByTestId('instance-log-entry')).toHaveLength(1);
    });
    expect(screen.queryByText('part')).not.toBeInTheDocument();
    expect(screen.queryByText('[message_delta]')).not.toBeInTheDocument();
    expect(screen.getByText('[message]')).toBeInTheDocument();
    expect(apiMock.getInstanceLogs).toHaveBeenLastCalledWith(7, 200, 0);
  });

  it('shows history loading failures without breaking the live stream', async () => {
    apiMock.getInstanceLogs.mockRejectedValueOnce(new Error('forbidden'));
    render(<InstanceLog instanceId={7} onClose={vi.fn()} />);

    expect(await screen.findByRole('alert')).toHaveTextContent('forbidden');
    act(() => {
      messageHandler?.({ channel: 'instance:7', data: { event_type: 'message', content: 'still live' } });
    });
    expect(screen.getByText('still live')).toBeInTheDocument();
  });

  it('cancels a throttled auto-scroll when the user leaves the bottom', async () => {
    vi.useFakeTimers();
    const originalScrollIntoView = Object.getOwnPropertyDescriptor(
      HTMLElement.prototype,
      'scrollIntoView',
    );
    const scrollIntoView = vi.fn();
    Object.defineProperty(HTMLElement.prototype, 'scrollIntoView', {
      configurable: true,
      value: scrollIntoView,
    });
    apiMock.getInstanceLogs.mockResolvedValueOnce([]);

    try {
      render(<InstanceLog instanceId={7} onClose={vi.fn()} />);
      await act(async () => Promise.resolve());
      act(() => vi.advanceTimersByTime(50));
      scrollIntoView.mockClear();

      const container = screen.getByText('No logs yet').parentElement;
      expect(container).not.toBeNull();
      Object.defineProperties(container!, {
        scrollHeight: { configurable: true, value: 1000 },
        scrollTop: { configurable: true, value: 0 },
        clientHeight: { configurable: true, value: 100 },
      });

      act(() => {
        messageHandler?.({
          channel: 'instance:7',
          data: { event_type: 'message', content: 'new live row' },
        });
      });
      fireEvent.scroll(container!);
      act(() => vi.advanceTimersByTime(50));

      expect(scrollIntoView).not.toHaveBeenCalled();
      expect(screen.getByText('new live row')).toBeInTheDocument();
    } finally {
      if (originalScrollIntoView) {
        Object.defineProperty(
          HTMLElement.prototype,
          'scrollIntoView',
          originalScrollIntoView,
        );
      } else {
        delete (HTMLElement.prototype as Partial<HTMLElement>).scrollIntoView;
      }
      vi.useRealTimers();
    }
  });
});
