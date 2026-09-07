import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, cleanup, render, screen } from '@testing-library/react';
import { useWebSocket } from './useWebSocket';

vi.mock('../config/server', () => ({ getWsUrl: () => 'ws://test' }));

class MockWebSocket {
  static OPEN = 1;
  readyState = 0;
  onopen: (() => void) | null = null;
  onmessage: ((event: { data: string }) => void) | null = null;
  onclose: (() => void) | null = null;

  send() {}

  close() {
    this.readyState = 3;
  }

  simulateOpen() {
    this.readyState = MockWebSocket.OPEN;
    this.onopen?.();
  }

  simulateClose() {
    this.readyState = 3;
    this.onclose?.();
  }

  simulateMessage(data: Record<string, unknown>) {
    this.onmessage?.({ data: JSON.stringify(data) });
  }
}

let sockets: MockWebSocket[] = [];

function ConnectionProbe({
  onReconnect,
  onSubscribed,
}: {
  onReconnect?: () => void;
  onSubscribed?: (channels: string[]) => void;
}) {
  const { isConnected } = useWebSocket(
    ['system'],
    undefined,
    onReconnect,
    onSubscribed,
  );
  return <div>{isConnected ? 'online' : 'offline'}</div>;
}

function MessageProbe({ channels, onMessage }: { channels: string[]; onMessage: (message: Record<string, unknown>) => void }) {
  useWebSocket(channels, onMessage);
  return null;
}

function SharedMessageProbes({ showTasks, tasks, workers }: {
  showTasks: boolean;
  tasks: (message: Record<string, unknown>) => void;
  workers: (message: Record<string, unknown>) => void;
}) {
  return (
    <>
      {showTasks && <MessageProbe key="tasks" channels={['tasks']} onMessage={tasks} />}
      <MessageProbe key="workers" channels={['workers']} onMessage={workers} />
    </>
  );
}

describe('useWebSocket connection state', () => {
  beforeEach(() => {
    sockets = [];
    vi.useFakeTimers();
    vi.stubGlobal('WebSocket', class extends MockWebSocket {
      constructor(url: string) {
        super();
        void url;
        sockets.push(this);
      }
    });
  });

  afterEach(() => {
    cleanup();
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it('keeps reconnect and subscribe-ACK callbacks distinct', () => {
    const onReconnect = vi.fn();
    const onSubscribed = vi.fn();
    render(
      <ConnectionProbe
        onReconnect={onReconnect}
        onSubscribed={onSubscribed}
      />,
    );
    expect(screen.getByText('offline')).toBeInTheDocument();

    act(() => sockets[0].simulateOpen());
    expect(screen.getByText('offline')).toBeInTheDocument();
    expect(onReconnect).not.toHaveBeenCalled();
    expect(onSubscribed).not.toHaveBeenCalled();

    act(() => sockets[0].simulateMessage({
      action: 'subscribed',
      channels: ['system'],
    }));
    expect(screen.getByText('online')).toBeInTheDocument();
    expect(onReconnect).not.toHaveBeenCalled();
    expect(onSubscribed).toHaveBeenCalledWith(['system']);

    act(() => sockets[0].simulateClose());
    expect(screen.getByText('offline')).toBeInTheDocument();

    act(() => vi.advanceTimersByTime(1000));
    expect(sockets).toHaveLength(2);
    act(() => sockets[1].simulateOpen());
    expect(onReconnect).toHaveBeenCalledTimes(1);
    expect(onSubscribed).toHaveBeenCalledTimes(1);

    act(() => sockets[1].simulateMessage({
      action: 'subscribed',
      channels: ['system'],
    }));
    expect(screen.getByText('online')).toBeInTheDocument();
    expect(onSubscribed).toHaveBeenCalledTimes(2);
  });

  it('shares one socket and routes channel messages to matching consumers', () => {
    const first = vi.fn();
    const second = vi.fn();
    const view = render(
      <SharedMessageProbes showTasks tasks={first} workers={second} />,
    );
    expect(sockets).toHaveLength(1);
    act(() => sockets[0].simulateOpen());
    act(() => sockets[0].simulateMessage({ action: 'subscribed', channels: ['tasks', 'workers'] }));
    act(() => sockets[0].simulateMessage({ channel: 'tasks', data: { event: 'task' } }));
    expect(first).toHaveBeenCalledTimes(1);
    expect(second).not.toHaveBeenCalled();
    view.unmount();
    expect(sockets[0].readyState).toBe(3);
  });

  it('keeps the shared socket alive when one consumer unmounts', () => {
    const first = vi.fn();
    const second = vi.fn();
    const view = render(
      <SharedMessageProbes showTasks tasks={first} workers={second} />,
    );
    expect(sockets).toHaveLength(1);
    act(() => sockets[0].simulateOpen());

    view.rerender(<SharedMessageProbes showTasks={false} tasks={first} workers={second} />);
    expect(sockets).toHaveLength(1);
    expect(sockets[0].readyState).toBe(MockWebSocket.OPEN);

    act(() => sockets[0].simulateMessage({ channel: 'tasks', data: {} }));
    act(() => sockets[0].simulateMessage({ channel: 'workers', data: {} }));
    expect(first).not.toHaveBeenCalled();
    expect(second).toHaveBeenCalledTimes(1);

    view.unmount();
    expect(sockets[0].readyState).toBe(3);
  });
});
