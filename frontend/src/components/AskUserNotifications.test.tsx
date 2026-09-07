import { act, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { AskUserNotifications } from './AskUserNotifications';

vi.mock('../api/client', () => ({
  api: { getAskUserPendingAll: vi.fn() },
}));

let onSubscribed: ((channels: string[]) => void) | undefined;
vi.mock('../hooks/useWebSocket', () => ({
  useWebSocket: vi.fn((
    _channels: string[],
    _onMessage?: (message: Record<string, unknown>) => void,
    _onReconnect?: () => void,
    subscribed?: (channels: string[]) => void,
  ) => {
    onSubscribed = subscribed;
    return { isConnected: true };
  }),
}));

import { api } from '../api/client';

describe('AskUserNotifications member fallback', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    onSubscribed = undefined;
    vi.mocked(api.getAskUserPendingAll).mockResolvedValue({ pending: [] });
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.clearAllMocks();
  });

  it('discovers a new pending question through polling without a global WS event', async () => {
    render(<AskUserNotifications />);
    await act(async () => { await Promise.resolve(); });
    expect(screen.queryByText('Task #42 needs your input')).not.toBeInTheDocument();

    vi.mocked(api.getAskUserPendingAll).mockResolvedValue({
      pending: [{
        task_id: 42,
        request_id: 'ask-42',
        summary: 'Choose a deployment target',
      }],
    });
    await act(async () => { await vi.advanceTimersByTimeAsync(5000); });

    expect(screen.getByText('Task #42 needs your input')).toBeInTheDocument();
    expect(screen.getByText('Choose a deployment target')).toBeInTheDocument();
  });

  it('uses a 30 second fallback after the tasks channel is authorized', async () => {
    render(<AskUserNotifications />);
    await act(async () => { await Promise.resolve(); });
    expect(api.getAskUserPendingAll).toHaveBeenCalledTimes(1);

    act(() => onSubscribed?.(['tasks']));
    await act(async () => { await vi.advanceTimersByTimeAsync(5_000); });
    expect(api.getAskUserPendingAll).toHaveBeenCalledTimes(1);

    await act(async () => { await vi.advanceTimersByTimeAsync(25_000); });
    expect(api.getAskUserPendingAll).toHaveBeenCalledTimes(2);
  });
});
