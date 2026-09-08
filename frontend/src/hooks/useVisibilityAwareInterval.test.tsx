import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, cleanup, render } from '@testing-library/react';
import { useVisibilityAwareInterval } from './useVisibilityAwareInterval';

function Probe({ callback, interval = 1000, enabled = true }: {
  callback: () => void;
  interval?: number;
  enabled?: boolean;
}) {
  useVisibilityAwareInterval(callback, interval, enabled);
  return null;
}

describe('useVisibilityAwareInterval', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    Object.defineProperty(document, 'visibilityState', { configurable: true, value: 'visible' });
  });
  afterEach(() => {
    cleanup();
    vi.useRealTimers();
  });

  it('refreshes immediately and pauses hidden tabs', () => {
    const callback = vi.fn();
    render(<Probe callback={callback} />);
    expect(callback).toHaveBeenCalledTimes(1);
    act(() => vi.advanceTimersByTime(1000));
    expect(callback).toHaveBeenCalledTimes(2);

    Object.defineProperty(document, 'visibilityState', { configurable: true, value: 'hidden' });
    act(() => document.dispatchEvent(new Event('visibilitychange')));
    act(() => vi.advanceTimersByTime(5000));
    expect(callback).toHaveBeenCalledTimes(2);

    Object.defineProperty(document, 'visibilityState', { configurable: true, value: 'visible' });
    act(() => document.dispatchEvent(new Event('visibilitychange')));
    expect(callback).toHaveBeenCalledTimes(3);
  });

  it('does not run when disabled', () => {
    const callback = vi.fn();
    render(<Probe callback={callback} enabled={false} />);
    act(() => vi.advanceTimersByTime(5000));
    expect(callback).not.toHaveBeenCalled();
  });

  it('can skip the mount refresh but still refreshes after becoming visible', () => {
    const callback = vi.fn();
    function DeferredProbe() {
      useVisibilityAwareInterval(callback, 1000, true, false);
      return null;
    }
    render(<DeferredProbe />);
    expect(callback).not.toHaveBeenCalled();
    Object.defineProperty(document, 'visibilityState', { configurable: true, value: 'hidden' });
    act(() => document.dispatchEvent(new Event('visibilitychange')));
    Object.defineProperty(document, 'visibilityState', { configurable: true, value: 'visible' });
    act(() => document.dispatchEvent(new Event('visibilitychange')));
    expect(callback).toHaveBeenCalledTimes(1);
  });

  it('waits for an async refresh to settle before scheduling the next one', async () => {
    let resolveRefresh: (() => void) | undefined;
    const callback = vi.fn(() => new Promise<void>((resolve) => {
      resolveRefresh = resolve;
    }));
    render(<Probe callback={callback} />);
    await act(async () => {});
    expect(callback).toHaveBeenCalledTimes(1);

    await act(async () => vi.advanceTimersByTimeAsync(5000));
    expect(callback).toHaveBeenCalledTimes(1);

    await act(async () => resolveRefresh?.());
    await act(async () => vi.advanceTimersByTimeAsync(999));
    expect(callback).toHaveBeenCalledTimes(1);
    await act(async () => vi.advanceTimersByTimeAsync(1));
    expect(callback).toHaveBeenCalledTimes(2);
  });

  it('does not overlap a pending refresh when the interval changes', async () => {
    let resolveFirst: (() => void) | undefined;
    let first = true;
    const callback = vi.fn(() => {
      if (!first) return Promise.resolve();
      first = false;
      return new Promise<void>((resolve) => {
        resolveFirst = resolve;
      });
    });
    const { rerender } = render(<Probe callback={callback} interval={1000} />);
    expect(callback).toHaveBeenCalledTimes(1);

    rerender(<Probe callback={callback} interval={2000} />);
    await act(async () => vi.advanceTimersByTimeAsync(5000));
    expect(callback).toHaveBeenCalledTimes(1);

    await act(async () => resolveFirst?.());
    expect(callback).toHaveBeenCalledTimes(2);
  });
});
