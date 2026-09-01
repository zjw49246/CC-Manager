import { useEffect, useRef } from 'react';

/**
 * Run a refresh while the document is visible. Hidden tabs do not keep a
 * timer alive, and returning to the tab performs one immediate refresh.
 */
export function useVisibilityAwareInterval(
  callback: () => void | Promise<void>,
  intervalMs: number,
  enabled = true,
  immediate = true,
) {
  const callbackRef = useRef(callback);
  const activeRunRef = useRef<Promise<void> | null>(null);
  useEffect(() => {
    callbackRef.current = callback;
  }, [callback]);

  useEffect(() => {
    if (!enabled) return;
    let disposed = false;
    let timer: ReturnType<typeof setTimeout> | null = null;
    let cyclePending = false;
    let refreshAfterPending = false;

    const stopTimer = () => {
      if (timer !== null) {
        clearTimeout(timer);
        timer = null;
      }
    };
    const schedule = () => {
      if (disposed || document.visibilityState === 'hidden' || timer !== null) return;
      timer = setTimeout(() => {
        timer = null;
        requestRefresh(false);
      }, intervalMs);
    };
    const finishRefresh = (run?: Promise<void>) => {
      if (run && activeRunRef.current === run) activeRunRef.current = null;
      cyclePending = false;
      if (disposed || document.visibilityState === 'hidden') return;
      if (refreshAfterPending) {
        refreshAfterPending = false;
        requestRefresh(false);
      } else {
        schedule();
      }
    };
    const executeRefresh = () => {
      if (disposed || document.visibilityState === 'hidden') {
        finishRefresh();
        return;
      }
      const activeRun = activeRunRef.current;
      if (activeRun) {
        void activeRun.then(executeRefresh, executeRefresh);
        return;
      }
      let result: void | Promise<void>;
      try {
        result = callbackRef.current();
      } catch (error) {
        console.error('Visibility-aware interval callback failed', error);
        finishRefresh();
        return;
      }
      if (result && typeof (result as Promise<void>).then === 'function') {
        const run = Promise.resolve(result);
        activeRunRef.current = run;
        void run.then(() => finishRefresh(run), (error) => {
          console.error('Visibility-aware interval callback failed', error);
          finishRefresh(run);
        });
      } else {
        finishRefresh();
      }
    };
    function requestRefresh(runAgainIfPending: boolean) {
      if (disposed || document.visibilityState === 'hidden') return;
      if (cyclePending) {
        if (runAgainIfPending) refreshAfterPending = true;
        return;
      }
      cyclePending = true;
      void executeRefresh();
    }
    const start = (refreshImmediately: boolean) => {
      // Some embedded/web-test documents report `prerender`; only an
      // explicitly hidden document should suspend a foreground refresh.
      if (document.visibilityState === 'hidden') return;
      if (refreshImmediately) requestRefresh(true);
      else schedule();
    };
    const onVisibilityChange = () => {
      if (document.visibilityState === 'visible') start(true);
      else {
        refreshAfterPending = false;
        stopTimer();
      }
    };

    document.addEventListener('visibilitychange', onVisibilityChange);
    start(immediate);
    return () => {
      disposed = true;
      document.removeEventListener('visibilitychange', onVisibilityChange);
      stopTimer();
    };
  }, [enabled, immediate, intervalMs]);
}
