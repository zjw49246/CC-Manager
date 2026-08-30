import { useState, useEffect, useRef, useCallback } from 'react';
import { api } from '../../api/client';
import type { LogEntry } from '../../api/client';
import { useWebSocket } from '../../hooks/useWebSocket';
import { X } from '../icons';

interface InstanceLogProps {
  instanceId: number;
  onClose: () => void;
}

type InstanceLogItem = LogEntry & {
  clientKey: string;
  sortSequence: number;
  streamItemId?: string;
  provisional?: boolean;
};

type LogTaskTurnIdentity = {
  taskId: number;
  retryCount: number;
  turnGeneration: number;
};

type HistoryState = {
  instanceId: number;
  cursor: number;
  snapshotEstablished: boolean;
  initialLoad: Promise<void>;
  resolveInitialLoad: () => void;
};

const MAX_VISIBLE_LOGS = 2000;
const BACKFILL_PAGE_SIZE = 200;
const AUTO_FOLLOW_THRESHOLD_PX = 80;
const SCROLL_THROTTLE_MS = 50;

function logTaskTurnIdentity(
  value: Pick<LogEntry, 'task_id' | 'task_retry_count' | 'task_turn_generation'>,
): LogTaskTurnIdentity | null {
  if (
    value.task_id == null
    || !Number.isInteger(value.task_retry_count)
    || (value.task_retry_count as number) < 0
    || !Number.isInteger(value.task_turn_generation)
    || (value.task_turn_generation as number) < 0
  ) {
    return null;
  }
  return {
    taskId: value.task_id,
    retryCount: value.task_retry_count as number,
    turnGeneration: value.task_turn_generation as number,
  };
}

function sameLogTaskTurn(
  left: LogTaskTurnIdentity,
  right: LogTaskTurnIdentity,
): boolean {
  return (
    left.taskId === right.taskId
    && left.retryCount === right.retryCount
    && left.turnGeneration === right.turnGeneration
  );
}

function compareLogTaskTurn(
  left: LogTaskTurnIdentity,
  right: LogTaskTurnIdentity,
): number {
  if (left.taskId !== right.taskId) return 0;
  if (left.turnGeneration !== right.turnGeneration) {
    return left.turnGeneration - right.turnGeneration;
  }
  return left.retryCount - right.retryCount;
}

function streamClientKey(
  identity: LogTaskTurnIdentity,
  itemId: string,
): string {
  return `delta:${identity.taskId}:${identity.retryCount}:${identity.turnGeneration}:${itemId}`;
}

function rememberLatestTaskTurns(
  latestByTask: Map<number, LogTaskTurnIdentity>,
  entries: LogEntry[],
): void {
  for (const entry of entries) {
    const identity = logTaskTurnIdentity(entry);
    if (!identity) continue;
    const current = latestByTask.get(identity.taskId);
    if (!current || compareLogTaskTurn(identity, current) > 0) {
      latestByTask.set(identity.taskId, identity);
    }
  }
}

function removeSupersededProvisionals(
  items: InstanceLogItem[],
  latestByTask: Map<number, LogTaskTurnIdentity>,
): InstanceLogItem[] {
  return items.filter((item) => {
    if (!item.provisional) return true;
    const identity = logTaskTurnIdentity(item);
    if (!identity) return false;
    const latest = latestByTask.get(identity.taskId);
    return !latest || sameLogTaskTurn(identity, latest);
  });
}

function historyItem(entry: LogEntry): InstanceLogItem {
  return {
    ...entry,
    clientKey: `db:${entry.id}`,
    sortSequence: entry.id,
    streamItemId: entry.item_id || undefined,
  };
}

function trimItems(items: InstanceLogItem[]): InstanceLogItem[] {
  return items.length > MAX_VISIBLE_LOGS ? items.slice(-MAX_VISIBLE_LOGS) : items;
}

function createHistoryState(instanceId: number): HistoryState {
  let resolveInitialLoad = () => {};
  const initialLoad = new Promise<void>((resolve) => {
    resolveInitialLoad = resolve;
  });
  return {
    instanceId,
    cursor: 0,
    snapshotEstablished: false,
    initialLoad,
    resolveInitialLoad,
  };
}

// History arrives a page at a time and can race with authoritative WS rows.
// Sorting here is acceptable; high-frequency token deltas use the O(n)
// update/append paths below and never rebuild a Map or sort the full log.
function mergeHistoryItems(
  current: InstanceLogItem[],
  incoming: InstanceLogItem[],
): InstanceLogItem[] {
  let merged = current;
  for (const entry of incoming) {
    merged = applyAuthoritativeItem(merged, entry);
  }
  return trimItems([...merged]
    .sort((a, b) => {
      const timeDiff = new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime();
      if (timeDiff !== 0) return timeDiff;
      return a.sortSequence - b.sortSequence;
    }));
}

function appendItem(
  current: InstanceLogItem[],
  entry: InstanceLogItem,
): InstanceLogItem[] {
  return trimItems([...current, entry]);
}

function matchesProvisional(
  candidate: InstanceLogItem,
  entry: InstanceLogItem,
): boolean {
  if (
    !candidate.provisional
    || !candidate.streamItemId
    || candidate.streamItemId !== entry.streamItemId
  ) {
    return false;
  }
  const candidateIdentity = logTaskTurnIdentity(candidate);
  const entryIdentity = logTaskTurnIdentity(entry);
  if (
    !candidateIdentity
    || !entryIdentity
    || !sameLogTaskTurn(candidateIdentity, entryIdentity)
  ) {
    return false;
  }
  return (
    (candidate.event_type === 'message_delta' && entry.event_type === 'message')
    || (
      candidate.event_type === 'thinking_delta'
      && entry.event_type === 'thinking'
    )
  );
}

function applyAuthoritativeItem(
  current: InstanceLogItem[],
  entry: InstanceLogItem,
): InstanceLogItem[] {
  const streamItemId = entry.streamItemId;
  if (streamItemId) {
    const provisional = current.find(
      (candidate) => matchesProvisional(candidate, entry),
    );
    if (provisional) {
      let inserted = false;
      const next: InstanceLogItem[] = [];
      for (const candidate of current) {
        if (
          candidate.clientKey === entry.clientKey
          || matchesProvisional(candidate, entry)
        ) {
          if (!inserted && candidate.provisional) {
            next.push({
              ...entry,
              // Keep the live bubble in place when the persisted final item
              // replaces it; a later history merge can restore DB ordering.
              sortSequence: provisional.sortSequence,
            });
            inserted = true;
          }
          continue;
        }
        next.push(candidate);
      }
      if (!inserted) next.push(entry);
      return trimItems(next);
    }
  }

  const persistedIndex = current.findIndex(
    (candidate) => candidate.clientKey === entry.clientKey,
  );
  if (persistedIndex < 0) return appendItem(current, entry);
  const next = [...current];
  next[persistedIndex] = { ...next[persistedIndex], ...entry };
  return next;
}

function displayValue(value: unknown): string | null {
  if (value == null || value === '') return null;
  return typeof value === 'string' ? value : JSON.stringify(value, null, 2);
}

export function InstanceLog({ instanceId, onClose }: InstanceLogProps) {
  const [logs, setLogs] = useState<InstanceLogItem[]>([]);
  const [loadError, setLoadError] = useState<string | null>(null);
  const bottomRef = useRef<HTMLDivElement>(null);
  const scrollContainerRef = useRef<HTMLDivElement>(null);
  const scrollTimerRef = useRef<number | null>(null);
  const shouldAutoFollowRef = useRef(true);
  const liveSequence = useRef(0);
  const latestTaskTurnsRef = useRef(new Map<number, LogTaskTurnIdentity>());
  const activeInstanceRef = useRef(instanceId);
  const backfillTailRef = useRef<Promise<void>>(Promise.resolve());
  const historyStateRef = useRef<HistoryState | null>(null);
  if (historyStateRef.current?.instanceId !== instanceId) {
    historyStateRef.current = createHistoryState(instanceId);
  }

  const backfillHistory = useCallback((): Promise<void> => {
    const requestedInstanceId = instanceId;
    const requestedHistoryState = historyStateRef.current!;
    const run = async () => {
      // The subscribe ACK can beat the initial latest-page request. Wait for
      // that bounded snapshot so an ACK never starts an `after_id=0` scan over
      // an existing full history. A failed snapshot has no safe baseline.
      await requestedHistoryState.initialLoad;
      if (
        activeInstanceRef.current !== requestedInstanceId
        || historyStateRef.current !== requestedHistoryState
        || !requestedHistoryState.snapshotEstablished
      ) {
        return;
      }

      let cursor = requestedHistoryState.cursor;
      let recovered: LogEntry[] = [];
      try {
        while (
          activeInstanceRef.current === requestedInstanceId
          && historyStateRef.current === requestedHistoryState
        ) {
          const page = await api.getInstanceLogs(
            requestedInstanceId,
            BACKFILL_PAGE_SIZE,
            cursor,
          );
          if (page.length === 0) break;

          const nextCursor = page.reduce(
            (highest, entry) => Math.max(highest, entry.id),
            cursor,
          );
          // A malformed/non-advancing page must not create an infinite loop.
          if (nextCursor <= cursor) break;
          cursor = nextCursor;
          requestedHistoryState.cursor = Math.max(
            requestedHistoryState.cursor,
            nextCursor,
          );
          recovered = [...recovered, ...page].slice(-MAX_VISIBLE_LOGS);
          if (page.length < BACKFILL_PAGE_SIZE) break;
        }
        if (activeInstanceRef.current === requestedInstanceId) {
          setLoadError(null);
        }
      } catch (error) {
        if (activeInstanceRef.current === requestedInstanceId) {
          setLoadError(error instanceof Error ? error.message : String(error));
        }
      } finally {
        if (
          recovered.length > 0
          && activeInstanceRef.current === requestedInstanceId
        ) {
          rememberLatestTaskTurns(latestTaskTurnsRef.current, recovered);
          setLogs((current) => mergeHistoryItems(
            removeSupersededProvisionals(current, latestTaskTurnsRef.current),
            recovered.map(historyItem),
          ));
        }
      }
    };

    // Every subscribe ACK queues a fresh cursor pass.  Do not coalesce a new
    // ACK into an older request that began before the subscription became
    // active, or its final HTTP snapshot can still leave a tail gap.
    const queued = backfillTailRef.current
      .catch(() => {})
      .then(run);
    backfillTailRef.current = queued;
    return queued;
  }, [instanceId]);

  const handleScroll = useCallback(() => {
    const container = scrollContainerRef.current;
    if (!container) return;
    shouldAutoFollowRef.current = (
      container.scrollHeight - container.scrollTop - container.clientHeight
      <= AUTO_FOLLOW_THRESHOLD_PX
    );
    if (!shouldAutoFollowRef.current && scrollTimerRef.current != null) {
      window.clearTimeout(scrollTimerRef.current);
      scrollTimerRef.current = null;
    }
  }, []);

  const handleMessage = useCallback((rawMessage: Record<string, unknown>) => {
    const msg = rawMessage as { channel?: string; data?: Record<string, unknown> };
    if (msg.channel !== `instance:${instanceId}` || !msg.data) return;
    const data = msg.data;
    const persistedId = typeof data.id === 'number' ? data.id : null;
    const eventType = typeof data.event_type === 'string'
      ? data.event_type
      : 'unknown';
    const streamItemId = typeof data.item_id === 'string'
      ? data.item_id
      : undefined;
    const sequence = ++liveSequence.current;
    const localId = -sequence;
    const entry: InstanceLogItem = {
      id: persistedId ?? localId,
      clientKey: persistedId == null
        ? `live:${instanceId}:${sequence}`
        : `db:${persistedId}`,
      sortSequence: sequence,
      streamItemId,
      instance_id: typeof data.instance_id === 'number' ? data.instance_id : instanceId,
      task_id: typeof data.task_id === 'number' ? data.task_id : null,
      task_retry_count: typeof data.task_retry_count === 'number'
        ? data.task_retry_count
        : null,
      task_turn_generation: typeof data.task_turn_generation === 'number'
        ? data.task_turn_generation
        : null,
      native_turn_id: typeof data.native_turn_id === 'string'
        ? data.native_turn_id
        : null,
      event_type: eventType,
      role: typeof data.role === 'string' ? data.role : null,
      content: displayValue(data.content)
        ?? displayValue(data.stderr)
        ?? (typeof data.exit_code === 'number' ? `exit code ${data.exit_code}` : null),
      tool_name: typeof data.tool_name === 'string' ? data.tool_name : null,
      tool_input: displayValue(data.tool_input),
      tool_output: displayValue(data.tool_output),
      is_error: data.is_error === true,
      timestamp: typeof data.timestamp === 'string' ? data.timestamp : new Date().toISOString(),
    };

    if (eventType === 'message_delta' || eventType === 'thinking_delta') {
      const identity = logTaskTurnIdentity(entry);
      if (!entry.content || !streamItemId || !identity) return;
      const latest = latestTaskTurnsRef.current.get(identity.taskId);
      const comparison = latest ? compareLogTaskTurn(identity, latest) : 1;
      if (comparison < 0) return;
      if (comparison > 0) {
        latestTaskTurnsRef.current.set(identity.taskId, identity);
      }
      entry.clientKey = streamClientKey(identity, streamItemId);
      entry.provisional = true;
      setLogs((current) => {
        const generationCurrent = comparison > 0
          ? removeSupersededProvisionals(current, latestTaskTurnsRef.current)
          : current;
        const finalShape = {
          ...entry,
          event_type: eventType === 'message_delta' ? 'message' : 'thinking',
        };
        const index = generationCurrent.findIndex((candidate) => (
          candidate.provisional === true
          && candidate.streamItemId === streamItemId
          && matchesProvisional(candidate, finalShape)
        ));
        if (index < 0) return appendItem(generationCurrent, entry);
        const next = [...generationCurrent];
        next[index] = {
          ...next[index],
          content: `${next[index].content || ''}${entry.content}`,
        };
        return next;
      });
      return;
    }

    if (persistedId != null) {
      const identity = logTaskTurnIdentity(entry);
      let comparison = 0;
      if (identity) {
        const latest = latestTaskTurnsRef.current.get(identity.taskId);
        comparison = latest ? compareLogTaskTurn(identity, latest) : 1;
        if (comparison > 0) {
          latestTaskTurnsRef.current.set(identity.taskId, identity);
        }
      }
      // A live row proves only that this one id was delivered. It must not
      // advance the contiguous HTTP cursor because rows immediately before it
      // may have fallen into the subscribe/reconnect window.
      setLogs((current) => {
        const generationCurrent = comparison > 0 && identity
          ? removeSupersededProvisionals(current, latestTaskTurnsRef.current)
          : current;
        return applyAuthoritativeItem(generationCurrent, entry);
      });
      return;
    }
    setLogs((current) => appendItem(current, entry));
  }, [instanceId]);

  const { isConnected } = useWebSocket(
    [`instance:${instanceId}`],
    handleMessage,
    undefined,
    () => { void backfillHistory(); },
  );

  useEffect(() => {
    activeInstanceRef.current = instanceId;
    liveSequence.current = 0;
    latestTaskTurnsRef.current.clear();
    shouldAutoFollowRef.current = true;
    setLogs([]);
    setLoadError(null);
    const historyState = historyStateRef.current!;
    let active = true;
    api.getInstanceLogs(instanceId, BACKFILL_PAGE_SIZE)
      .then((entries) => {
        if (!active || activeInstanceRef.current !== instanceId) return;
        rememberLatestTaskTurns(latestTaskTurnsRef.current, entries);
        historyState.cursor = entries.reduce(
          (highest, entry) => Math.max(highest, entry.id),
          historyState.cursor,
        );
        historyState.snapshotEstablished = true;
        setLogs((current) => mergeHistoryItems(
          removeSupersededProvisionals(current, latestTaskTurnsRef.current),
          entries.slice().reverse().map(historyItem),
        ));
        setLoadError(null);
      })
      .catch((error) => {
        if (active && activeInstanceRef.current === instanceId) {
          setLoadError(error instanceof Error ? error.message : String(error));
        }
      })
      .finally(() => {
        historyState.resolveInitialLoad();
      });
    return () => {
      active = false;
      if (activeInstanceRef.current === instanceId) {
        activeInstanceRef.current = -1;
      }
    };
  }, [instanceId]);

  useEffect(() => {
    if (!shouldAutoFollowRef.current || scrollTimerRef.current != null) return;
    scrollTimerRef.current = window.setTimeout(() => {
      scrollTimerRef.current = null;
      if (shouldAutoFollowRef.current) {
        bottomRef.current?.scrollIntoView({ block: 'end' });
      }
    }, SCROLL_THROTTLE_MS);
  }, [logs]);

  useEffect(() => () => {
    if (scrollTimerRef.current != null) {
      window.clearTimeout(scrollTimerRef.current);
    }
  }, []);

  const typeColors: Record<string, string> = {
    message: 'text-gray-300',
    tool_use: 'text-blue-400',
    tool_result: 'text-green-400',
    result: 'text-indigo-400',
    process_exit: 'text-yellow-400',
    parse_error: 'text-red-400',
  };

  return (
    <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50 p-4">
      <div className="bg-gray-900 rounded-lg w-full max-w-3xl max-h-[80vh] flex flex-col">
        <div className="flex items-center justify-between px-4 py-3 border-b border-gray-700">
          <div className="flex items-center gap-2">
            <h3 className="text-foreground font-medium text-sm">Instance #{instanceId} Logs</h3>
            <span className={`text-[10px] ${isConnected ? 'text-green-400' : 'text-yellow-400'}`}>
              {isConnected ? 'Live' : 'Reconnecting…'}
            </span>
          </div>
          <button onClick={onClose} className="text-gray-400 hover:text-foreground">
            <X size={18} />
          </button>
        </div>
        <div
          ref={scrollContainerRef}
          onScroll={handleScroll}
          className="flex-1 overflow-y-auto p-4 font-mono text-xs space-y-1"
        >
          {loadError && <p role="alert" className="text-red-400 mb-2">Could not load history: {loadError}</p>}
          {logs.length === 0 && <p className="text-gray-500">No logs yet</p>}
          {logs.map((log) => (
            <div
              key={log.clientKey}
              data-testid="instance-log-entry"
              className={`${log.is_error ? 'text-red-400' : typeColors[log.event_type] || 'text-gray-400'} whitespace-pre-wrap break-words`}
            >
              <div>
                <span className="text-gray-600 mr-2">{new Date(log.timestamp).toLocaleTimeString()}</span>
                <span className="text-gray-500 mr-2">[{log.event_type}]</span>
                {log.tool_name && <span className="text-blue-300 mr-2">{log.tool_name}</span>}
                <span>{log.content || ''}</span>
              </div>
              {log.tool_input && (
                <div className="pl-4 text-gray-400"><span className="text-gray-600">input: </span>{log.tool_input}</div>
              )}
              {log.tool_output && (
                <div className="pl-4 text-gray-400"><span className="text-gray-600">output: </span>{log.tool_output}</div>
              )}
            </div>
          ))}
          <div ref={bottomRef} />
        </div>
      </div>
    </div>
  );
}
