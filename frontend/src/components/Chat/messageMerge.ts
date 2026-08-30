import type { ChatMessage } from '../../api/client';

/**
 * Older backends persisted a collab tool's local completed status as a
 * system separator.  Require the native type and status extracted from
 * raw_json so an unrelated system message containing "completed" survives.
 */
export function isLegacyCodexCollabCompleted(
  message: Pick<
    ChatMessage,
    'event_type' | 'content' | 'native_item_type' | 'native_item_status'
  >,
): boolean {
  return (
    message.event_type === 'system_event'
    && message.content === 'completed'
    && (
      message.native_item_type === 'collabAgentToolCall'
      || message.native_item_type === 'collab_agent_tool_call'
    )
    && message.native_item_status === 'completed'
  );
}

function attachmentKey(message: ChatMessage): string {
  return JSON.stringify(
    (message.attachments || []).map((attachment) => [
      attachment.url,
      attachment.name,
      attachment.is_image,
    ]),
  );
}

/**
 * Identity used only for reconciling live-only bubbles with an HTTP snapshot.
 * Counts are consumed one-by-one so repeated, identical model messages remain
 * repeated instead of being collapsed globally.
 */
function messageFingerprint(message: ChatMessage): string {
  const nativeId = message.item_id || message.stream_item_id || null;
  return JSON.stringify([
    message.client_message_id || null,
    nativeId,
    message.task_retry_count ?? null,
    message.task_turn_generation ?? null,
    message.request_id || null,
    message.event_type,
    message.role,
    message.raw_content ?? message.content,
    message.tool_name,
    message.tool_input,
    message.tool_output,
    message.loop_iteration,
    message.source || null,
    attachmentKey(message),
  ]);
}

function stableLiveKey(message: ChatMessage): string | null {
  if (message.client_message_id) {
    return `client-message:${message.client_message_id}`;
  }
  const nativeId = message.item_id || message.stream_item_id;
  if (nativeId) {
    return JSON.stringify([
      'item',
      message.task_retry_count ?? null,
      message.task_turn_generation ?? null,
      nativeId,
    ]);
  }
  if (message.request_id) return `request:${message.request_id}`;
  return null;
}

function consume(counts: Map<string, number>, key: string): boolean {
  const count = counts.get(key) || 0;
  if (count <= 0) return false;
  if (count === 1) counts.delete(key);
  else counts.set(key, count - 1);
  return true;
}

/**
 * These notices describe a currently active transport state, not durable chat
 * history. Keep one when the HTTP snapshot is older than it, but retire it as
 * soon as a later persisted event proves that the retry has progressed.
 */
function isExpiredEphemeral(
  message: ChatMessage,
  currentIndex: number,
  current: ChatMessage[],
  snapshot: ChatMessage[],
): boolean {
  const isTransientRetry = message.event_type === 'transient_retry';
  const isPtyRecovery = Boolean(message.pty_cold_start);
  if (!isTransientRetry && !isPtyRecovery) return false;

  if (
    current.slice(currentIndex + 1).some((candidate) =>
      candidate.persisted
      && candidate.event_type !== 'transient_retry'
      && !candidate.pty_cold_start
    )
  ) {
    return true;
  }

  const ephemeralTime = message.timestamp ? Date.parse(message.timestamp) : Number.NaN;
  if (!Number.isFinite(ephemeralTime)) return false;
  return snapshot.some((candidate) => {
    if (
      !candidate.persisted
      || candidate.event_type === 'transient_retry'
      || candidate.pty_cold_start
    ) return false;
    const candidateTime = candidate.timestamp ? Date.parse(candidate.timestamp) : Number.NaN;
    return Number.isFinite(candidateTime) && candidateTime > ephemeralTime;
  });
}

/**
 * Merge an authoritative history snapshot with the current UI state.
 *
 * A stale snapshot must not erase messages that arrived over WebSocket while
 * the request was in flight. Conversely, a subscription backfill may already
 * contain those live messages, so matching occurrences are consumed rather
 * than appended again.
 */
export function mergeChatHistory(
  snapshot: ChatMessage[],
  current: ChatMessage[],
): ChatMessage[] {
  const snapshotIds = new Set(
    snapshot
      .filter((message) => message.persisted)
      .map((message) => message.id),
  );
  const snapshotStableKeys = new Set(
    snapshot
      .map(stableLiveKey)
      .filter((key): key is string => key !== null),
  );
  const snapshotCounts = new Map<string, number>();
  for (const message of snapshot) {
    const key = messageFingerprint(message);
    snapshotCounts.set(key, (snapshotCounts.get(key) || 0) + 1);
  }

  const extras: ChatMessage[] = [];
  for (const [currentIndex, message] of current.entries()) {
    if (isExpiredEphemeral(message, currentIndex, current, snapshot)) {
      continue;
    }
    const key = messageFingerprint(message);
    if (message.persisted && snapshotIds.has(message.id)) {
      consume(snapshotCounts, key);
      continue;
    }
    const stableKey = stableLiveKey(message);
    if (!message.persisted && stableKey && snapshotStableKeys.has(stableKey)) {
      continue;
    }
    if (!message.persisted && consume(snapshotCounts, key)) {
      continue;
    }
    extras.push(message);
  }

  const persistedById = new Map<number, ChatMessage>();
  for (const message of extras) {
    if (message.persisted) persistedById.set(message.id, message);
  }
  // The newest HTTP representation wins for rows present in both sources.
  for (const message of snapshot) {
    if (message.persisted) persistedById.set(message.id, message);
  }

  const persisted = [...persistedById.values()].sort((a, b) => a.id - b.id);
  const snapshotLive = snapshot.filter((message) => !message.persisted);
  const extraLive = extras.filter((message) => !message.persisted);
  return [...persisted, ...snapshotLive, ...extraLive];
}
