import { describe, expect, it } from 'vitest';
import type { ChatMessage } from '../../api/client';
import { mergeChatHistory } from './messageMerge';

function message(overrides: Partial<ChatMessage> = {}): ChatMessage {
  return {
    id: 1,
    role: 'assistant',
    event_type: 'message',
    content: 'message',
    tool_name: null,
    tool_input: null,
    tool_output: null,
    is_error: false,
    loop_iteration: null,
    timestamp: '2026-01-01T00:00:00Z',
    image_urls: null,
    attachments: null,
    ...overrides,
  };
}

describe('mergeChatHistory ephemeral events', () => {
  it('keeps a retry notice when the HTTP snapshot is older than the live event', () => {
    const snapshot = [
      message({
        id: 10,
        content: 'older durable output',
        timestamp: '2026-01-01T00:00:00Z',
        persisted: true,
      }),
    ];
    const retry = message({
      id: 1000,
      role: 'system',
      event_type: 'transient_retry',
      content: 'retrying',
      timestamp: '2026-01-01T00:00:01Z',
    });

    expect(mergeChatHistory(snapshot, [retry]).map((entry) => entry.content)).toEqual([
      'older durable output',
      'retrying',
    ]);
  });

  it('retires a retry notice once a later persisted response exists', () => {
    const final = message({
      id: 11,
      content: 'final answer',
      timestamp: '2026-01-01T00:00:02Z',
      persisted: true,
    });
    const retry = message({
      id: 1000,
      role: 'system',
      event_type: 'transient_retry',
      content: 'retrying',
      timestamp: '2026-01-01T00:00:01Z',
    });

    expect(mergeChatHistory([final], [retry]).map((entry) => entry.content)).toEqual([
      'final answer',
    ]);
    // Also cover state produced by an older merge, where the ephemeral notice
    // had already been appended after the durable response.
    expect(mergeChatHistory([final], [final, retry]).map((entry) => entry.content)).toEqual([
      'final answer',
    ]);
  });

  it('keeps a PTY recovery notice in place until durable progress retires it', () => {
    const older = message({
      id: 10,
      content: 'older output',
      timestamp: '2026-01-01T00:00:00Z',
      persisted: true,
    });
    const recovery = message({
      id: 1000,
      role: 'system',
      event_type: 'system_event',
      content: '正在恢复 PTY 会话，请稍候...',
      timestamp: '2026-01-01T00:00:01Z',
      pty_cold_start: true,
    });

    expect(mergeChatHistory([older], [older, recovery]).map((entry) => entry.content)).toEqual([
      'older output',
      '正在恢复 PTY 会话，请稍候...',
    ]);

    const progress = message({
      id: 11,
      content: 'recovered output',
      timestamp: '2026-01-01T00:00:02Z',
      persisted: true,
    });
    expect(
      mergeChatHistory([older, progress], [older, recovery]).map((entry) => entry.content),
    ).toEqual([
      'older output',
      'recovered output',
    ]);
  });
});

describe('mergeChatHistory persisted identity', () => {
  it('reconciles only the matching optimistic occurrence when equal text is sent twice', () => {
    const firstOptimistic = message({
      id: 1001,
      role: 'user',
      event_type: 'user_message',
      content: '[Admin] same text',
      raw_content: 'same text',
      client_message_id: 'client-message-first',
    });
    const secondOptimistic = message({
      id: 1002,
      role: 'user',
      event_type: 'user_message',
      content: '[Admin] same text',
      raw_content: 'same text',
      client_message_id: 'client-message-second',
    });
    const firstPersisted = message({
      id: 20,
      role: 'user',
      event_type: 'user_message',
      content: '[Admin] same text',
      raw_content: 'same text',
      client_message_id: 'client-message-first',
      task_retry_count: 1,
      task_turn_generation: 8,
      persisted: true,
    });

    const firstMerge = mergeChatHistory(
      [firstPersisted],
      [firstOptimistic, secondOptimistic],
    );

    expect(firstMerge.map((entry) => entry.id)).toEqual([20, 1002]);

    const secondPersisted = message({
      ...firstPersisted,
      id: 21,
      client_message_id: 'client-message-second',
      task_turn_generation: 9,
    });
    const secondMerge = mergeChatHistory(
      [firstPersisted, secondPersisted],
      firstMerge,
    );

    expect(secondMerge.map((entry) => entry.id)).toEqual([20, 21]);
    expect(secondMerge.filter((entry) => entry.raw_content === 'same text')).toHaveLength(2);
  });

  it('keeps a persisted injected message outside the latest page in id order', () => {
    const injected = message({
      id: 10,
      role: 'user',
      event_type: 'user_message',
      content: '[Admin] steer the active turn',
      raw_content: 'steer the active turn',
      source: 'inject',
      persisted: true,
    });
    const latest = [
      message({ id: 210, content: 'tool output', persisted: true }),
      message({ id: 211, content: 'final answer', persisted: true }),
    ];

    expect(
      mergeChatHistory(latest, [injected, ...latest]).map((entry) => entry.id),
    ).toEqual([10, 210, 211]);
  });

  it('deduplicates an older page by id without collapsing equal text at different ids', () => {
    const first = message({
      id: 20,
      role: 'user',
      event_type: 'user_message',
      content: 'same text',
      persisted: true,
    });
    const second = message({
      id: 21,
      role: 'user',
      event_type: 'user_message',
      content: 'same text',
      persisted: true,
    });

    const merged = mergeChatHistory([first], [first, second]);

    expect(merged.map((entry) => entry.id)).toEqual([20, 21]);
    expect(merged.filter((entry) => entry.content === 'same text')).toHaveLength(2);
  });
});
