import { act, renderHook } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type { Task } from '../api/client';
import { useTaskSearch } from './useTaskSearch';

vi.mock('../api/client', () => ({
  api: { listTasks: vi.fn() },
}));

import { api } from '../api/client';

function task(id: number, title: string): Task {
  return {
    id,
    title,
    description: title,
    status: 'pending',
    priority: 0,
    target_repo: null,
    target_branch: 'main',
    mode: 'single',
    merge_status: 'none',
    retry_count: 0,
    turn_generation: 0,
    max_retries: 3,
    provider: 'claude',
    starred: false,
    archived: false,
    has_unread: false,
    created_at: '2026-01-01T00:00:00Z',
    sort_order: null,
    last_accessed_at: null,
    session_id: null,
    last_cwd: null,
    project_id: null,
    error_message: null,
    started_at: null,
    completed_at: null,
    model: null,
    effort_level: null,
    goal_condition: null,
    goal_max_turns: 10,
    goal_turns_used: 0,
    goal_last_reason: null,
    goal_evaluator_model: null,
    loop_progress: null,
    todo_file_path: null,
    timeout_hours: null,
    worker_id: null,
    enabled_skills: {},
    enable_workflows: false,
    thinking_budget: null,
    context_usage: null,
  };
}

afterEach(() => {
  vi.useRealTimers();
  vi.clearAllMocks();
});

describe('useTaskSearch', () => {
  it('ignores an older request that resolves after the latest query', async () => {
    vi.useFakeTimers();
    let resolveFirst!: (tasks: Task[]) => void;
    let resolveSecond!: (tasks: Task[]) => void;
    vi.mocked(api.listTasks)
      .mockReturnValueOnce(new Promise((resolve) => { resolveFirst = resolve; }))
      .mockReturnValueOnce(new Promise((resolve) => { resolveSecond = resolve; }));

    const { result, rerender } = renderHook(
      ({ query }) => useTaskSearch(query, false, 'main'),
      { initialProps: { query: 'alpha' } },
    );
    await act(async () => { await vi.advanceTimersByTimeAsync(300); });

    rerender({ query: 'beta' });
    await act(async () => { await vi.advanceTimersByTimeAsync(300); });

    await act(async () => {
      resolveSecond([task(1, 'alpha'), task(2, 'beta')]);
      await Promise.resolve();
    });
    expect(result.current[0]?.map((item) => item.id)).toEqual([2]);

    await act(async () => {
      resolveFirst([task(1, 'alpha'), task(2, 'beta')]);
      await Promise.resolve();
    });
    expect(result.current[0]?.map((item) => item.id)).toEqual([2]);
    expect(vi.mocked(api.listTasks).mock.calls.every((call) => call[8] === 'main')).toBe(true);
  });
});
