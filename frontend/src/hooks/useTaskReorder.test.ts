import { describe, it, expect } from 'vitest';
import { effectiveKey, mergeVisibleTaskOrder, newSortFor } from './useTaskReorder';
import type { Task } from '../api/client';

function makeTask(overrides: Partial<Task> = {}): Task {
  return {
    id: 1,
    title: '',
    description: '',
    status: 'pending',
    priority: 0,
    target_repo: '/tmp',
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
    created_at: '2025-01-01T00:00:00Z',
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
    ...overrides,
  };
}

describe('effectiveKey', () => {
  it('uses sort_order when present', () => {
    const t = makeTask({ sort_order: 42 });
    expect(effectiveKey(t)).toBe(42);
  });

  it('falls back to last_accessed_at timestamp', () => {
    const t = makeTask({ last_accessed_at: '2025-06-01T12:00:00Z' });
    const expected = new Date('2025-06-01T12:00:00Z').getTime() / 1000;
    expect(effectiveKey(t)).toBe(expected);
  });

  it('falls back to created_at timestamp', () => {
    const t = makeTask({ created_at: '2025-01-01T00:00:00Z', last_accessed_at: null });
    const expected = new Date('2025-01-01T00:00:00Z').getTime() / 1000;
    expect(effectiveKey(t)).toBe(expected);
  });

  it('returns 0 when no timestamps', () => {
    const t = makeTask({ created_at: undefined as unknown as string, last_accessed_at: null });
    expect(effectiveKey(t)).toBe(0);
  });
});

describe('mergeVisibleTaskOrder', () => {
  it('does not substitute a search result that is absent from the local page', () => {
    const page = [
      makeTask({ id: 5, title: 'page match' }),
      makeTask({ id: 6, title: 'unrelated page task' }),
    ];
    const serverWideSearchOrder = [
      makeTask({ id: 50, title: 'off-page match' }),
      makeTask({ id: 5, title: 'page match' }),
    ];

    const merged = mergeVisibleTaskOrder(page, serverWideSearchOrder);

    expect(merged.map((task) => task.id)).toEqual([5, 6]);
    expect(merged[0].title).toBe('page match');
  });

  it('reorders only matching slots while preserving unrelated rows', () => {
    const page = [
      makeTask({ id: 1 }),
      makeTask({ id: 2 }),
      makeTask({ id: 3 }),
      makeTask({ id: 4 }),
    ];
    const optimistic = [
      makeTask({ id: 30 }),
      makeTask({ id: 3 }),
      makeTask({ id: 1 }),
    ];

    expect(mergeVisibleTaskOrder(page, optimistic).map((task) => task.id)).toEqual([
      3, 2, 1, 4,
    ]);
  });
});

describe('newSortFor', () => {
  it('places between two same-group neighbors', () => {
    const list = [
      makeTask({ id: 1, sort_order: 300, starred: false }),
      makeTask({ id: 2, sort_order: 200, starred: false }),
      makeTask({ id: 3, sort_order: 100, starred: false }),
    ];
    // Move task 1 (idx 0) to idx 1 (between task 2 and task 3 after removal)
    // without = [T2(200), T3(100)], prev=T2(200), next=T3(100)
    const result = newSortFor(list, 0, 1);
    expect(result).toBe(150); // (200 + 100) / 2
  });

  it('places at top of group with +60 offset', () => {
    const list = [
      makeTask({ id: 1, sort_order: 300, starred: false }),
      makeTask({ id: 2, sort_order: 200, starred: false }),
    ];
    // Move task 2 (idx 1) to idx 0 (top)
    const result = newSortFor(list, 1, 0);
    expect(result).toBe(360); // 300 + 60
  });

  it('places at bottom of group with -60 offset', () => {
    const list = [
      makeTask({ id: 1, sort_order: 300, starred: false }),
      makeTask({ id: 2, sort_order: 200, starred: false }),
    ];
    // Move task 1 (idx 0) to idx 1 (bottom)
    const result = newSortFor(list, 0, 1);
    expect(result).toBe(140); // 200 - 60
  });

  it('skips cross-group neighbor at starred boundary', () => {
    // ★A(500) | B(1100) — B has higher key but is non-starred
    const list = [
      makeTask({ id: 1, sort_order: 500, starred: true }),
      makeTask({ id: 2, sort_order: 1100, starred: false }),
    ];
    // Move starred task to its own position (drag to bottom of starred group = idx 0)
    // With only one starred task, this is a no-op in commit(), but let's test newSortFor
    // directly: moving ★A to toIdx=1 (after removing ★A, without=[B])
    // The next neighbor at without[1] is B which is non-starred, so it should be skipped.
    // prev at without[0] is B, also non-starred, skipped.
    // Both null → use Date.now()/1000
    const result = newSortFor(list, 0, 1);
    const now = Date.now() / 1000;
    expect(result).toBeGreaterThan(now - 10);
    expect(result).toBeLessThan(now + 10);
  });

  it('uses only same-starred-group neighbors when groups are mixed', () => {
    // ★A(1000), ★B(800), C(1500), D(500)
    // C has a higher key than starred tasks but is non-starred.
    const list = [
      makeTask({ id: 1, sort_order: 1000, starred: true }),
      makeTask({ id: 2, sort_order: 800, starred: true }),
      makeTask({ id: 3, sort_order: 1500, starred: false }),
      makeTask({ id: 4, sort_order: 500, starred: false }),
    ];
    // Move ★A (idx 0) to idx 1 (below ★B)
    // without = [★B(800), C(1500), D(500)]
    // toIdx = 1, prev search from idx 0: ★B → same group ✓, pk = 800
    // next search from idx 1: C → different group, D → different group → null
    // result = pk - 60 = 740
    const result = newSortFor(list, 0, 1);
    expect(result).toBe(740); // 800 - 60, not (800 + 1500)/2 = 1150
  });

  it('autoSort=false uses created_at as fallback', () => {
    const list = [
      makeTask({ id: 1, sort_order: null, created_at: '2025-01-01T00:00:00Z', last_accessed_at: '2025-06-01T00:00:00Z', starred: false }),
      makeTask({ id: 2, sort_order: null, created_at: '2025-03-01T00:00:00Z', last_accessed_at: '2025-01-01T00:00:00Z', starred: false }),
    ];
    // autoSort=false: effectiveKey uses created_at, so t2 (Mar) > t1 (Jan)
    expect(effectiveKey(list[0], false)).toBeLessThan(effectiveKey(list[1], false));
    // autoSort=true: effectiveKey uses last_accessed_at, so t1 (Jun) > t2 (Jan)
    expect(effectiveKey(list[0], true)).toBeGreaterThan(effectiveKey(list[1], true));
  });

  it('newSortFor respects autoSort=false', () => {
    // Two tasks with no sort_order: created_at determines effective key
    const list = [
      makeTask({ id: 1, sort_order: null, created_at: '2025-06-01T00:00:00Z', last_accessed_at: null, starred: false }),
      makeTask({ id: 2, sort_order: null, created_at: '2025-01-01T00:00:00Z', last_accessed_at: null, starred: false }),
    ];
    const ek1 = effectiveKey(list[0], false);
    const ek2 = effectiveKey(list[1], false);
    // Moving t2 to top: prev=null, next=t1 → nk + 60
    const result = newSortFor(list, 1, 0, false);
    expect(result).toBe(ek1 + 60);
    // Same with autoSort=true (no last_accessed_at, same fallback)
    const result2 = newSortFor(list, 1, 0, true);
    expect(result2).toBe(ek1 + 60);
  });

  it('handles drag within non-starred group past starred boundary', () => {
    // ★A(1000), B(800), C(500)
    const list = [
      makeTask({ id: 1, sort_order: 1000, starred: true }),
      makeTask({ id: 2, sort_order: 800, starred: false }),
      makeTask({ id: 3, sort_order: 500, starred: false }),
    ];
    // Move C (idx 2) to idx 0 — will be clamped by groupRange in commit,
    // but newSortFor itself should handle it: fromIdx=2, toIdx=0
    // without = [★A(1000), B(800)]
    // toIdx = 0, prev search backward from -1 → null
    // next search from 0: ★A → different group ✗, B → same group ✓, nk = 800
    // result = 800 + 60 = 860
    const result = newSortFor(list, 2, 0);
    expect(result).toBe(860); // 800 + 60, not 1000 + 60
  });
});
