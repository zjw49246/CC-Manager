import { describe, expect, it } from 'vitest';
import type { Task } from '../../api/client';
import { canControlTask, taskHasSession } from './taskSharePermissions';

describe('Task access projection helpers', () => {
  it('allows only the exact control scope when access_scope is present', () => {
    expect(canControlTask({ access_scope: 'control' } as Task)).toBe(true);
    expect(canControlTask({ access_scope: 'chat' } as Task)).toBe(false);
    expect(canControlTask({ access_scope: 'unexpected' } as unknown as Task)).toBe(false);
    expect(canControlTask({ access_scope: undefined } as unknown as Task)).toBe(false);
  });

  it('uses shared_from_id only for a legacy response without access_scope', () => {
    expect(canControlTask({ shared_from_id: null } as Task)).toBe(true);
    expect(canControlTask({ shared_from_id: 7 } as Task)).toBe(false);
  });

  it('uses has_session authoritatively and falls back only when it is absent', () => {
    expect(taskHasSession({ has_session: true, session_id: undefined } as Task)).toBe(true);
    expect(taskHasSession({ has_session: false, session_id: 'hidden' } as Task)).toBe(false);
    expect(taskHasSession({ session_id: 'legacy-session' } as Task)).toBe(true);
  });
});
