import type { Task } from '../../api/client';

interface StoredUserIdentity {
  id?: unknown;
  role?: unknown;
}

interface TaskAccessProjection {
  access_scope?: unknown;
  shared_from_id?: number | null;
}

interface TaskSessionProjection {
  has_session?: unknown;
  session_id?: unknown;
}

/**
 * Mounted Task APIs include an authoritative access_scope projection.  Once
 * that field is present, only the exact `control` value enables mutation UI;
 * unexpected/nullish values must fail closed.  The missing-field branch is
 * solely for rolling upgrades from older CCM backends.
 */
export function canControlTask(
  task: TaskAccessProjection,
): boolean {
  if ('access_scope' in task) return task.access_scope === 'control';
  return task.shared_from_id == null;
}

/** Prefer the public boolean projection and consult the native session id
 * only when talking to a pre-projection backend. */
export function taskHasSession(
  task: TaskSessionProjection,
): boolean {
  if ('has_session' in task) return task.has_session === true;
  return typeof task.session_id === 'string' && task.session_id.length > 0;
}

export function readStoredUserIdentity(): StoredUserIdentity {
  try {
    return JSON.parse(localStorage.getItem('cc_user') || '{}') as StoredUserIdentity;
  } catch {
    return {};
  }
}

export function canManageTaskShare(
  task: Pick<Task, 'created_by'>,
  user: StoredUserIdentity,
): boolean {
  if (user.role === 'admin' || user.role === 'super_admin') {
    return true;
  }
  return typeof user.id === 'number' && task.created_by === user.id;
}
