import type { Task } from '../../api/client';

/**
 * PR Monitor display Tasks are durable, read-only projections.  Reviewer
 * execution Tasks intentionally do not carry this marker and stay internal.
 */
export function isPRMonitorDisplayTask(
  task: Pick<Task, 'mode' | 'metadata_'>,
): boolean {
  return task.mode === 'pr_monitor' || task.metadata_?.pr_monitor_display === true;
}
