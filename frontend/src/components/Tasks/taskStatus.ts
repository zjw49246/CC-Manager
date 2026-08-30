import type { Task } from '../../api/client';

const ACTIVE_PLAN_STAGES = new Set(['planning', 'reviewing']);

function titleCaseStatus(status: string): string {
  return status
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

export function getTaskStatusLabel(task: Task): string {
  if (task.background_active) return 'Background';

  if (task.status === 'waiting_capability') return 'Waiting Capability';

  if (task.mode === 'delivery_loop' && task.delivery_run_id != null) {
    if (task.delivery_phase === 'done') {
      if (task.delivery_outcome === 'success') {
        return task.delivery_terminal === 'merged' ? 'Merged' : 'Ready to Merge';
      }
      return task.delivery_outcome
        ? `Delivery ${titleCaseStatus(task.delivery_outcome)}`
        : 'Delivery Done';
    }
    if (task.delivery_phase) {
      const phase = titleCaseStatus(task.delivery_phase);
      const activity = task.delivery_activity
        ? titleCaseStatus(task.delivery_activity)
        : null;
      return activity ? `${phase} · ${activity}` : phase;
    }
    return 'Delivery Preparing';
  }

  if (
    task.mode === 'plan'
    && ['in_progress', 'executing'].includes(task.status)
    && task.plan_stage
    && ACTIVE_PLAN_STAGES.has(task.plan_stage)
  ) {
    const label = titleCaseStatus(task.plan_stage);
    const round = Number.isInteger(task.plan_stage_round)
      ? Math.max(1, task.plan_stage_round as number)
      : 1;
    return round > 1 ? `${label} · Round ${round}` : label;
  }

  return titleCaseStatus(task.status);
}
