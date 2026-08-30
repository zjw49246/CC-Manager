import type { PlanResource, PlanVersion } from '../../api/client';

const DISPLAY_STATES = {
  queued: { label: 'Queued', className: 'border-blue-500/40 bg-blue-500/15 text-blue-300' },
  planner: { label: 'Planning', className: 'border-indigo-500/40 bg-indigo-500/15 text-indigo-300' },
  reviewer: { label: 'Reviewing', className: 'border-violet-500/40 bg-violet-500/15 text-violet-300' },
  running: { label: 'Running', className: 'border-blue-400/40 bg-blue-400/15 text-blue-200' },
  waiting_user: { label: 'Needs input', className: 'border-amber-500/40 bg-amber-500/15 text-amber-300' },
  cancelling: { label: 'Cancelling', className: 'border-orange-500/40 bg-orange-500/15 text-orange-300' },
  awaiting_review: { label: 'Needs approval', className: 'border-purple-500/40 bg-purple-500/15 text-purple-300' },
  approved: { label: 'Approved', className: 'border-emerald-500/40 bg-emerald-500/15 text-emerald-300' },
  rejected: { label: 'Rejected', className: 'border-rose-500/40 bg-rose-500/15 text-rose-300' },
  applied: { label: 'Applied', className: 'border-teal-500/40 bg-teal-500/15 text-teal-300' },
  failed: { label: 'Failed', className: 'border-red-500/40 bg-red-500/15 text-red-300' },
  cancelled: { label: 'Cancelled', className: 'border-orange-500/40 bg-orange-500/15 text-orange-300' },
  archived: { label: 'Archived', className: 'border-gray-600 bg-gray-700/50 text-gray-400' },
  draft: { label: 'Draft', className: 'border-gray-600 bg-gray-700/50 text-gray-400' },
};

export function planDisplayStateLabel(state: PlanResource['display_state']) {
  return DISPLAY_STATES[state as keyof typeof DISPLAY_STATES]?.label ?? state.replaceAll('_', ' ');
}

export function planDisplayStateClassName(state: PlanResource['display_state']) {
  return DISPLAY_STATES[state as keyof typeof DISPLAY_STATES]?.className ?? 'border-gray-600 bg-gray-700/50 text-gray-400';
}

const VERSION_LABELS: Record<PlanVersion['display_state'], string> = {
  applied: 'Applied',
  approved: 'Approved',
  rejected: 'Rejected',
  superseded: 'Superseded (not decided)',
  awaiting_review: 'Awaiting approval',
  draft: 'Draft',
};

export function planVersionDisplayLabel(version: PlanVersion) {
  return VERSION_LABELS[version.display_state];
}
