import { describe, expect, it } from 'vitest';

import type { Task } from '../../api/client';
import { getTaskStatusLabel } from './taskStatus';

function deliveryTask(overrides: Partial<Task>): Task {
  return {
    mode: 'delivery_loop',
    status: 'delivery_waiting',
    delivery_run_id: 7,
    delivery_phase: 'planning',
    delivery_activity: 'waiting',
    delivery_outcome: null,
    delivery_terminal: 'ready_to_merge',
    background_active: false,
    ...overrides,
  } as Task;
}

describe('getTaskStatusLabel Delivery projection', () => {
  it('uses the Run phase and activity instead of the resting Task status', () => {
    expect(getTaskStatusLabel(deliveryTask({}))).toBe('Planning · Waiting');
  });

  it('renders the exact successful terminal as ready to merge', () => {
    expect(getTaskStatusLabel(deliveryTask({
      delivery_phase: 'done',
      delivery_activity: 'terminal',
      delivery_outcome: 'success',
      status: 'completed',
    }))).toBe('Ready to Merge');
  });

  it('renders an auto-merge terminal as merged', () => {
    expect(getTaskStatusLabel(deliveryTask({
      delivery_phase: 'done',
      delivery_activity: 'terminal',
      delivery_outcome: 'success',
      delivery_terminal: 'merged',
      status: 'completed',
    }))).toBe('Merged');
  });

  it('keeps a failed Delivery terminal explicit', () => {
    expect(getTaskStatusLabel(deliveryTask({
      delivery_phase: 'done',
      delivery_activity: 'terminal',
      delivery_outcome: 'failed',
      status: 'failed',
    }))).toBe('Delivery Failed');
  });
});

describe('getTaskStatusLabel Capability projection', () => {
  it('renders the durable waiting state as a user-facing status', () => {
    expect(getTaskStatusLabel({
      mode: 'auto',
      status: 'waiting_capability',
      background_active: false,
    } as Task)).toBe('Waiting Capability');
  });
});
