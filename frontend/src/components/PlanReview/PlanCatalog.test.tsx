import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';

import type { PlanResource } from '../../api/client';
import { PlanCatalog } from './PlanCatalog';

const plan = (id: number, title: string) => ({
  id,
  title,
  target_task_id: null,
  project_id: null,
  display_state: 'awaiting_review',
  active_run_id: null,
  archived_at: null,
  lock_version: 2,
  current_version: null,
  applications: [],
} as PlanResource);

describe('PlanCatalog', () => {
  it('visibly marks the selected Plan and reports selection changes', async () => {
    const onSelectPlan = vi.fn();
    render(
      <PlanCatalog
        plans={[plan(1, 'First Plan'), plan(2, 'Second Plan')]}
        projects={[]}
        selectedPlanId={2}
        onSelectPlan={onSelectPlan}
        onNavigateTask={vi.fn()}
        onSetArchived={vi.fn()}
      />,
    );

    const selected = screen.getByRole('button', { name: /Second Plan/ });
    expect(selected).toHaveAttribute('aria-current', 'true');
    expect(selected.parentElement?.className).toContain('border-indigo-500/70');
    expect(screen.getByRole('button', { name: /First Plan/ })).not.toHaveAttribute('aria-current');

    await userEvent.click(screen.getByRole('button', { name: /First Plan/ }));
    expect(onSelectPlan).toHaveBeenCalledWith(1);
  });

  it.each([
    ['queued', 'text-blue-300'],
    ['waiting_user', 'text-amber-300'],
    ['cancelling', 'text-orange-300'],
    ['awaiting_review', 'text-purple-300'],
    ['approved', 'text-emerald-300'],
    ['applied', 'text-teal-300'],
    ['failed', 'text-red-300'],
    ['rejected', 'text-rose-300'],
    ['cancelled', 'text-orange-300'],
    ['archived', 'text-gray-400'],
    ['draft', 'text-gray-400'],
  ])('uses a semantic badge color for %s', (displayState, expectedClass) => {
    render(<PlanCatalog plans={[{ ...plan(1, 'Plan'), display_state: displayState }]} projects={[]} selectedPlanId={null} onSelectPlan={vi.fn()} onNavigateTask={vi.fn()} onSetArchived={vi.fn()} />);
    expect(screen.getByText(displayState === 'waiting_user' ? 'Needs input' : displayState === 'awaiting_review' ? 'Needs approval' : displayState[0].toUpperCase() + displayState.slice(1))).toHaveClass(expectedClass);
  });

  it('archives and restores without selecting the Plan', async () => {
    const onSelectPlan = vi.fn();
    const onSetArchived = vi.fn().mockResolvedValue(undefined);
    const archived = { ...plan(2, 'Archived Plan'), archived_at: '2026-08-01T00:00:00Z' };
    render(<PlanCatalog plans={[plan(1, 'Current Plan'), archived]} projects={[]} selectedPlanId={null} onSelectPlan={onSelectPlan} onNavigateTask={vi.fn()} onSetArchived={onSetArchived} />);

    await userEvent.click(screen.getByRole('button', { name: 'Archive Plan #1' }));
    await userEvent.click(screen.getByRole('button', { name: 'Restore Plan #2' }));

    expect(onSetArchived).toHaveBeenNthCalledWith(1, expect.objectContaining({ id: 1 }), true);
    expect(onSetArchived).toHaveBeenNthCalledWith(2, expect.objectContaining({ id: 2 }), false);
    expect(onSelectPlan).not.toHaveBeenCalled();
  });

  it('hides archive actions for active Plans', () => {
    render(<PlanCatalog plans={[{ ...plan(1, 'Running Plan'), active_run_id: 42 }]} projects={[]} selectedPlanId={null} onSelectPlan={vi.fn()} onNavigateTask={vi.fn()} onSetArchived={vi.fn()} />);
    expect(screen.queryByRole('button', { name: 'Archive Plan #1' })).not.toBeInTheDocument();
  });

  it('labels Capability ownership and hides archive for a terminal read-only Plan', () => {
    render(<PlanCatalog plans={[{ ...plan(1, 'Capability Plan'), ownership: 'capability', read_only: true }]} projects={[]} selectedPlanId={null} onSelectPlan={vi.fn()} onNavigateTask={vi.fn()} onSetArchived={vi.fn()} />);
    expect(screen.getByText('Capability · read-only')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Archive Plan #1' })).not.toBeInTheDocument();
  });

  it('disables the archive action while it is pending and keeps its accessible name', async () => {
    let resolve!: () => void;
    const pending = new Promise<void>((done) => { resolve = done; });
    render(<PlanCatalog plans={[plan(1, 'Current Plan')]} projects={[]} selectedPlanId={null} onSelectPlan={vi.fn()} onNavigateTask={vi.fn()} onSetArchived={() => pending} />);

    const archive = screen.getByRole('button', { name: 'Archive Plan #1' });
    await userEvent.click(archive);
    expect(archive).toBeDisabled();
    expect(archive).toHaveAttribute('title', 'Archive');
    resolve();
    await vi.waitFor(() => expect(archive).not.toBeDisabled());
  });

  it('opens a related Task without selecting the Plan', async () => {
    const onSelectPlan = vi.fn();
    const onNavigateTask = vi.fn();
    render(<PlanCatalog plans={[{ ...plan(1, 'Related Plan'), target_task_id: 200 }]} projects={[]} selectedPlanId={null} onSelectPlan={onSelectPlan} onNavigateTask={onNavigateTask} onSetArchived={vi.fn()} />);

    const openTask = screen.getByRole('button', { name: 'Open related Task #200' });
    expect(openTask).toHaveAttribute('title', 'Open related Task #200');
    await userEvent.click(openTask);

    expect(onNavigateTask).toHaveBeenCalledWith(200);
    expect(onSelectPlan).not.toHaveBeenCalled();
  });

  it('opens the Delivery workspace for a Delivery-owned Plan', async () => {
    const onNavigateTask = vi.fn();
    const onNavigateDelivery = vi.fn();
    render(<PlanCatalog plans={[{ ...plan(2, 'Delivery Plan'), target_task_id: 1005, delivery_run_id: 1 }]} projects={[]} selectedPlanId={null} onSelectPlan={vi.fn()} onNavigateTask={onNavigateTask} onNavigateDelivery={onNavigateDelivery} onSetArchived={vi.fn()} />);

    await userEvent.click(screen.getByRole('button', { name: 'Open Delivery DLV-1' }));

    expect(onNavigateDelivery).toHaveBeenCalledWith(1);
    expect(onNavigateTask).not.toHaveBeenCalled();
  });
});
