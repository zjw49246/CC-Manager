import { render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { api, type PlanResource } from '../../api/client';
import { VersionedPlanPanel } from './VersionedPlanPanel';

vi.mock('../../api/client', () => ({
  api: {
    listPlans: vi.fn(),
  },
}));

vi.mock('./usePlanEvents', () => ({
  usePlanEvents: vi.fn(),
}));

vi.mock('../../hooks/useDialogA11y', () => ({
  useDialogA11y: () => ({ current: null }),
}));

vi.mock('./PlanDetail', () => ({
  PlanDetail: () => <div>Plan detail</div>,
}));

const completedPlan = (
  id: number,
  title: string,
  overrides: Partial<PlanResource> = {},
) => ({
  id,
  title,
  target_task_id: 7,
  display_state: 'awaiting_review',
  read_only: false,
  ownership: 'standard',
  current_version: {
    id: id * 10,
    version_number: 1,
  },
  ...overrides,
} as PlanResource);

describe('VersionedPlanPanel', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('excludes completed Capability Plans from the ordinary review or execution card', async () => {
    vi.mocked(api.listPlans).mockResolvedValue([
      completedPlan(1, 'Capability result', {
        ownership: 'capability',
        read_only: true,
      }),
      completedPlan(2, 'Human review'),
    ]);
    const onVisibilityChange = vi.fn();

    render(
      <VersionedPlanPanel
        onVisibilityChange={onVisibilityChange}
        onNavigateTask={vi.fn()}
      />,
    );

    expect(await screen.findByText(/Human review/)).toBeInTheDocument();
    expect(screen.queryByText(/Capability result/)).not.toBeInTheDocument();
    expect(screen.getByText('Review or execute').parentElement).toHaveTextContent('1');
    expect(onVisibilityChange).toHaveBeenLastCalledWith(true);
  });

  it('stays hidden when Capability results are the only completed Plans', async () => {
    vi.mocked(api.listPlans).mockResolvedValue([
      completedPlan(1, 'Capability result', {
        ownership: 'capability',
        read_only: true,
      }),
    ]);
    const onVisibilityChange = vi.fn();

    render(
      <VersionedPlanPanel
        onVisibilityChange={onVisibilityChange}
        onNavigateTask={vi.fn()}
      />,
    );

    await waitFor(() => expect(api.listPlans).toHaveBeenCalled());
    await waitFor(() => expect(onVisibilityChange).toHaveBeenLastCalledWith(false));
    expect(screen.queryByText('Review or execute')).not.toBeInTheDocument();
  });
});
