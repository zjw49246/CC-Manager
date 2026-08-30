import { render, screen, within } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import { VersionedPlansDialog } from './VersionedPlansDialog';

vi.mock('../../api/client', () => ({
  api: {
    listPlans: vi.fn(() => new Promise(() => {})),
  },
}));

vi.mock('./usePlanEvents', () => ({
  usePlanEvents: vi.fn(),
}));

describe('VersionedPlansDialog mobile layout', () => {
  it('keeps full-screen content and controls inside iOS safe areas', () => {
    render(
      <VersionedPlansDialog
        open
        taskId={78}
        selectedVersionIds={[]}
        onToggleVersion={vi.fn()}
        onAttachVersion={vi.fn()}
        onPlansChange={vi.fn()}
        onClose={vi.fn()}
      />,
    );

    const dialog = screen.getByRole('dialog', { name: 'Plans for Task #78' });
    expect(dialog.className).toContain('pt-[env(safe-area-inset-top)]');
    expect(dialog.className).toContain('pb-[env(safe-area-inset-bottom)]');

    const close = within(dialog).getByRole('button', { name: 'Close Plans' });
    expect(close.className).toContain('top-[calc(env(safe-area-inset-top)+0.75rem)]');
    expect(close.className).toContain('h-11');
    expect(close.className).toContain('w-11');
  }, 15_000);
});
