import { render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { api } from '../api/client';
import { TeamShareModal } from './TeamShareModal';

vi.mock('../api/client', () => ({
  api: {
    getTeamUsers: vi.fn(),
    getTaskSharesTeam: vi.fn(),
    teamGetProjectShares: vi.fn(),
    getTeamGroups: vi.fn(),
    teamShareProject: vi.fn(),
    teamUnshareProject: vi.fn(),
    shareTaskTeam: vi.fn(),
    unshareTaskTeam: vi.fn(),
  },
}));

describe('TeamShareModal', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
    vi.mocked(api.getTaskSharesTeam).mockResolvedValue([]);
    vi.mocked(api.teamGetProjectShares).mockResolvedValue([]);
    vi.mocked(api.getTeamGroups).mockResolvedValue([]);
  });

  it('shows a load failure instead of silently rendering an empty share list', async () => {
    vi.mocked(api.getTeamUsers).mockRejectedValue(new Error('Admin only'));

    render(
      <TeamShareModal
        type="task"
        itemId={7}
        itemTitle="Private task"
        onClose={vi.fn()}
      />,
    );

    expect(await screen.findByRole('alert')).toHaveTextContent(
      'Unable to load sharing options: Admin only',
    );
    expect(screen.queryByText('No users or groups to share with.')).not.toBeInTheDocument();
  });

  it('renders the empty state only after all share data loads successfully', async () => {
    vi.mocked(api.getTeamUsers).mockResolvedValue([]);

    render(
      <TeamShareModal
        type="task"
        itemId={7}
        itemTitle="Private task"
        onClose={vi.fn()}
      />,
    );

    expect(await screen.findByText('No users or groups to share with.')).toBeInTheDocument();
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  });
});
