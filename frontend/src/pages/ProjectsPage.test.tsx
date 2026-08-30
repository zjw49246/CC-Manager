import { cleanup, render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { api } from '../api/client';
import { ProjectsPage } from './ProjectsPage';

vi.mock('../api/client', () => ({
  api: {
    listTasks: vi.fn(),
    listProjects: vi.fn(),
    listProjectTags: vi.fn(),
    listTags: vi.fn(),
  },
}));

vi.mock('../components/Projects/ProjectTodoList', () => ({
  ProjectTodoList: () => null,
}));

const projects = [
  {
    id: 11,
    name: 'Local remote project',
    worker_id: null,
    location: 'local',
    git_url: 'https://github.com/example/local.git',
    has_remote: true,
    local_path: '/workspace/local',
    default_branch: 'main',
    status: 'ready',
    error_message: null,
    show_in_selector: true,
    sort_order: 0,
    tags: [],
    env_files: [],
    preview_config: null,
    git_author_name: null,
    git_author_email: null,
    git_credential_type: null,
    git_ssh_key_path: null,
    git_https_username: null,
    git_https_token: null,
    badge_color: null,
    created_at: '2026-08-15T00:00:00Z',
  },
  {
    id: 12,
    name: 'Worker remote project',
    worker_id: 7,
    location: 'worker-7',
    git_url: 'https://github.com/example/worker.git',
    has_remote: true,
    local_path: '/workspace/worker',
    default_branch: 'main',
    status: 'ready',
    error_message: null,
    show_in_selector: true,
    sort_order: 1,
    tags: [],
    env_files: [],
    preview_config: null,
    git_author_name: null,
    git_author_email: null,
    git_credential_type: null,
    git_ssh_key_path: null,
    git_https_username: null,
    git_https_token: null,
    badge_color: null,
    created_at: '2026-08-15T00:00:00Z',
  },
];

describe('ProjectsPage permissions', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(api.listTasks).mockResolvedValue([]);
    vi.mocked(api.listProjects).mockResolvedValue(projects as never);
    vi.mocked(api.listProjectTags).mockResolvedValue([]);
    vi.mocked(api.listTags).mockResolvedValue([]);
  });

  afterEach(() => {
    cleanup();
    localStorage.clear();
  });

  it('keeps Project creation, configuration, sharing, and reclone hidden from members', async () => {
    localStorage.setItem('cc_user', JSON.stringify({ id: 9, role: 'member' }));

    render(<ProjectsPage />);

    expect(await screen.findByText('Local remote project')).toBeInTheDocument();
    expect(screen.getByText('Worker remote project')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /New project/i })).not.toBeInTheDocument();
    expect(screen.queryByTitle('Global Git Config')).not.toBeInTheDocument();
    expect(screen.queryByTitle('Manage Tags')).not.toBeInTheDocument();
    expect(screen.queryByTitle('Share to team members')).not.toBeInTheDocument();
    expect(screen.queryByTitle('Re-clone')).not.toBeInTheDocument();
    expect(screen.queryByTitle('Drag to reorder')).not.toBeInTheDocument();
    expect(screen.getByText('Local remote project').closest('[draggable]')).toHaveAttribute('draggable', 'false');
  });

  it('offers local Project reclone to admins but never targets a Worker checkout', async () => {
    localStorage.setItem('cc_user', JSON.stringify({ id: 1, role: 'admin' }));

    render(<ProjectsPage />);

    expect(await screen.findByText('Worker remote project')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /New project/i })).toBeInTheDocument();
    await waitFor(() => {
      expect(screen.getAllByTitle('Re-clone')).toHaveLength(1);
    });
    expect(screen.getAllByTitle('Drag to reorder')).toHaveLength(2);
    expect(screen.getByText('Local remote project').closest('[draggable]')).toHaveAttribute('draggable', 'true');
  });
});
