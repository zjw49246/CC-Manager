import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { cleanup, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { SSHProfile, Task, TaskSSHGrant } from '../../api/client';
import { SSHGrantPicker, TaskSSHAccessBadge } from './TaskSSHAccess';

vi.mock('../../api/client', () => ({
  api: {
    listSSHProfiles: vi.fn(),
    listTaskSSHGrants: vi.fn(),
    updateTaskSSHGrants: vi.fn(),
  },
}));

import { api } from '../../api/client';

const profile: SSHProfile = {
  id: 41,
  name: 'production-box',
  host: 'ssh.internal',
  port: 2222,
  username: 'deploy',
  key_path_hint: '…/id_ed25519',
  public_key_fingerprint: 'SHA256:client-key',
  host_key_type: 'ssh-ed25519',
  host_key_fingerprint: 'SHA256:server-key',
  revision: 2,
  enabled: true,
  task_access_enabled: true,
  task_capabilities: ['read', 'exec', 'write'],
  allowed_roots: ['/'],
  created_by: 1,
  last_tested_at: null,
  last_test_ok: null,
  last_error_code: null,
  last_error_detail: null,
  created_at: '2026-08-06T00:00:00',
  updated_at: '2026-08-06T00:00:00',
};

const grant: TaskSSHGrant = {
  id: 7,
  task_id: 9,
  profile_id: 41,
  profile_name: profile.name,
  host: profile.host,
  port: profile.port,
  username: profile.username,
  host_key_fingerprint: profile.host_key_fingerprint,
  profile_revision: 2,
  current_profile_revision: 2,
  capabilities: ['read'],
  profile_task_access_enabled: true,
  profile_task_capabilities: ['read', 'exec', 'write'],
  profile_allowed_roots: ['/'],
  valid: true,
  invalid_reason: null,
  created_by: 1,
  created_at: '2026-08-06T00:00:00',
  updated_at: '2026-08-06T00:00:00',
};

const task = {
  id: 9,
  worker_id: null,
  shared_from_id: null,
  metadata_: null,
} as Task;

describe('Task SSH authorization UI', () => {
  beforeEach(() => {
    localStorage.setItem('cc_user', JSON.stringify({ id: 1, role: 'admin' }));
    vi.mocked(api.listSSHProfiles).mockResolvedValue([profile]);
    vi.mocked(api.listTaskSSHGrants).mockResolvedValue([]);
    vi.mocked(api.updateTaskSSHGrants).mockResolvedValue([]);
  });

  afterEach(() => {
    cleanup();
    localStorage.clear();
    vi.clearAllMocks();
  });

  it('loads only Task-eligible profiles lazily and defaults to read access', async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();
    render(<SSHGrantPicker value={[]} onChange={onChange} />);

    expect(api.listSSHProfiles).not.toHaveBeenCalled();
    await user.click(screen.getByRole('button', { name: /SSH access/i }));
    await screen.findByText('production-box');
    expect(screen.getByText('Files: /')).toBeInTheDocument();
    await user.click(screen.getByLabelText('Grant production-box'));

    expect(api.listSSHProfiles).toHaveBeenCalledWith(true);
    expect(onChange).toHaveBeenCalledWith([
      { profile_id: 41, capabilities: ['read'] },
    ]);
  });

  it('disables capabilities that the connection policy does not expose', async () => {
    vi.mocked(api.listSSHProfiles).mockResolvedValue([{
      ...profile,
      task_capabilities: ['read'],
    }]);
    const user = userEvent.setup();
    render(<SSHGrantPicker
      value={[{ profile_id: 41, capabilities: ['read'] }]}
      onChange={vi.fn()}
    />);

    await user.click(screen.getByRole('button', { name: /SSH access/i }));
    expect(await screen.findByLabelText('production-box: Read files')).toBeEnabled();
    expect(screen.getByLabelText('production-box: Run commands')).toBeDisabled();
    expect(screen.getByLabelText('production-box: Write files')).toBeDisabled();
  });

  it('does not persist edits until the administrator explicitly saves them', async () => {
    vi.mocked(api.listTaskSSHGrants).mockResolvedValue([grant]);
    vi.mocked(api.updateTaskSSHGrants).mockResolvedValue([
      { ...grant, capabilities: ['read', 'write'] },
    ]);
    const user = userEvent.setup();
    render(<TaskSSHAccessBadge task={task} />);

    await waitFor(() => expect(api.listTaskSSHGrants).toHaveBeenCalledWith(9));
    await user.click(screen.getByRole('button', { name: 'SSH 1' }));
    await user.click(screen.getByLabelText('production-box: Write files'));
    expect(api.updateTaskSSHGrants).not.toHaveBeenCalled();

    await user.click(screen.getByRole('button', { name: 'Save SSH grants' }));
    await waitFor(() => expect(api.updateTaskSSHGrants).toHaveBeenCalledWith(9, [
      { profile_id: 41, capabilities: ['read', 'write'] },
    ]));
  });

  it('allows an unchanged stale snapshot to be re-authorized', async () => {
    const staleGrant = {
      ...grant,
      profile_revision: 1,
      current_profile_revision: 2,
      valid: false,
      invalid_reason: 'profile_revision_changed',
    };
    vi.mocked(api.listTaskSSHGrants).mockResolvedValue([staleGrant]);
    vi.mocked(api.updateTaskSSHGrants).mockResolvedValue([grant]);
    const user = userEvent.setup();
    render(<TaskSSHAccessBadge task={task} />);

    await user.click(await screen.findByRole('button', { name: 'SSH 1' }));
    expect(screen.getByText('re-authorize')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Save SSH grants' }));

    expect(api.updateTaskSSHGrants).toHaveBeenCalledWith(9, [
      { profile_id: 41, capabilities: ['read'] },
    ]);
  });

  it('shows member grants read-only', async () => {
    localStorage.setItem('cc_user', JSON.stringify({ id: 2, role: 'member' }));
    vi.mocked(api.listTaskSSHGrants).mockResolvedValue([grant]);
    const user = userEvent.setup();
    render(<TaskSSHAccessBadge task={task} />);

    await user.click(await screen.findByRole('button', { name: 'SSH 1' }));
    expect(screen.getByText(/Read-only view/)).toBeInTheDocument();
    expect(screen.getByLabelText('Grant production-box')).toBeDisabled();
    expect(screen.queryByRole('button', { name: 'Save SSH grants' })).not.toBeInTheDocument();
  });
});
