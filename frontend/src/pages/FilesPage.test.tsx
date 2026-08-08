import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { cleanup, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { FilesPage } from './FilesPage';
import type { SSHProfile } from '../api/client';

vi.mock('../api/client', () => ({
  getToken: vi.fn(() => 'test-token'),
  api: {
    listProjects: vi.fn(),
    listWorkers: vi.fn(),
    listSSHProfiles: vi.fn(),
    managedSSHListDir: vi.fn(),
    managedSSHReadFile: vi.fn(),
    managedSSHDownloadFile: vi.fn(),
    probeSSHHostKey: vi.fn(),
    uploadSSHPrivateKey: vi.fn(),
    cancelSSHPrivateKeyUpload: vi.fn(),
    createSSHProfile: vi.fn(),
    updateSSHProfile: vi.fn(),
    deleteSSHProfile: vi.fn(),
    testSSHProfile: vi.fn(),
  },
}));

import { api } from '../api/client';

const managedProfile: SSHProfile = {
  id: 41,
  name: 'production-box',
  host: 'ssh.internal',
  port: 2222,
  username: 'deploy',
  key_path_hint: '…/id_ed25519',
  public_key_fingerprint: 'SHA256:client-key',
  host_key_type: 'ssh-ed25519',
  host_key_fingerprint: 'SHA256:server-key',
  revision: 1,
  enabled: true,
  task_access_enabled: true,
  task_capabilities: ['read', 'exec'],
  allowed_roots: ['/'],
  created_by: 1,
  last_tested_at: null,
  last_test_ok: null,
  last_error_code: null,
  last_error_detail: null,
  created_at: '2026-08-06T00:00:00',
  updated_at: '2026-08-06T00:00:00',
};

describe('FilesPage managed SSH workspace', () => {
  beforeEach(() => {
    localStorage.setItem('cc_user', JSON.stringify({ id: 1, role: 'admin' }));
    vi.mocked(api.listProjects).mockResolvedValue([]);
    vi.mocked(api.listWorkers).mockResolvedValue([]);
    vi.mocked(api.listSSHProfiles).mockResolvedValue([managedProfile]);
    vi.mocked(api.managedSSHListDir).mockResolvedValue({ path: '/', entries: [] });
    vi.mocked(api.probeSSHHostKey).mockResolvedValue({
      key_type: 'ssh-ed25519',
      host_key_value: 'ssh-ed25519 AAAAhost',
      fingerprint: 'SHA256:verified-host',
    });
    vi.mocked(api.createSSHProfile).mockResolvedValue(managedProfile);
    vi.mocked(api.updateSSHProfile).mockResolvedValue(managedProfile);
    vi.mocked(api.uploadSSHPrivateKey).mockResolvedValue({
      upload_token: 'upload-token-1',
      filename: 'production.pem',
      public_key_fingerprint: 'SHA256:uploaded-client-key',
    });
    vi.mocked(api.cancelSSHPrivateKeyUpload).mockResolvedValue({ ok: true });
  });

  afterEach(() => {
    cleanup();
    localStorage.clear();
    vi.clearAllMocks();
  });

  it('browses through a managed profile id from the unified connection list', async () => {
    const user = userEvent.setup();
    render(<FilesPage />);

    await user.click(screen.getByRole('button', { name: /SSH workspace/i }));
    await user.click(await screen.findByRole('button', { name: /production-box/i }));

    await waitFor(() => {
      expect(api.managedSSHListDir).toHaveBeenCalledWith(41, '/');
    });
    expect(screen.getByText('SSH connections')).toBeInTheDocument();
    expect(screen.getByText('Task: read')).toBeInTheDocument();
    expect(screen.queryByText('Legacy browser-only connections')).not.toBeInTheDocument();
    expect(screen.queryByText('/absolute/path/to/id_ed25519')).not.toBeInTheDocument();
  });

  it('requires probing and persists only the backend key path plus pinned host key', async () => {
    vi.mocked(api.listSSHProfiles)
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([managedProfile]);
    const user = userEvent.setup();
    render(<FilesPage />);

    await user.click(screen.getByRole('button', { name: /SSH workspace/i }));
    await user.click(await screen.findByRole('button', { name: /Add SSH connection/i }));
    await user.type(screen.getByLabelText('Name'), 'production-box');
    await user.type(screen.getByLabelText('Username'), 'deploy');
    await user.type(screen.getByLabelText('Host'), 'ssh.internal');
    await user.clear(screen.getByLabelText('Port'));
    await user.type(screen.getByLabelText('Port'), '2222');
    await user.type(screen.getByLabelText('Private key path on Manager'), '/keys/id_ed25519');
    await user.click(screen.getByRole('button', { name: /Probe host identity/i }));

    expect(await screen.findByText('SHA256:verified-host')).toBeInTheDocument();
    await user.click(screen.getByLabelText('I verified this host fingerprint'));
    await user.click(screen.getByRole('button', { name: /^Save$/i }));

    await waitFor(() => {
      expect(api.createSSHProfile).toHaveBeenCalledWith({
        name: 'production-box',
        host: 'ssh.internal',
        port: 2222,
        username: 'deploy',
        key_path: '/keys/id_ed25519',
        host_key_value: 'ssh-ed25519 AAAAhost',
        enabled: true,
        allowed_roots: ['/'],
        task_access_enabled: false,
        task_capabilities: [],
      });
    });
  });

  it('uploads a private key and saves only its one-time token', async () => {
    vi.mocked(api.listSSHProfiles)
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([managedProfile]);
    const user = userEvent.setup();
    render(<FilesPage />);

    await user.click(screen.getByRole('button', { name: /SSH workspace/i }));
    await user.click(await screen.findByRole('button', { name: /Add SSH connection/i }));
    const key = new File(['private-key'], 'production.pem', {
      type: 'application/x-pem-file',
    });
    await user.upload(screen.getByLabelText('Upload SSH private key'), key);

    expect(await screen.findByText('Uploaded: production.pem')).toBeInTheDocument();
    expect(screen.getByText('SHA256:uploaded-client-key')).toBeInTheDocument();
    expect(api.uploadSSHPrivateKey).toHaveBeenCalledWith(key);

    await user.type(screen.getByLabelText('Name'), 'production-box');
    await user.type(screen.getByLabelText('Username'), 'deploy');
    await user.type(screen.getByLabelText('Host'), 'ssh.internal');
    await user.click(screen.getByRole('button', { name: /Probe host identity/i }));
    await user.click(screen.getByLabelText('I verified this host fingerprint'));
    await user.click(screen.getByRole('button', { name: /^Save$/i }));

    await waitFor(() => {
      expect(api.createSSHProfile).toHaveBeenCalledWith({
        name: 'production-box',
        host: 'ssh.internal',
        port: 22,
        username: 'deploy',
        key_upload_token: 'upload-token-1',
        host_key_value: 'ssh-ed25519 AAAAhost',
        enabled: true,
        allowed_roots: ['/'],
        task_access_enabled: false,
        task_capabilities: [],
      });
    });
    expect(api.cancelSSHPrivateKeyUpload).not.toHaveBeenCalled();
  });

  it('cancels a pending private-key upload when the editor is cancelled', async () => {
    vi.mocked(api.listSSHProfiles).mockResolvedValueOnce([]);
    const user = userEvent.setup();
    render(<FilesPage />);

    await user.click(screen.getByRole('button', { name: /SSH workspace/i }));
    await user.click(await screen.findByRole('button', { name: /Add SSH connection/i }));
    await user.upload(
      screen.getByLabelText('Upload SSH private key'),
      new File(['private-key'], 'production.pem'),
    );
    await screen.findByText('Uploaded: production.pem');
    await user.click(screen.getByRole('button', { name: /^Cancel$/i }));

    expect(api.cancelSSHPrivateKeyUpload).toHaveBeenCalledWith('upload-token-1');
    expect(screen.queryByText('Uploaded: production.pem')).not.toBeInTheDocument();
  });

  it('configures the maximum capabilities exposed to Tasks', async () => {
    vi.mocked(api.listSSHProfiles)
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([managedProfile]);
    const user = userEvent.setup();
    render(<FilesPage />);

    await user.click(screen.getByRole('button', { name: /SSH workspace/i }));
    await user.click(await screen.findByRole('button', { name: /Add SSH connection/i }));
    await user.type(screen.getByLabelText('Name'), 'task-readable');
    await user.type(screen.getByLabelText('Username'), 'deploy');
    await user.type(screen.getByLabelText('Host'), 'ssh.internal');
    await user.type(screen.getByLabelText('Private key path on Manager'), '/keys/id_ed25519');
    await user.click(screen.getByRole('button', { name: /Probe host identity/i }));
    await user.click(screen.getByLabelText('I verified this host fingerprint'));
    await user.click(screen.getByLabelText('Allow Tasks to use this connection'));
    await user.click(screen.getByLabelText('Task capability: Run commands'));
    await user.click(screen.getByRole('button', { name: /^Save$/i }));

    await waitFor(() => expect(api.createSSHProfile).toHaveBeenCalledWith(
      expect.objectContaining({
        task_access_enabled: true,
        task_capabilities: ['read', 'exec'],
      }),
    ));
  });

  it('keeps a legacy connection in the unified list and migrates it through PEM upload', async () => {
    localStorage.setItem('cc_ssh_profiles', JSON.stringify([{
      id: 'legacy-1',
      label: 'files-box',
      host: 'legacy.internal',
      port: 22,
      username: 'reader',
      password: 'not-copied',
      key_path: '',
    }]));
    const user = userEvent.setup();
    render(<FilesPage />);

    await user.click(screen.getByRole('button', { name: /SSH workspace/i }));
    expect(await screen.findByText('Legacy · migrate to expose to Tasks')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'migrate' }));
    expect(screen.getByDisplayValue('files-box')).toBeInTheDocument();
    expect(screen.getByText(/Legacy passwords are not copied/)).toBeInTheDocument();

    await user.upload(
      screen.getByLabelText('Upload SSH private key'),
      new File(['private-key'], 'files-box.pem'),
    );
    await user.click(screen.getByRole('button', { name: /Probe host identity/i }));
    await user.click(screen.getByLabelText('I verified this host fingerprint'));
    await user.click(screen.getByRole('button', { name: /^Save$/i }));

    await waitFor(() => expect(api.createSSHProfile).toHaveBeenCalledWith(
      expect.objectContaining({
        name: 'files-box',
        key_upload_token: 'upload-token-1',
        task_access_enabled: false,
        task_capabilities: [],
      }),
    ));
    expect(JSON.parse(localStorage.getItem('cc_ssh_profiles') || '[]')).toEqual([]);
  });
});
