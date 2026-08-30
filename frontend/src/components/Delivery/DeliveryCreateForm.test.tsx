import { afterEach, describe, expect, it, vi } from 'vitest';
import { cleanup, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import { api, type MonitoredRepo, type Project, type SystemConfig } from '../../api/client';
import { DeliveryCreateForm } from './DeliveryCreateForm';

vi.mock('../../api/client', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../../api/client')>();
  return { ...actual, api: { ...actual.api, quickStartDelivery: vi.fn() } };
});

const project: Project = { id: 1, name: 'CCM', worker_id: null, git_url: 'git@github.com:acme/ccm.git', has_remote: true, local_path: '/srv/ccm', default_branch: 'main', status: 'ready', error_message: null, show_in_selector: true, sort_order: 0, tags: [], env_files: [], git_author_name: null, git_author_email: null, git_credential_type: null, git_ssh_key_path: null, git_https_username: null, git_https_token: null, badge_color: null, created_at: '2026-08-12T00:00:00Z' };
const repo: MonitoredRepo = { id: 2, repo_full_name: 'acme/ccm', project_id: 1, worker_id: null, enabled: true, auto_merge: false, webhook_secret: 'masked', provider: 'codex', review_model: null, review_effort: null, review_mode: 'panel', wait_for_ci: true, required_checks: [{ kind: 'check_run', name: 'tests', app_slug: 'github-actions' }], auto_repair: true, max_repair_attempts: 3, merge_queue_mode: 'manual', default_branch: 'main', allowed_authors: [], status: 'active', error_message: null, created_at: '', updated_at: '' };
const config = { delivery_loop_enabled: true, provider_options: ['codex'], default_provider: 'codex', default_model: 'claude-opus-4-6', default_codex_model: 'gpt-5.6-sol', default_effort: 'high', default_codex_service_tier: 'default' } as SystemConfig;

describe('DeliveryCreateForm', () => {
  afterEach(() => { cleanup(); vi.clearAllMocks(); sessionStorage.clear(); });

  it('automatically uses the one compatible repository and server defaults', async () => {
    vi.mocked(api.quickStartDelivery).mockResolvedValue({ id: 9 } as never);
    render(<DeliveryCreateForm projects={[project]} repos={[repo]} config={config} onCreated={() => {}} onNavigateProjects={() => {}} onNavigatePRMonitor={() => {}} />);
    await userEvent.click(screen.getByRole('button', { name: 'Select Project' }));
    await userEvent.click(screen.getByText('CCM'));
    expect(screen.getByText('acme/ccm')).toBeInTheDocument();
    await userEvent.type(screen.getByLabelText('Delivery title'), 'Ship workspace');
    await userEvent.type(screen.getByLabelText('Delivery requirements'), 'Implement and test it.');
    await userEvent.click(screen.getByRole('button', { name: 'Start Delivery' }));
    expect(api.quickStartDelivery).toHaveBeenCalledWith(expect.objectContaining({
      project_id: 1,
      title: 'Ship workspace',
      requirements: 'Implement and test it.',
      auto_merge: false,
      frontend_review: 'auto',
    }));
  });

  it('starts from one requirement and lazily configures a missing monitor', async () => {
    vi.mocked(api.quickStartDelivery).mockResolvedValue({ id: 10 } as never);
    render(<DeliveryCreateForm projects={[project]} repos={[]} config={config} onCreated={() => {}} onNavigateProjects={() => {}} onNavigatePRMonitor={() => {}} />);
    await userEvent.click(screen.getByRole('button', { name: 'Select Project' }));
    await userEvent.click(screen.getByText('CCM'));
    expect(screen.getByText(/PR Monitor is created and bound automatically/)).toBeInTheDocument();
    await userEvent.type(screen.getByLabelText('Delivery requirements'), 'Fix the login flow and test it.');
    await userEvent.click(screen.getByRole('button', { name: 'Configure & Start Delivery' }));
    expect(api.quickStartDelivery).toHaveBeenCalledWith(expect.objectContaining({
      project_id: 1,
      requirements: 'Fix the login flow and test it.',
    }));
    expect(vi.mocked(api.quickStartDelivery).mock.calls[0][0].title).toBeUndefined();
  });
  it('freezes the explicit automatic merge choice for this Delivery', async () => {
    vi.mocked(api.quickStartDelivery).mockResolvedValue({ id: 11 } as never);
    render(<DeliveryCreateForm projects={[project]} repos={[repo]} config={config} onCreated={() => {}} onNavigateProjects={() => {}} onNavigatePRMonitor={() => {}} />);
    await userEvent.click(screen.getByRole('button', { name: 'Select Project' }));
    await userEvent.click(screen.getByText('CCM'));
    await userEvent.type(screen.getByLabelText('Delivery requirements'), 'Implement the release.');
    await userEvent.click(screen.getByRole('checkbox', { name: /Merge automatically/ }));
    await userEvent.click(screen.getByRole('button', { name: 'Start Delivery' }));

    expect(api.quickStartDelivery).toHaveBeenCalledWith(expect.objectContaining({
      auto_merge: true,
    }));
  });

  it('freezes an explicit required frontend review policy', async () => {
    vi.mocked(api.quickStartDelivery).mockResolvedValue({ id: 12 } as never);
    render(<DeliveryCreateForm projects={[project]} repos={[repo]} config={config} onCreated={() => {}} onNavigateProjects={() => {}} onNavigatePRMonitor={() => {}} />);
    await userEvent.click(screen.getByRole('button', { name: 'Select Project' }));
    await userEvent.click(screen.getByText('CCM'));
    await userEvent.type(screen.getByLabelText('Delivery requirements'), 'Validate the visible workflow.');
    await userEvent.selectOptions(screen.getByLabelText('Frontend review gate'), 'required');
    await userEvent.click(screen.getByRole('button', { name: 'Start Delivery' }));

    expect(api.quickStartDelivery).toHaveBeenCalledWith(expect.objectContaining({
      frontend_review: 'required',
    }));
  });

  it('disables automatic merge for a status-only CI policy', async () => {
    const statusRepo: MonitoredRepo = {
      ...repo,
      required_checks: [{ kind: 'status', name: 'legacy-ci', app_slug: 'ci-user' }],
    };
    render(<DeliveryCreateForm projects={[project]} repos={[statusRepo]} config={config} onCreated={() => {}} onNavigateProjects={() => {}} onNavigatePRMonitor={() => {}} />);
    await userEvent.click(screen.getByRole('button', { name: 'Select Project' }));
    await userEvent.click(screen.getByText('CCM'));

    expect(screen.getByRole('checkbox', { name: /Merge automatically/ })).toBeDisabled();
    expect(screen.getByText(/automatic merge is unavailable/)).toBeInTheDocument();
  });
});
