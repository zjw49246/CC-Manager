import { describe, expect, it } from 'vitest';

import type { MonitoredRepo, Project } from '../../api/client';
import { isDeliveryCompatible } from './deliveryCompatibility';

const project: Project = {
  id: 1,
  name: 'repo',
  worker_id: null,
  git_url: 'git@github.com:acme/repo.git',
  has_remote: true,
  local_path: '/srv/repo',
  default_branch: 'main',
  status: 'ready',
  error_message: null,
  show_in_selector: true,
  sort_order: 0,
  tags: [],
  env_files: [],
  git_author_name: null,
  git_author_email: null,
  git_credential_type: null,
  git_ssh_key_path: null,
  git_https_username: null,
  git_https_token: null,
  badge_color: null,
  created_at: '2026-08-05T00:00:00Z',
};

const repo: MonitoredRepo = {
  id: 2,
  repo_full_name: 'acme/repo',
  project_id: 1,
  worker_id: null,
  enabled: true,
  auto_merge: false,
  webhook_secret: 'masked',
  provider: 'codex',
  review_model: null,
  review_effort: null,
  review_mode: 'panel',
  wait_for_ci: true,
  required_checks: [{ kind: 'check_run', name: 'tests', app_slug: 'github-actions' }],
  auto_repair: true,
  max_repair_attempts: 3,
  merge_queue_mode: 'manual',
  default_branch: 'main',
  allowed_authors: [],
  status: 'active',
  error_message: null,
  created_at: '2026-08-05T00:00:00Z',
  updated_at: '2026-08-05T00:00:00Z',
};

describe('isDeliveryCompatible', () => {
  it('accepts the complete local exact-head policy', () => {
    expect(isDeliveryCompatible(project, repo, ['claude', 'codex'])).toBe(true);
  });

  it('accepts the mandatory Panel when the repository declares no required CI', () => {
    expect(isDeliveryCompatible(
      project,
      { ...repo, wait_for_ci: false, required_checks: [] },
      ['claude', 'codex'],
    )).toBe(true);
  });

  it('accepts a repository whose PR Monitor owns automatic merge', () => {
    expect(isDeliveryCompatible(
      project,
      { ...repo, auto_merge: true },
      ['claude', 'codex'],
    )).toBe(true);
  });

  it('accepts a Claude monitor in a Claude-only deployment', () => {
    expect(isDeliveryCompatible(
      project,
      { ...repo, provider: 'claude' },
      ['claude'],
    )).toBe(true);
  });

  it('rejects a Codex monitor in a Claude-only deployment', () => {
    expect(isDeliveryCompatible(project, repo, ['claude'])).toBe(false);
  });

  it('rejects all monitors until provider configuration has loaded', () => {
    expect(isDeliveryCompatible(project, repo, [])).toBe(false);
  });

  it.each([
    ['remote project', { ...project, worker_id: 9 }, repo],
    ['project without remote', { ...project, has_remote: false }, repo],
    ['remote monitor', project, { ...repo, worker_id: 9 }],
    ['base mismatch', project, { ...repo, default_branch: 'develop' }],
    ['automatic Merge Queue', project, { ...repo, merge_queue_mode: 'auto' }],
    ['missing checks', project, { ...repo, required_checks: [] }],
  ])('rejects %s before admission', (_label, candidateProject, candidateRepo) => {
    expect(isDeliveryCompatible(
      candidateProject as Project,
      candidateRepo as MonitoredRepo,
      ['claude', 'codex'],
    )).toBe(false);
  });
});
