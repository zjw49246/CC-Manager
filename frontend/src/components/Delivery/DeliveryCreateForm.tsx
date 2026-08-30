import { useEffect, useMemo, useState } from 'react';

import { api, type MonitoredRepo, type Project, type SystemConfig } from '../../api/client';
import { ProjectSelect } from '../ProjectSelect';
import { AlertCircle, Play } from '../icons';
import { acknowledgeDeliveryQuickStart, prepareDeliveryQuickStart } from '../Tasks/deliveryAdmission';
import { filterDeliveryRepos } from '../Tasks/deliveryCompatibility';

interface Props {
  projects: Project[];
  repos: MonitoredRepo[];
  config: SystemConfig | null;
  onCreated: () => void;
  onNavigateProjects: () => void;
  onNavigatePRMonitor: () => void;
}

export function DeliveryCreateForm({ projects, repos, config, onCreated, onNavigateProjects, onNavigatePRMonitor }: Props) {
  const [projectId, setProjectId] = useState<number | undefined>();
  const [title, setTitle] = useState('');
  const [requirements, setRequirements] = useState('');
  const [autoMerge, setAutoMerge] = useState(false);
  const [strictBranchProtection, setStrictBranchProtection] = useState(false);
  const [frontendReview, setFrontendReview] = useState<'auto' | 'required' | 'off'>('auto');
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState('');
  const project = projects.find((item) => item.id === projectId);
  const providerOptions = useMemo(() => config?.provider_options || [], [config?.provider_options]);
  const compatibleRepos = useMemo(
    () => filterDeliveryRepos(project, repos, providerOptions),
    [project, providerOptions, repos],
  );
  const repo = compatibleRepos.length === 1 ? compatibleRepos[0] : null;

  useEffect(() => {
    setError('');
  }, [projectId]);

  const submit = async (event: React.FormEvent) => {
    event.preventDefault();
    if (!project || !config) return;
    const draft = {
      project_id: project.id,
      requirements: requirements.trim(),
      auto_merge: autoMerge,
      strict_branch_protection: strictBranchProtection,
      frontend_review: frontendReview,
      ...(title.trim() ? { title: title.trim() } : {}),
    };
    const scope = `delivery-quick-start:${project.id}`;
    const request = prepareDeliveryQuickStart(scope, draft);
    setSubmitting(true);
    setError('');
    try {
      await api.quickStartDelivery(request);
      acknowledgeDeliveryQuickStart(scope, request);
      setTitle('');
      setRequirements('');
      setAutoMerge(false);
      setStrictBranchProtection(false);
      setFrontendReview('auto');
      onCreated();
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : String(reason));
    } finally {
      setSubmitting(false);
    }
  };

  const disabledReason = !config?.delivery_loop_enabled
    ? 'Delivery Loop is disabled in server configuration.'
    : project?.worker_id != null
      ? 'Delivery V1 only supports local projects.'
      : project && project.status !== 'ready'
        ? 'Project import is still running. Start Delivery after the clone is ready.'
      : project && !project.has_remote
        ? 'This project has no configured Git remote.'
        : null;
  const willAutoConfigure = Boolean(project && compatibleRepos.length === 0 && !disabledReason);
  const autoMergeUnavailable = Boolean(repo && (
    !repo.wait_for_ci
    || repo.required_checks.length === 0
    || repo.required_checks.some((check) => (
      check.kind !== 'check_run'
      || !check.name.trim()
      || !check.app_slug.trim()
    ))
  ));

  return (
    <form onSubmit={submit} className="rounded-lg border border-gray-800 bg-gray-900/70 p-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div><h2 className="text-sm font-semibold text-gray-100">New Delivery</h2><p className="mt-1 text-xs text-gray-500">Choose a Project and describe the outcome.</p></div>
      </div>
      <div className="mt-4 grid gap-3 lg:grid-cols-[minmax(220px,.7fr)_1fr]">
        <ProjectSelect projects={projects.filter((item) => item.show_in_selector)} value={projectId} onChange={(value) => setProjectId(value ? Number(value) : undefined)} placeholder="Select Project" />
        <input aria-label="Delivery title" value={title} onChange={(event) => setTitle(event.target.value)} maxLength={200} placeholder="Optional short title (defaults to the first line)" className="rounded-lg border border-gray-700 bg-gray-800 px-3 py-2 text-sm text-gray-100 outline-none focus:border-indigo-500" />
      </div>
      <textarea aria-label="Delivery requirements" value={requirements} onChange={(event) => setRequirements(event.target.value)} required rows={3} placeholder="Describe the task and acceptance criteria…" className="mt-3 w-full resize-y rounded-lg border border-gray-700 bg-gray-800 px-3 py-2 text-sm text-gray-100 outline-none focus:border-indigo-500" />
      {project && repo && <div className="mt-3 flex flex-wrap gap-x-5 gap-y-1 rounded-lg border border-gray-800 bg-gray-950/50 px-3 py-2 text-[11px] text-gray-500"><span>Repository <b className="text-gray-300">{repo.repo_full_name}</b></span><span>Branch <b className="text-gray-300">{project.default_branch}</b></span><span>Provider <b className="text-gray-300">{repo.provider}</b></span><span>PR Monitor <b className="text-gray-300">Panel{repo.wait_for_ci ? ' + exact CI' : ''}</b></span></div>}
      {willAutoConfigure && <div className="mt-3 rounded-lg border border-indigo-500/25 bg-indigo-500/10 px-3 py-2 text-xs leading-5 text-indigo-200">PR Monitor is created and bound automatically on first start. Panel review is always enabled; exact-head CI is added when GitHub exposes a required set, otherwise CCM continues with Panel review without asking you to configure Monitor fields.</div>}
      <label className="mt-3 flex items-start gap-2 rounded-lg border border-gray-800 bg-gray-950/40 px-3 py-2 text-xs text-gray-300">
        <input type="checkbox" checked={autoMerge} disabled={autoMergeUnavailable} onChange={(event) => setAutoMerge(event.target.checked)} className="mt-0.5 accent-indigo-500" />
        <span><b>Merge automatically after all gates pass</b><span className="mt-0.5 block text-gray-500">Off by default. When enabled, exact required CI, Panel findings and GitHub write permission must all pass.</span>{autoMergeUnavailable && <span className="mt-0.5 block text-amber-400">This repository has no exact required CI policy, so automatic merge is unavailable.</span>}</span>
      </label>
      {autoMerge && (
        <label className="mt-3 flex items-start gap-2 rounded-lg border border-gray-800 bg-gray-950/40 px-3 py-2 text-xs text-gray-300">
          <input type="checkbox" checked={strictBranchProtection} onChange={(event) => setStrictBranchProtection(event.target.checked)} className="mt-0.5 accent-indigo-500" />
          <span><b>Strict branch-protection mode</b><span className="mt-0.5 block text-gray-500">Off by default. Enable it only when GitHub Branch Protection and repository rules must also be proved before merging.</span></span>
        </label>
      )}
      <label className="mt-3 block rounded-lg border border-gray-800 bg-gray-950/40 px-3 py-2 text-xs text-gray-300">
        <span className="font-semibold">Frontend review gate</span>
        <select
          aria-label="Frontend review gate"
          value={frontendReview}
          onChange={(event) => setFrontendReview(event.target.value as 'auto' | 'required' | 'off')}
          className="mt-2 w-full rounded-lg border border-gray-700 bg-gray-900 px-3 py-2 text-xs text-gray-200 outline-none focus:border-indigo-500"
        >
          <option value="auto">Auto — run when a trusted Preview is configured</option>
          <option value="required">Required — fail admission unless Browser review is available</option>
          <option value="off">Off — use code review and PR gates only</option>
        </select>
        <span className="mt-1.5 block leading-5 text-gray-500">
          Browser Agent validates the visible flow and reports evidence. It never edits code; failed findings return to the next Developer cycle.
        </span>
      </label>
      {disabledReason && <div className="mt-3 flex items-start gap-2 rounded-lg border border-amber-500/25 bg-amber-500/10 px-3 py-2 text-xs leading-5 text-amber-300"><AlertCircle size={14} className="mt-0.5 shrink-0" /><div>{disabledReason}<div className="mt-1 flex gap-3"><button type="button" onClick={onNavigateProjects} className="underline underline-offset-2">Open Projects</button><button type="button" onClick={onNavigatePRMonitor} className="underline underline-offset-2">View PR Monitor</button></div></div></div>}
      {error && <div className="mt-3 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs text-red-300">{error}</div>}
      <div className="mt-4 flex justify-end"><button type="submit" disabled={submitting || !project || !requirements.trim() || Boolean(disabledReason)} className="inline-flex items-center gap-2 rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-500 disabled:cursor-not-allowed disabled:opacity-40"><Play size={15} />{submitting ? 'Configuring & starting…' : willAutoConfigure ? 'Configure & Start Delivery' : 'Start Delivery'}</button></div>
    </form>
  );
}
