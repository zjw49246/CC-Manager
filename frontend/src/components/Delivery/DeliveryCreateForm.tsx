import { useEffect, useMemo, useState } from 'react';

import { api, type MonitoredRepo, type Project, type SystemConfig } from '../../api/client';
import { ProjectSelect } from '../ProjectSelect';
import { AlertCircle, Play } from '../icons';
import { acknowledgeDeliveryAdmission, prepareDeliveryAdmission, resolveDeliveryProvider } from '../Tasks/deliveryAdmission';
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
  const [submitting, setSubmitting] = useState(false);
  const [strictBranchProtection, setStrictBranchProtection] = useState(false);
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
    if (!project || !repo || !config) return;
    const provider = resolveDeliveryProvider(repo.provider, config.default_provider, providerOptions);
    if (!provider) {
      setError('The configured Delivery provider is not available.');
      return;
    }
    const draft = {
      project_id: project.id,
      monitored_repo_id: repo.id,
      title: title.trim(),
      requirements: requirements.trim(),
      base_branch: project.default_branch,
      provider,
      model: provider === 'codex' ? config.default_codex_model : config.default_model,
      codex_service_tier: provider === 'codex' ? (config.default_codex_service_tier || 'default') : 'default',
      effort_level: config.default_effort,
      strict_branch_protection: strictBranchProtection,
    } as const;
    const request = prepareDeliveryAdmission(`delivery-page:${project.id}`, draft);
    setSubmitting(true);
    setError('');
    try {
      await api.createDeliveryRun(request);
      acknowledgeDeliveryAdmission(`delivery-page:${project.id}`, request);
      setTitle('');
      setRequirements('');
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
      : project && !project.has_remote
        ? 'This project has no configured Git remote.'
        : project && compatibleRepos.length === 0
          ? 'No compatible enabled PR Monitor configuration is bound to this project.'
          : compatibleRepos.length > 1
            ? 'Multiple compatible PR Monitor repositories are bound to this project. Keep exactly one enabled for automatic selection.'
            : null;

  return (
    <form onSubmit={submit} className="rounded-2xl border border-gray-800 bg-gray-900/70 p-4 shadow-sm sm:p-5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div><h1 className="text-lg font-semibold text-gray-100">Start a Delivery</h1><p className="mt-1 text-xs leading-5 text-gray-500">Choose a configured Project. Repository access, required checks, review policy and merge behavior come from Projects and PR Monitor.</p></div>
        <span className="rounded-full border border-indigo-500/30 bg-indigo-500/10 px-2.5 py-1 text-[11px] text-indigo-300">Plan → Code → Review → PR Gate</span>
      </div>
      {config?.agent_sandbox_unrestricted_enabled && (
        <div role="alert" className="mt-4 rounded-lg border border-red-500/50 bg-red-950/50 px-3 py-2 text-xs leading-5 text-red-200">
          Agent unrestricted permissions are ON. Delivery coding turns can access the host filesystem and network without CCM permission prompts. Plan, Reviewer and Browser capability boundaries remain restricted.
        </div>
      )}
      <div className="mt-4 grid gap-3 lg:grid-cols-[minmax(220px,.7fr)_1fr]">
        <ProjectSelect projects={projects.filter((item) => item.show_in_selector)} value={projectId} onChange={(value) => setProjectId(value ? Number(value) : undefined)} placeholder="Select Project" />
        <input aria-label="Delivery title" value={title} onChange={(event) => setTitle(event.target.value)} maxLength={200} required placeholder="What should this Delivery accomplish?" className="rounded-lg border border-gray-700 bg-gray-800 px-3 py-2 text-sm text-gray-100 outline-none focus:border-indigo-500" />
      </div>
      <textarea aria-label="Delivery requirements" value={requirements} onChange={(event) => setRequirements(event.target.value)} required rows={4} placeholder="Describe requirements and acceptance criteria…" className="mt-3 w-full resize-y rounded-lg border border-gray-700 bg-gray-800 px-3 py-2 text-sm text-gray-100 outline-none focus:border-indigo-500" />
      {repo?.auto_merge && (
        <label className="mt-3 flex cursor-pointer items-start gap-3 rounded-lg border border-gray-800 bg-gray-950/50 px-3 py-2.5">
          <input aria-label="Require strict GitHub branch protection" type="checkbox" checked={strictBranchProtection} onChange={(event) => setStrictBranchProtection(event.target.checked)} className="mt-0.5 h-4 w-4 rounded border-gray-600 bg-gray-800 text-indigo-600" />
          <span><span className="block text-xs font-medium text-gray-200">Strict branch-protection mode</span><span className="mt-0.5 block text-[11px] leading-4 text-gray-500">Off by default. Trusted mode still requires exact CI, Panel findings and GitHub write permission; enable this only when GitHub Branch Protection must also be proved.</span></span>
        </label>
      )}
      {project && repo && <div className="mt-3 flex flex-wrap gap-x-5 gap-y-1 rounded-lg border border-gray-800 bg-gray-950/50 px-3 py-2 text-[11px] text-gray-500"><span>Repository <b className="text-gray-300">{repo.repo_full_name}</b></span><span>Branch <b className="text-gray-300">{project.default_branch}</b></span><span>Provider <b className="text-gray-300">{repo.provider}</b></span><span>Completion <b className="text-gray-300">{repo.auto_merge ? 'merged' : 'ready to merge'}</b></span></div>}
      {disabledReason && <div className="mt-3 flex items-start gap-2 rounded-lg border border-amber-500/25 bg-amber-500/10 px-3 py-2 text-xs leading-5 text-amber-300"><AlertCircle size={14} className="mt-0.5 shrink-0" /><div>{disabledReason}<div className="mt-1 flex gap-3"><button type="button" onClick={onNavigateProjects} className="underline underline-offset-2">Open Projects</button><button type="button" onClick={onNavigatePRMonitor} className="underline underline-offset-2">Open PR Monitor</button></div></div></div>}
      {error && <div className="mt-3 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs text-red-300">{error}</div>}
      <div className="mt-4 flex justify-end"><button type="submit" disabled={submitting || !repo || !title.trim() || !requirements.trim() || Boolean(disabledReason)} className="inline-flex items-center gap-2 rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-500 disabled:cursor-not-allowed disabled:opacity-40"><Play size={15} />{submitting ? 'Starting…' : 'Start Delivery'}</button></div>
    </form>
  );
}
