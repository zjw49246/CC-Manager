import { useCallback, useEffect, useRef, useState } from 'react';

import { api, type PlanResource, type Project, type TagItem } from '../../api/client';
import { useFileDrop } from '../../hooks/useFileDrop';
import { useFileUpload } from '../../hooks/useFileUpload';
import { ProjectSelect } from '../ProjectSelect';
import { AlertCircle, Loader2, Paperclip, Plus, Settings, X } from '../icons';
import { VoiceButton } from '../Voice/VoiceButton';

const NEW_PROJECT_VALUE = '__new__';

interface Props {
  onCreated: (plan: PlanResource) => void;
  onNavigateSettings?: () => void;
}

export function PlanCreateForm({ onCreated, onNavigateSettings }: Props) {
  const ccUser = JSON.parse(localStorage.getItem('cc_user') || '{}');
  const isAdmin = ccUser.role === 'admin' || ccUser.role === 'super_admin' || !ccUser.id;
  const [title, setTitle] = useState('');
  const [request, setRequest] = useState('');
  const [projectId, setProjectId] = useState<number | ''>('');
  const [projects, setProjects] = useState<Project[]>([]);
  const [tags, setTags] = useState<TagItem[]>([]);
  const [isNewProject, setIsNewProject] = useState(false);
  const [newProjectName, setNewProjectName] = useState('');
  const [newProjectUrl, setNewProjectUrl] = useState('');
  const [priority, setPriority] = useState(0);
  const [timeoutHours, setTimeoutHours] = useState('');
  const [expanded, setExpanded] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [dropError, setDropError] = useState('');
  const formRef = useRef<HTMLFormElement>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const uploads = useFileUpload();
  const addFiles = uploads.addFiles;

  const loadProjects = useCallback(() => {
    void api.listProjects().then(setProjects).catch(() => {});
    void api.listTags().then(setTags).catch(() => {});
  }, []);

  useEffect(() => {
    loadProjects();
  }, [loadProjects]);

  useFileDrop({
    targetRef: formRef,
    onDrop: (files) => addFiles(files, setDropError),
    disabled: busy,
  });

  useEffect(() => {
    const form = formRef.current;
    if (!form) return;
    const handlePaste = (event: ClipboardEvent) => {
      const files = Array.from(event.clipboardData?.items || [])
        .filter((item) => item.kind === 'file')
        .map((item) => item.getAsFile())
        .filter((file): file is File => file != null);
      if (files.length === 0) return;
      event.preventDefault();
      addFiles(files, setDropError);
    };
    form.addEventListener('paste', handlePaste);
    return () => form.removeEventListener('paste', handlePaste);
  }, [addFiles]);

  const handleProjectChange = (value: string) => {
    if (value === NEW_PROJECT_VALUE) {
      setIsNewProject(true);
      setProjectId('');
      return;
    }
    setIsNewProject(false);
    setNewProjectName('');
    setNewProjectUrl('');
    setProjectId(value ? Number(value) : '');
  };

  const selectedProject = projectId
    ? projects.find((project) => project.id === projectId)
    : undefined;

  const canSubmit = Boolean(
    request.trim()
    && (projectId || (isNewProject && newProjectName.trim()))
    && selectedProject?.status !== 'error'
    && !uploads.isUploading
    && !uploads.hasFailed
    && !busy,
  );

  const submit = async (event: React.FormEvent) => {
    event.preventDefault();
    if (!canSubmit) return;
    setBusy(true);
    setError('');
    try {
      let selectedProjectId = projectId || undefined;
      if (isNewProject) {
        const project = await api.createProject({
          name: newProjectName.trim(),
          git_url: newProjectUrl.trim() || undefined,
        });
        selectedProjectId = project.id;
        setProjectId(project.id);
        setIsNewProject(false);
        setNewProjectName('');
        setNewProjectUrl('');
        loadProjects();
      }
      const results = uploads.uploadedResults;
      const plan = await api.createPlan({
        input: request.trim(),
        ...(title.trim() ? { title: title.trim() } : {}),
        project_id: selectedProjectId as number,
        priority,
        ...(timeoutHours !== '' ? { timeout_hours: Number(timeoutHours) } : {}),
        ...(results.length ? {
          file_paths: results.map((item) => item.path),
          image_paths: results.filter((item) => item.is_image).map((item) => item.path),
          attachments: results.map((item) => ({
            url: item.url,
            name: item.filename || item.url.split('/').pop() || 'file',
            is_image: item.is_image,
          })),
        } : {}),
      });
      setTitle('');
      setRequest('');
      uploads.clear();
      setExpanded(false);
      onCreated(plan);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : 'Failed to create Plan');
    } finally {
      setBusy(false);
    }
  };

  const tagColorMap = Object.fromEntries(tags.map((tag) => [tag.name, tag.color]));

  if (!expanded) {
    return <div className="flex items-center justify-between border-b border-gray-800 pb-3">
      <button type="button" onClick={() => setExpanded(true)} className="inline-flex items-center gap-1.5 rounded-lg bg-indigo-600 px-3 py-2 text-xs font-semibold text-white hover:bg-indigo-500"><Plus size={14} /> New Plan</button>
      {isAdmin && onNavigateSettings && <button type="button" onClick={onNavigateSettings} aria-label="Plan settings" title="Plan settings" className="rounded-md p-2 text-gray-500 transition-colors hover:bg-gray-800 hover:text-gray-200"><Settings size={15} /></button>}
    </div>;
  }

  return <form ref={formRef} onSubmit={submit} className="space-y-3 border-b border-gray-800 pb-4">
    <div className="flex items-center justify-between gap-3">
      <h2 className="text-sm font-semibold text-gray-200">New Plan</h2>
      <button type="button" onClick={() => setExpanded(false)} aria-label="Close New Plan" className="rounded p-1.5 text-gray-500 hover:bg-gray-800 hover:text-gray-200"><X size={15} /></button>
    </div>
    {(error || dropError) && <div className="flex items-center justify-between gap-3 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs text-red-300"><span>{error || dropError}</span><button type="button" onClick={() => { setError(''); setDropError(''); }} aria-label="Dismiss Plan form error"><X size={13} /></button></div>}
    <input value={title} onChange={(event) => setTitle(event.target.value)} maxLength={200} placeholder="Plan title (optional)" className="w-full rounded-lg border border-gray-600/60 bg-gray-700 px-3 py-2 text-sm text-gray-100 outline-none focus:border-indigo-500" />
    <div className="flex gap-2">
      <textarea value={request} onChange={(event) => setRequest(event.target.value)} maxLength={200000} rows={5} required placeholder="What should this Plan investigate and decide?" className="min-w-0 flex-1 resize-y rounded-lg border border-gray-600/60 bg-gray-700 px-3 py-2 text-sm text-gray-100 outline-none focus:border-indigo-500" />
      <VoiceButton onTranscribed={(text) => setRequest((current) => current ? `${current} ${text}` : text)} />
    </div>
    {uploads.uploads.length > 0 && <div className="flex flex-wrap gap-2">{uploads.uploads.map((item) => { const name = item.file?.name || item.result?.filename || 'file'; return <div key={item.id} className="flex items-center gap-2 rounded-lg border border-gray-700 bg-gray-900/50 px-2 py-1.5 text-xs text-gray-400">{item.preview && <img src={item.preview} alt="" className="h-8 w-8 rounded object-cover" />}<span className="max-w-48 truncate">{name}</span>{item.status === 'uploading' && <Loader2 size={12} className="animate-spin" />}{item.status === 'failed' && <AlertCircle size={12} className="text-red-400" />}<button type="button" onClick={() => uploads.removeFile(item.id)} aria-label={`Remove ${name}`}><X size={12} /></button></div>; })}</div>}
    <div className="grid gap-3 md:grid-cols-[minmax(220px,1fr)_auto] md:items-center">
      <ProjectSelect projects={projects.filter((project) => project.show_in_selector)} value={isNewProject ? NEW_PROJECT_VALUE : projectId || undefined} onChange={handleProjectChange} placeholder="Select project…" extraOptions={isAdmin ? [{ value: NEW_PROJECT_VALUE, label: '+ New project' }] : []} showStatus tagColorMap={tagColorMap} />
      <div className="flex justify-end gap-2"><input ref={fileInputRef} type="file" multiple className="hidden" onChange={(event) => { addFiles(Array.from(event.target.files || []), setDropError); event.target.value = ''; }} /><button type="button" onClick={() => fileInputRef.current?.click()} className="rounded-lg border border-gray-600 p-2 text-gray-400 hover:text-gray-200" aria-label="Attach Plan files"><Paperclip size={14} /></button><button type="submit" disabled={!canSubmit} className="rounded-lg bg-indigo-600 px-4 py-2 text-xs font-semibold text-white hover:bg-indigo-500 disabled:opacity-40">{busy ? 'Creating…' : 'Create Plan'}</button></div>
    </div>
    {selectedProject && selectedProject.status && selectedProject.status !== 'ready' && (
      selectedProject.status === 'error'
        ? <div className="flex items-start gap-2 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs leading-5 text-red-300"><AlertCircle size={14} className="mt-0.5 shrink-0" /><div>项目克隆失败，请先在 Projects 页 Re-clone 项目。{selectedProject.error_message && <div className="mt-0.5 text-red-400">{selectedProject.error_message}</div>}</div></div>
        : <div className="flex items-start gap-2 rounded-lg border border-amber-500/25 bg-amber-500/10 px-3 py-2 text-xs leading-5 text-amber-300"><AlertCircle size={14} className="mt-0.5 shrink-0" /><span>项目仍在克隆/初始化，任务将等待项目就绪后自动开始。</span></div>
    )}
    {isNewProject && <div className="grid gap-2 sm:grid-cols-2"><input value={newProjectName} onChange={(event) => setNewProjectName(event.target.value)} required placeholder="Project name" className="rounded-lg border border-gray-600 bg-gray-700 px-3 py-2 text-sm text-gray-100" /><input value={newProjectUrl} onChange={(event) => setNewProjectUrl(event.target.value)} placeholder="Git URL (optional)" className="rounded-lg border border-gray-600 bg-gray-700 px-3 py-2 text-sm text-gray-100" /></div>}
    <details className="text-xs text-gray-500">
      <summary className="cursor-pointer hover:text-gray-300">Advanced</summary>
      <div className="mt-2 flex flex-wrap gap-4">
        <label className="flex items-center gap-2">Priority <select value={priority} onChange={(event) => setPriority(Number(event.target.value))} className="rounded border border-gray-700 bg-gray-800 px-2 py-1 text-gray-200">{Array.from({ length: 10 }, (_, value) => <option key={value} value={value}>{value}</option>)}</select></label>
        <label className="flex items-center gap-2">Timeout <input type="number" min="0" step="0.5" value={timeoutHours} onChange={(event) => setTimeoutHours(event.target.value)} placeholder="default" className="w-20 rounded border border-gray-700 bg-gray-800 px-2 py-1 text-gray-200" /></label>
      </div>
    </details>
  </form>;
}
