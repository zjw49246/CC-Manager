import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import { api, type PlanResource, type UploadResult } from '../../api/client';
import { useDialogA11y } from '../../hooks/useDialogA11y';
import { useFileUpload } from '../../hooks/useFileUpload';
import { ChevronLeft, ChevronRight, ListTodo, Loader2, Paperclip, X } from '../icons';
import { PlanDetail } from './PlanDetail';
import { planDisplayStateLabel } from './planResourceStatus';
import { usePlanEvents } from './usePlanEvents';

type Filter = 'all' | 'input' | 'review' | 'running' | 'approved';
interface Props { open: boolean; taskId: number; refreshGeneration?: number; selectedVersionIds: number[]; onToggleVersion: (versionId: number) => void; onAttachVersion: (versionId: number) => void; onPlansChange: (plans: PlanResource[]) => void; onClose: () => void; }
const RUNNING = new Set(['planner', 'reviewer', 'queued', 'running', 'cancelling']);
const filterPlan = (plan: PlanResource, filter: Filter) => filter === 'all' || (filter === 'input' ? plan.display_state === 'waiting_user' : filter === 'review' ? plan.display_state === 'awaiting_review' : filter === 'running' ? RUNNING.has(plan.display_state) : ['approved', 'applied'].includes(plan.display_state));
const uploadPayload = (results: UploadResult[]) => results.length ? { file_paths: results.map((item) => item.path), image_paths: results.filter((item) => item.is_image).map((item) => item.path), attachments: results.map((item) => ({ url: item.url, name: item.filename || item.url.split('/').pop() || 'file', is_image: item.is_image })) } : {};

export function VersionedPlansDialog({ open, taskId, refreshGeneration = 0, selectedVersionIds, onToggleVersion, onAttachVersion, onPlansChange, onClose }: Props) {
  const [plans, setPlans] = useState<PlanResource[]>([]);
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [filter, setFilter] = useState<Filter>('all');
  const [requestText, setRequestText] = useState('');
  const [loading, setLoading] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const uploads = useFileUpload();
  const addPlanFiles = uploads.addFiles;
  const fileInput = useRef<HTMLInputElement>(null);
  const createForm = useRef<HTMLFormElement>(null);
  const refreshRequest = useRef(0);
  const dialogRef = useDialogA11y(open, onClose);

  useEffect(() => {
    const form = createForm.current;
    if (!form) return;
    const handlePaste = (event: ClipboardEvent) => {
      const files = Array.from(event.clipboardData?.items || [])
        .filter((item) => item.kind === 'file')
        .map((item) => item.getAsFile())
        .filter((file): file is File => file != null);
      if (files.length === 0) return;
      event.preventDefault();
      event.stopPropagation();
      addPlanFiles(files, setError);
    };
    form.addEventListener('paste', handlePaste);
    return () => form.removeEventListener('paste', handlePaste);
  }, [addPlanFiles]);

  const refresh = useCallback(async (showLoading = false) => {
    const requestId = ++refreshRequest.current;
    if (showLoading) setLoading(true);
    try {
      const rows = await api.listPlans({ target_task_id: taskId });
      if (requestId !== refreshRequest.current) return;
      setPlans(rows); onPlansChange(rows); setError(null);
      setSelectedId((current) => current != null && rows.some((plan) => plan.id === current) ? current : null);
    } catch (reason) { if (requestId === refreshRequest.current) setError(reason instanceof Error ? reason.message : String(reason)); }
    finally { if (requestId === refreshRequest.current) setLoading(false); }
  }, [onPlansChange, taskId]);

  useEffect(() => { if (!open) return; void refresh(true); const timer = window.setInterval(() => void refresh(), 15000); return () => window.clearInterval(timer); }, [open, refresh]);
  useEffect(() => { if (open && refreshGeneration > 0) void refresh(); }, [open, refresh, refreshGeneration]);
  usePlanEvents(plans, refresh);
  const selected = plans.find((plan) => plan.id === selectedId) || null;
  const filtered = useMemo(() => plans.filter((plan) => filterPlan(plan, filter)), [filter, plans]);

  const create = async () => {
    if (!requestText.trim() || busy || uploads.isUploading || uploads.hasFailed) return;
    setBusy(true); setError(null);
    try {
      await api.createPlan({ input: requestText.trim(), target_task_id: taskId, ...uploadPayload(uploads.uploadedResults) });
      setRequestText(''); uploads.clear(); await refresh();
    } catch (reason) { setError(reason instanceof Error ? reason.message : String(reason)); }
    finally { setBusy(false); }
  };
  if (!open) return null;
  const filters: { id: Filter; label: string }[] = [{ id: 'all', label: 'All' }, { id: 'input', label: 'Input' }, { id: 'review', label: 'Review' }, { id: 'running', label: 'Running' }, { id: 'approved', label: 'Approved' }];

  return <div className="fixed inset-0 z-[75] flex items-end justify-center bg-black/65 sm:items-center sm:p-5" onMouseDown={(event) => event.target === event.currentTarget && onClose()}>
    <div ref={dialogRef} role="dialog" aria-modal="true" aria-label={`Plans for Task #${taskId}`} className="relative flex h-[100dvh] w-full overflow-hidden border border-gray-700 bg-gray-900 pb-[env(safe-area-inset-bottom)] pt-[env(safe-area-inset-top)] shadow-2xl sm:h-[min(86vh,820px)] sm:max-w-6xl sm:rounded-2xl sm:pb-0 sm:pt-0">
      <button type="button" onClick={onClose} className="absolute right-3 top-[calc(env(safe-area-inset-top)+0.75rem)] z-20 flex h-11 w-11 items-center justify-center rounded-lg bg-gray-900/90 text-gray-500 transition-colors hover:bg-gray-800 hover:text-gray-200 sm:top-3" aria-label="Close Plans"><X size={18} /></button>
      <section className={`${selected ? 'hidden' : 'flex'} w-full flex-col border-gray-800 sm:flex sm:w-80 sm:shrink-0 sm:border-r`}>
        <div className="border-b border-gray-800 p-4 pr-16"><div className="flex items-center gap-2 text-sm font-semibold text-gray-100"><ListTodo size={16} className="text-indigo-300" /> Plans <span className="text-xs font-normal text-gray-500">Task #{taskId}</span></div>
          <form ref={createForm} data-attachment-paste-target="plan-create" className="mt-3 space-y-2" onSubmit={(event) => { event.preventDefault(); void create(); }}><fieldset disabled={busy} className="space-y-2"><textarea value={requestText} onChange={(event) => setRequestText(event.target.value)} rows={4} maxLength={200000} placeholder="Create an independent Plan…" className="w-full resize-none rounded-lg border border-gray-700 bg-gray-800 px-3 py-2 text-sm text-gray-100 outline-none focus:border-indigo-500" />
            {uploads.uploads.length > 0 && <div className="flex flex-wrap gap-1.5">{uploads.uploads.map((item) => <span key={item.id} className="flex items-center gap-1 rounded border border-gray-700 px-2 py-1 text-[10px] text-gray-400">{item.preview && <img src={item.preview} alt="" className="h-7 w-7 rounded object-cover" />}<span className="max-w-32 truncate">{item.file?.name || item.result?.filename || 'file'}</span>{item.status === 'uploading' && <Loader2 size={10} className="animate-spin" />}<button type="button" onClick={() => uploads.removeFile(item.id)} className="rounded p-0.5 transition-colors hover:bg-gray-800 hover:text-gray-200"><X size={10} /></button></span>)}</div>}
            <div className="flex items-center justify-between"><input ref={fileInput} type="file" multiple className="hidden" onChange={(event) => { addPlanFiles(Array.from(event.target.files || []), setError); event.target.value = ''; }} /><button type="button" onClick={() => fileInput.current?.click()} className="rounded-lg border border-gray-700 p-2 text-gray-400 transition-colors hover:border-gray-600 hover:bg-gray-800 hover:text-gray-200" aria-label="Attach Plan files"><Paperclip size={13} /></button><button type="submit" disabled={!requestText.trim() || uploads.isUploading || uploads.hasFailed} className="rounded-lg bg-indigo-600 px-3 py-2 text-xs font-semibold text-white transition-colors hover:bg-indigo-500 disabled:pointer-events-none disabled:opacity-40">Create Plan</button></div></fieldset></form>
        </div>
        <div className="flex gap-1 overflow-x-auto border-b border-gray-800 px-3 py-2">{filters.map((item) => <button key={item.id} type="button" onClick={() => setFilter(item.id)} className={`rounded-full px-2 py-1 text-[10px] transition-colors ${filter === item.id ? 'bg-indigo-500/20 text-indigo-300 hover:bg-indigo-500/30' : 'text-gray-500 hover:bg-gray-800 hover:text-gray-300'}`}>{item.label} {plans.filter((plan) => filterPlan(plan, item.id)).length}</button>)}</div>
        {error && <div className="m-3 rounded border border-red-500/30 bg-red-500/10 p-2 text-xs text-red-300">{error}</div>}
        <div className="min-h-0 flex-1 space-y-2 overflow-y-auto p-3">
          {loading && <div role="status" aria-label="Loading Plans" className="flex justify-center p-6"><Loader2 className="animate-spin text-gray-500" /></div>}
          {filtered.map((plan) => {
            const isSelected = plan.id === selectedId;
            return <button
              key={plan.id}
              type="button"
              onClick={() => setSelectedId(plan.id)}
              aria-current={isSelected ? 'true' : undefined}
              className={`w-full rounded-xl border p-3 text-left transition-colors ${isSelected
                ? 'border-indigo-500/70 bg-indigo-500/15 ring-1 ring-inset ring-indigo-400/30 hover:border-indigo-400/80 hover:bg-indigo-500/20'
                : 'border-gray-700 bg-gray-800/60 hover:border-gray-600 hover:bg-gray-800'
              }`}
            >
              <div className="flex items-start gap-2">
                <div className="min-w-0 flex-1">
                  <div className="flex flex-wrap gap-1">
                    <span className={`rounded-full px-2 py-0.5 text-[10px] ${isSelected ? 'bg-indigo-500/20 text-indigo-300 ring-1 ring-inset ring-indigo-500/30' : 'bg-gray-700 text-gray-300'}`}>{planDisplayStateLabel(plan.display_state)}</span>
                    {plan.current_version && <span className="rounded-full bg-indigo-500/15 px-2 py-0.5 text-[10px] text-indigo-300">v{plan.current_version.version_number}</span>}
                    {plan.current_version && selectedVersionIds.includes(plan.current_version.id) && <span className="rounded-full bg-teal-500/15 px-2 py-0.5 text-[10px] text-teal-300">Attached</span>}
                  </div>
                  <div className="mt-2 truncate text-sm font-medium text-gray-100">#{plan.id} {plan.title}</div>
                  <div className={`mt-1 line-clamp-2 text-[11px] leading-4 ${isSelected ? 'text-gray-300' : 'text-gray-500'}`}>{plan.initial_request}</div>
                </div>
                <ChevronRight size={14} className={`mt-1 ${isSelected ? 'text-indigo-300' : 'text-gray-600'}`} />
              </div>
            </button>;
          })}
        </div>
      </section>
      <section className={`${selected ? 'flex' : 'hidden'} min-w-0 flex-1 flex-col sm:flex`}>{selected ? <><button type="button" onClick={() => setSelectedId(null)} className="absolute left-3 top-[calc(env(safe-area-inset-top)+0.75rem)] z-20 flex h-11 w-11 items-center justify-center rounded-lg bg-gray-900/90 text-gray-500 transition-colors hover:bg-gray-800 hover:text-gray-200 sm:hidden" aria-label="Back to Plans"><ChevronLeft size={18} /></button><PlanDetail key={selected.id} plan={selected} onRefresh={refresh} onClose={onClose} selectedVersionIds={selectedVersionIds} onToggleVersion={onToggleVersion} onAttachVersion={onAttachVersion} /></> : <div className="m-auto text-sm text-gray-500">Select or create a Plan</div>}</section>
    </div>
  </div>;
}
