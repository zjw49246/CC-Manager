import { useState, useEffect, useCallback, useRef } from 'react';
import { api } from '../api/client';
import type { Project, GlobalSettings, TagItem, ProjectPreviewProfilesConfig, WorkspacePreviewConfig } from '../api/client';
import { Trash2, RotateCcw, FolderGit2, Globe, HardDrive, Plus, Settings, X, ChevronDown, ChevronUp, GripVertical, Tag, FileKey, Palette, Server, UserPlus } from '../components/icons';
import { TeamShareModal } from '../components/TeamShareModal';
import { resolveTagColor, TAG_COLOR_OPTIONS } from '../components/TagColors';
import { TagManager } from '../components/TagManager';
import { EnvFilesEditor } from '../components/EnvFilesEditor';
import { ProjectTodoList } from '../components/Projects/ProjectTodoList';

// ── Shared: identity warning ──────────────────────────────────────────────────

function IdentityWarning({ name, email }: { name: string; email: string }) {
  const hasName = name.trim() !== '';
  const hasEmail = email.trim() !== '';
  if ((hasName && hasEmail) || (!hasName && !hasEmail)) return null;
  return (
    <p className="col-span-2 text-xs text-amber-400">
      姓名和邮箱必须同时填写才会生效，否则将使用全局配置
    </p>
  );
}

// ── Badge color picker ───────────────────────────────────────────────────────

function BadgeColorPicker({ value, onChange }: { value: string | null; onChange: (color: string | null) => void }) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [open]);

  const current = TAG_COLOR_OPTIONS.find((c) => c.key === value);

  return (
    <div className="relative" ref={ref}>
      <button
        type="button"
        onClick={() => setOpen(!open)}
        className="flex items-center gap-1.5 px-2 py-1 text-xs text-gray-400 hover:text-gray-200 hover:bg-gray-700 rounded transition-colors"
        title="Badge color"
      >
        {current ? (
          <span className={`w-3 h-3 rounded-full ${current.dot}`} />
        ) : (
          <Palette size={14} />
        )}
        <span className="hidden sm:inline">{current ? current.label : 'Color'}</span>
      </button>
      {open && (
        <div className="absolute top-full left-0 mt-1 bg-gray-700 border border-gray-600 rounded-lg shadow-lg z-20 p-2 flex flex-wrap gap-1.5 w-48">
          <button
            onClick={() => { onChange(null); setOpen(false); }}
            className={`w-6 h-6 rounded-full border-2 bg-emerald-600/30 ${!value ? 'border-white' : 'border-transparent hover:border-gray-400'}`}
            title="Default (emerald)"
          />
          {TAG_COLOR_OPTIONS.map((c) => (
            <button
              key={c.key}
              onClick={() => { onChange(c.key); setOpen(false); }}
              className={`w-6 h-6 rounded-full border-2 ${c.dot} ${value === c.key ? 'border-white' : 'border-transparent hover:border-gray-400'}`}
              title={c.label}
            />
          ))}
        </div>
      )}
    </div>
  );
}

// ── Inline tag editor ─────────────────────────────────────────────────────────

function TagEditor({
  tags,
  allTags,
  onSave,
  tagColorMap = {},
}: {
  tags: string[];
  allTags: string[];
  onSave: (tags: string[]) => void;
  tagColorMap?: Record<string, string>;
}) {
  const [input, setInput] = useState('');
  const [showSuggestions, setShowSuggestions] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  const suggestions = allTags.filter(
    (t) => t.toLowerCase().includes(input.toLowerCase()) && !tags.includes(t)
  );

  const addTag = (tag: string) => {
    const trimmed = tag.trim().toLowerCase();
    if (!trimmed || tags.includes(trimmed)) return;
    onSave([...tags, trimmed]);
    setInput('');
    setShowSuggestions(false);
  };

  const removeTag = (tag: string) => {
    onSave(tags.filter((t) => t !== tag));
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter' || e.key === ',') {
      e.preventDefault();
      addTag(input);
    } else if (e.key === 'Escape') {
      setInput('');
      setShowSuggestions(false);
    }
  };

  return (
    <div className="flex items-center gap-1.5 flex-wrap">
      {tags.map((tag) => {
        const c = resolveTagColor(tag, tagColorMap[tag]);
        return (
        <span
          key={tag}
          className={`inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-xs border ${c.bg} ${c.text} ${c.border}`}
        >
          {tag}
          <button
            onClick={() => removeTag(tag)}
            className="opacity-60 hover:opacity-100 leading-none"
          >
            <X size={10} />
          </button>
        </span>
        );
      })}

      <div className="relative">
        <input
          ref={inputRef}
          value={input}
          onChange={(e) => { setInput(e.target.value); setShowSuggestions(true); }}
          onKeyDown={handleKeyDown}
          onFocus={() => setShowSuggestions(true)}
          onBlur={() => setTimeout(() => setShowSuggestions(false), 150)}
          placeholder="+ tag"
          className="w-16 bg-transparent text-xs text-gray-400 placeholder-gray-600 outline-none focus:placeholder-gray-500"
        />
        {showSuggestions && suggestions.length > 0 && (
          <div className="absolute top-full left-0 mt-1 bg-gray-700 border border-gray-600 rounded shadow-lg z-20 min-w-[120px] max-h-48 overflow-y-auto">
            {suggestions.map((s) => (
              <button
                key={s}
                onMouseDown={() => addTag(s)}
                className="w-full text-left px-3 py-1.5 text-xs text-gray-300 hover:bg-gray-600"
              >
                {s}
              </button>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

// ── Global Git Config Modal ───────────────────────────────────────────────────

function GlobalGitConfigModal({ onClose }: { onClose: () => void }) {
  const [form, setForm] = useState<Omit<GlobalSettings, never>>({
    git_author_name: null,
    git_author_email: null,
    git_credential_type: null,
    git_ssh_key_path: null,
    git_https_username: null,
    git_https_token: null,
  });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  const name = form.git_author_name ?? '';
  const email = form.git_author_email ?? '';
  const credType = form.git_credential_type ?? '';

  useEffect(() => {
    api.getGitSettings().then((data) => {
      setForm(data);
      setLoading(false);
    }).catch((e) => {
      setError(String(e));
      setLoading(false);
    });
  }, []);

  const set = (key: keyof GlobalSettings, value: string) =>
    setForm((f) => ({ ...f, [key]: value || null }));

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setSubmitting(true);
    setError(null);
    try {
      await api.updateGitSettings({
        git_author_name: name.trim() || null,
        git_author_email: email.trim() || null,
        git_credential_type: credType || null,
        git_ssh_key_path: form.git_ssh_key_path?.trim() || null,
        git_https_username: form.git_https_username?.trim() || null,
        git_https_token: form.git_https_token?.trim() || null,
      });
      onClose();
    } catch (e) {
      setError(String(e));
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50 p-4">
      <div className="bg-gray-800 rounded-xl shadow-2xl w-full max-w-lg max-h-[90vh] overflow-y-auto">
        <div className="flex items-center justify-between px-5 py-4 border-b border-gray-700">
          <h3 className="text-foreground font-semibold">Global Git Config</h3>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-200"><X size={18} /></button>
        </div>

        <form onSubmit={handleSubmit} className="p-5 space-y-4">
          {error && <p className="text-red-400 text-sm">{error}</p>}
          {loading ? (
            <p className="text-gray-400 text-sm">Loading...</p>
          ) : (
            <>
              <p className="text-xs text-gray-500">项目未配置时使用此全局默认值。</p>

              <div className="grid grid-cols-2 gap-3">
                <div>
                  <label className="block text-xs text-gray-400 mb-1">Author name</label>
                  <input
                    className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
                    value={name} onChange={(e) => set('git_author_name', e.target.value)}
                    placeholder="Zhang San"
                  />
                </div>
                <div>
                  <label className="block text-xs text-gray-400 mb-1">Author email</label>
                  <input
                    className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
                    value={email} onChange={(e) => set('git_author_email', e.target.value)}
                    placeholder="zhang@example.com"
                  />
                </div>
                <IdentityWarning name={name} email={email} />
              </div>

              <div>
                <label className="block text-xs text-gray-400 mb-1">Credential type</label>
                <select
                  className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
                  value={credType} onChange={(e) => set('git_credential_type', e.target.value)}
                >
                  <option value="">None</option>
                  <option value="ssh">SSH key</option>
                  <option value="https">HTTPS token</option>
                </select>
              </div>

              {credType === 'ssh' && (
                <div>
                  <label className="block text-xs text-gray-400 mb-1">SSH private key path</label>
                  <input
                    className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
                    value={form.git_ssh_key_path ?? ''} onChange={(e) => set('git_ssh_key_path', e.target.value)}
                    placeholder="/home/alice/.ssh/id_ed25519"
                  />
                </div>
              )}

              {credType === 'https' && (
                <div className="space-y-2">
                  <div>
                    <label className="block text-xs text-gray-400 mb-1">Username</label>
                    <input
                      className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
                      value={form.git_https_username ?? ''} onChange={(e) => set('git_https_username', e.target.value)}
                      placeholder="github-username"
                    />
                  </div>
                  <div>
                    <label className="block text-xs text-gray-400 mb-1">Personal access token</label>
                    <input
                      type="password"
                      className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
                      value={form.git_https_token ?? ''} onChange={(e) => set('git_https_token', e.target.value)}
                      placeholder="ghp_..."
                    />
                  </div>
                </div>
              )}
            </>
          )}

          <div className="flex justify-end gap-2 pt-1">
            <button type="button" onClick={onClose} className="px-4 py-2 text-sm text-gray-300 hover:text-foreground">Cancel</button>
            <button
              type="submit"
              disabled={submitting || loading}
              className="px-4 py-2 text-sm bg-indigo-600 text-white rounded hover:bg-indigo-500 disabled:opacity-50"
            >
              {submitting ? 'Saving...' : 'Save'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

// ── Create Project Modal ──────────────────────────────────────────────────────

interface CreateForm {
  name: string;
  git_url: string;
  default_branch: string;
  git_author_name: string;
  git_author_email: string;
  git_credential_type: string;  // "" | "ssh" | "https"
  git_ssh_key_path: string;
  git_https_username: string;
  git_https_token: string;
}

const emptyForm = (): CreateForm => ({
  name: '',
  git_url: '',
  default_branch: 'main',
  git_author_name: '',
  git_author_email: '',
  git_credential_type: '',
  git_ssh_key_path: '',
  git_https_username: '',
  git_https_token: '',
});

function CreateModal({ onClose, onCreated }: { onClose: () => void; onCreated: () => void }) {
  const [form, setForm] = useState<CreateForm>(emptyForm());
  const [showGit, setShowGit] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [selectedWorkerId, setSelectedWorkerId] = useState('');
  const [workerList, setWorkerList] = useState<{ id: number; name: string }[]>([]);
  const ccU = JSON.parse(localStorage.getItem('cc_user') || '{}');
  const modalIsAdmin = ccU.role === 'admin' || ccU.role === 'super_admin' || !ccU.id;

  useEffect(() => {
    api.listWorkers().then(w => setWorkerList(w.filter(wk => wk.status !== 'terminated'))).catch(() => {});
  }, []);

  const identityName = form.git_author_name;
  const identityEmail = form.git_author_email;

  const set = (key: keyof CreateForm, value: string) =>
    setForm((f) => ({ ...f, [key]: value }));

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!form.name.trim()) return;
    setSubmitting(true);
    setError(null);
    try {
      await api.createProject({
        name: form.name.trim(),
        worker_id: selectedWorkerId ? Number(selectedWorkerId) : undefined,
        git_url: form.git_url.trim() || undefined,
        default_branch: form.default_branch.trim() || 'main',
        git_author_name: form.git_author_name.trim() || undefined,
        git_author_email: form.git_author_email.trim() || undefined,
        git_credential_type: form.git_credential_type || undefined,
        git_ssh_key_path: form.git_ssh_key_path.trim() || undefined,
        git_https_username: form.git_https_username.trim() || undefined,
        git_https_token: form.git_https_token.trim() || undefined,
      });
      onCreated();
      onClose();
    } catch (e) {
      setError(String(e));
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50 p-4">
      <div className="bg-gray-800 rounded-xl shadow-2xl w-full max-w-lg max-h-[90vh] overflow-y-auto">
        <div className="flex items-center justify-between px-5 py-4 border-b border-gray-700">
          <h3 className="text-foreground font-semibold">New Project</h3>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-200"><X size={18} /></button>
        </div>

        <form onSubmit={handleSubmit} className="p-5 space-y-4">
          {error && <p className="text-red-400 text-sm">{error}</p>}

          {/* Basic */}
          <div className="space-y-3">
            <div>
              <label className="block text-xs text-gray-400 mb-1">Project name *</label>
              <input
                className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
                value={form.name} onChange={(e) => set('name', e.target.value)}
                placeholder="my-project" required
              />
            </div>
            <div>
              <label className="block text-xs text-gray-400 mb-1">Git URL (leave empty for local-only)</label>
              <input
                className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
                value={form.git_url} onChange={(e) => set('git_url', e.target.value)}
                placeholder="https://github.com/org/repo.git"
              />
            </div>
            <div>
              <label className="block text-xs text-gray-400 mb-1">Default branch</label>
              <input
                className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
                value={form.default_branch} onChange={(e) => set('default_branch', e.target.value)}
                placeholder="main"
              />
            </div>
          </div>

          {/* Worker selection */}
          {workerList.length > 0 && (
            <div>
              <label className="block text-xs text-gray-400 mb-1">Run on</label>
              <select
                className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
                value={selectedWorkerId}
                onChange={(e) => setSelectedWorkerId(e.target.value)}
              >
                {modalIsAdmin && <option value="">本机</option>}
                {workerList.map(w => <option key={w.id} value={w.id}>{w.name}</option>)}
              </select>
            </div>
          )}

          {/* Git config (collapsible) */}
          <div className="border border-gray-700 rounded-lg overflow-hidden">
            <button
              type="button"
              className="w-full flex items-center justify-between px-4 py-3 text-sm text-gray-300 hover:bg-gray-700/50"
              onClick={() => setShowGit(!showGit)}
            >
              <span className="flex items-center gap-2"><Settings size={14} /> Git identity &amp; credentials</span>
              {showGit ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
            </button>

            {showGit && (
              <div className="px-4 pb-4 space-y-3 border-t border-gray-700">
                <p className="text-xs text-gray-500 pt-3">Optional. Overrides the machine's global git config for this project.</p>

                <div className="grid grid-cols-2 gap-3">
                  <div>
                    <label className="block text-xs text-gray-400 mb-1">Author name</label>
                    <input
                      className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
                      value={form.git_author_name} onChange={(e) => set('git_author_name', e.target.value)}
                      placeholder="Zhang San"
                    />
                  </div>
                  <div>
                    <label className="block text-xs text-gray-400 mb-1">Author email</label>
                    <input
                      className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
                      value={form.git_author_email} onChange={(e) => set('git_author_email', e.target.value)}
                      placeholder="zhang@example.com"
                    />
                  </div>
                  <IdentityWarning name={identityName} email={identityEmail} />
                </div>

                <div>
                  <label className="block text-xs text-gray-400 mb-1">Credential type</label>
                  <select
                    className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
                    value={form.git_credential_type} onChange={(e) => set('git_credential_type', e.target.value)}
                  >
                    <option value="">System default</option>
                    <option value="ssh">SSH key</option>
                    <option value="https">HTTPS token</option>
                  </select>
                </div>

                {form.git_credential_type === 'ssh' && (
                  <div>
                    <label className="block text-xs text-gray-400 mb-1">SSH private key path</label>
                    <input
                      className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
                      value={form.git_ssh_key_path} onChange={(e) => set('git_ssh_key_path', e.target.value)}
                      placeholder="/home/alice/.ssh/id_ed25519_work"
                    />
                  </div>
                )}

                {form.git_credential_type === 'https' && (
                  <div className="space-y-2">
                    <div>
                      <label className="block text-xs text-gray-400 mb-1">Username</label>
                      <input
                        className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
                        value={form.git_https_username} onChange={(e) => set('git_https_username', e.target.value)}
                        placeholder="github-username"
                      />
                    </div>
                    <div>
                      <label className="block text-xs text-gray-400 mb-1">Personal access token</label>
                      <input
                        type="password"
                        className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
                        value={form.git_https_token} onChange={(e) => set('git_https_token', e.target.value)}
                        placeholder="ghp_..."
                      />
                    </div>
                  </div>
                )}
              </div>
            )}
          </div>

          <div className="flex justify-end gap-2 pt-1">
            <button type="button" onClick={onClose} className="px-4 py-2 text-sm text-gray-300 hover:text-foreground">Cancel</button>
            <button
              type="submit"
              disabled={submitting || !form.name.trim()}
              className="px-4 py-2 text-sm bg-indigo-600 text-white rounded hover:bg-indigo-500 disabled:opacity-50"
            >
              {submitting ? 'Creating...' : 'Create'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

// ── Git Config Edit Modal ─────────────────────────────────────────────────────

function GitConfigModal({ project, onClose, onSaved }: { project: Project; onClose: () => void; onSaved: () => void }) {
  const [form, setForm] = useState({
    git_author_name: project.git_author_name ?? '',
    git_author_email: project.git_author_email ?? '',
    git_credential_type: project.git_credential_type ?? '',
    git_ssh_key_path: project.git_ssh_key_path ?? '',
    git_https_username: project.git_https_username ?? '',
    git_https_token: project.git_https_token ?? '',
  });
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  const set = (key: keyof typeof form, value: string) =>
    setForm((f) => ({ ...f, [key]: value }));

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setSubmitting(true);
    setError(null);
    try {
      await api.updateProject(project.id, {
        git_author_name: form.git_author_name.trim() || undefined,
        git_author_email: form.git_author_email.trim() || undefined,
        git_credential_type: form.git_credential_type || undefined,
        git_ssh_key_path: form.git_ssh_key_path.trim() || undefined,
        git_https_username: form.git_https_username.trim() || undefined,
        git_https_token: form.git_https_token.trim() || undefined,
      });
      onSaved();
      onClose();
    } catch (e) {
      setError(String(e));
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50 p-4">
      <div className="bg-gray-800 rounded-xl shadow-2xl w-full max-w-lg max-h-[90vh] overflow-y-auto">
        <div className="flex items-center justify-between px-5 py-4 border-b border-gray-700">
          <h3 className="text-foreground font-semibold">Git config — {project.name}</h3>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-200"><X size={18} /></button>
        </div>

        <form onSubmit={handleSubmit} className="p-5 space-y-4">
          {error && <p className="text-red-400 text-sm">{error}</p>}
          <p className="text-xs text-gray-500">Overrides the machine's global git config for this project only. Leave blank to use system default.</p>

          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="block text-xs text-gray-400 mb-1">Author name</label>
              <input
                className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
                value={form.git_author_name} onChange={(e) => set('git_author_name', e.target.value)}
                placeholder="Zhang San"
              />
            </div>
            <div>
              <label className="block text-xs text-gray-400 mb-1">Author email</label>
              <input
                className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
                value={form.git_author_email} onChange={(e) => set('git_author_email', e.target.value)}
                placeholder="zhang@example.com"
              />
            </div>
            <IdentityWarning name={form.git_author_name} email={form.git_author_email} />
          </div>

          <div>
            <label className="block text-xs text-gray-400 mb-1">Credential type</label>
            <select
              className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
              value={form.git_credential_type} onChange={(e) => set('git_credential_type', e.target.value)}
            >
              <option value="">System default</option>
              <option value="ssh">SSH key</option>
              <option value="https">HTTPS token</option>
            </select>
          </div>

          {form.git_credential_type === 'ssh' && (
            <div>
              <label className="block text-xs text-gray-400 mb-1">SSH private key path</label>
              <input
                className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
                value={form.git_ssh_key_path} onChange={(e) => set('git_ssh_key_path', e.target.value)}
                placeholder="/home/alice/.ssh/id_ed25519_work"
              />
            </div>
          )}

          {form.git_credential_type === 'https' && (
            <div className="space-y-2">
              <div>
                <label className="block text-xs text-gray-400 mb-1">Username</label>
                <input
                  className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
                  value={form.git_https_username} onChange={(e) => set('git_https_username', e.target.value)}
                  placeholder="github-username"
                />
              </div>
              <div>
                <label className="block text-xs text-gray-400 mb-1">Personal access token</label>
                <input
                  type="password"
                  className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
                  value={form.git_https_token} onChange={(e) => set('git_https_token', e.target.value)}
                  placeholder="ghp_..."
                />
              </div>
            </div>
          )}

          <div className="flex justify-end gap-2 pt-1">
            <button type="button" onClick={onClose} className="px-4 py-2 text-sm text-gray-300 hover:text-foreground">Cancel</button>
            <button
              type="submit"
              disabled={submitting}
              className="px-4 py-2 text-sm bg-indigo-600 text-white rounded hover:bg-indigo-500 disabled:opacity-50"
            >
              {submitting ? 'Saving...' : 'Save'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

// ── Trusted Preview Profiles Modal ───────────────────────────────────────────

function previewProfilesForProject(project: Project): ProjectPreviewProfilesConfig {
  const existing = project.preview_config;
  if (existing?.version === 2) return existing;
  if (existing?.version === 1) {
    return {
      version: 2,
      default_profile: 'default',
      profiles: [{ ...existing, id: 'default', match_paths: ['**'], enabled: true }],
    };
  }
  const starter: WorkspacePreviewConfig = {
    version: 1,
    name: 'Web preview',
    setup: [],
    processes: [{
      name: 'web',
      command: ['npm', 'run', 'dev', '--', '--host', '127.0.0.1', '--port', '{preview_port}'],
      cwd: '.',
      env: {},
    }],
    url: 'http://127.0.0.1:{preview_port}/',
    health_url: 'http://127.0.0.1:{preview_port}/',
    startup_timeout_seconds: 90,
    sandbox: null,
  };
  return {
    version: 2,
    default_profile: 'web',
    profiles: [{ ...starter, id: 'web', match_paths: ['web/**'], enabled: true }],
  };
}

function PreviewProfilesModal({ project, onClose, onSaved }: { project: Project; onClose: () => void; onSaved: () => void }) {
  const initial = previewProfilesForProject(project);
  const [text, setText] = useState(() => JSON.stringify(initial, null, 2));
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  let parsed: ProjectPreviewProfilesConfig | null = null;
  try {
    const candidate = JSON.parse(text) as ProjectPreviewProfilesConfig;
    if (candidate?.version === 2 && Array.isArray(candidate.profiles)) parsed = candidate;
  } catch {
    parsed = null;
  }

  const save = async () => {
    if (!parsed) {
      setError('Preview Profiles JSON is invalid.');
      return;
    }
    setSubmitting(true);
    setError(null);
    try {
      await api.updateProject(project.id, { preview_config: parsed });
      onSaved();
      onClose();
    } catch (reason) {
      setError(String(reason));
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50 p-4">
      <div className="bg-gray-800 rounded-xl shadow-2xl w-full max-w-4xl max-h-[92vh] overflow-y-auto">
        <div className="flex items-center justify-between px-5 py-4 border-b border-gray-700">
          <div>
            <h3 className="text-foreground font-semibold">Trusted Preview Profiles — {project.name}</h3>
            <p className="text-xs text-gray-400 mt-1">Delivery matches the final changed paths and runs every matching enabled profile.</p>
          </div>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-200"><X size={18} /></button>
        </div>
        <div className="p-5 space-y-4">
          {parsed && (
            <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
              {parsed.profiles.map((profile) => (
                <div key={profile.id} className="rounded-lg border border-gray-700 bg-gray-900/40 p-3">
                  <div className="flex items-center justify-between gap-2">
                    <span className="font-medium text-sm text-foreground">{profile.name}</span>
                    <span className={`text-[11px] ${profile.enabled ? 'text-emerald-400' : 'text-gray-500'}`}>{profile.enabled ? 'Enabled' : 'Disabled'}</span>
                  </div>
                  <div className="text-xs text-gray-400 mt-1">ID: {profile.id}{parsed.default_profile === profile.id ? ' · default' : ''}</div>
                  <div className="text-xs text-sky-400 mt-2 break-all">{profile.match_paths.join(', ')}</div>
                </div>
              ))}
            </div>
          )}
          <div className="rounded-lg border border-amber-700/50 bg-amber-950/20 px-4 py-3 text-xs text-amber-200">
            These commands are an administrator-owned trust boundary. Delivery Agents may select profiles, but cannot replace their commands from PR code.
          </div>
          {error && <p className="text-red-400 text-sm">{error}</p>}
          <textarea
            value={text}
            onChange={(event) => setText(event.target.value)}
            spellCheck={false}
            className="w-full min-h-[420px] font-mono text-xs bg-gray-950 text-gray-200 rounded-lg border border-gray-700 px-4 py-3 outline-none focus:ring-1 focus:ring-indigo-500"
          />
          <div className="flex justify-end gap-2">
            <button type="button" onClick={onClose} className="px-4 py-2 text-sm text-gray-300 hover:text-foreground">Cancel</button>
            <button onClick={save} disabled={submitting || !parsed} className="px-4 py-2 text-sm bg-indigo-600 text-white rounded hover:bg-indigo-500 disabled:opacity-50">
              {submitting ? 'Saving...' : 'Save trusted profiles'}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

// ── Main Page ─────────────────────────────────────────────────────────────────

export function ProjectsPage() {
  const [projects, setProjects] = useState<Project[]>([]);
  const [allTags, setAllTags] = useState<string[]>([]);
  const [tagFilter, setTagFilter] = useState<string>('');
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState<Record<number, boolean>>({});
  const [showCreate, setShowCreate] = useState(false);
  const [editingGit, setEditingGit] = useState<Project | null>(null);
  const [editingPreviewProfiles, setEditingPreviewProfiles] = useState<Project | null>(null);
  const [editingEnvFiles, setEditingEnvFiles] = useState<Project | null>(null);
  const [teamSharingProject, setTeamSharingProject] = useState<Project | null>(null);
  const ccUser = JSON.parse(localStorage.getItem('cc_user') || '{}');
  const isAdmin = ccUser.role === 'admin' || ccUser.role === 'super_admin' || !ccUser.id;
  const [showGlobalGit, setShowGlobalGit] = useState(false);
  const [showTagManager, setShowTagManager] = useState(false);
  const [tagItems, setTagItems] = useState<TagItem[]>([]);

  // Worker task counts per project
  const [workerTaskCounts, setWorkerTaskCounts] = useState<Record<number, number>>({});
  useEffect(() => {
    api.listTasks(undefined, undefined, undefined, undefined, 1000).then((tasks) => {
      const counts: Record<number, number> = {};
      for (const t of tasks) {
        if (t.worker_id && t.project_id) {
          counts[t.project_id] = (counts[t.project_id] || 0) + 1;
        }
      }
      setWorkerTaskCounts(counts);
    }).catch(() => {});
  }, []);

  // Drag state
  const [draggingId, setDraggingId] = useState<number | null>(null);
  const [dragOverId, setDragOverId] = useState<number | null>(null);

  // Touch drag state
  const touchDragId = useRef<number | null>(null);
  const touchOverId = useRef<number | null>(null);
  const touchStartY = useRef<number>(0);
  const touchMoved = useRef(false);
  const cardRefs = useRef<Map<number, HTMLDivElement>>(new Map());

  const refresh = useCallback(async () => {
    try {
      const [list, tags, tagList] = await Promise.all([api.listProjects(), api.listProjectTags(), api.listTags()]);
      setProjects(list);
      // Merge tags from both sources: tags table (authoritative registry) + project assignments (backward compat)
      const merged = new Set([...tags, ...tagList.map((t) => t.name)]);
      setAllTags(Array.from(merged).sort());
      setTagItems(tagList);
      setError(null);
    } catch (e) {
      setError(String(e));
    }
  }, []);

  useEffect(() => {
    refresh();
    const interval = setInterval(refresh, 5000);
    return () => clearInterval(interval);
  }, [refresh]);

  const toggleSelector = async (project: Project) => {
    setLoading((prev) => ({ ...prev, [project.id]: true }));
    try {
      await api.updateProject(project.id, { show_in_selector: !project.show_in_selector });
      await refresh();
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading((prev) => ({ ...prev, [project.id]: false }));
    }
  };

  const handleDelete = async (id: number) => {
    if (!confirm('Delete this project?')) return;
    try {
      await api.deleteProject(id);
      await refresh();
    } catch (e) {
      setError(String(e));
    }
  };

  const handleBadgeColor = async (project: Project, color: string | null) => {
    try {
      await api.updateProject(project.id, { badge_color: color });
      setProjects((prev) => prev.map((p) => p.id === project.id ? { ...p, badge_color: color } : p));
    } catch (e) {
      setError(String(e));
    }
  };

  const handleTagSave = async (project: Project, tags: string[]) => {
    try {
      await api.updateProject(project.id, { tags });
      setProjects((prev) => prev.map((p) => p.id === project.id ? { ...p, tags } : p));
      // Refresh allTags
      const newAllTags = new Set(allTags);
      tags.forEach((t) => newAllTags.add(t));
      setAllTags(Array.from(newAllTags).sort());
    } catch (e) {
      setError(String(e));
    }
  };

  // ── Drag handlers ──
  const handleDragStart = (id: number) => (e: React.DragEvent) => {
    setDraggingId(id);
    e.dataTransfer.effectAllowed = 'move';
  };

  const handleDragOver = (id: number) => (e: React.DragEvent) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = 'move';
    if (id !== draggingId) setDragOverId(id);
  };

  const handleDrop = (targetId: number) => async (e: React.DragEvent) => {
    e.preventDefault();
    if (draggingId === null || draggingId === targetId) {
      setDraggingId(null);
      setDragOverId(null);
      return;
    }

    // Reorder locally
    const reordered = [...projects];
    const fromIdx = reordered.findIndex((p) => p.id === draggingId);
    const toIdx = reordered.findIndex((p) => p.id === targetId);
    const [moved] = reordered.splice(fromIdx, 1);
    reordered.splice(toIdx, 0, moved);

    // Assign new sort_order values (0, 1, 2, ...)
    const updated = reordered.map((p, i) => ({ ...p, sort_order: i }));
    setProjects(updated);
    setDraggingId(null);
    setDragOverId(null);

    try {
      await api.reorderProjects(updated.map((p) => ({ id: p.id, sort_order: p.sort_order })));
    } catch (e) {
      setError(String(e));
      await refresh(); // revert on error
    }
  };

  const handleDragEnd = () => {
    setDraggingId(null);
    setDragOverId(null);
  };

  // ── Touch drag handlers (mobile) ──
  const performReorder = useCallback(async (sourceId: number, targetId: number) => {
    if (sourceId === targetId) return;
    const reordered = [...projects];
    const fromIdx = reordered.findIndex((p) => p.id === sourceId);
    const toIdx = reordered.findIndex((p) => p.id === targetId);
    if (fromIdx === -1 || toIdx === -1) return;
    const [moved] = reordered.splice(fromIdx, 1);
    reordered.splice(toIdx, 0, moved);
    const updated = reordered.map((p, i) => ({ ...p, sort_order: i }));
    setProjects(updated);
    try {
      await api.reorderProjects(updated.map((p) => ({ id: p.id, sort_order: p.sort_order })));
    } catch (e) {
      setError(String(e));
      await refresh();
    }
  }, [projects, refresh]);

  const handleTouchStart = (id: number) => (e: React.TouchEvent) => {
    touchDragId.current = id;
    touchStartY.current = e.touches[0].clientY;
    touchMoved.current = false;
  };

  const handleTouchMove = useCallback((e: React.TouchEvent) => {
    if (touchDragId.current === null) return;
    const dy = Math.abs(e.touches[0].clientY - touchStartY.current);
    if (dy > 8) {
      touchMoved.current = true;
      e.preventDefault();
    }
    if (!touchMoved.current) return;

    setDraggingId(touchDragId.current);

    const touch = e.touches[0];
    let foundId: number | null = null;
    for (const [pid, el] of cardRefs.current) {
      const rect = el.getBoundingClientRect();
      if (touch.clientY >= rect.top && touch.clientY <= rect.bottom) {
        foundId = pid;
        break;
      }
    }
    const overId = foundId !== touchDragId.current ? foundId : null;
    touchOverId.current = overId;
    setDragOverId(overId);
  }, []);

  const handleTouchEnd = useCallback(() => {
    const sourceId = touchDragId.current;
    const targetId = touchOverId.current;
    touchDragId.current = null;
    touchOverId.current = null;
    setDraggingId(null);
    setDragOverId(null);
    if (!touchMoved.current) return;
    if (sourceId !== null && targetId !== null) {
      performReorder(sourceId, targetId);
    }
  }, [performReorder]);

  const setCardRef = useCallback((id: number) => (el: HTMLDivElement | null) => {
    if (el) cardRefs.current.set(id, el);
    else cardRefs.current.delete(id);
  }, []);

  const statusColor: Record<string, string> = {
    ready: 'bg-green-500',
    pending: 'bg-yellow-500',
    cloning: 'bg-blue-500 animate-pulse',
    initializing: 'bg-blue-500 animate-pulse',
    error: 'bg-red-500',
  };

  // Build tag name → color key map from stored tags
  const tagColorMap: Record<string, string> = {};
  for (const t of tagItems) tagColorMap[t.name] = t.color;

  const filteredProjects = tagFilter
    ? projects.filter((p) => p.tags.includes(tagFilter))
    : projects;

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-foreground font-semibold text-lg">Projects</h2>
        <div className="flex items-center gap-2">
          {isAdmin && (
            <button
              onClick={() => setShowGlobalGit(true)}
              className="flex items-center gap-1.5 px-3 py-1.5 text-sm text-gray-300 border border-gray-600 rounded hover:bg-gray-700"
              title="Global Git Config"
            >
              <Settings size={14} /><span className="hidden sm:inline"> Global Git</span> Config
            </button>
          )}
          {isAdmin && (
            <button
              onClick={() => setShowTagManager(true)}
              className="flex items-center gap-1.5 px-3 py-1.5 text-sm text-gray-300 border border-gray-600 rounded hover:bg-gray-700"
              title="Manage Tags"
            >
              <Tag size={14} /> Tags
            </button>
          )}
          {isAdmin && (
            <button
              onClick={() => setShowCreate(true)}
              className="flex items-center gap-1.5 px-3 py-1.5 text-sm bg-indigo-600 text-white rounded hover:bg-indigo-500"
            >
              <Plus size={14} /><span className="hidden sm:inline"> New project</span><span className="sm:hidden"> New</span>
            </button>
          )}
        </div>
      </div>

      {/* Tag filter */}
      {allTags.length > 0 && (
        <div className="flex items-center gap-2 flex-wrap">
          <span className="flex items-center gap-1 text-xs text-gray-500">
            <Tag size={12} /> Tags:
          </span>
          <button
            onClick={() => setTagFilter('')}
            className={`px-2 py-0.5 rounded text-xs transition-colors border ${
              tagFilter === ''
                ? 'bg-indigo-600 text-white border-indigo-500'
                : 'bg-gray-800 text-gray-400 border-gray-700 hover:bg-gray-700'
            }`}
          >
            All
          </button>
          {allTags.map((tag) => {
            const c = resolveTagColor(tag, tagColorMap[tag]);
            return (
              <button
                key={tag}
                onClick={() => setTagFilter(tagFilter === tag ? '' : tag)}
                className={`px-2 py-0.5 rounded text-xs transition-colors border ${c.bg} ${c.text} ${c.border} ${
                  tagFilter === tag ? 'opacity-100' : 'opacity-60 hover:opacity-100'
                }`}
              >
                {tag}
              </button>
            );
          })}
        </div>
      )}

      {error && (
        <div className="bg-red-500/20 text-red-400 px-4 py-2 rounded text-sm">
          Error: {error}
        </div>
      )}

      {filteredProjects.length === 0 ? (
        <p className="text-gray-400 text-sm">{projects.length === 0 ? 'No projects yet.' : 'No projects match this tag.'}</p>
      ) : (() => {
        const groups = new Map<string, typeof filteredProjects>();
        for (const p of filteredProjects) {
          const loc = p.location || 'local';
          if (!groups.has(loc)) groups.set(loc, []);
          groups.get(loc)!.push(p);
        }
        const sortedKeys = [...groups.keys()].sort((a, b) => a === 'local' ? -1 : b === 'local' ? 1 : a.localeCompare(b));
        return (
          <div className="space-y-4">
            {sortedKeys.map(loc => (
              <div key={loc}>
                {groups.size > 1 && (
                  <h3 className="text-xs text-gray-500 uppercase tracking-wider mb-2 flex items-center gap-1.5">
                    {loc === 'local' ? '📍 本机' : `🖥 ${loc}`}
                  </h3>
                )}
                <div className="space-y-3">
                  {groups.get(loc)!.map((p) => (
            <div
              key={p.id}
              ref={setCardRef(p.id)}
              draggable={isAdmin}
              onDragStart={isAdmin ? handleDragStart(p.id) : undefined}
              onDragOver={isAdmin ? handleDragOver(p.id) : undefined}
              onDrop={isAdmin ? handleDrop(p.id) : undefined}
              onDragEnd={isAdmin ? handleDragEnd : undefined}
              className={`bg-gray-800 rounded-lg p-3 sm:p-4 space-y-2 transition-opacity ${
                draggingId === p.id ? 'opacity-40' : 'opacity-100'
              } ${
                dragOverId === p.id && draggingId !== p.id
                  ? 'border-2 border-dashed border-indigo-500'
                  : 'border-2 border-transparent'
              }`}
            >
              <div className="flex items-start gap-3">
                {/* Drag handle */}
                {isAdmin && <div
                  className="mt-1 text-gray-600 hover:text-gray-400 cursor-grab active:cursor-grabbing select-none touch-none"
                  title="Drag to reorder"
                  onTouchStart={handleTouchStart(p.id)}
                  onTouchMove={handleTouchMove}
                  onTouchEnd={handleTouchEnd}
                >
                  <GripVertical size={18} />
                </div>}

                {/* Icon */}
                <div className="mt-1 text-gray-400">
                  <FolderGit2 size={20} />
                </div>

                {/* Info */}
                <div className="flex-1 min-w-0 space-y-1">
                  <div className="flex items-center gap-2 flex-wrap">
                    <span className="text-foreground font-medium">{p.name}</span>
                    {isAdmin && <BadgeColorPicker value={p.badge_color} onChange={(c) => handleBadgeColor(p, c)} />}
                    <span className={`inline-block w-2 h-2 rounded-full ${statusColor[p.status] || 'bg-gray-500'}`} title={p.status} />
                    <span className="text-xs text-gray-500 capitalize">{p.status}</span>
                    {p.has_remote ? (
                      <span className="flex items-center gap-1 text-xs text-sky-400">
                        <Globe size={12} /> Remote
                      </span>
                    ) : (
                      <span className="flex items-center gap-1 text-xs text-gray-500">
                        <HardDrive size={12} /> Local
                      </span>
                    )}
                    {(workerTaskCounts[p.id] || 0) > 0 && (
                      <span className="flex items-center gap-1 text-xs text-indigo-400" title="在 Worker 上运行的任务数">
                        <Server size={12} /> Worker ×{workerTaskCounts[p.id]}
                      </span>
                    )}
                  </div>

                  {p.git_url && (
                    <p className="text-xs text-gray-500 truncate" title={p.git_url}>{p.git_url}</p>
                  )}
                  {p.local_path && (
                    <p className="text-xs text-gray-500 truncate" title={p.local_path}>{p.local_path}</p>
                  )}
                  {p.error_message && (
                    <p className="text-xs text-red-400 truncate" title={p.error_message}>{p.error_message}</p>
                  )}

                  <div className="flex items-center gap-4 text-xs text-gray-500">
                    <span>Branch: {p.default_branch}</span>
                    {p.git_author_name && <span>Author: {p.git_author_name}</span>}
                    {p.git_credential_type && <span>Creds: {p.git_credential_type}</span>}
                    <span>Created: {new Date(p.created_at).toLocaleDateString()}</span>
                  </div>

                  {/* Tags row */}
                  {isAdmin ? (
                    <div className="pt-0.5">
                      <TagEditor
                        tags={p.tags}
                        allTags={allTags}
                        onSave={(tags) => handleTagSave(p, tags)}
                        tagColorMap={tagColorMap}
                      />
                    </div>
                  ) : p.tags.length > 0 ? (
                    <div className="flex flex-wrap gap-1.5 pt-0.5">
                      {p.tags.map((tag) => {
                        const color = resolveTagColor(tag, tagColorMap[tag]);
                        return <span key={tag} className={`rounded border px-1.5 py-0.5 text-xs ${color.bg} ${color.text} ${color.border}`}>{tag}</span>;
                      })}
                    </div>
                  ) : null}

                  <ProjectTodoList projectId={p.id} project={p} />
                </div>
              </div>

              {/* Actions */}
              <div className="flex items-center gap-3 pl-10 sm:pl-0 sm:justify-end">
                {/* Show in selector toggle */}
                {isAdmin && <label className="flex items-center gap-2 cursor-pointer select-none" title="Show in task project dropdown">
                  <span className="text-xs text-gray-400">Selector</span>
                  <button
                    onClick={() => toggleSelector(p)}
                    disabled={loading[p.id]}
                    className={`relative w-11 h-6 rounded-full transition-colors ${
                      p.show_in_selector ? 'bg-indigo-600' : 'bg-gray-600'
                    } ${loading[p.id] ? 'opacity-50' : ''}`}
                  >
                    <span
                      className={`absolute top-0.5 left-0.5 w-5 h-5 bg-white rounded-full transition-transform ${
                        p.show_in_selector ? 'translate-x-5' : ''
                      }`}
                    />
                  </button>
                </label>}

                {/* Project configuration and sharing are administrator-only. */}
                {isAdmin && (
                  <>
                    <button
                      onClick={() => setEditingPreviewProfiles(p)}
                      className="p-2 text-gray-400 hover:text-cyan-400 hover:bg-gray-700 rounded transition-colors"
                      title="Manage trusted Preview profiles"
                    >
                      <Globe size={16} />
                    </button>
                    <button
                      onClick={() => setEditingGit(p)}
                      className="p-2 text-gray-400 hover:text-indigo-400 hover:bg-gray-700 rounded transition-colors"
                      title="Edit git config"
                    >
                      <Settings size={16} />
                    </button>
                    <button
                      onClick={() => setEditingEnvFiles(p)}
                      className="p-2 text-gray-400 hover:text-green-400 hover:bg-gray-700 rounded transition-colors"
                      title="Manage env files"
                    >
                      <FileKey size={16} />
                    </button>
                    {p.has_remote && p.worker_id == null && (
                      <button
                        onClick={async () => {
                          try {
                            await api.recloneProject(p.id);
                            await refresh();
                          } catch (e) {
                            setError(String(e));
                          }
                        }}
                        className="min-w-[44px] min-h-[44px] flex items-center justify-center text-gray-400 hover:text-sky-400 hover:bg-gray-700 rounded transition-colors"
                        title="Re-clone"
                      >
                        <RotateCcw size={16} />
                      </button>
                    )}
                    <button
                      onClick={() => setTeamSharingProject(p)}
                      className="min-w-[44px] min-h-[44px] flex items-center justify-center text-gray-400 hover:text-indigo-400 hover:bg-gray-700 rounded transition-colors"
                      title="Share to team members"
                    >
                      <UserPlus size={16} />
                    </button>
                    <button
                      onClick={() => handleDelete(p.id)}
                      className="min-w-[44px] min-h-[44px] flex items-center justify-center text-gray-400 hover:text-red-400 hover:bg-gray-700 rounded transition-colors"
                      title="Delete project"
                    >
                      <Trash2 size={16} />
                    </button>
                  </>
                )}
              </div>
            </div>
                  ))}
                </div>
              </div>
            ))}
          </div>
        );
      })()}

      {showCreate && <CreateModal onClose={() => setShowCreate(false)} onCreated={refresh} />}
      {editingGit && <GitConfigModal project={editingGit} onClose={() => setEditingGit(null)} onSaved={refresh} />}
      {editingPreviewProfiles && (
        <PreviewProfilesModal
          project={editingPreviewProfiles}
          onClose={() => setEditingPreviewProfiles(null)}
          onSaved={refresh}
        />
      )}
      {editingEnvFiles && (
        <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50 p-4">
          <div className="bg-gray-800 rounded-xl shadow-2xl w-full max-w-xl max-h-[85vh] flex flex-col">
            <div className="flex items-center justify-between px-5 py-4 border-b border-gray-700 flex-shrink-0">
              <h3 className="text-foreground font-semibold">Env files — {editingEnvFiles.name}</h3>
              <button onClick={() => setEditingEnvFiles(null)} className="text-gray-400 hover:text-gray-200">
                <X size={18} />
              </button>
            </div>
            <div className="flex-1 overflow-y-auto p-5">
              <EnvFilesEditor
                project={editingEnvFiles}
                onProjectUpdated={(updated) => {
                  setEditingEnvFiles(updated);
                  setProjects((prev) => prev.map((p) => p.id === updated.id ? updated : p));
                }}
              />
            </div>
          </div>
        </div>
      )}
      {showGlobalGit && <GlobalGitConfigModal onClose={() => setShowGlobalGit(false)} />}
      {showTagManager && <TagManager onClose={() => setShowTagManager(false)} onChanged={refresh} />}

      {teamSharingProject && (
        <TeamShareModal
          type="project"
          itemId={teamSharingProject.id}
          itemTitle={teamSharingProject.name}
          onClose={() => setTeamSharingProject(null)}
        />
      )}
    </div>
  );
}
