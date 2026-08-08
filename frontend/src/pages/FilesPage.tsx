import React, { useState, useEffect, useRef, useCallback } from 'react';
import {
  ChevronRight, ChevronDown, Folder, FolderOpen, FileText,
  AlertCircle, Loader2, Plus, Trash2, Server, HardDrive, Download, Upload,
  GitBranch, RefreshCw,
} from '../components/icons';
import { api, getToken } from '../api/client';
import type {
  Project,
  SSHProfile as ManagedSSHProfile,
  SSHProfileInput,
  TaskSSHCapability,
} from '../api/client';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface DirEntry {
  name: string;
  path: string;
  is_dir: boolean;
  size: number | null;
}

interface LegacySSHProfile {
  id: string;
  label: string;
  host: string;
  port: number;
  username: string;
  password: string;
  key_path: string;
}

type SSHConnection =
  | { kind: 'managed'; profile: ManagedSSHProfile }
  | { kind: 'legacy'; profile: LegacySSHProfile };

type Mode = 'local' | 'ssh' | 'git';

interface GitFileEntry {
  path: string;
  status: string;
  x: string;
  y: string;
}

const SSH_PROFILES_KEY = 'cc_ssh_profiles';

// ---------------------------------------------------------------------------
// SSH profile storage helpers
// ---------------------------------------------------------------------------

function loadProfiles(): LegacySSHProfile[] {
  try {
    return JSON.parse(localStorage.getItem(SSH_PROFILES_KEY) || '[]');
  } catch {
    return [];
  }
}

function saveProfiles(profiles: LegacySSHProfile[]) {
  localStorage.setItem(SSH_PROFILES_KEY, JSON.stringify(profiles));
}

// ---------------------------------------------------------------------------
// Auto-inject Worker SSH profiles
// ---------------------------------------------------------------------------

function useWorkerProfiles(): LegacySSHProfile[] {
  const [wps, setWps] = useState<LegacySSHProfile[]>([]);
  useEffect(() => {
    api.listWorkers()
      .then((workers) => {
        setWps(
          workers
            .filter((w) => w.status === 'ready' && w.private_ip)
            .map((w) => ({
              id: `worker-${w.id}`,
              label: w.name,
              host: w.private_ip!,
              port: 22,
              username: w.ssh_user || 'ubuntu',
              password: '',
              key_path: w.ssh_key_path || '',
            })),
        );
      })
      .catch(() => {});
  }, []);
  return wps;
}

// ---------------------------------------------------------------------------
// Shared file tree node (works for both local and SSH)
// ---------------------------------------------------------------------------

interface TreeNodeProps {
  entry: DirEntry;
  selectedPath: string | null;
  onSelect: (path: string, isDir: boolean) => void;
  fetchChildren: (path: string) => Promise<DirEntry[]>;
}

function TreeNode({ entry, selectedPath, onSelect, fetchChildren }: TreeNodeProps) {
  const [open, setOpen] = useState(false);
  const [children, setChildren] = useState<DirEntry[] | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleClick = async () => {
    if (!entry.is_dir) {
      onSelect(entry.path, false);
      return;
    }
    if (!open && children === null) {
      setLoading(true);
      setError(null);
      try {
        const result = await fetchChildren(entry.path);
        setChildren(result);
      } catch (e) {
        setError((e as Error).message);
      } finally {
        setLoading(false);
      }
    }
    setOpen((v) => !v);
    onSelect(entry.path, true);
  };

  const isSelected = selectedPath === entry.path;

  return (
    <div>
      <div
        onClick={handleClick}
        className={`flex items-center gap-1 px-2 py-0.5 rounded cursor-pointer text-sm select-none hover:bg-gray-700 ${
          isSelected ? 'bg-gray-700 text-indigo-400' : 'text-gray-300'
        }`}
      >
        <span className="w-4 flex-shrink-0 text-gray-500">
          {entry.is_dir ? (
            loading ? (
              <Loader2 size={14} className="animate-spin" />
            ) : open ? (
              <ChevronDown size={14} />
            ) : (
              <ChevronRight size={14} />
            )
          ) : null}
        </span>
        {entry.is_dir
          ? open ? <FolderOpen size={14} className="text-yellow-400 flex-shrink-0" /> : <Folder size={14} className="text-yellow-400 flex-shrink-0" />
          : <FileText size={14} className="text-gray-400 flex-shrink-0" />}
        <span className="truncate">{entry.name}</span>
        {entry.size !== null && (
          <span className="ml-auto text-xs text-gray-600 flex-shrink-0">{formatSize(entry.size)}</span>
        )}
      </div>
      {error && <div className="ml-8 text-xs text-red-400 py-0.5">{error}</div>}
      {open && children && (
        <div className="ml-4 border-l border-gray-700">
          {children.length === 0 && <div className="ml-4 text-xs text-gray-600 py-0.5">empty</div>}
          {children.map((child) => (
            <TreeNode key={child.path} entry={child} selectedPath={selectedPath} onSelect={onSelect} fetchChildren={fetchChildren} />
          ))}
        </div>
      )}
    </div>
  );
}

function formatSize(bytes: number): string {
  if (bytes < 1024) return `${bytes}B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)}K`;
  return `${(bytes / 1024 / 1024).toFixed(1)}M`;
}

interface ManagedSSHPanelProps {
  profiles: ManagedSSHProfile[];
  legacyProfiles: LegacySSHProfile[];
  activeId: number | null;
  activeLegacyId: string | null;
  onActivate: (profile: ManagedSSHProfile) => void;
  onActivateLegacy: (profile: LegacySSHProfile) => void;
  onRefresh: (preferredId?: number) => Promise<void>;
  onLegacyMigrated: (legacyId: string) => void;
  onDeleteLegacy: (legacyId: string) => void;
  isAdmin: boolean;
}

interface ManagedProfileDraft {
  id: number | null;
  legacyId: string | null;
  name: string;
  host: string;
  port: number;
  username: string;
  keyPath: string;
  keyUploadToken: string;
  keyUploadFilename: string;
  keyUploadFingerprint: string;
  hostKeyValue: string;
  hostKeyFingerprint: string;
  hostKeyConfirmed: boolean;
  enabled: boolean;
  allowedRootsText: string;
  taskAccessEnabled: boolean;
  taskCapabilities: TaskSSHCapability[];
}

function emptyManagedDraft(): ManagedProfileDraft {
  return {
    id: null,
    legacyId: null,
    name: '',
    host: '',
    port: 22,
    username: '',
    keyPath: '',
    keyUploadToken: '',
    keyUploadFilename: '',
    keyUploadFingerprint: '',
    hostKeyValue: '',
    hostKeyFingerprint: '',
    hostKeyConfirmed: false,
    enabled: true,
    allowedRootsText: '/',
    taskAccessEnabled: false,
    taskCapabilities: [],
  };
}

const TASK_CAPABILITY_OPTIONS: Array<{
  key: TaskSSHCapability;
  label: string;
  detail: string;
}> = [
  { key: 'read', label: 'Read files', detail: 'List directories and read files' },
  { key: 'exec', label: 'Run commands', detail: 'Execute non-interactive shell commands' },
  { key: 'write', label: 'Write files', detail: 'Create or replace remote files' },
];

function ManagedSSHPanel({
  profiles,
  legacyProfiles,
  activeId,
  activeLegacyId,
  onActivate,
  onActivateLegacy,
  onRefresh,
  onLegacyMigrated,
  onDeleteLegacy,
  isAdmin,
}: ManagedSSHPanelProps) {
  const [editing, setEditing] = useState<ManagedProfileDraft | null>(null);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const keyUploadInputRef = useRef<HTMLInputElement>(null);

  const abandonUploadedKey = (draft: ManagedProfileDraft | null) => {
    if (draft?.keyUploadToken) {
      void api.cancelSSHPrivateKeyUpload(draft.keyUploadToken).catch(() => undefined);
    }
  };

  const beginEditing = (draft: ManagedProfileDraft) => {
    abandonUploadedKey(editing);
    setMessage(null);
    setEditing(draft);
  };

  const startEdit = (profile: ManagedSSHProfile) => {
    beginEditing({
      id: profile.id,
      legacyId: null,
      name: profile.name,
      host: profile.host,
      port: profile.port,
      username: profile.username,
      keyPath: '',
      keyUploadToken: '',
      keyUploadFilename: '',
      keyUploadFingerprint: '',
      hostKeyValue: '',
      hostKeyFingerprint: profile.host_key_fingerprint,
      hostKeyConfirmed: true,
      enabled: profile.enabled,
      allowedRootsText: profile.allowed_roots.join('\n'),
      taskAccessEnabled: profile.task_access_enabled,
      taskCapabilities: profile.task_capabilities,
    });
  };

  const startLegacyMigration = (profile: LegacySSHProfile) => {
    beginEditing({
      ...emptyManagedDraft(),
      legacyId: profile.id,
      name: profile.label || `${profile.username}@${profile.host}`,
      host: profile.host,
      port: profile.port,
      username: profile.username,
      keyPath: profile.key_path,
    });
    setMessage(profile.password && !profile.key_path
      ? 'Legacy passwords are not copied. Upload a PEM/private key to migrate this connection.'
      : 'Verify the host fingerprint, review Task access, and save to finish migration.');
  };

  const uploadPrivateKey = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    event.target.value = '';
    if (!file || !editing) return;
    if (file.size > 1024 * 1024) {
      setMessage('Private keys must be no larger than 1 MB.');
      return;
    }
    const previousToken = editing.keyUploadToken;
    setBusy(true);
    setMessage(null);
    try {
      const uploaded = await api.uploadSSHPrivateKey(file);
      if (previousToken) {
        void api.cancelSSHPrivateKeyUpload(previousToken).catch(() => undefined);
      }
      setEditing((current) => current ? {
        ...current,
        keyPath: '',
        keyUploadToken: uploaded.upload_token,
        keyUploadFilename: uploaded.filename,
        keyUploadFingerprint: uploaded.public_key_fingerprint,
      } : current);
      setMessage('Private key uploaded securely. Save the profile to finish.');
    } catch (error) {
      setMessage((error as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const removeUploadedKey = () => {
    if (!editing?.keyUploadToken) return;
    void api.cancelSSHPrivateKeyUpload(editing.keyUploadToken).catch(() => undefined);
    setEditing({
      ...editing,
      keyUploadToken: '',
      keyUploadFilename: '',
      keyUploadFingerprint: '',
    });
  };

  const probeHostKey = async () => {
    if (!editing?.host.trim()) return;
    setBusy(true);
    setMessage(null);
    try {
      const result = await api.probeSSHHostKey({ host: editing.host, port: editing.port });
      setEditing({
        ...editing,
        hostKeyValue: result.host_key_value,
        hostKeyFingerprint: result.fingerprint,
        hostKeyConfirmed: false,
      });
      setMessage('Host identity captured. Verify the fingerprint before saving.');
    } catch (error) {
      setMessage((error as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const saveManagedProfile = async () => {
    if (!editing) return;
    if (!editing.name.trim() || !editing.host.trim() || !editing.username.trim()) {
      setMessage('Name, host, and username are required.');
      return;
    }
    if (editing.id === null && ((!editing.keyPath.trim() && !editing.keyUploadToken) || !editing.hostKeyValue)) {
      setMessage('Upload a private key or enter its Manager path, then verify the host fingerprint.');
      return;
    }
    if (editing.hostKeyValue && !editing.hostKeyConfirmed) {
      setMessage('Confirm that you verified the probed host fingerprint.');
      return;
    }
    setBusy(true);
    setMessage(null);
    try {
      const allowedRoots = editing.allowedRootsText
        .split(/\r?\n/)
        .map((root) => root.trim())
        .filter(Boolean);
      if (!allowedRoots.length || allowedRoots.some((root) => !root.startsWith('/'))) {
        setMessage('Enter at least one absolute POSIX allowed root.');
        return;
      }
      const common = {
        name: editing.name.trim(),
        host: editing.host.trim(),
        port: editing.port,
        username: editing.username.trim(),
        enabled: editing.enabled,
        allowed_roots: allowedRoots,
        task_access_enabled: editing.taskAccessEnabled,
        task_capabilities: editing.taskAccessEnabled
          ? editing.taskCapabilities
          : [],
      };
      let saved: ManagedSSHProfile;
      if (editing.id === null) {
        const input: SSHProfileInput = {
          ...common,
          ...(editing.keyUploadToken
            ? { key_upload_token: editing.keyUploadToken }
            : { key_path: editing.keyPath.trim() }),
          host_key_value: editing.hostKeyValue,
        };
        saved = await api.createSSHProfile(input);
      } else {
        saved = await api.updateSSHProfile(editing.id, {
          ...common,
          ...(editing.keyUploadToken
            ? { key_upload_token: editing.keyUploadToken }
            : editing.keyPath.trim() ? { key_path: editing.keyPath.trim() } : {}),
          ...(editing.hostKeyValue ? { host_key_value: editing.hostKeyValue } : {}),
        });
      }
      const migratedLegacyId = editing.legacyId;
      setEditing(null);
      await onRefresh(saved.id);
      if (migratedLegacyId) onLegacyMigrated(migratedLegacyId);
    } catch (error) {
      setMessage((error as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const testManagedProfile = async (profile: ManagedSSHProfile) => {
    setBusy(true);
    setMessage(null);
    try {
      const result = await api.testSSHProfile(profile.id);
      setMessage(result.ok ? `Connected to ${profile.name}.` : (result.detail || 'Connection test failed.'));
      await onRefresh(profile.id);
    } catch (error) {
      setMessage((error as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const deleteManagedProfile = async (profile: ManagedSSHProfile) => {
    if (!window.confirm(`Delete SSH profile “${profile.name}”?`)) return;
    setBusy(true);
    setMessage(null);
    try {
      await api.deleteSSHProfile(profile.id);
      if (editing?.id === profile.id) {
        abandonUploadedKey(editing);
        setEditing(null);
      }
      await onRefresh();
    } catch (error) {
      setMessage((error as Error).message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between gap-2">
        <div>
          <div className="text-sm font-medium text-gray-200">SSH connections</div>
          <div className="text-xs text-gray-500">Every managed connection can browse files. You decide whether Tasks may use it.</div>
        </div>
        {isAdmin && <button
          disabled={busy}
          onClick={() => beginEditing(emptyManagedDraft())}
          className="flex items-center gap-1 px-2 py-1 bg-indigo-600 text-white rounded text-xs hover:bg-indigo-700 disabled:opacity-50"
        >
          <Plus size={12} /> Add SSH connection
        </button>}
      </div>

      <div className="grid gap-2 sm:grid-cols-2 xl:grid-cols-3">
        {profiles.map((profile) => (
          <div
            key={profile.id}
            className={`rounded border p-3 ${activeId === profile.id ? 'border-indigo-500 bg-indigo-500/10' : 'border-gray-700 bg-gray-800/70'}`}
          >
            <button onClick={() => onActivate(profile)} className="w-full text-left">
              <div className="flex items-center gap-2 text-sm text-gray-200">
                <Server size={14} className="text-indigo-400" />
                <span className="truncate font-medium">{profile.name}</span>
                {!profile.enabled && <span className="ml-auto text-[10px] text-amber-400">disabled</span>}
              </div>
              <div className="mt-1 truncate text-xs text-gray-500">{profile.username}@{profile.host}:{profile.port}</div>
              <div className="mt-1 truncate font-mono text-[10px] text-gray-600" title={profile.host_key_fingerprint}>
                host {profile.host_key_fingerprint}
              </div>
              <div className="mt-2 flex flex-wrap gap-1">
                <span className="rounded bg-indigo-500/15 px-1.5 py-0.5 text-[10px] text-indigo-300">Files</span>
                {profile.allowed_roots.map((root) => (
                  <span key={root} className="max-w-full truncate rounded bg-violet-500/15 px-1.5 py-0.5 font-mono text-[10px] text-violet-300" title={root}>root: {root}</span>
                ))}
                {profile.task_access_enabled ? (
                  profile.task_capabilities.map((capability) => (
                    <span key={capability} className="rounded bg-cyan-500/15 px-1.5 py-0.5 text-[10px] text-cyan-300">Task: {capability}</span>
                  ))
                ) : (
                  <span className="rounded bg-gray-700 px-1.5 py-0.5 text-[10px] text-gray-400">Not exposed to Tasks</span>
                )}
              </div>
            </button>
            <div className="mt-2 flex gap-2 text-xs">
              <button disabled={busy} onClick={() => testManagedProfile(profile)} className="text-emerald-400 hover:text-emerald-300 disabled:opacity-50">test</button>
              {isAdmin && <button disabled={busy} onClick={() => startEdit(profile)} className="text-indigo-400 hover:text-indigo-300 disabled:opacity-50">edit</button>}
              {isAdmin && <button disabled={busy} onClick={() => deleteManagedProfile(profile)} className="ml-auto text-red-400 hover:text-red-300 disabled:opacity-50"><Trash2 size={12} /></button>}
            </div>
          </div>
        ))}
        {legacyProfiles.map((profile) => (
          <div
            key={`legacy-${profile.id}`}
            className={`rounded border p-3 ${activeLegacyId === profile.id ? 'border-amber-500 bg-amber-500/10' : 'border-gray-700 bg-gray-800/70'}`}
          >
            <button onClick={() => onActivateLegacy(profile)} className="w-full text-left">
              <div className="flex items-center gap-2 text-sm text-gray-200">
                <Server size={14} className="text-amber-400" />
                <span className="truncate font-medium">{profile.label || `${profile.username}@${profile.host}`}</span>
              </div>
              <div className="mt-1 truncate text-xs text-gray-500">{profile.username}@{profile.host}:{profile.port}</div>
              <div className="mt-2 flex flex-wrap gap-1">
                <span className="rounded bg-indigo-500/15 px-1.5 py-0.5 text-[10px] text-indigo-300">Files</span>
                <span className="rounded bg-amber-500/15 px-1.5 py-0.5 text-[10px] text-amber-300">Legacy · migrate to expose to Tasks</span>
              </div>
            </button>
            {isAdmin && !profile.id.startsWith('worker-') && (
              <div className="mt-2 flex gap-2 text-xs">
                <button disabled={busy} onClick={() => startLegacyMigration(profile)} className="text-indigo-400 hover:text-indigo-300 disabled:opacity-50">migrate</button>
                <button disabled={busy} onClick={() => onDeleteLegacy(profile.id)} className="ml-auto text-red-400 hover:text-red-300 disabled:opacity-50"><Trash2 size={12} /></button>
              </div>
            )}
          </div>
        ))}
        {profiles.length === 0 && legacyProfiles.length === 0 && (
          <div className="rounded border border-dashed border-gray-700 p-4 text-xs text-gray-500">No SSH connections yet.</div>
        )}
      </div>

      {editing && (
        <div className="rounded border border-gray-700 bg-gray-800 p-3 space-y-3">
          <div className="text-sm font-medium text-gray-200">{editing.legacyId ? 'Migrate SSH connection' : editing.id === null ? 'New SSH connection' : 'Edit SSH connection'}</div>
          <div className="grid gap-2 md:grid-cols-2">
            <label className="text-xs text-gray-400">Name<input value={editing.name} onChange={(e) => setEditing({ ...editing, name: e.target.value })} className="mt-1 w-full rounded bg-gray-700 px-2 py-1.5 text-gray-200" /></label>
            <label className="text-xs text-gray-400">Username<input value={editing.username} onChange={(e) => setEditing({ ...editing, username: e.target.value })} className="mt-1 w-full rounded bg-gray-700 px-2 py-1.5 text-gray-200" /></label>
            <label className="text-xs text-gray-400">Host<input value={editing.host} onChange={(e) => setEditing({ ...editing, host: e.target.value, hostKeyValue: '', hostKeyFingerprint: '', hostKeyConfirmed: false })} className="mt-1 w-full rounded bg-gray-700 px-2 py-1.5 text-gray-200" /></label>
            <label className="text-xs text-gray-400">Port<input type="number" min={1} max={65535} value={editing.port} onChange={(e) => setEditing({ ...editing, port: Number(e.target.value), hostKeyValue: '', hostKeyFingerprint: '', hostKeyConfirmed: false })} className="mt-1 w-full rounded bg-gray-700 px-2 py-1.5 text-gray-200" /></label>
            <div className="text-xs text-gray-400 md:col-span-2">
              <div className="flex items-center justify-between gap-2">
                <span>Private key</span>
                <button disabled={busy} onClick={() => keyUploadInputRef.current?.click()} className="flex items-center gap-1 rounded bg-gray-700 px-2 py-1 text-indigo-300 hover:bg-gray-600 disabled:opacity-50">
                  <Upload size={12} /> Upload PEM or private key
                </button>
                <input ref={keyUploadInputRef} aria-label="Upload SSH private key" type="file" className="hidden" onChange={uploadPrivateKey} />
              </div>
              {editing.keyUploadToken ? (
                <div className="mt-1 flex flex-wrap items-center gap-2 rounded border border-emerald-800 bg-emerald-950/30 px-2 py-1.5">
                  <span className="text-emerald-300">Uploaded: {editing.keyUploadFilename}</span>
                  <span className="min-w-0 truncate font-mono text-[10px] text-gray-500">{editing.keyUploadFingerprint}</span>
                  <button disabled={busy} onClick={removeUploadedKey} className="ml-auto text-red-400 hover:text-red-300 disabled:opacity-50">remove</button>
                </div>
              ) : (
                <input
                  aria-label="Private key path on Manager"
                  value={editing.keyPath}
                  onChange={(e) => setEditing({ ...editing, keyPath: e.target.value })}
                  placeholder={editing.id === null ? '/absolute/path/to/id_ed25519' : 'Leave blank to keep the current key'}
                  className="mt-1 w-full rounded bg-gray-700 px-2 py-1.5 text-gray-200"
                />
              )}
              <div className="mt-1 text-[10px] text-gray-500">Uploaded keys are validated and stored only on the CCM server with mode 0600.</div>
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-2 rounded bg-gray-900/70 p-2">
            <button disabled={busy || !editing.host.trim()} onClick={probeHostKey} className="px-2 py-1 rounded bg-amber-600 text-white text-xs hover:bg-amber-700 disabled:opacity-50">Probe host identity</button>
            <span className="min-w-0 truncate font-mono text-[10px] text-gray-400">{editing.hostKeyFingerprint || 'No newly verified fingerprint'}</span>
            {editing.hostKeyValue && (
              <label className="flex items-center gap-1.5 text-xs text-amber-300">
                <input type="checkbox" checked={editing.hostKeyConfirmed} onChange={(e) => setEditing({ ...editing, hostKeyConfirmed: e.target.checked })} />
                I verified this host fingerprint
              </label>
            )}
          </div>
          <label className="block text-xs text-gray-400">
            Allowed remote roots
            <textarea
              aria-label="Allowed remote roots"
              rows={3}
              value={editing.allowedRootsText}
              onChange={(event) => setEditing({ ...editing, allowedRootsText: event.target.value })}
              placeholder="/srv/app\n/var/log/my-app"
              className="mt-1 w-full rounded bg-gray-700 px-2 py-1.5 font-mono text-gray-200"
            />
            <span className="mt-1 block text-[10px] text-gray-500">
              One absolute POSIX path per line. This limits Files and Task file operations; command execution is not path-scoped. Changing roots requires existing Task grants to be re-authorized.
            </span>
          </label>
          <div className="rounded border border-gray-700 bg-gray-900/50 p-3">
            <label className="flex items-start gap-2 text-xs text-gray-300">
              <input
                aria-label="Allow Tasks to use this connection"
                type="checkbox"
                checked={editing.taskAccessEnabled}
                onChange={(event) => setEditing({
                  ...editing,
                  taskAccessEnabled: event.target.checked,
                  taskCapabilities: event.target.checked
                    ? (editing.taskCapabilities.length ? editing.taskCapabilities : ['read'])
                    : [],
                })}
                className="mt-0.5 accent-cyan-500"
              />
              <span>
                <span className="block font-medium">Allow Tasks to use this connection</span>
                <span className="block text-[10px] text-gray-500">Off means this connection is available only in Files.</span>
              </span>
            </label>
            {editing.taskAccessEnabled && (
              <div className="mt-3 grid gap-2 sm:grid-cols-3">
                {TASK_CAPABILITY_OPTIONS.map((capability) => (
                  <label key={capability.key} className="rounded bg-gray-800 p-2 text-xs text-gray-300" title={capability.detail}>
                    <span className="flex items-center gap-1.5">
                      <input
                        type="checkbox"
                        aria-label={`Task capability: ${capability.label}`}
                        checked={editing.taskCapabilities.includes(capability.key)}
                        onChange={(event) => {
                          const next = event.target.checked
                            ? [...editing.taskCapabilities, capability.key]
                            : editing.taskCapabilities.filter((item) => item !== capability.key);
                          if (next.length) setEditing({ ...editing, taskCapabilities: next });
                        }}
                        className="accent-cyan-500"
                      />
                      {capability.label}
                    </span>
                    <span className="mt-1 block text-[10px] text-gray-500">{capability.detail}</span>
                  </label>
                ))}
              </div>
            )}
          </div>
          <label className="flex items-center gap-2 text-xs text-gray-400"><input type="checkbox" checked={editing.enabled} onChange={(e) => setEditing({ ...editing, enabled: e.target.checked })} />Enabled</label>
          <div className="flex gap-2">
            <button disabled={busy} onClick={saveManagedProfile} className="px-3 py-1 bg-indigo-600 text-white rounded text-xs hover:bg-indigo-700 disabled:opacity-50">{busy ? 'Working…' : 'Save'}</button>
            <button disabled={busy} onClick={() => { abandonUploadedKey(editing); setEditing(null); }} className="px-3 py-1 bg-gray-700 text-gray-300 rounded text-xs hover:bg-gray-600 disabled:opacity-50">Cancel</button>
          </div>
        </div>
      )}

      {message && <div className="rounded border border-gray-700 bg-gray-900/60 px-3 py-2 text-xs text-gray-300">{message}</div>}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Git diff renderer
// ---------------------------------------------------------------------------

const STATUS_COLORS: Record<string, string> = {
  modified: 'text-yellow-400',
  added: 'text-green-400',
  deleted: 'text-red-400',
  untracked: 'text-gray-400',
  renamed: 'text-blue-400',
};

const STATUS_LABELS: Record<string, string> = {
  modified: 'M',
  added: 'A',
  deleted: 'D',
  untracked: '?',
  renamed: 'R',
};

function DiffView({ diff }: { diff: string }) {
  if (!diff.trim()) {
    return <div className="p-4 text-gray-500 text-sm">No changes</div>;
  }

  const lines = diff.split('\n');
  return (
    <pre className="p-4 text-xs font-mono leading-relaxed overflow-auto">
      {lines.map((line, i) => {
        let cls = 'text-gray-400';
        let bg = '';
        if (line.startsWith('+') && !line.startsWith('+++')) {
          cls = 'text-green-400';
          bg = 'bg-green-500/10';
        } else if (line.startsWith('-') && !line.startsWith('---')) {
          cls = 'text-red-400';
          bg = 'bg-red-500/10';
        } else if (line.startsWith('@@')) {
          cls = 'text-indigo-400';
          bg = 'bg-indigo-500/10';
        } else if (line.startsWith('diff ') || line.startsWith('index ') || line.startsWith('---') || line.startsWith('+++')) {
          cls = 'text-gray-500';
        }
        return (
          <div key={i} className={`${cls} ${bg} px-1 whitespace-pre`}>
            {line}
          </div>
        );
      })}
    </pre>
  );
}

// ---------------------------------------------------------------------------
// Main FilesPage
// ---------------------------------------------------------------------------

export function FilesPage() {
  const ccUser = JSON.parse(localStorage.getItem('cc_user') || '{}');
  const isAdmin = ccUser.role === 'admin' || ccUser.role === 'super_admin' || !ccUser.id;
  const [mode, setMode] = useState<Mode>('local');
  const [projects, setProjects] = useState<Project[]>([]);

  // Local state
  const [inputPath, setInputPath] = useState('');
  const [rootPath, setRootPath] = useState('');
  const [rootEntries, setRootEntries] = useState<DirEntry[] | null>(null);
  const [rootLoading, setRootLoading] = useState(false);
  const [rootError, setRootError] = useState<string | null>(null);

  // SSH state
  const [profiles, setProfiles] = useState<LegacySSHProfile[]>(loadProfiles);
  const workerProfiles = useWorkerProfiles();
  const allProfiles = [...workerProfiles, ...profiles];
  const [managedProfiles, setManagedProfiles] = useState<ManagedSSHProfile[]>([]);
  const [activeConnection, setActiveConnection] = useState<SSHConnection | null>(null);
  const [sshPath, setSshPath] = useState('/');
  const [sshEntries, setSshEntries] = useState<DirEntry[] | null>(null);
  const [sshLoading, setSshLoading] = useState(false);
  const [sshError, setSshError] = useState<string | null>(null);

  // Upload state
  const uploadInputRef = useRef<HTMLInputElement>(null);
  const [uploading, setUploading] = useState(false);
  const [uploadError, setUploadError] = useState<string | null>(null);

  // Git state
  const [gitPath, setGitPath] = useState('');
  const [gitBranch, setGitBranch] = useState('');
  const [gitFiles, setGitFiles] = useState<GitFileEntry[] | null>(null);
  const [gitLoading, setGitLoading] = useState(false);
  const [gitError, setGitError] = useState<string | null>(null);
  const [gitSelectedFile, setGitSelectedFile] = useState<string | null>(null);
  const [gitDiff, setGitDiff] = useState<string | null>(null);
  const [gitDiffLoading, setGitDiffLoading] = useState(false);
  const [gitShowStaged, setGitShowStaged] = useState(false);

  // Shared viewer state
  const [selectedFile, setSelectedFile] = useState<string | null>(null);
  const [fileContent, setFileContent] = useState<string | null>(null);
  const [fileLoading, setFileLoading] = useState(false);
  const [fileError, setFileError] = useState<string | null>(null);

  useEffect(() => {
    api.listProjects().then(setProjects).catch(() => {});
    api.listSSHProfiles().then(setManagedProfiles).catch(() => {});
  }, []);

  const refreshManagedProfiles = useCallback(async (preferredId?: number) => {
    const next = await api.listSSHProfiles();
    setManagedProfiles(next);
    setActiveConnection((current) => {
      const targetId = preferredId ?? (current?.kind === 'managed' ? current.profile.id : null);
      if (targetId === null) return current;
      const refreshed = next.find((profile) => profile.id === targetId);
      return refreshed ? { kind: 'managed', profile: refreshed } : null;
    });
  }, []);

  // --- git helpers ---

  const loadGitStatus = useCallback(async (repoPath: string) => {
    if (!repoPath.trim()) return;
    setGitLoading(true);
    setGitError(null);
    setGitFiles(null);
    setGitSelectedFile(null);
    setGitDiff(null);
    try {
      const res = await api.gitStatus(repoPath.trim());
      setGitPath(res.path);
      setGitBranch(res.branch);
      setGitFiles(res.files);
    } catch (e) {
      setGitError((e as Error).message);
    } finally {
      setGitLoading(false);
    }
  }, []);

  const loadGitDiff = useCallback(async (repoPath: string, file?: string, staged?: boolean) => {
    setGitDiffLoading(true);
    try {
      const res = await api.gitDiff(repoPath, file, staged);
      setGitDiff(res.diff);
    } catch (e) {
      setGitDiff(`Error: ${(e as Error).message}`);
    } finally {
      setGitDiffLoading(false);
    }
  }, []);

  const handleGitFileSelect = useCallback((filePath: string) => {
    setGitSelectedFile(filePath);
    loadGitDiff(gitPath, filePath, gitShowStaged);
  }, [gitPath, gitShowStaged, loadGitDiff]);

  const handleGitShowAll = useCallback(() => {
    setGitSelectedFile(null);
    loadGitDiff(gitPath, undefined, gitShowStaged);
  }, [gitPath, gitShowStaged, loadGitDiff]);

  // --- local helpers ---

  const loadLocalRoot = async (path: string) => {
    if (!path.trim()) return;
    setRootLoading(true);
    setRootError(null);
    setRootEntries(null);
    setSelectedFile(null);
    setFileContent(null);
    try {
      const res = await api.listDir(path.trim());
      setRootPath(res.path);
      setRootEntries(res.entries);
    } catch (e) {
      setRootError((e as Error).message);
    } finally {
      setRootLoading(false);
    }
  };

  const localFetchChildren = async (path: string): Promise<DirEntry[]> => {
    const res = await api.listDir(path);
    return res.entries;
  };

  const handleLocalSelect = async (path: string, isDir: boolean) => {
    if (isDir) return;
    openFile(path);
  };

  // --- SSH helpers ---

  const loadSshRoot = async (connection: SSHConnection, path: string) => {
    setSshLoading(true);
    setSshError(null);
    setSshEntries(null);
    setSelectedFile(null);
    setFileContent(null);
    try {
      const res = connection.kind === 'managed'
        ? await api.managedSSHListDir(connection.profile.id, path)
        : await api.sshListDir(profileToCreds(connection.profile), path);
      setSshPath(res.path);
      setSshEntries(res.entries);
    } catch (e) {
      setSshError((e as Error).message);
    } finally {
      setSshLoading(false);
    }
  };

  const sshFetchChildren = async (path: string): Promise<DirEntry[]> => {
    if (!activeConnection) return [];
    const res = activeConnection.kind === 'managed'
      ? await api.managedSSHListDir(activeConnection.profile.id, path)
      : await api.sshListDir(profileToCreds(activeConnection.profile), path);
    return res.entries;
  };

  const handleSshSelect = async (path: string, isDir: boolean) => {
    if (isDir) return;
    openFile(path, activeConnection);
  };

  const activateSSHConnection = (connection: SSHConnection) => {
    const initialPath = connection.kind === 'managed'
      ? (connection.profile.allowed_roots[0] || '/')
      : sshPath;
    setActiveConnection(connection);
    setSshPath(initialPath);
    setSshEntries(null);
    setSelectedFile(null);
    setFileContent(null);
    loadSshRoot(connection, initialPath);
  };

  // --- shared file opener ---

  const openFile = async (path: string, connection: SSHConnection | null = null) => {
    setSelectedFile(path);
    setFileContent(null);
    setFileError(null);
    setFileLoading(true);
    try {
      const res = connection?.kind === 'managed'
        ? await api.managedSSHReadFile(connection.profile.id, path)
        : connection?.kind === 'legacy'
          ? await api.sshReadFile(profileToCreds(connection.profile), path)
          : await api.readFile(path);
      setFileContent(res.content);
    } catch (e) {
      setFileError((e as Error).message);
    } finally {
      setFileLoading(false);
    }
  };

  // --- profile persistence ---

  const handleSaveProfiles = (next: LegacySSHProfile[]) => {
    setProfiles(next);
    saveProfiles(next);
  };

  const removeLegacyProfile = (legacyId: string) => {
    const next = profiles.filter((profile) => profile.id !== legacyId);
    handleSaveProfiles(next);
    if (activeConnection?.kind === 'legacy' && activeConnection.profile.id === legacyId) {
      setActiveConnection(null);
      setSshEntries(null);
      setSelectedFile(null);
      setFileContent(null);
    }
  };

  const handleDownload = async () => {
    if (!selectedFile) return;
    const filename = selectedFile.split('/').pop() || 'download';
    if (mode === 'local') {
      let url = api.downloadFileUrl(selectedFile);
      const token = getToken();
      if (token) url += `&token=${encodeURIComponent(token)}`;
      const iframe = document.createElement('iframe');
      iframe.style.display = 'none';
      iframe.src = url;
      document.body.appendChild(iframe);
      setTimeout(() => document.body.removeChild(iframe), 10000);
    } else if (activeConnection) {
      try {
        const res = activeConnection.kind === 'managed'
          ? await api.managedSSHDownloadFile(activeConnection.profile.id, selectedFile)
          : await api.sshDownloadFile(profileToCreds(activeConnection.profile), selectedFile);
        if (!res.ok) { setFileError('Download failed'); return; }
        const blob = await res.blob();
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        setTimeout(() => URL.revokeObjectURL(url), 5000);
      } catch {
        setFileError('Download failed');
      }
    }
  };

  const handleUploadFiles = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = Array.from(e.target.files || []);
    if (!files.length || !rootPath) return;
    e.target.value = '';
    setUploading(true);
    setUploadError(null);
    try {
      await api.uploadToDir(rootPath, files);
      await loadLocalRoot(rootPath);
    } catch (err) {
      setUploadError(err instanceof Error ? err.message : 'Upload failed');
    } finally {
      setUploading(false);
    }
  };

  const currentEntries = mode === 'local' ? rootEntries : sshEntries;
  const currentRootLabel = mode === 'local'
    ? rootPath
    : activeConnection
      ? `${activeConnection.profile.username}@${activeConnection.profile.host}:${sshPath}`
      : '';

  return (
    <div className="space-y-4">
      {/* Mode toggle + path bar */}
      <div className="bg-gray-800 rounded-lg p-4 space-y-3">
        <div className="flex items-center gap-3 flex-wrap">
          <h2 className="text-sm font-semibold text-foreground">Files</h2>
          <div className="flex rounded overflow-hidden border border-gray-600 text-xs">
            <button
              onClick={() => setMode('local')}
              className={`flex items-center gap-1 px-3 py-1.5 ${mode === 'local' ? 'bg-indigo-600 text-white' : 'bg-gray-700 text-gray-400 hover:bg-gray-600'}`}
            >
              <HardDrive size={12} /> Local
            </button>
            <button
              onClick={() => setMode('ssh')}
              className={`flex items-center gap-1 px-3 py-1.5 ${mode === 'ssh' ? 'bg-indigo-600 text-white' : 'bg-gray-700 text-gray-400 hover:bg-gray-600'}`}
            >
              <Server size={12} /> SSH workspace
            </button>
            <button
              onClick={() => setMode('git')}
              className={`flex items-center gap-1 px-3 py-1.5 ${mode === 'git' ? 'bg-indigo-600 text-white' : 'bg-gray-700 text-gray-400 hover:bg-gray-600'}`}
            >
              <GitBranch size={12} /> Git
            </button>
          </div>
        </div>

        {mode === 'local' && <React.Fragment>
          <div className="flex gap-2 flex-wrap">
            {projects.filter((p) => p.local_path && (!p.location || p.location === 'local')).length > 0 && (
              <select
                onChange={(e) => {
                  const proj = projects.find((p) => String(p.id) === e.target.value);
                  if (proj?.local_path) { setInputPath(proj.local_path); loadLocalRoot(proj.local_path); }
                }}
                defaultValue=""
                className="bg-gray-700 text-gray-300 text-sm rounded px-2 py-1.5 border border-gray-600 focus:outline-none focus:border-indigo-500"
              >
                <option value="" disabled>Select project...</option>
                {projects.filter((p) => p.local_path && (!p.location || p.location === 'local')).map((p) => (
                  <option key={p.id} value={String(p.id)}>{p.name}</option>
                ))}
              </select>
            )}
            <input
              type="text"
              value={inputPath}
              onChange={(e) => setInputPath(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && loadLocalRoot(inputPath)}
              placeholder="/path/to/directory"
              className="flex-1 bg-gray-700 text-gray-300 text-sm rounded px-3 py-1.5 border border-gray-600 focus:outline-none focus:border-indigo-500 min-w-48"
            />
            <button
              onClick={() => loadLocalRoot(inputPath)}
              disabled={rootLoading || !inputPath.trim()}
              className="px-3 py-1.5 bg-indigo-600 text-white text-sm rounded hover:bg-indigo-700 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              Browse
            </button>
            {rootPath && (
              <>
                <input ref={uploadInputRef} type="file" multiple className="hidden" onChange={handleUploadFiles} />
                <button
                  onClick={() => uploadInputRef.current?.click()}
                  disabled={uploading}
                  className="flex items-center gap-1.5 px-3 py-1.5 bg-emerald-600 text-white text-sm rounded hover:bg-emerald-700 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  {uploading ? <Loader2 size={14} className="animate-spin" /> : <Upload size={14} />}
                  Upload
                </button>
              </>
            )}
          </div>
          {uploadError && (
            <div className="text-xs text-red-400 bg-red-500/10 border border-red-500/30 rounded px-2 py-1">
              {uploadError}
            </div>
          )}
        </React.Fragment>}

        {mode === 'ssh' && (
          <div className="space-y-3">
            <ManagedSSHPanel
              profiles={managedProfiles}
              legacyProfiles={allProfiles}
              activeId={activeConnection?.kind === 'managed' ? activeConnection.profile.id : null}
              activeLegacyId={activeConnection?.kind === 'legacy' ? activeConnection.profile.id : null}
              onActivate={(profile) => activateSSHConnection({ kind: 'managed', profile })}
              onActivateLegacy={(profile) => activateSSHConnection({ kind: 'legacy', profile })}
              onRefresh={refreshManagedProfiles}
              onLegacyMigrated={removeLegacyProfile}
              onDeleteLegacy={removeLegacyProfile}
              isAdmin={isAdmin}
            />

            {activeConnection && (
              <div className="flex gap-2">
                <input
                  type="text"
                  value={sshPath}
                  onChange={(e) => setSshPath(e.target.value)}
                  onKeyDown={(e) => e.key === 'Enter' && loadSshRoot(activeConnection, sshPath)}
                  placeholder="/home/user"
                  className="flex-1 bg-gray-700 text-gray-300 text-sm rounded px-3 py-1.5 border border-gray-600 focus:outline-none focus:border-indigo-500"
                />
                <button
                  onClick={() => loadSshRoot(activeConnection, sshPath)}
                  disabled={sshLoading}
                  className="px-3 py-1.5 bg-indigo-600 text-white text-sm rounded hover:bg-indigo-700 disabled:opacity-50"
                >
                  Browse
                </button>
              </div>
            )}
          </div>
        )}

        {mode === 'git' && (
          <div className="space-y-2">
            <div className="flex gap-2 flex-wrap">
              {projects.filter((p) => p.local_path && (!p.location || p.location === 'local')).length > 0 && (
                <select
                  onChange={(e) => {
                    const proj = projects.find((p) => String(p.id) === e.target.value);
                    if (proj?.local_path) { setGitPath(proj.local_path); loadGitStatus(proj.local_path); }
                  }}
                  defaultValue=""
                  className="bg-gray-700 text-gray-300 text-sm rounded px-2 py-1.5 border border-gray-600 focus:outline-none focus:border-indigo-500"
                >
                  <option value="" disabled>Select project...</option>
                  {projects.filter((p) => p.local_path && (!p.location || p.location === 'local')).map((p) => (
                    <option key={p.id} value={String(p.id)}>{p.name}</option>
                  ))}
                </select>
              )}
              <input
                type="text"
                value={gitPath}
                onChange={(e) => setGitPath(e.target.value)}
                onKeyDown={(e) => e.key === 'Enter' && loadGitStatus(gitPath)}
                placeholder="/path/to/git/repo"
                className="flex-1 bg-gray-700 text-gray-300 text-sm rounded px-3 py-1.5 border border-gray-600 focus:outline-none focus:border-indigo-500 min-w-48"
              />
              <button
                onClick={() => loadGitStatus(gitPath)}
                disabled={gitLoading || !gitPath.trim()}
                className="px-3 py-1.5 bg-indigo-600 text-white text-sm rounded hover:bg-indigo-700 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {gitLoading ? <Loader2 size={14} className="animate-spin" /> : 'Load'}
              </button>
            </div>
          </div>
        )}

        {(rootError || sshError || (mode === 'git' && gitError)) && (
          <div className="flex items-center gap-2 text-red-400 text-sm">
            <AlertCircle size={14} /> {mode === 'local' ? rootError : mode === 'ssh' ? sshError : gitError}
          </div>
        )}
      </div>

      {/* Git diff area */}
      {mode === 'git' && gitFiles !== null && (
        <div className="flex flex-col md:flex-row gap-4 h-auto md:h-[calc(100vh-260px)] min-h-80">
          {/* Changed files list */}
          <div className="w-full md:w-72 md:flex-shrink-0 max-h-64 md:max-h-none bg-gray-800 rounded-lg overflow-y-auto p-2">
            <div className="flex items-center gap-2 px-2 pb-2 border-b border-gray-700 mb-2">
              <GitBranch size={14} className="text-indigo-400" />
              <span className="text-xs text-indigo-400 font-medium">{gitBranch || 'HEAD'}</span>
              <span className="text-xs text-gray-500">({gitFiles.length} changes)</span>
              <button
                onClick={() => loadGitStatus(gitPath)}
                className="ml-auto text-gray-500 hover:text-gray-300"
                title="Refresh"
              >
                <RefreshCw size={12} />
              </button>
            </div>

            <div className="flex gap-1 mb-2 px-1">
              <button
                onClick={() => { setGitShowStaged(false); if (gitSelectedFile) loadGitDiff(gitPath, gitSelectedFile, false); else if (gitDiff !== null) loadGitDiff(gitPath, undefined, false); }}
                className={`px-2 py-0.5 rounded text-xs ${!gitShowStaged ? 'bg-indigo-600 text-white' : 'bg-gray-700 text-gray-400 hover:bg-gray-600'}`}
              >
                Unstaged
              </button>
              <button
                onClick={() => { setGitShowStaged(true); if (gitSelectedFile) loadGitDiff(gitPath, gitSelectedFile, true); else if (gitDiff !== null) loadGitDiff(gitPath, undefined, true); }}
                className={`px-2 py-0.5 rounded text-xs ${gitShowStaged ? 'bg-indigo-600 text-white' : 'bg-gray-700 text-gray-400 hover:bg-gray-600'}`}
              >
                Staged
              </button>
            </div>

            {gitFiles.length === 0 && (
              <div className="text-xs text-gray-500 px-2 py-4 text-center">Working tree clean</div>
            )}

            {gitFiles.length > 0 && (
              <button
                onClick={handleGitShowAll}
                className={`w-full text-left flex items-center gap-2 px-2 py-1 rounded text-xs cursor-pointer hover:bg-gray-700 ${
                  gitSelectedFile === null && gitDiff !== null ? 'bg-gray-700 text-indigo-400' : 'text-gray-300'
                }`}
              >
                <FileText size={12} className="text-gray-500" />
                <span>All changes</span>
              </button>
            )}

            {gitFiles.map((f) => (
              <button
                key={f.path}
                onClick={() => handleGitFileSelect(f.path)}
                className={`w-full text-left flex items-center gap-2 px-2 py-1 rounded text-xs cursor-pointer hover:bg-gray-700 ${
                  gitSelectedFile === f.path ? 'bg-gray-700 text-indigo-400' : 'text-gray-300'
                }`}
              >
                <span className={`w-3 font-mono font-bold flex-shrink-0 ${STATUS_COLORS[f.status] || 'text-gray-400'}`}>
                  {STATUS_LABELS[f.status] || '?'}
                </span>
                <span className="truncate">{f.path}</span>
              </button>
            ))}
          </div>

          {/* Diff viewer */}
          <div className="flex-1 min-h-80 bg-gray-800 rounded-lg overflow-hidden flex flex-col">
            {gitDiff === null && !gitDiffLoading && (
              <div className="flex-1 flex items-center justify-center text-gray-600 text-sm">
                Select a file or click "All changes" to view diff
              </div>
            )}
            {gitDiffLoading && (
              <div className="flex-1 flex items-center justify-center text-gray-400 text-sm gap-2">
                <Loader2 size={14} className="animate-spin" /> Loading diff...
              </div>
            )}
            {gitDiff !== null && !gitDiffLoading && (
              <>
                <div className="px-4 py-2 border-b border-gray-700 text-xs text-gray-400">
                  {gitSelectedFile || 'All changes'} — {gitShowStaged ? 'staged' : 'unstaged'}
                </div>
                <div className="flex-1 overflow-auto">
                  <DiffView diff={gitDiff} />
                </div>
              </>
            )}
          </div>
        </div>
      )}

      {/* Main browser area */}
      {currentEntries !== null && mode !== 'git' && (
        <div className="flex flex-col md:flex-row gap-4 h-auto md:h-[calc(100vh-260px)] min-h-80">
          {/* File tree */}
          <div className="w-full md:w-64 md:flex-shrink-0 max-h-64 md:max-h-none bg-gray-800 rounded-lg overflow-y-auto p-2">
            <div className="text-xs text-gray-500 px-2 pb-1 truncate" title={currentRootLabel}>{currentRootLabel}</div>
            {(rootLoading || sshLoading) && (
              <div className="flex items-center gap-2 px-2 py-4 text-gray-400 text-sm">
                <Loader2 size={14} className="animate-spin" /> Loading...
              </div>
            )}
            {currentEntries.length === 0 && !rootLoading && !sshLoading && (
              <div className="text-xs text-gray-600 px-2 py-2">empty directory</div>
            )}
            {currentEntries.map((entry) => (
              <TreeNode
                key={entry.path}
                entry={entry}
                selectedPath={selectedFile}
                onSelect={mode === 'local' ? handleLocalSelect : handleSshSelect}
                fetchChildren={mode === 'local' ? localFetchChildren : sshFetchChildren}
              />
            ))}
          </div>

          {/* File viewer */}
          <div className="flex-1 min-h-80 bg-gray-800 rounded-lg overflow-hidden flex flex-col">
            {!selectedFile && (
              <div className="flex-1 flex items-center justify-center text-gray-600 text-sm">
                Select a file to preview
              </div>
            )}
            {selectedFile && (
              <>
                <div className="px-4 py-2 border-b border-gray-700 text-xs text-gray-400 flex items-center gap-2">
                  <span className="truncate flex-1" title={selectedFile}>{selectedFile}</span>
                  <button
                    onClick={handleDownload}
                    className="flex items-center gap-1 px-2 py-1 bg-indigo-600 text-white rounded text-xs hover:bg-indigo-700 flex-shrink-0"
                    title="Download file"
                  >
                    <Download size={12} /> Download
                  </button>
                </div>
                <div className="flex-1 overflow-auto">
                  {fileLoading && (
                    <div className="flex items-center gap-2 p-4 text-gray-400 text-sm">
                      <Loader2 size={14} className="animate-spin" /> Loading...
                    </div>
                  )}
                  {fileError && (
                    <div className="flex items-center gap-2 p-4 text-red-400 text-sm">
                      <AlertCircle size={14} /> {fileError}
                    </div>
                  )}
                  {fileContent !== null && (
                    <pre className="p-4 text-xs text-gray-300 font-mono whitespace-pre-wrap break-all leading-relaxed">
                      {fileContent}
                    </pre>
                  )}
                </div>
              </>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

function profileToCreds(p: LegacySSHProfile) {
  return {
    host: p.host,
    port: p.port,
    username: p.username,
    ...(p.password ? { password: p.password } : {}),
    ...(p.key_path ? { key_path: p.key_path } : {}),
  };
}
