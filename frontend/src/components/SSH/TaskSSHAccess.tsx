import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { api } from '../../api/client';
import type {
  SSHProfile,
  Task,
  TaskSSHCapability,
  TaskSSHGrant,
  TaskSSHGrantInput,
} from '../../api/client';
import { AlertCircle, ChevronDown, Loader2, Server } from '../icons';


const CAPABILITIES: Array<{
  key: TaskSSHCapability;
  label: string;
  description: string;
}> = [
  { key: 'read', label: 'Read files', description: 'List directories and preview text files' },
  { key: 'exec', label: 'Run commands', description: 'Arbitrary non-interactive shell commands' },
  { key: 'write', label: 'Write files', description: 'Create or replace remote text files' },
];

interface SSHGrantPickerProps {
  value: TaskSSHGrantInput[];
  onChange: (value: TaskSSHGrantInput[]) => void;
  snapshots?: TaskSSHGrant[];
  readOnly?: boolean;
  compact?: boolean;
  disabledReason?: string;
  busy?: boolean;
  error?: string;
  onSave?: () => void;
  dirty?: boolean;
}

export function SSHGrantPicker({
  value,
  onChange,
  snapshots = [],
  readOnly = false,
  compact = false,
  disabledReason,
  busy = false,
  error,
  onSave,
  dirty = false,
}: SSHGrantPickerProps) {
  const [open, setOpen] = useState(false);
  const [profiles, setProfiles] = useState<SSHProfile[]>([]);
  const [loadingProfiles, setLoadingProfiles] = useState(false);
  const [profilesLoaded, setProfilesLoaded] = useState(false);
  const [profilesError, setProfilesError] = useState('');
  const rootRef = useRef<HTMLDivElement>(null);

  const loadProfiles = () => {
    if (readOnly || profilesLoaded || loadingProfiles) return;
    setLoadingProfiles(true);
    api.listSSHProfiles(true)
      .then(setProfiles)
      .catch((loadError) => setProfilesError(
        loadError instanceof Error ? loadError.message : 'Failed to load SSH profiles',
      ))
      .finally(() => {
        setLoadingProfiles(false);
        setProfilesLoaded(true);
      });
  };

  const toggleOpen = () => {
    if (disabledReason) return;
    const nextOpen = !open;
    setOpen(nextOpen);
    if (nextOpen) loadProfiles();
  };

  useEffect(() => {
    if (!open) return;
    const close = (event: MouseEvent) => {
      if (!rootRef.current?.contains(event.target as Node)) setOpen(false);
    };
    document.addEventListener('mousedown', close);
    return () => document.removeEventListener('mousedown', close);
  }, [open]);

  const snapshotById = useMemo(
    () => new Map(snapshots.map((grant) => [grant.profile_id, grant])),
    [snapshots],
  );
  const visibleProfiles = useMemo(() => {
    const items = profiles.map((profile) => ({
      id: profile.id,
      name: profile.name,
      host: profile.host,
      port: profile.port,
      username: profile.username,
      enabled: profile.enabled,
      taskAccessEnabled: profile.task_access_enabled,
      taskCapabilities: profile.task_capabilities,
      allowedRoots: profile.allowed_roots,
    }));
    for (const grant of snapshots) {
      if (items.some((profile) => profile.id === grant.profile_id)) continue;
      items.push({
        id: grant.profile_id,
        name: grant.profile_name,
        host: grant.host,
        port: grant.port,
        username: grant.username,
        enabled: grant.valid,
        taskAccessEnabled: grant.profile_task_access_enabled,
        taskCapabilities: grant.profile_task_capabilities,
        allowedRoots: grant.profile_allowed_roots,
      });
    }
    return items;
  }, [profiles, snapshots]);

  const selected = (profileId: number) => value.find(
    (grant) => grant.profile_id === profileId,
  );
  const toggleProfile = (profileId: number) => {
    if (readOnly || busy) return;
    const current = selected(profileId);
    const profile = visibleProfiles.find((item) => item.id === profileId);
    const defaultCapability = CAPABILITIES.find((capability) => (
      profile?.taskCapabilities.includes(capability.key)
    ))?.key;
    if (!current && (!profile?.taskAccessEnabled || !defaultCapability)) return;
    onChange(current
      ? value.filter((grant) => grant.profile_id !== profileId)
      : [...value, { profile_id: profileId, capabilities: [defaultCapability!] }]);
  };
  const toggleCapability = (
    profileId: number,
    capability: TaskSSHCapability,
  ) => {
    if (readOnly || busy) return;
    const profile = visibleProfiles.find((item) => item.id === profileId);
    if (!profile?.taskCapabilities.includes(capability)) return;
    onChange(value.map((grant) => {
      if (grant.profile_id !== profileId) return grant;
      const has = grant.capabilities.includes(capability);
      if (has && grant.capabilities.length === 1) return grant;
      return {
        ...grant,
        capabilities: has
          ? grant.capabilities.filter((item) => item !== capability)
          : [...grant.capabilities, capability],
      };
    }));
  };

  const invalidCount = snapshots.filter((grant) => !grant.valid).length;
  const effectiveError = error || profilesError;

  return (
    <div ref={rootRef} className="relative" data-ssh-grant-picker>
      <button
        type="button"
        onClick={toggleOpen}
        disabled={Boolean(disabledReason)}
        title={disabledReason || 'Manage Task SSH access'}
        className={compact
          ? `text-xs px-1.5 rounded flex items-center gap-0.5 transition-colors ${invalidCount ? 'bg-red-600/25 text-red-300' : 'bg-cyan-600/25 text-cyan-300'} disabled:opacity-50`
          : `flex items-center gap-1 text-xs px-2 py-1.5 rounded border transition-colors ${value.length ? 'bg-cyan-600/20 text-cyan-300 border-cyan-500/40' : 'bg-gray-700 text-gray-400 border-gray-600 hover:bg-gray-600'} disabled:opacity-50`}
      >
        <Server size={12} />
        <span>{compact ? `SSH ${value.length}` : `SSH access${value.length ? ` (${value.length})` : ''}`}</span>
        {!compact && <ChevronDown size={11} />}
      </button>

      {open && (
        <div className={`absolute top-full mt-1 ${compact ? 'right-0' : 'left-0'} z-40 w-[min(26rem,calc(100vw-1rem))] rounded-lg border border-gray-600 bg-gray-800 p-3 shadow-xl`}>
          <div className="mb-2 flex items-start justify-between gap-3">
            <div>
              <div className="text-sm font-medium text-gray-200">Task SSH authorization</div>
              <div className="text-xs text-gray-500">Keys remain on the Manager. Grants apply only to this Task.</div>
            </div>
            {busy && <Loader2 size={14} className="animate-spin text-cyan-400" />}
          </div>

          {effectiveError && (
            <div role="alert" className="mb-2 flex items-center gap-1.5 rounded border border-red-500/30 bg-red-500/10 px-2 py-1.5 text-xs text-red-300">
              <AlertCircle size={12} /> {effectiveError}
            </div>
          )}
          {loadingProfiles && (
            <div className="flex items-center gap-2 py-4 text-xs text-gray-500"><Loader2 size={13} className="animate-spin" />Loading SSH profiles…</div>
          )}
          {!loadingProfiles && visibleProfiles.length === 0 && (
            <div className="rounded border border-dashed border-gray-700 px-3 py-4 text-center text-xs text-gray-500">
              No SSH connections are currently exposed to Tasks. Enable Task access in <a href="#/files" className="text-indigo-400 hover:text-indigo-300">Files → SSH workspace</a>.
            </div>
          )}

          <div className="max-h-72 space-y-2 overflow-y-auto">
            {visibleProfiles.map((profile) => {
              const grant = selected(profile.id);
              const snapshot = snapshotById.get(profile.id);
              return (
                <div key={profile.id} className={`rounded border p-2 ${grant ? 'border-cyan-500/40 bg-cyan-500/5' : 'border-gray-700 bg-gray-900/40'}`}>
                  <label className="flex items-start gap-2 text-xs">
                    <input
                      type="checkbox"
                      aria-label={`Grant ${profile.name}`}
                      checked={Boolean(grant)}
                      disabled={readOnly || busy || ((!profile.enabled || !profile.taskAccessEnabled) && !grant)}
                      onChange={() => toggleProfile(profile.id)}
                      className="mt-0.5 accent-cyan-500"
                    />
                    <span className="min-w-0 flex-1">
                      <span className="block truncate font-medium text-gray-200">{profile.name}</span>
                      <span className="block truncate text-gray-500">{profile.username}@{profile.host}:{profile.port}</span>
                      <span className="block truncate font-mono text-[10px] text-violet-300" title={profile.allowedRoots.join(', ')}>
                        Files: {profile.allowedRoots.join(', ')}
                      </span>
                    </span>
                    {snapshot && !snapshot.valid && <span className="text-[10px] text-red-300">re-authorize</span>}
                  </label>
                  {grant && (
                    <div className="mt-2 grid gap-1 pl-6 sm:grid-cols-3">
                      {CAPABILITIES.map((capability) => (
                        <label key={capability.key} className={`flex items-start gap-1.5 rounded bg-gray-800 px-2 py-1.5 text-[11px] ${profile.taskCapabilities.includes(capability.key) ? 'text-gray-300' : 'text-gray-600'}`} title={profile.taskCapabilities.includes(capability.key) ? capability.description : 'This connection does not expose this capability to Tasks'}>
                          <input
                            type="checkbox"
                            aria-label={`${profile.name}: ${capability.label}`}
                            checked={grant.capabilities.includes(capability.key)}
                            disabled={readOnly || busy || !profile.taskCapabilities.includes(capability.key)}
                            onChange={() => toggleCapability(profile.id, capability.key)}
                            className="mt-0.5 accent-cyan-500"
                          />
                          <span>{capability.label}</span>
                        </label>
                      ))}
                    </div>
                  )}
                  {snapshot && !snapshot.valid && (
                    <div className="mt-1 pl-6 text-[10px] text-red-300">Invalid: {snapshot.invalid_reason?.replaceAll('_', ' ')}</div>
                  )}
                </div>
              );
            })}
          </div>

          {!readOnly && value.some((grant) => grant.capabilities.includes('exec')) && (
            <div className="mt-2 text-[10px] text-amber-300">Run commands grants broad access as the configured remote user; allowed file roots do not restrict command execution.</div>
          )}
          {!readOnly && onSave && (
            <div className="mt-3 flex justify-end border-t border-gray-700 pt-2">
              <button
                type="button"
                onClick={onSave}
                disabled={!dirty || busy}
                className="rounded bg-cyan-600 px-3 py-1.5 text-xs text-white hover:bg-cyan-700 disabled:cursor-not-allowed disabled:opacity-40"
              >
                {busy ? 'Saving…' : 'Save SSH grants'}
              </button>
            </div>
          )}
          {readOnly && <div className="mt-2 text-[10px] text-gray-500">Read-only view. Only an administrator can change SSH grants.</div>}
        </div>
      )}
    </div>
  );
}


export function TaskSSHAccessBadge({ task }: { task: Task }) {
  const cachedUser = JSON.parse(localStorage.getItem('cc_user') || '{}');
  const isAdmin = cachedUser.role === 'admin' || cachedUser.role === 'super_admin' || !cachedUser.id;
  const [snapshots, setSnapshots] = useState<TaskSSHGrant[]>([]);
  const [value, setValue] = useState<TaskSSHGrantInput[]>([]);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');

  const load = useCallback(async () => {
    try {
      const grants = typeof api.listTaskSSHGrants === 'function'
        ? await api.listTaskSSHGrants(task.id)
        : [];
      setSnapshots(grants);
      setValue(grants.map((grant) => ({
        profile_id: grant.profile_id,
        capabilities: grant.profile_task_access_enabled
          ? grant.capabilities.filter((capability) => (
            grant.profile_task_capabilities.includes(capability)
          ))
          : grant.capabilities,
      })));
      setError('');
    } catch (loadError) {
      setError(loadError instanceof Error ? loadError.message : 'Failed to load SSH grants');
    }
  }, [task.id]);

  useEffect(() => { void load(); }, [load]);

  const remoteScope = task.worker_id != null
    || task.shared_from_id != null
    || task.metadata_?.ccm_worker_managed_task === true;
  if (!isAdmin && value.length === 0) return null;

  const persistedValue = snapshots.map((grant) => ({
    profile_id: grant.profile_id,
    capabilities: grant.capabilities,
  }));
  const dirty = snapshots.some((grant) => !grant.valid)
    || JSON.stringify(value) !== JSON.stringify(persistedValue);

  const save = async () => {
    if (!isAdmin || remoteScope) return;
    setSaving(true);
    setError('');
    try {
      const grants = await api.updateTaskSSHGrants(task.id, value);
      setSnapshots(grants);
      setValue(grants.map((grant) => ({
        profile_id: grant.profile_id,
        capabilities: grant.capabilities.filter((capability) => (
          grant.profile_task_capabilities.includes(capability)
        )),
      })));
    } catch (saveError) {
      setError(saveError instanceof Error ? saveError.message : 'Failed to save SSH grants');
      await load();
    } finally {
      setSaving(false);
    }
  };

  return (
    <SSHGrantPicker
      value={value}
      onChange={(next) => { setValue(next); setError(''); }}
      snapshots={snapshots}
      readOnly={!isAdmin}
      compact
      disabledReason={remoteScope ? 'Manager-local SSH keys are unavailable to Worker Tasks' : undefined}
      busy={saving}
      error={error}
      onSave={() => { void save(); }}
      dirty={dirty}
    />
  );
}
