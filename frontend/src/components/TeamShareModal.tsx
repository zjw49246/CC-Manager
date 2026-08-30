import { useState, useEffect } from 'react';
import { X, Check, Users } from './icons';
import { api } from '../api/client';
import type { TeamUser } from '../api/client';

interface Group {
  id: number;
  name: string;
  members: { id: number; name: string }[];
}

interface TeamShareRecord {
  target_id: number;
  target_type: string;
}

interface TeamShareModalProps {
  type: 'project' | 'task';
  itemId: number;
  itemTitle: string;
  onClose: () => void;
}

export function TeamShareModal({ type, itemId, itemTitle, onClose }: TeamShareModalProps) {
  const [users, setUsers] = useState<TeamUser[]>([]);
  const [groups, setGroups] = useState<Group[]>([]);
  const [shares, setShares] = useState<{ target_id: number; target_type: string }[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [saving, setSaving] = useState<string | null>(null);

  const me = JSON.parse(localStorage.getItem('cc_user') || '{}');

  useEffect(() => {
    let disposed = false;
    setLoading(true);
    setLoadError(null);
    Promise.all([
      api.getTeamUsers(),
      type === 'project' ? api.teamGetProjectShares(itemId) : api.getTaskSharesTeam(itemId),
      api.getTeamGroups(),
    ]).then(([u, s, g]) => {
      if (disposed) return;
      setUsers(u);
      setShares(s.map((share: TeamShareRecord) => ({
        target_id: share.target_id,
        target_type: share.target_type,
      })));
      setGroups(g);
    }).catch((reason: unknown) => {
      if (disposed) return;
      const detail = reason instanceof Error ? reason.message : String(reason);
      setLoadError(`Unable to load sharing options: ${detail}`);
    }).finally(() => {
      if (!disposed) setLoading(false);
    });
    return () => {
      disposed = true;
    };
  }, [type, itemId]);

  const isSharedUser = (userId: number) => shares.some(s => s.target_type === 'user' && s.target_id === userId);
  const isSharedGroup = (groupId: number) => shares.some(s => s.target_type === 'group' && s.target_id === groupId);

  const toggleUser = async (userId: number) => {
    setSaving(`user:${userId}`);
    try {
      if (isSharedUser(userId)) {
        if (type === 'project') await api.teamUnshareProject(itemId, 'user', userId);
        else await api.unshareTaskTeam(itemId, 'user', userId);
        setShares(shares.filter(s => !(s.target_type === 'user' && s.target_id === userId)));
      } else {
        if (type === 'project') await api.teamShareProject(itemId, 'user', userId);
        else await api.shareTaskTeam(itemId, 'user', userId);
        setShares([...shares, { target_type: 'user', target_id: userId }]);
      }
    } catch (e) {
      alert(String(e));
    } finally {
      setSaving(null);
    }
  };

  const toggleGroup = async (groupId: number) => {
    setSaving(`group:${groupId}`);
    try {
      if (isSharedGroup(groupId)) {
        if (type === 'project') await api.teamUnshareProject(itemId, 'group', groupId);
        else await api.unshareTaskTeam(itemId, 'group', groupId);
        setShares(shares.filter(s => !(s.target_type === 'group' && s.target_id === groupId)));
      } else {
        if (type === 'project') await api.teamShareProject(itemId, 'group', groupId);
        else await api.shareTaskTeam(itemId, 'group', groupId);
        setShares([...shares, { target_type: 'group', target_id: groupId }]);
      }
    } catch (e) {
      alert(String(e));
    } finally {
      setSaving(null);
    }
  };

  const filteredUsers = users.filter(u =>
    u.role !== 'admin' && u.role !== 'super_admin' && u.id !== me.id
  );

  return (
    <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50 p-4">
      <div className="bg-gray-800 rounded-xl shadow-2xl w-full max-w-sm">
        <div className="flex items-center justify-between px-5 py-4 border-b border-gray-700">
          <h3 className="text-foreground font-semibold text-sm">
            Share {type === 'project' ? 'Project' : 'Task'}: {itemTitle}
          </h3>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-200"><X size={18} /></button>
        </div>
        <div className="p-4 space-y-3 max-h-[60vh] overflow-y-auto">
          {loading ? (
            <p className="text-gray-400 text-sm">Loading...</p>
          ) : loadError ? (
            <p role="alert" className="rounded border border-red-500/40 bg-red-950/40 px-3 py-2 text-sm text-red-300">
              {loadError}
            </p>
          ) : (
            <>
              {/* Groups */}
              {groups.length > 0 && (
                <div>
                  <p className="text-xs text-gray-500 mb-1.5">Groups</p>
                  <div className="space-y-1">
                    {groups.map(g => (
                      <div
                        key={`g:${g.id}`}
                        className="flex items-center justify-between p-2 rounded hover:bg-gray-700 cursor-pointer"
                        onClick={() => saving === null && toggleGroup(g.id)}
                      >
                        <div className="flex items-center gap-2">
                          <Users size={16} className="text-indigo-400" />
                          <div>
                            <div className="text-sm text-foreground">{g.name}</div>
                            <div className="text-xs text-gray-500">{g.members.length} members</div>
                          </div>
                        </div>
                        <div className="shrink-0 ml-2">
                          {saving === `group:${g.id}` ? (
                            <div className="w-5 h-5 border-2 border-gray-500 border-t-transparent rounded-full animate-spin" />
                          ) : isSharedGroup(g.id) ? (
                            <div className="w-5 h-5 bg-green-500 rounded flex items-center justify-center">
                              <Check size={12} className="text-white" />
                            </div>
                          ) : (
                            <div className="w-5 h-5 border border-gray-600 rounded" />
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Users */}
              {filteredUsers.length > 0 && (
                <div>
                  {groups.length > 0 && <p className="text-xs text-gray-500 mb-1.5">Users</p>}
                  <div className="space-y-1">
                    {filteredUsers.map(u => (
                      <div
                        key={`u:${u.id}`}
                        className="flex items-center justify-between p-2 rounded hover:bg-gray-700 cursor-pointer"
                        onClick={() => saving === null && toggleUser(u.id)}
                      >
                        <div className="flex items-center gap-2 min-w-0">
                          {u.avatar_url ? (
                            <img src={u.avatar_url} className="w-7 h-7 rounded-full" alt="" />
                          ) : (
                            <div className="w-7 h-7 rounded-full bg-gray-600 flex items-center justify-center text-xs text-gray-300">
                              {u.name.charAt(0).toUpperCase()}
                            </div>
                          )}
                          <div className="min-w-0">
                            <div className="text-sm text-foreground truncate">{u.name}</div>
                            <div className="text-xs text-gray-500 truncate">{u.email}</div>
                          </div>
                        </div>
                        <div className="shrink-0 ml-2">
                          {saving === `user:${u.id}` ? (
                            <div className="w-5 h-5 border-2 border-gray-500 border-t-transparent rounded-full animate-spin" />
                          ) : isSharedUser(u.id) ? (
                            <div className="w-5 h-5 bg-green-500 rounded flex items-center justify-center">
                              <Check size={12} className="text-white" />
                            </div>
                          ) : (
                            <div className="w-5 h-5 border border-gray-600 rounded" />
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {filteredUsers.length === 0 && groups.length === 0 && (
                <p className="text-gray-500 text-sm">No users or groups to share with.</p>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  );
}
