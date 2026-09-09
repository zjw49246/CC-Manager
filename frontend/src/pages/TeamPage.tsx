import { useState, useEffect, useCallback } from 'react';
import { api } from '../api/client';
import type { TeamUser, Worker } from '../api/client';
import { Plus, X, Trash2, UserPlus, Users, Shield, ShieldCheck, User } from '../components/icons';

interface UserGroup {
  id: number;
  name: string;
  description: string;
  members: { id: number; name: string; email: string; avatar_url: string }[];
}

/* ── Create / Edit Group Modal ─────────────────────────────────── */
function GroupModal({
  group,
  onClose,
  onSaved,
}: {
  group?: UserGroup | null;
  onClose: () => void;
  onSaved: () => void;
}) {
  const [name, setName] = useState(group?.name || '');
  const [description, setDescription] = useState(group?.description || '');
  const [saving, setSaving] = useState(false);

  const handleSave = async () => {
    if (!name.trim()) return;
    setSaving(true);
    try {
      if (group) {
        await api.updateTeamGroup(group.id, name.trim(), description.trim());
      } else {
        await api.createTeamGroup(name.trim(), description.trim());
      }
      onSaved();
      onClose();
    } catch { /* ignore */ }
    setSaving(false);
  };

  return (
    <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50 p-4">
      <div className="bg-gray-800 rounded-xl shadow-2xl w-full max-w-sm">
        <div className="flex items-center justify-between px-5 py-4 border-b border-gray-700">
          <h3 className="text-foreground font-semibold">{group ? 'Edit Group' : 'New Group'}</h3>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-200"><X size={18} /></button>
        </div>
        <div className="p-5 space-y-3">
          <input
            className="w-full bg-gray-700 text-foreground rounded px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500"
            placeholder="Group name"
            value={name}
            onChange={(e) => setName(e.target.value)}
            autoFocus
          />
          <input
            className="w-full bg-gray-700 text-foreground rounded px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500"
            placeholder="Description (optional)"
            value={description}
            onChange={(e) => setDescription(e.target.value)}
          />
          <div className="flex justify-end gap-2 pt-1">
            <button onClick={onClose} className="px-4 py-2 text-sm text-gray-300 hover:text-foreground">Cancel</button>
            <button onClick={handleSave} disabled={saving || !name.trim()}
              className="px-4 py-2 text-sm bg-indigo-600 text-white rounded hover:bg-indigo-500 disabled:opacity-50">
              {saving ? 'Saving...' : 'Save'}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

/* ── Add Member to Group Modal ────────────────────────────────── */
function AddMemberModal({
  groupId,
  existingUserIds,
  allUsers,
  onClose,
  onSaved,
}: {
  groupId: number;
  existingUserIds: Set<number>;
  allUsers: TeamUser[];
  onClose: () => void;
  onSaved: () => void;
}) {
  const available = allUsers.filter(u => !existingUserIds.has(u.id));
  const [adding, setAdding] = useState<number | null>(null);

  const handleAdd = async (userId: number) => {
    setAdding(userId);
    try {
      await api.addTeamGroupMember(groupId, userId);
      onSaved();
    } catch { /* ignore */ }
    setAdding(null);
  };

  return (
    <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50 p-4">
      <div className="bg-gray-800 rounded-xl shadow-2xl w-full max-w-sm">
        <div className="flex items-center justify-between px-5 py-4 border-b border-gray-700">
          <h3 className="text-foreground font-semibold">Add Member</h3>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-200"><X size={18} /></button>
        </div>
        <div className="p-4 space-y-1 max-h-[50vh] overflow-y-auto">
          {available.length === 0 ? (
            <p className="text-gray-500 text-sm">All users are already in this group.</p>
          ) : (
            available.map(u => (
              <div key={u.id} className="flex items-center justify-between p-2 rounded bg-gray-700/50 hover:bg-gray-700">
                <div className="flex items-center gap-2">
                  {u.avatar_url ? (
                    <img src={u.avatar_url} className="w-7 h-7 rounded-full" alt="" />
                  ) : (
                    <div className="w-7 h-7 rounded-full bg-gray-600 flex items-center justify-center text-xs text-gray-300">
                      {u.name.charAt(0).toUpperCase()}
                    </div>
                  )}
                  <div>
                    <div className="text-sm text-foreground">{u.name}</div>
                    <div className="text-xs text-gray-500">{u.email}</div>
                  </div>
                </div>
                <button onClick={() => handleAdd(u.id)} disabled={adding === u.id}
                  className="text-xs px-2 py-1 bg-indigo-600/20 text-indigo-300 rounded hover:bg-indigo-600/30 disabled:opacity-50">
                  {adding === u.id ? 'Adding...' : 'Add'}
                </button>
              </div>
            ))
          )}
        </div>
      </div>
    </div>
  );
}

/* ── Team Page ────────────────────────────────────────────────── */
export default function TeamPage() {
  const ccUser = JSON.parse(localStorage.getItem('cc_user') || '{}');
  const isAdmin = ccUser.role === 'admin' || ccUser.role === 'super_admin' || !ccUser.id;
  const isSuperAdmin = ccUser.role === 'super_admin' || !ccUser.id;
  const [teamUsers, setTeamUsers] = useState<TeamUser[]>([]);
  const [workers, setWorkers] = useState<Worker[]>([]);
  const [groups, setGroups] = useState<UserGroup[]>([]);
  const [selectedGroupId, setSelectedGroupId] = useState<number | null>(null);
  const [showGroupModal, setShowGroupModal] = useState(false);
  const [editingGroup, setEditingGroup] = useState<UserGroup | null>(null);
  const [showAddMember, setShowAddMember] = useState(false);
  const [loading, setLoading] = useState(true);

  const fetchAll = useCallback(async () => {
    const results = await Promise.allSettled([
      api.getTeamUsers(),
      isAdmin ? api.listWorkers() : Promise.resolve([]),
      api.getTeamGroups(),
    ]);
    if (results[0].status === 'fulfilled') setTeamUsers(results[0].value as TeamUser[]);
    if (results[1].status === 'fulfilled') setWorkers(results[1].value as Worker[]);
    if (results[2].status === 'fulfilled') setGroups(results[2].value as UserGroup[]);
    setLoading(false);
  }, [isAdmin]);

  useEffect(() => {
    let active = true;
    (async () => {
      const results = await Promise.allSettled([
        api.getTeamUsers(),
        isAdmin ? api.listWorkers() : Promise.resolve([]),
        api.getTeamGroups(),
      ]);
      if (!active) return;
      if (results[0].status === 'fulfilled') setTeamUsers(results[0].value as TeamUser[]);
      if (results[1].status === 'fulfilled') setWorkers(results[1].value as Worker[]);
      if (results[2].status === 'fulfilled') setGroups(results[2].value as UserGroup[]);
      setLoading(false);
    })();
    return () => { active = false; };
  }, [isAdmin]);

  const selectedGroup = groups.find(g => g.id === selectedGroupId) ?? null;

  const handleDeleteGroup = async (id: number) => {
    if (!confirm('Delete this group?')) return;
    try {
      await api.deleteTeamGroup(id);
      if (selectedGroupId === id) setSelectedGroupId(null);
      await fetchAll();
    } catch { /* ignore */ }
  };

  const handleRemoveMember = async (userId: number) => {
    if (!selectedGroupId) return;
    try {
      await api.removeTeamGroupMember(selectedGroupId, userId);
      await fetchAll();
    } catch { /* ignore */ }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center py-20">
        <p className="text-gray-400">Loading...</p>
      </div>
    );
  }

  return (
    <div className="p-4 space-y-6">
      {/* ── Users Management (Admin only) ── */}
      {isAdmin && teamUsers.length > 0 && (
        <div>
          <h2 className="text-lg font-semibold text-foreground flex items-center gap-2 mb-3">
            <User size={20} /> Users
          </h2>
          <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
            {teamUsers.map(u => (
              <div key={u.id} className="bg-gray-800 rounded-lg p-3 flex items-center justify-between gap-2">
                <div className="flex items-center gap-2 min-w-0">
                  {u.avatar_url ? (
                    <img src={u.avatar_url} className="w-8 h-8 rounded-full shrink-0" alt="" />
                  ) : (
                    <div className="w-8 h-8 rounded-full bg-gray-600 flex items-center justify-center text-xs text-gray-300 shrink-0">
                      {u.name.charAt(0).toUpperCase()}
                    </div>
                  )}
                  <div className="min-w-0">
                    <div className="text-sm text-foreground truncate">{u.name}</div>
                    <div className="text-xs text-gray-500 truncate">{u.email}</div>
                  </div>
                </div>
                <div className="flex items-center gap-2 shrink-0">
                  <span className={`text-[10px] px-1.5 py-0.5 rounded ${
                    u.role === 'super_admin' ? 'bg-yellow-600/20 text-yellow-400' :
                    u.role === 'admin' ? 'bg-indigo-600/20 text-indigo-400' :
                    'bg-gray-600/20 text-gray-400'
                  }`}>
                    {u.role === 'super_admin' ? 'Super Admin' : u.role === 'admin' ? 'Admin' : 'Member'}
                  </span>
                  {(() => {
                    const count = workers.filter(w => w.owner_user_id === u.id).length;
                    return count > 0 ? (
                      <span className="text-[10px] px-1.5 py-0.5 rounded bg-green-600/20 text-green-400">
                        {count} Worker{count > 1 ? 's' : ''}
                      </span>
                    ) : null;
                  })()}
                  {isSuperAdmin && u.role !== 'super_admin' && (
                    <button
                      onClick={async () => {
                        const newRole = u.role === 'admin' ? 'member' : 'admin';
                        const label = newRole === 'admin' ? '管理员' : '普通用户';
                        if (!confirm(`将 ${u.name} 设为${label}？`)) return;
                        try {
                          await api.updateTeamUserRole(u.id, newRole);
                          fetchAll();
                        } catch { /* role update failed; keep current view */ }
                      }}
                      className="p-1 text-gray-500 hover:text-indigo-400"
                      title={u.role === 'admin' ? '降级为普通用户' : '提升为管理员'}
                    >
                      {u.role === 'admin' ? <Shield size={14} /> : <ShieldCheck size={14} />}
                    </button>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* ── Groups Section ── */}
      <div>
        <div className="flex items-center justify-between mb-3">
          <h2 className="text-lg font-semibold text-foreground flex items-center gap-2">
            <Users size={20} /> Groups
          </h2>
          {isAdmin && (
            <button
              onClick={() => { setEditingGroup(null); setShowGroupModal(true); }}
              className="flex items-center gap-1 px-3 py-1.5 text-sm bg-indigo-600 text-white rounded hover:bg-indigo-500"
            >
              <Plus size={14} /> New Group
            </button>
          )}
        </div>

        {groups.length === 0 ? (
          <div className="text-center py-8 bg-gray-800 rounded-lg text-gray-500">
            <Users size={32} className="mx-auto mb-2 opacity-40" />
            <p className="text-sm">No groups yet.{isAdmin ? ' Click "New Group" to create one.' : ''}</p>
          </div>
        ) : (
          <div className="grid md:grid-cols-3 gap-4">
            {/* Left: Group list */}
            <div className="space-y-1">
              {groups.map(group => (
                <div
                  key={group.id}
                  onClick={() => setSelectedGroupId(group.id)}
                  className={`p-3 rounded-lg cursor-pointer transition-colors group ${
                    selectedGroupId === group.id ? 'bg-indigo-900/30 ring-1 ring-indigo-500/30' : 'bg-gray-800 hover:bg-gray-700'
                  }`}
                >
                  <div className="flex items-center justify-between">
                    <div className="font-medium text-sm text-foreground">{group.name}</div>
                    {isAdmin && (
                      <div className="flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
                        <button
                          onClick={(e) => { e.stopPropagation(); setEditingGroup(group); setShowGroupModal(true); }}
                          className="text-gray-400 hover:text-gray-200 p-1"
                        >
                          <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M17 3a2.85 2.83 0 1 1 4 4L7.5 20.5 2 22l1.5-5.5Z"/></svg>
                        </button>
                        <button
                          onClick={(e) => { e.stopPropagation(); handleDeleteGroup(group.id); }}
                          className="text-gray-400 hover:text-red-400 p-1"
                        >
                          <Trash2 size={14} />
                        </button>
                      </div>
                    )}
                  </div>
                  {group.description && <div className="text-xs text-gray-400 mt-0.5">{group.description}</div>}
                  <div className="text-xs text-gray-500 mt-1">{group.members?.length || 0} members</div>
                </div>
              ))}
            </div>

            {/* Right: Selected group members */}
            <div className="md:col-span-2 bg-gray-800 rounded-lg p-4 min-h-[200px]">
              {selectedGroup ? (
                <>
                  <div className="flex items-center justify-between mb-4">
                    <div>
                      <h3 className="text-foreground font-medium">{selectedGroup.name}</h3>
                      {selectedGroup.description && (
                        <p className="text-xs text-gray-400 mt-0.5">{selectedGroup.description}</p>
                      )}
                    </div>
                    {isAdmin && (
                      <button
                        onClick={() => setShowAddMember(true)}
                        className="flex items-center gap-1 px-3 py-1.5 text-sm bg-indigo-600/20 text-indigo-300 rounded hover:bg-indigo-600/30"
                      >
                        <UserPlus size={14} /> Add Member
                      </button>
                    )}
                  </div>
                  {(!selectedGroup.members || selectedGroup.members.length === 0) ? (
                    <p className="text-gray-500 text-sm">No members in this group yet.</p>
                  ) : (
                    <div className="space-y-1">
                      {selectedGroup.members.map(m => (
                        <div key={m.id} className="flex items-center justify-between p-2 rounded bg-gray-700/50 hover:bg-gray-700 group">
                          <div className="flex items-center gap-2">
                            {m.avatar_url ? (
                              <img src={m.avatar_url} className="w-7 h-7 rounded-full" alt="" />
                            ) : (
                              <div className="w-7 h-7 rounded-full bg-gray-600 flex items-center justify-center text-xs text-gray-300">
                                {m.name.charAt(0).toUpperCase()}
                              </div>
                            )}
                            <div>
                              <span className="text-sm text-foreground">{m.name}</span>
                              <span className="text-xs text-gray-500 ml-2">{m.email}</span>
                            </div>
                          </div>
                          {isAdmin && (
                            <button
                              onClick={() => handleRemoveMember(m.id)}
                              className="text-gray-500 hover:text-red-400 opacity-0 group-hover:opacity-100 transition-opacity p-1"
                            >
                              <X size={14} />
                            </button>
                          )}
                        </div>
                      ))}
                    </div>
                  )}
                </>
              ) : (
                <div className="flex items-center justify-center h-full text-gray-500 text-sm">
                  Select a group to view its members
                </div>
              )}
            </div>
          </div>
        )}
      </div>

      {/* Modals */}
      {showGroupModal && (
        <GroupModal
          group={editingGroup}
          onClose={() => { setShowGroupModal(false); setEditingGroup(null); }}
          onSaved={fetchAll}
        />
      )}
      {showAddMember && selectedGroupId && (
        <AddMemberModal
          groupId={selectedGroupId}
          existingUserIds={new Set((selectedGroup?.members ?? []).map(m => m.id))}
          allUsers={teamUsers}
          onClose={() => setShowAddMember(false)}
          onSaved={fetchAll}
        />
      )}
    </div>
  );
}
