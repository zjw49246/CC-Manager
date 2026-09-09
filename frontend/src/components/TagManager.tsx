import { useState, useEffect, useRef } from 'react';
import { api } from '../api/client';
import type { TagItem } from '../api/client';
import { X, Pencil, Trash2, Check, Plus } from './icons';
import { TAG_COLOR_OPTIONS, resolveTagColor } from './TagColors';

interface TagManagerProps {
  onClose: () => void;
  onChanged: () => void;
}

export function TagManager({ onClose, onChanged }: TagManagerProps) {
  const [tags, setTags] = useState<TagItem[]>([]);
  const [editingId, setEditingId] = useState<number | null>(null);
  const [editName, setEditName] = useState('');
  const [editColor, setEditColor] = useState('');
  const [newName, setNewName] = useState('');
  const [newColor, setNewColor] = useState('indigo');
  const [error, setError] = useState<string | null>(null);

  const refresh = async () => {
    try {
      const list = await api.listTags();
      setTags(list);
    } catch (e) {
      setError(String(e));
    }
  };

  useEffect(() => {
    let active = true;
    (async () => {
      try {
        const list = await api.listTags();
        if (active) setTags(list);
      } catch (e) {
        if (active) setError(String(e));
      }
    })();
    return () => { active = false; };
  }, []);

  const startEdit = (tag: TagItem) => {
    setEditingId(tag.id);
    setEditName(tag.name);
    setEditColor(tag.color);
  };

  const saveEdit = async () => {
    if (!editingId || !editName.trim()) return;
    setError(null);
    try {
      await api.updateTag(editingId, { name: editName.trim(), color: editColor });
      setEditingId(null);
      await refresh();
      onChanged();
    } catch (e) {
      setError(String(e));
    }
  };

  const handleCreate = async () => {
    if (!newName.trim()) return;
    setError(null);
    try {
      await api.createTag({ name: newName.trim().toLowerCase(), color: newColor });
      setNewName('');
      setNewColor('indigo');
      await refresh();
      onChanged();
    } catch (e) {
      setError(String(e));
    }
  };

  const handleDelete = async (id: number) => {
    setError(null);
    try {
      await api.deleteTag(id);
      await refresh();
      onChanged();
    } catch (e) {
      setError(String(e));
    }
  };

  return (
    <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50 p-4">
      <div className="bg-gray-800 rounded-xl shadow-2xl w-full max-w-lg max-h-[90vh] overflow-y-auto">
        <div className="flex items-center justify-between px-5 py-4 border-b border-gray-700">
          <h3 className="text-foreground font-semibold">Manage Tags</h3>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-200"><X size={18} /></button>
        </div>

        <div className="p-5 space-y-4">
          {error && <p className="text-red-400 text-sm">{error}</p>}

          {/* Create new tag */}
          <div className="flex items-center gap-2">
            <input
              value={newName}
              onChange={(e) => setNewName(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && handleCreate()}
              placeholder="New tag name..."
              className="flex-1 bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
            />
            <ColorPicker value={newColor} onChange={setNewColor} />
            <button
              onClick={handleCreate}
              disabled={!newName.trim()}
              className="flex items-center gap-1 px-3 py-2 text-sm bg-indigo-600 text-white rounded hover:bg-indigo-500 disabled:opacity-50"
            >
              <Plus size={14} /> Add
            </button>
          </div>

          {/* Tag list */}
          {tags.length === 0 ? (
            <p className="text-gray-500 text-sm text-center py-4">No tags yet. Tags are created when you add them to projects.</p>
          ) : (
            <div className="space-y-1">
              {tags.map((tag) => {
                const isEditing = editingId === tag.id;
                const color = resolveTagColor(tag.name, tag.color);
                return (
                  <div key={tag.id} className="flex items-center gap-2 px-3 py-2 rounded hover:bg-gray-700/50 group">
                    {isEditing ? (
                      <>
                        <input
                          value={editName}
                          onChange={(e) => setEditName(e.target.value)}
                          onKeyDown={(e) => e.key === 'Enter' && saveEdit()}
                          className="flex-1 bg-gray-700 text-foreground text-sm rounded px-2 py-1 outline-none focus:ring-1 focus:ring-indigo-500"
                          autoFocus
                        />
                        <ColorPicker value={editColor} onChange={setEditColor} />
                        <button onClick={saveEdit} className="p-1 text-green-400 hover:text-green-300">
                          <Check size={16} />
                        </button>
                        <button onClick={() => setEditingId(null)} className="p-1 text-gray-400 hover:text-gray-200">
                          <X size={16} />
                        </button>
                      </>
                    ) : (
                      <>
                        <span className={`inline-flex items-center gap-1.5 px-2 py-0.5 rounded text-xs border ${color.bg} ${color.text} ${color.border}`}>
                          {tag.name}
                        </span>
                        <span className="flex-1" />
                        <button
                          onClick={() => startEdit(tag)}
                          className="p-1 text-gray-500 hover:text-gray-300 opacity-0 group-hover:opacity-100 transition-opacity"
                        >
                          <Pencil size={14} />
                        </button>
                        <button
                          onClick={() => handleDelete(tag.id)}
                          className="p-1 text-gray-500 hover:text-red-400 opacity-0 group-hover:opacity-100 transition-opacity"
                        >
                          <Trash2 size={14} />
                        </button>
                      </>
                    )}
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

function ColorPicker({ value, onChange }: { value: string; onChange: (v: string) => void }) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  const current = TAG_COLOR_OPTIONS.find((c) => c.key === value) || TAG_COLOR_OPTIONS[0];

  useEffect(() => {
    if (!open) return;
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [open]);

  return (
    <div className="relative" ref={ref}>
      <button
        type="button"
        onClick={() => setOpen(!open)}
        className="w-8 h-8 rounded-lg border-2 border-gray-600 bg-gray-800 flex items-center justify-center hover:border-gray-500"
        title={current.label}
      >
        <span className={`w-4 h-4 rounded-full ${current.dot}`} />
      </button>
      {open && (
        <div className="absolute z-50 top-full right-0 mt-1 p-2.5 bg-gray-800 border border-gray-600 rounded-lg shadow-xl flex gap-2 flex-wrap" style={{ width: '180px' }}>
          {TAG_COLOR_OPTIONS.map((c) => (
            <button
              key={c.key}
              type="button"
              onClick={() => { onChange(c.key); setOpen(false); }}
              className={`w-8 h-8 rounded-full flex items-center justify-center transition-transform hover:scale-110 ${
                value === c.key ? 'ring-2 ring-white ring-offset-2 ring-offset-gray-800' : ''
              }`}
              title={c.label}
            >
              <span className={`w-6 h-6 rounded-full ${c.dot}`} />
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
