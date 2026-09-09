import { useState, useEffect, useRef, useCallback } from 'react';
import { api } from '../../api/client';
import type { QuickPhrase } from '../../api/client';
import { Zap, Plus, Trash2, Pencil, Check, X, GripVertical } from '../icons';

interface QuickPhraseDropdownProps {
  onSelect: (content: string) => void;
  disabled?: boolean;
}

export function QuickPhraseDropdown({ onSelect, disabled }: QuickPhraseDropdownProps) {
  const [open, setOpen] = useState(false);
  const [phrases, setPhrases] = useState<QuickPhrase[]>([]);
  const [editing, setEditing] = useState<number | null>(null);
  const [adding, setAdding] = useState(false);
  const [editLabel, setEditLabel] = useState('');
  const [editContent, setEditContent] = useState('');
  const ref = useRef<HTMLDivElement>(null);

  const load = useCallback(async () => {
    try {
      const data = await api.listQuickPhrases();
      setPhrases(data);
    } catch (e) {
      console.error('Failed to load quick phrases:', e);
    }
  }, []);

  useEffect(() => {
    let active = true;
    (async () => {
      try {
        const data = await api.listQuickPhrases();
        if (active) setPhrases(data);
      } catch (e) {
        if (active) console.error('Failed to load quick phrases:', e);
      }
    })();
    return () => { active = false; };
  }, []);

  useEffect(() => {
    if (!open) return;
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setOpen(false);
        setEditing(null);
        setAdding(false);
      }
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [open]);

  const handleSelect = (phrase: QuickPhrase) => {
    if (editing !== null) return;
    onSelect(phrase.content);
    setOpen(false);
  };

  const handleAdd = async () => {
    if (!editLabel.trim() || !editContent.trim()) return;
    try {
      await api.createQuickPhrase({
        label: editLabel.trim(),
        content: editContent.trim(),
        sort_order: phrases.length,
      });
      setAdding(false);
      setEditLabel('');
      setEditContent('');
      load();
    } catch (e) {
      console.error('Failed to create quick phrase:', e);
    }
  };

  const handleUpdate = async (id: number) => {
    if (!editLabel.trim() || !editContent.trim()) return;
    try {
      await api.updateQuickPhrase(id, {
        label: editLabel.trim(),
        content: editContent.trim(),
      });
      setEditing(null);
      setEditLabel('');
      setEditContent('');
      load();
    } catch (e) {
      console.error('Failed to update quick phrase:', e);
    }
  };

  const handleDelete = async (id: number, e: React.MouseEvent) => {
    e.stopPropagation();
    try {
      await api.deleteQuickPhrase(id);
      load();
    } catch (err) {
      console.error('Failed to delete quick phrase:', err);
    }
  };

  const startEdit = (phrase: QuickPhrase, e: React.MouseEvent) => {
    e.stopPropagation();
    setEditing(phrase.id);
    setEditLabel(phrase.label);
    setEditContent(phrase.content);
  };

  const cancelEdit = () => {
    setEditing(null);
    setAdding(false);
    setEditLabel('');
    setEditContent('');
  };

  return (
    <div className="relative" ref={ref}>
      <button
        type="button"
        onClick={() => setOpen(!open)}
        disabled={disabled}
        className="p-2.5 text-gray-500 hover:text-amber-400 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
        title="常用语"
      >
        <Zap size={18} />
      </button>

      {open && (
        <div className="absolute bottom-full left-0 mb-2 w-80 bg-gray-800 border border-gray-700 rounded-lg shadow-xl z-50 overflow-hidden">
          <div className="flex items-center justify-between px-3 py-2 border-b border-gray-700">
            <span className="text-xs font-medium text-gray-400">常用语</span>
            <button
              onClick={() => {
                setAdding(true);
                setEditLabel('');
                setEditContent('');
              }}
              className="flex items-center gap-1 text-xs text-gray-500 hover:text-emerald-400 transition-colors"
            >
              <Plus size={12} />
              <span>添加</span>
            </button>
          </div>

          <div className="max-h-64 overflow-y-auto">
            {phrases.length === 0 && !adding && (
              <p className="text-xs text-gray-600 px-3 py-4 text-center">暂无常用语，点击上方「添加」创建</p>
            )}

            {phrases.map((phrase) => (
              <div key={phrase.id}>
                {editing === phrase.id ? (
                  <div className="px-3 py-2 space-y-1.5 bg-gray-750">
                    <input
                      type="text"
                      value={editLabel}
                      onChange={(e) => setEditLabel(e.target.value)}
                      placeholder="标题"
                      className="w-full bg-gray-700 text-foreground rounded px-2 py-1 text-xs focus:outline-none focus:ring-1 focus:ring-indigo-500"
                      autoFocus
                    />
                    <textarea
                      value={editContent}
                      onChange={(e) => setEditContent(e.target.value)}
                      placeholder="内容"
                      rows={2}
                      className="w-full bg-gray-700 text-foreground rounded px-2 py-1 text-xs focus:outline-none focus:ring-1 focus:ring-indigo-500 resize-none"
                    />
                    <div className="flex justify-end gap-1">
                      <button onClick={cancelEdit} className="p-1 text-gray-500 hover:text-gray-300"><X size={14} /></button>
                      <button onClick={() => handleUpdate(phrase.id)} className="p-1 text-emerald-500 hover:text-emerald-400"><Check size={14} /></button>
                    </div>
                  </div>
                ) : (
                  <button
                    onClick={() => handleSelect(phrase)}
                    className="w-full text-left px-3 py-2 hover:bg-gray-700/50 transition-colors group flex items-center gap-2"
                  >
                    <GripVertical size={12} className="text-gray-700 shrink-0" />
                    <div className="flex-1 min-w-0">
                      <div className="text-sm text-foreground truncate">{phrase.label}</div>
                      <div className="text-xs text-gray-500 truncate">{phrase.content}</div>
                    </div>
                    <div className="flex items-center gap-0.5 opacity-0 group-hover:opacity-100 transition-opacity shrink-0">
                      <button
                        onClick={(e) => startEdit(phrase, e)}
                        className="p-1 text-gray-500 hover:text-indigo-400"
                      >
                        <Pencil size={12} />
                      </button>
                      <button
                        onClick={(e) => handleDelete(phrase.id, e)}
                        className="p-1 text-gray-500 hover:text-red-400"
                      >
                        <Trash2 size={12} />
                      </button>
                    </div>
                  </button>
                )}
              </div>
            ))}

            {adding && (
              <div className="px-3 py-2 space-y-1.5 bg-gray-750 border-t border-gray-700">
                <input
                  type="text"
                  value={editLabel}
                  onChange={(e) => setEditLabel(e.target.value)}
                  placeholder="标题（如：继续推进）"
                  className="w-full bg-gray-700 text-foreground rounded px-2 py-1 text-xs focus:outline-none focus:ring-1 focus:ring-indigo-500"
                  autoFocus
                />
                <textarea
                  value={editContent}
                  onChange={(e) => setEditContent(e.target.value)}
                  placeholder="发送内容"
                  rows={2}
                  className="w-full bg-gray-700 text-foreground rounded px-2 py-1 text-xs focus:outline-none focus:ring-1 focus:ring-indigo-500 resize-none"
                />
                <div className="flex justify-end gap-1">
                  <button onClick={cancelEdit} className="p-1 text-gray-500 hover:text-gray-300"><X size={14} /></button>
                  <button onClick={handleAdd} className="p-1 text-emerald-500 hover:text-emerald-400"><Check size={14} /></button>
                </div>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
