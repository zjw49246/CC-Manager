import { useState, useEffect, useCallback } from 'react';
import { api } from '../api/client';
import type { Project } from '../api/client';
import { Plus, Trash2, Edit2, X, Search, Check } from './icons';

// ── Helpers ───────────────────────────────────────────────────────────────────

function parseEnvContent(content: string): { key: string; value: string; comment: boolean }[] {
  return content.split('\n').map((line) => {
    const trimmed = line.trim();
    if (trimmed.startsWith('#') || trimmed === '') {
      return { key: '', value: line, comment: true };
    }
    const eqIdx = line.indexOf('=');
    if (eqIdx === -1) return { key: '', value: line, comment: true };
    const key = line.slice(0, eqIdx).trim();
    let value = line.slice(eqIdx + 1).trim();
    // Strip surrounding quotes
    if ((value.startsWith('"') && value.endsWith('"')) ||
        (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1);
    }
    return { key, value, comment: false };
  });
}

function serializeEnvContent(rows: { key: string; value: string; comment: boolean }[]): string {
  return rows.map((r) => {
    if (r.comment) return r.value;
    const needsQuotes = r.value.includes(' ') || r.value.includes('#');
    const val = needsQuotes ? `"${r.value}"` : r.value;
    return `${r.key}=${val}`;
  }).join('\n');
}

// ── File Content Editor ───────────────────────────────────────────────────────

function FileEditor({
  projectId,
  filepath,
  onClose,
}: {
  projectId: number;
  filepath: string;
  onClose: () => void;
}) {
  const [content, setContent] = useState('');
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [mode, setMode] = useState<'form' | 'raw'>('form');
  const [rows, setRows] = useState<{ key: string; value: string; comment: boolean }[]>([]);
  // Track which row's delete button has been clicked once (pending confirmation)
  const [pendingDeleteIdx, setPendingDeleteIdx] = useState<number | null>(null);

  useEffect(() => {
    api.getEnvFileContent(projectId, filepath)
      .then(({ content: c }) => {
        setContent(c);
        setRows(parseEnvContent(c));
        setLoading(false);
      })
      .catch((e) => { setError(String(e)); setLoading(false); });
  }, [projectId, filepath]);

  const syncRawToForm = (raw: string) => {
    setContent(raw);
    setRows(parseEnvContent(raw));
  };

  const syncFormToRaw = (newRows: typeof rows) => {
    setRows(newRows);
    setContent(serializeEnvContent(newRows));
  };

  const handleSave = async () => {
    setSaving(true);
    setError(null);
    const finalContent = mode === 'form' ? serializeEnvContent(rows) : content;
    try {
      await api.updateEnvFileContent(projectId, filepath, finalContent);
      onClose();
    } catch (e) {
      setError(String(e));
    } finally {
      setSaving(false);
    }
  };

  const addRow = () => {
    syncFormToRaw([...rows, { key: '', value: '', comment: false }]);
  };

  const updateRow = (idx: number, field: 'key' | 'value', val: string) => {
    const updated = rows.map((r, i) => i === idx ? { ...r, [field]: val } : r);
    syncFormToRaw(updated);
  };

  const deleteRow = (idx: number) => {
    if (pendingDeleteIdx === idx) {
      syncFormToRaw(rows.filter((_, i) => i !== idx));
      setPendingDeleteIdx(null);
    } else {
      setPendingDeleteIdx(idx);
    }
  };

  return (
    <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-60 p-4">
      <div className="bg-gray-800 rounded-xl shadow-2xl w-full max-w-2xl max-h-[85vh] flex flex-col">
        {/* Header */}
        <div className="flex items-center justify-between px-5 py-4 border-b border-gray-700 flex-shrink-0">
          <div className="min-w-0">
            <h3 className="text-foreground font-semibold">Edit env file</h3>
            <p className="text-xs text-gray-500 font-mono truncate">{filepath}</p>
          </div>
          <div className="flex items-center gap-2 ml-3 flex-shrink-0">
            {/* Mode toggle */}
            <div className="flex rounded overflow-hidden border border-gray-600 text-xs">
              <button
                onClick={() => { setMode('form'); setRows(parseEnvContent(content)); }}
                className={`px-3 py-1.5 transition-colors ${mode === 'form' ? 'bg-indigo-600 text-white' : 'text-gray-400 hover:text-gray-200'}`}
              >
                Form
              </button>
              <button
                onClick={() => { setMode('raw'); setContent(serializeEnvContent(rows)); }}
                className={`px-3 py-1.5 transition-colors ${mode === 'raw' ? 'bg-indigo-600 text-white' : 'text-gray-400 hover:text-gray-200'}`}
              >
                Raw
              </button>
            </div>
            <button onClick={onClose} className="text-gray-400 hover:text-gray-200"><X size={18} /></button>
          </div>
        </div>

        {/* Body */}
        <div className="flex-1 overflow-y-auto p-5">
          {error && <p className="text-red-400 text-sm mb-3">{error}</p>}
          {loading ? (
            <p className="text-gray-400 text-sm">Loading...</p>
          ) : mode === 'raw' ? (
            <textarea
              className="w-full h-72 bg-gray-900 text-green-400 text-sm font-mono rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500 resize-y"
              value={content}
              onChange={(e) => syncRawToForm(e.target.value)}
              spellCheck={false}
              placeholder="KEY=VALUE"
            />
          ) : (
            <div className="space-y-2">
              <p className="text-xs text-gray-500">Comments and blank lines are preserved in raw format.</p>
              {rows.map((row, idx) => {
                if (row.comment) return null;
                const isPendingDelete = pendingDeleteIdx === idx;
                return (
                  <div key={idx} className="flex items-center gap-2">
                    <input
                      className="flex-1 bg-gray-700 text-foreground text-sm font-mono rounded px-3 py-1.5 outline-none focus:ring-1 focus:ring-indigo-500"
                      value={row.key}
                      onChange={(e) => updateRow(idx, 'key', e.target.value)}
                      placeholder="KEY"
                    />
                    <span className="text-gray-500">=</span>
                    <input
                      className="flex-[2] bg-gray-700 text-foreground text-sm font-mono rounded px-3 py-1.5 outline-none focus:ring-1 focus:ring-indigo-500"
                      value={row.value}
                      onChange={(e) => updateRow(idx, 'value', e.target.value)}
                      placeholder="value"
                    />
                    <button
                      onClick={() => deleteRow(idx)}
                      onBlur={() => setPendingDeleteIdx(null)}
                      className={`p-1.5 rounded transition-colors flex-shrink-0 ${
                        isPendingDelete
                          ? 'bg-red-600 text-white hover:bg-red-500'
                          : 'text-gray-500 hover:text-red-400 hover:bg-gray-700'
                      }`}
                      title={isPendingDelete ? 'Click again to confirm delete' : 'Delete row'}
                    >
                      {isPendingDelete ? <Check size={14} /> : <Trash2 size={14} />}
                    </button>
                  </div>
                );
              })}
              <button
                onClick={addRow}
                className="flex items-center gap-1.5 text-xs text-indigo-400 hover:text-indigo-300 mt-1"
              >
                <Plus size={13} /> Add key
              </button>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="flex justify-end gap-2 px-5 py-4 border-t border-gray-700 flex-shrink-0">
          <button onClick={onClose} className="px-4 py-2 text-sm text-gray-300 hover:text-foreground">Cancel</button>
          <button
            onClick={handleSave}
            disabled={saving || loading}
            className="px-4 py-2 text-sm bg-indigo-600 text-white rounded hover:bg-indigo-500 disabled:opacity-50"
          >
            {saving ? 'Saving...' : 'Save'}
          </button>
        </div>
      </div>
    </div>
  );
}

// ── Main EnvFilesEditor ───────────────────────────────────────────────────────

export function EnvFilesEditor({ project, onProjectUpdated }: { project: Project; onProjectUpdated: (p: Project) => void }) {
  const [fileStatuses, setFileStatuses] = useState<{ path: string; exists: boolean }[]>([]);
  const [newPath, setNewPath] = useState('');
  const [editingFile, setEditingFile] = useState<string | null>(null);
  const [scanning, setScanning] = useState(false);
  const [discovered, setDiscovered] = useState<string[]>([]);
  const [selectedDiscovered, setSelectedDiscovered] = useState<Set<string>>(new Set());
  const [error, setError] = useState<string | null>(null);
  // Track which file's remove button is pending confirmation
  const [pendingRemovePath, setPendingRemovePath] = useState<string | null>(null);

  const loadFileStatuses = useCallback(async () => {
    if (!project.local_path) return;
    try {
      const { files } = await api.listEnvFiles(project.id);
      setFileStatuses(files);
    } catch (e) {
      setError(String(e));
    }
  }, [project.id, project.local_path]);

  useEffect(() => {
    loadFileStatuses();
  }, [loadFileStatuses, project.env_files]);

  const saveEnvFiles = async (envFiles: string[]) => {
    try {
      const updated = await api.updateProject(project.id, { env_files: envFiles });
      onProjectUpdated(updated);
    } catch (e) {
      setError(String(e));
    }
  };

  const addPath = async () => {
    const trimmed = newPath.trim();
    if (!trimmed || project.env_files.includes(trimmed)) return;
    setNewPath('');
    await saveEnvFiles([...project.env_files, trimmed]);
  };

  const removePath = async (path: string) => {
    if (pendingRemovePath === path) {
      setPendingRemovePath(null);
      await saveEnvFiles(project.env_files.filter((p) => p !== path));
    } else {
      setPendingRemovePath(path);
    }
  };

  const handleScan = async () => {
    setScanning(true);
    setError(null);
    try {
      const { discovered: found } = await api.scanEnvFiles(project.id);
      setDiscovered(found);
      setSelectedDiscovered(new Set(found));
    } catch (e) {
      setError(String(e));
    } finally {
      setScanning(false);
    }
  };

  const addDiscovered = async () => {
    const toAdd = [...selectedDiscovered].filter((p) => !project.env_files.includes(p));
    if (toAdd.length === 0) return;
    setDiscovered([]);
    setSelectedDiscovered(new Set());
    await saveEnvFiles([...project.env_files, ...toAdd]);
  };

  const toggleDiscovered = (path: string) => {
    setSelectedDiscovered((prev) => {
      const next = new Set(prev);
      if (next.has(path)) next.delete(path);
      else next.add(path);
      return next;
    });
  };

  return (
    <div className="space-y-3">
      {error && <p className="text-red-400 text-xs">{error}</p>}

      {/* File list */}
      {fileStatuses.length > 0 ? (
        <div className="space-y-1">
          {fileStatuses.map(({ path, exists }) => {
            const isPendingRemove = pendingRemovePath === path;
            return (
              <div key={path} className="flex items-center gap-2 bg-gray-700/50 rounded px-3 py-2">
                <span
                  className={`w-1.5 h-1.5 rounded-full flex-shrink-0 ${exists ? 'bg-green-500' : 'bg-yellow-500'}`}
                  title={exists ? 'File exists' : 'File not yet created'}
                />
                <span className="flex-1 text-sm font-mono text-gray-300 truncate" title={path}>{path}</span>
                <button
                  onClick={() => setEditingFile(path)}
                  className="p-1.5 text-gray-400 hover:text-indigo-400 hover:bg-gray-600 rounded transition-colors flex-shrink-0"
                  title="Edit file"
                >
                  <Edit2 size={13} />
                </button>
                <button
                  onClick={() => removePath(path)}
                  onBlur={() => setPendingRemovePath(null)}
                  className={`p-1.5 rounded transition-colors flex-shrink-0 text-xs ${
                    isPendingRemove
                      ? 'bg-red-600 text-white hover:bg-red-500 px-2'
                      : 'text-gray-400 hover:text-red-400 hover:bg-gray-600'
                  }`}
                  title={isPendingRemove ? 'Click again to confirm removal' : 'Remove from list (file stays on disk)'}
                >
                  {isPendingRemove ? 'Confirm' : <Trash2 size={13} />}
                </button>
              </div>
            );
          })}
        </div>
      ) : (
        <p className="text-xs text-gray-500">No env files configured.</p>
      )}

      {/* Add path input */}
      <div className="flex items-center gap-2">
        <input
          className="flex-1 bg-gray-700 text-foreground text-sm font-mono rounded px-3 py-1.5 outline-none focus:ring-1 focus:ring-indigo-500"
          value={newPath}
          onChange={(e) => setNewPath(e.target.value)}
          onKeyDown={(e) => { if (e.key === 'Enter') { e.preventDefault(); addPath(); } }}
          placeholder=".env or config/.env.local"
        />
        <button
          onClick={addPath}
          disabled={!newPath.trim()}
          className="flex items-center gap-1 px-3 py-1.5 text-sm bg-indigo-600 text-white rounded hover:bg-indigo-500 disabled:opacity-50 flex-shrink-0"
        >
          <Plus size={13} /> Add
        </button>
        <button
          onClick={handleScan}
          disabled={scanning || !project.local_path}
          className="flex items-center gap-1 px-3 py-1.5 text-sm text-gray-300 border border-gray-600 rounded hover:bg-gray-700 disabled:opacity-50 flex-shrink-0"
          title="Scan repo for .env files"
        >
          <Search size={13} /> {scanning ? 'Scanning...' : 'Scan repo'}
        </button>
      </div>

      {/* Scan results */}
      {discovered.length > 0 && (
        <div className="border border-gray-600 rounded-lg p-3 space-y-2">
          <p className="text-xs text-gray-400 flex items-center gap-1">
            <Search size={12} /> Found {discovered.length} new file{discovered.length > 1 ? 's' : ''} — select to add:
          </p>
          <div className="space-y-1">
            {discovered.map((path) => (
              <label key={path} className="flex items-center gap-2 cursor-pointer">
                <input
                  type="checkbox"
                  checked={selectedDiscovered.has(path)}
                  onChange={() => toggleDiscovered(path)}
                  className="accent-indigo-500"
                />
                <span className="text-sm font-mono text-gray-300">{path}</span>
              </label>
            ))}
          </div>
          <div className="flex items-center gap-2 pt-1">
            <button
              onClick={addDiscovered}
              disabled={selectedDiscovered.size === 0}
              className="px-3 py-1.5 text-xs bg-indigo-600 text-white rounded hover:bg-indigo-500 disabled:opacity-50"
            >
              Add selected ({selectedDiscovered.size})
            </button>
            <button
              onClick={() => { setDiscovered([]); setSelectedDiscovered(new Set()); }}
              className="px-3 py-1.5 text-xs text-gray-400 hover:text-gray-200"
            >
              Dismiss
            </button>
          </div>
        </div>
      )}

      {discovered.length === 0 && !scanning && project.local_path && fileStatuses.length > 0 && (
        <p className="text-xs text-gray-600">
          Green dot = file exists on disk · Yellow dot = file not yet created (will be created on first save)
        </p>
      )}

      {/* File editor modal */}
      {editingFile && (
        <FileEditor
          projectId={project.id}
          filepath={editingFile}
          onClose={() => { setEditingFile(null); loadFileStatuses(); }}
        />
      )}
    </div>
  );
}
