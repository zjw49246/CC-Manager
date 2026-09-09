import { useState, useEffect, useCallback } from 'react';
import { api } from '../api/client';
import type { Secret } from '../api/client';
import { Plus, Trash2, Pencil, X, Eye, EyeOff, KeyRound } from '../components/icons';

function SecretModal({ secret, onClose, onSaved }: { secret?: Secret; onClose: () => void; onSaved: () => void }) {
  const [name, setName] = useState(secret?.name ?? '');
  const [content, setContent] = useState(secret?.content ?? '');
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!name.trim() || !content.trim()) return;
    setSubmitting(true);
    setError(null);
    try {
      if (secret) {
        await api.updateSecret(secret.id, { name: name.trim(), content: content.trim() });
      } else {
        await api.createSecret({ name: name.trim(), content: content.trim() });
      }
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
      <div className="bg-gray-800 rounded-xl shadow-2xl w-full max-w-md">
        <div className="flex items-center justify-between px-5 py-4 border-b border-gray-700">
          <h3 className="text-foreground font-semibold">{secret ? 'Edit Secret' : 'New Secret'}</h3>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-200"><X size={18} /></button>
        </div>
        <form onSubmit={handleSubmit} className="p-5 space-y-4">
          {error && <p className="text-red-400 text-sm">{error}</p>}
          <div>
            <label className="block text-xs text-gray-400 mb-1">Name</label>
            <input
              className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
              value={name} onChange={(e) => setName(e.target.value)}
              placeholder="e.g. GitHub Token" required
            />
          </div>
          <div>
            <label className="block text-xs text-gray-400 mb-1">Content</label>
            <textarea
              className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500 h-28 resize-none"
              value={content} onChange={(e) => setContent(e.target.value)}
              placeholder="ghp_xxxxxxxxxxxxxxxxxxxx" required
            />
          </div>
          <div className="flex justify-end gap-2 pt-1">
            <button type="button" onClick={onClose} className="px-4 py-2 text-sm text-gray-300 hover:text-foreground">Cancel</button>
            <button
              type="submit"
              disabled={submitting || !name.trim() || !content.trim()}
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

export function SecretsPage() {
  const [secrets, setSecrets] = useState<Secret[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [showModal, setShowModal] = useState(false);
  const [editing, setEditing] = useState<Secret | undefined>(undefined);
  const [revealed, setRevealed] = useState<Record<number, boolean>>({});

  const refresh = useCallback(async () => {
    try {
      setSecrets(await api.listSecrets());
      setError(null);
    } catch (e) {
      setError(String(e));
    }
  }, []);

  useEffect(() => {
    let active = true;
    (async () => {
      try {
        const list = await api.listSecrets();
        if (active) {
          setSecrets(list);
          setError(null);
        }
      } catch (e) {
        if (active) setError(String(e));
      }
    })();
    return () => { active = false; };
  }, []);

  const handleDelete = async (id: number) => {
    if (!confirm('Delete this secret?')) return;
    try {
      await api.deleteSecret(id);
      await refresh();
    } catch (e) {
      setError(String(e));
    }
  };

  const toggleReveal = (id: number) => {
    setRevealed((prev) => ({ ...prev, [id]: !prev[id] }));
  };

  const maskContent = (content: string) => {
    if (content.length <= 4) return '****';
    return content.slice(0, 2) + '*'.repeat(Math.min(content.length - 4, 20)) + content.slice(-2);
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-foreground font-semibold text-lg">Secrets</h2>
        <button
          onClick={() => { setEditing(undefined); setShowModal(true); }}
          className="flex items-center gap-1.5 px-3 py-1.5 text-sm bg-indigo-600 text-white rounded hover:bg-indigo-500"
        >
          <Plus size={14} /> New Secret
        </button>
      </div>

      <p className="text-xs text-gray-500">
        Store sensitive information (accounts, tokens, etc.) that can be injected into task prompts. Secrets are never stored in the git repo.
      </p>

      {error && (
        <div className="bg-red-500/20 text-red-400 px-4 py-2 rounded text-sm">Error: {error}</div>
      )}

      {secrets.length === 0 ? (
        <p className="text-gray-400 text-sm">No secrets yet.</p>
      ) : (
        <div className="space-y-2">
          {secrets.map((s) => (
            <div key={s.id} className="bg-gray-800 rounded-lg p-4 flex items-center gap-4">
              <KeyRound size={18} className="text-gray-400 shrink-0" />
              <div className="flex-1 min-w-0">
                <p className="text-foreground font-medium text-sm">{s.name}</p>
                <p className="text-xs text-gray-500 font-mono truncate">
                  {revealed[s.id] ? s.content : maskContent(s.content)}
                </p>
              </div>
              <div className="flex items-center gap-2 shrink-0">
                <button
                  onClick={() => toggleReveal(s.id)}
                  className="p-2 text-gray-400 hover:text-gray-200 hover:bg-gray-700 rounded"
                  title={revealed[s.id] ? 'Hide' : 'Show'}
                >
                  {revealed[s.id] ? <EyeOff size={16} /> : <Eye size={16} />}
                </button>
                <button
                  onClick={() => { setEditing(s); setShowModal(true); }}
                  className="p-2 text-gray-400 hover:text-indigo-400 hover:bg-gray-700 rounded"
                  title="Edit"
                >
                  <Pencil size={16} />
                </button>
                <button
                  onClick={() => handleDelete(s.id)}
                  className="p-2 text-gray-400 hover:text-red-400 hover:bg-gray-700 rounded"
                  title="Delete"
                >
                  <Trash2 size={16} />
                </button>
              </div>
            </div>
          ))}
        </div>
      )}

      {showModal && (
        <SecretModal
          secret={editing}
          onClose={() => setShowModal(false)}
          onSaved={refresh}
        />
      )}
    </div>
  );
}
