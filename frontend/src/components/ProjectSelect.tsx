import { useState, useRef, useEffect } from 'react';
import type { Project } from '../api/client';
import { ChevronDown, FolderOpen } from './icons';
import { resolveTagColor } from './TagColors';

function TagBadge({ tag, colorKey }: { tag: string; colorKey?: string }) {
  const c = resolveTagColor(tag, colorKey);
  return (
    <span className={`inline-block px-1.5 py-0 rounded text-[10px] font-medium leading-4 ${c.bg} ${c.text}`}>
      {tag}
    </span>
  );
}

interface ProjectSelectProps {
  projects: Project[];
  value: number | string | undefined;
  onChange: (value: string) => void;
  placeholder?: string;
  extraOptions?: { value: string; label: string }[];
  className?: string;
  showStatus?: boolean;
  tagColorMap?: Record<string, string>;
}

export function ProjectSelect({
  projects,
  value,
  onChange,
  placeholder = 'All Projects',
  extraOptions,
  className = '',
  showStatus = false,
  tagColorMap = {},
}: ProjectSelectProps) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  const selected = projects.find((p) => String(p.id) === String(value));
  const extraSelected = extraOptions?.find((o) => o.value === String(value));
  const displayValue = selected ? selected.name : extraSelected ? extraSelected.label : placeholder;

  return (
    <div ref={ref} className={`relative ${className}`}>
      <button
        type="button"
        onClick={() => setOpen(!open)}
        className={`flex items-center gap-1.5 w-full min-w-[160px] px-2.5 py-1 rounded text-xs font-medium transition-colors text-left border ${
          selected
            ? 'bg-indigo-600/20 text-indigo-300 border-indigo-500/50 hover:bg-indigo-600/30'
            : 'bg-gray-800 text-gray-300 border-gray-600 hover:bg-gray-700'
        }`}
      >
        <FolderOpen size={12} className="shrink-0 opacity-70" />
        <span className="flex-1 flex items-center gap-1.5 min-w-0 truncate">
          <span className="truncate">{displayValue}</span>
          {selected && selected.tags.length > 0 && (
            <span className="flex gap-1 shrink-0">
              {selected.tags.map((t) => <TagBadge key={t} tag={t} colorKey={tagColorMap[t]} />)}
            </span>
          )}
          {showStatus && selected && selected.status !== 'ready' && (
            <span
              className={`${selected.status === 'error' ? 'text-red-400' : 'text-yellow-400'} text-[10px]`}
              title={selected.error_message ?? undefined}
            >
              ({selected.status})
            </span>
          )}
        </span>
        <ChevronDown size={12} className={`shrink-0 transition-transform ${open ? 'rotate-180' : ''}`} />
      </button>

      {open && (
        <div className="absolute z-50 mt-1 w-full min-w-[220px] max-h-60 overflow-auto bg-gray-800 border border-gray-700 rounded-lg shadow-xl">
          <div
            onClick={() => { onChange(''); setOpen(false); }}
            className={`px-3 py-1.5 text-xs cursor-pointer hover:bg-gray-700 transition-colors ${
              !value ? 'text-white bg-gray-700/50' : 'text-gray-400'
            }`}
          >
            {placeholder}
          </div>

          {projects.map((p) => (
            <div
              key={p.id}
              onClick={() => { onChange(String(p.id)); setOpen(false); }}
              title={p.error_message ?? undefined}
              className={`px-3 py-1.5 text-xs cursor-pointer hover:bg-gray-700 transition-colors flex items-center gap-1.5 ${
                String(p.id) === String(value) ? 'text-white bg-gray-700/50' : 'text-gray-300'
              }`}
            >
              <span className="truncate">{p.name}</span>
              {p.tags.length > 0 && (
                <span className="flex gap-1 shrink-0 ml-auto">
                  {p.tags.map((t) => <TagBadge key={t} tag={t} colorKey={tagColorMap[t]} />)}
                </span>
              )}
              {showStatus && p.status !== 'ready' && (
                <span className={`${p.status === 'error' ? 'text-red-400' : 'text-yellow-400'} text-[10px] shrink-0`}>
                  ({p.status})
                </span>
              )}
            </div>
          ))}

          {extraOptions?.map((opt) => (
            <div
              key={opt.value}
              onClick={() => { onChange(opt.value); setOpen(false); }}
              className={`px-3 py-1.5 text-xs cursor-pointer hover:bg-gray-700 transition-colors border-t border-gray-700 ${
                String(value) === opt.value ? 'text-white bg-gray-700/50' : 'text-indigo-400'
              }`}
            >
              {opt.label}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
