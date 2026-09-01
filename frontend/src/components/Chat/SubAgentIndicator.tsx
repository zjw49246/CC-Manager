import { useState, useEffect, useRef } from 'react';
import { Users } from '../icons';
import { api } from '../../api/client';
import type { SubAgentSummary } from '../../api/client';

interface SubAgentIndicatorProps {
  taskId: number;
  count: number;
  active?: boolean;
  onNavigate?: () => void;
}

/** ChatView 头部的子 agent 小人：常驻显示；点开按类型汇总
 *（和任务卡上的 SubAgentsBadge 一致），没有子 agent 显示 "No sub-agents"。 */
export function SubAgentIndicator({ taskId, count, onNavigate }: SubAgentIndicatorProps) {
  const [expanded, setExpanded] = useState(false);
  const [summary, setSummary] = useState<SubAgentSummary | null>(null);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!expanded) return;
    api.getSubAgentSummary(taskId).then(setSummary).catch(() => setSummary({ by_type: {} }));
    const handleClick = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setExpanded(false);
      }
    };
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, [expanded, taskId]);

  const types = summary ? Object.entries(summary.by_type) : null;

  return (
    <div className="relative inline-block" ref={ref}>
      <button
        onClick={() => setExpanded(!expanded)}
        className="text-xs bg-teal-600/30 text-teal-300 px-1.5 rounded cursor-pointer hover:bg-teal-600/40 flex items-center gap-0.5"
      >
        <Users size={12} />
        {count}
      </button>

      {expanded && (
        <div className="absolute top-full right-0 mt-1 bg-gray-800 border border-gray-600 rounded shadow-lg z-10 min-w-[140px] py-1">
          {types === null ? (
            <div className="px-3 py-1.5 text-xs text-gray-500">…</div>
          ) : types.length > 0 ? (
            types.map(([type, counts]) => (
              <button
                key={type}
                className="flex items-center justify-between w-full gap-3 px-3 py-1.5 text-xs text-gray-300 hover:bg-gray-700 transition-colors"
                onClick={() => { onNavigate?.(); setExpanded(false); }}
              >
                <span>{type.charAt(0).toUpperCase() + type.slice(1)}</span>
                <span className={counts.running > 0 ? 'text-emerald-400 font-medium' : 'text-gray-500'}>
                  {counts.running} running
                </span>
              </button>
            ))
          ) : (
            <div className="px-3 py-1.5 text-xs text-gray-500">No sub-agents</div>
          )}
        </div>
      )}
    </div>
  );
}
