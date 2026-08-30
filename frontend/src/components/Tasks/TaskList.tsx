import { useState, useMemo, useRef, useEffect, useCallback } from 'react';

import { api } from '../../api/client';
import type { PRReviewResult, Task, Project } from '../../api/client';
import { Trash2, RotateCcw, XCircle, MessageCircle, Archive, ArchiveRestore, Star, Copy, Check, MoreVertical, Pencil, Mail, MailOpen, Clock, GripVertical, UserPlus, Pin, GitPullRequest } from '../icons';
import { FastModeBadge, PlanPipelineBadge, PlanRevisionBadge, PluginsBadge, SubAgentsBadge, TaskConfigBadge } from './TaskBadges';
import { AttentionTag } from './AttentionTag';
import { TAG_COLOR_OPTIONS } from '../TagColors';
import { ExpandableText } from '../ExpandableText';
import { formatDateTime } from '../../config/timezone';
import { useTaskReorder } from '../../hooks/useTaskReorder';
import { getTaskStatusLabel } from './taskStatus';
import { DeliveryRunPanel } from './DeliveryRunPanel';
import {
  canControlTask,
  canManageTaskShare,
  readStoredUserIdentity,
  taskHasSession,
} from './taskSharePermissions';
import { isPRMonitorDisplayTask } from './prMonitorTask';
import { PRMonitorTaskSummary } from './PRMonitorTaskSummary';

export interface TaskListProps {
  tasks: Task[];
  projects: Project[];
  onRefresh: () => void;
  onTaskUpdated?: (task: Task) => void;
  onOpenChat: (task: Task) => void;
  activeTaskId?: number | null;
  autoSortOnAccess?: boolean;
  onBeforeArchive?: () => void;
  onReorder?: (tasks: Task[]) => void;
  /** Safe aggregate PR results keyed by the durable display Task id. */
  prResults?: ReadonlyMap<number, PRReviewResult>;
}

const statusColors: Record<string, string> = {
  pending: 'bg-yellow-500',
  in_progress: 'bg-blue-500',
  executing: 'bg-blue-400 animate-pulse',
  waiting_capability: 'bg-violet-400 animate-pulse',
  background: 'bg-teal-400 animate-pulse',
  delivery_waiting: 'bg-indigo-400',
  plan_review: 'bg-purple-500',
  superseded: 'bg-gray-500',
  completed: 'bg-green-500',
  failed: 'bg-red-500',
  cancelled: 'bg-gray-500',
};

// Keep this aligned with backend/api/tasks.py::_MANUAL_RETRYABLE_STATUSES.
// Active background output and controller-owned task modes have separate
// lifecycle controls and cannot be retried through the public Task action.
const manualRetryableStatuses = new Set(['failed', 'cancelled', 'conflict', 'completed']);

function canRetryTask(task: Task): boolean {
  return !task.background_active
    && task.mode !== 'plan'
    && task.mode !== 'delivery_loop'
    && task.delivery_run_id == null
    && manualRetryableStatuses.has(task.status);
}

export function TaskList({ tasks, projects, onRefresh, onTaskUpdated, onOpenChat, activeTaskId, autoSortOnAccess, onBeforeArchive, onReorder, prResults }: TaskListProps) {
  const currentUser = readStoredUserIdentity();
  const projectMap = useMemo(() => {
    const map: Record<number, { name: string; color: string | null }> = {};
    for (const p of projects) map[p.id] = { name: p.name, color: p.badge_color };
    return map;
  }, [projects]);

  const [copiedId, setCopiedId] = useState<number | null>(null);
  const [menuOpenId, setMenuOpenId] = useState<number | null>(null);
  const [editingTitleId, setEditingTitleId] = useState<number | null>(null);
  const [editingAttentionTagId, setEditingAttentionTagId] = useState<number | null>(null);
  const [openDeliveryRunId, setOpenDeliveryRunId] = useState<number | null>(null);
  const [titleDraft, setTitleDraft] = useState('');
  const menuRef = useRef<HTMLDivElement>(null);
  const titleInputRef = useRef<HTMLInputElement>(null);

  // Close overflow menu on outside click
  useEffect(() => {
    if (menuOpenId === null) return;
    const handleClick = (e: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(e.target as Node)) {
        setMenuOpenId(null);
      }
    };
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, [menuOpenId]);

  // Auto-focus title input
  useEffect(() => {
    if (editingTitleId !== null) titleInputRef.current?.focus();
  }, [editingTitleId]);

  const handleDelete = async (id: number) => {
    await api.deleteTask(id);
    onRefresh();
  };
  const handleCancel = async (task: Task) => {
    if (task.background_active) {
      await api.stopTaskSession(task.id);
    } else {
      await api.cancelTask(task.id);
    }
    onRefresh();
  };
  const handleRetry = async (task: Task) => {
    try {
      await api.retryTask(task.id, {
        provider: task.provider,
        model: task.model,
        codex_service_tier: task.codex_service_tier,
      });
    } finally {
      // A stale routing expectation returns 409; refresh the Fast/Standard
      // badge even though the action itself was intentionally rejected.
      onRefresh();
    }
  };
  const handleStar = async (id: number) => {
    await api.starTask(id);
    onRefresh();
  };
  const handleToggleUnread = async (id: number, currentlyUnread: boolean) => {
    if (currentlyUnread) {
      await api.markTaskRead(id);
    } else {
      await api.markTaskUnread(id);
    }
    onRefresh();
  };
  const handleArchive = async (id: number) => {
    await api.archiveTask(id);
    onBeforeArchive?.();
    onRefresh();
  };

  const handleCopy = async (t: Task) => {
    try {
      await navigator.clipboard.writeText(t.description || '');
      setCopiedId(t.id);
      setTimeout(() => setCopiedId(null), 2000);
    } catch { /* clipboard may fail in insecure context */ }
  };

  const handleTitleSave = async (t: Task) => {
    const trimmed = titleDraft.trim();
    setEditingTitleId(null);
    if (trimmed === (t.title || '')) return;
    try {
      await api.updateTask(t.id, { title: trimmed });
      onRefresh();
    } catch { /* ignore */ }
  };

  const handleAttentionTagSaved = (updated: Task) => {
    setEditingAttentionTagId(null);
    if (onTaskUpdated) onTaskUpdated(updated);
    else onRefresh();
  };

  // 拖拽排序（长按/拖动；标星置顶保留，仅同组内移动）
  const handleReordered = useCallback((optimistic?: Task[]) => {
    if (optimistic) {
      onReorder?.(optimistic);
      return;
    }
    onRefresh();
  }, [onRefresh, onReorder]);
  const {
    draggingId,
    overIndex,
    dropTargetProps,
    targetProps,
    pointerHandleProps,
    ghost,
  } = useTaskReorder(tasks, handleReordered, autoSortOnAccess);

  if (tasks.length === 0) {
    return <p className="text-gray-500 text-sm text-center py-8">No tasks yet</p>;
  }

  return (
    <>
    <div className="space-y-2">
      {ghost}
      {tasks.map((t, idx) => (
        // PR Monitor display Tasks are durable read-only projections. Their
        // reviewer execution Tasks remain hidden and never enter this list.
        (() => {
        const prDisplay = isPRMonitorDisplayTask(t);
        const prResult = prResults?.get(t.id);
        return (
        <div
          key={t.id}
          {...(prDisplay
            ? {}
            : canControlTask(t) ? targetProps(t, idx) : dropTargetProps(t, idx))}
          className={`relative rounded-xl p-3 border transition-[opacity,border-color,box-shadow] ${
            draggingId === t.id ? 'opacity-40' : ''
          } ${overIndex === idx && draggingId !== null && draggingId !== t.id ? 'ring-2 ring-indigo-400' : ''} ${
            activeTaskId === t.id
              ? 'bg-indigo-900/60 border-indigo-500/40 ring-1 ring-indigo-400/40'
              : t.has_unread
                ? 'bg-indigo-900/50 border-indigo-500/30 ring-1 ring-indigo-500/30'
                : 'bg-gray-800 border-gray-700/60 hover:border-gray-600/70 shadow-sm'
          }`}
        >
          {/* 拖拽手柄（右下角，空间更宽裕）：卡片正文是可选中文字，整卡
              draggable 会被文本选择手势抢走，必须用显式手柄拖动 */}
          {canControlTask(t) && !prDisplay && t.mode !== 'delivery_loop' && (
            <span
              {...pointerHandleProps(t, idx)}
              className="absolute bottom-1.5 right-1.5 p-1 cursor-grab active:cursor-grabbing text-gray-600 hover:text-gray-400 select-none"
              title="按住拖动排序"
            >
              <GripVertical size={16} />
            </span>
          )}
          {/* Row 1: status dot + badges (left, wraps) | action buttons (right, no wrap) */}
          <div className="flex items-center gap-2">
            <span className={`w-2.5 h-2.5 rounded-full shrink-0 self-start mt-[9px] ${statusColors[
              t.background_active
                ? 'background'
                : t.mode === 'delivery_loop' && t.delivery_activity === 'running'
                  ? 'executing'
                  : t.mode === 'delivery_loop' && t.delivery_activity === 'waiting'
                    ? 'delivery_waiting'
                    : t.mode === 'delivery_loop' && t.delivery_activity === 'paused'
                      ? 'plan_review'
                      : t.status
            ] || 'bg-gray-500'}`} />
            <div className="flex items-center gap-2 flex-wrap flex-1 min-w-0 min-h-[28px]">
              <span className="text-xs text-gray-500">#{t.id}</span>
              {t.mode === 'delivery_loop' && t.attention_tag ? (
                <span
                  className="inline-flex min-w-0 max-w-[min(16rem,55vw)] items-center gap-1 rounded-md border border-amber-400/25 bg-amber-500/15 px-1.5 py-0.5 text-xs font-medium text-amber-300"
                  title="Delivery-owned Task attention tag"
                >
                  <Pin size={11} className="shrink-0" />
                  <span className="truncate">{t.attention_tag}</span>
                </span>
              ) : !canControlTask(t) && t.attention_tag ? (
                <span className="inline-flex min-w-0 max-w-[min(16rem,55vw)] items-center gap-1 rounded-md border border-amber-400/25 bg-amber-500/15 px-1.5 py-0.5 text-xs font-medium text-amber-300">
                  <Pin size={11} className="shrink-0" />
                  <span className="truncate">{t.attention_tag}</span>
                </span>
              ) : canControlTask(t) && !prDisplay && editingAttentionTagId !== t.id && (
                <AttentionTag
                  taskId={t.id}
                  value={t.attention_tag}
                  editing={false}
                  onEdit={() => setEditingAttentionTagId(t.id)}
                  onCancel={() => setEditingAttentionTagId(null)}
                  onSaved={handleAttentionTagSaved}
                  className="max-w-[min(16rem,55vw)]"
                />
              )}
              {prDisplay ? null : t.access_scope === 'chat' ? (
                <span className="text-xs bg-orange-600/30 text-orange-300 px-1.5 rounded font-medium">Shared · Chat</span>
              ) : !canControlTask(t) ? (
                <span className="text-xs bg-orange-600/30 text-orange-300 px-1.5 rounded font-medium">Restricted</span>
              ) : t.shared_from_id ? (
                <span className="text-xs bg-orange-600/30 text-orange-300 px-1.5 rounded font-medium">Shared</span>
              ) : null}
              {t.project_id && projectMap[t.project_id] && (() => {
                const proj = projectMap[t.project_id!];
                const colorDef = TAG_COLOR_OPTIONS.find((c) => c.key === proj.color);
                const bg = colorDef ? colorDef.bg : 'bg-emerald-600/30';
                const text = colorDef ? colorDef.text : 'text-emerald-300';
                return <span className={`text-xs ${bg} ${text} px-1.5 rounded font-medium whitespace-nowrap`}>{proj.name}</span>;
              })()}
              {t.priority > 0 && (
                <span className="text-xs bg-indigo-600/30 text-indigo-300 px-1.5 rounded">P{t.priority}</span>
              )}
              <span className={`text-xs text-gray-500 ${
                (t.mode === 'plan' && ['in_progress', 'executing'].includes(t.status))
                  || t.mode === 'delivery_loop'
                  ? ''
                  : 'hidden sm:inline'
              }`}>
                {getTaskStatusLabel(t)}
              </span>
              {prDisplay && (
                <span className="inline-flex items-center gap-1 rounded bg-indigo-600/20 px-1.5 py-0.5 text-[10px] font-medium text-indigo-300">
                  <GitPullRequest size={11} aria-hidden="true" /> PR review
                </span>
              )}
              {canControlTask(t) && t.mode === 'delivery_loop' && t.delivery_run_id != null && (
                <button
                  type="button"
                  onClick={() => setOpenDeliveryRunId((current) => (
                    current === t.delivery_run_id ? null : t.delivery_run_id!
                  ))}
                  aria-expanded={openDeliveryRunId === t.delivery_run_id}
                  className="rounded bg-indigo-600/20 px-1.5 text-[10px] font-medium text-indigo-300 hover:bg-indigo-600/30"
                  title={`Delivery Run #${t.delivery_run_id}`}
                >
                  DLV-{t.delivery_run_id}
                </button>
              )}
              {t.mode === 'plan' ? (
                <>
                  {t.canonical_plan_id != null && (
                    <button
                      type="button"
                      onClick={() => { window.location.hash = `#/plans/${t.canonical_plan_id}`; }}
                      className="rounded bg-teal-600/20 px-1.5 text-[10px] font-medium text-teal-300 hover:bg-teal-600/30"
                      title={`This historical Task has migrated to Plan #${t.canonical_plan_id}`}
                    >
                      Plan #{t.canonical_plan_id}
                    </button>
                  )}
                  <PlanPipelineBadge task={t} />
                  <PlanRevisionBadge task={t} />
                </>
              ) : prDisplay ? null : (
                <>
                  <span className={`hidden sm:inline text-xs px-1.5 rounded font-medium ${t.provider === 'codex' ? 'bg-green-600/30 text-green-300' : 'bg-blue-600/30 text-blue-300'}`}>
                    {t.provider === 'codex' ? 'Codex' : 'Claude'}
                  </span>
                  <FastModeBadge task={t} />
                  {canControlTask(t) && !prDisplay && t.mode !== 'delivery_loop' && (
                    <>
                      <TaskConfigBadge task={t} onRefresh={onRefresh} />
                      <PluginsBadge task={t} onRefresh={onRefresh} />
                    </>
                  )}
                  <SubAgentsBadge task={t} />
                </>
              )}
            </div>
            {/* Action buttons — always top-right aligned */}
            <div className="flex gap-1 shrink-0 items-center">
              {!prDisplay && canControlTask(t) && <button
                  onClick={() => handleStar(t.id)}
                  className={`p-1.5 transition-colors ${t.starred ? 'text-yellow-400 hover:text-yellow-300' : 'text-gray-600 hover:text-yellow-400'}`}
                  title={t.starred ? "Unstar" : "Star"}
                >
                  <Star size={16} fill={t.starred ? 'currentColor' : 'none'} />
                </button>}
              {!prDisplay && canControlTask(t) && <button
                  onClick={() => handleToggleUnread(t.id, t.has_unread)}
                  className={`p-1.5 transition-colors ${t.has_unread ? 'text-indigo-400 hover:text-indigo-300' : 'text-gray-600 hover:text-indigo-400'}`}
                  title={t.has_unread ? "Mark as read" : "Mark as unread"}
                >
                  {t.has_unread ? <MailOpen size={16} /> : <Mail size={16} />}
                </button>}
              {prDisplay ? (
                <button
                  type="button"
                  onClick={() => onOpenChat(t)}
                  className="flex items-center gap-1 rounded bg-indigo-600/20 px-2 py-1 text-xs font-medium text-indigo-300 hover:bg-indigo-600/30"
                  title="View PR review result"
                >
                  <GitPullRequest size={14} /><span className="hidden sm:inline"> View result</span>
                </button>
              ) : taskHasSession(t) && (
                <button
                  onClick={() => onOpenChat(t)}
                  className="flex items-center gap-1 px-2 py-1 rounded text-xs font-medium bg-indigo-600/20 text-indigo-400 hover:bg-indigo-600/30"
                  title="Chat"
                >
                  <MessageCircle size={14} /><span className="hidden sm:inline"> Chat</span>
                </button>
              )}
              {!prDisplay && <button
                onClick={() => handleCopy(t)}
                className="p-1.5 text-gray-600 hover:text-gray-300 transition-colors"
                title="Copy prompt"
              >
                {copiedId === t.id ? <Check size={16} className="text-green-400" /> : <Copy size={16} />}
              </button>}
              {/* Controller-owned Delivery Tasks are read-only scheduler shells. */}
              {!prDisplay && canControlTask(t) && t.mode !== 'delivery_loop' && <div className="relative">
                <button
                  onClick={() => {
                    setMenuOpenId(menuOpenId === t.id ? null : t.id);
                  }}
                  className="p-1.5 text-gray-600 hover:text-gray-300 transition-colors"
                  title="More actions"
                >
                  <MoreVertical size={16} />
                </button>
                {menuOpenId === t.id && (
                  <div ref={menuRef} className="absolute top-full mt-1 right-0 bg-gray-900 border border-gray-700 rounded-lg shadow-xl z-20 py-1 min-w-[140px]">
                    <button
                      onClick={() => { setTitleDraft(t.title || ''); setEditingTitleId(t.id); setMenuOpenId(null); }}
                      className="w-full flex items-center gap-2 px-3 py-1.5 text-sm text-gray-300 hover:bg-gray-800 text-left"
                    >
                      <Pencil size={14} /> Edit title
                    </button>
                    {!t.attention_tag && (
                      <button
                        onClick={() => { setEditingAttentionTagId(t.id); setMenuOpenId(null); }}
                        className="w-full flex items-center gap-2 px-3 py-1.5 text-sm text-amber-300 hover:bg-gray-800 text-left"
                      >
                        <Pin size={14} /> Add attention tag
                      </button>
                    )}

                    {canManageTaskShare(t, currentUser) && (
                      <button
                        onClick={(e) => {
                          e.stopPropagation();
                          setMenuOpenId(null);
                          window.dispatchEvent(new CustomEvent('ccm-team-share-task', { detail: { task: t } }));
                        }}
                        className="w-full flex items-center gap-2 px-3 py-1.5 text-sm text-gray-300 hover:bg-gray-800 text-left"
                      >
                        <UserPlus size={14} /> Team Share
                      </button>
                    )}
                    {(t.background_active || ['in_progress', 'executing', 'waiting_capability'].includes(t.status)) && (
                      <button
                        onClick={() => { handleCancel(t); setMenuOpenId(null); }}
                        className="w-full flex items-center gap-2 px-3 py-1.5 text-sm text-yellow-400 hover:bg-gray-800 text-left"
                      >
                        <XCircle size={14} /> Cancel
                      </button>
                    )}
                    {canRetryTask(t) && (
                      <button
                        onClick={() => { handleRetry(t); setMenuOpenId(null); }}
                        className="w-full flex items-center gap-2 px-3 py-1.5 text-sm text-blue-400 hover:bg-gray-800 text-left"
                      >
                        <RotateCcw size={14} /> Retry
                      </button>
                    )}
                    <button
                      onClick={() => { handleArchive(t.id); setMenuOpenId(null); }}
                      className="w-full flex items-center gap-2 px-3 py-1.5 text-sm text-amber-400 hover:bg-gray-800 text-left"
                    >
                      {t.archived ? <ArchiveRestore size={14} /> : <Archive size={14} />}
                      {t.archived ? 'Unarchive' : 'Archive'}
                    </button>
                    {!t.background_active && ['pending', 'failed', 'cancelled', 'completed'].includes(t.status) && (
                      <button
                        onClick={() => { handleDelete(t.id); setMenuOpenId(null); }}
                        className="w-full flex items-center gap-2 px-3 py-1.5 text-sm text-red-400 hover:bg-gray-800 text-left"
                      >
                        <Trash2 size={14} /> Delete
                      </button>
                    )}
                  </div>
                )}
              </div>}
            </div>
          </div>
          {/* Row 2: title + description (full width; pr 留出右下角手柄位) */}
          <div className="mt-1 pl-[1.125rem] pr-7">
            {canControlTask(t) && !prDisplay && t.mode !== 'delivery_loop' && editingAttentionTagId === t.id && (
              <AttentionTag
                taskId={t.id}
                value={t.attention_tag}
                editing
                onEdit={() => setEditingAttentionTagId(t.id)}
                onCancel={() => setEditingAttentionTagId(null)}
                onSaved={handleAttentionTagSaved}
                className="mb-1 w-full"
              />
            )}
            {/* Title (editable) */}
            {canControlTask(t) && !prDisplay && t.mode !== 'delivery_loop' && editingTitleId === t.id ? (
              <input
                ref={titleInputRef}
                value={titleDraft}
                onChange={(e) => setTitleDraft(e.target.value)}
                onBlur={() => handleTitleSave(t)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') handleTitleSave(t);
                  if (e.key === 'Escape') setEditingTitleId(null);
                }}
                className="w-full bg-gray-700 text-foreground text-sm rounded px-2 py-0.5 mt-0.5 focus:outline-none focus:ring-1 focus:ring-indigo-500"
                placeholder="Enter title..."
              />
            ) : (
              t.title ? (
                <p className="text-foreground text-sm font-medium mt-0.5 line-clamp-1">{t.title}</p>
              ) : null
            )}
            {/* Description (expandable) */}
            {prDisplay ? (
              <PRMonitorTaskSummary task={t} result={prResult} />
            ) : t.mode === 'loop' && !t.description ? (
              <p className="text-sm mt-0.5 text-gray-500 italic">{t.todo_file_path}</p>
            ) : t.description ? (
              <ExpandableText
                text={t.description}
                collapsedLines={2}
                className={`text-sm mt-0.5 ${t.title ? 'text-gray-400' : 'text-foreground'}`}
              />
            ) : null}
            {t.mode === 'goal' && t.goal_condition && (
              <p className="text-emerald-500/70 text-xs mt-0.5 line-clamp-1">Goal: {t.goal_condition}</p>
            )}
            {t.mode === 'loop' && t.loop_progress && (
              <p className="text-indigo-400 text-xs mt-0.5">⟳ {t.loop_progress}</p>
            )}
            {t.mode === 'goal' && t.goal_turns_used > 0 && (
              <p className="text-emerald-400 text-xs mt-0.5">
                ◎ Turn {t.goal_turns_used}/{t.goal_max_turns}
                {t.goal_last_reason && <span className="text-gray-500 ml-1">— {t.goal_last_reason}</span>}
              </p>
            )}
            {t.target_repo && (
              <p className="text-gray-600 text-xs mt-0.5 truncate">{t.target_repo}</p>
            )}
            {t.created_at && (
              <p className="text-gray-600 text-xs mt-0.5 flex items-center gap-1">
                <Clock size={10} className="shrink-0" />
                {formatDateTime(t.created_at)}
              </p>
            )}
            {t.error_message && (
              <p className="text-red-400 text-xs mt-1">{t.error_message}</p>
            )}
          </div>
          {canControlTask(t)
            && t.mode === 'delivery_loop'
            && t.delivery_run_id != null
            && openDeliveryRunId === t.delivery_run_id && (
            <DeliveryRunPanel
              runId={t.delivery_run_id}
              className="mt-3"
            />
          )}
        </div>
        );
        })()
      ))}
    </div>
    </>
  );
}
