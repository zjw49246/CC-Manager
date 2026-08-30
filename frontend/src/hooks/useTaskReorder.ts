import React, { useRef, useState, useCallback, useEffect } from 'react';
import { api } from '../api/client';
import type { Task } from '../api/client';

function tsFromIso(s: string): number {
  const iso = s.endsWith('Z') || s.includes('+') ? s : s + 'Z';
  return new Date(iso).getTime() / 1000;
}

/** 排序键。autoSort=true: sort_order ?? last_accessed_at ?? created_at;
 *  autoSort=false: sort_order ?? created_at。越大越靠前。 */
export function effectiveKey(t: Task, autoSort = true): number {
  if (t.sort_order != null) return t.sort_order;
  const ts = autoSort ? (t.last_accessed_at || t.created_at) : t.created_at;
  if (!ts) return 0;
  return tsFromIso(ts);
}

/** 计算把 fromIdx 的任务移到 toIdx（移除后的下标）所需的 sort_order。
 *  只使用同 starred 组的邻居计算中点，避免跨组边界取到另一组的值。 */
export function newSortFor(list: Task[], fromIdx: number, toIdx: number, autoSort = true): number {
  const starred = list[fromIdx]?.starred ?? false;
  const without = list.filter((_, i) => i !== fromIdx);
  let prev: Task | null = null;
  for (let i = toIdx - 1; i >= 0; i--) {
    if ((without[i].starred ?? false) === starred) { prev = without[i]; break; }
  }
  let next: Task | null = null;
  for (let i = toIdx; i < without.length; i++) {
    if ((without[i].starred ?? false) === starred) { next = without[i]; break; }
  }
  const pk = prev ? effectiveKey(prev, autoSort) : null;
  const nk = next ? effectiveKey(next, autoSort) : null;
  if (pk != null && nk != null) return (pk + nk) / 2;
  if (pk == null && nk != null) return nk + 60;
  if (pk != null && nk == null) return pk - 60;
  return Date.now() / 1000;
}

/**
 * Apply an optimistic order to the matching slots of a possibly smaller local
 * page. Server-wide search results may contain tasks that are absent from that
 * page; those entries must never be substituted for an unrelated local row.
 */
export function mergeVisibleTaskOrder(current: Task[], optimistic: Task[]): Task[] {
  const currentIds = new Set(current.map((task) => task.id));
  const visibleInCurrent = optimistic.filter((task) => currentIds.has(task.id));
  const visibleIds = new Set(visibleInCurrent.map((task) => task.id));
  let visibleIndex = 0;
  return current.map((task) =>
    visibleIds.has(task.id) ? visibleInCurrent[visibleIndex++] : task
  );
}

/** 同 starred 分组内的下标范围（标星置顶逻辑：不允许跨组拖动）。 */
function groupRange(list: Task[], idx: number): [number, number] {
  const starred = list[idx]?.starred ?? false;
  let start = 0;
  let end = list.length - 1;
  for (let i = 0; i < list.length; i++) {
    if ((list[i].starred ?? false) === starred) { start = i; break; }
  }
  for (let i = list.length - 1; i >= 0; i--) {
    if ((list[i].starred ?? false) === starred) { end = i; break; }
  }
  return [start, end];
}

/** 将目标行的上/下半区映射为该行之前/之后的插入槽位。 */
function insertionSlot(target: Element, idx: number, clientY: number): number {
  const rect = target.getBoundingClientRect();
  // jsdom 等无布局环境会返回零高；此时沿用历史语义（插到目标行前）。
  if (rect.height <= 0 || !Number.isFinite(clientY)) return idx;
  return clientY >= rect.top + rect.height / 2 ? idx + 1 : idx;
}

interface ReorderApi {
  draggingId: number | null;
  overIndex: number | null;
  /** 整行可拖（侧边栏用）：targetProps + handleProps 合并。 */
  itemProps: (t: Task, idx: number) => Record<string, unknown>;
  /** 仅作为 HTML5 拖放目标，不允许该行自身发起桌面或移动端拖拽。 */
  dropTargetProps: (t: Task, idx: number) => Record<string, unknown>;
  /** 拖放目标 + 移动端长按（行容器用）。 */
  targetProps: (t: Task, idx: number) => Record<string, unknown>;
  /** 桌面拖拽手柄（行内有大段可选中文字时，整行 draggable 会被
   * 文本选择手势抢走 dragStart——主列表必须用显式手柄）。 */
  handleProps: (t: Task, idx: number) => Record<string, unknown>;
  /** Pointer 拖拽手柄（自实现，不依赖 HTML5 DnD）：按住即拖，
   * 配合 ghost 悬浮卡片使用。 */
  pointerHandleProps: (t: Task, idx: number) => Record<string, unknown>;
  /** 拖动中跟随光标的悬浮卡片，渲染在列表容器末尾。 */
  ghost: React.ReactNode;
}

/**
 * 任务拖拽排序（任务列表与侧边栏共用）。
 * - 桌面：HTML5 drag & drop
 * - 移动端：长按 450ms 激活；激活后在 document 上挂非被动 touchmove
 *   （preventDefault 阻止浏览器把手势接管成滚动 → 否则会收到 touchcancel
 *   导致"浮起来但拖不动"）
 * - 标星置顶保留：只能在同 starred 分组内移动
 */
export function useTaskReorder(tasks: Task[], onReordered: (optimistic?: Task[]) => void, autoSort = true): ReorderApi {
  const [draggingId, setDraggingId] = useState<number | null>(null);
  const [overIndex, setOverIndex] = useState<number | null>(null);
  const longPress = useRef<ReturnType<typeof setTimeout> | null>(null);
  // 实时引用，供 document 级监听器读取（避免闭包过期）
  const tasksRef = useRef(tasks);
  tasksRef.current = tasks;
  const dragRef = useRef<number | null>(null);
  const overRef = useRef<number | null>(null);
  const [ghostPos, setGhostPos] = useState<{ x: number; y: number } | null>(null);
  const pointerMode = useRef(false);

  const autoSortRef = useRef(autoSort);
  autoSortRef.current = autoSort;

  const commit = useCallback(async (fromId: number, toIdxRaw: number) => {
    const list = tasksRef.current;
    const fromIdx = list.findIndex((t) => t.id === fromId);
    if (fromIdx < 0) return;
    const [gs, ge] = groupRange(list, fromIdx);
    // Drop positions are insertion slots in the pre-removal list. A group
    // spanning rows gs..ge therefore owns slots gs..ge+1.
    const targetSlot = Math.min(Math.max(toIdxRaw, gs), ge + 1);
    // newSortFor and Array.splice both expect the index after removing the item.
    const toIdx = targetSlot > fromIdx ? targetSlot - 1 : targetSlot;
    if (toIdx === fromIdx) return;
    const sort = newSortFor(list, fromIdx, toIdx, autoSortRef.current);
    // Optimistic: reorder locally first for instant feedback
    const reordered = [...list];
    const [moved] = reordered.splice(fromIdx, 1);
    reordered.splice(toIdx, 0, moved);
    onReordered(reordered);
    try {
      await api.updateTask(fromId, { sort_order: sort });
    } catch { /* the authoritative refresh below restores the server order */ }
    onReordered();
  }, [onReordered]);

  const endDrag = useCallback((commitDrop: boolean) => {
    const fromId = dragRef.current;
    const toIdx = overRef.current;
    dragRef.current = null;
    overRef.current = null;
    setDraggingId(null);
    setOverIndex(null);
    setGhostPos(null);
    pointerMode.current = false;
    document.body.style.overflow = '';
    document.body.style.userSelect = '';
    if (commitDrop && fromId != null && toIdx != null) void commit(fromId, toIdx);
  }, [commit]);

  // Pointer 拖拽：document 级跟踪（鼠标/触控笔/触摸统一）
  useEffect(() => {
    if (draggingId == null || !pointerMode.current) return;
    const onMove = (e: PointerEvent) => {
      e.preventDefault();
      setGhostPos({ x: e.clientX, y: e.clientY });
      const el = document.elementFromPoint(e.clientX, e.clientY);
      const item = el?.closest('[data-reorder-idx]');
      if (item) {
        const idx = Number(item.getAttribute('data-reorder-idx'));
        overRef.current = insertionSlot(item, idx, e.clientY);
        setOverIndex(idx);
      }
    };
    const onUp = () => endDrag(true);
    const onCancel = () => endDrag(false);
    document.addEventListener('pointermove', onMove);
    document.addEventListener('pointerup', onUp);
    document.addEventListener('pointercancel', onCancel);
    return () => {
      document.removeEventListener('pointermove', onMove);
      document.removeEventListener('pointerup', onUp);
      document.removeEventListener('pointercancel', onCancel);
    };
  }, [draggingId, endDrag]);

  // 移动端：激活后用 document 级监听器跟踪手指（非被动，可 preventDefault）
  useEffect(() => {
    if (draggingId == null || pointerMode.current) return;
    const onMove = (e: TouchEvent) => {
      e.preventDefault(); // 阻止滚动接管，避免 touchcancel
      const touch = e.touches[0];
      const el = document.elementFromPoint(touch.clientX, touch.clientY);
      const item = el?.closest('[data-reorder-idx]');
      if (item) {
        const idx = Number(item.getAttribute('data-reorder-idx'));
        overRef.current = insertionSlot(item, idx, touch.clientY);
        setOverIndex(idx);
      }
    };
    const onEnd = () => endDrag(true);
    const onCancel = () => endDrag(false);
    document.addEventListener('touchmove', onMove, { passive: false });
    document.addEventListener('touchend', onEnd);
    document.addEventListener('touchcancel', onCancel);
    return () => {
      document.removeEventListener('touchmove', onMove);
      document.removeEventListener('touchend', onEnd);
      document.removeEventListener('touchcancel', onCancel);
    };
  }, [draggingId, endDrag]);

  const handleProps = useCallback((t: Task) => ({
    draggable: true,
    onDragStart: (e: React.DragEvent) => {
      e.dataTransfer.effectAllowed = 'move';
      e.dataTransfer.setData('text/plain', String(t.id)); // Firefox 需要 setData 才会启动拖拽
      dragRef.current = t.id;
      setDraggingId(t.id);
    },
    onDragEnd: () => endDrag(false),
  }), [endDrag]);

  const dropTargetProps = useCallback((_t: Task, idx: number) => ({
    'data-reorder-idx': idx,
    onDragOver: (e: React.DragEvent) => {
      if (dragRef.current == null) return;
      e.preventDefault();
      overRef.current = insertionSlot(e.currentTarget, idx, e.clientY);
      setOverIndex(idx);
    },
    onDrop: (e: React.DragEvent) => {
      e.preventDefault();
      overRef.current = insertionSlot(e.currentTarget, idx, e.clientY);
      endDrag(true);
    },
  }), [endDrag]);

  const targetProps = useCallback((t: Task, idx: number) => ({
    ...dropTargetProps(t, idx),
    // 移动端长按激活
    onTouchStart: () => {
      longPress.current = setTimeout(() => {
        dragRef.current = t.id;
        overRef.current = idx;
        setDraggingId(t.id);
        document.body.style.overflow = 'hidden';
        if (navigator.vibrate) navigator.vibrate(30);
      }, 450);
    },
    onTouchMove: () => {
      // 长按未触发前移动 = 滚动意图，取消长按（激活后由 document 监听接管）
      if (dragRef.current == null && longPress.current) {
        clearTimeout(longPress.current);
        longPress.current = null;
      }
    },
    onTouchEnd: () => {
      if (longPress.current) { clearTimeout(longPress.current); longPress.current = null; }
    },
  }), [dropTargetProps]);

  const pointerHandleProps = useCallback((t: Task, idx: number) => ({
    onPointerDown: (e: React.PointerEvent) => {
      if (e.button !== 0) return;
      e.preventDefault(); // 阻止文本选择与原生拖拽
      pointerMode.current = true;
      dragRef.current = t.id;
      overRef.current = idx;
      setDraggingId(t.id);
      setGhostPos({ x: e.clientX, y: e.clientY });
      document.body.style.userSelect = 'none';
      if (e.pointerType === 'touch') document.body.style.overflow = 'hidden';
    },
    // touch-action: none 让触摸按下手柄时浏览器不接管滚动
    style: { touchAction: 'none' } as React.CSSProperties,
  }), []);

  const dragTask = tasks.find((t) => t.id === draggingId);
  const ghost: React.ReactNode = (draggingId != null && ghostPos && dragTask)
    ? React.createElement(
        'div',
        {
          style: {
            // 手柄在卡片右下角 → 幽灵卡右对齐光标、向左延伸，避免飞出视口右缘
            position: 'fixed', left: ghostPos.x - 12, top: ghostPos.y - 16,
            transform: 'translateX(-100%)',
            zIndex: 9999, pointerEvents: 'none', maxWidth: 320,
          },
          className: 'px-3 py-2 rounded-lg bg-gray-700/95 border border-indigo-400 shadow-2xl text-xs text-gray-100 truncate',
        },
        `#${dragTask.id} ${dragTask.title || dragTask.description || ''}`.slice(0, 60),
      )
    : null;

  const itemProps = useCallback((t: Task, idx: number) => ({
    ...targetProps(t, idx),
    ...handleProps(t),
  }), [targetProps, handleProps]);

  return {
    draggingId,
    overIndex,
    itemProps,
    dropTargetProps,
    targetProps,
    handleProps,
    pointerHandleProps,
    ghost,
  };
}
