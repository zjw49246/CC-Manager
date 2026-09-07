import { useState, useEffect, useRef, useCallback, useMemo, memo } from 'react';
import { api } from '../../api/client';
import type {
  DiscussionDetail,
  DiscussionMessage,
  DiscussionAgentInfo,
  DiscussionEventItem,
} from '../../api/client';
import { useWebSocket } from '../../hooks/useWebSocket';
import { useVisibilityAwareInterval } from '../../hooks/useVisibilityAwareInterval';
import {
  Send,
  ArrowLeft,
  Loader2,
  Trash2,
  ChevronDown,
  ChevronRight,
  StopCircle,
  Play,
  MessageCircle,
  Wrench,
  Clock,
  UserPlus,
} from '../icons';
import { QuickPhraseDropdown } from '../QuickPhrases/QuickPhraseDropdown';
import { MarkdownRenderer } from '../Markdown/MarkdownRenderer';

const AGENT_COLORS = [
  { border: 'border-blue-500', bg: 'bg-blue-500/10', dot: 'bg-blue-500', text: 'text-blue-400', tab: 'bg-blue-500/20', tabActive: 'bg-blue-500/30 border-blue-500' },
  { border: 'border-emerald-500', bg: 'bg-emerald-500/10', dot: 'bg-emerald-500', text: 'text-emerald-400', tab: 'bg-emerald-500/20', tabActive: 'bg-emerald-500/30 border-emerald-500' },
  { border: 'border-amber-500', bg: 'bg-amber-500/10', dot: 'bg-amber-500', text: 'text-amber-400', tab: 'bg-amber-500/20', tabActive: 'bg-amber-500/30 border-amber-500' },
  { border: 'border-purple-500', bg: 'bg-purple-500/10', dot: 'bg-purple-500', text: 'text-purple-400', tab: 'bg-purple-500/20', tabActive: 'bg-purple-500/30 border-purple-500' },
  { border: 'border-rose-500', bg: 'bg-rose-500/10', dot: 'bg-rose-500', text: 'text-rose-400', tab: 'bg-rose-500/20', tabActive: 'bg-rose-500/30 border-rose-500' },
];

// ---------------------------------------------------------------------------
// Event grouping (consecutive tool_use/tool_result → single collapsible group)
// ---------------------------------------------------------------------------

type EventGroup =
  | { type: 'activity-group'; events: DiscussionEventItem[] }
  | { type: 'single'; event: DiscussionEventItem };

const STANDALONE_TYPES = new Set(['message', 'result', 'user_message', 'process_exit']);
const NOISE_TYPES = new Set(['system_event', 'system_init', 'rate_limit_event']);

function groupEvents(events: DiscussionEventItem[]): EventGroup[] {
  const groups: EventGroup[] = [];
  let buf: DiscussionEventItem[] = [];

  const flushBuf = () => {
    if (buf.length > 0) {
      groups.push({ type: 'activity-group', events: [...buf] });
      buf = [];
    }
  };

  for (const evt of events) {
    if (NOISE_TYPES.has(evt.event_type)) continue;
    if (STANDALONE_TYPES.has(evt.event_type)) {
      flushBuf();
      groups.push({ type: 'single', event: evt });
    } else {
      buf.push(evt);
    }
  }
  flushBuf();
  return groups;
}

function countToolCalls(events: DiscussionEventItem[]): number {
  return events.filter((e) => e.event_type === 'tool_use').length;
}

function formatIdleTime(timestamp: string | undefined, isRunning: boolean): string | null {
  if (isRunning || !timestamp) return null;
  let ts = timestamp;
  if (!ts.endsWith('Z') && !ts.includes('+')) ts += 'Z';
  const lastTime = new Date(ts).getTime();
  if (!lastTime) return null;
  const seconds = Math.floor((Date.now() - lastTime) / 1000);
  if (seconds < 10) return null;
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
  return `${Math.floor(seconds / 3600)}h`;
}

const IdleTimeBadge = memo(function IdleTimeBadge({ timestamp, isRunning }: {
  timestamp?: string;
  isRunning: boolean;
}) {
  const [, setTick] = useState(0);

  // Keep the small clock active only while this tab is visible.
  useVisibilityAwareInterval(() => setTick((t) => t + 1), 5000, !isRunning && Boolean(timestamp), false);

  const idleTime = formatIdleTime(timestamp, isRunning);
  if (!idleTime) return null;
  return (
    <span className="flex items-center gap-0.5 text-gray-600" title="Time since last output">
      <Clock size={9} />{idleTime}
    </span>
  );
});

function formatToolInput(input: string): string {
  try {
    return JSON.stringify(JSON.parse(input), null, 2);
  } catch {
    return input;
  }
}

function toolUseSummary(evt: DiscussionEventItem): string {
  if (!evt.tool_input) return '';
  try {
    const parsed = JSON.parse(evt.tool_input);
    if (parsed.command) {
      const cmd = parsed.command as string;
      return cmd.length > 80 ? cmd.slice(0, 80) + '...' : cmd;
    }
    if (parsed.file_path) return parsed.file_path as string;
    if (parsed.pattern) return `${parsed.pattern}${parsed.path ? ` in ${parsed.path}` : ''}`;
  } catch { /* ignore */ }
  return '';
}

// ---------------------------------------------------------------------------
// Type guards
// ---------------------------------------------------------------------------

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}
function isString(value: unknown): value is string {
  return typeof value === 'string';
}
function nullableString(value: unknown): string | null {
  return typeof value === 'string' ? value : null;
}
function isDiscussionMessage(value: unknown): value is DiscussionMessage {
  return (
    isObject(value) &&
    typeof value.id === 'number' &&
    typeof value.discussion_id === 'number' &&
    isString(value.role) &&
    (isString(value.agent_role_name) || value.agent_role_name === null) &&
    isString(value.content) &&
    isString(value.created_at)
  );
}
function isDiscussionAgentInfo(value: unknown): value is DiscussionAgentInfo {
  return (
    isObject(value) &&
    typeof value.id === 'number' &&
    typeof value.discussion_id === 'number' &&
    isString(value.role_name) &&
    (isString(value.session_id) || value.session_id === null) &&
    isString(value.status) &&
    isString(value.created_at)
  );
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

interface DiscussionViewProps {
  discussionId: number;
  onBack: () => void;
  onDeleted?: () => void;
}

export function DiscussionView({ discussionId, onBack, onDeleted }: DiscussionViewProps) {
  const [discussion, setDiscussion] = useState<DiscussionDetail | null>(null);
  const [agents, setAgents] = useState<DiscussionAgentInfo[]>([]);
  const [messages, setMessages] = useState<DiscussionMessage[]>([]);
  const [agentEvents, setAgentEvents] = useState<Record<number, DiscussionEventItem[]>>({});
  const [input, setInput] = useState('');
  const [sending, setSending] = useState(false);
  const [replyingTo, setReplyingTo] = useState<number | null>(null);
  const [selectedAgent, setSelectedAgent] = useState<number | null>(-1);
  const [facilitatorEvents, setFacilitatorEvents] = useState<DiscussionEventItem[]>([]);
  const [facilitatorStatus, setFacilitatorStatus] = useState<'idle' | 'running' | 'done' | 'error'>('idle');
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    if (!discussion) return;
    const prev = document.title;
    const preview = discussion.title.length > 30 ? discussion.title.slice(0, 30) + '...' : discussion.title;
    document.title = preview || 'Discussion - CCM';
    return () => { document.title = prev; };
  }, [discussion?.title]);

  const loadDiscussion = useCallback(async () => {
    try {
      const d = await api.getDiscussion(discussionId);
      setDiscussion(d);
      setMessages(d.messages || []);
      setAgents(d.agents || []);
      for (const a of (d.agents || [])) {
        const events = await api.getAgentEvents(discussionId, a.id);
        setAgentEvents((prev) => ({ ...prev, [a.id]: events }));
      }
      const facEvents = await api.getAgentEvents(discussionId, 0);
      if (facEvents.length > 0) {
        setFacilitatorEvents(facEvents);
      }
      if ((d.agents || []).length > 0) {
        setSelectedAgent(d.agents[0].id);
      }
    } catch (e) {
      console.error('Failed to load discussion:', e);
    }
  }, [discussionId]);

  useEffect(() => {
    loadDiscussion();
  }, [loadDiscussion]);

  const wsChannels = [
    `discussion:${discussionId}`,
    ...agents.map((a) => `discussion:${discussionId}:agent:${a.id}`),
  ];

  useWebSocket(wsChannels, (raw) => {
    const envelope = raw as { channel?: string; data?: Record<string, unknown> };
    const data = envelope.data;
    if (!data) return;

    if (data.event_type === 'discussion_message' && isDiscussionMessage(data.message)) {
      const msg = data.message;
      setMessages((prev) => {
        if (prev.some((m) => m.id === msg.id)) return prev;
        return [...prev, msg];
      });
    }

    if (data.event_type === 'agent_spawned' && isDiscussionAgentInfo(data.agent)) {
      const agent = data.agent;
      setAgents((prev) => {
        if (prev.some((a) => a.id === agent.id)) return prev;
        return [...prev, agent];
      });
      setAgentEvents((prev) => ({ ...prev, [agent.id]: [] }));
    }

    if (isString(data.event_type) && data.event_type.startsWith('facilitator_')) {
      const subType = data.event_type.replace('facilitator_', '');
      if (subType === 'status' && isString(data.status)) {
        const s = data.status as 'running' | 'done' | 'error';
        setFacilitatorStatus(s);
        if (s === 'running') {
          setSelectedAgent(-1);
        }
        return;
      }
      const evt: DiscussionEventItem = {
        id: Date.now() + Math.random(),
        discussion_id: discussionId,
        agent_id: 0,
        event_type: subType,
        role: nullableString(data.role),
        content: nullableString(data.content),
        tool_name: nullableString(data.tool_name),
        tool_input: nullableString(data.tool_input),
        tool_output: nullableString(data.tool_output),
        is_error: typeof data.is_error === 'boolean' ? data.is_error : false,
        timestamp: isString(data.timestamp) ? data.timestamp : new Date().toISOString(),
      };
      setFacilitatorEvents((prev) => [...prev, evt]);
      return;
    }

    if (
      data.event_type === 'agent_status' &&
      typeof data.agent_id === 'number' &&
      isString(data.status)
    ) {
      setAgents((prev) =>
        prev.map((a) => (a.id === data.agent_id ? { ...a, status: data.status as string } : a))
      );
    }

    if (
      typeof data.agent_id === 'number' &&
      isString(data.event_type) &&
      !['agent_spawned', 'agent_status', 'discussion_message'].includes(data.event_type as string)
    ) {
      const agentId = data.agent_id as number;
      const evt: DiscussionEventItem = {
        id: Date.now() + Math.random(),
        discussion_id: discussionId,
        agent_id: agentId,
        event_type: data.event_type as string,
        role: nullableString(data.role),
        content: nullableString(data.content),
        tool_name: nullableString(data.tool_name),
        tool_input: nullableString(data.tool_input),
        tool_output: nullableString(data.tool_output),
        is_error: typeof data.is_error === 'boolean' ? data.is_error : false,
        timestamp: isString(data.timestamp) ? data.timestamp : new Date().toISOString(),
      };
      setAgentEvents((prev) => ({
        ...prev,
        [agentId]: [...(prev[agentId] || []), evt],
      }));
    }
  });

  // No auto-scroll — user controls scroll position

  const handleSend = async (overrideText?: string) => {
    const text = (overrideText ?? input).trim();
    if (!text || sending) return;
    setSending(true);
    if (!overrideText) setInput('');
    try {
      if (replyingTo) {
        const agent = agents.find((a) => a.id === replyingTo);
        if (agent?.session_id) {
          await api.sendAgentChat(discussionId, replyingTo, text);
        }
        setReplyingTo(null);
      } else {
        await api.sendDiscussionMessage(discussionId, text);
      }
    } catch (e) {
      console.error('Failed to send:', e);
    } finally {
      setSending(false);
      textareaRef.current?.focus();
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  const handleDelete = async () => {
    if (!confirm('Delete this discussion?')) return;
    try {
      await api.deleteDiscussion(discussionId);
      onDeleted?.();
      onBack();
    } catch (e) {
      console.error('Failed to delete:', e);
    }
  };

  const handleTriggerAgent = async (agentId: number) => {
    try { await api.triggerAgent(discussionId, agentId); } catch (e) { console.error('Failed to trigger agent:', e); }
  };

  const handleStopAgent = async (agentId: number) => {
    try { await api.stopAgent(discussionId, agentId); } catch (e) { console.error('Failed to stop agent:', e); }
  };

  const [addingAgent, setAddingAgent] = useState(false);
  const handleAddAgent = async () => {
    if (addingAgent) return;
    setAddingAgent(true);
    try {
      await api.addDiscussionAgent(discussionId);
    } catch (e) {
      console.error('Failed to add agent:', e);
    } finally {
      setAddingAgent(false);
    }
  };

  if (!discussion) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="animate-spin text-gray-400" size={24} />
      </div>
    );
  }

  const facilitatorAsAgent: DiscussionAgentInfo = {
    id: -1,
    discussion_id: discussionId,
    role_name: 'Facilitator',
    session_id: null,
    status: facilitatorStatus === 'running' ? 'running' : 'idle',
    created_at: '',
  };

  const allTabs = [facilitatorAsAgent, ...agents];
  const activeTab = allTabs.find((a) => a.id === selectedAgent);
  const activeColor = activeTab
    ? activeTab.id === -1
      ? { border: 'border-indigo-400', bg: 'bg-indigo-500/10', dot: 'bg-indigo-400', text: 'text-indigo-300', tab: 'bg-indigo-500/20', tabActive: 'bg-indigo-500/30 border-indigo-400' }
      : AGENT_COLORS[(allTabs.indexOf(activeTab) - 1) % AGENT_COLORS.length]
    : null;
  const activeEvents = selectedAgent === -1
    ? facilitatorEvents
    : typeof selectedAgent === 'number'
      ? (agentEvents[selectedAgent] || [])
      : [];

  return (
    <div className="flex flex-col h-[calc(100vh-8rem)]">
      {/* Header */}
      <div className="flex items-center gap-3 pb-3 border-b border-gray-700 shrink-0">
        <button onClick={onBack} className="text-gray-400 hover:text-foreground">
          <ArrowLeft size={20} />
        </button>
        <div className="flex-1 min-w-0">
          <h2 className="text-lg font-semibold text-foreground truncate">{discussion.title}</h2>
          <p className="text-xs text-gray-400">{agents.length} agents · {messages.length} messages</p>
        </div>
        <button
          onClick={handleAddAgent}
          disabled={addingAgent}
          className="flex items-center gap-1 px-2.5 py-1.5 text-xs text-gray-400 hover:text-emerald-400 border border-gray-700 hover:border-emerald-600 rounded-lg transition-colors disabled:opacity-50"
          title="Let Facilitator add one more agent"
        >
          {addingAgent ? <Loader2 className="animate-spin" size={14} /> : <UserPlus size={14} />}
          <span>Add Agent</span>
        </button>
        <button onClick={handleDelete} className="p-2 text-gray-500 hover:text-red-400 transition-colors" title="Delete discussion">
          <Trash2 size={16} />
        </button>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-y-auto py-4 space-y-4">
        {/* Messages (oldest first, newest at bottom) */}
        {messages.map((msg) => (
          <div key={msg.id} className={`flex ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}>
            <div className={`max-w-[85%] rounded-lg px-4 py-3 ${
              msg.role === 'user'
                ? 'bg-indigo-600 text-white'
                : 'bg-emerald-900/30 border border-emerald-700/50 text-foreground'
            }`}>
              {msg.role !== 'user' && msg.agent_role_name && (
                <div className="text-xs text-emerald-400 font-medium mb-1">{msg.agent_role_name}</div>
              )}
              <div className="text-sm prose prose-sm max-w-none prose-invert">
                <MarkdownRenderer content={msg.content} />
              </div>
            </div>
          </div>
        ))}

        {/* Agent tabs (horizontal) */}
        {allTabs.length > 0 && (
          <div className="space-y-0">
            {/* Tab bar */}
            <div className="flex gap-1.5 overflow-x-auto pb-0">
              {allTabs.map((tab, idx) => {
                const isFac = tab.id === -1;
                const tabColor = isFac
                  ? { border: 'border-indigo-400', bg: 'bg-indigo-500/10', dot: 'bg-indigo-400', text: 'text-indigo-300', tab: 'bg-indigo-500/20', tabActive: 'bg-indigo-500/30 border-indigo-400' }
                  : AGENT_COLORS[(idx - 1) % AGENT_COLORS.length];
                const tabEvents = isFac ? facilitatorEvents : (agentEvents[tab.id] || []);
                return (
                  <AgentTab
                    key={tab.id}
                    agent={tab}
                    color={tabColor}
                    events={tabEvents}
                    isSelected={selectedAgent === tab.id}
                    onClick={() => setSelectedAgent(selectedAgent === tab.id ? null : tab.id)}
                  />
                );
              })}
            </div>

            {/* Selected tab content */}
            {activeTab && activeColor && (
              <div className={`border-l-4 ${activeColor.border} ${activeColor.bg} rounded-b-lg rounded-tr-lg overflow-hidden min-w-0`}>
                {/* Action bar */}
                {activeTab.id !== -1 && (
                  <div className="flex items-center gap-1 px-3 py-1.5 border-b border-white/5">
                    {activeTab.status === 'running' && (
                      <button onClick={() => handleStopAgent(activeTab.id)} className="flex items-center gap-1 px-2 py-1 text-xs text-gray-400 hover:text-red-400 rounded hover:bg-white/5">
                        <StopCircle size={12} /> Stop
                      </button>
                    )}
                    {activeTab.status === 'idle' && (
                      <>
                        <button onClick={() => handleTriggerAgent(activeTab.id)} className="flex items-center gap-1 px-2 py-1 text-xs text-gray-400 hover:text-emerald-400 rounded hover:bg-white/5">
                          <Play size={12} /> Trigger
                        </button>
                        {activeTab.session_id && (
                          <button
                            onClick={() => {
                              setReplyingTo(replyingTo === activeTab.id ? null : activeTab.id);
                              textareaRef.current?.focus();
                            }}
                            className={`flex items-center gap-1 px-2 py-1 text-xs rounded hover:bg-white/5 ${replyingTo === activeTab.id ? 'text-indigo-400' : 'text-gray-400 hover:text-indigo-400'}`}
                          >
                            <MessageCircle size={12} /> Reply
                          </button>
                        )}
                      </>
                    )}
                  </div>
                )}
                {activeTab.id === -1 && facilitatorStatus === 'running' && (
                  <div className="flex items-center gap-1 px-3 py-1.5 border-b border-white/5">
                    <span className="flex items-center gap-1 px-2 py-1 text-xs text-indigo-400">
                      <Loader2 className="animate-spin" size={12} /> 正在分析...
                    </span>
                  </div>
                )}

                {activeEvents.length === 0 && activeTab.status !== 'running' && (
                  <p className="text-xs text-gray-500 px-4 py-3">No output yet</p>
                )}
                <GroupedEventList events={activeEvents} />
              </div>
            )}
          </div>
        )}

      </div>

      {/* Input */}
      <div className="border-t border-gray-700 pt-3 shrink-0">
        {replyingTo && (
          <div className="flex items-center gap-2 mb-2 text-xs text-indigo-400">
            <MessageCircle size={12} />
            <span>Replying to: {agents.find((a) => a.id === replyingTo)?.role_name}</span>
            <button onClick={() => setReplyingTo(null)} className="text-gray-500 hover:text-foreground ml-1">Cancel</button>
          </div>
        )}
        <div className="flex items-end gap-2">
          <QuickPhraseDropdown onSelect={(text) => handleSend(text)} disabled={sending} />
          <textarea
            ref={textareaRef}
            value={input}
            onChange={(e) => {
              setInput(e.target.value);
              const el = e.target;
              el.style.height = 'auto';
              el.style.height = Math.min(el.scrollHeight, 192) + 'px';
            }}
            onKeyDown={handleKeyDown}
            placeholder={replyingTo ? `Reply to ${agents.find((a) => a.id === replyingTo)?.role_name}...` : 'Send a message to the group...'}
            className="flex-1 bg-gray-800 text-foreground rounded-lg px-4 py-2.5 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500 resize-none disabled:opacity-50 max-h-48 overflow-y-auto"
            style={{ minHeight: '40px' }}
            rows={1}
            disabled={sending}
          />
          <button
            onClick={() => handleSend()}
            disabled={!input.trim() || sending}
            title="Send (Enter)"
            className="p-2.5 bg-indigo-600 hover:bg-indigo-700 text-white rounded-lg disabled:opacity-40 disabled:cursor-not-allowed"
          >
            <Send size={18} />
          </button>
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Agent tab
// ---------------------------------------------------------------------------

function AgentTab({
  agent,
  color,
  events,
  isSelected,
  onClick,
}: {
  agent: DiscussionAgentInfo;
  color: (typeof AGENT_COLORS)[number];
  events: DiscussionEventItem[];
  isSelected: boolean;
  onClick: () => void;
}) {
  const isRunning = agent.status === 'running';
  const toolCount = useMemo(() => countToolCalls(events), [events]);
  const lastEventTimestamp = events.length > 0 ? events[events.length - 1].timestamp : undefined;

  return (
    <button
      onClick={onClick}
      className={`flex items-center gap-1.5 px-3 py-2 rounded-t-lg text-xs whitespace-nowrap border-b-2 transition-colors ${
        isSelected
          ? `${color.tabActive} ${color.text}`
          : `bg-gray-800 border-transparent text-gray-400 hover:text-gray-300 hover:bg-gray-750`
      }`}
    >
      <div className={`w-1.5 h-1.5 rounded-full ${isRunning ? color.dot : agent.status === 'idle' ? color.dot : 'bg-red-500'}`} />
      <span className="font-medium">{agent.role_name}</span>
      {isRunning && <Loader2 className="animate-spin" size={10} />}
      {toolCount > 0 && (
        <span className="flex items-center gap-0.5 text-gray-500" title={`${toolCount} tool calls`}>
          <Wrench size={9} />{toolCount}
        </span>
      )}
      <IdleTimeBadge timestamp={lastEventTimestamp} isRunning={isRunning} />
    </button>
  );
}

// ---------------------------------------------------------------------------
// Grouped event list
// ---------------------------------------------------------------------------

function GroupedEventList({ events }: { events: DiscussionEventItem[] }) {
  const grouped = useMemo(() => groupEvents(events).reverse(), [events]);

  if (grouped.length === 0) return null;

  return (
    <div className="px-4 py-3 space-y-1 overflow-hidden break-words">
      {grouped.map((g, i) =>
        g.type === 'activity-group' ? (
          <ActivityGroup key={i} events={g.events} />
        ) : (
          <EventItem key={g.event.id || i} event={g.event} />
        )
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Activity group (collapsed: thinking + tool calls bundled together)
// ---------------------------------------------------------------------------

function ActivityGroup({ events }: { events: DiscussionEventItem[] }) {
  const [expanded, setExpanded] = useState(false);
  const hasError = events.some((e) => e.is_error);
  const toolUseCount = events.filter((e) => e.event_type === 'tool_use').length;
  const thinkingCount = events.filter((e) => e.event_type === 'thinking').length;

  const parts: string[] = [];
  if (toolUseCount > 0) parts.push(`${toolUseCount} tool call${toolUseCount !== 1 ? 's' : ''}`);
  if (thinkingCount > 0) parts.push(`${thinkingCount} thinking`);
  const label = parts.join(' + ') || `${events.length} events`;

  return (
    <div>
      <button
        onClick={() => setExpanded(!expanded)}
        className={`flex items-center gap-1.5 text-xs py-1 hover:text-gray-400 transition-colors ${hasError ? 'text-red-400/70' : 'text-gray-600'}`}
      >
        {expanded ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
        <Wrench size={11} />
        <span>{label}</span>
      </button>
      {expanded && (
        <div className="ml-3 border-l border-gray-800 pl-3 space-y-0.5 mt-1">
          {events.map((evt, i) => (
            <ActivityItem key={evt.id || i} event={evt} />
          ))}
        </div>
      )}
    </div>
  );
}

function ActivityItem({ event }: { event: DiscussionEventItem }) {
  const [expanded, setExpanded] = useState(false);

  if (event.event_type === 'thinking') {
    return (
      <div className="text-xs text-gray-500 cursor-pointer hover:text-gray-400" onClick={() => setExpanded(!expanded)}>
        <span className="italic">{expanded ? '▾ thinking' : '▸ thinking...'}</span>
        {expanded && (
          <pre className="mt-1 whitespace-pre-wrap text-gray-400 text-xs max-h-40 overflow-y-auto">{event.content}</pre>
        )}
      </div>
    );
  }

  if (event.event_type === 'tool_use') {
    const summary = toolUseSummary(event);
    return (
      <div>
        <button
          onClick={() => setExpanded(!expanded)}
          className="flex items-center gap-1.5 text-xs text-gray-500 hover:text-gray-400 py-0.5 max-w-full"
        >
          {expanded ? <ChevronDown size={10} className="shrink-0" /> : <ChevronRight size={10} className="shrink-0" />}
          <span className="font-medium">{event.tool_name}</span>
          {summary && <span className="text-gray-600 truncate">{summary}</span>}
        </button>
        {expanded && event.tool_input && (
          <pre className="ml-4 mt-1 mb-1 whitespace-pre-wrap text-gray-400 text-xs max-h-40 overflow-y-auto bg-black/20 rounded p-2">
            {truncate(formatToolInput(event.tool_input), 2000)}
          </pre>
        )}
      </div>
    );
  }

  if (event.event_type === 'tool_result') {
    const statusIcon = event.is_error ? '✗' : '✓';
    const statusColor = event.is_error ? 'text-red-400' : 'text-green-600';
    const detail = event.tool_output || event.content;
    return (
      <div>
        <button
          onClick={() => setExpanded(!expanded)}
          className="flex items-center gap-1.5 text-xs text-gray-600 hover:text-gray-400 py-0.5"
        >
          {expanded ? <ChevronDown size={10} className="shrink-0" /> : <ChevronRight size={10} className="shrink-0" />}
          <span className={statusColor}>{statusIcon}</span>
          <span>{event.tool_name || 'result'}</span>
        </button>
        {expanded && detail && (
          <pre className="ml-4 mt-1 mb-1 whitespace-pre-wrap text-gray-400 text-xs max-h-40 overflow-y-auto bg-black/20 rounded p-2">
            {truncate(detail, 2000)}
          </pre>
        )}
      </div>
    );
  }

  return null;
}

// ---------------------------------------------------------------------------
// Single event item
// ---------------------------------------------------------------------------

function EventItem({ event }: { event: DiscussionEventItem }) {
  if (event.event_type === 'message' || event.event_type === 'result') {
    return (
      <div className="text-sm prose prose-sm max-w-none prose-invert overflow-x-auto break-words">
        <MarkdownRenderer content={event.content || ''} />
      </div>
    );
  }

  if (event.event_type === 'user_message') {
    return (
      <div className="text-sm text-indigo-300 bg-indigo-500/10 rounded px-3 py-2 my-1">{event.content}</div>
    );
  }

  if (event.event_type === 'process_exit') return null;

  return null;
}

function truncate(s: string, max: number): string {
  if (s.length <= max) return s;
  return s.slice(0, max) + `\n... (${s.length - max} chars truncated)`;
}
