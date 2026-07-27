import { getApiBase } from '../config/server';

function getBase(): string {
  return getApiBase();
}

export function getToken(): string {
  return localStorage.getItem('cc_token') || '';
}

export function setToken(token: string) {
  localStorage.setItem('cc_token', token);
}

export function clearToken() {
  localStorage.removeItem('cc_token');
}

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const token = getToken();
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    ...(options?.headers as Record<string, string>),
  };
  if (token) {
    headers['Authorization'] = `Bearer ${token}`;
  }
  const res = await fetch(`${getBase()}${path}`, { ...options, headers });
  if (res.status === 401) {
    clearToken();
    window.location.reload();
    throw new Error('Unauthorized');
  }
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    const detail = err.detail;
    const message = typeof detail === 'string'
      ? detail
      : detail && typeof detail === 'object' && typeof detail.error === 'string'
        ? detail.error
        : res.statusText;
    const requestError = new Error(message) as Error & {
      status?: number;
      detail?: unknown;
    };
    requestError.status = res.status;
    requestError.detail = detail;
    throw requestError;
  }
  const refreshedToken = res.headers.get('X-Refreshed-Token');
  if (refreshedToken) {
    setToken(refreshedToken);
  }
  return res.json();
}

export interface RuntimeSettings {
  use_pty_mode: boolean;
  pty_available: boolean;
  codex_app_server_enabled: boolean;
  /** Absent when proxying an older Worker that predates this capability. */
  codex_main_mcp_enabled?: boolean;
  auto_sort_on_access: boolean;
  /** 会话上下文利用率达到该比例自动压缩换新 session（0-1，有效值） */
  context_compact_threshold: number;
}

export interface GlobalSettings {
  git_author_name: string | null;
  git_author_email: string | null;
  git_credential_type: string | null;  // "ssh" | "https" | null
  git_ssh_key_path: string | null;
  git_https_username: string | null;
  git_https_token: string | null;
}

export interface Project {
  id: number;
  name: string;
  git_url: string | null;
  has_remote: boolean;
  local_path: string | null;
  default_branch: string;
  status: string;
  error_message: string | null;
  show_in_selector: boolean;
  sort_order: number;
  tags: string[];
  env_files: string[];
  git_author_name: string | null;
  git_author_email: string | null;
  git_credential_type: string | null;  // "ssh" | "https" | null
  git_ssh_key_path: string | null;
  git_https_username: string | null;
  git_https_token: string | null;
  badge_color: string | null;
  created_at: string;
  location?: string;  // "local" or worker name
}

export type ProjectTodoStatus = 'open' | 'done' | 'archived';

export interface ProjectTodo {
  id: number;
  project_id: number;
  title: string;
  prompt: string;
  status: ProjectTodoStatus;
  sort_order: number;
  created_task_id: number | null;
  created_at: string;
  updated_at: string;
}

export interface Task {
  id: number;
  worker_id: number | null;
  created_by: number | null;
  title: string;
  description: string | null;
  status: string;
  priority: number;
  project_id: number | null;
  target_repo: string | null;
  target_branch: string;
  result_branch: string | null;
  merge_status: string;
  instance_id: number | null;
  retry_count: number;
  max_retries: number;
  mode: string;
  todo_file_path: string | null;
  loop_progress: string | null;
  max_iterations: number;
  must_complete: boolean;
  goal_condition: string | null;
  goal_evaluator_model: string | null;
  goal_max_turns: number;
  goal_turns_used: number;
  goal_last_reason: string | null;
  plan_content: string | null;
  plan_approved: boolean | null;
  starred: boolean;
  archived: boolean;
  has_unread: boolean;
  session_id: string | null;
  error_message: string | null;
  provider: string;
  model: string | null;
  effort_level: string | null;
  thinking_budget?: number | null;
  system_prompt_mode?: string | null;
  timeout_hours?: number | null;
  last_accessed_at?: string | null;
  sort_order?: number | null;
  enable_workflows: boolean;
  enabled_skills: Record<string, boolean> | null;
  selected_user_skills: number[] | null;
  shared_from_id: number | null;
  active_sub_agents: number;
  tags: string[] | null;
  metadata_: {
    image_paths?: string[];
    attachments?: FileAttachment[];
    secret_ids?: number[];
    codex_account_id?: string;
    forked_from_task_id?: number;
    forked_from_log_id?: number | null;
    forked_from_turn_id?: string;
    fork_seed_message?: string;
    fork_seed_log_id?: number | null;
    fork_seed_uploads?: UploadResult[];
  } | null;
  context_window_usage: {
    input_tokens: number;
    cache_read_input_tokens: number;
    cache_creation_input_tokens: number;
    output_tokens: number;
    total_input_tokens: number;
    context_window?: number;
  } | null;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
}

export interface Instance {
  id: number;
  name: string;
  pid: number | null;
  status: string;
  current_task_id: number | null;
  worktree_path: string | null;
  provider: string;
  model: string;
  effort_level: string | null;
  thinking_budget: number | null;
  system_prompt_mode: string | null;
  total_tasks_completed: number;
  total_cost_usd: number;
  started_at: string | null;
  last_heartbeat: string | null;
}

export interface FileAttachment {
  url: string;
  name: string;
  is_image: boolean;
}

export interface ChatMessage {
  id: number;
  role: string;
  event_type: string;
  content: string | null;
  tool_name: string | null;
  tool_input: string | null;
  tool_output: string | null;
  is_error: boolean;
  pty_cold_start?: boolean;
  loop_iteration: number | null;
  timestamp: string | null;
  image_urls: string[] | null;
  attachments: FileAttachment[] | null;
  source?: string | null;
  /** Original user text without the display-only sender prefix. */
  raw_content?: string | null;
  /** Live-only app-server item id used to merge streamed deltas into the final message. */
  stream_item_id?: string | null;
  /** Native Codex ids used to resolve a safe thread/fork boundary. */
  item_id?: string | null;
  turn_id?: string | null;
  /** True when this row came from persisted chat history, not live optimism. */
  persisted?: boolean;
  // 权限透传卡片（event_type === 'permission_request' 时存在）
  request_id?: string | null;
  permission_status?: 'pending' | 'allow' | 'deny' | 'expired' | null;
  // ask_user 卡片（event_type === 'ask_user_question' 时存在）
  ask_questions?: AskUserQuestion[] | null;
  ask_status?: 'pending' | 'answered' | 'timed_out' | 'expired' | null;
}

export interface CodexForkAnchor {
  type: 'initial' | 'user_message';
  id: number | null;
  content: string;
  timestamp: string | null;
  attachments: FileAttachment[];
}

export interface AskUserOption {
  label: string;
  description?: string;
}

export interface AskUserQuestion {
  question: string;
  header?: string;
  options: AskUserOption[];
  multiSelect?: boolean;
}

export interface AskUserAnswer {
  labels: string[];
  text?: string;
}

export interface LogEntry {
  id: number;
  instance_id: number;
  task_id: number | null;
  event_type: string;
  role: string | null;
  content: string | null;
  tool_name: string | null;
  tool_input: string | null;
  tool_output: string | null;
  item_id?: string | null;
  is_error: boolean;
  timestamp: string;
}

export interface Secret {
  id: number;
  name: string;
  content: string;
  created_at: string;
  updated_at: string;
}

export interface TagItem {
  id: number;
  name: string;
  color: string;
  created_at: string;
}

export interface UploadResult {
  id: string;
  filename: string | null;
  path: string;
  url: string;
  is_image: boolean;
}

export interface DiscussionMessage {
  id: number;
  discussion_id: number;
  role: string;
  agent_role_name: string | null;
  content: string;
  created_at: string;
}

export interface DiscussionAgentInfo {
  id: number;
  discussion_id: number;
  role_name: string;
  session_id: string | null;
  status: string;
  created_at: string;
}

export interface QuickPhrase {
  id: number;
  label: string;
  content: string;
  sort_order: number;
}

export interface DiscussionEventItem {
  id: number;
  discussion_id: number;
  agent_id: number;
  event_type: string;
  role: string | null;
  content: string | null;
  tool_name: string | null;
  tool_input: string | null;
  tool_output: string | null;
  is_error: boolean;
  timestamp: string;
}

export interface DiscussionListItem {
  id: number;
  title: string;
  project_id: number | null;
  max_agents: number;
  facilitator_model: string;
  agent_model: string;
  status: string;
  created_at: string;
  agent_count: number;
  message_count: number;
}

export interface DiscussionDetail {
  id: number;
  title: string;
  project_id: number | null;
  max_agents: number;
  facilitator_model: string;
  agent_model: string;
  status: string;
  created_at: string;
  messages: DiscussionMessage[];
  agents: DiscussionAgentInfo[];
}

export interface MonitorSession {
  id: number;
  task_id: number;
  agent_type: string;   // monitor | native-agent | native-monitor | ...
  source: string;       // ccm（$命令启动）| native（模型自己开的）
  description: string;
  monitor_context: string | null;
  interval: number;
  max_checks: number;
  status: string;
  checks_done: number;
  last_summary: string | null;
  created_at: string;
  completed_at: string | null;
}

export interface MonitorCheck {
  id: number;
  monitor_session_id: number;
  check_number: number;
  status: string;
  summary: string | null;
  full_output: string | null;
  created_at: string;
}

export interface SubAgentTypeSummary {
  running: number;
  completed: number;
}

export interface SubAgentSummary {
  by_type: Record<string, SubAgentTypeSummary>;
}

export interface MonitoredRepo {
  id: number;
  repo_full_name: string;
  project_id: number | null;
  enabled: boolean;
  auto_merge: boolean;
  webhook_secret: string;
  provider: string;
  review_model: string | null;
  default_branch: string;
  allowed_authors: string[];
  status: string;
  error_message: string | null;
  created_at: string;
  updated_at: string;
}

export interface PRReview {
  id: number;
  repo_id: number;
  pr_number: number;
  head_sha: string | null;
  delivery_id: string | null;
  pr_title: string;
  pr_author: string;
  pr_url: string;
  task_id: number | null;
  status: string;
  review_summary: string | null;
  action_taken: string | null;
  created_at: string;
  completed_at: string | null;
}

export interface PoolUsageWindow {
  utilization: number | null;
  resets_at: string | null;
}

export interface CloudRouterQuotaWindow {
  id?: string | null;
  label?: string | null;
  used?: number | null;
  limit?: number | null;
  remaining?: number | null;
  utilization?: number | null;
  reset_at?: string | number | null;
  resets_at?: string | number | null;
  currency?: string | null;
}

export interface CloudRouterQuotaTotal {
  used?: number | null;
  limit?: number | null;
  remaining?: number | null;
  currency?: string | null;
}

export interface CloudRouterApiQuota {
  state: string;
  status?: string | null;
  mode?: string | null;
  currency?: string | null;
  unit?: string | null;
  quota?: CloudRouterQuotaTotal | null;
  remaining?: number | null;
  balance?: number | null;
  windows?: CloudRouterQuotaWindow[];
  available?: boolean;
  known?: boolean;
  stale?: boolean;
  reason?: string | null;
  plan_name?: string | null;
  expires_at?: string | number | null;
  days_until_expiry?: number | null;
  fetched_at?: string | number | null;
  usage?: Record<string, unknown> | null;
  account_id?: string;
}

export interface CloudRouterModelMap {
  claude: string[];
  codex: string[];
}

export interface CloudRouterAccount {
  id: string;
  name: string;
  enabled: boolean;
  retired: boolean;
  key_hint: string;
  models: CloudRouterModelMap;
  providers: string[];
  account_dir: string;
  claude_config_dir: string;
  codex_home: string;
  supported_models: string[];
  endpoints: Record<string, string>;
  api_quota?: CloudRouterApiQuota | null;
}

export interface CloudRouterRetireResult extends CloudRouterAccount {
  ok: boolean;
}

export interface CloudRouterAccountProjection {
  auth_kind?: string | null;
  display_name?: string | null;
  api_account_id?: string | null;
  supported_models?: string[];
  api_quota?: CloudRouterApiQuota | null;
}

export interface PoolAccountUsage extends CloudRouterAccountProjection {
  id: string;
  config_dir: string;
  email: string;
  role: string;
  enabled: boolean;
  available: boolean;
  cooldown_until: number | null;
  cooldown_remaining: number;
  // 仅 /api/pool/usage 返回以下字段（/status 不含）
  subscription_type?: string | null;
  usage?: {
    five_hour: PoolUsageWindow | null;
    seven_day: PoolUsageWindow | null;
    seven_day_opus: PoolUsageWindow | null;
    seven_day_sonnet: PoolUsageWindow | null;
  } | null;
  usage_error?: string | null;
}

export interface PoolUsageStatus {
  enabled: boolean;
  total: number;
  available: number;
  cooldown: number;
  disabled: number;
  preferred?: string | null;
  last_selected?: string | null;
  accounts: PoolAccountUsage[];
}

export type CodexLoginMethod = '171mail' | 'mailcatcher' | 'mailcom' | 'onet' | 'gazeta';

export interface CodexPoolQuota {
  primary_used_percent: number | null;
  primary_window_minutes: number | null;
  primary_resets_at: number | null;
  secondary_used_percent: number | null;
  secondary_window_minutes: number | null;
  secondary_resets_at: number | null;
  plan_type?: string | null;
  is_rate_limited: boolean;
  has_credits: boolean;
}

export interface CodexPoolAccountUsage extends CloudRouterAccountProjection {
  id: string;
  codex_home: string;
  email: string;
  enabled: boolean;
  available: boolean;
  cooldown_until: number | null;
  cooldown_remaining: number;
  plan_type?: string | null;
  quota?: CodexPoolQuota | null;
  quota_error?: string | null;
}

export interface CodexPoolUsageStatus {
  enabled: boolean;
  total: number;
  available: number;
  cooldown: number;
  disabled: number;
  preferred: string | null;
  last_selected?: string | null;
  accounts: CodexPoolAccountUsage[];
}

export type CodexLoginStatusName =
  | 'idle'
  | 'running'
  | 'awaiting_otp'
  | 'verifying_otp'
  | 'finalizing'
  | 'cancelling'
  | 'success'
  | 'failed'
  | 'expired'
  | 'cancelled';

export interface CodexLoginStatus {
  status: CodexLoginStatusName;
  detail?: string;
  attempt_id?: string;
  challenge_id?: string;
  expires_at?: number;
  account_id?: string;
}


export interface TeamUser {
  id: number;
  email: string;
  name: string;
  role: string;
  avatar_url: string;
}

export type WorkerProvider = 'codex' | 'claude';

export interface WorkerAccountInput {
  email: string;
  provider?: WorkerProvider;
  token?: string;
  password?: string;
  login_method?: string;
}

export interface WorkerAccountSummary {
  email: string;
  provider?: WorkerProvider;
  status: string;
}

export interface WorkerPoolUsageWindow {
  utilization: number;
  resets_at: string | null;
}

export interface WorkerPoolAccount {
  id: string;
  email?: string | null;
  enabled: boolean;
  available: boolean;
  cooldown_remaining?: number;
  subscription_type?: string | null;
  plan_type?: string | null;
  usage?: {
    five_hour?: WorkerPoolUsageWindow | null;
    seven_day?: WorkerPoolUsageWindow | null;
    seven_day_opus?: WorkerPoolUsageWindow | null;
  } | null;
  usage_error?: string | null;
  quota?: CodexPoolQuota | null;
  quota_error?: string | null;
}

export interface WorkerPoolStatus {
  enabled?: boolean;
  provider?: WorkerProvider;
  total?: number;
  available?: number;
  accounts: WorkerPoolAccount[];
}

export interface Worker {
  id: number;
  name: string;
  status: string;
  owner_user_id: number | null;
  cloud_instance_id: string | null;
  private_ip: string | null;
  public_ip: string | null;
  ssh_user: string;
  ssh_key_path: string | null;
  ccm_port: number;
  ccm_commit: string | null;
  accounts: WorkerAccountSummary[] | null;
  last_heartbeat: string | null;
  bootstrap_step: string | null;
  bootstrap_error: string | null;
  created_at: string;
  updated_at: string;
}

export interface OrgMember {
  feishu_open_id: string;
  name: string;
  ccm_url: string;
  avatar_url?: string;
}

export interface SharedTaskReceived {
  id: number;
  owner_ccm_url: string;
  owner_name?: string;
  remote_task_id: number;
  share_token: string;
  local_task_id?: number;
  task_title?: string;
  task_description?: string;
  project_name?: string;
  received_at?: string;
  remote_task?: {
    id: number;
    title?: string;
    description?: string;
    status: string;
    priority?: number;
    mode?: string;
    model?: string;
    provider?: string;
    effort_level?: string;
    project_id?: number;
    project_name?: string;
    session_id?: string;
    target_repo?: string;
    error_message?: string;
    loop_progress?: string;
    created_at?: string;
    started_at?: string;
    completed_at?: string;
  };
}

export interface OrgTeam {
  id: number;
  name: string;
  description?: string;
  members?: OrgMember[];
}

export interface UpdateReconcileResult {
  update_blocked: boolean;
  active_task_count: number;
  active_tasks: Array<{
    id: number;
    title: string;
    status: string;
    kind?: 'task' | 'instance' | 'monitor' | 'sub_agent';
    instance_id?: number;
    instance_claim_count?: number;
  }>;
  reconciled?: boolean;
}

// ---------------------------------------------------------------------------
// Skills / User-Skills cache (avoid re-fetching on every TaskForm mount)
// ---------------------------------------------------------------------------
let _skillsCache: { key: string; label: string; description: string; always: boolean; priority: number; tags: string[] }[] | null = null;
let _userSkillsCache: any[] | null = null;

export function invalidateSkillsCache() { _skillsCache = null; }
export function invalidateUserSkillsCache() { _userSkillsCache = null; }

async function listSkillsCached() {
  if (_skillsCache) return _skillsCache;
  const result = await request<{ key: string; label: string; description: string; always: boolean; priority: number; tags: string[] }[]>('/api/system/skills');
  _skillsCache = result;
  return result;
}

async function listUserSkillsCached() {
  if (_userSkillsCache) return _userSkillsCache;
  const result = await request<any[]>('/api/user-skills');
  _userSkillsCache = result;
  return result;
}

export const api = {
  // Feishu
  getFeishuAuthUrl: () => request<{ url: string }>('/api/feishu/auth-url'),
  getFeishuStatus: () => request<{ bound: boolean; name?: string; open_id?: string; avatar_url?: string; is_registry?: boolean }>('/api/feishu/status'),
  unbindFeishu: () => request<{ ok: boolean }>('/api/feishu/unbind', { method: 'DELETE' }),

  // Org
  getOrgMembers: () => request<OrgMember[]>('/api/org/members'),
  getOrgTeams: () => request<OrgTeam[]>('/api/org/teams'),
  createOrgTeam: (name: string, description?: string) => request<OrgTeam>('/api/org/teams', { method: 'POST', body: JSON.stringify({ name, description }) }),
  updateOrgTeam: (id: number, name: string, description?: string) => request<OrgTeam>(`/api/org/teams/${id}`, { method: 'PUT', body: JSON.stringify({ name, description }) }),
  deleteOrgTeam: (id: number) => request<{ ok: boolean }>(`/api/org/teams/${id}`, { method: 'DELETE' }),
  addTeamMember: (teamId: number, openId: string) => request<{ ok: boolean }>(`/api/org/teams/${teamId}/members`, { method: 'POST', body: JSON.stringify({ open_id: openId }) }),
  removeTeamMember: (teamId: number, openId: string) => request<{ ok: boolean }>(`/api/org/teams/${teamId}/members/${openId}`, { method: 'DELETE' }),
  transferRegistry: (targetCcmUrl: string) => request<{ ok: boolean }>('/api/org/transfer', { method: 'POST', body: JSON.stringify({ target_ccm_url: targetCcmUrl }) }),

  // Task sharing
  shareTask: (taskId: number, targets: { open_id: string; name?: string; ccm_url: string }[]) =>
    request<{ shares: any[] }>(`/api/tasks/${taskId}/share`, { method: 'POST', body: JSON.stringify({ targets }) }),
  revokeTaskShare: (taskId: number, openId: string) =>
    request<{ ok: boolean }>(`/api/tasks/${taskId}/share/${openId}`, { method: 'DELETE' }),
  getTaskShares: (taskId: number) =>
    request<{ shares: any[] }>(`/api/tasks/${taskId}/shares`),

  // Project sharing
  shareProject: (projectId: number, targets: { open_id: string; name?: string; ccm_url: string }[]) =>
    request<{ shares: any[] }>(`/api/projects/${projectId}/share`, { method: 'POST', body: JSON.stringify({ targets }) }),
  revokeProjectShare: (projectId: number, openId: string) =>
    request<{ ok: boolean }>(`/api/projects/${projectId}/share/${openId}`, { method: 'DELETE' }),
  getProjectShares: (projectId: number) =>
    request<{ shares: any[] }>(`/api/projects/${projectId}/shares`),

  // Shared tasks (received from others)
  getSharedTasks: (enrich = false) =>
    request<{ tasks: SharedTaskReceived[] }>(`/api/shared/tasks${enrich ? '?enrich=true' : ''}`),
  leaveSharedTask: (sharedId: number) =>
    request<{ ok: boolean }>(`/api/shared/${sharedId}`, { method: 'DELETE' }),
  getSharedHistory: (sharedId: number, limit?: number, beforeId?: number) => {
    const params = new URLSearchParams();
    if (limit) params.set('limit', String(limit));
    if (beforeId) params.set('before_id', String(beforeId));
    const qs = params.toString();
    return request<any[]>(`/api/shared/${sharedId}/history${qs ? '?' + qs : ''}`);
  },
  sendSharedChat: (sharedId: number, message: string) =>
    request<{ ok: boolean }>(`/api/shared/${sharedId}/chat`, { method: 'POST', body: JSON.stringify({ message }) }),
  getSharedConfig: (sharedId: number) =>
    request<any>(`/api/shared/${sharedId}/config`),
  pingSharer: (sharedId: number) =>
    request<{ online: boolean }>(`/api/shared/${sharedId}/ping`),

  // Projects
  listProjects: () => request<Project[]>('/api/projects'),
  listProjectTags: () => request<string[]>('/api/projects/tags'),
  createProject: (data: {
    name: string;
    worker_id?: number;
    git_url?: string;
    default_branch?: string;
    sort_order?: number;
    tags?: string[];
    git_author_name?: string;
    git_author_email?: string;
    git_credential_type?: string;
    git_ssh_key_path?: string;
    git_https_username?: string;
    git_https_token?: string;
  }) =>
    request<Project>('/api/projects', { method: 'POST', body: JSON.stringify(data) }),
  updateProject: (id: number, data: Partial<Pick<Project, 'name' | 'show_in_selector' | 'sort_order' | 'tags' | 'env_files' | 'badge_color' | 'git_author_name' | 'git_author_email' | 'git_credential_type' | 'git_ssh_key_path' | 'git_https_username' | 'git_https_token'>>) =>
    request<Project>(`/api/projects/${id}`, { method: 'PUT', body: JSON.stringify(data) }),
  reorderProjects: (orders: { id: number; sort_order: number }[]) =>
    request<Project[]>('/api/projects/reorder', { method: 'PUT', body: JSON.stringify(orders) }),
  deleteProject: (id: number) =>
    request<{ ok: boolean }>(`/api/projects/${id}`, { method: 'DELETE' }),
  recloneProject: (id: number) =>
    request<{ ok: boolean }>(`/api/projects/${id}/reclone`, { method: 'POST' }),
  listProjectTodos: (projectId: number, includeArchived = false) =>
    request<ProjectTodo[]>(`/api/projects/${projectId}/todos${includeArchived ? '?include_archived=true' : ''}`),
  createProjectTodo: (projectId: number, data: { title: string; prompt: string }) =>
    request<ProjectTodo>(`/api/projects/${projectId}/todos`, { method: 'POST', body: JSON.stringify(data) }),
  updateProjectTodo: (projectId: number, todoId: number, data: Partial<Pick<ProjectTodo, 'title' | 'prompt' | 'status' | 'sort_order' | 'created_task_id'>>) =>
    request<ProjectTodo>(`/api/projects/${projectId}/todos/${todoId}`, { method: 'PATCH', body: JSON.stringify(data) }),
  deleteProjectTodo: (projectId: number, todoId: number) =>
    request<{ ok: boolean }>(`/api/projects/${projectId}/todos/${todoId}`, { method: 'DELETE' }),

  // Env files
  listEnvFiles: (projectId: number) =>
    request<{ files: { path: string; exists: boolean }[] }>(`/api/projects/${projectId}/env-files`),
  getEnvFileContent: (projectId: number, filepath: string) =>
    request<{ content: string }>(`/api/projects/${projectId}/env-files/${filepath}`),
  updateEnvFileContent: (projectId: number, filepath: string, content: string) =>
    request<{ content: string }>(`/api/projects/${projectId}/env-files/${filepath}`, {
      method: 'PUT',
      body: JSON.stringify({ content }),
    }),
  scanEnvFiles: (projectId: number) =>
    request<{ tracked: string[]; discovered: string[] }>(`/api/projects/${projectId}/scan-env-files`, {
      method: 'POST',
    }),

  // Claude Pool
  getPoolStatus: () => request<PoolUsageStatus>('/api/pool/status'),
  getPoolUsage: (force?: boolean) => request<PoolUsageStatus>('/api/pool/usage' + (force ? '?force=true' : '')),
  clearPoolCooldown: (accountId: string) =>
    request<{ ok: boolean }>(`/api/pool/accounts/${accountId}/clear-cooldown`, { method: 'POST' }),
  setPoolPreferred: (accountId: string | null) =>
    request<{ ok: boolean; preferred: string | null }>('/api/pool/preferred', { method: 'POST', body: JSON.stringify({ account_id: accountId }) }),
  // 重新登录：后端先试 OAuth refresh（秒回 success），失败才后台跑 auto_login（running，需轮询）
  poolDeleteAccount: (accountId: string) =>
    request<{ ok: boolean }>(`/api/pool/accounts/${accountId}`, { method: 'DELETE' }),
  poolRelogin: (accountId: string) =>
    request<{ ok: boolean; method: string; status: string }>(`/api/pool/accounts/${accountId}/relogin`, { method: 'POST' }),
  poolReloginStatus: (accountId: string) =>
    request<{ status: string; detail?: string }>(`/api/pool/accounts/${accountId}/relogin`),

  // CloudRouter API accounts are projected into both existing pools. Their
  // secret is accepted only when creating the dedicated account directory.
  getCloudRouterAccounts: (force?: boolean) =>
    request<CloudRouterAccount[]>('/api/cloudrouter/accounts' + (force ? '?force=true' : '')),
  createCloudRouterAccount: (data: { name: string; api_key: string }) =>
    request<CloudRouterAccount>('/api/cloudrouter/accounts', {
      method: 'POST',
      body: JSON.stringify(data),
    }),
  refreshCloudRouterAccount: (accountId: string) =>
    request<CloudRouterAccount>(`/api/cloudrouter/accounts/${encodeURIComponent(accountId)}/refresh`, {
      method: 'POST',
    }),
  deleteCloudRouterAccount: (accountId: string) =>
    request<CloudRouterRetireResult>(`/api/cloudrouter/accounts/${encodeURIComponent(accountId)}`, {
      method: 'DELETE',
    }),

  // Global Settings
  getRuntimeSettings: () => request<RuntimeSettings>('/api/settings/runtime'),
  updateRuntimeSettings: (data: Partial<Pick<RuntimeSettings, 'use_pty_mode' | 'auto_sort_on_access' | 'context_compact_threshold'>>) =>
    request<RuntimeSettings>('/api/settings/runtime', { method: 'PUT', body: JSON.stringify(data) }),
  getGitSettings: () => request<GlobalSettings>('/api/settings/git'),
  updateGitSettings: (data: Partial<GlobalSettings>) =>
    request<GlobalSettings>('/api/settings/git', { method: 'PUT', body: JSON.stringify(data) }),
  getDefaultSkills: () => request<{ default_enabled_plugins: Record<string, boolean> | null; default_enabled_user_skills: number[] | null }>('/api/settings/default-skills'),
  setDefaultSkills: (plugins: Record<string, boolean> | null, userSkills: number[] | null) =>
    request<{ default_enabled_plugins: Record<string, boolean> | null; default_enabled_user_skills: number[] | null }>('/api/settings/default-skills', { method: 'PUT', body: JSON.stringify({ default_enabled_plugins: plugins, default_enabled_user_skills: userSkills }) }),

  // Secrets
  listSecrets: () => request<Secret[]>('/api/secrets'),
  createSecret: (data: { name: string; content: string }) =>
    request<Secret>('/api/secrets', { method: 'POST', body: JSON.stringify(data) }),
  updateSecret: (id: number, data: { name?: string; content?: string }) =>
    request<Secret>(`/api/secrets/${id}`, { method: 'PUT', body: JSON.stringify(data) }),
  deleteSecret: (id: number) =>
    request<{ ok: boolean }>(`/api/secrets/${id}`, { method: 'DELETE' }),

  // Tags
  listTags: () => request<TagItem[]>('/api/tags'),
  createTag: (data: { name: string; color: string }) =>
    request<TagItem>('/api/tags', { method: 'POST', body: JSON.stringify(data) }),
  updateTag: (id: number, data: { name?: string; color?: string }) =>
    request<TagItem>(`/api/tags/${id}`, { method: 'PUT', body: JSON.stringify(data) }),
  deleteTag: (id: number) =>
    request<{ ok: boolean }>(`/api/tags/${id}`, { method: 'DELETE' }),

  // Uploads
  uploadImages: (files: File[]): Promise<UploadResult[]> => {
    const token = getToken();
    const formData = new FormData();
    for (const file of files) {
      formData.append('files', file);
    }
    const controller = new AbortController();
    const totalSize = files.reduce((sum, f) => sum + f.size, 0);
    const timeoutMs = Math.max(120_000, Math.ceil(totalSize / 50_000) * 1000);
    const timeout = setTimeout(() => controller.abort(), timeoutMs);
    return fetch(`${getBase()}/api/uploads`, {
      method: 'POST',
      headers: token ? { Authorization: `Bearer ${token}` } : {},
      body: formData,
      signal: controller.signal,
    }).then(async (res) => {
      clearTimeout(timeout);
      if (res.status === 401) { clearToken(); window.location.reload(); throw new Error('Unauthorized'); }
      if (!res.ok) { const err = await res.json().catch(() => ({ detail: res.statusText })); throw new Error(err.detail || res.statusText); }
      return res.json();
    }).catch((e) => {
      clearTimeout(timeout);
      if (e.name === 'AbortError') throw new Error(`Upload timed out. Total size: ${(totalSize / 1024 / 1024).toFixed(1)}MB`);
      throw e;
    });
  },

  // Tasks
  getTask: (id: number) =>
    request<Task>(`/api/tasks/${id}`),
  listTasks: (status?: string, includeArchived?: boolean, projectId?: number, starred?: boolean, limit?: number, offset?: number, archivedOnly?: boolean, hasUnread?: boolean) =>
    request<Task[]>(`/api/tasks?${new URLSearchParams({
      ...(status ? { status } : {}),
      ...(archivedOnly ? { archived_only: 'true' } : includeArchived ? { include_archived: 'true' } : {}),
      ...(projectId != null ? { project_id: String(projectId) } : {}),
      ...(starred != null ? { starred: String(starred) } : {}),
      ...(hasUnread != null ? { has_unread: String(hasUnread) } : {}),
      ...(limit != null ? { limit: String(limit) } : {}),
      ...(offset != null ? { offset: String(offset) } : {}),
    })}`),
  countTasks: (status?: string, includeArchived?: boolean, projectId?: number, starred?: boolean, archivedOnly?: boolean, hasUnread?: boolean) =>
    request<{ total: number }>(`/api/tasks/count?${new URLSearchParams({
      ...(status ? { status } : {}),
      ...(archivedOnly ? { archived_only: 'true' } : includeArchived ? { include_archived: 'true' } : {}),
      ...(projectId != null ? { project_id: String(projectId) } : {}),
      ...(starred != null ? { starred: String(starred) } : {}),
      ...(hasUnread != null ? { has_unread: String(hasUnread) } : {}),
    })}`),
  starTask: (id: number) =>
    request<Task>(`/api/tasks/${id}/star`, { method: 'POST' }),
  archiveTask: (id: number) =>
    request<Task>(`/api/tasks/${id}/archive`, { method: 'POST' }),
  markTaskRead: (id: number) =>
    request<Task>(`/api/tasks/${id}/read`, { method: 'POST' }),
  markTaskUnread: (id: number) =>
    request<Task>(`/api/tasks/${id}/unread`, { method: 'POST' }),
  stopTaskSession: (id: number) =>
    request<{ ok: boolean; stopped?: boolean; cleared_messages?: number; note?: string }>(`/api/tasks/${id}/stop-session`, { method: 'POST' }),
  listForkAnchors: (id: number) =>
    request<CodexForkAnchor[]>(`/api/tasks/${id}/fork-anchors`),
  forkTask: (
    id: number,
    anchor: { type: 'initial'; id?: never } | { type: 'user_message'; id: number },
    title?: string,
  ) =>
    request<Task>(`/api/tasks/${id}/fork`, {
      method: 'POST',
      body: JSON.stringify({ anchor, ...(title?.trim() ? { title: title.trim() } : {}) }),
    }),
  distillTask: (id: number, customInstruction?: string) =>
    request<{ task_id: number; suggested_name: string; content: string; provider: string; model: string }>(`/api/tasks/${id}/distill`, { method: 'POST', body: JSON.stringify({ custom_instruction: customInstruction || null }) }),
  saveDistilledSkill: (taskId: number, data: { name: string; description?: string; content: string }) =>
    request<{ id: number; name: string; description: string; content: string }>(`/api/tasks/${taskId}/distill/save`, { method: 'POST', body: JSON.stringify(data) }),
  createTask: (data: { id?: number; worker_id?: number; title?: string; description?: string; project_id?: number; priority?: number; target_branch?: string; mode?: string; todo_file_path?: string; max_iterations?: number; goal_condition?: string; goal_max_turns?: number; goal_evaluator_model?: string; image_paths?: string[]; file_paths?: string[]; attachments?: { url: string; name: string; is_image: boolean }[]; secret_ids?: number[]; provider?: string; model?: string; effort_level?: string; thinking_budget?: number | null; timeout_hours?: number | null; enable_workflows?: boolean; enabled_skills?: Record<string, boolean>; starred?: boolean; clone_from_task_id?: number }) =>
    request<Task>('/api/tasks', { method: 'POST', body: JSON.stringify(data) }),
  updateTask: (id: number, data: { worker_id?: number; title?: string; description?: string; priority?: number; enabled_skills?: Record<string, boolean>; model?: string; effort_level?: string; thinking_budget?: number | null; system_prompt_mode?: string | null; timeout_hours?: number | null; sort_order?: number | null }) =>
    request<Task>(`/api/tasks/${id}`, { method: 'PUT', body: JSON.stringify(data) }),
  deleteTask: (id: number) =>
    request<{ ok: boolean }>(`/api/tasks/${id}`, { method: 'DELETE' }),
  cancelTask: (id: number) =>
    request<Task>(`/api/tasks/${id}/cancel`, { method: 'POST' }),
  retryTask: (id: number) =>
    request<Task>(`/api/tasks/${id}/retry`, { method: 'POST' }),
  approvePlan: (id: number) =>
    request<Task>(`/api/tasks/${id}/plan/approve`, { method: 'POST' }),
  rejectPlan: (id: number) =>
    request<Task>(`/api/tasks/${id}/plan/reject`, { method: 'POST' }),
  // Instances
  listInstances: () => request<Instance[]>('/api/instances'),
  createInstance: (data: { name: string }) =>
    request<Instance>('/api/instances', { method: 'POST', body: JSON.stringify(data) }),
  deleteInstance: (id: number) =>
    request<{ ok: boolean }>(`/api/instances/${id}`, { method: 'DELETE' }),
  cleanupInstances: () =>
    request<{ ok: boolean; deleted: number; skipped_running: number[] }>('/api/instances/cleanup', { method: 'DELETE' }),
  stopInstance: (
    id: number,
    expectedTaskId: number,
    expectedPid: number | null,
    expectedStartedAt: string | null,
  ) =>
    request<{ ok: boolean }>(`/api/instances/${id}/stop`, {
      method: 'POST',
      body: JSON.stringify({
        expected_task_id: expectedTaskId,
        expected_pid: expectedPid,
        expected_started_at: expectedStartedAt,
      }),
    }),
  getInstanceLogs: (id: number, limit = 100, afterId?: number) =>
    request<LogEntry[]>(`/api/instances/${id}/logs?${new URLSearchParams({
      limit: String(limit),
      ...(afterId != null ? { after_id: String(afterId) } : {}),
    })}`),

  // Dispatcher
  dispatcherStatus: () =>
    request<{ running: boolean; active_tasks: Record<string, boolean> }>('/api/dispatcher/status'),
  startDispatcher: () =>
    request<{ ok: boolean }>('/api/dispatcher/start', { method: 'POST' }),
  stopDispatcher: () =>
    request<{ ok: boolean }>('/api/dispatcher/stop', { method: 'POST' }),

  // Chat (task-based)
  sendTaskChat: (taskId: number, message: string, filePaths?: string[], secretIds?: number[], model?: string | null) =>
    request<{ ok: boolean; pid: number; instance_id: number; session_id: string }>(`/api/tasks/${taskId}/chat`, { method: 'POST', body: JSON.stringify({ message, file_paths: filePaths, secret_ids: secretIds, ...(model ? { model } : {}) }) }),
  injectTaskMessage: (taskId: number, message: string) =>
    request<{ ok: boolean; injected: boolean }>(`/api/tasks/${taskId}/inject`, { method: 'POST', body: JSON.stringify({ message }) }),
  // touch=true 仅在用户真正打开聊天（首页加载）时传——后端以此更新访问排序；
  // 分页翻旧消息不传，避免后台轮询/旧版客户端把任务在列表里来回顶到最前
  getTaskChatHistory: (taskId: number, compact = true, limit = 0, beforeId = 0, touch = false) =>
    request<ChatMessage[]>(`/api/tasks/${taskId}/chat/history?compact=${compact}${limit ? `&limit=${limit}` : ''}${beforeId ? `&before_id=${beforeId}` : ''}${touch ? '&touch=true' : ''}`),
  getMessageDetail: (taskId: number, messageId: number) =>
    request<{ id: number; tool_input: string | null; tool_output: string | null; content: string | null }>(`/api/tasks/${taskId}/chat/${messageId}/detail`),

  // Files (local)
  listDir: (path: string) =>
    request<{ path: string; entries: { name: string; path: string; is_dir: boolean; size: number | null }[] }>(`/api/files/list?path=${encodeURIComponent(path)}`),
  readFile: (path: string) =>
    request<{ path: string; content: string; size: number }>(`/api/files/read?path=${encodeURIComponent(path)}`),
  uploadToDir: (targetDir: string, files: File[]): Promise<{ name: string; path: string; size: number }[]> => {
    const token = getToken();
    const formData = new FormData();
    formData.append('target_dir', targetDir);
    for (const file of files) formData.append('files', file);
    const controller = new AbortController();
    const totalSize = files.reduce((sum, f) => sum + f.size, 0);
    const timeoutMs = Math.max(120_000, Math.ceil(totalSize / 50_000) * 1000);
    const timeout = setTimeout(() => controller.abort(), timeoutMs);
    return fetch(`${getBase()}/api/files/upload`, {
      method: 'POST',
      headers: token ? { Authorization: `Bearer ${token}` } : {},
      body: formData,
      signal: controller.signal,
    }).then(async (res) => {
      clearTimeout(timeout);
      if (res.status === 401) { clearToken(); window.location.reload(); throw new Error('Unauthorized'); }
      if (!res.ok) { const err = await res.json().catch(() => ({ detail: res.statusText })); throw new Error(err.detail || res.statusText); }
      return res.json();
    }).catch((e) => {
      clearTimeout(timeout);
      if (e.name === 'AbortError') throw new Error(`Upload timed out. Total size: ${(totalSize / 1024 / 1024).toFixed(1)}MB`);
      throw e;
    });
  },

  // Git
  gitStatus: (path: string) =>
    request<{ path: string; branch: string; files: { path: string; status: string; x: string; y: string }[] }>(`/api/files/git/status?path=${encodeURIComponent(path)}`),
  gitDiff: (path: string, file?: string, staged?: boolean) => {
    let url = `/api/files/git/diff?path=${encodeURIComponent(path)}`;
    if (file) url += `&file=${encodeURIComponent(file)}`;
    if (staged) url += `&staged=true`;
    return request<{ path: string; diff: string; file: string | null; staged: boolean }>(url);
  },

  // Files (download)
  downloadFileUrl: (path: string) =>
    `${getBase()}/api/files/download?path=${encodeURIComponent(path)}`,

  // Files (SSH)
  sshListDir: (creds: { host: string; port: number; username: string; password?: string; key_path?: string }, path: string) =>
    request<{ path: string; entries: { name: string; path: string; is_dir: boolean; size: number | null }[] }>('/api/files/ssh/list', { method: 'POST', body: JSON.stringify({ ...creds, path }) }),
  sshReadFile: (creds: { host: string; port: number; username: string; password?: string; key_path?: string }, path: string) =>
    request<{ path: string; content: string; size: number }>('/api/files/ssh/read', { method: 'POST', body: JSON.stringify({ ...creds, path }) }),
  sshDownloadFile: (creds: { host: string; port: number; username: string; password?: string; key_path?: string }, path: string) => {
    const token = getToken();
    return fetch(`${getBase()}/api/files/ssh/download`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...(token ? { 'Authorization': `Bearer ${token}` } : {}),
      },
      body: JSON.stringify({ ...creds, path }),
    });
  },

  // Discussions
  listDiscussions: () => request<DiscussionListItem[]>('/api/discussions'),
  createDiscussion: (data: { title: string; project_id?: number; max_agents?: number; facilitator_model?: string; agent_model?: string }) =>
    request<DiscussionListItem>('/api/discussions', { method: 'POST', body: JSON.stringify(data) }),
  getDiscussion: (id: number) => request<DiscussionDetail>(`/api/discussions/${id}`),
  sendDiscussionMessage: (id: number, message: string) =>
    request<{ ok: boolean; agents: { id: number; role_name: string; status: string }[] }>(`/api/discussions/${id}/messages`, {
      method: 'POST',
      body: JSON.stringify({ message }),
    }),
  sendAgentChat: (discussionId: number, agentId: number, message: string) =>
    request<{ ok: boolean }>(`/api/discussions/${discussionId}/agents/${agentId}/chat`, {
      method: 'POST',
      body: JSON.stringify({ message }),
    }),
  triggerAgent: (discussionId: number, agentId: number) =>
    request<{ ok: boolean }>(`/api/discussions/${discussionId}/agents/${agentId}/trigger`, { method: 'POST' }),
  stopAgent: (discussionId: number, agentId: number) =>
    request<{ ok: boolean }>(`/api/discussions/${discussionId}/agents/${agentId}/stop`, { method: 'POST' }),
  getAgentEvents: (discussionId: number, agentId: number) =>
    request<DiscussionEventItem[]>(`/api/discussions/${discussionId}/agents/${agentId}/events`),
  addDiscussionAgent: (discussionId: number) =>
    request<{ ok: boolean; agent: { id: number; role_name: string; status: string } }>(`/api/discussions/${discussionId}/add-agent`, { method: 'POST' }),
  resumeAllAgents: (discussionId: number) =>
    request<{ ok: boolean; resumed: number }>(`/api/discussions/${discussionId}/resume-all`, { method: 'POST' }),
  deleteDiscussion: (id: number) =>
    request<{ ok: boolean }>(`/api/discussions/${id}`, { method: 'DELETE' }),

  // Quick Phrases
  listQuickPhrases: () => request<QuickPhrase[]>('/api/quick-phrases'),
  createQuickPhrase: (data: { label: string; content: string; sort_order?: number }) =>
    request<QuickPhrase>('/api/quick-phrases', { method: 'POST', body: JSON.stringify(data) }),
  updateQuickPhrase: (id: number, data: { label?: string; content?: string; sort_order?: number }) =>
    request<QuickPhrase>(`/api/quick-phrases/${id}`, { method: 'PUT', body: JSON.stringify(data) }),
  deleteQuickPhrase: (id: number) =>
    request<{ ok: boolean }>(`/api/quick-phrases/${id}`, { method: 'DELETE' }),

  // Monitor Sessions
  listMonitorSessions: (taskId: number) =>
    request<MonitorSession[]>(`/api/tasks/${taskId}/monitor-sessions`),
  getMonitorChecks: (taskId: number, sessionId: number) =>
    request<MonitorCheck[]>(`/api/tasks/${taskId}/monitor-sessions/${sessionId}/checks`),
  deleteMonitorSession: (taskId: number, sessionId: number) =>
    request<{ ok: boolean }>(`/api/tasks/${taskId}/monitor-sessions/${sessionId}`, { method: 'DELETE' }),

  // Sub-Agent Sessions (one-shot tasks)
  createSubAgentSession: (taskId: number, body: { name: string; prompt: string; context?: string; model?: string | null }) =>
    request<MonitorSession>(`/api/tasks/${taskId}/sub-agent-sessions`, {
      method: 'POST',
      body: JSON.stringify(body),
    }),
  listSubAgentSessions: (taskId: number) =>
    request<MonitorSession[]>(`/api/tasks/${taskId}/sub-agent-sessions`),
  deleteSubAgentSession: (taskId: number, sessionId: number) =>
    request<{ ok: boolean }>(`/api/tasks/${taskId}/sub-agent-sessions/${sessionId}`, { method: 'DELETE' }),

  // Permissions / Sub-Agents (legacy)
  resolvePermission: (taskId: number, requestId: string, behavior: 'allow' | 'deny') =>
    request<{ ok: boolean; behavior: string }>(`/api/tasks/${taskId}/permissions/${requestId}`, {
      method: 'POST',
      body: JSON.stringify({ behavior }),
    }),
  // ask_user 卡片回包 / 重连回填
  submitAskUser: (taskId: number, requestId: string, answers: AskUserAnswer[]) =>
    request<{ ok: boolean }>(`/api/tasks/${taskId}/ask-user/${requestId}`, {
      method: 'POST',
      body: JSON.stringify({ answers }),
    }),
  getAskUserPending: (taskId: number) =>
    request<{ pending: { request_id: string; questions: AskUserQuestion[] }[] }>(
      `/api/tasks/${taskId}/ask-user/pending`,
    ),
  // 全局：所有正在等待回答的提问（驱动跨页面通知）
  getAskUserPendingAll: () =>
    request<{ pending: { task_id: number; request_id: string; summary: string }[] }>(
      `/api/ask-user/pending`,
    ),
  getSubAgentSummary: (taskId: number) =>
    request<SubAgentSummary>(`/api/tasks/${taskId}/sub-agents/summary`),

  // PR Monitor
  getMonitoredRepos: () =>
    request<MonitoredRepo[]>('/api/pr-monitor/repos'),
  createMonitoredRepo: (data: { repo_full_name: string; project_id?: number; worker_id?: number; auto_merge?: boolean; provider?: string; review_model?: string; default_branch?: string; allowed_authors?: string[] }) =>
    request<MonitoredRepo>('/api/pr-monitor/repos', { method: 'POST', body: JSON.stringify(data) }),
  updateMonitoredRepo: (id: number, data: { project_id?: number; auto_merge?: boolean; provider?: string; review_model?: string | null; default_branch?: string; allowed_authors?: string[]; enabled?: boolean }) =>
    request<MonitoredRepo>(`/api/pr-monitor/repos/${id}`, { method: 'PUT', body: JSON.stringify(data) }),
  deleteMonitoredRepo: (id: number) =>
    request<{ ok: boolean }>(`/api/pr-monitor/repos/${id}`, { method: 'DELETE' }),
  toggleMonitoredRepo: (id: number) =>
    request<MonitoredRepo>(`/api/pr-monitor/repos/${id}/toggle`, { method: 'POST' }),
  regenerateSecret: (id: number) =>
    request<MonitoredRepo>(`/api/pr-monitor/repos/${id}/regenerate-secret`, { method: 'POST' }),
  getRepoReviews: (repoId: number, page = 1, size = 20) =>
    request<PRReview[]>(`/api/pr-monitor/repos/${repoId}/reviews?page=${page}&size=${size}`),
  getReviewDetail: (reviewId: number) =>
    request<PRReview>(`/api/pr-monitor/reviews/${reviewId}`),
  getWebhookInfo: () =>
    request<{ webhook_url: string | null }>('/api/pr-monitor/webhook-info'),

  // Workers (distributed)
  listWorkers: () => request<Worker[]>('/api/workers'),
  addWorkerAccount: (workerId: number, data: WorkerAccountInput) =>
    request<{ ok: boolean; status: string; slot?: string; provider?: WorkerProvider; account_id?: string }>(`/api/workers/${workerId}/pool/add`, { method: 'POST', body: JSON.stringify(data) }),
  workerAddStatus: (workerId: number, email: string, provider: WorkerProvider = 'codex') =>
    request<CodexLoginStatus & { provider?: WorkerProvider }>(`/api/workers/${workerId}/pool/add/${encodeURIComponent(email)}?provider=${provider}`),
  submitWorkerLoginOtp: (workerId: number, attemptId: string, challengeId: string, code: string) =>
    request<{ ok: boolean; status: CodexLoginStatusName }>(`/api/workers/${workerId}/pool/login-attempts/${encodeURIComponent(attemptId)}/otp`, {
      method: 'POST',
      body: JSON.stringify({ challenge_id: challengeId, code }),
    }),
  cancelWorkerLogin: (workerId: number, attemptId: string) =>
    request<{ ok: boolean; status: string }>(`/api/workers/${workerId}/pool/login-attempts/${encodeURIComponent(attemptId)}`, { method: 'DELETE' }),
  deleteWorkerAccount: (workerId: number, accountId: string, provider: WorkerProvider = 'codex') =>
    request<{ ok: boolean }>(`/api/workers/${workerId}/pool/${encodeURIComponent(accountId)}?provider=${provider}`, { method: 'DELETE' }),
  getWorkerPoolUsage: (id: number, provider: WorkerProvider = 'codex') =>
    request<WorkerPoolStatus>(`/api/workers/${id}/pool/usage?provider=${provider}`),
  getWorkerRuntimeSettings: (id: number) =>
    request<RuntimeSettings>(`/api/workers/${id}/settings/runtime`),
  updateWorkerRuntimeSettings: (id: number, data: Partial<RuntimeSettings>) =>
    request<RuntimeSettings>(`/api/workers/${id}/settings/runtime`, { method: 'PUT', body: JSON.stringify(data) }),
  getWorkerPool: (id: number, provider: WorkerProvider = 'codex') =>
    request<WorkerPoolStatus>(`/api/workers/${id}/pool?provider=${provider}`),
  createWorker: (data: { accounts: WorkerAccountInput[]; name?: string }) =>
    request<Worker>('/api/workers', { method: 'POST', body: JSON.stringify(data) }),
  getWorker: (id: number) => request<Worker>(`/api/workers/${id}`),
  getWorkerLogs: (id: number) => request<{ id: number; bootstrap_log: string | null }>(`/api/workers/${id}/logs`),
  stopWorker: (id: number) => request<Worker>(`/api/workers/${id}/stop`, { method: 'POST' }),
  startWorker: (id: number) => request<Worker>(`/api/workers/${id}/start`, { method: 'POST' }),
  destroyWorker: (id: number) => request<Worker>(`/api/workers/${id}/destroy`, { method: 'POST' }),
  retryWorker: (id: number) => request<Worker>(`/api/workers/${id}/retry`, { method: 'POST' }),
  renameWorker: (id: number, name: string) =>
    request<Worker>(`/api/workers/${id}/rename`, { method: 'PATCH', body: JSON.stringify({ name }) }),
  assignWorker: (id: number, ownerUserId: number | null) =>
    request<Worker>(`/api/workers/${id}/assign`, { method: 'PUT', body: JSON.stringify({ owner_user_id: ownerUserId }) }),

  // Team CCM
  getTeamUsers: () => request<TeamUser[]>('/api/team/users'),
  getTeamGroups: () => request<any[]>('/api/team/groups'),
  createTeamGroup: (name: string, description?: string) =>
    request<any>('/api/team/groups', { method: 'POST', body: JSON.stringify({ name, description }) }),
  updateTeamGroup: (id: number, name: string, description?: string) =>
    request<any>(`/api/team/groups/${id}`, { method: 'PUT', body: JSON.stringify({ name, description }) }),
  deleteTeamGroup: (id: number) =>
    request<{ ok: boolean }>(`/api/team/groups/${id}`, { method: 'DELETE' }),
  addTeamGroupMember: (groupId: number, userId: number) =>
    request<{ ok: boolean }>(`/api/team/groups/${groupId}/members`, { method: 'POST', body: JSON.stringify({ user_id: userId }) }),
  removeTeamGroupMember: (groupId: number, userId: number) =>
    request<{ ok: boolean }>(`/api/team/groups/${groupId}/members/${userId}`, { method: 'DELETE' }),
  teamShareProject: (projectId: number, targetType: string, targetId: number) =>
    request<{ ok: boolean }>(`/api/team/projects/${projectId}/share`, { method: 'POST', body: JSON.stringify({ target_type: targetType, target_id: targetId }) }),
  teamUnshareProject: (projectId: number, targetType: string, targetId: number) =>
    request<{ ok: boolean }>(`/api/team/projects/${projectId}/share`, { method: 'DELETE', body: JSON.stringify({ target_type: targetType, target_id: targetId }) }),
  teamGetProjectShares: (projectId: number) =>
    request<any[]>(`/api/team/projects/${projectId}/shares`),
  shareTaskTeam: (taskId: number, targetType: string, targetId: number, permission?: string) =>
    request<{ ok: boolean }>(`/api/team/tasks/${taskId}/share`, { method: 'POST', body: JSON.stringify({ target_type: targetType, target_id: targetId, permission: permission || 'chat' }) }),
  unshareTaskTeam: (taskId: number, targetType: string, targetId: number) =>
    request<{ ok: boolean }>(`/api/team/tasks/${taskId}/share`, { method: 'DELETE', body: JSON.stringify({ target_type: targetType, target_id: targetId }) }),
  getTaskSharesTeam: (taskId: number) =>
    request<any[]>(`/api/team/tasks/${taskId}/shares`),

  // Pool add account
  poolAddAccount: (data: { email: string; token: string; login_method?: string }) =>
    request<{ ok: boolean; status: string; account_id?: string }>('/api/pool/add', { method: 'POST', body: JSON.stringify(data) }),
  poolAddStatus: (email: string) =>
    request<{ status: string; detail?: string }>(`/api/pool/add/${encodeURIComponent(email)}`),
  getCcSettings: () =>
    request<{ settings: Record<string, unknown> }>('/api/pool/cc-settings'),
  putCcSettings: (settings: Record<string, unknown>) =>
    request<{ ok: boolean; synced: number; settings: Record<string, unknown> }>('/api/pool/cc-settings', { method: 'PUT', body: JSON.stringify({ settings }) }),

  // Codex Pool
  getCodexPoolStatus: () => request<CodexPoolUsageStatus>('/api/codex-pool/status'),
  getCodexPoolUsage: (force?: boolean) => request<CodexPoolUsageStatus>('/api/codex-pool/usage' + (force ? '?force=true' : '')),
  clearCodexPoolCooldown: (accountId: string) =>
    request<{ ok: boolean }>(`/api/codex-pool/accounts/${accountId}/clear-cooldown`, { method: 'POST' }),
  setCodexPoolPreferred: (accountId: string | null) =>
    request<{ ok: boolean; preferred: string | null }>('/api/codex-pool/preferred', { method: 'POST', body: JSON.stringify({ account_id: accountId }) }),
  codexPoolDeleteAccount: (accountId: string) =>
    request<{ ok: boolean }>(`/api/codex-pool/accounts/${accountId}`, { method: 'DELETE' }),
  codexPoolVerify: (accountId: string) =>
    request<any>(`/api/codex-pool/accounts/${accountId}/verify`),
  codexPoolRelogin: (accountId: string) =>
    request<{ ok: boolean; status: CodexLoginStatusName; attempt_id?: string }>(`/api/codex-pool/accounts/${accountId}/relogin`, { method: 'POST' }),
  codexPoolReloginStatus: (accountId: string) =>
    request<CodexLoginStatus>(`/api/codex-pool/accounts/${accountId}/relogin`),
  codexPoolAddAccount: (data: { email: string; token?: string; password?: string; login_method?: CodexLoginMethod }) =>
    request<{ ok: boolean; status: CodexLoginStatusName; account_id?: string; attempt_id?: string }>('/api/codex-pool/add', { method: 'POST', body: JSON.stringify(data) }),
  codexPoolAddStatus: (email: string) =>
    request<CodexLoginStatus>(`/api/codex-pool/add/${encodeURIComponent(email)}`),
  codexPoolSubmitOtp: (attemptId: string, challengeId: string, code: string) =>
    request<{ ok: boolean; status: CodexLoginStatusName }>(`/api/codex-pool/login-attempts/${attemptId}/otp`, {
      method: 'POST',
      body: JSON.stringify({ challenge_id: challengeId, code }),
    }),

  // User Skills
  listUserSkills: () => request<any[]>('/api/user-skills'),
  getUserSkill: (id: number) => request<any>(`/api/user-skills/${id}`),
  createUserSkill: (data: { name: string; description?: string; content?: string }) =>
    request<any>('/api/user-skills', { method: 'POST', body: JSON.stringify(data) }),
  updateUserSkill: (id: number, data: { name?: string; description?: string; content?: string }) =>
    request<any>(`/api/user-skills/${id}`, { method: 'PUT', body: JSON.stringify(data) }),
  deleteUserSkill: (id: number) =>
    request<{ ok: boolean }>(`/api/user-skills/${id}`, { method: 'DELETE' }),

  // System Update
  startUpdate: (data: { skip_frontend_build?: boolean; dry_run?: boolean; force?: boolean; branch?: string | null } = {}) =>
    request<any>('/api/system/update', { method: 'POST', body: JSON.stringify(data) }),
  getUpdateStatus: () =>
    request<any>('/api/system/update/status'),
  reconcileUpdateState: () =>
    request<UpdateReconcileResult>('/api/system/update/reconcile', { method: 'POST' }),
  repairUpdate: () =>
    request<{ update_id?: string; old_commit?: string; status?: string }>(
      '/api/system/update/repair',
      { method: 'POST', body: JSON.stringify({}) },
    ),
  restartService: () =>
    request<{ status?: string }>('/api/system/restart', { method: 'POST' }),
  rollbackUpdate: (data: { confirm_database_restore?: boolean } = {}) =>
    request<{ status?: string }>(
      '/api/system/update/rollback',
      { method: 'POST', body: JSON.stringify(data) },
    ),

  // System
  health: () => request<{ status: string; commit?: string }>('/api/system/health'),
  stats: () => request<{ tasks: Record<string, number>; running_instances: number }>('/api/system/stats'),
  config: () => request<{ default_provider: string; provider_options: string[]; default_model: string; model_options: string[]; default_codex_model: string; codex_model_options: string[]; default_effort: string; effort_options: string[]; claude_model_efforts: Record<string, string[]>; claude_model_context_windows: Record<string, number>; codex_effort_options: string[]; codex_model_efforts: Record<string, string[]> }>('/api/system/config'),
  listSkills: () => request<{ key: string; label: string; description: string; always: boolean; priority: number; tags: string[] }[]>('/api/system/skills'),
  listSkillsCached: () => listSkillsCached(),
  listUserSkillsCached: () => listUserSkillsCached(),
};
