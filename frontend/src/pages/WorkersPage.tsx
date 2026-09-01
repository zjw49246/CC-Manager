import { useCallback, useEffect, useRef, useState } from 'react';
import { api } from '../api/client';
import { useWebSocket } from '../hooks/useWebSocket';
import { useVisibilityAwareInterval } from '../hooks/useVisibilityAwareInterval';
import type {
  CodexLoginStatus,
  TeamUser,
  Worker,
  WorkerAccountInput,
  WorkerPoolAccount,
  WorkerPoolStatus,
  WorkerProvider,
} from '../api/client';
import {
  Pencil,
  Play,
  Plus,
  Power,
  RefreshCw,
  ScrollText,
  Server,
  Trash2,
  X,
} from '../components/icons';

const STATUS_COLORS: Record<string, string> = {
  creating: 'bg-blue-500/20 text-blue-400',
  bootstrapping: 'bg-blue-500/20 text-blue-400',
  ready: 'bg-green-500/20 text-green-400',
  error: 'bg-red-500/20 text-red-400',
  stopping: 'bg-yellow-500/20 text-yellow-400',
  stopped: 'bg-gray-500/20 text-gray-400',
  starting: 'bg-blue-500/20 text-blue-400',
  destroying: 'bg-red-500/20 text-red-400',
  terminated: 'bg-gray-500/20 text-gray-500',
};

const BUSY = new Set(['creating', 'bootstrapping', 'stopping', 'starting', 'destroying']);
const DESTROYABLE = new Set(['ready', 'stopped', 'error']);
const ACTIVE_WORKER_LOGINS = new Set([
  'running',
  'awaiting_otp',
  'verifying_otp',
  'finalizing',
  'cancelling',
]);

type WorkerAccountDraft = {
  provider: WorkerProvider;
  email: string;
  token: string;
  password: string;
  login_method: string;
};

type UsageBar = {
  label: string;
  utilization: number;
  resetsAt: string | number | null | undefined;
};

const emptyWorkerAccount = (): WorkerAccountDraft => ({
  provider: 'codex',
  email: '',
  token: '',
  password: '',
  login_method: '',
});

function preferredWorkerProvider(worker: Pick<Worker, 'accounts'>): WorkerProvider {
  if (worker.accounts?.some((account) => account.provider === 'codex')) return 'codex';
  // Historical Worker records did not persist provider and were Claude-only.
  if (worker.accounts?.some((account) => account.provider === 'claude' || !account.provider)) {
    return 'claude';
  }
  return 'codex';
}

function accountHasInput(account: WorkerAccountDraft): boolean {
  return Boolean(account.email.trim() || account.token.trim() || account.password);
}

function accountPayload(account: WorkerAccountDraft): WorkerAccountInput {
  const payload: WorkerAccountInput = {
    email: account.email.trim(),
    provider: account.provider,
  };
  if (account.token.trim()) payload.token = account.token.trim();
  // Passwords are opaque: preserve leading/trailing characters exactly.
  if (account.provider === 'codex' && account.password) payload.password = account.password;
  if (account.login_method) payload.login_method = account.login_method;
  return payload;
}

function formatQuotaReset(value: string | number | null | undefined): string {
  if (value == null || value === '') return '';
  const date = typeof value === 'number' ? new Date(value * 1000) : new Date(value);
  if (Number.isNaN(date.getTime())) return '';
  return date.toLocaleString(undefined, {
    month: 'numeric',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
}

function poolErrorText(error: string): string {
  if (error === 'no_credentials') return '未找到凭据文件';
  if (error === 'token_expired') return 'Token 过期';
  return error;
}

function accountUsageBars(account: WorkerPoolAccount, provider: WorkerProvider): UsageBar[] {
  if (provider === 'codex') {
    const quota = account.quota;
    if (!quota) return [];
    const bars: UsageBar[] = [];
    if (typeof quota.primary_used_percent === 'number') {
      bars.push({
        label: quota.primary_window_minutes === 300 ? '5h' : '主',
        utilization: quota.primary_used_percent,
        resetsAt: quota.primary_resets_at,
      });
    }
    if (typeof quota.secondary_used_percent === 'number') {
      bars.push({
        label: quota.secondary_window_minutes === 10080 ? '7d' : '副',
        utilization: quota.secondary_used_percent,
        resetsAt: quota.secondary_resets_at,
      });
    }
    return bars;
  }

  const usage = account.usage;
  if (!usage) return [];
  const bars: UsageBar[] = [];
  if (usage.five_hour) {
    bars.push({
      label: '5h',
      utilization: usage.five_hour.utilization,
      resetsAt: usage.five_hour.resets_at,
    });
  }
  if (usage.seven_day) {
    bars.push({
      label: '7d',
      utilization: usage.seven_day.utilization,
      resetsAt: usage.seven_day.resets_at,
    });
  }
  if (usage.seven_day_opus) {
    bars.push({
      label: 'Opus',
      utilization: usage.seven_day_opus.utilization,
      resetsAt: usage.seven_day_opus.resets_at,
    });
  }
  return bars;
}

function AddWorkerModal({
  onClose,
  onSaved,
}: {
  onClose: () => void;
  onSaved: () => void;
}) {
  const [name, setName] = useState('');
  const [accounts, setAccounts] = useState<WorkerAccountDraft[]>([emptyWorkerAccount()]);
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  const handleSubmit = async (event: React.FormEvent) => {
    event.preventDefault();
    setError(null);
    const activeAccounts = accounts.filter(accountHasInput);
    const invalidIndex = activeAccounts.findIndex((account) => !account.email.trim());
    if (invalidIndex !== -1) {
      setError(`账号 ${invalidIndex + 1} 的 Email 必填`);
      return;
    }
    const invalidCodexIndex = activeAccounts.findIndex(
      (account) => account.provider === 'codex' && !account.token.trim(),
    );
    if (invalidCodexIndex !== -1) {
      setError(`Codex 账号 ${invalidCodexIndex + 1} 的 Worker 自动登录需要邮箱 Token`);
      return;
    }
    const invalidClaudeIndex = activeAccounts.findIndex(
      (account) => account.provider === 'claude' && !account.token.trim(),
    );
    if (invalidClaudeIndex !== -1) {
      setError(`Claude 账号 ${invalidClaudeIndex + 1} 的接码 Token 必填`);
      return;
    }

    setSubmitting(true);
    try {
      await api.createWorker({
        name: name.trim(),
        accounts: activeAccounts.map(accountPayload),
      });
      onSaved();
      onClose();
    } catch (caught) {
      setError(String(caught));
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50 p-4">
      <div className="bg-gray-800 rounded-xl shadow-2xl w-full max-w-md">
        <div className="flex items-center justify-between px-5 py-4 border-b border-gray-700">
          <h3 className="text-foreground font-semibold">Add Worker</h3>
          <button type="button" onClick={onClose} className="text-gray-400 hover:text-gray-200">
            <X size={18} />
          </button>
        </div>
        <form onSubmit={handleSubmit} className="p-5 space-y-4">
          {error && <p className="text-red-400 text-sm">{error}</p>}
          <p className="text-xs text-gray-400">
            机型、镜像和子网可继承本机；SSH 公钥与专属安全组会自动配置。账号在 Worker 上独立登录，登录信息仅用于 bootstrap 与失败重试。
          </p>
          <div>
            <label className="block text-xs text-gray-400 mb-1">
              Worker 名称 *（也作为 AWS 实例名）
            </label>
            <input
              className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
              value={name}
              onChange={(event) => setName(event.target.value)}
              placeholder="如 worker-prod-1"
              required
            />
          </div>
          {accounts.map((account, index) => (
            <div key={index} className="flex gap-2 items-start">
              <div className="flex-1 space-y-2">
                <select
                  aria-label={`账号 ${index + 1} Provider`}
                  className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
                  value={account.provider}
                  onChange={(event) => {
                    const provider = event.target.value as WorkerProvider;
                    setAccounts(accounts.map((item, itemIndex) => (
                      itemIndex === index
                        ? {
                            ...item,
                            provider,
                            login_method: provider === 'claude' && item.login_method === 'mailcatcher'
                              ? ''
                              : item.login_method,
                          }
                        : item
                    )));
                  }}
                >
                  <option value="codex">Codex（默认）</option>
                  <option value="claude">Claude</option>
                </select>
                <input
                  className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
                  value={account.email}
                  placeholder={`账号 ${index + 1} Email`}
                  onChange={(event) => setAccounts(accounts.map((item, itemIndex) => (
                    itemIndex === index ? { ...item, email: event.target.value } : item
                  )))}
                />
                {account.provider === 'codex' && (
                  <input
                    className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
                    type="password"
                    value={account.password}
                    placeholder="OpenAI 密码（可选）"
                    onChange={(event) => setAccounts(accounts.map((item, itemIndex) => (
                      itemIndex === index ? { ...item, password: event.target.value } : item
                    )))}
                  />
                )}
                <input
                  className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
                  type="password"
                  value={account.token}
                  placeholder={account.provider === 'codex' ? '邮箱接码 Token *' : '接码 Token *'}
                  onChange={(event) => setAccounts(accounts.map((item, itemIndex) => (
                    itemIndex === index ? { ...item, token: event.target.value } : item
                  )))}
                />
                {account.provider === 'codex' && (
                  <p className="text-[11px] text-gray-500">
                    邮箱 Token 用于无人值守获取验证码；OpenAI 密码可选。
                  </p>
                )}
                <select
                  className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
                  value={account.login_method}
                  onChange={(event) => setAccounts(accounts.map((item, itemIndex) => (
                    itemIndex === index ? { ...item, login_method: event.target.value } : item
                  )))}
                >
                  <option value="">自动识别（按邮箱后缀）</option>
                  <option value="171mail">171mail（API 接码）</option>
                  {account.provider === 'codex' && (
                    <option value="mailcatcher">MailCatcher（Token 接码）</option>
                  )}
                  <option value="mailcom">mail.com（Chrome 接码）</option>
                  <option value="onet">Onet（Token 接码）</option>
                  <option value="gazeta">Gazeta（Token 接码）</option>
                </select>
              </div>
              {accounts.length > 1 && (
                <button
                  type="button"
                  className="text-gray-500 hover:text-red-400 mt-2"
                  onClick={() => setAccounts(accounts.filter((_, itemIndex) => itemIndex !== index))}
                >
                  <X size={16} />
                </button>
              )}
            </div>
          ))}
          <button
            type="button"
            onClick={() => setAccounts([...accounts, emptyWorkerAccount()])}
            className="text-xs text-indigo-400 hover:text-indigo-300"
          >
            + 再加一个账号
          </button>
          <div className="flex justify-end gap-2 pt-1">
            <button type="button" onClick={onClose} className="px-4 py-2 text-sm text-gray-300 hover:text-foreground">
              Cancel
            </button>
            <button
              type="submit"
              disabled={submitting || !name.trim()}
              className="px-4 py-2 text-sm bg-indigo-600 text-white rounded hover:bg-indigo-500 disabled:opacity-50"
            >
              {submitting ? 'Creating...' : 'Create'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

function LogsModal({ worker, onClose }: { worker: Worker; onClose: () => void }) {
  const [log, setLog] = useState('');
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    api.getWorkerLogs(worker.id)
      .then((result) => setLog(result.bootstrap_log || ''))
      .catch(() => {});
  }, [worker.id]);

  useWebSocket([`worker:${worker.id}`], (message) => {
    const data = (message.data || {}) as Record<string, unknown>;
    if (
      message.channel === `worker:${worker.id}`
      && data.worker_id === worker.id
      && typeof data.log_line === 'string'
    ) {
      setLog((previous) => previous + data.log_line);
    }
  });

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [log]);

  return (
    <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50 p-4">
      <div className="bg-gray-800 rounded-xl shadow-2xl w-full max-w-2xl">
        <div className="flex items-center justify-between px-5 py-4 border-b border-gray-700">
          <h3 className="text-foreground font-semibold">{worker.name} — Bootstrap Log</h3>
          <button type="button" onClick={onClose} className="text-gray-400 hover:text-gray-200">
            <X size={18} />
          </button>
        </div>
        <pre className="p-4 text-xs text-gray-300 font-mono overflow-auto max-h-[60vh] whitespace-pre-wrap">
          {log || '(暂无日志)'}
          <div ref={bottomRef} />
        </pre>
      </div>
    </div>
  );
}

function shortName(worker: Worker): string {
  return worker.name;
}

function WorkerCard({
  worker,
  onAction,
  users,
  isAdmin,
}: {
  worker: Worker;
  onAction: () => void;
  users: TeamUser[];
  isAdmin: boolean;
}) {
  const inferredPoolProvider = preferredWorkerProvider(worker);
  const [logsOpen, setLogsOpen] = useState(false);
  const [poolOpen, setPoolOpen] = useState(false);
  const [poolProvider, setPoolProvider] = useState<WorkerProvider>(inferredPoolProvider);
  const [pool, setPool] = useState<WorkerPoolStatus | null>(null);
  const [poolErr, setPoolErr] = useState<string | null>(null);
  const [addingPoolAccount, setAddingPoolAccount] = useState(false);
  const [poolAccountDraft, setPoolAccountDraft] = useState<WorkerAccountDraft>(emptyWorkerAccount());
  const [poolAccountError, setPoolAccountError] = useState<string | null>(null);
  const [poolAccountSubmitting, setPoolAccountSubmitting] = useState(false);
  const [poolLoginEmail, setPoolLoginEmail] = useState('');
  const [poolLoginState, setPoolLoginState] = useState<CodexLoginStatus | null>(null);
  const [poolOtp, setPoolOtp] = useState('');
  const [poolCancelSubmitting, setPoolCancelSubmitting] = useState(false);
  const [ptyEnabled, setPtyEnabled] = useState<boolean | null>(null);
  const [ptySwitching, setPtySwitching] = useState(false);
  const [editing, setEditing] = useState(false);
  const [editName, setEditName] = useState(worker.name);
  const editRef = useRef<HTMLInputElement>(null);
  const poolRequestRef = useRef(0);
  const busy = BUSY.has(worker.status);

  const currentUser = JSON.parse(localStorage.getItem('cc_user') || '{}');
  const canControl = isAdmin || worker.owner_user_id === currentUser.id;
  const hasKnownInstance = Boolean(worker.cloud_instance_id);
  const stable = DESTROYABLE.has(worker.status);
  const hasPendingDestroy = worker.bootstrap_step === 'destroy';
  const canRename = canControl && hasKnownInstance && stable && !hasPendingDestroy;
  // A provision failure can leave a DB record before an EC2 id is known.
  // Destroy is also the supported way to retire that record safely.
  const canDestroy = isAdmin && stable;
  const poolLoginActive = Boolean(
    poolLoginEmail
    && poolLoginState
    && ACTIVE_WORKER_LOGINS.has(poolLoginState.status),
  );

  useEffect(() => {
    if (editing && editRef.current) editRef.current.focus();
  }, [editing]);

  useEffect(() => {
    if (!editing) setEditName(worker.name);
  }, [editing, worker.name]);

  const saveRename = async () => {
    const trimmed = editName.trim();
    if (!canRename || !trimmed || trimmed === worker.name) {
      setEditing(false);
      setEditName(worker.name);
      return;
    }
    try {
      await api.renameWorker(worker.id, trimmed);
      onAction();
    } catch (caught) {
      window.alert(String(caught));
      setEditName(worker.name);
    }
    setEditing(false);
  };

  useEffect(() => {
    if (worker.status !== 'ready') return;
    api.getWorkerRuntimeSettings(worker.id)
      .then((settings) => setPtyEnabled(settings.use_pty_mode))
      .catch(() => {});
  }, [worker.id, worker.status]);

  useEffect(() => {
    if (!poolOpen) setPoolProvider(inferredPoolProvider);
  }, [inferredPoolProvider, poolOpen]);

  const loadPool = useCallback(async (provider: WorkerProvider) => {
    const requestId = ++poolRequestRef.current;
    setPool(null);
    setPoolErr(null);
    try {
      const result = await api.getWorkerPoolUsage(worker.id, provider);
      if (requestId === poolRequestRef.current) setPool(result);
    } catch (caught) {
      if (requestId === poolRequestRef.current) setPoolErr(String(caught));
    }
  }, [worker.id]);

  useEffect(() => {
    if (!poolLoginEmail || poolCancelSubmitting) return;
    let stopped = false;
    let timer: ReturnType<typeof setTimeout> | undefined;

    const poll = async () => {
      try {
        const state = await api.workerAddStatus(worker.id, poolLoginEmail, poolProvider);
        if (stopped) return;
        setPoolLoginState(state);
        if (state.status === 'success') {
          setPoolLoginEmail('');
          setPoolOtp('');
          setAddingPoolAccount(false);
          setPoolAccountDraft({ ...emptyWorkerAccount(), provider: poolProvider });
          await loadPool(poolProvider);
          return;
        }
        if (['failed', 'expired', 'cancelled'].includes(state.status)) {
          setPoolLoginEmail('');
          setPoolAccountError(state.detail || `账号登录${state.status}`);
          return;
        }
        timer = setTimeout(poll, 1500);
      } catch (caught) {
        if (!stopped) {
          setPoolAccountError(String(caught));
          timer = setTimeout(poll, 3000);
        }
      }
    };

    timer = setTimeout(poll, 300);
    return () => {
      stopped = true;
      if (timer) clearTimeout(timer);
    };
  }, [loadPool, poolCancelSubmitting, poolLoginEmail, poolProvider, worker.id]);

  const togglePool = async () => {
    if (poolOpen) {
      setPoolOpen(false);
      return;
    }
    setPoolOpen(true);
    await loadPool(poolProvider);
  };

  const selectPoolProvider = (provider: WorkerProvider) => {
    if (poolLoginEmail) return;
    setPoolProvider(provider);
    setAddingPoolAccount(false);
    setPoolAccountError(null);
    setPoolLoginState(null);
    setPoolAccountDraft({ ...emptyWorkerAccount(), provider });
    if (poolOpen) void loadPool(provider);
  };

  const addPoolAccount = async (event: React.FormEvent) => {
    event.preventDefault();
    setPoolAccountError(null);
    if (!poolAccountDraft.email.trim()) {
      setPoolAccountError('账号 Email 必填');
      return;
    }
    if (poolProvider === 'codex' && !poolAccountDraft.token.trim()) {
      setPoolAccountError('Worker 自动登录需要邮箱 Token');
      return;
    }
    if (poolProvider === 'claude' && !poolAccountDraft.token.trim()) {
      setPoolAccountError('Claude 账号的接码 Token 必填');
      return;
    }

    setPoolAccountSubmitting(true);
    try {
      const result = await api.addWorkerAccount(worker.id, accountPayload({
        ...poolAccountDraft,
        provider: poolProvider,
      }));
      if (result.status === 'success') {
        setAddingPoolAccount(false);
        setPoolAccountDraft({ ...emptyWorkerAccount(), provider: poolProvider });
        await loadPool(poolProvider);
      } else {
        setPoolLoginState({
          status: (result.status || 'running') as CodexLoginStatus['status'],
        });
        setPoolLoginEmail(poolAccountDraft.email.trim());
      }
    } catch (caught) {
      setPoolAccountError(String(caught));
    } finally {
      setPoolAccountSubmitting(false);
    }
  };

  const submitPoolOtp = async () => {
    if (!poolLoginState?.attempt_id || !poolLoginState.challenge_id) return;
    if (!/^\d{6}$/.test(poolOtp.trim())) {
      setPoolAccountError('请输入 6 位数字验证码');
      return;
    }
    setPoolAccountError(null);
    try {
      const result = await api.submitWorkerLoginOtp(
        worker.id,
        poolLoginState.attempt_id,
        poolLoginState.challenge_id,
        poolOtp.trim(),
      );
      setPoolOtp('');
      setPoolLoginState({ ...poolLoginState, status: result.status });
    } catch (caught) {
      setPoolAccountError(String(caught));
    }
  };

  const cancelPoolLogin = async () => {
    if (!poolLoginState?.attempt_id || poolCancelSubmitting) return;
    const previousState = poolLoginState;
    setPoolCancelSubmitting(true);
    setPoolLoginState({ ...poolLoginState, status: 'cancelling' });
    setPoolAccountError(null);
    try {
      const result = await api.cancelWorkerLogin(worker.id, poolLoginState.attempt_id);
      // Cancellation is asynchronous on the Manager: keep the email/state so
      // polling continues until the old login has finished its final DB write.
      setPoolLoginState({
        ...poolLoginState,
        status: result.status as CodexLoginStatus['status'],
      });
      setPoolOtp('');
    } catch (caught) {
      setPoolLoginState(previousState);
      setPoolAccountError(String(caught));
    } finally {
      setPoolCancelSubmitting(false);
    }
  };

  const act = async (
    operation: (id: number) => Promise<Worker>,
    confirmMessage?: string,
  ) => {
    if (confirmMessage && !window.confirm(confirmMessage)) return;
    try {
      await operation(worker.id);
      onAction();
    } catch (caught) {
      window.alert(String(caught));
    }
  };

  return (
    <div className="bg-gray-800 rounded-lg p-4 space-y-2.5">
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-2 min-w-0">
          <Server size={16} className="text-indigo-400 shrink-0" />
          {editing ? (
            <input
              ref={editRef}
              className="bg-gray-700 text-foreground text-sm font-medium rounded px-2 py-0.5 outline-none focus:ring-1 focus:ring-indigo-500 min-w-0"
              value={editName}
              onChange={(event) => setEditName(event.target.value)}
              onKeyDown={(event) => {
                if (event.key === 'Enter') void saveRename();
                if (event.key === 'Escape') {
                  setEditing(false);
                  setEditName(worker.name);
                }
              }}
              onBlur={() => void saveRename()}
            />
          ) : (
            <span
              className={`text-foreground font-medium truncate ${canRename ? 'cursor-pointer hover:text-indigo-300 group' : ''}`}
              title={canRename ? 'Click to rename' : worker.name}
              onClick={() => {
                if (canRename) {
                  setEditName(worker.name);
                  setEditing(true);
                }
              }}
            >
              {shortName(worker)}
              {canRename && <Pencil size={11} className="inline ml-1 opacity-0 group-hover:opacity-60" />}
            </span>
          )}
          <span className={`text-xs px-2 py-0.5 rounded-full shrink-0 ${STATUS_COLORS[worker.status] || 'bg-gray-500/20 text-gray-400'}`}>
            {worker.status}{busy && worker.bootstrap_step ? `: ${worker.bootstrap_step}` : ''}
          </span>
          {isAdmin && (
            <select
              value={worker.owner_user_id ?? ''}
              onChange={async (event) => {
                const ownerId = event.target.value ? Number(event.target.value) : null;
                try {
                  await api.assignWorker(worker.id, ownerId);
                  onAction();
                } catch {
                  // The next refresh keeps the authoritative owner.
                }
              }}
              className="text-xs bg-gray-700 text-gray-300 rounded px-1.5 py-0.5 border border-gray-600 shrink-0"
              title="Assign to user"
            >
              <option value="">Public Pool</option>
              {users.filter((user) => user.role === 'member').map((user) => (
                <option key={user.id} value={user.id}>{user.name}</option>
              ))}
            </select>
          )}
          {!isAdmin && worker.owner_user_id && (
            <span className="text-xs text-gray-500 shrink-0">
              {users.find((user) => user.id === worker.owner_user_id)?.name || ''}
            </span>
          )}
        </div>
        <div className="flex items-center gap-1 shrink-0">
          {worker.status === 'ready' && ptyEnabled !== null && canControl && (
            <button
              type="button"
              title={ptyEnabled ? 'PTY 模式：开（点击关闭）' : 'PTY 模式：关（点击开启）'}
              disabled={ptySwitching}
              onClick={async () => {
                if (ptyEnabled && !window.confirm('关闭 PTY 模式将回退到 claude -p 一次性进程。确定？')) return;
                setPtySwitching(true);
                try {
                  const settings = await api.updateWorkerRuntimeSettings(worker.id, {
                    use_pty_mode: !ptyEnabled,
                  });
                  setPtyEnabled(settings.use_pty_mode);
                } catch {
                  // Keep current state on a failed remote update.
                } finally {
                  setPtySwitching(false);
                }
              }}
              className={`flex items-center gap-0.5 px-1.5 py-0.5 rounded text-[10px] font-semibold ${ptyEnabled ? 'bg-green-600/30 text-green-400' : 'bg-gray-700 text-gray-400'}`}
            >
              PTY
            </button>
          )}
          {worker.status === 'ready' && canControl && (
            <button
              type="button"
              title="Worker 号池额度"
              onClick={() => void togglePool()}
              className={`flex items-center gap-0.5 px-1.5 py-0.5 rounded ${poolOpen ? 'bg-indigo-600/30 text-indigo-300' : 'bg-gray-700 text-gray-400'} hover:text-indigo-300 text-[10px] font-semibold`}
            >
              Pro
            </button>
          )}
          {canControl && (
            <button
              type="button"
              title="日志"
              onClick={() => setLogsOpen(true)}
              className="p-1.5 text-gray-400 hover:text-gray-200"
            >
              <ScrollText size={15} />
            </button>
          )}
          {canControl && worker.status === 'error' && !hasPendingDestroy && (
            <button
              type="button"
              title="重试 bootstrap"
              onClick={() => void act(api.retryWorker)}
              className="p-1.5 text-gray-400 hover:text-blue-400"
            >
              <RefreshCw size={15} />
            </button>
          )}
          {canControl && worker.status === 'ready' && (
            <button
              type="button"
              title="关机（EC2 stop，数据保留）"
              onClick={() => void act(
                api.stopWorker,
                `关机 ${shortName(worker)}？数据保留，停机期间不可派发任务。`,
              )}
              className="p-1.5 text-gray-400 hover:text-yellow-400"
            >
              <Power size={15} />
            </button>
          )}
          {canControl && worker.status === 'stopped' && (
            <button
              type="button"
              title="开机"
              onClick={() => void act(api.startWorker)}
              className="p-1.5 text-gray-400 hover:text-green-400"
            >
              <Play size={15} />
            </button>
          )}
          {canDestroy && (
            <button
              type="button"
              title="销毁（terminate EC2）"
              onClick={() => void act(
                api.destroyWorker,
                `销毁 ${shortName(worker)}？EC2 实例将被 terminate，不可恢复！`,
              )}
              className="p-1.5 text-gray-400 hover:text-red-400"
            >
              <Trash2 size={15} />
            </button>
          )}
        </div>
      </div>

      {isAdmin && (
        <div className="text-xs text-gray-400 flex flex-wrap gap-x-4 gap-y-1">
          {worker.private_ip && <span>内网 {worker.private_ip}</span>}
          {worker.cloud_instance_id && <span>{worker.cloud_instance_id}</span>}
          {worker.ccm_commit && <span title={worker.ccm_commit}>@{worker.ccm_commit.slice(0, 8)}</span>}
          {worker.last_heartbeat && (
            <span>心跳 {new Date(`${worker.last_heartbeat}Z`).toLocaleTimeString()}</span>
          )}
        </div>
      )}

      {poolOpen && (
        <div className="bg-gray-900/60 rounded p-3 space-y-2">
          <div role="tablist" aria-label="Worker 号池 Provider" className="flex gap-1">
            {(['codex', 'claude'] as WorkerProvider[]).map((provider) => (
              <button
                key={provider}
                type="button"
                role="tab"
                aria-selected={poolProvider === provider}
                disabled={Boolean(poolLoginEmail)}
                onClick={() => selectPoolProvider(provider)}
                className={`rounded px-2 py-1 text-xs ${poolProvider === provider ? 'bg-indigo-600 text-white' : 'bg-gray-800 text-gray-400'}`}
              >
                {provider === 'codex' ? 'Codex' : 'Claude'}
              </button>
            ))}
          </div>

          {poolErr ? (
            <span className="text-xs text-red-400 break-all">{poolErr}</span>
          ) : pool === null ? (
            <span className="text-xs text-gray-500">加载额度…</span>
          ) : (
            <>
              <div className="flex items-center gap-2 text-xs text-gray-500">
                <span>{pool.accounts?.length || 0} 个账号</span>
                {pool.available != null && <span>· {pool.available}/{pool.total} 可用</span>}
              </div>
              {pool.accounts?.length ? pool.accounts.map((account) => {
                const dot = !account.enabled
                  ? 'bg-gray-500'
                  : account.available
                    ? 'bg-green-500'
                    : 'bg-yellow-500';
                const bars = accountUsageBars(account, poolProvider);
                const planType = account.plan_type || account.subscription_type;
                const quotaError = poolProvider === 'codex'
                  ? account.quota_error
                  : account.usage_error;
                return (
                  <div key={account.id} className="rounded-lg border border-gray-700 p-3 space-y-2">
                    <div className="flex items-center gap-2">
                      <span className={`h-2 w-2 shrink-0 rounded-full ${dot}`} />
                      <span className="text-sm font-medium text-foreground truncate">{account.id}</span>
                      {planType && (
                        <span className="px-1.5 py-0.5 rounded bg-indigo-600/30 text-indigo-300 text-[10px] font-semibold uppercase">
                          {planType}
                        </span>
                      )}
                      <div className="ml-auto flex items-center gap-1">
                        <button
                          type="button"
                          onClick={async () => {
                            if (!window.confirm(`从 ${poolProvider === 'codex' ? 'Codex' : 'Claude'} 号池删除 ${account.id}（${account.email || ''}）？`)) return;
                            try {
                              await api.deleteWorkerAccount(worker.id, account.id, poolProvider);
                              await loadPool(poolProvider);
                            } catch (caught) {
                              window.alert(String(caught));
                            }
                          }}
                          className="text-[10px] px-1.5 py-0.5 rounded border border-gray-600 text-gray-400 hover:text-red-400 hover:border-red-500"
                        >
                          删除
                        </button>
                      </div>
                    </div>
                    {account.email && <div className="text-xs text-gray-500 truncate">{account.email}</div>}
                    {bars.length > 0 ? (
                      <div className="space-y-1.5">
                        {bars.map((bar, index) => {
                          const utilization = Number.isFinite(bar.utilization) ? bar.utilization : 0;
                          const barColor = utilization >= 85
                            ? 'bg-red-500'
                            : utilization >= 60
                              ? 'bg-yellow-500'
                              : 'bg-green-500';
                          const textColor = utilization >= 85
                            ? 'text-red-400'
                            : utilization >= 60
                              ? 'text-yellow-400'
                              : 'text-green-400';
                          return (
                            <div key={`${bar.label}-${index}`} className="flex items-center gap-2 text-xs">
                              <span className="w-7 shrink-0 text-gray-500">{bar.label}</span>
                              <div className="flex-1 h-2 rounded-full bg-gray-700 overflow-hidden">
                                <div
                                  className={`h-full rounded-full ${barColor}`}
                                  style={{ width: `${Math.max(0, Math.min(100, utilization))}%` }}
                                />
                              </div>
                              <span className={`w-10 shrink-0 text-right font-medium ${textColor}`}>
                                {utilization.toFixed(0)}%
                              </span>
                              <span className="w-24 shrink-0 text-right text-gray-500">
                                {formatQuotaReset(bar.resetsAt)}
                              </span>
                            </div>
                          );
                        })}
                      </div>
                    ) : quotaError ? (
                      <div className="text-xs text-red-400">{poolErrorText(quotaError)}</div>
                    ) : null}
                  </div>
                );
              }) : <div className="text-xs text-gray-500">暂无账号</div>}

              <div className="pt-1 border-t border-gray-700">
                <button
                  type="button"
                  onClick={() => {
                    if (poolLoginActive) return;
                    setAddingPoolAccount((value) => !value);
                    setPoolAccountError(null);
                    setPoolLoginState(null);
                    setPoolAccountDraft({ ...emptyWorkerAccount(), provider: poolProvider });
                  }}
                  className="text-xs text-indigo-400 hover:text-indigo-300"
                >
                  + 添加 {poolProvider === 'codex' ? 'Codex' : 'Claude'} 账号
                </button>
                {addingPoolAccount && (
                  <form onSubmit={addPoolAccount} className="mt-2 space-y-2 rounded border border-gray-700 p-2">
                    {poolAccountError && <p className="text-xs text-red-400">{poolAccountError}</p>}
                    {poolLoginActive && poolLoginState && (
                      <p className="text-xs text-blue-400">
                        {poolLoginState.status === 'awaiting_otp'
                          ? 'OpenAI 正在等待邮箱验证码'
                          : poolLoginState.status === 'verifying_otp'
                            ? '正在验证邮箱验证码…'
                            : poolLoginState.status === 'finalizing'
                              ? '登录成功，正在保存账号…'
                              : poolLoginState.status === 'cancelling'
                                ? '正在取消登录…'
                                : `正在自动登录 ${poolProvider === 'codex' ? 'Codex' : 'Claude'}…`}
                      </p>
                    )}
                    <input
                      aria-label="Worker 号池账号 Email"
                      className="w-full rounded bg-gray-700 px-2 py-1.5 text-xs text-foreground outline-none focus:ring-1 focus:ring-indigo-500"
                      value={poolAccountDraft.email}
                      disabled={poolLoginActive}
                      placeholder="账号 Email"
                      onChange={(event) => setPoolAccountDraft({
                        ...poolAccountDraft,
                        email: event.target.value,
                      })}
                    />
                    {poolProvider === 'codex' && (
                      <input
                        aria-label="Worker 号池 OpenAI 密码"
                        className="w-full rounded bg-gray-700 px-2 py-1.5 text-xs text-foreground outline-none focus:ring-1 focus:ring-indigo-500"
                        type="password"
                        disabled={poolLoginActive}
                        value={poolAccountDraft.password}
                        placeholder="OpenAI 密码（可选）"
                        onChange={(event) => setPoolAccountDraft({
                          ...poolAccountDraft,
                          password: event.target.value,
                        })}
                      />
                    )}
                    <input
                      aria-label="Worker 号池接码 Token"
                      className="w-full rounded bg-gray-700 px-2 py-1.5 text-xs text-foreground outline-none focus:ring-1 focus:ring-indigo-500"
                      type="password"
                      disabled={poolLoginActive}
                      value={poolAccountDraft.token}
                      placeholder={poolProvider === 'codex' ? '邮箱接码 Token *' : '接码 Token *'}
                      onChange={(event) => setPoolAccountDraft({
                        ...poolAccountDraft,
                        token: event.target.value,
                      })}
                    />
                    <select
                      aria-label="Worker 号池登录方式"
                      className="w-full rounded bg-gray-700 px-2 py-1.5 text-xs text-foreground outline-none focus:ring-1 focus:ring-indigo-500"
                      value={poolAccountDraft.login_method}
                      disabled={poolLoginActive}
                      onChange={(event) => setPoolAccountDraft({
                        ...poolAccountDraft,
                        login_method: event.target.value,
                      })}
                    >
                      <option value="">自动识别</option>
                      <option value="171mail">171mail</option>
                      {poolProvider === 'codex' && <option value="mailcatcher">MailCatcher</option>}
                      <option value="mailcom">mail.com</option>
                      <option value="onet">Onet</option>
                      <option value="gazeta">Gazeta</option>
                    </select>
                    {poolLoginState?.status === 'awaiting_otp' && (
                      <div className="flex gap-2">
                        <input
                          aria-label="Worker OpenAI 邮箱验证码"
                          className="min-w-0 flex-1 rounded bg-gray-700 px-2 py-1.5 text-xs text-foreground outline-none focus:ring-1 focus:ring-indigo-500"
                          inputMode="numeric"
                          autoComplete="one-time-code"
                          maxLength={6}
                          value={poolOtp}
                          placeholder="6 位验证码"
                          onChange={(event) => setPoolOtp(event.target.value.replace(/\D/g, '').slice(0, 6))}
                        />
                        <button
                          type="button"
                          onClick={() => void submitPoolOtp()}
                          disabled={poolOtp.length !== 6}
                          className="rounded bg-indigo-600 px-2 py-1 text-xs text-white disabled:opacity-50"
                        >
                          提交验证码
                        </button>
                      </div>
                    )}
                    <div className="flex justify-end gap-2">
                      <button
                        type="button"
                        onClick={() => {
                          if (poolLoginActive) void cancelPoolLogin();
                          else setAddingPoolAccount(false);
                        }}
                        disabled={
                          (poolLoginActive && !poolLoginState?.attempt_id)
                          || poolCancelSubmitting
                        }
                        className="px-2 py-1 text-xs text-gray-400 hover:text-gray-200 disabled:opacity-50"
                      >
                        {poolCancelSubmitting
                          ? '取消中…'
                          : poolLoginActive
                            ? '取消登录'
                            : '取消'}
                      </button>
                      <button
                        type="submit"
                        disabled={poolAccountSubmitting || poolLoginActive}
                        className="rounded bg-indigo-600 px-2 py-1 text-xs text-white disabled:opacity-50"
                      >
                        {poolAccountSubmitting ? '登录中…' : '开始登录'}
                      </button>
                    </div>
                  </form>
                )}
              </div>
            </>
          )}
        </div>
      )}

      {worker.bootstrap_error && (
        <p className="text-xs text-red-400 break-all">
          [{worker.bootstrap_step}] {worker.bootstrap_error}
        </p>
      )}
      {logsOpen && <LogsModal worker={worker} onClose={() => setLogsOpen(false)} />}
    </div>
  );
}

export default function WorkersPage() {
  const [workers, setWorkers] = useState<Worker[]>([]);
  const [users, setUsers] = useState<TeamUser[]>([]);
  const [loading, setLoading] = useState(true);
  const [adding, setAdding] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const currentUser = JSON.parse(localStorage.getItem('cc_user') || '{}');
  const isAdmin = currentUser.role === 'admin'
    || currentUser.role === 'super_admin'
    || !currentUser.role;

  const load = useCallback(() => {
    api.listWorkers()
      .then((result) => {
        setWorkers(result);
        setError(null);
      })
      .catch((caught) => setError(String(caught)))
      .finally(() => setLoading(false));
    if (isAdmin) {
      api.getTeamUsers().then(setUsers).catch(() => {});
    }
  }, [isAdmin]);

  const workerWs = useWebSocket(['workers'], (message) => {
    if (message.channel === 'workers') load();
  });
  const isConnected = workerWs?.isConnected ?? false;
  // WebSocket is primary; this slow poll only covers disconnects or lost events.
  useEffect(() => { void load(); }, [load]);
  useVisibilityAwareInterval(load, isConnected ? 120000 : 30000, true, false);

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold text-foreground">
          {isAdmin ? 'Workers' : 'My Workers'}
        </h2>
        {isAdmin && (
          <button
            type="button"
            onClick={() => setAdding(true)}
            className="flex items-center gap-1.5 px-3 py-1.5 text-sm bg-indigo-600 text-white rounded hover:bg-indigo-500"
          >
            <Plus size={15} /> Add Worker
          </button>
        )}
      </div>
      {error && <p className="text-red-400 text-sm">{error}</p>}
      {loading ? (
        <p className="text-gray-400 text-sm">Loading...</p>
      ) : workers.length === 0 ? (
        <div className="text-center py-16 text-gray-500">
          <Server size={32} className="mx-auto mb-3 opacity-50" />
          <p className="text-sm">还没有 Worker。点击 Add Worker 开一台和本机同配置的 EC2。</p>
        </div>
      ) : (
        <div className="grid gap-3 sm:grid-cols-2">
          {workers.map((worker) => (
            <WorkerCard
              key={worker.id}
              worker={worker}
              onAction={load}
              users={users}
              isAdmin={isAdmin}
            />
          ))}
        </div>
      )}
      {adding && <AddWorkerModal onClose={() => setAdding(false)} onSaved={load} />}
    </div>
  );
}
