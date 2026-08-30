import { useCallback, useEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { Eye, EyeOff, Plus, RefreshCw, X, Users, Settings } from '../icons';
import { api, isApiRequestError } from '../../api/client';
import type {
  ApiAccountCleanupConflictDetail,
  ApiAccountCleanupDiagnostics,
  ApiAccountProvider,
  CloudRouterAccount,
  CloudRouterApiQuota,
  CloudRouterUsageBreakdown,
  CloudRouterUsageMetrics,
  CodexLoginMethod,
  CodexLoginStatus,
  CodexPoolAccountUsage,
  CodexPoolSettings,
  CodexPoolUsageStatus,
  PoolAccountUsage,
  PoolUsageStatus,
  PoolUsageWindow,
} from '../../api/client';

const ACTIVE_CODEX_LOGIN_STATUSES = new Set([
  'running', 'awaiting_otp', 'verifying_otp', 'finalizing',
]);

const API_AUTH_KINDS = new Set(['cloudrouter_api', 'apex_api', 'apibest_api']);

function isApiAuthKind(authKind: string | null | undefined): boolean {
  return authKind != null && API_AUTH_KINDS.has(authKind);
}

function resolveApiProvider(
  authKind: string | null | undefined,
  provider: ApiAccountProvider | null | undefined,
): ApiAccountProvider {
  if (provider) return provider;
  if (authKind === 'apex_api') return 'apex';
  if (authKind === 'apibest_api') return 'apibest';
  return 'cloudrouter';
}

function isApiCleanupPending(
  projection: ApiAccountCleanupDiagnostics,
  catalog?: ApiAccountCleanupDiagnostics,
): boolean {
  return projection.cleanup_pending === true || catalog?.cleanup_pending === true;
}

function mergeApiCleanupDiagnostics(
  projection: ApiAccountCleanupDiagnostics,
  catalog?: ApiAccountCleanupDiagnostics,
): ApiAccountCleanupDiagnostics {
  return {
    cleanup_pending: isApiCleanupPending(projection, catalog),
    cleanup_code: catalog?.cleanup_code ?? projection.cleanup_code ?? null,
    cleanup_reason: catalog?.cleanup_reason ?? projection.cleanup_reason ?? null,
    cleanup_last_attempt_at: catalog?.cleanup_last_attempt_at
      ?? projection.cleanup_last_attempt_at
      ?? null,
    cleanup_last_error_at: catalog?.cleanup_last_error_at
      ?? projection.cleanup_last_error_at
      ?? null,
  };
}

const API_CLEANUP_CODE_SUMMARIES: Record<string, string> = {
  credential_busy: '账号仍有额度或凭据请求正在进行，请等待请求结束后重试。',
  runtime_busy: '账号仍被运行中的任务或监控使用，请先停止相关运行再重试。',
  migration_busy: '账号正在参与任务上下文迁移，请等待迁移完成后重试。',
  runtime_verification_failed: '系统暂时无法安全确认账号是否空闲，请检查运行服务后重试。',
  cleanup_blocked: '账号清理仍被后台运行状态阻塞，请根据后端详情解除阻塞后重试。',
};

function apiCleanupSummary(code: string | null | undefined): string {
  if (code && API_CLEANUP_CODE_SUMMARIES[code]) {
    return API_CLEANUP_CODE_SUMMARIES[code];
  }
  return '账号删除仍受阻，请确认相关任务与运行服务状态后重试。';
}

function apiCleanupConflictDiagnostics(error: unknown): ApiAccountCleanupDiagnostics | null {
  if (!isApiRequestError(error) || error.status !== 409) return null;
  const detail = error.detail as ApiAccountCleanupConflictDetail | null;
  if (detail && typeof detail === 'object') {
    const reason = typeof detail.reason === 'string' ? detail.reason.trim() : '';
    const message = typeof detail.message === 'string' ? detail.message.trim() : '';
    const fallback = typeof detail.error === 'string' ? detail.error.trim() : '';
    const code = typeof detail.code === 'string' ? detail.code.trim() : '';
    return {
      cleanup_pending: detail.cleanup_pending === true,
      cleanup_code: code || null,
      cleanup_reason: reason || message || fallback || error.message.trim() || null,
    };
  }
  return {
    cleanup_pending: true,
    cleanup_reason: error.message.trim() || null,
  };
}

function barColor(utilization: number): string {
  if (utilization >= 90) return 'bg-red-500';
  if (utilization >= 60) return 'bg-yellow-500';
  return 'bg-green-500';
}

function textColor(utilization: number): string {
  if (utilization >= 90) return 'text-red-400';
  if (utilization >= 60) return 'text-yellow-400';
  return 'text-green-400';
}

function formatReset(resetsAt: string | null): string {
  if (!resetsAt) return '';
  const d = new Date(resetsAt);
  if (isNaN(d.getTime())) return '';
  return d.toLocaleString(undefined, { month: 'numeric', day: 'numeric', hour: '2-digit', minute: '2-digit' });
}

function UsageBar({ label, window: w }: { label: string; window: PoolUsageWindow | null }) {
  if (!w || w.utilization == null) return null;
  const pct = Math.min(100, Math.max(0, w.utilization));
  return (
    <div className="flex items-center gap-2 text-xs">
      <span className="w-7 shrink-0 text-gray-500">{label}</span>
      <div className="flex-1 h-2 rounded-full bg-gray-700 overflow-hidden">
        <div className={`h-full rounded-full ${barColor(pct)}`} style={{ width: `${pct}%` }} />
      </div>
      <span className={`w-10 shrink-0 text-right font-medium ${textColor(pct)}`}>{pct.toFixed(0)}%</span>
      <span className="w-24 shrink-0 text-right text-gray-500" title="额度重置时间">{formatReset(w.resets_at)}</span>
    </div>
  );
}

function formatApiAmount(value: number | null | undefined, currency?: string | null): string {
  if (value == null || !Number.isFinite(value)) return '—';
  // CloudRouter uses -1 for an unrestricted wallet balance. Rendering it as
  // a negative currency amount would incorrectly look overdrawn/exhausted.
  if (value === -1) return '无限';
  const formatted = value.toLocaleString(undefined, { maximumFractionDigits: 4 });
  const unit = currency?.trim();
  if (!unit) return formatted;
  if (['usd', '$'].includes(unit.toLowerCase())) return `$${formatted}`;
  if (['credit', 'credits'].includes(unit.toLowerCase())) return `${formatted} credits`;
  return `${formatted} ${unit}`;
}

function formatApiTimestamp(value: string | number | null | undefined): string {
  if (value == null || value === '') return '无法确认';
  let timestamp: string | number = value;
  if (typeof value === 'number') {
    timestamp = value < 1_000_000_000_000 ? value * 1000 : value;
  } else if (/^\d+(?:\.\d+)?$/.test(value)) {
    const numeric = Number(value);
    timestamp = numeric < 1_000_000_000_000 ? numeric * 1000 : numeric;
  }
  const parsed = new Date(timestamp);
  if (Number.isNaN(parsed.getTime())) return '无法确认';
  return parsed.toLocaleString(undefined, {
    year: 'numeric',
    month: 'numeric',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
}

function finiteMetric(value: number | null | undefined): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function metricTokenTotal(metrics: CloudRouterUsageMetrics): number | null {
  const explicit = finiteMetric(metrics.total_tokens);
  if (explicit != null) return explicit;
  const parts = [
    metrics.input_tokens,
    metrics.output_tokens,
    metrics.cache_creation_tokens,
    metrics.cache_write_tokens,
    metrics.cache_read_tokens,
  ].map(finiteMetric).filter((value): value is number => value != null);
  return parts.length > 0 ? parts.reduce((sum, value) => sum + value, 0) : null;
}

function UsageMetricSummary({
  label,
  metrics,
  currency,
}: {
  label: string;
  metrics: CloudRouterUsageMetrics | null | undefined;
  currency?: string | null;
}) {
  if (!metrics) return null;
  const cost = finiteMetric(metrics.actual_cost ?? metrics.cost);
  const requests = finiteMetric(metrics.requests);
  const tokens = metricTokenTotal(metrics);
  if (cost == null && requests == null && tokens == null) return null;
  return (
    <div className="rounded border border-gray-700/70 bg-gray-900/40 px-2 py-1.5">
      <div className="text-[10px] font-medium text-gray-300">{label}</div>
      <div className="mt-0.5 flex flex-wrap gap-x-3 gap-y-0.5 text-[10px] text-gray-500">
        {cost != null && <span>费用 <span className="text-gray-300">{formatApiAmount(cost, currency || 'USD')}</span></span>}
        {requests != null && <span>请求 <span className="text-gray-300">{requests.toLocaleString()}</span></span>}
        {tokens != null && <span>Tokens <span className="text-gray-300">{tokens.toLocaleString()}</span></span>}
      </div>
    </div>
  );
}

function usageBreakdownRows(
  value: CloudRouterUsageBreakdown | null | undefined,
  labelField: 'model' | 'date',
): Array<{ key: string; label: string; metrics: CloudRouterUsageMetrics }> {
  if (!value) return [];
  const looksLikeOneRecord = !Array.isArray(value) && (
    typeof value.model === 'string'
    || typeof value.date === 'string'
    || [
      value.requests,
      value.total_tokens,
      value.actual_cost,
      value.cost,
    ].some((metric) => typeof metric === 'number')
  );
  const entries: Array<readonly [string, CloudRouterUsageMetrics]> = Array.isArray(value)
    ? value.map((metrics, index) => [String(index), metrics] as const)
    : looksLikeOneRecord
      ? [['0', value as CloudRouterUsageMetrics]]
      : Object.entries(value) as Array<[string, CloudRouterUsageMetrics]>;
  return entries
    .filter((entry): entry is readonly [string, CloudRouterUsageMetrics] =>
      Boolean(entry[1]) && typeof entry[1] === 'object' && !Array.isArray(entry[1])
    )
    .slice(0, 20)
    .map(([key, metrics]) => {
      const supplied = metrics[labelField];
      return {
        key,
        label: typeof supplied === 'string' && supplied.trim() ? supplied.trim() : key,
        metrics,
      };
    });
}

function ApiUsageDetails({
  quota,
  currency,
}: {
  quota: CloudRouterApiQuota;
  currency?: string | null;
}) {
  const usage = quota.usage;
  const daily = usageBreakdownRows(usage?.daily_usage, 'date');
  const models = usageBreakdownRows(usage?.model_stats, 'model');
  const hasSummary = Boolean(
    usage?.today && (
      finiteMetric(usage.today.actual_cost ?? usage.today.cost) != null
      || finiteMetric(usage.today.requests) != null
      || metricTokenTotal(usage.today) != null
    )
  ) || Boolean(
    usage?.total && (
      finiteMetric(usage.total.actual_cost ?? usage.total.cost) != null
      || finiteMetric(usage.total.requests) != null
      || metricTokenTotal(usage.total) != null
    )
  );
  if (!hasSummary && daily.length === 0 && models.length === 0) return null;

  return (
    <div className="space-y-1.5">
      <div className="grid grid-cols-1 gap-1 sm:grid-cols-2">
        <UsageMetricSummary label="当前 Key 今日用量" metrics={usage?.today} currency={currency} />
        <UsageMetricSummary label="当前 Key 累计用量" metrics={usage?.total} currency={currency} />
      </div>
      {daily.length > 0 && (
        <details className="rounded border border-gray-700/60 bg-gray-900/30 px-2 py-1">
          <summary className="cursor-pointer text-[10px] text-gray-400">逐日用量（最多显示 20 条）</summary>
          <div className="mt-1 space-y-1">
            {daily.map((row) => (
              <UsageMetricSummary key={`${row.key}:${row.label}`} label={row.label} metrics={row.metrics} currency={currency} />
            ))}
          </div>
        </details>
      )}
      {models.length > 0 && (
        <details className="rounded border border-gray-700/60 bg-gray-900/30 px-2 py-1">
          <summary className="cursor-pointer text-[10px] text-gray-400">逐模型用量（最多显示 20 条）</summary>
          <div className="mt-1 space-y-1">
            {models.map((row) => (
              <UsageMetricSummary key={`${row.key}:${row.label}`} label={row.label} metrics={row.metrics} currency={currency} />
            ))}
          </div>
        </details>
      )}
    </div>
  );
}

function ApiQuotaPanel({ quota, onRefresh }: {
  quota: CloudRouterApiQuota | null | undefined;
  onRefresh: () => void;
}) {
  if (!quota) {
    return (
      <div className="space-y-1.5 text-xs text-gray-500">
        <div className="flex items-center gap-2">
          <span>额度无法确认</span>
          <button
            onClick={onRefresh}
            className="shrink-0 text-[10px] px-1.5 py-0.5 rounded border border-gray-600 text-gray-400 hover:text-foreground"
          >
            刷新
          </button>
        </div>
        <div>到期时间：无法确认</div>
        <div>剩余天数：无法确认</div>
      </div>
    );
  }

  const state = quota.state || quota.status || 'unknown';
  const currency = quota.currency || quota.unit;
  const normalizedState = state.trim().toLowerCase();
  const unlimited = quota.unlimited === true
    || quota.mode?.trim().toLowerCase() === 'unrestricted';
  const expiryNotSupplied = unlimited
    && quota.expires_at == null
    && quota.days_until_expiry == null;
  const total = quota.quota;
  const hasSharedGroup = Boolean(quota.group_name);
  const hasWindows = Boolean(quota.windows?.length);
  const totalUtilization = total?.used != null && total.limit != null && total.limit > 0
    ? Math.min(100, Math.max(0, (total.used / total.limit) * 100))
    : null;
  const remaining = quota.remaining ?? total?.remaining;
  const stateColor = ['ok', 'active'].includes(normalizedState)
    ? 'text-green-400'
    : ['exhausted', 'quota_exhausted', 'expired', 'forbidden', 'error'].includes(normalizedState)
      ? 'text-red-400'
      : 'text-yellow-400';

  return (
    <div className="space-y-2">
      <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-[10px]">
        <span className="text-gray-500">
          状态 <span className={`font-medium ${stateColor}`}>{state}</span>
        </span>
        {quota.mode && <span className="text-gray-500">模式 <span className="text-gray-300">{quota.mode}</span></span>}
        {currency && <span className="text-gray-500">单位 <span className="text-gray-300">{currency}</span></span>}
        {quota.plan_name && <span className="text-gray-500">套餐 <span className="text-gray-300">{quota.plan_name}</span></span>}
        {quota.key_name && <span className="text-gray-500">Key <span className="text-gray-300">{quota.key_name}</span></span>}
        {quota.group_name && (
          <span className="text-gray-500">
            分组 <span className="text-gray-300">{quota.group_name}</span>
          </span>
        )}
        {quota.stale === true && (
          <span className="rounded bg-amber-600/20 px-1 py-0.5 font-medium text-amber-300" title="刷新失败后保留的上次成功结果">
            缓存数据
          </span>
        )}
        <button
          onClick={onRefresh}
          className="ml-auto text-[10px] text-gray-400 hover:text-foreground underline"
        >
          刷新额度
        </button>
      </div>
      {hasSharedGroup && (
        <div className="text-[10px] text-amber-300">
          剩余、上限和额度窗口均为分组共享值；“本 Key 已用”仅统计当前 Key。
        </div>
      )}
      {quota.concurrency != null && (
        <div className="text-xs text-gray-400">
          分组并发上限 <span className="font-medium text-foreground">{quota.concurrency.toLocaleString()}</span>
        </div>
      )}
      {unlimited ? (
        <div className="rounded border border-emerald-600/30 bg-emerald-950/20 p-2 text-[10px] leading-relaxed">
          <div className="font-medium text-emerald-300">Key 无独立额度上限</div>
          <div className="mt-1 flex flex-wrap gap-x-4 gap-y-0.5 text-gray-400">
            <span>Key 额度上限：<span className="font-medium text-emerald-300">等于所属账号额度上限</span></span>
            <span>Key 剩余额度：<span className="text-gray-300">等于所属账号剩余额度</span></span>
            {expiryNotSupplied && (
              <span>Key 使用时间：<span className="font-medium text-emerald-300">不限制</span></span>
            )}
          </div>
          <div className="mt-0.5 text-gray-400">
            此 Key 未设置独立额度，实际额度随所属账号；下方仅展示当前 Key 的调用用量。
            账号额度具体数值、充值和结算状态请前往 CloudRouter 控制台查看。
          </div>
        </div>
      ) : total && (total.used != null || total.limit != null || total.remaining != null) ? (
        <div className="space-y-1">
          <div className="flex items-center justify-between gap-2 text-[10px]">
            <span className="text-gray-400">{hasSharedGroup ? '分组共享总额度' : '总额度'}</span>
            {totalUtilization != null && <span className={textColor(totalUtilization)}>已用 {totalUtilization.toFixed(0)}%</span>}
          </div>
          {totalUtilization != null && (
            <div className="h-2 rounded-full bg-gray-700 overflow-hidden">
              <div className={`h-full rounded-full ${barColor(totalUtilization)}`} style={{ width: `${totalUtilization}%` }} />
            </div>
          )}
          <div className="flex flex-wrap gap-x-3 text-[10px] text-gray-500">
            {total.used != null && <span>{hasSharedGroup ? '分组共享已用' : '已用'} {formatApiAmount(total.used, total.currency || currency)}</span>}
            {total.limit != null && <span>{hasSharedGroup ? '分组共享上限' : '上限'} {formatApiAmount(total.limit, total.currency || currency)}</span>}
            {total.remaining != null && <span>{hasSharedGroup ? '分组共享剩余' : '剩余'} {formatApiAmount(total.remaining, total.currency || currency)}</span>}
          </div>
        </div>
      ) : (remaining != null || quota.balance != null) ? (
        <div className="flex gap-4 text-xs">
          {remaining != null && (
            <span className="text-gray-400">{hasSharedGroup ? '分组共享剩余' : '剩余'} <span className="font-medium text-foreground">{formatApiAmount(remaining, currency)}</span></span>
          )}
          {quota.balance != null && (
            <span className="text-gray-400">余额 <span className="font-medium text-foreground">{formatApiAmount(quota.balance, currency)}</span></span>
          )}
        </div>
      ) : !hasWindows ? (
        <div className="text-xs text-gray-500">总额度：无法确认</div>
      ) : null}
      <ApiUsageDetails quota={quota} currency={currency} />
      <div className="grid grid-cols-1 gap-1 text-[10px] text-gray-500">
        <span>到期时间：<span className="text-gray-300">
          {expiryNotSupplied
            ? '不限制'
            : formatApiTimestamp(quota.expires_at)}
        </span></span>
        <span>剩余天数：<span className="text-gray-300">
          {quota.days_until_expiry == null
            ? expiryNotSupplied ? '不限制' : '无法确认'
            : `${quota.days_until_expiry.toLocaleString()} 天`}
        </span></span>
        <span>数据时间：<span className={quota.stale ? 'text-amber-300' : 'text-gray-300'}>
          {formatApiTimestamp(quota.fetched_at)}
        </span></span>
        {quota.stale === true && quota.refresh_failed_at != null && (
          <span>刷新失败时间：<span className="text-amber-300">
            {formatApiTimestamp(quota.refresh_failed_at)}
          </span></span>
        )}
      </div>
      {quota.windows?.map((window, index) => {
        const windowUnlimited = window.unlimited === true;
        const utilization = windowUnlimited
          ? null
          : window.utilization != null
            ? window.utilization
            : window.used != null && window.limit != null && window.limit > 0
              ? (window.used / window.limit) * 100
              : null;
        const pct = utilization == null ? null : Math.min(100, Math.max(0, utilization));
        const resetAt = window.reset_at ?? window.resets_at ?? null;
        const windowId = window.id || `${window.label || 'window'}-${index}`;
        const isSharedWindow = window.scope === 'group' || hasSharedGroup;
        const keyUsed = window.key_used ?? quota.key_usage?.[window.id || ''];
        const windowLabel = window.label || window.id || `额度窗口 ${index + 1}`;
        const hasFiniteAmounts = !windowUnlimited
          && (window.used != null || window.limit != null || window.remaining != null);
        return (
          <div key={windowId} className="space-y-1">
            <div className="flex items-center justify-between gap-2 text-[10px]">
              <span className="text-gray-400">
                {windowLabel}
                {isSharedWindow && !windowLabel.includes('分组共享') ? '（分组共享）' : ''}
              </span>
              <span className={windowUnlimited ? 'font-medium text-emerald-300' : 'text-gray-500'}>
                {windowUnlimited
                  ? isSharedWindow ? '分组不限额' : '不限额'
                  : resetAt == null ? '重置时间无法确认' : formatApiTimestamp(resetAt)}
              </span>
            </div>
            {pct != null && (
              <div className="flex items-center gap-2">
                <div className="flex-1 h-2 rounded-full bg-gray-700 overflow-hidden">
                  <div className={`h-full rounded-full ${barColor(pct)}`} style={{ width: `${pct}%` }} />
                </div>
                <span className={`w-10 shrink-0 text-right text-xs font-medium ${textColor(pct)}`}>{pct.toFixed(0)}%</span>
              </div>
            )}
            {(hasFiniteAmounts || keyUsed != null) && (
              <div className="flex flex-wrap gap-x-3 text-[10px] text-gray-500">
                {!windowUnlimited && window.used != null && <span>{isSharedWindow ? '分组共享已用' : '已用'} {formatApiAmount(window.used, window.currency || quota.currency)}</span>}
                {!windowUnlimited && window.limit != null && <span>{isSharedWindow ? '分组共享上限' : '上限'} {formatApiAmount(window.limit, window.currency || quota.currency)}</span>}
                {!windowUnlimited && window.remaining != null && <span>{isSharedWindow ? '分组共享剩余' : '剩余'} {formatApiAmount(window.remaining, window.currency || quota.currency)}</span>}
                {keyUsed != null && <span>本 Key 已用 {formatApiAmount(keyUsed, window.currency || quota.currency)}</span>}
              </div>
            )}
          </div>
        );
      })}
      {quota.reason && normalizedState !== 'active' && <div className="text-[10px] text-red-400 break-all">{quota.reason}</div>}
    </div>
  );
}

function ApiQuotaDisclosure({ quota, onRefresh }: {
  quota: CloudRouterApiQuota | null | undefined;
  onRefresh: () => void;
}) {
  const [visible, setVisible] = useState(false);

  return (
    <div className="space-y-2">
      <button
        type="button"
        onClick={() => setVisible((current) => !current)}
        aria-expanded={visible}
        className="inline-flex items-center gap-1 text-[10px] px-1.5 py-0.5 rounded border border-gray-600 text-gray-400 hover:text-foreground hover:border-gray-500"
      >
        {visible ? <EyeOff size={11} /> : <Eye size={11} />}
        {visible ? '收起额度与有效期' : '查看额度与有效期'}
      </button>
      {visible && <ApiQuotaPanel quota={quota} onRefresh={onRefresh} />}
    </div>
  );
}

function ApiAccountModels({ models }: { models?: string[] }) {
  if (!models?.length) return <div className="text-[10px] text-gray-500">支持模型：尚未获取</div>;
  return (
    <div className="flex flex-wrap gap-1" aria-label="支持模型">
      {models.map((model) => (
        <span key={model} className="px-1.5 py-0.5 rounded bg-sky-600/15 text-sky-300 text-[10px]">
          {model}
        </span>
      ))}
    </div>
  );
}

function formatApiCleanupTime(epochSeconds: number | null | undefined): string | null {
  if (epochSeconds == null || !Number.isFinite(epochSeconds)) return null;
  const value = new Date(epochSeconds * 1000);
  if (Number.isNaN(value.getTime())) return null;
  return value.toLocaleString(undefined, {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}

function ApiAccountCleanupBlocked({ diagnostics }: {
  diagnostics: ApiAccountCleanupDiagnostics;
}) {
  const summary = apiCleanupSummary(diagnostics.cleanup_code);
  const backendDetail = diagnostics.cleanup_reason?.trim() || null;
  const failedAt = formatApiCleanupTime(diagnostics.cleanup_last_error_at);
  const attemptedAt = failedAt
    ? null
    : formatApiCleanupTime(diagnostics.cleanup_last_attempt_at);

  return (
    <div className="rounded border border-amber-500/30 bg-amber-500/10 p-2 text-[10px] leading-relaxed text-amber-100 space-y-1.5">
      <div className="font-semibold text-amber-300">删除受阻</div>
      <div>
        账号已停用，新的 Claude/Codex 任务不会再使用它；API Key 与配置尚未删除。
      </div>
      <div className="rounded bg-black/15 px-2 py-1.5 space-y-0.5">
        <div><span className="text-amber-300">原因：</span>{summary}</div>
        {backendDetail && backendDetail !== summary && (
          <div className="text-amber-200/75"><span>后端详情：</span>{backendDetail}</div>
        )}
        {diagnostics.cleanup_code && (
          <div className="text-amber-200/60">
            诊断代码：<span className="font-mono">{diagnostics.cleanup_code}</span>
          </div>
        )}
        {failedAt && <div><span className="text-amber-300">最近失败：</span>{failedAt}</div>}
        {attemptedAt && <div><span className="text-amber-300">最近尝试：</span>{attemptedAt}</div>}
      </div>
      <div>
        活跃任务不会被强制终止。确认阻塞原因已解除后，可点击“重试清理”。
        Claude projects 与 Codex sessions 会保留。
      </div>
      <div className="border-t border-amber-500/20 pt-1 text-amber-200/80">
        同一共享 API 账号会投影到 Claude 与 Codex 页签；两处显示和清理的是同一个账号。
      </div>
    </div>
  );
}

// --- Codex quota helpers ---
function formatResetCountdown(epochSec: number | null): string {
  if (!epochSec) return '';
  const now = Date.now() / 1000;
  const diff = epochSec - now;
  if (diff <= 0) return '已重置';
  const d = Math.floor(diff / 86400);
  const h = Math.floor((diff % 86400) / 3600);
  const m = Math.floor((diff % 3600) / 60);
  if (d > 0) return `${d}天${h}小时后重置`;
  if (h > 0) return `${h}小时${m}分钟后重置`;
  return `${m}分钟后重置`;
}

function formatWindowName(minutes: number | null): string {
  if (!minutes) return '';
  const days = Math.round(minutes / 60 / 24);
  if (days >= 7) return '7天窗口';
  if (days >= 1) return `${days}天窗口`;
  const hours = Math.round(minutes / 60);
  return `${hours}小时窗口`;
}

function AccountCard({ account, preferred, lastSelected, apiAccount, onClearCooldown, onSetPreferred, onRelogin, onRetryUsage, onDelete, deleting, reloginState }: {
  account: PoolAccountUsage;
  preferred: string | null;
  lastSelected: string | null;
  apiAccount?: CloudRouterAccount;
  onClearCooldown: (id: string) => void;
  onSetPreferred: (id: string | null) => void;
  onRelogin: (id: string) => void;
  onRetryUsage: () => void;
  onDelete?: () => void;
  deleting?: boolean;
  reloginState?: { status: string; message?: string };
}) {
  const isApi = isApiAuthKind(account.auth_kind);
  const cleanupPending = isApi && isApiCleanupPending(account, apiAccount);
  const cleanupDiagnostics = mergeApiCleanupDiagnostics(account, apiAccount);
  const apiProvider = resolveApiProvider(account.auth_kind, account.api_provider);
  const statusDot = cleanupPending
    ? { cls: 'bg-amber-500', label: '删除受阻' }
    : !account.enabled
    ? { cls: 'bg-gray-500', label: '已禁用' }
    : account.available
      ? { cls: 'bg-green-500', label: '可用' }
      : { cls: 'bg-yellow-500', label: '冷却中' };
  const isPreferred = !cleanupPending && preferred === account.id;
  const isLastSelected = !cleanupPending && lastSelected === account.id;

  return (
    <div className={`rounded-lg border bg-gray-800 p-3 space-y-2 ${
      cleanupPending ? 'border-amber-500/60' : isPreferred ? 'border-indigo-500' : 'border-gray-700'
    }`}>
      <div className="flex items-center gap-2">
        <span className={`h-2 w-2 shrink-0 rounded-full ${statusDot.cls}`} title={statusDot.label} />
        <span className="text-sm font-medium text-foreground truncate" title={account.id}>
          {isApi ? account.display_name || account.id : account.id}
        </span>
        {isApi && (
          <span className="px-1.5 py-0.5 rounded bg-sky-600/30 text-sky-300 text-[10px] font-semibold uppercase">
            {apiProvider === 'apex' ? 'APEXROUTER API' : apiProvider === 'apibest' ? 'APIBEST API' : 'API'}
          </span>
        )}
        {cleanupPending && (
          <span className="px-1.5 py-0.5 rounded bg-amber-600/25 text-amber-300 text-[10px] font-semibold">
            已停用
          </span>
        )}
        {!isApi && account.subscription_type && (
          <span className="px-1.5 py-0.5 rounded bg-indigo-600/30 text-indigo-300 text-[10px] font-semibold uppercase">
            {account.subscription_type}
          </span>
        )}
        {isPreferred && (
          <span className="px-1.5 py-0.5 rounded bg-green-600/30 text-green-300 text-[10px] font-semibold">
            优先账号
          </span>
        )}
        {isLastSelected && (
          <span className="px-1.5 py-0.5 rounded bg-cyan-600/30 text-cyan-300 text-[10px] font-semibold" title="最近一次由路由器分配的账号">
            最近使用
          </span>
        )}
        <div className="ml-auto flex items-center gap-2">
          {!isApi && !account.available && account.enabled && (
            <button
              onClick={() => onClearCooldown(account.id)}
              className="text-[10px] text-gray-400 hover:text-foreground underline"
              title="清除冷却，立即恢复可用"
            >
              解除冷却
            </button>
          )}
          {!cleanupPending && account.enabled && (
            isPreferred ? (
              <button
                onClick={() => onSetPreferred(null)}
                className="text-[10px] px-1.5 py-0.5 rounded border border-gray-600 text-gray-400 hover:text-foreground hover:border-gray-400"
                title="取消全局优先；新会话优先兼容且可用的 API，已有对话继续使用绑定账号"
              >
                恢复自动
              </button>
            ) : (
              <button
                onClick={() => onSetPreferred(account.id)}
                className="text-[10px] px-1.5 py-0.5 rounded border border-indigo-500/50 text-indigo-300 hover:bg-indigo-600/20"
                title="后续任务及当前对话下一轮优先切换；不可用或迁移失败时安全回退"
              >
                切换到此账号
              </button>
            )
          )}
          {onDelete && (
            <button
              onClick={onDelete}
              disabled={deleting}
              className="text-[10px] px-1.5 py-0.5 rounded border border-gray-600 text-gray-400 hover:text-red-400 hover:border-red-500 disabled:opacity-50"
              title={isApi
                ? cleanupPending
                  ? '账号已停用；重试安全清理 API Key 与配置'
                  : '同时从 Claude 与 Codex 视图删除此 API 账号'
                : '从号池删除'}
            >
              {deleting ? '处理中…' : cleanupPending ? '重试清理' : isApi ? '删除 API 账号' : '删除'}
            </button>
          )}
        </div>
      </div>
      {!isApi && account.email && <div className="text-xs text-gray-500 truncate">{account.email}</div>}
      {isApi && apiAccount?.key_hint && (
        <div className="text-[10px] text-sky-300/80 font-mono" title="来自管理员 API 账号目录的脱敏 Key 指纹">
          Key 指纹：{apiAccount.key_hint}
        </div>
      )}
      {isApi && (
        <div className="text-[10px] text-gray-500 font-mono truncate" title={account.config_dir}>
          CLAUDE_CONFIG_DIR: {account.config_dir}
        </div>
      )}
      {isApi && <ApiAccountModels models={account.supported_models} />}
      {cleanupPending ? (
        <ApiAccountCleanupBlocked diagnostics={cleanupDiagnostics} />
      ) : isApi ? (
        <ApiQuotaDisclosure quota={account.api_quota} onRefresh={onRetryUsage} />
      ) : account.usage ? (
        <div className="space-y-1.5">
          <UsageBar label="5h" window={account.usage.five_hour} />
          <UsageBar label="7d" window={account.usage.seven_day} />
          <UsageBar label="Opus" window={account.usage.seven_day_opus} />
        </div>
      ) : (
        <div className={`text-xs space-y-1 ${account.usage_error === 'token_expired' ? 'text-yellow-400' : 'text-red-400'}`}>
          <div className="flex items-center gap-2">
            <span>
              {account.usage_error === 'no_credentials' && '未找到凭据文件'}
              {account.usage_error === 'token_expired' && 'Token 过期，将在使用时自动刷新'}
              {account.usage_error && !['no_credentials', 'token_expired'].includes(account.usage_error) && `额度获取失败: ${account.usage_error}`}
            </span>
            {account.usage_error === 'no_credentials' && !isApi && (
              <button
                onClick={() => onRelogin(account.id)}
                disabled={reloginState?.status === 'running'}
                className="shrink-0 text-[10px] px-1.5 py-0.5 rounded border border-red-500/50 text-red-300 hover:bg-red-600/20 disabled:opacity-50"
              >
                {reloginState?.status === 'running' ? '登录中…' : '重新登录'}
              </button>
            )}
            {account.usage_error && !['no_credentials', 'token_expired'].includes(account.usage_error) && (
              <button
                onClick={onRetryUsage}
                className="shrink-0 text-[10px] px-1.5 py-0.5 rounded border border-yellow-500/50 text-yellow-300 hover:bg-yellow-600/20"
              >
                重试
              </button>
            )}
          </div>
          {reloginState?.message && (
            <div className="text-[10px] text-gray-400 whitespace-pre-wrap break-all">{reloginState.message}</div>
          )}
        </div>
      )}
    </div>
  );
}

// --- Codex Account Card ---
function CodexOtpPrompt({ state, onSubmit }: {
  state: CodexLoginStatus;
  onSubmit: (code: string) => Promise<void>;
}) {
  const [code, setCode] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const submit = async () => {
    if (!/^\d{6}$/.test(code)) {
      setError('请输入 6 位数字验证码');
      return;
    }
    setSubmitting(true);
    setError(null);
    try {
      await onSubmit(code);
      setCode('');
    } catch (e) {
      setError(e instanceof Error ? e.message : '验证码提交失败');
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="rounded border border-amber-500/40 bg-amber-500/10 p-2 space-y-2">
      <div className="text-xs text-amber-300">OpenAI 要求邮箱验证码，请从邮箱中取得最新的 6 位码。</div>
      {state.expires_at && <div className="text-[10px] text-gray-400">{formatResetCountdown(state.expires_at)}</div>}
      <div className="flex gap-2">
        <input
          aria-label="OpenAI 邮箱验证码"
          inputMode="numeric"
          autoComplete="one-time-code"
          maxLength={6}
          value={code}
          onChange={(e) => setCode(e.target.value.replace(/\D/g, '').slice(0, 6))}
          className="min-w-0 flex-1 bg-gray-700 text-foreground text-xs rounded px-2.5 py-1.5 outline-none focus:ring-1 focus:ring-amber-500"
          placeholder="6 位验证码"
        />
        <button
          type="button"
          onClick={submit}
          disabled={submitting || code.length !== 6}
          className="px-2.5 py-1.5 text-xs rounded bg-amber-600 text-white hover:bg-amber-500 disabled:opacity-50"
        >
          {submitting ? '提交中…' : '继续登录'}
        </button>
      </div>
      {error && <div className="text-[10px] text-red-400">{error}</div>}
    </div>
  );
}

function CodexAccountCard({ account, preferred, lastSelected, apiAccount, onClearCooldown, onSetPreferred, onRelogin, onSubmitOtp, onDelete, deleting, onRetryUsage, reloginState }: {
  account: CodexPoolAccountUsage;
  preferred: string | null;
  lastSelected: string | null;
  apiAccount?: CloudRouterAccount;
  onClearCooldown: (id: string) => void;
  onSetPreferred: (id: string | null) => void;
  onRelogin: (id: string) => void;
  onSubmitOtp: (state: CodexLoginStatus, code: string) => Promise<void>;
  onDelete?: () => void;
  deleting?: boolean;
  onRetryUsage: () => void;
  reloginState?: CodexLoginStatus;
}) {
  const isApi = isApiAuthKind(account.auth_kind);
  const cleanupPending = isApi && isApiCleanupPending(account, apiAccount);
  const cleanupDiagnostics = mergeApiCleanupDiagnostics(account, apiAccount);
  const apiProvider = resolveApiProvider(account.auth_kind, account.api_provider);
  const statusDot = cleanupPending
    ? { cls: 'bg-amber-500', label: '删除受阻' }
    : !account.enabled
    ? { cls: 'bg-gray-500', label: '已禁用' }
    : account.available
      ? { cls: 'bg-green-500', label: '可用' }
      : { cls: 'bg-yellow-500', label: '冷却中' };

  const q = isApi ? null : account.quota;
  const isPreferred = !cleanupPending && preferred === account.id;
  const isLastSelected = !cleanupPending && lastSelected === account.id;

  return (
    <div className={`rounded-lg border bg-gray-800 p-3 space-y-2 ${
      cleanupPending ? 'border-amber-500/60' : isPreferred ? 'border-emerald-500' : 'border-gray-700'
    }`}>
      <div className="flex items-center gap-2">
        <span className={`h-2 w-2 shrink-0 rounded-full ${statusDot.cls}`} title={statusDot.label} />
        <span className="text-sm font-medium text-foreground truncate" title={account.id}>
          {isApi ? account.display_name || account.id : account.id}
        </span>
        {isApi && (
          <span className="px-1.5 py-0.5 rounded bg-sky-600/30 text-sky-300 text-[10px] font-semibold uppercase">
            {apiProvider === 'apex' ? 'APEXROUTER API' : apiProvider === 'apibest' ? 'APIBEST API' : 'API'}
          </span>
        )}
        {cleanupPending && (
          <span className="px-1.5 py-0.5 rounded bg-amber-600/25 text-amber-300 text-[10px] font-semibold">
            已停用
          </span>
        )}
        {!isApi && account.plan_type && (
          <span className="px-1.5 py-0.5 rounded bg-emerald-600/30 text-emerald-300 text-[10px] font-semibold uppercase">
            {account.plan_type}
          </span>
        )}
        {!isApi && q?.has_credits && (
          <span className="px-1.5 py-0.5 rounded bg-amber-600/30 text-amber-300 text-[10px] font-semibold">
            Credits
          </span>
        )}
        {isPreferred && (
          <span className="px-1.5 py-0.5 rounded bg-green-600/30 text-green-300 text-[10px] font-semibold">
            优先账号
          </span>
        )}
        {isLastSelected && (
          <span className="px-1.5 py-0.5 rounded bg-cyan-600/30 text-cyan-300 text-[10px] font-semibold" title="最近一次由路由器分配的账号">
            最近使用
          </span>
        )}
        <div className="ml-auto flex items-center gap-2">
          {!isApi && !account.available && account.enabled && (
            <button onClick={() => onClearCooldown(account.id)} className="text-[10px] text-gray-400 hover:text-foreground underline">
              解除冷却
            </button>
          )}
          {!cleanupPending && account.enabled && (
            isPreferred ? (
              <button
                onClick={() => onSetPreferred(null)}
                className="text-[10px] px-1.5 py-0.5 rounded border border-gray-600 text-gray-400 hover:text-foreground hover:border-gray-400"
                title="取消全局优先；新会话优先兼容且可用的 API，已有对话继续使用绑定账号"
              >
                恢复自动
              </button>
            ) : (
              <button
                onClick={() => onSetPreferred(account.id)}
                className="text-[10px] px-1.5 py-0.5 rounded border border-emerald-500/50 text-emerald-300 hover:bg-emerald-600/20"
                title="后续任务及当前对话下一轮优先切换；不可用或迁移失败时安全回退"
              >
                切换到此账号
              </button>
            )
          )}
          {!isApi && (
            <button
              onClick={() => onRelogin(account.id)}
              disabled={Boolean(reloginState && ACTIVE_CODEX_LOGIN_STATUSES.has(reloginState.status))}
              className="text-[10px] px-1.5 py-0.5 rounded border border-emerald-500/50 text-emerald-300 hover:bg-emerald-600/20 disabled:opacity-50"
            >
              {reloginState && ACTIVE_CODEX_LOGIN_STATUSES.has(reloginState.status) ? '登录中…' : '重新登录'}
            </button>
          )}
          {onDelete && (
            <button
              onClick={onDelete}
              disabled={deleting}
              className="text-[10px] px-1.5 py-0.5 rounded border border-gray-600 text-gray-400 hover:text-red-400 hover:border-red-500 disabled:opacity-50"
              title={isApi
                ? cleanupPending
                  ? '账号已停用；重试安全清理 API Key 与配置'
                  : '同时从 Claude 与 Codex 视图删除此 API 账号'
                : '从号池删除'}
            >
              {deleting ? '处理中…' : cleanupPending ? '重试清理' : isApi ? '删除 API 账号' : '删除'}
            </button>
          )}
        </div>
      </div>
      {!isApi && account.email && <div className="text-xs text-gray-500 truncate">{account.email}</div>}
      {isApi && apiAccount?.key_hint && (
        <div className="text-[10px] text-sky-300/80 font-mono" title="来自管理员 API 账号目录的脱敏 Key 指纹">
          Key 指纹：{apiAccount.key_hint}
        </div>
      )}
      <div className="text-[10px] text-gray-500 font-mono truncate" title={account.codex_home}>
        CODEX_HOME: {account.codex_home}
      </div>
      {isApi && <ApiAccountModels models={account.supported_models} />}
      {cleanupPending ? (
        <ApiAccountCleanupBlocked diagnostics={cleanupDiagnostics} />
      ) : isApi ? (
        <ApiQuotaDisclosure quota={account.api_quota} onRefresh={onRetryUsage} />
      ) : q ? (
        <div className="space-y-2">
          {/* Primary window */}
          {q.primary_used_percent != null && (
            <div className="space-y-1">
              <div className="flex items-center justify-between text-[10px]">
                <span className="text-gray-400">{formatWindowName(q.primary_window_minutes) || '主窗口'}</span>
                <span className="text-gray-500">{formatResetCountdown(q.primary_resets_at)}</span>
              </div>
              <div className="flex items-center gap-2 text-xs">
                <div className="flex-1 h-2.5 rounded-full bg-gray-700 overflow-hidden">
                  <div className={`h-full rounded-full ${barColor(q.primary_used_percent)}`} style={{ width: `${Math.min(100, q.primary_used_percent)}%` }} />
                </div>
                <span className={`w-16 shrink-0 text-right font-medium ${textColor(q.primary_used_percent)}`}>
                  已用 {q.primary_used_percent.toFixed(1)}%
                </span>
              </div>
              <div className="text-[10px] text-gray-500">
                剩余 {(100 - q.primary_used_percent).toFixed(1)}%
              </div>
            </div>
          )}
          {/* Secondary window */}
          {q.secondary_used_percent != null && (
            <div className="space-y-1">
              <div className="flex items-center justify-between text-[10px]">
                <span className="text-gray-400">{formatWindowName(q.secondary_window_minutes) || '副窗口'}</span>
                <span className="text-gray-500">{formatResetCountdown(q.secondary_resets_at)}</span>
              </div>
              <div className="flex items-center gap-2 text-xs">
                <div className="flex-1 h-2.5 rounded-full bg-gray-700 overflow-hidden">
                  <div className={`h-full rounded-full ${barColor(q.secondary_used_percent)}`} style={{ width: `${Math.min(100, q.secondary_used_percent)}%` }} />
                </div>
                <span className={`w-16 shrink-0 text-right font-medium ${textColor(q.secondary_used_percent)}`}>
                  已用 {q.secondary_used_percent.toFixed(1)}%
                </span>
              </div>
            </div>
          )}
          {q.is_rate_limited && (
            <div className="text-[10px] text-red-400 font-medium">已触发限速</div>
          )}
        </div>
      ) : (
        <div className="text-xs space-y-1">
          <div className="flex items-center gap-2 text-gray-500">
            <span>{account.quota_error === 'no_rollout_data'
              ? '暂无额度数据（使用后自动更新）'
              : account.quota_error === 'live_unavailable'
                ? '实时额度查询失败，无法确认当前额度'
                : (account.quota_error || '未知')}</span>
            <button
              onClick={onRetryUsage}
              className="shrink-0 text-[10px] px-1.5 py-0.5 rounded border border-gray-600 text-gray-400 hover:text-foreground"
            >
              刷新
            </button>
          </div>
        </div>
      )}
      {!isApi && reloginState?.status === 'running' && (
        <div className="text-xs text-blue-400">自动登录中…</div>
      )}
      {!isApi && reloginState?.status === 'awaiting_otp' && (
        <CodexOtpPrompt state={reloginState} onSubmit={(code) => onSubmitOtp(reloginState, code)} />
      )}
      {!isApi && reloginState?.status === 'verifying_otp' && (
        <div className="text-xs text-blue-400">验证码已提交，正在继续登录…</div>
      )}
      {!isApi && reloginState?.status === 'finalizing' && (
        <div className="text-xs text-blue-400">登录已完成，正在安全提交登录结果…</div>
      )}
      {!isApi && reloginState && ACTIVE_CODEX_LOGIN_STATUSES.has(reloginState.status) && reloginState.detail && (
        <div className="text-[10px] text-amber-400 break-all">{reloginState.detail}</div>
      )}
      {!isApi && (reloginState?.status === 'failed' || reloginState?.status === 'expired') && (
        <div className="text-xs text-red-400 break-all">{reloginState.detail || '登录失败'}</div>
      )}
      {!isApi && reloginState?.status === 'success' && (
        <div className="text-xs text-green-400">登录成功</div>
      )}
    </div>
  );
}

function AddApiAccountModal({ onClose, onAdded }: {
  onClose: () => void;
  onAdded: () => void | Promise<void>;
}) {
  const [apiProvider, setApiProvider] = useState<ApiAccountProvider>('cloudrouter');
  const [name, setName] = useState('');
  const [apiKey, setApiKey] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const providerName = apiProvider === 'apex'
    ? 'ApexRouter'
    : apiProvider === 'apibest' ? 'APIBest' : 'CloudRouter';

  const handleSubmit = async (event: React.FormEvent) => {
    event.preventDefault();
    const trimmedName = name.trim();
    const secret = apiKey.trim();
    if (!trimmedName || !secret) return;

    setSubmitting(true);
    setError(null);
    // Do not retain a reusable API key in component state while validation and
    // quota discovery are in flight.
    setApiKey('');
    try {
      await api.createCloudRouterAccount({
        name: trimmedName,
        api_key: secret,
        api_provider: apiProvider,
      });
      await onAdded();
      onClose();
    } catch (e) {
      setError(e instanceof Error ? e.message : '添加 API 账号失败');
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="absolute inset-0 bg-gray-900/80 z-10 flex items-start justify-center pt-16">
      <div className="bg-gray-800 rounded-lg shadow-xl w-full max-w-xs">
        <div className="flex items-center justify-between px-4 py-3 border-b border-gray-700">
          <h3 className="text-sm font-semibold text-foreground">添加 API 账号</h3>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-200"><X size={14} /></button>
        </div>
        <form onSubmit={handleSubmit} className="p-4 space-y-3">
          <div>
            <label htmlFor="api-account-provider" className="block text-xs text-gray-400 mb-1">API 渠道</label>
            <select
              id="api-account-provider"
              className="w-full bg-gray-700 text-foreground text-xs rounded px-2.5 py-1.5 outline-none focus:ring-1 focus:ring-sky-500"
              value={apiProvider}
              onChange={(event) => setApiProvider(event.target.value as ApiAccountProvider)}
              disabled={submitting}
            >
              <option value="cloudrouter">CloudRouter</option>
              <option value="apex">ApexRouter</option>
              <option value="apibest">APIBest</option>
            </select>
          </div>
          <div>
            <label htmlFor="api-account-name" className="block text-xs text-gray-400 mb-1">账号名称</label>
            <input
              id="api-account-name"
              className="w-full bg-gray-700 text-foreground text-xs rounded px-2.5 py-1.5 outline-none focus:ring-1 focus:ring-sky-500"
              value={name}
              onChange={(event) => setName(event.target.value)}
              placeholder={apiProvider === 'apex' ? '例如：ApexRouter' : apiProvider === 'apibest' ? '例如：APIBest' : '例如：CloudRouter Claude'}
              autoComplete="off"
              required
            />
          </div>
          <div>
            <label htmlFor="api-account-key" className="block text-xs text-gray-400 mb-1">{providerName} API Key</label>
            <input
              id="api-account-key"
              type="password"
              className="w-full bg-gray-700 text-foreground text-xs rounded px-2.5 py-1.5 outline-none focus:ring-1 focus:ring-sky-500"
              value={apiKey}
              onChange={(event) => setApiKey(event.target.value)}
              placeholder={apiProvider === 'apex' ? 'lck_...' : apiProvider === 'apibest' ? 'sk-...' : 'cr-...'}
              autoComplete="new-password"
              required
            />
          </div>
          <div className="space-y-1 text-[11px] leading-relaxed text-gray-500">
            <p>每把 Key 建立一个独立 API 账号目录，Key 会以 0600 权限持久保存，不会显示在账号列表或日志中。</p>
            {apiProvider === 'cloudrouter' ? (
              <p>系统通过 /v1/models 自动识别该 Key 可用于 Claude、Codex 或两者。CloudRouter 通常一把 Key 对应一个模型分组；同时使用两类模型时通常需要分别添加两把 Key。</p>
            ) : apiProvider === 'apex' ? (
              <>
                <p>系统通过 ApexRouter /v1/models 自动识别该 Key 可用于 Claude、Codex 或两者，并分别配置 Anthropic Messages 与 OpenAI Responses 协议。</p>
                <p>额度通过 /v1/usage 获取：“已用”为当前 Key 用量，剩余、上限与并发限制由同组 Key 共享。</p>
                <p>ApexRouter 当前不返回到期时间，因此到期时间和剩余天数会显示为无法确认。</p>
              </>
            ) : (
              <p>系统先通过 APIBest /v1/models 验证 Key，再从公开价格目录识别可用的 Claude 与 Codex 模型；该渠道暂不展示额度。</p>
            )}
            <p>API 账号会直接加入现有账号池，任务、换模型、会话与自动轮换方式不变。</p>
          </div>
          {error && <p className="text-xs text-red-400 break-all">{error}</p>}
          <div className="flex justify-end gap-2 pt-1">
            <button type="button" onClick={onClose} className="px-3 py-1.5 text-xs text-gray-300 hover:text-foreground">取消</button>
            <button
              type="submit"
              disabled={submitting || !name.trim() || !apiKey.trim()}
              className="px-3 py-1.5 text-xs bg-sky-600 text-white rounded hover:bg-sky-500 disabled:opacity-50"
            >
              {submitting ? '验证并添加…' : '验证并添加'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}


function AddAccountModal({ onClose, onAdded }: { onClose: () => void; onAdded: () => void }) {
  const [email, setEmail] = useState('');
  const [token, setToken] = useState('');
  const [loginMethod, setLoginMethod] = useState('');

  const [status, setStatus] = useState<string | null>(null);
  const [detail, setDetail] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!email.trim() || !token.trim()) return;
    setSubmitting(true);
    setDetail(null);
    try {
      await api.poolAddAccount({ email: email.trim(), token: token.trim(), login_method: loginMethod || undefined });
      setStatus('running');
      const poll = async () => {
        const s = await api.poolAddStatus(email.trim());
        if (s.status === 'running') { setTimeout(poll, 5000); return; }
        setStatus(s.status);
        if (s.status === 'failed') setDetail(s.detail?.slice(-500) || '登录失败');
        if (s.status === 'success') { onAdded(); onClose(); }
      };
      setTimeout(poll, 5000);
    } catch (e) {
      setStatus('failed');
      setDetail(e instanceof Error ? e.message : '请求失败');
      setSubmitting(false);
    }
  };

  return (
    <div className="absolute inset-0 bg-gray-900/80 z-10 flex items-start justify-center pt-16">
      <div className="bg-gray-800 rounded-lg shadow-xl w-full max-w-xs">
        <div className="flex items-center justify-between px-4 py-3 border-b border-gray-700">
          <h3 className="text-sm font-semibold text-foreground">添加 Claude 账号</h3>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-200"><X size={14} /></button>
        </div>
        <form onSubmit={handleSubmit} className="p-4 space-y-3">
          <div>
            <label className="block text-xs text-gray-400 mb-1">邮箱</label>
            <input className="w-full bg-gray-700 text-foreground text-xs rounded px-2.5 py-1.5 outline-none focus:ring-1 focus:ring-indigo-500"
              value={email} onChange={e => setEmail(e.target.value)} placeholder="user@example.com" required />
          </div>
          <div>
            <label className="block text-xs text-gray-400 mb-1">接码 API Token</label>
            <input className="w-full bg-gray-700 text-foreground text-xs rounded px-2.5 py-1.5 outline-none focus:ring-1 focus:ring-indigo-500"
              type="password" value={token} onChange={e => setToken(e.target.value)}
              placeholder="171mail / MailCatcher Token" required />
          </div>
          <div>
            <label className="block text-xs text-gray-400 mb-1">登录方式</label>
            <select
              className="w-full bg-gray-700 text-foreground text-xs rounded px-2.5 py-1.5 outline-none focus:ring-1 focus:ring-indigo-500"
              value={loginMethod} onChange={e => setLoginMethod(e.target.value)}
            >
              <option value="">自动识别（按邮箱后缀）</option>
              <option value="171mail">171mail（API 接码）</option>
              <option value="mailcom">mail.com（Chrome 接码）</option>
              <option value="onet">Onet（Token 接码）</option>
              <option value="gazeta">Gazeta（Token 接码）</option>
            </select>
          </div>
          {status === 'running' && <p className="text-xs text-blue-400">登录中… 请等待（可能需要 1-2 分钟）</p>}
          {status === 'failed' && <p className="text-xs text-red-400 break-all">{detail || '登录失败'}</p>}
          <div className="flex justify-end gap-2 pt-1">
            <button type="button" onClick={onClose} className="px-3 py-1.5 text-xs text-gray-300 hover:text-foreground">取消</button>
            <button type="submit" disabled={submitting || status === 'running' || !email.trim() || !token.trim()}
              className="px-3 py-1.5 text-xs bg-indigo-600 text-white rounded hover:bg-indigo-500 disabled:opacity-50">
              {status === 'running' ? '登录中…' : '添加'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}


function AddCodexAccountModal({ onClose, onAdded }: { onClose: () => void; onAdded: () => void }) {
  const [email, setEmail] = useState('');
  const [token, setToken] = useState('');
  const [password, setPassword] = useState('');
  const [loginMethod, setLoginMethod] = useState<CodexLoginMethod | ''>('');

  const [loginState, setLoginState] = useState<CodexLoginStatus | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const emailDomain = email.trim().toLowerCase().split('@').pop() || '';
  const detectedMethod: CodexLoginMethod = emailDomain === '163.com'
    ? 'mailcatcher'
    : emailDomain === 'mail.com'
      ? 'mailcom'
    : emailDomain === 'onet.pl'
      ? 'onet'
      : emailDomain === 'gazeta.pl'
        ? 'gazeta'
        : '171mail';
  const activeMethod = loginMethod || detectedMethod;
  const usesMailCatcher = activeMethod !== '171mail';
  const loginActive = Boolean(loginState && ACTIVE_CODEX_LOGIN_STATUSES.has(loginState.status));

  useEffect(() => {
    if (!loginActive || !email.trim()) return;
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;

    const poll = async () => {
      try {
        const state = await api.codexPoolAddStatus(email.trim());
        if (cancelled) return;
        setLoginState(state);
        if (state.status === 'success') {
          onAdded();
          onClose();
          return;
        }
        if (ACTIVE_CODEX_LOGIN_STATUSES.has(state.status)) {
          timer = setTimeout(poll, 2000);
        }
      } catch (e) {
        if (!cancelled) {
          setLoginState((current) => ({
            ...(current || { status: 'running' }),
            detail: e instanceof Error
              ? `状态查询暂时失败，正在重试：${e.message}`
              : '状态查询暂时失败，正在重试',
          }));
          timer = setTimeout(poll, 2000);
        }
      }
    };

    timer = setTimeout(poll, 1000);
    return () => {
      cancelled = true;
      if (timer) clearTimeout(timer);
    };
  }, [loginActive, email, onAdded, onClose]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!email.trim()) return;
    setSubmitting(true);
    try {
      const state = await api.codexPoolAddAccount({
        email: email.trim(),
        token: token.trim() || undefined,
        password: password || undefined,
        login_method: loginMethod || undefined,
      });
      // The child login process has received the credentials at this point;
      // do not retain reusable secrets in React state for the rest of the
      // potentially long-running browser/OTP flow.
      setToken('');
      setPassword('');
      setLoginState({ status: state.status, attempt_id: state.attempt_id });
    } catch (e) {
      setLoginState({
        status: 'failed',
        detail: e instanceof Error ? e.message : '请求失败',
      });
    } finally {
      setSubmitting(false);
    }
  };

  const submitOtp = async (code: string) => {
    if (!loginState?.attempt_id || !loginState.challenge_id) {
      throw new Error('验证码挑战信息缺失，请重新登录');
    }
    await api.codexPoolSubmitOtp(loginState.attempt_id, loginState.challenge_id, code);
    setLoginState({ ...loginState, status: 'verifying_otp' });
  };

  return (
    <div className="absolute inset-0 bg-gray-900/80 z-10 flex items-start justify-center pt-16">
      <div className="bg-gray-800 rounded-lg shadow-xl w-full max-w-xs">
        <div className="flex items-center justify-between px-4 py-3 border-b border-gray-700">
          <h3 className="text-sm font-semibold text-foreground">添加 Codex 账号</h3>
          <button disabled={loginActive} onClick={onClose} className="text-gray-400 hover:text-gray-200 disabled:opacity-40" title={loginActive ? '请等待登录完成' : undefined}><X size={14} /></button>
        </div>
        <form onSubmit={handleSubmit} className="p-4 space-y-3">
          <div>
            <label htmlFor="codex-account-email" className="block text-xs text-gray-400 mb-1">OpenAI 邮箱</label>
            <input id="codex-account-email" className="w-full bg-gray-700 text-foreground text-xs rounded px-2.5 py-1.5 outline-none focus:ring-1 focus:ring-emerald-500"
              value={email} onChange={e => setEmail(e.target.value)} placeholder="user@example.com" required disabled={loginActive} />
          </div>
          <div>
            <label htmlFor="codex-login-method" className="block text-xs text-gray-400 mb-1">验证码邮箱来源</label>
            <select
              id="codex-login-method"
              className="w-full bg-gray-700 text-foreground text-xs rounded px-2.5 py-1.5 outline-none focus:ring-1 focus:ring-emerald-500"
              value={loginMethod}
              onChange={e => setLoginMethod(e.target.value as CodexLoginMethod | '')}
              disabled={loginActive}
            >
              <option value="">自动识别（163/mail.com/Onet/Gazeta → MailCatcher）</option>
              <option value="171mail">171mail（API 接码）</option>
              <option value="mailcatcher">MailCatcher（163 / mail.com / Onet / Gazeta 等）</option>
              <option value="mailcom">mail.com（MailCatcher 接码）</option>
              <option value="onet">Onet（MailCatcher 接码）</option>
              <option value="gazeta">Gazeta（MailCatcher 接码）</option>
            </select>
          </div>
          <div>
            <label htmlFor="codex-mail-credential" className="block text-xs text-gray-400 mb-1">
              {activeMethod === '171mail' ? '171mail API Token（可选）' : 'MailCatcher 查询 Token（可选）'}
            </label>
            <input id="codex-mail-credential" type="password" className="w-full bg-gray-700 text-foreground text-xs rounded px-2.5 py-1.5 outline-none focus:ring-1 focus:ring-emerald-500"
              value={token} onChange={e => setToken(e.target.value)} placeholder="仅在使用邮箱验证码时需要" disabled={loginActive} />
            {usesMailCatcher && <p className="mt-1 text-[11px] text-gray-500">填写 MailCatcher 平台签发的查询 Token，不是邮箱密码。</p>}
          </div>
          <div>
            <label htmlFor="codex-openai-password" className="block text-xs text-gray-400 mb-1">OpenAI 密码（可选）</label>
            <input id="codex-openai-password" type="password" className="w-full bg-gray-700 text-foreground text-xs rounded px-2.5 py-1.5 outline-none focus:ring-1 focus:ring-emerald-500"
              value={password} onChange={e => setPassword(e.target.value)} placeholder="有密码时优先使用密码登录" disabled={loginActive} />
            <p className="mt-1 text-[11px] text-gray-500">Token 和密码都可不填：CCM 会尝试切换到 OpenAI 邮箱验证码，并在这里等你输入 6 位码；若账号只提供密码登录，会提示补密码重试。只有实际填写的长期凭据才会以 0600 权限保存在 CCM 服务器；验证码不会保存。</p>
          </div>
          {loginState?.status === 'running' && <p className="text-xs text-blue-400">登录中… 请等待（可能需要 1-3 分钟）</p>}
          {loginState?.status === 'awaiting_otp' && <CodexOtpPrompt state={loginState} onSubmit={submitOtp} />}
          {loginState?.status === 'verifying_otp' && <p className="text-xs text-blue-400">验证码已提交，正在继续登录…</p>}
          {loginState?.status === 'finalizing' && <p className="text-xs text-blue-400">登录已完成，正在安全提交登录结果…</p>}
          {loginActive && loginState?.detail && <p className="text-xs text-amber-400 break-all">{loginState.detail}</p>}
          {(loginState?.status === 'failed' || loginState?.status === 'expired') && <p className="text-xs text-red-400 break-all">{loginState.detail || '登录失败'}</p>}
          <div className="flex justify-end gap-2 pt-1">
            <button type="button" disabled={loginActive} onClick={onClose} className="px-3 py-1.5 text-xs text-gray-300 hover:text-white disabled:opacity-40">取消</button>
            <button type="submit" disabled={submitting || loginActive || !email.trim()}
              className="px-3 py-1.5 text-xs bg-emerald-600 text-white rounded hover:bg-emerald-500 disabled:opacity-50">
              {loginActive ? '登录中…' : '添加'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

function CcSettingsModal({ onClose }: { onClose: () => void }) {
  const [text, setText] = useState('');
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [result, setResult] = useState<{ ok: boolean; message: string } | null>(null);

  useEffect(() => {
    api.getCcSettings()
      .then((r) => setText(JSON.stringify(r.settings, null, 2)))
      .catch((e) => setText(`// 加载失败: ${e instanceof Error ? e.message : e}`))
      .finally(() => setLoading(false));
  }, []);

  const handleSave = async () => {
    setResult(null);
    let parsed: Record<string, unknown>;
    try {
      parsed = JSON.parse(text);
    } catch {
      setResult({ ok: false, message: 'JSON 格式错误' });
      return;
    }
    setSaving(true);
    try {
      const r = await api.putCcSettings(parsed);
      setResult({ ok: true, message: `已同步到 ${r.synced} 个账号` });
    } catch (e) {
      setResult({ ok: false, message: e instanceof Error ? e.message : '保存失败' });
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="absolute inset-0 bg-gray-900/80 z-10 flex items-start justify-center pt-12">
      <div className="bg-gray-800 rounded-lg shadow-xl w-full max-w-xs flex flex-col max-h-[80%]">
        <div className="flex items-center justify-between px-4 py-3 border-b border-gray-700">
          <h3 className="text-sm font-semibold text-foreground">CC Settings 模板</h3>
          <button onClick={onClose} className="text-gray-400 hover:text-foreground"><X size={14} /></button>
        </div>
        <div className="flex-1 overflow-hidden p-3 flex flex-col gap-2">
          <p className="text-[10px] text-gray-500">编辑后保存将同步到所有 Pool 账号的 settings.json（hooks 字段会保留）</p>
          {loading ? (
            <div className="text-xs text-gray-500 py-4 text-center">加载中…</div>
          ) : (
            <textarea
              value={text}
              onChange={(e) => setText(e.target.value)}
              className="flex-1 min-h-[200px] bg-gray-900 text-gray-300 text-[11px] font-mono rounded border border-gray-700 p-2 resize-none focus:outline-none focus:border-indigo-500"
              spellCheck={false}
            />
          )}
          {result && (
            <div className={`text-xs ${result.ok ? 'text-green-400' : 'text-red-400'}`}>{result.message}</div>
          )}
        </div>
        <div className="flex justify-end gap-2 px-4 py-3 border-t border-gray-700">
          <button onClick={onClose} className="px-3 py-1.5 text-xs rounded bg-gray-700 text-gray-300 hover:bg-gray-600">关闭</button>
          <button
            onClick={handleSave}
            disabled={saving || loading}
            className="px-3 py-1.5 text-xs bg-indigo-600 text-white rounded hover:bg-indigo-500 disabled:opacity-50"
          >
            {saving ? '同步中…' : '保存并同步'}
          </button>
        </div>
      </div>
    </div>
  );
}

function CodexPoolSettingsModal({
  initial,
  onClose,
  onSaved,
}: {
  initial: CodexPoolSettings;
  onClose: () => void;
  onSaved: () => Promise<void>;
}) {
  const [form, setForm] = useState(initial);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSave = async () => {
    setSaving(true);
    setError(null);
    try {
      await api.putCodexPoolSettings(form);
      await onSaved();
      onClose();
    } catch (e) {
      setError(e instanceof Error ? e.message : '保存失败');
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="absolute inset-0 bg-gray-900/80 z-10 flex items-start justify-center pt-12">
      <div className="bg-gray-800 rounded-lg shadow-xl w-full max-w-xs">
        <div className="flex items-center justify-between px-4 py-3 border-b border-gray-700">
          <h3 className="text-sm font-semibold text-foreground">Codex 号池设置</h3>
          <button onClick={onClose} className="text-gray-400 hover:text-foreground"><X size={14} /></button>
        </div>
        <div className="p-4 space-y-4">
          <label className="flex items-start justify-between gap-3 text-xs text-gray-300">
            <span>
              <span className="block font-medium text-foreground">启用账号路由</span>
              <span className="mt-1 block text-[10px] text-gray-500">关闭后阻止新的 Codex 回合，不会中断正在执行的回合。</span>
            </span>
            <input
              type="checkbox"
              checked={form.enabled}
              onChange={(e) => setForm((value) => ({ ...value, enabled: e.target.checked }))}
              className="mt-0.5 accent-emerald-500"
            />
          </label>
          <label className="block text-xs text-gray-300">
            撞限冷却时间（秒）
            <input
              type="number"
              min={1}
              max={691200}
              value={form.cooldown_seconds}
              onChange={(e) => setForm((value) => ({ ...value, cooldown_seconds: Number(e.target.value) }))}
              className="mt-1 w-full rounded bg-gray-900 border border-gray-700 px-2.5 py-1.5 text-xs text-foreground outline-none focus:border-emerald-500"
            />
          </label>
          <label className="block text-xs text-gray-300">
            主动换号阈值（%）
            <input
              type="number"
              min={1}
              max={100}
              step={1}
              value={form.quota_switch_threshold_percent}
              onChange={(e) => setForm((value) => ({ ...value, quota_switch_threshold_percent: Number(e.target.value) }))}
              className="mt-1 w-full rounded bg-gray-900 border border-gray-700 px-2.5 py-1.5 text-xs text-foreground outline-none focus:border-emerald-500"
            />
          </label>
          <label className="block text-xs text-gray-300">
            新会话路由顺序
            <select
              value={form.routing_policy}
              onChange={(e) => setForm((value) => ({
                ...value,
                routing_policy: e.target.value as CodexPoolSettings['routing_policy'],
              }))}
              className="mt-1 w-full rounded bg-gray-900 border border-gray-700 px-2.5 py-1.5 text-xs text-foreground outline-none focus:border-emerald-500"
            >
              <option value="api_first">API 优先，OAuth 回退</option>
              <option value="native_first">OAuth 优先，API 回退</option>
            </select>
          </label>
          <p className="text-[10px] leading-relaxed text-gray-500">
            设置与账号列表一起安全持久化，保存后立即生效；服务器配置路径仍由部署环境管理。
          </p>
          {error && <div className="text-xs text-red-400">{error}</div>}
        </div>
        <div className="flex justify-end gap-2 px-4 py-3 border-t border-gray-700">
          <button onClick={onClose} className="px-3 py-1.5 text-xs rounded bg-gray-700 text-gray-300 hover:bg-gray-600">取消</button>
          <button
            onClick={handleSave}
            disabled={saving || !Number.isFinite(form.cooldown_seconds) || !Number.isFinite(form.quota_switch_threshold_percent)}
            className="px-3 py-1.5 text-xs bg-emerald-600 text-white rounded hover:bg-emerald-500 disabled:opacity-50"
          >
            {saving ? '保存中…' : '保存并生效'}
          </button>
        </div>
      </div>
    </div>
  );
}

type PoolTab = 'claude' | 'codex';

export function PoolDrawer() {
  const [claudeEnabled, setClaudeEnabled] = useState(false);
  const [codexEnabled, setCodexEnabled] = useState(false);
  const [apiAccountsAvailable, setApiAccountsAvailable] = useState(false);
  const [apiAccountCatalog, setApiAccountCatalog] = useState<Record<string, CloudRouterAccount>>({});
  const [open, setOpen] = useState(false);
  const [tab, setTab] = useState<PoolTab>('claude');

  // Claude pool state
  const [claudeStatus, setClaudeStatus] = useState<PoolUsageStatus | null>(null);
  const [claudeLoading, setClaudeLoading] = useState(false);
  const [claudeError, setClaudeError] = useState<string | null>(null);

  // Codex pool state
  const [codexStatus, setCodexStatus] = useState<CodexPoolUsageStatus | null>(null);
  const [codexLoading, setCodexLoading] = useState(false);
  const [codexError, setCodexError] = useState<string | null>(null);
  const codexUsageRequestSeq = useRef(0);

  const loadApiAccountCatalog = useCallback(async () => {
    try {
      const accounts = await api.getCloudRouterAccounts();
      setApiAccountCatalog(Object.fromEntries(
        accounts
          .filter((account) => Boolean(account.id))
          .map((account) => [account.id, account]),
      ));
      setApiAccountsAvailable(true);
    } catch {
      // This endpoint is administrator-only. Native Pool views remain usable
      // for other users without exposing even a partial credential fingerprint.
    }
  }, []);

  useEffect(() => {
    api.getPoolStatus()
      .then((s) => setClaudeEnabled(s.enabled))
      .catch(() => setClaudeEnabled(false));
    api.getCodexPoolStatus()
      .then(() => { setCodexEnabled(true); })
      .catch(() => setCodexEnabled(false));
    // The API-account manager remains available even when both native pools
    // are disabled, so users can add the first key from the same drawer.
    void loadApiAccountCatalog();
  }, [loadApiAccountCatalog]);

  const loadClaudeUsage = useCallback(async (force?: boolean) => {
    setClaudeLoading(true);
    setClaudeError(null);
    try {
      setClaudeStatus(await api.getPoolUsage(force));
    } catch (e) {
      setClaudeError(e instanceof Error ? e.message : '加载失败');
    } finally {
      setClaudeLoading(false);
    }
  }, []);

  const loadCodexUsage = useCallback(async (force = true) => {
    const requestSeq = ++codexUsageRequestSeq.current;
    setCodexLoading(true);
    setCodexError(null);
    if (force) {
      // A previous account snapshot is not evidence of the current quota.
      // Hide it while the live-only request is pending or if that request fails.
      setCodexStatus(null);
    }
    try {
      const status = await api.getCodexPoolUsage(force);
      if (requestSeq === codexUsageRequestSeq.current) {
        setCodexStatus(status);
      }
    } catch (e) {
      if (requestSeq === codexUsageRequestSeq.current) {
        setCodexError(e instanceof Error ? e.message : '加载失败');
      }
    } finally {
      if (requestSeq === codexUsageRequestSeq.current) {
        setCodexLoading(false);
      }
    }
  }, []);

  useEffect(() => () => {
    // Invalidate any request still in flight when the drawer component leaves.
    codexUsageRequestSeq.current += 1;
  }, []);

  useEffect(() => {
    if (open) {
      if (tab === 'claude' && claudeEnabled) loadClaudeUsage();
      else if (tab === 'codex' && codexEnabled) loadCodexUsage(true);
    }
  }, [open, tab, claudeEnabled, codexEnabled, loadClaudeUsage, loadCodexUsage]);

  // Claude handlers
  const handleClaudeClearCooldown = useCallback(async (accountId: string) => {
    try { await api.clearPoolCooldown(accountId); await loadClaudeUsage(); } catch { /* Keep current drawer state on request failure. */ }
  }, [loadClaudeUsage]);

  const handleClaudeSetPreferred = useCallback(async (accountId: string | null) => {
    try { await api.setPoolPreferred(accountId); await loadClaudeUsage(); } catch { /* Keep current drawer state on request failure. */ }
  }, [loadClaudeUsage]);

  const [relogin, setRelogin] = useState<Record<string, { status: string; message?: string }>>({});
  const [showAdd, setShowAdd] = useState(false);
  const [showCodexAdd, setShowCodexAdd] = useState(false);
  const [showApiAdd, setShowApiAdd] = useState(false);
  const [showCcSettings, setShowCcSettings] = useState(false);
  const [showCodexSettings, setShowCodexSettings] = useState(false);
  const [apiDeleting, setApiDeleting] = useState<Record<string, boolean>>({});

  const handleClaudeRelogin = useCallback(async (accountId: string) => {
    setRelogin((m) => ({ ...m, [accountId]: { status: 'running' } }));
    try {
      const res = await api.poolRelogin(accountId);
      if (res.status === 'success') {
        setRelogin((m) => ({ ...m, [accountId]: { status: 'success' } }));
        await loadClaudeUsage();
        return;
      }
      const poll = async () => {
        const s = await api.poolReloginStatus(accountId);
        if (s.status === 'running') { setTimeout(poll, 5000); return; }
        setRelogin((m) => ({ ...m, [accountId]: {
          status: s.status,
          message: s.status === 'failed' ? `登录失败：${(s.detail || '').slice(-300)}` : undefined,
        } }));
        if (s.status === 'success') await loadClaudeUsage();
      };
      setTimeout(poll, 5000);
    } catch (e) {
      setRelogin((m) => ({ ...m, [accountId]: {
        status: 'failed',
        message: e instanceof Error ? e.message : '重新登录失败',
      } }));
    }
  }, [loadClaudeUsage]);

  // Codex handlers
  const handleCodexClearCooldown = useCallback(async (accountId: string) => {
    try { await api.clearCodexPoolCooldown(accountId); await loadCodexUsage(); } catch { /* Keep current drawer state on request failure. */ }
  }, [loadCodexUsage]);

  const handleCodexSetPreferred = useCallback(async (accountId: string | null) => {
    try { await api.setCodexPoolPreferred(accountId); await loadCodexUsage(); } catch { /* Keep current drawer state on request failure. */ }
  }, [loadCodexUsage]);

  const [codexRelogin, setCodexRelogin] = useState<Record<string, CodexLoginStatus>>({});
  const codexReloginAlive = useRef(true);
  const codexReloginTimers = useRef<Map<string, ReturnType<typeof setTimeout>>>(new Map());

  useEffect(() => {
    // React StrictMode runs an extra setup/cleanup cycle in development, so
    // restore the live flag on every setup rather than only initializing it.
    codexReloginAlive.current = true;
    const timers = codexReloginTimers.current;
    return () => {
      codexReloginAlive.current = false;
      for (const timer of timers.values()) clearTimeout(timer);
      timers.clear();
    };
  }, []);

  const handleCodexRelogin = useCallback(async (accountId: string) => {
    setCodexRelogin((m) => ({ ...m, [accountId]: { status: 'running' } }));
    try {
      const started = await api.codexPoolRelogin(accountId);
      setCodexRelogin((m) => ({ ...m, [accountId]: {
        status: started.status,
        attempt_id: started.attempt_id,
      } }));

      const schedulePoll = (delay: number) => {
        const previous = codexReloginTimers.current.get(accountId);
        if (previous) clearTimeout(previous);
        const timer = setTimeout(poll, delay);
        codexReloginTimers.current.set(accountId, timer);
      };
      const poll = async () => {
        if (!codexReloginAlive.current) return;
        try {
          const s = await api.codexPoolReloginStatus(accountId);
          if (!codexReloginAlive.current) return;
          setCodexRelogin((m) => ({ ...m, [accountId]: s }));
          if (ACTIVE_CODEX_LOGIN_STATUSES.has(s.status)) {
            schedulePoll(2000);
            return;
          }
          codexReloginTimers.current.delete(accountId);
          if (s.status === 'success') await loadCodexUsage();
        } catch (e) {
          if (!codexReloginAlive.current) return;
          setCodexRelogin((current) => ({
            ...current,
            [accountId]: {
              ...(current[accountId] || { status: 'running' }),
              detail: e instanceof Error
                ? `状态查询暂时失败，正在重试：${e.message}`
                : '状态查询暂时失败，正在重试',
            },
          }));
          schedulePoll(2000);
        }
      };
      schedulePoll(1000);
    } catch (e) {
      setCodexRelogin((m) => ({ ...m, [accountId]: {
        status: 'failed',
        detail: e instanceof Error ? e.message : '重新登录失败',
      } }));
    }
  }, [loadCodexUsage]);

  const handleCodexSubmitOtp = useCallback(async (
    accountId: string,
    state: CodexLoginStatus,
    code: string,
  ) => {
    if (!state.attempt_id || !state.challenge_id) {
      throw new Error('验证码挑战信息缺失，请重新登录');
    }
    await api.codexPoolSubmitOtp(state.attempt_id, state.challenge_id, code);
    setCodexRelogin((current) => ({
      ...current,
      [accountId]: { ...state, status: 'verifying_otp' },
    }));
  }, []);

  const refreshBothPools = useCallback(async () => {
    // create/refresh already performs the live API-provider requests. Read the
    // resulting pool snapshots without issuing duplicate force requests.
    await Promise.all([
      loadClaudeUsage(false),
      loadCodexUsage(false),
      loadApiAccountCatalog(),
    ]);
    const [claudeResult, codexResult] = await Promise.allSettled([
      api.getPoolStatus(),
      api.getCodexPoolStatus(),
    ]);
    const nextClaudeEnabled = claudeResult.status === 'fulfilled' && claudeResult.value.enabled;
    const nextCodexEnabled = codexResult.status === 'fulfilled' && codexResult.value.enabled !== false;
    if (claudeResult.status === 'fulfilled') {
      setClaudeEnabled(nextClaudeEnabled);
    }
    if (codexResult.status === 'fulfilled') {
      setCodexEnabled(nextCodexEnabled);
    }
    if (!nextClaudeEnabled && nextCodexEnabled) setTab('codex');
    else if (nextClaudeEnabled && !nextCodexEnabled) setTab('claude');
    setApiAccountsAvailable(true);
  }, [loadApiAccountCatalog, loadClaudeUsage, loadCodexUsage]);

  const handleApiRefresh = useCallback(async (accountId: string | null | undefined) => {
    if (!accountId) {
      window.alert('API 账号标识缺失，无法安全刷新');
      return;
    }
    try {
      await api.refreshCloudRouterAccount(accountId);
      await refreshBothPools();
    } catch (e) {
      window.alert(e instanceof Error ? e.message : 'API 账号刷新失败');
    }
  }, [refreshBothPools]);

  const handleApiDelete = useCallback(async (
    accountId: string | null | undefined,
    displayName: string,
    cleanupPending: boolean,
  ) => {
    // Pool projections have their own provider-specific ids. Only the shared
    // API account id is valid for deletion, otherwise one projection could be
    // mistaken for the underlying credential account.
    if (!accountId) {
      window.alert('API 账号标识缺失，已拒绝删除；请刷新账号池后重试');
      return;
    }
    const prompt = cleanupPending
      ? `重试清理 API 账号“${displayName}”？\n\n`
        + '账号已经同时从 Claude 与 Codex 新任务中停用。重试成功后会永久删除 API Key 和账号配置，无法恢复；'
        + 'Claude projects 与 Codex sessions 会保留。活跃任务不会被强制终止，若阻塞仍未解除，账号会继续显示“删除受阻”。'
      : `删除 API 账号“${displayName}”？\n\n`
        + '此操作会同时从 Claude 与 Codex 账号视图停用该账号，并永久删除 API Key 和账号配置，无法恢复。\n\n'
        + 'Claude projects 与 Codex sessions 会保留，供已有任务上下文迁移；活跃任务不会被强制终止。'
        + '账号会先立即停用；若删除受阻，请解除界面显示的具体原因后点击“重试清理”。';
    if (!window.confirm(prompt)) return;

    setApiDeleting((current) => ({ ...current, [accountId]: true }));
    try {
      const result = await api.deleteCloudRouterAccount(accountId);
      await refreshBothPools();
      if (result.cleanup_pending) {
        window.alert('账号已停用，但删除仍受阻。请解除界面显示的具体原因后点击“重试清理”。');
      }
    } catch (e) {
      // Retirement is staged before the backend checks every runtime fence.
      // Refresh both projections even on 409 so the resumable tombstone is
      // immediately visible from whichever tab initiated the request.
      await refreshBothPools();
      if (isApiRequestError(e) && e.status === 409) {
        const diagnostics = apiCleanupConflictDiagnostics(e);
        const summary = apiCleanupSummary(diagnostics?.cleanup_code);
        const backendDetail = diagnostics?.cleanup_reason?.trim();
        window.alert(
          `账号已停用，但删除受阻：${summary}`
          + (backendDetail && backendDetail !== summary ? `\n后端详情：${backendDetail}` : '')
          + '\n\n'
          + '活跃任务不会被强制终止。请解除该阻塞原因后点击“重试清理”；'
          + 'Claude projects 与 Codex sessions 会保留。',
        );
      } else {
        window.alert(e instanceof Error ? e.message : 'API 账号删除失败');
      }
    } finally {
      setApiDeleting((current) => {
        const next = { ...current };
        delete next[accountId];
        return next;
      });
    }
  }, [refreshBothPools]);

  if (!claudeEnabled && !codexEnabled && !apiAccountsAvailable) return null;

  const hasBothPools = claudeEnabled && codexEnabled;
  const hasActivePool = claudeEnabled || codexEnabled;
  const loading = tab === 'claude' ? claudeLoading : codexLoading;

  return (
    <>
      <button
        onClick={() => {
          if (!claudeEnabled && codexEnabled) setTab('codex');
          setOpen(true);
        }}
        className="flex items-center gap-1 px-2 py-1 rounded bg-gray-800 border border-gray-700 hover:border-indigo-500 transition-colors"
        title={hasActivePool ? '账号池额度' : 'API 账号与额度'}
      >
        <Users size={13} className="text-indigo-400" />
        <span className="text-xs font-semibold text-indigo-300">{hasActivePool ? 'Pro' : 'API'}</span>
      </button>
      {open && createPortal(
        <div className="fixed inset-0 z-[70]">
          <div className="absolute inset-0 bg-black/50" onClick={() => { if (!showCodexAdd) setOpen(false); }} />
          <div className="absolute right-0 top-0 h-full w-full max-w-sm bg-gray-900 border-l border-gray-700 shadow-xl flex flex-col pt-[env(safe-area-inset-top)]">
            <div className="flex items-center gap-2 px-4 py-3 border-b border-gray-700">
              <Users size={16} className="text-indigo-400" />
              <h2 className="text-sm font-semibold text-foreground">
                {!hasActivePool ? 'API 账号' : tab === 'claude' ? 'Claude Pool' : 'Codex Pool'}
              </h2>
              {hasActivePool && tab === 'claude' && claudeStatus && (
                <span className="text-xs text-gray-500">
                  {claudeStatus.available}/{claudeStatus.total} 可用
                </span>
              )}
              {hasActivePool && tab === 'codex' && codexStatus && (
                <span className="text-xs text-gray-500">
                  {codexStatus.available}/{codexStatus.total} 可用
                </span>
              )}
              <div className="ml-auto flex items-center gap-1">
                {claudeEnabled && tab === 'claude' && (
                  <button
                    onClick={() => setShowCcSettings(true)}
                    className="p-1.5 rounded text-gray-400 hover:text-foreground hover:bg-gray-800"
                    title="CC Settings 模板"
                  >
                    <Settings size={14} />
                  </button>
                )}
                {codexEnabled && tab === 'codex' && codexStatus?.settings && (
                  <button
                    onClick={() => setShowCodexSettings(true)}
                    className="p-1.5 rounded text-gray-400 hover:text-foreground hover:bg-gray-800"
                    title="Codex 号池设置"
                  >
                    <Settings size={14} />
                  </button>
                )}
                {hasActivePool && (
                  <button
                    onClick={() => tab === 'claude' ? setShowAdd(true) : setShowCodexAdd(true)}
                    className="p-1.5 rounded text-gray-400 hover:text-foreground hover:bg-gray-800"
                    title="添加账号"
                  >
                    <Plus size={14} />
                  </button>
                )}
                <button
                  onClick={() => setShowApiAdd(true)}
                  className="px-1.5 py-1 rounded text-[10px] font-semibold text-sky-300 border border-sky-600/40 hover:bg-sky-600/15"
                  title="添加 API 账号"
                >
                  API
                </button>
                {hasActivePool && (
                  <button
                    onClick={() => tab === 'claude' ? loadClaudeUsage(true) : loadCodexUsage(true)}
                    disabled={loading}
                    className="p-1.5 rounded text-gray-400 hover:text-foreground hover:bg-gray-800 disabled:opacity-50"
                    title="刷新"
                  >
                    <RefreshCw size={14} className={loading ? 'animate-spin' : ''} />
                  </button>
                )}
                <button
                  onClick={() => { if (!showCodexAdd) setOpen(false); }}
                  disabled={showCodexAdd}
                  className="p-1.5 rounded text-gray-400 hover:text-foreground hover:bg-gray-800 disabled:opacity-40"
                  title={showCodexAdd ? '请先关闭账号登录窗口' : undefined}
                >
                  <X size={14} />
                </button>
              </div>
            </div>

            {/* Tab bar */}
            {hasBothPools && (
              <div className="flex border-b border-gray-700">
                <button
                  onClick={() => setTab('claude')}
                  className={`flex-1 py-2 text-xs font-medium text-center transition-colors ${
                    tab === 'claude'
                      ? 'text-indigo-300 border-b-2 border-indigo-500 bg-gray-800/50'
                      : 'text-gray-500 hover:text-gray-300'
                  }`}
                >
                  Claude
                </button>
                <button
                  onClick={() => setTab('codex')}
                  className={`flex-1 py-2 text-xs font-medium text-center transition-colors ${
                    tab === 'codex'
                      ? 'text-emerald-300 border-b-2 border-emerald-500 bg-gray-800/50'
                      : 'text-gray-500 hover:text-gray-300'
                  }`}
                >
                  Codex
                </button>
              </div>
            )}

            <div className="flex-1 overflow-y-auto p-3 space-y-2 relative">
              {showAdd && <AddAccountModal onClose={() => setShowAdd(false)} onAdded={loadClaudeUsage} />}
              {showCodexAdd && <AddCodexAccountModal onClose={() => setShowCodexAdd(false)} onAdded={loadCodexUsage} />}
              {showApiAdd && (
                <AddApiAccountModal
                  onClose={() => setShowApiAdd(false)}
                  onAdded={refreshBothPools}
                />
              )}
              {showCcSettings && <CcSettingsModal onClose={() => setShowCcSettings(false)} />}
              {showCodexSettings && codexStatus?.settings && (
                <CodexPoolSettingsModal
                  initial={codexStatus.settings}
                  onClose={() => setShowCodexSettings(false)}
                  onSaved={() => loadCodexUsage(false)}
                />
              )}

              {!hasActivePool && (
                <div className="rounded-lg border border-sky-700/40 bg-sky-950/20 p-3 text-xs leading-relaxed text-gray-400">
                  还没有可用账号。点击右上角 <span className="font-semibold text-sky-300">API</span>，
                  选择 CloudRouter 或 ApexRouter，并添加相应的 API Key。
                </div>
              )}

              {/* Claude tab */}
              {hasActivePool && tab === 'claude' && (
                <>
                  {claudeError && <div className="text-xs text-red-400">{claudeError}</div>}
                  {claudeLoading && !claudeStatus && <div className="text-xs text-gray-500">加载中…</div>}
                  {claudeStatus?.accounts.map((a) => (
                    <AccountCard
                      key={a.id}
                      account={a}
                      preferred={claudeStatus?.preferred ?? null}
                      lastSelected={claudeStatus?.last_selected ?? null}
                      apiAccount={a.api_account_id ? apiAccountCatalog[a.api_account_id] : undefined}
                      onClearCooldown={handleClaudeClearCooldown}
                      onSetPreferred={handleClaudeSetPreferred}
                      onRelogin={handleClaudeRelogin}
                      onRetryUsage={() => {
                        if (isApiAuthKind(a.auth_kind)) {
                          void handleApiRefresh(a.api_account_id);
                        } else {
                          void loadClaudeUsage(true);
                        }
                      }}
                      onDelete={isApiAuthKind(a.auth_kind)
                        ? () => {
                            void handleApiDelete(
                              a.api_account_id,
                              a.display_name || a.api_account_id || '未命名 API 账号',
                              isApiCleanupPending(
                                a,
                                a.api_account_id ? apiAccountCatalog[a.api_account_id] : undefined,
                              ),
                            );
                          }
                        : async () => {
                            if (!window.confirm(`从 Claude 号池中删除 ${a.id}？`)) return;
                            try { await api.poolDeleteAccount(a.id); await loadClaudeUsage(); } catch (e) { window.alert(String(e)); }
                          }}
                      deleting={isApiAuthKind(a.auth_kind) && a.api_account_id
                        ? apiDeleting[a.api_account_id] === true
                        : false}
                      reloginState={relogin[a.id]}
                    />
                  ))}
                </>
              )}

              {/* Codex tab */}
              {hasActivePool && tab === 'codex' && (
                <>
                  {codexError && <div className="text-xs text-red-400">{codexError}</div>}
                  {codexLoading && !codexStatus && <div className="text-xs text-gray-500">加载中…</div>}
                  {codexStatus?.settings && !codexStatus.settings.enabled && (
                    <div className="rounded border border-amber-500/40 bg-amber-500/10 p-2 text-xs text-amber-300">
                      Codex 账号路由已暂停；新的 Codex 回合会被明确拒绝，且不会回落到默认账号。
                    </div>
                  )}
                  {codexStatus?.accounts.map((a) => (
                    <CodexAccountCard
                      key={`${a.id}:${a.codex_home}`}
                      account={a}
                      preferred={codexStatus.preferred ?? null}
                      lastSelected={codexStatus.last_selected ?? null}
                      apiAccount={a.api_account_id ? apiAccountCatalog[a.api_account_id] : undefined}
                      onClearCooldown={handleCodexClearCooldown}
                      onSetPreferred={handleCodexSetPreferred}
                      onRelogin={handleCodexRelogin}
                      onSubmitOtp={(state, code) => handleCodexSubmitOtp(a.id, state, code)}
                      onDelete={isApiAuthKind(a.auth_kind)
                        ? () => {
                            void handleApiDelete(
                              a.api_account_id,
                              a.display_name || a.api_account_id || '未命名 API 账号',
                              isApiCleanupPending(
                                a,
                                a.api_account_id ? apiAccountCatalog[a.api_account_id] : undefined,
                              ),
                            );
                          }
                        : async () => {
                            if (!window.confirm(`从 Codex 号池中删除 ${a.id}？将清除 OAuth、邮箱 Token、OpenAI 密码以及该账号的日志、历史和配置；仅保留原生会话文件用于任务上下文迁移。`)) return;
                            try { await api.codexPoolDeleteAccount(a.id); await loadCodexUsage(); } catch (e) { window.alert(String(e)); }
                          }}
                      deleting={isApiAuthKind(a.auth_kind) && a.api_account_id
                        ? apiDeleting[a.api_account_id] === true
                        : false}
                      onRetryUsage={() => {
                        if (isApiAuthKind(a.auth_kind)) {
                          void handleApiRefresh(a.api_account_id);
                        } else {
                          void loadCodexUsage(true);
                        }
                      }}
                      reloginState={codexRelogin[a.id]}
                    />
                  ))}
                </>
              )}
            </div>
          </div>
        </div>,
        document.body,
      )}
    </>
  );
}
