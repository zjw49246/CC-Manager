const STORAGE_KEY = 'cc_timezone';

/** Common timezones grouped by region */
export const TIMEZONE_OPTIONS: { label: string; value: string }[] = [
  { label: 'Auto', value: 'auto' },
  { label: 'UTC', value: 'UTC' },
  { label: 'Pacific', value: 'America/Los_Angeles' },
  { label: 'Mountain', value: 'America/Denver' },
  { label: 'Central', value: 'America/Chicago' },
  { label: 'Eastern', value: 'America/New_York' },
  { label: 'São Paulo', value: 'America/Sao_Paulo' },
  { label: 'London', value: 'Europe/London' },
  { label: 'Paris', value: 'Europe/Paris' },
  { label: 'Moscow', value: 'Europe/Moscow' },
  { label: 'Dubai', value: 'Asia/Dubai' },
  { label: 'Kolkata', value: 'Asia/Kolkata' },
  { label: 'Bangkok', value: 'Asia/Bangkok' },
  { label: 'Singapore', value: 'Asia/Singapore' },
  { label: 'Shanghai', value: 'Asia/Shanghai' },
  { label: 'Seoul', value: 'Asia/Seoul' },
  { label: 'Tokyo', value: 'Asia/Tokyo' },
  { label: 'Sydney', value: 'Australia/Sydney' },
  { label: 'Auckland', value: 'Pacific/Auckland' },
];

export function getTimezone(): string {
  return localStorage.getItem(STORAGE_KEY) || 'auto';
}

export function setTimezone(tz: string) {
  localStorage.setItem(STORAGE_KEY, tz);
}

/** Resolve the effective IANA timezone string */
export function resolveTimezone(): string {
  const tz = getTimezone();
  if (tz === 'auto') {
    return Intl.DateTimeFormat().resolvedOptions().timeZone;
  }
  return tz;
}

function getDateParts(date: Date, tz: string): { year: number; month: number; day: number } {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: tz,
    year: 'numeric',
    month: 'numeric',
    day: 'numeric',
  }).formatToParts(date);
  return {
    year: Number(parts.find(p => p.type === 'year')!.value),
    month: Number(parts.find(p => p.type === 'month')!.value),
    day: Number(parts.find(p => p.type === 'day')!.value),
  };
}

/** Normalize an ISO timestamp to ensure UTC interpretation.
 *  Backend sends naive datetimes (no Z suffix) that are actually UTC. */
function ensureUtc(iso: string): string {
  if (/(?:Z|[+-]\d{2}(?::?\d{2})?)$/i.test(iso)) return iso;
  return iso + 'Z';
}

/** Parse backend DateTime values, whose missing offset always means UTC. */
export function parseBackendTimestamp(iso: string): Date {
  return new Date(ensureUtc(iso));
}

/** Format an ISO timestamp string for display in chat.
 *  Today → HH:MM, same year → MM/DD HH:MM, different year → YYYY/MM/DD HH:MM */
export function formatMessageTime(isoString: string, now?: Date): string {
  const tz = resolveTimezone();
  const date = parseBackendTimestamp(isoString);
  const msgParts = getDateParts(date, tz);
  const nowParts = getDateParts(now ?? new Date(), tz);

  const time = date.toLocaleTimeString(undefined, {
    timeZone: tz,
    hour: '2-digit',
    minute: '2-digit',
  });

  const isToday = msgParts.year === nowParts.year
    && msgParts.month === nowParts.month
    && msgParts.day === nowParts.day;

  if (isToday) return time;

  const mm = String(msgParts.month).padStart(2, '0');
  const dd = String(msgParts.day).padStart(2, '0');

  if (msgParts.year !== nowParts.year) {
    return `${msgParts.year}/${mm}/${dd} ${time}`;
  }
  return `${mm}/${dd} ${time}`;
}

/** Format an ISO timestamp for general display (tasks, headers).
 *  Always shows date + time: YYYY/MM/DD HH:MM or MM/DD HH:MM (same year). */
export function formatDateTime(isoString: string, now?: Date): string {
  const tz = resolveTimezone();
  const date = parseBackendTimestamp(isoString);
  const msgParts = getDateParts(date, tz);
  const nowParts = getDateParts(now ?? new Date(), tz);

  const time = date.toLocaleTimeString(undefined, {
    timeZone: tz,
    hour: '2-digit',
    minute: '2-digit',
  });

  const mm = String(msgParts.month).padStart(2, '0');
  const dd = String(msgParts.day).padStart(2, '0');

  if (msgParts.year !== nowParts.year) {
    return `${msgParts.year}/${mm}/${dd} ${time}`;
  }
  return `${mm}/${dd} ${time}`;
}
