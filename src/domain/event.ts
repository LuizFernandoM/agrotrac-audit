export type Failure = { code: string; status: number };
export type Result<T> = { ok: true; value: T } | { ok: false; error: Failure };
export const ok = <T>(value: T): Result<T> => ({ ok: true, value });
export const fail = (code: string, status = 400): Result<never> => ({ ok: false, error: { code, status } });
export const uuid = (v: unknown): v is string => typeof v === 'string' && /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(v);
export const eventType = (v: unknown): v is string => typeof v === 'string' && v.length <= 100 && /^agrotrac\.(core|live)(\.[a-z0-9_]+)+$/.test(v);
export const object = (v: unknown): v is Record<string, unknown> => !!v && typeof v === 'object' && !Array.isArray(v) && [Object.prototype, null].includes(Object.getPrototypeOf(v));
export function utc(v: unknown): v is string {
  if (typeof v !== 'string' || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3})?Z$/.test(v)) return false;
  const d = new Date(v); return Number.isFinite(d.getTime()) && d.toISOString() === (v.length === 20 ? v.replace('Z', '.000Z') : v);
}
export type Event = { event_id: string; type: string; version: '1.0.0'; producer: 'agrotrac-core' | 'agrotrac-live'; occurred_at: string; correlation_id: string; payload: Record<string, unknown> };
const jsonString = (v: string) => !/\u0000|[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/.test(v);
function json(v: unknown, depth = 0): boolean {
  if (depth > 32) return false;
  if (typeof v === 'string') return jsonString(v);
  if (v === null || typeof v === 'boolean') return true;
  if (typeof v === 'number') return Number.isFinite(v) && (!Number.isInteger(v) || Number.isSafeInteger(v));
  if (Array.isArray(v)) return Array.from(v).every(item => json(item, depth + 1));
  return object(v) && Object.entries(v).every(([key, item]) => jsonString(key) && json(item, depth + 1));
}
export function validateEvent(input: unknown): Result<Event> {
  if (!object(input) || Object.keys(input).sort().join(',') !== 'correlation_id,event_id,occurred_at,payload,producer,type,version'
    || !uuid(input.event_id) || !uuid(input.correlation_id) || !eventType(input.type) || !utc(input.occurred_at)
    || !['agrotrac-core', 'agrotrac-live'].includes(input.producer as string)
    || !input.type.startsWith(`${String(input.producer).replace('-', '.')}.`) || !object(input.payload) || !json(input.payload)) return fail('INVALID_ENVELOPE');
  if (input.version !== '1.0.0') return fail('UNSUPPORTED_VERSION');
  return ok(input as Event);
}
// JSONB object equality ignores key order; arrays retain order. Used by memory adapter too.
export function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  if (object(value)) return `{${Object.keys(value).sort().map(k => `${JSON.stringify(k)}:${canonical(value[k])}`).join(',')}}`;
  return JSON.stringify(value);
}
