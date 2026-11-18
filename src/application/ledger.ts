import { eventType, fail, ok, uuid, validateEvent, type Event, type Result } from '../domain/event.js';
export type Entry = { sequence: string; event: Event; received_at: string };
export type Filters = { type?: string; correlation_id?: string; event_id?: string; cursor: string; limit: number };
export type Page = { items: Entry[]; next_cursor: string | null };
export interface Ledger {
  // Resolves only after COMMIT. Concurrent insert-or-compare is atomic and append-only.
  append(event: Event): Promise<{ inserted: boolean; same: boolean }>;
  list(filters: Filters): Promise<Page>;
}
export class Audit {
  constructor(private ledger: Ledger) {}
  async ingest(input: unknown): Promise<Result<'inserted' | 'duplicate'>> {
    const validated = validateEvent(input); if (!validated.ok) return validated;
    const result = await this.ledger.append(validated.value);
    if (!result.same) return fail('EVENT_ID_CONFLICT', 409);
    return ok(result.inserted ? 'inserted' : 'duplicate');
  }
  async list(query: Record<string, unknown>): Promise<Result<Page>> {
    if (Object.keys(query).some(k => !['type', 'correlation_id', 'event_id', 'cursor', 'limit'].includes(k))) return fail('INVALID_QUERY');
    if (query.type !== undefined && !eventType(query.type)) return fail('INVALID_QUERY');
    for (const key of ['correlation_id', 'event_id']) if (query[key] !== undefined && !uuid(query[key])) return fail('INVALID_QUERY');
    const cursor = query.cursor ?? '0', limit = query.limit ?? '50';
    if (typeof cursor !== 'string' || !/^(0|[1-9]\d{0,18})$/.test(cursor) || BigInt(cursor) > 9223372036854775807n
      || typeof limit !== 'string' || !/^[1-9]\d{0,2}$/.test(limit) || Number(limit) > 100) return fail('INVALID_QUERY');
    return ok(await this.ledger.list({ type: query.type as string | undefined, correlation_id: query.correlation_id as string | undefined, event_id: query.event_id as string | undefined, cursor, limit: Number(limit) }));
  }
}
