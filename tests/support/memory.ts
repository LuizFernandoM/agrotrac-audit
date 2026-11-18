import type { Entry, Filters, Ledger, Page } from '../../src/application/ledger.js';
import { canonical, type Event } from '../../src/domain/event.js';
export class MemoryLedger implements Ledger {
  readonly entries = new Map<string, Entry>();
  async append(event: Event) {
    const previous = this.entries.get(event.event_id);
    if (previous) return { inserted: false, same: canonical(previous.event) === canonical(event) };
    this.entries.set(event.event_id, { sequence: String(this.entries.size + 1), event: structuredClone(event), received_at: new Date().toISOString() });
    return { inserted: true, same: true };
  }
  async list(filters: Filters): Promise<Page> {
    const rows = [...this.entries.values()].filter(r => BigInt(r.sequence) > BigInt(filters.cursor) && (!filters.type || r.event.type === filters.type) && (!filters.correlation_id || r.event.correlation_id === filters.correlation_id) && (!filters.event_id || r.event.event_id === filters.event_id));
    const items = structuredClone(rows.slice(0, filters.limit));
    return { items, next_cursor: rows.length > filters.limit ? items.at(-1)!.sequence : null };
  }
}
