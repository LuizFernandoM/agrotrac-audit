import type { Pool } from 'pg';
import type { Entry, Filters, Ledger, Page } from '../application/ledger.js';
import type { Event } from '../domain/event.js';
export function schemaName(schema: string) {
  if (!/^[a-z][a-z0-9_]{0,62}$/.test(schema) || ['public', 'information_schema'].includes(schema) || schema.startsWith('pg_')) throw new Error('Explicit non-system AUDIT_DB_SCHEMA required');
  return `"${schema}"`;
}
export class PgLedger implements Ledger {
  private schema: string;
  constructor(readonly pool: Pool, schema: string, private hooks: { beforeCommit?(): Promise<void> } = {}) { this.schema = schemaName(schema); }
  async append(event: Event) {
    const c = await this.pool.connect();
    try {
      await c.query('BEGIN ISOLATION LEVEL READ COMMITTED');
      await c.query("SET LOCAL lock_timeout='3s'"); await c.query("SET LOCAL statement_timeout='5s'");
      // Serialize short ledger appends so cursor sequence order follows COMMIT order.
      await c.query('SELECT pg_advisory_xact_lock(hashtext($1))', [`audit-ledger:${this.schema}`]);
      const inserted = await c.query(`INSERT INTO ${this.schema}.event_ledger(event_id,type,correlation_id,envelope) VALUES($1,$2,$3,$4::jsonb) ON CONFLICT(event_id) DO NOTHING RETURNING sequence`, [event.event_id, event.type, event.correlation_id, JSON.stringify(event)]);
      // A separate READ COMMITTED statement sees the winner after concurrent conflict wait.
      const same = inserted.rowCount ? true : (await c.query(`SELECT envelope=$2::jsonb AS same FROM ${this.schema}.event_ledger WHERE event_id=$1`, [event.event_id, JSON.stringify(event)])).rows[0]?.same === true;
      await this.hooks.beforeCommit?.();
      await c.query('COMMIT'); return { inserted: !!inserted.rowCount, same };
    } catch (e) { await c.query('ROLLBACK'); throw e; } finally { c.release(); }
  }
  async list(filters: Filters): Promise<Page> {
    const values: unknown[] = [filters.cursor], conditions = ['sequence>$1::bigint'];
    for (const key of ['type', 'correlation_id', 'event_id'] as const) if (filters[key]) { values.push(filters[key]); conditions.push(`${key}=$${values.length}`); }
    values.push(filters.limit + 1);
    const result = await this.pool.query(`SELECT sequence,envelope,received_at FROM ${this.schema}.event_ledger WHERE ${conditions.join(' AND ')} ORDER BY sequence ASC LIMIT $${values.length}`, values);
    const items: Entry[] = result.rows.slice(0, filters.limit).map(r => ({ sequence: String(r.sequence), event: r.envelope, received_at: r.received_at.toISOString() }));
    return { items, next_cursor: result.rows.length > filters.limit ? items.at(-1)!.sequence : null };
  }
}
