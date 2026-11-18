import type { Pool } from 'pg';
import { canonical, fail, ok } from '../domain/event.js';
import type { Photo, RecordWeighing, Weighing } from '../domain/weighing.js';
import type { WeighingFilters, WeighingRepository } from '../application/weighings.js';
import { schemaName } from './postgres.js';
function map(row: any): RecordWeighing {
  const value = { ...row.data } as Weighing;
  return { ...value, sequence: String(row.sequence), received_at: row.received_at.toISOString(), romaneio: { file_sha256: row.file_sha256, file_size_bytes: Number(row.file_size_bytes), mime_type: row.file_mime_type } };
}
export class PgWeighings implements WeighingRepository {
  private schema: string;
  constructor(private pool: Pool, schema: string, private hooks: { beforeCommit?(input: Weighing): Promise<void> } = {}) { this.schema = schemaName(schema); }
  async append(input: Weighing, photo: Photo) {
    const c = await this.pool.connect();
    try {
      await c.query('BEGIN'); await c.query("SET LOCAL lock_timeout='3s'"); await c.query("SET LOCAL statement_timeout='5s'");
      // Sequence allocation and commit are serialized, including previously nonexistent IDs.
      await c.query('SELECT pg_advisory_xact_lock(hashtext($1))', [`audit-weighings:${this.schema}`]);
      const inserted = await c.query(`INSERT INTO ${this.schema}.weighings(local_id,placa,booking_id,captured_at,peso_bruto_kg,tara_kg,peso_liquido_kg,data,file_sha256,file_size_bytes,file_mime_type) VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,$10,$11) ON CONFLICT(local_id) DO NOTHING RETURNING *`, [input.local_id, input.placa, input.booking_id, input.captured_at, input.peso_bruto_kg, input.tara_kg, input.peso_liquido_kg, JSON.stringify(input), photo.file_sha256, photo.file_size_bytes, photo.mime_type]);
      const row = inserted.rows[0] ?? (await c.query(`SELECT * FROM ${this.schema}.weighings WHERE local_id=$1`, [input.local_id])).rows[0];
      const same = canonical(row.data) === canonical(input) && row.file_sha256 === photo.file_sha256 && Number(row.file_size_bytes) === photo.file_size_bytes && row.file_mime_type === photo.mime_type;
      if (!same) { await c.query('ROLLBACK'); return fail('WEIGHING_ID_CONFLICT', 409); }
      await this.hooks.beforeCommit?.(input); await c.query('COMMIT');
      return ok({ created: !!inserted.rowCount, record: map(row) });
    } catch (e) { await c.query('ROLLBACK'); throw e; } finally { c.release(); }
  }
  async get(id: string) { const r = await this.pool.query(`SELECT * FROM ${this.schema}.weighings WHERE local_id=$1`, [id]); return r.rows[0] ? map(r.rows[0]) : undefined; }
  async list(filters: WeighingFilters) {
    const values: unknown[] = [filters.cursor], conditions = ['sequence>$1::bigint'];
    for (const key of ['local_id', 'booking_id', 'placa'] as const) if (filters[key]) { values.push(filters[key]); conditions.push(`${key}=$${values.length}`); }
    values.push(filters.limit + 1);
    const r = await this.pool.query(`SELECT * FROM ${this.schema}.weighings WHERE ${conditions.join(' AND ')} ORDER BY sequence LIMIT $${values.length}`, values);
    const items = r.rows.slice(0, filters.limit).map(map);
    return { items, next_cursor: r.rows.length > filters.limit ? items.at(-1)!.sequence : null };
  }
}
