import type { Pool } from 'pg';
import { schemaName } from './postgres.js';
export async function migrate(pool: Pool, schema: string, requireNew = false, target: 1 | 2 = 2) {
  const s = schemaName(schema), c = await pool.connect();
  try {
    await c.query('BEGIN');
    await c.query('SELECT pg_advisory_xact_lock(hashtext($1))', [`agrotrac-audit:${schema}`]);
    const exists = await c.query('SELECT 1 FROM information_schema.schemata WHERE schema_name=$1', [schema]);
    if (requireNew && exists.rowCount) throw new Error('Integration requires new Audit schema');
    const tables = await c.query('SELECT table_name FROM information_schema.tables WHERE table_schema=$1', [schema]);
    let versions: number[] = [];
    if (tables.rowCount) {
      if (!tables.rows.some(r => r.table_name === 'audit_migrations')) throw new Error('Unrecognized existing Audit schema');
      versions = (await c.query(`SELECT version FROM ${s}.audit_migrations ORDER BY version`)).rows.map(r => r.version);
      if (!['1', '1,2'].includes(versions.join(',')) || (target === 1 && versions.length !== 1)) throw new Error('Unrecognized Audit migration version');
      const expected = versions.length === 1 ? ['audit_migrations', 'event_ledger'] : ['audit_migrations', 'event_ledger', 'weighings'];
      if (tables.rows.map(r => r.table_name).sort().join(',') !== expected.sort().join(',')) throw new Error('Unrecognized existing Audit tables');
    } else {
    await c.query(`CREATE SCHEMA IF NOT EXISTS ${s}`);
    await c.query(`
      CREATE TABLE ${s}.event_ledger (
        sequence bigserial PRIMARY KEY, event_id uuid NOT NULL UNIQUE, type varchar(100) NOT NULL,
        correlation_id uuid NOT NULL, envelope jsonb NOT NULL CHECK(jsonb_typeof(envelope)='object'),
        received_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        CHECK(envelope->>'event_id'=event_id::text AND envelope->>'type'=type AND envelope->>'correlation_id'=correlation_id::text),
        CHECK(envelope ?& ARRAY['event_id','type','version','producer','occurred_at','correlation_id','payload']),
        CHECK(envelope->>'version'='1.0.0'), CHECK(envelope->>'producer' IN ('agrotrac-core','agrotrac-live')),
        CHECK(jsonb_typeof(envelope->'payload')='object')
      );
      CREATE INDEX ON ${s}.event_ledger(type,sequence);
      CREATE INDEX ON ${s}.event_ledger(correlation_id,sequence);
      CREATE TABLE ${s}.audit_migrations(version integer PRIMARY KEY,applied_at timestamptz NOT NULL DEFAULT now());
      INSERT INTO ${s}.audit_migrations(version) VALUES(1);
    `);
      versions = [1];
    }
    if (target === 2 && versions.length === 1) await c.query(`
      CREATE TABLE ${s}.weighings (
        local_id uuid PRIMARY KEY, sequence bigserial UNIQUE NOT NULL,
        placa varchar(7) NOT NULL CHECK(placa ~ '^[A-Z]{3}[0-9][A-Z0-9][0-9]{2}$'), booking_id uuid, captured_at timestamptz,
        peso_bruto_kg bigint NOT NULL CHECK(peso_bruto_kg BETWEEN 1 AND 9007199254740991),
        tara_kg bigint NOT NULL CHECK(tara_kg BETWEEN 0 AND 9007199254740991),
        peso_liquido_kg bigint NOT NULL CHECK(peso_liquido_kg BETWEEN 0 AND 9007199254740991),
        data jsonb NOT NULL CHECK(jsonb_typeof(data)='object'),
        file_sha256 varchar(64) NOT NULL CHECK(file_sha256 ~ '^[a-f0-9]{64}$'),
        file_size_bytes bigint NOT NULL CHECK(file_size_bytes BETWEEN 1 AND 104857600),
        file_mime_type text NOT NULL CHECK(file_mime_type IN ('image/jpeg','image/png')),
        received_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        CHECK(peso_bruto_kg>=tara_kg AND peso_liquido_kg=peso_bruto_kg-tara_kg),
        CHECK(data ?& ARRAY['local_id','placa','peso_bruto_kg','tara_kg','peso_liquido_kg','booking_id','captured_at','motorista_nome','motorista_documento','observacoes']),
        CHECK(jsonb_typeof(data->'peso_bruto_kg')='number' AND jsonb_typeof(data->'tara_kg')='number' AND jsonb_typeof(data->'peso_liquido_kg')='number'),
        CHECK((data->>'booking_id')::uuid IS NOT DISTINCT FROM booking_id AND (data->>'captured_at')::timestamptz IS NOT DISTINCT FROM captured_at),
        CHECK(data->>'local_id'=local_id::text AND data->>'placa'=placa AND (data->>'peso_bruto_kg')::bigint=peso_bruto_kg AND (data->>'tara_kg')::bigint=tara_kg AND (data->>'peso_liquido_kg')::bigint=peso_liquido_kg)
      );
      CREATE INDEX ON ${s}.weighings(placa,sequence);
      CREATE INDEX ON ${s}.weighings(booking_id,sequence);
      CREATE INDEX ON ${s}.weighings(file_sha256);
      INSERT INTO ${s}.audit_migrations(version) VALUES(2);
    `);
    await c.query('COMMIT');
  } catch (e) { await c.query('ROLLBACK'); throw e; } finally { c.release(); }
}
