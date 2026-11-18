import { readFileSync } from 'node:fs';
import { parse } from 'dotenv';
import { Pool } from 'pg';
import { schemaName } from './postgres.js';
import { isAbsolute, resolve } from 'node:path';
import { FILE_CEILING } from '../domain/weighing.js';
export function configuration() {
  const local = process.env.AUDIT_INFRA_ENV_PATH ? parse(readFileSync(process.env.AUDIT_INFRA_ENV_PATH)) : {};
  const env = { ...local, ...process.env };
  const schema = env.AUDIT_DB_SCHEMA ?? ''; schemaName(schema);
  const storagePath = env.AUDIT_STORAGE_PATH ?? resolve('uploads', 'romaneios');
  const maxFileBytes = Number(env.AUDIT_MAX_FILE_BYTES ?? 10485760), maxJsonBytes = Number(env.AUDIT_MAX_JSON_BYTES ?? 65536), maxPixels = Number(env.AUDIT_MAX_IMAGE_PIXELS ?? 16777216);
  if (!isAbsolute(storagePath) || !Number.isSafeInteger(maxFileBytes) || maxFileBytes < 1 || maxFileBytes > FILE_CEILING || !Number.isSafeInteger(maxJsonBytes) || maxJsonBytes < 1 || maxJsonBytes > 65536 || !Number.isSafeInteger(maxPixels) || maxPixels < 1 || maxPixels > 16777216) throw new Error('Invalid Audit storage limits/path');
  if (!env.AUDIT_DATABASE_URL && (!(env.PGUSER ?? env.POSTGRES_USER) || !(env.PGPASSWORD ?? env.POSTGRES_PASSWORD))) throw new Error('Audit database credentials must be explicitly configured');
  const pool = new Pool(env.AUDIT_DATABASE_URL ? { connectionString: env.AUDIT_DATABASE_URL, max: 10, connectionTimeoutMillis: 3000, statement_timeout: 5000, query_timeout: 6000 } : {
    host: env.PGHOST ?? '127.0.0.1', port: Number(env.PGPORT ?? 5432), user: env.PGUSER ?? env.POSTGRES_USER,
    password: env.PGPASSWORD ?? env.POSTGRES_PASSWORD, database: env.AUDIT_PGDATABASE ?? 'agrotrac_audit', max: 10, connectionTimeoutMillis: 3000, statement_timeout: 5000, query_timeout: 6000,
  });
  pool.on('error', () => {});
  const rabbit = env.RABBITMQ_URL ?? `amqp://${encodeURIComponent(env.RABBITMQ_DEFAULT_USER ?? '')}:${encodeURIComponent(env.RABBITMQ_DEFAULT_PASS ?? '')}@127.0.0.1:5672`;
  return { pool, schema, rabbit, storagePath, maxFileBytes, maxJsonBytes, maxPixels, host: env.AUDIT_HOST ?? '127.0.0.1', port: Number(env.AUDIT_PORT ?? 3003), topology: {
    exchange: env.AUDIT_EVENTS_EXCHANGE ?? 'agrotrac.events', queue: env.AUDIT_EVENTS_QUEUE ?? 'agrotrac.audit.events',
    quarantineExchange: env.AUDIT_QUARANTINE_EXCHANGE ?? 'agrotrac.audit.quarantine', quarantineQueue: env.AUDIT_QUARANTINE_QUEUE ?? 'agrotrac.audit.quarantine.events',
  } };
}
