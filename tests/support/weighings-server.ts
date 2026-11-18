// HTTP-only fixture process: never subscribes to operational RabbitMQ queues.
import { Audit } from '../../src/application/ledger.js';
import { Weighings } from '../../src/application/weighings.js';
import { configuration } from '../../src/infra/config.js';
import { PgLedger } from '../../src/infra/postgres.js';
import { PgWeighings } from '../../src/infra/pg-weighings.js';
import { DiskPhotos } from '../../src/infra/storage.js';
import { http } from '../../src/infra/http.js';
const c = configuration(), storage = new DiskPhotos(c.storagePath, c.maxFileBytes, c.maxPixels);
const app = http(new Audit(new PgLedger(c.pool, c.schema)), { service: new Weighings(new PgWeighings(c.pool, c.schema), storage), storage, maxJsonBytes: c.maxJsonBytes });
let closing: Promise<void> | undefined;
function close() { return closing ??= (async () => { await app.close(); await c.pool.end(); if (process.connected) process.disconnect(); })(); }
process.on('message', m => { if (m === 'stop') void close(); }); process.once('SIGTERM', () => void close());
try { const url = await app.listen({ host: '127.0.0.1', port: 0 }); process.send?.({ url }); }
catch { process.send?.({ error: 'FIXTURE_START_FAILED' }); await close(); process.exitCode = 1; }
