import { Audit } from './application/ledger.js';
import { configuration } from './infra/config.js';
import { PgLedger } from './infra/postgres.js';
import { http } from './infra/http.js';
import { AuditConsumer } from './infra/consumer.js';
import { Weighings } from './application/weighings.js';
import { PgWeighings } from './infra/pg-weighings.js';
import { DiskPhotos } from './infra/storage.js';
const config = configuration(), audit = new Audit(new PgLedger(config.pool, config.schema));
const storage = new DiskPhotos(config.storagePath, config.maxFileBytes, config.maxPixels);
const weighings = new Weighings(new PgWeighings(config.pool, config.schema), storage);
const app = http(audit, { service: weighings, storage, maxJsonBytes: config.maxJsonBytes }), consumer = new AuditConsumer(config.rabbit, audit, config.topology);
let closing: Promise<void> | undefined;
function close() { return closing ??= (async () => { await app.close(); await consumer.stop(); await config.pool.end(); })(); }
process.once('SIGINT', () => void close()); process.once('SIGTERM', () => void close());
try {
  // Detect missing schema/migration before subscribing to operational events.
  await audit.list({ limit: '1' });
  await weighings.list({ limit: '1' }); await storage.init();
  await app.listen({ host: config.host, port: config.port });
  void consumer.start(); console.log(`Audit HTTP listening on ${config.host}:${config.port}; consumer reconnects automatically.`);
} catch { console.error('Audit startup failed; inspect configuration/migration locally.'); await close(); process.exitCode = 1; }
