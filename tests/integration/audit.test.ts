import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { Pool } from 'pg';
import type { ConsumeMessage } from 'amqplib';
import { Audit } from '../../src/application/ledger.js';
import { configuration } from '../../src/infra/config.js';
import { PgLedger } from '../../src/infra/postgres.js';
import { migrate } from '../../src/infra/migration.js';
import { http } from '../../src/infra/http.js';
import { AuditConsumer } from '../../src/infra/consumer.js';
import { connect, closeConnection } from '../../src/infra/amqp.js';
import { canonical, type Event } from '../../src/domain/event.js';
const delay = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
async function until(predicate: () => boolean | Promise<boolean>, label: string, timeout = 10000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) { if (await predicate()) return; await delay(20); }
  assert.fail(`Timed out: ${label}`);
}
const fresh = (): Event => ({ event_id: randomUUID(), type: 'agrotrac.live.session.opened', producer: 'agrotrac-live', version: '1.0.0', occurred_at: new Date().toISOString(), correlation_id: randomUUID(), payload: { sample: { b: 2, a: 1 }, order: [1, 2] } });
const idOf = (m: ConsumeMessage) => { try { return JSON.parse(m.content.toString()).event_id as string; } catch { return m.properties.messageId as string; } };
test('Audit SQL/HTTP/RabbitMQ with opt-in real Core producer', { timeout: 120000 }, async t => {
  const corePath = process.env.AUDIT_E2E_CORE_PATH;
  assert.ok(corePath, 'AUDIT_E2E_CORE_PATH must explicitly opt in to local Core source; integration is never skipped');
  const module = (path: string) => import(pathToFileURL(resolve(corePath, path)).href);
  const [{ Core }, { PgStore }, { migrate: migrateCore }, { http: coreHttp }, { RabbitPublisher }, { OutboxWorker }] = await Promise.all([
    module('src/application/core.ts'), module('src/infra/postgres.ts'), module('src/infra/migration.ts'), module('src/infra/http.ts'), module('src/infra/publisher.ts'), module('src/infra/outbox.ts'),
  ]);
  const suffix = `${Date.now()}_${randomUUID().replaceAll('-', '').slice(0, 8)}`;
  const schema = `audit_it_${suffix}`, coreSchema = `core_audit_it_${suffix}`;
  process.env.AUDIT_DB_SCHEMA = schema;
  const { pool, rabbit } = configuration();
  const corePool = new Pool({ ...pool.options, password: pool.options.password, connectionString: undefined, database: process.env.AUDIT_E2E_CORE_DATABASE ?? 'agrotrac_core' });
  corePool.on('error', () => {});
  const s = `"${schema}"`, run = randomUUID().replaceAll('-', '');
  const topology = { exchange: `agrotrac.audit.test.${run}`, queue: `agrotrac.audit.test.${run}.input`, quarantineExchange: `agrotrac.audit.test.${run}.quarantine`, quarantineQueue: `agrotrac.audit.test.${run}.rejected` };
  const coreTopology = { exchange: topology.exchange, auditQueue: topology.queue, liveQueue: `agrotrac.audit.test.${run}.live` };
  console.log(`AUDIT_SCHEMA=${schema}; CORE_FIXTURE_SCHEMA=${coreSchema}`);
  console.log(`ISOLATED_TOPOLOGY=${JSON.stringify({ ...topology, liveQueue: coreTopology.liveQueue })}`);
  let sqlOffline = false, beforeCommitFailures = 0, ackFailureId: string | undefined;
  let gateId: string | undefined, releaseQuarantine: (() => void) | undefined, quarantineReached: (() => void) | undefined;
  const acks = new Map<string, number>(), redelivered = new Set<string>(), trace: string[] = [];
  const ledger = new PgLedger(pool, schema, { beforeCommit: async () => { if (sqlOffline) { beforeCommitFailures++; throw new Error('INJECTED_SQL_FAILURE'); } } });
  const audit = new Audit(ledger), app = http(audit);
  const consumer = new AuditConsumer(rabbit, audit, topology, { prefetch: 4, attempts: 3, retryDelay: 40, reconnectDelay: 300, maxReconnectDelay: 1000, timeout: 1000, hooks: {
    beforeAck: async (m, disposition) => { const id = idOf(m); if (m.fields.redelivered) redelivered.add(id); trace.push(`beforeAck:${id}:${disposition}`); if (ackFailureId === id) { ackFailureId = undefined; throw new Error('INJECTED_ACK_FAILURE'); } },
    afterAck: (m, disposition) => { const id = idOf(m); acks.set(id, (acks.get(id) ?? 0) + 1); trace.push(`ack:${id}:${disposition}`); },
    quarantineConfirmed: async m => { const id = idOf(m); trace.push(`quarantineConfirmed:${id}`); if (gateId === id) { quarantineReached?.(); await new Promise<void>(r => { releaseQuarantine = r; }); gateId = undefined; } },
  } });
  const coreApp = coreHttp(new Core(new PgStore(corePool, coreSchema), { id: randomUUID, now: () => new Date().toISOString() }));
  const corePublisher = new RabbitPublisher(rabbit, coreTopology, 1500);
  const broker = await connect(rabbit, 3000), channel = await broker.createConfirmChannel(); channel.on('error', () => {});
  let auditUrl = '', coreUrl = '', coreBookingEvent: Event | undefined;
  async function sendRaw(raw: Buffer, key = 'agrotrac.live.session.opened', messageId: string = randomUUID()) {
    await new Promise<void>((resolve, reject) => { channel.publish(topology.exchange, key, raw, { persistent: true, contentType: 'application/json', mandatory: true, messageId }, e => e ? reject(new Error('TEST_PUBLISH_FAILED')) : resolve()); });
  }
  const send = (e: Event) => sendRaw(Buffer.from(JSON.stringify(e)), e.type, e.event_id);
  const count = async (id: string) => Number((await pool.query(`SELECT count(*) n FROM ${s}.event_ledger WHERE event_id=$1`, [id])).rows[0].n);
  async function get(query = '') {
    const r = await fetch(`${auditUrl}/v1/audit/events${query}`, { signal: AbortSignal.timeout(5000) });
    const data = await r.json() as any; assert.ok(r.headers.get('x-correlation-id')); return { status: r.status, data };
  }
  async function corePost(route: string, body: unknown) {
    const r = await fetch(`${coreUrl}/v1/core/${route}`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body), signal: AbortSignal.timeout(5000) });
    const data = await r.json() as any; assert.equal(r.status, 201); return data;
  }
  try {
    const databases = { audit: (await pool.query('SELECT current_database() name')).rows[0].name, core: (await corePool.query('SELECT current_database() name')).rows[0].name };
    assert.equal(databases.audit, 'agrotrac_audit'); assert.notEqual(databases.audit, databases.core);
    console.log(`DATABASES=${JSON.stringify(databases)}`);
    const inspection = (await pool.query("SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog','information_schema') ORDER BY 1,2")).rows;
    console.log(`AUDIT_PRE_MIGRATION_TABLES=${JSON.stringify(inspection)}`);
    await migrate(pool, schema, true); await assert.rejects(migrate(pool, schema, true), /new Audit schema/); await migrate(pool, schema);
    await migrateCore(corePool, coreSchema, true);
    auditUrl = await app.listen({ host: '127.0.0.1', port: 0 }); coreUrl = await coreApp.listen({ host: '127.0.0.1', port: 0 });
    void consumer.start(); await until(() => consumer.ready, 'consumer subscribed');

    await t.test('Core HTTP reservation -> atomic outbox -> RabbitMQ -> other database -> Audit GET', async () => {
      const silo = await corePost('silos', { codigo: randomUUID(), nome: 'Audit E2E', tipo_grao: 'SOJA', capacidade_total_kg: 10000 });
      const window = await corePost('windows', { silo_id: silo.id, inicio_em: new Date(Date.now() + 60000).toISOString(), termino_em: new Date(Date.now() + 3600000).toISOString(), capacidade_maxima_kg: 10000 });
      const booking = await corePost('bookings', { window_id: window.id, placa: 'ABC1D23', motorista_nome: 'Teste Audit', peso_estimado_kg: 2000 });
      const recordingPublisher = { publish: async (event: Event, key: string) => { if (event.type === 'agrotrac.core.booking.created') coreBookingEvent = event; await corePublisher.publish(event, key); }, close: async () => {} };
      const worker = new OutboxWorker(corePool, coreSchema, recordingPublisher); let published = 0;
      while (await worker.dispatchOne()) { published++; assert.ok(published < 10); }
      assert.equal(published, 3); assert.ok(coreBookingEvent);
      await until(() => acks.has(coreBookingEvent!.event_id), 'Core booking persisted and ACKed');
      const response = await get(`?event_id=${coreBookingEvent.event_id}`);
      assert.equal(response.status, 200); assert.equal(response.data.items.length, 1); assert.equal(response.data.items[0].event.payload.booking_id, booking.id);
      assert.equal(canonical(response.data.items[0].event), canonical(coreBookingEvent)); assert.equal(await count(coreBookingEvent.event_id), 1);
      console.log(`E2E_BOOKING_EVENT=${coreBookingEvent.event_id}; core.outbox.published=3; audit.entries=3`);
    });
    await t.test('sequential and concurrent broker replay with reordered JSON keys never duplicates', async () => {
      const original = coreBookingEvent!; const baseline = acks.get(original.event_id)!;
      await send(original); await until(() => (acks.get(original.event_id) ?? 0) >= baseline + 1, 'sequential replay');
      const reordered = Object.fromEntries(Object.entries(original).reverse()) as Event;
      reordered.payload = Object.fromEntries(Object.entries(original.payload).reverse());
      await Promise.all(Array.from({ length: 20 }, () => send(reordered)));
      await until(() => (acks.get(original.event_id) ?? 0) >= baseline + 21, '20 concurrent replays');
      assert.equal(await count(original.event_id), 1);
      // Separate concurrent transactions exercise the UNIQUE/JSONB path without transport serialization.
      const outcomes = await Promise.all(Array.from({ length: 10 }, () => audit.ingest(reordered)));
      assert.ok(outcomes.every(r => r.ok && r.value === 'duplicate')); assert.equal(await count(original.event_id), 1);
      console.log('IDEMPOTENCY=1 sequential +20 concurrent broker replays +10 concurrent SQL attempts =>1 row');
    });
    await t.test('before-COMMIT SQL failure leaves no row or ACK; bounded retry/reconnect then persists', async () => {
      const e = fresh(), previousReconnects = consumer.stats.reconnects; sqlOffline = true;
      await send(e);
      await until(() => consumer.stats.reconnects > previousReconnects, 'failed delivery session closed');
      assert.equal(await count(e.event_id), 0); assert.equal(acks.has(e.event_id), false); assert.equal(beforeCommitFailures, 3);
      sqlOffline = false; await until(() => acks.has(e.event_id), 'SQL recovered redelivery');
      assert.equal(await count(e.event_id), 1); assert.ok(redelivered.has(e.event_id)); assert.ok(consumer.stats.sqlRetries >= 2);
      console.log('SQL_FAILURE=3 rollbacks,0 rows,0 ACK before recovery; broker redelivery =>1 committed row');
    });
    await t.test('COMMIT followed by ACK failure redelivers and preserves one row', async () => {
      const e = fresh(); ackFailureId = e.event_id; const generation = consumer.generation;
      await send(e); await until(async () => (await count(e.event_id)) === 1 && !consumer.ready, 'commit occurred before ACK failure');
      assert.equal(acks.has(e.event_id), false);
      await until(() => acks.has(e.event_id) && consumer.generation > generation, 'ACK failure reconnected');
      assert.equal(await count(e.event_id), 1); assert.ok(redelivered.has(e.event_id));
      assert.ok(trace.includes(`ack:${e.event_id}:duplicate`));
      console.log('ACK_FAILURE=commit then connection recycle; redelivery duplicate ACK; ledger rows=1');
    });
    await t.test('invalid JSON/version/conflict quarantine durably, preserve original; confirm precedes ACK', async () => {
      const invalidId = randomUUID(); gateId = invalidId;
      const reached = new Promise<void>(resolve => { quarantineReached = resolve; });
      await sendRaw(Buffer.from('{broken'), 'agrotrac.core.invalid', invalidId); await reached;
      assert.equal(acks.has(invalidId), false);
      const rejected = await channel.get(topology.quarantineQueue, { noAck: false }); assert.ok(rejected);
      assert.equal(rejected.properties.deliveryMode, 2);
      const rejection = JSON.parse(rejected.content.toString()); assert.equal(rejection.error.code, 'INVALID_JSON'); assert.equal(Buffer.from(rejection.raw_base64, 'base64').toString(), '{broken');
      // Keep quarantine observations unacked; closing this test channel requeues fixtures intact.
      releaseQuarantine!(); await until(() => acks.has(invalidId), 'ACK after quarantine confirm gate');
      assert.ok(trace.indexOf(`quarantineConfirmed:${invalidId}`) < trace.indexOf(`ack:${invalidId}:quarantined`));
      const unsupported = { ...fresh(), version: '9.0.0' };
      await sendRaw(Buffer.from(JSON.stringify(unsupported)), unsupported.type, unsupported.event_id);
      await until(() => acks.has(unsupported.event_id), 'unsupported version quarantined');
      const versionMessage = await channel.get(topology.quarantineQueue, { noAck: false }); assert.ok(versionMessage); assert.equal(JSON.parse(versionMessage.content.toString()).error.code, 'UNSUPPORTED_VERSION');
      const conflict = { ...coreBookingEvent!, payload: { changed: true } }; const baseline = acks.get(conflict.event_id)!;
      await send(conflict); await until(() => acks.get(conflict.event_id)! > baseline, 'conflict quarantined');
      const conflictMessage = await channel.get(topology.quarantineQueue, { noAck: false }); assert.ok(conflictMessage); assert.equal(JSON.parse(conflictMessage.content.toString()).error.code, 'EVENT_ID_CONFLICT');
      assert.equal(await count(conflict.event_id), 1);
      const preserved = await get(`?event_id=${conflict.event_id}`); assert.equal(canonical(preserved.data.items[0].event), canonical(coreBookingEvent));
      console.log('QUARANTINE=INVALID_JSON,UNSUPPORTED_VERSION,EVENT_ID_CONFLICT; original preserved; confirms before ACK');
    });
    await t.test('quarantine routing failure does not ACK; reconnect restores binding and retains poison message', async () => {
      const id = randomUUID(), generation = consumer.generation;
      await channel.unbindQueue(topology.quarantineQueue, topology.quarantineExchange, 'rejected');
      await sendRaw(Buffer.from('not-json'), 'agrotrac.core.invalid', id);
      await until(() => !consumer.ready, 'unrouted quarantine failed session');
      assert.equal(acks.has(id), false); assert.equal(await channel.get(topology.quarantineQueue, { noAck: false }), false);
      await until(() => consumer.generation > generation && acks.has(id), 'routing recovered after reconnect');
      const m = await channel.get(topology.quarantineQueue, { noAck: false }); assert.ok(m); assert.equal(JSON.parse(m.content.toString()).source.message_id, id);
      console.log('QUARANTINE_UNROUTED=0 ACK before reconnection; confirmed on redelivery');
    });
    await t.test('structurally invalid envelope and routing mismatch are retained in quarantine', async () => {
      const malformed = { ...fresh(), payload: [] }, wrongRoute = fresh();
      await sendRaw(Buffer.from(JSON.stringify(malformed)), malformed.type, malformed.event_id);
      await sendRaw(Buffer.from(JSON.stringify(wrongRoute)), 'agrotrac.core.wrong.route', wrongRoute.event_id);
      await until(() => acks.has(malformed.event_id) && acks.has(wrongRoute.event_id), 'invalid envelope dispositions');
      assert.equal(await count(malformed.event_id), 0); assert.equal(await count(wrongRoute.event_id), 0);
      const reasons = new Set<string>();
      for (let i = 0; i < 2; i++) { const m = await channel.get(topology.quarantineQueue, { noAck: false }); assert.ok(m); reasons.add(JSON.parse(m.content.toString()).error.code); }
      assert.deepEqual(reasons, new Set(['INVALID_ENVELOPE', 'ROUTING_KEY_MISMATCH']));
    });
    await t.test('HTTP filters, bounded keyset pagination, malformed queries and append-only behavior', async () => {
      const e1 = fresh(), e2 = { ...fresh(), correlation_id: randomUUID() }; e1.correlation_id = e2.correlation_id;
      await Promise.all([send(e1), send(e2)]); await until(() => acks.has(e1.event_id) && acks.has(e2.event_id), 'filter fixtures');
      const response = await get(`?type=${e1.type}&correlation_id=${e1.correlation_id}`); assert.equal(response.data.items.length, 2);
      assert.equal((await get(`?event_id=${e1.event_id}&type=agrotrac.core.booking.created`)).data.items.length, 0);
      const ids = new Set<string>(); let cursor = '';
      for (let page = 0; page < 20; page++) { const r = await get(`?limit=2${cursor ? `&cursor=${cursor}` : ''}`); assert.equal(r.status, 200); assert.ok(r.data.items.length <= 2); for (const entry of r.data.items) { assert.equal(ids.has(entry.event.event_id), false); ids.add(entry.event.event_id); assert.equal(typeof entry.sequence, 'string'); } cursor = r.data.next_cursor; if (!cursor) break; }
      const total = Number((await pool.query(`SELECT count(*) n FROM ${s}.event_ledger`)).rows[0].n); assert.equal(ids.size, total);
      for (const query of ['?limit=0', '?limit=101', '?limit=1.5', '?cursor=-1', '?cursor=9223372036854775808', '?event_id=bad', '?correlation_id=bad', '?type=bad', '?unknown=1']) {
        const invalid = await get(query); assert.equal(invalid.status, 400); assert.equal(invalid.data.error.code, 'INVALID_QUERY'); assert.ok(invalid.data.error.correlation_id); }
      assert.equal((await fetch(`${auditUrl}/v1/audit/events`, { headers: { 'x-correlation-id': 'bad' } })).status, 400);
      assert.equal((await fetch(`${auditUrl}/v1/audit/weighings`)).status, 404);
      assert.equal((await fetch(`${auditUrl}/v1/audit/events`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{}' })).status, 404);
      console.log(`HTTP_PAGINATION=${ids.size} unique entries,limit=2; filtered correlation=2; invalid queries=9x400`);
    });
    await t.test('separate consumer recovers from simulated broker refusal without stopping shared RabbitMQ', async () => {
      // A local TCP proxy first closes sockets, later forwards AMQP to the real broker.
      const net = await import('node:net'); const target = new URL(rabbit), sockets = new Set<import('node:net').Socket>(); let online = false;
      const proxy = net.createServer(client => {
        sockets.add(client); client.on('error', () => {}); client.on('close', () => sockets.delete(client));
        if (!online) { client.destroy(); return; }
        const upstream = net.connect(Number(target.port || 5672), target.hostname); sockets.add(upstream); upstream.on('error', () => client.destroy()); upstream.on('close', () => { sockets.delete(upstream); client.destroy(); });
        client.on('close', () => upstream.destroy()); client.pipe(upstream); upstream.pipe(client);
      });
      await new Promise<void>(resolve => proxy.listen(0, '127.0.0.1', resolve));
      const url = new URL(rabbit); url.hostname = '127.0.0.1'; url.port = String((proxy.address() as import('node:net').AddressInfo).port);
      const otherTopology = { ...topology, queue: `${topology.queue}.reconnect` };
      const other = new AuditConsumer(url.toString(), audit, otherTopology, { timeout: 300, reconnectDelay: 80, maxReconnectDelay: 200 });
      try { void other.start(); await until(() => other.stats.reconnects >= 2, 'refused connection retries'); assert.equal(other.ready, false); online = true; await until(() => other.ready, 'broker proxy recovered');
        const e = fresh(); await send(e); await until(() => other.stats.acked >= 1 && acks.has(e.event_id), 'both consumers persisted isolated deliveries'); assert.equal(await count(e.event_id), 1);
        const previous = other.generation; for (const socket of sockets) socket.destroy(); await until(() => other.generation > previous, 'established connection reconnected');
        console.log(`BROKER_RECOVERY=reconnects>=${other.stats.reconnects}; established connection restored; event rows=1`);
      } finally { await other.stop(); for (const socket of sockets) socket.destroy(); await new Promise<void>(resolve => proxy.close(() => resolve())); }
    });
    await channel.close(); // Requeue all observed quarantine records; never remove rejection fixtures.
    const inspectionChannel = await broker.createChannel();
    const quarantines = await inspectionChannel.checkQueue(topology.quarantineQueue); await inspectionChannel.close();
    assert.equal(quarantines.messageCount, 6);
    console.log(`PRESERVED_QUARANTINE_MESSAGES=${quarantines.messageCount}`);
    console.log(`FINAL_LEDGER_COUNT=${(await pool.query(`SELECT count(*) n FROM ${s}.event_ledger`)).rows[0].n}`);
    console.log(`CONSUMER_STATS=${JSON.stringify(consumer.stats)}`);
  } finally {
    sqlOffline = false; releaseQuarantine?.(); await consumer.stop(); await app.close(); await coreApp.close(); await corePublisher.close();
    await channel.close().catch(() => {}); await closeConnection(broker, 1000); await pool.end(); await corePool.end();
  }
});
