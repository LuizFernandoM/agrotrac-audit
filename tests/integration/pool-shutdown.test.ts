import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { performance } from 'node:perf_hooks';
import { Pool, type PoolClient } from 'pg';
import { Audit } from '../../src/application/ledger.js';
import type { Event } from '../../src/domain/event.js';
import { configuration } from '../../src/infra/config.js';
import { migrate } from '../../src/infra/migration.js';
import { PgLedger } from '../../src/infra/postgres.js';
import { AuditConsumer } from '../../src/infra/consumer.js';
import { bounded, closeConnection, connect } from '../../src/infra/amqp.js';

const delay = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
async function until(predicate: () => boolean, label: string) {
  const deadline = performance.now() + 3000;
  while (performance.now() < deadline) { if (predicate()) return; await delay(5); }
  assert.fail(`Timed out: ${label}`);
}
const event = (): Event => ({ event_id: randomUUID(), correlation_id: randomUUID(), type: 'agrotrac.live.pool.test', producer: 'agrotrac-live', version: '1.0.0', occurred_at: new Date().toISOString(), payload: { scenario: 'saturated-pool-checkout' } });

test('real saturated PostgreSQL pool bounds checkout and consumer shutdown without premature ACK', { timeout: 15000 }, async t => {
  const schema = `audit_pool_it_${Date.now()}_${randomUUID().replaceAll('-', '').slice(0, 8)}`;
  process.env.AUDIT_DB_SCHEMA = schema;
  const { pool: observer, rabbit } = configuration();
  // Keep the production configuration assertion alongside the shortened real-driver fixture.
  assert.equal(observer.options.connectionTimeoutMillis, 3000);
  assert.equal(observer.options.statement_timeout, 5000);
  assert.equal(observer.options.query_timeout, 6000);
  const checkoutDeadline = 250;
  const pool = new Pool({ ...observer.options, password: observer.options.password, min: 1, max: 1, connectionTimeoutMillis: checkoutDeadline });
  pool.on('error', () => {});
  const ledger = new PgLedger(pool, schema), audit = new Audit(ledger), run = randomUUID().replaceAll('-', '');
  const topology = { exchange: `agrotrac.audit.pool.${run}`, queue: `agrotrac.audit.pool.${run}.input`, quarantineExchange: `agrotrac.audit.pool.${run}.quarantine`, quarantineQueue: `agrotrac.audit.pool.${run}.rejected` };
  const first = new AuditConsumer(rabbit, audit, topology, { prefetch: 1, timeout: 500 });
  let recoveredAcks = 0, redeliverySeen = false, rowsVisibleBeforeAck = 0, held: PoolClient | undefined;
  const count = async (id: string) => Number((await observer.query(`SELECT count(*) n FROM "${schema}".event_ledger WHERE event_id=$1`, [id])).rows[0].n);
  const replacement = new AuditConsumer(rabbit, audit, topology, { prefetch: 1, timeout: 500, hooks: {
    beforeAck: async message => { redeliverySeen = message.fields.redelivered; rowsVisibleBeforeAck = await count(JSON.parse(message.content.toString()).event_id); },
    afterAck: () => { recoveredAcks++; },
  } });
  const broker = await connect(rabbit, 2000), channel = await broker.createConfirmChannel(); channel.on('error', () => {});
  console.log(`POOL_REVIEW_SCHEMA=${schema}`);
  console.log(`POOL_REVIEW_TOPOLOGY=${JSON.stringify(topology)}`);
  try {
    const database = (await observer.query('SELECT current_database() name')).rows[0].name; assert.equal(database, 'agrotrac_audit');
    const tables = (await observer.query('SELECT table_name FROM information_schema.tables WHERE table_schema=$1', [schema])).rows;
    assert.equal(tables.length, 0); await migrate(observer, schema, true);
    held = await pool.connect();
    const pid = (await held.query('SELECT pg_backend_pid() pid')).rows[0].pid;
    assert.equal(pool.totalCount, 1); assert.equal(pool.idleCount, 0);
    console.log(`SATURATED_POOL=max=1,min=1,held_backend_pid=${pid},connectionTimeoutMillis=${checkoutDeadline}`);
    await t.test('PgLedger.append checkout times out while the only real client remains held', async () => {
      const e = event(), start = performance.now();
      // Attach rejection observation immediately; no unhandled rejection while inspecting waitingCount.
      const result = ledger.append(e).then(() => 'unexpected-success', () => 'checkout-rejected');
      await until(() => pool.waitingCount === 1, 'append queued for checkout');
      assert.equal(await count(e.event_id), 0);
      assert.equal(await bounded(result, 2000), 'checkout-rejected');
      const duration = Math.round(performance.now() - start);
      assert.ok(duration >= checkoutDeadline - 30 && duration < 1500, `checkout duration ${duration}ms`);
      assert.equal(pool.waitingCount, 0); assert.equal(pool.idleCount, 0); assert.equal(await count(e.event_id), 0);
      assert.equal((await held!.query('SELECT pg_backend_pid() pid')).rows[0].pid, pid);
      console.log(`CHECKOUT_TIMEOUT_MS=${duration}; waiting=0; client_still_held=true; ledger_rows=0`);
    });
    await t.test('stop drains pending checkout within deadline, no ACK/no row, real redelivery later commits', async () => {
      void first.start(); await until(() => first.ready, 'first consumer ready');
      const e = event();
      await new Promise<void>((resolve, reject) => channel.publish(topology.exchange, e.type, Buffer.from(JSON.stringify(e)), { persistent: true, mandatory: true, contentType: 'application/json', messageId: e.event_id }, err => err ? reject(new Error('PUBLISH_FAILED')) : resolve()));
      await until(() => pool.waitingCount === 1, 'consumer append pending checkout');
      assert.equal(first.stats.acked, 0); assert.equal(await count(e.event_id), 0);
      const start = performance.now(); await bounded(first.stop(), 2000); const duration = Math.round(performance.now() - start);
      assert.ok(duration < 1500, `stop duration ${duration}ms`);
      assert.equal(first.stats.acked, 0); assert.equal(first.stats.inserted, 0); assert.equal(await count(e.event_id), 0);
      assert.equal(pool.waitingCount, 0); assert.equal(pool.idleCount, 0);
      const pending = await channel.checkQueue(topology.queue); assert.equal(pending.messageCount, 1);
      console.log(`STOP_WITH_CHECKOUT_MS=${duration}; ACK_before_persistence=0; ledger_rows=0; ready_messages_after_stop=${pending.messageCount}`);
      // Only release the scarce client AFTER stop resolved: saturation was present for its entire duration.
      held!.release(); held = undefined;
      void replacement.start(); await until(() => recoveredAcks === 1, 'replacement committed and ACKed redelivery');
      assert.equal(redeliverySeen, true); assert.equal(rowsVisibleBeforeAck, 1); assert.equal(await count(e.event_id), 1);
      await replacement.stop();
      const final = await channel.checkQueue(topology.queue); assert.equal(final.messageCount, 0);
      console.log(`RECOVERY_EVENT_ID=${e.event_id}; redelivered=true; committed_rows_visible_before_ACK=${rowsVisibleBeforeAck}; recovered_ACKs=${recoveredAcks}; final_ledger_rows=1`);
    });
    assert.equal(Number((await observer.query(`SELECT count(*) n FROM "${schema}".event_ledger`)).rows[0].n), 1);
  } finally {
    // On a failed assertion release the held fixture before cleanup, preserving all messages/data.
    held?.release(); await first.stop(); await replacement.stop();
    await channel.close().catch(() => {}); await closeConnection(broker, 500); await pool.end(); await observer.end();
  }
});
