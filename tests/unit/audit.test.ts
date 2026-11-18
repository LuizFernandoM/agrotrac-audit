import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { Audit } from '../../src/application/ledger.js';
import { canonical, validateEvent, type Event } from '../../src/domain/event.js';
import { MemoryLedger } from '../support/memory.js';
const event = (): Event => ({ event_id: randomUUID(), type: 'agrotrac.core.booking.created', version: '1.0.0', producer: 'agrotrac-core', occurred_at: new Date().toISOString(), correlation_id: randomUUID(), payload: { booking: { a: 1, b: [2, { x: 'value' }] }, value: 1 } });
test('core and future live envelopes are accepted, versions and malformed fields rejected', () => {
  const e = event(); assert.equal(validateEvent(e).ok, true);
  assert.equal(validateEvent({ ...e, producer: 'agrotrac-live', type: 'agrotrac.live.session.opened' }).ok, true);
  for (const patch of [{ event_id: 'bad' }, { correlation_id: 'bad' }, { producer: 'other' }, { producer: 'agrotrac-live' }, { occurred_at: '2026-02-30T00:00:00Z' }, { occurred_at: '2026-11-15T00:00:00+00:00' }, { payload: [] }, { payload: { bad: Number.MAX_SAFE_INTEGER + 1 } }, { payload: { bad: '\u0000' } }, { payload: { bad: '\ud800' } }, { unknown: 1 }]) assert.equal(validateEvent({ ...e, ...patch }).ok, false);
  const version = validateEvent({ ...e, version: '2.0.0' }); assert.equal(!version.ok && version.error.code, 'UNSUPPORTED_VERSION');
});
test('canonical comparison ignores object order, preserves arrays and values', () => {
  assert.equal(canonical({ z: [{ b: 2, a: 1 }], a: 1 }), canonical({ a: 1, z: [{ a: 1, b: 2 }] }));
  assert.notEqual(canonical([1, 2]), canonical([2, 1])); assert.notEqual(canonical({ a: 1 }), canonical({ a: '1' }));
});
test('sequential and concurrent identical deliveries yield one immutable entry', async () => {
  const memory = new MemoryLedger(), audit = new Audit(memory), e = event();
  assert.deepEqual(await audit.ingest(e), { ok: true, value: 'inserted' });
  const reordered = { ...e, payload: { value: 1, booking: { b: [2, { x: 'value' }], a: 1 } } };
  const results = await Promise.all(Array.from({ length: 20 }, () => audit.ingest(reordered)));
  assert.ok(results.every(r => r.ok && r.value === 'duplicate')); assert.equal(memory.entries.size, 1);
  assert.deepEqual(memory.entries.get(e.event_id)!.event, e);
});
test('conflicting event_id returns failure and preserves original', async () => {
  const memory = new MemoryLedger(), audit = new Audit(memory), e = event(); await audit.ingest(e);
  const conflict = await audit.ingest({ ...e, payload: { changed: true } });
  assert.equal(!conflict.ok && conflict.error.code, 'EVENT_ID_CONFLICT'); assert.deepEqual(memory.entries.get(e.event_id)!.event, e);
});
test('validation is independent from HTTP and rejects cycles/deep payloads', async () => {
  const memory = new MemoryLedger(), audit = new Audit(memory); assert.equal((await audit.ingest({})).ok, false);
  const cycle: Record<string, unknown> = {}; cycle.self = cycle;
  assert.equal((await audit.ingest({ ...event(), payload: cycle })).ok, false); assert.equal(memory.entries.size, 0);
});
test('bounded cursor pagination and exact filters', async () => {
  const memory = new MemoryLedger(), audit = new Audit(memory), e = event(); await audit.ingest(e);
  await audit.ingest({ ...event(), correlation_id: e.correlation_id }); await audit.ingest(event());
  const page = await audit.list({ limit: '1' }); assert.ok(page.ok); assert.equal(page.value.items.length, 1); assert.equal(page.value.next_cursor, '1');
  const next = await audit.list({ limit: '2', cursor: page.value.next_cursor }); assert.ok(next.ok); assert.equal(next.value.items.length, 2); assert.equal(next.value.next_cursor, null);
  const filtered = await audit.list({ correlation_id: e.correlation_id }); assert.ok(filtered.ok); assert.equal(filtered.value.items.length, 2);
  for (const query of [{ limit: '0' }, { limit: '101' }, { limit: 1 }, { cursor: '-1' }, { cursor: '9223372036854775808' }, { event_id: 'bad' }, { type: 'bad' }, { unknown: 'x' }]) assert.equal((await audit.list(query)).ok, false);
});
