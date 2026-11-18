import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { normalize } from '../../src/domain/weighing.js';
import { Weighings } from '../../src/application/weighings.js';
import { MemoryPhotos, MemoryWeighings } from '../support/weighings-memory.js';
const input = () => ({ local_id: randomUUID(), placa: 'ABC1D23', peso_bruto_kg: 40000, tara_kg: 15000, peso_liquido_kg: 25000 });
const photo = { token: randomUUID(), file_sha256: 'a'.repeat(64), file_size_bytes: 100, mime_type: 'image/jpeg' as const };
test('weighing invariants validate integer safe kg and calculate net in pure domain', () => {
  assert.equal(normalize(input()).ok, true);
  for (const patch of [{ peso_bruto_kg: 0 }, { tara_kg: -1 }, { tara_kg: 1.1 }, { peso_bruto_kg: Number.MAX_SAFE_INTEGER + 1 }, { peso_liquido_kg: '25000' }, { local_id: 'bad' }, { placa: '../x' }, { booking_id: 'bad' }, { extra: true }]) assert.equal(normalize({ ...input(), ...patch }).ok, false);
  for (const patch of [{ tara_kg: 50000 }, { peso_liquido_kg: 1 }]) { const result = normalize({ ...input(), ...patch }); assert.equal(!result.ok && result.error.code, 'INVALID_NET_WEIGHT'); }
  const max = normalize({ ...input(), peso_bruto_kg: Number.MAX_SAFE_INTEGER, tara_kg: 0, peso_liquido_kg: Number.MAX_SAFE_INTEGER }); assert.ok(max.ok);
});
test('optional null/absent and UTC representations normalize stably', () => {
  const a = input(), absent = normalize(a), nullable = normalize({ ...a, booking_id: null, captured_at: null, motorista_nome: null, motorista_documento: null, observacoes: null });
  assert.deepEqual(absent, nullable);
  assert.deepEqual(normalize({ ...a, captured_at: '2026-11-15T12:30:00Z' }), normalize({ ...a, captured_at: '2026-11-15T12:30:00.000Z' }));
  assert.equal(normalize({ ...a, captured_at: '2026-02-30T00:00:00Z' }).ok, false);
});
test('10 identical sync calls produce one normalized record and final hash, conflicts preserve original', async () => {
  const repository = new MemoryWeighings(), storage = new MemoryPhotos(), service = new Weighings(repository, storage), a = input();
  const results = await Promise.all(Array.from({ length: 10 }, () => service.sync(a, photo)));
  assert.equal(results.filter(r => r.ok && r.value.created).length, 1); assert.equal(repository.rows.size, 1); assert.equal(storage.final.size, 1);
  const replay = await service.sync({ ...a, captured_at: null }, photo); assert.ok(replay.ok && !replay.value.created);
  const conflict = await service.sync({ ...a, placa: 'XYZ1234' }, photo); assert.equal(!conflict.ok && conflict.error.code, 'WEIGHING_ID_CONFLICT');
  const changedPhoto = await service.sync(a, { ...photo, file_sha256: 'b'.repeat(64) }); assert.equal(!changedPhoto.ok && changedPhoto.error.code, 'WEIGHING_ID_CONFLICT');
  assert.equal(repository.rows.get(a.local_id)!.placa, a.placa);
});
test('storage failure/invalid domain never appends; filters are bounded without Docker', async () => {
  const repository = new MemoryWeighings(), storage = new MemoryPhotos(), service = new Weighings(repository, storage), a = input();
  storage.unavailable = true; assert.equal((await service.sync(a, photo)).ok, false); assert.equal(repository.rows.size, 0);
  storage.unavailable = false; assert.equal((await service.sync({ ...a, unknown: 1 }, photo)).ok, false); assert.equal(storage.final.size, 0);
  await service.sync(a, photo); const found = await service.list({ placa: a.placa, limit: '1' }); assert.ok(found.ok && found.value.items.length === 1);
  for (const q of [{ limit: '101' }, { cursor: '-1' }, { booking_id: 'bad' }, { placa: 'bad' }, { extra: '1' }]) assert.equal((await service.list(q)).ok, false);
});
