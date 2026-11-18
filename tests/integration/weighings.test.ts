import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createHash, randomUUID, randomBytes } from 'node:crypto';
import { resolve, join } from 'node:path';
import { mkdir, readdir, readFile, writeFile, lstat, symlink } from 'node:fs/promises';
import { request as nodeRequest } from 'node:http';
import { fork } from 'node:child_process';
import sharp from 'sharp';
import { configuration } from '../../src/infra/config.js';
import { migrate } from '../../src/infra/migration.js';
import { Audit } from '../../src/application/ledger.js';
import { PgLedger } from '../../src/infra/postgres.js';
import { PgWeighings } from '../../src/infra/pg-weighings.js';
import { DiskPhotos } from '../../src/infra/storage.js';
import { Weighings } from '../../src/application/weighings.js';
import { http } from '../../src/infra/http.js';
import type { Event } from '../../src/domain/event.js';
import { bounded } from '../../src/infra/amqp.js';
type Part = { name: string; value?: string; bytes?: Buffer; mime?: string; filename?: string };
function body(parts: Part[]) {
  const boundary = `audit_${randomUUID()}`, chunks: Buffer[] = [];
  for (const p of parts) {
    chunks.push(Buffer.from(`--${boundary}\r\nContent-Disposition: form-data; name="${p.name}"${p.bytes ? `; filename="${p.filename ?? 'photo'}"` : ''}\r\n${p.bytes ? `Content-Type: ${p.mime ?? 'image/jpeg'}\r\n` : ''}\r\n`));
    chunks.push(p.bytes ?? Buffer.from(p.value ?? '')); chunks.push(Buffer.from('\r\n'));
  }
  chunks.push(Buffer.from(`--${boundary}--\r\n`)); return { bytes: Buffer.concat(chunks), type: `multipart/form-data; boundary=${boundary}` };
}
const weight = () => ({ local_id: randomUUID(), placa: 'ABC1D23', peso_bruto_kg: 40000, tara_kg: 15000, peso_liquido_kg: 25000 });
const digest = (b: Buffer) => createHash('sha256').update(b).digest('hex');
async function exists(path: string) { return lstat(path).then(() => true, () => false); }
async function finals(root: string): Promise<string[]> {
  const all: string[] = [];
  for (const dir of await readdir(root, { withFileTypes: true })) if (/^[a-f0-9]{2}$/.test(dir.name) && dir.isDirectory()) for (const file of await readdir(join(root, dir.name))) all.push(join(root, dir.name, file));
  return all;
}
test('weighings multipart PostgreSQL + immutable filesystem, additive migration and previous ledger', { timeout: 120000 }, async t => {
  const suffix = `${Date.now()}_${randomUUID().replaceAll('-', '').slice(0, 8)}`, schema = `audit_weight_it_${suffix}`;
  process.env.AUDIT_DB_SCHEMA = schema;
  const config = configuration(), { pool } = config, s = `"${schema}"`;
  const root = resolve('tests/fixtures/generated', `weighings_${suffix}`), storage = new DiskPhotos(root);
  const jpeg = await sharp(randomBytes(640 * 480 * 3), { raw: { width: 640, height: 480, channels: 3 } }).jpeg({ quality: 92 }).toBuffer();
  const png = await sharp(randomBytes(256 * 192 * 3), { raw: { width: 256, height: 192, channels: 3 } }).png().toBuffer();
  const rejectedPng = await sharp({ create: { width: 16, height: 16, channels: 3, background: '#334477' } }).png().toBuffer();
  let sqlFailure = false, blockedId: string | undefined, entered: (() => void) | undefined, release: (() => void) | undefined, lostId: string | undefined;
  const repository = new PgWeighings(pool, schema, { beforeCommit: async input => {
    if (sqlFailure) throw new Error('INJECTED_SQL_FAILURE');
    if (input.local_id === blockedId) { entered?.(); await new Promise<void>(r => { release = r; }); }
  } });
  const service = new Weighings(repository, storage), audit = new Audit(new PgLedger(pool, schema));
  function build(disk = storage, jsonMax = 65536) {
    const app = http(audit, { service: disk === storage ? service : new Weighings(repository, disk), storage: disk, maxJsonBytes: jsonMax });
    app.addHook('onSend', async (req, reply, payload) => {
      if (lostId && req.url === '/v1/audit/weighings/sync' && typeof payload === 'string' && payload.includes(lostId)) { lostId = undefined; reply.raw.destroy(); }
      return payload;
    });
    return app;
  }
  let app = build(), url = '';
  const count = async (id?: string) => Number((await pool.query(`SELECT count(*) n FROM ${s}.weighings${id ? ' WHERE local_id=$1' : ''}`, id ? [id] : [])).rows[0].n);
  async function post(parts: Part[], target = url) {
    const form = body(parts), response = await fetch(`${target}/v1/audit/weighings/sync`, { method: 'POST', headers: { 'content-type': form.type }, body: form.bytes, signal: AbortSignal.timeout(20000) });
    const data = await response.json() as any; assert.ok(response.headers.get('x-correlation-id'));
    assert.equal(JSON.stringify(data).includes(root), false); return { status: response.status, data };
  }
  function parts(input: unknown, bytes = jpeg, mime = 'image/jpeg', reverse = false, filename = 'camera.jpg') {
    const a: Part[] = [{ name: 'weighing', value: JSON.stringify(input) }, { name: 'romaneio', bytes, mime, filename }]; return reverse ? a.reverse() : a;
  }
  async function get(query = '') { const r = await fetch(`${url}/v1/audit/weighings${query}`, { signal: AbortSignal.timeout(10000) }); return { status: r.status, data: await r.json() as any }; }
  const finalPath = (bytes: Buffer, base = root, ext = 'png') => join(base, digest(bytes).slice(0, 2), `${digest(bytes)}.${ext}`);
  async function childServer() {
    const child = fork(resolve('tests/support/weighings-server.ts'), [], { execArgv: ['--import', 'tsx'], env: { ...process.env, AUDIT_DB_SCHEMA: schema, AUDIT_STORAGE_PATH: root }, stdio: ['ignore', 'ignore', 'ignore', 'ipc'] });
    const exited = new Promise<number | null>(r => child.once('exit', r));
    const started = new Promise<string>((r, reject) => { child.once('message', (m: any) => m?.url ? r(m.url) : reject(new Error('CHILD_START_FAILED'))); child.once('error', () => reject(new Error('CHILD_START_FAILED'))); child.once('exit', () => reject(new Error('CHILD_EXITED'))); });
    try { return { url: await bounded(started, 8000), stop: async () => { child.send('stop'); try { assert.equal(await bounded(exited, 5000), 0); } catch (e) { child.kill(); throw e; } } }; }
    catch (e) { child.kill(); await exited; throw e; }
  }
  console.log(`WEIGHINGS_SCHEMA=${schema}; STORAGE_ROOT=${root}`);
  try {
    assert.equal((await pool.query('SELECT current_database() name')).rows[0].name, 'agrotrac_audit');
    assert.equal((await pool.query('SELECT table_name FROM information_schema.tables WHERE table_schema=$1', [schema])).rows.length, 0);
    await migrate(pool, schema, true, 1);
    const event: Event = { event_id: randomUUID(), correlation_id: randomUUID(), type: 'agrotrac.core.booking.created', version: '1.0.0', producer: 'agrotrac-core', occurred_at: new Date().toISOString(), payload: { migration_fixture: true } };
    assert.ok((await audit.ingest(event)).ok);
    await t.test('v1 with persisted ledger event -> v2 and reapply preserve envelope/sequence', async () => {
      const before = (await audit.list({ event_id: event.event_id })); assert.ok(before.ok);
      await migrate(pool, schema); await migrate(pool, schema);
      assert.deepEqual((await pool.query(`SELECT version FROM ${s}.audit_migrations ORDER BY version`)).rows.map(r => r.version), [1, 2]);
      assert.deepEqual(await audit.list({ event_id: event.event_id }), before);
      const badSchema = `audit_bad_history_${suffix}`; await migrate(pool, badSchema, true, 1);
      await pool.query(`INSERT INTO "${badSchema}".audit_migrations(version) VALUES(99)`);
      await assert.rejects(migrate(pool, badSchema), /Unrecognized/);
      assert.equal((await pool.query('SELECT table_name FROM information_schema.tables WHERE table_schema=$1 AND table_name=$2', [badSchema, 'weighings'])).rows.length, 0);
      const gap = `audit_gap_history_${suffix}`;
      await pool.query(`CREATE SCHEMA "${gap}"; CREATE TABLE "${gap}".audit_migrations(version integer PRIMARY KEY); INSERT INTO "${gap}".audit_migrations(version) VALUES(2)`);
      await assert.rejects(migrate(pool, gap), /Unrecognized/);
      assert.equal((await pool.query('SELECT table_name FROM information_schema.tables WHERE table_schema=$1', [gap])).rows.length, 1);
      console.log(`MIGRATION=[1]->[1,2], reapply preserved event ${event.event_id}; rejected_history_schema=${badSchema}`);
    });
    await storage.init(); url = await app.listen({ host: '127.0.0.1', port: 0 });
    const a = weight();
    await t.test('10 concurrent multipart retries => 1x201,9x200,1 row,1 final JPEG', async () => {
      const results = await Promise.all(Array.from({ length: 10 }, (_, i) => post(parts(a, jpeg, 'image/jpeg', i % 2 === 0, '../../../../etc/passwd'))));
      assert.equal(results.filter(r => r.status === 201).length, 1); assert.equal(results.filter(r => r.status === 200).length, 9);
      assert.equal(await count(a.local_id), 1); assert.equal((await finals(root)).length, 1);
      assert.equal(results[0].data.captured_at, null); assert.equal(results[0].data.romaneio.file_sha256, digest(jpeg));
      assert.equal((await readFile(finalPath(jpeg, root, 'jpg'))).equals(jpeg), true);
      console.log('CONCURRENT_SYNC=1x201,9x200; SQL_rows=1; final_files=1');
    });
    await t.test('normalized optionals, UTC and lost response after commit + server restart replay', async () => {
      assert.equal((await post(parts({ ...a, captured_at: null, booking_id: null, motorista_nome: null, motorista_documento: null, observacoes: null }))).status, 200);
      const b = { ...weight(), captured_at: '2026-11-15T10:00:00Z' }; assert.equal((await post(parts(b))).status, 201);
      assert.equal((await post(parts({ ...b, captured_at: '2026-11-15T10:00:00.000Z' }))).status, 200);
      const lost = weight(); lostId = lost.local_id;
      await assert.rejects(post(parts(lost))); assert.equal(await count(lost.local_id), 1);
      await app.close(); app = build(); url = await app.listen({ host: '127.0.0.1', port: 0 });
      assert.equal((await post(parts(lost))).status, 200); assert.equal(await count(lost.local_id), 1);
      for (let i = 0; i < 2; i++) { const child = await childServer(); try { assert.equal((await post(parts(lost), child.url)).status, 200); assert.equal(await count(lost.local_id), 1); } finally { await child.stop(); } }
      console.log(`LOST_RESPONSE_RESTART=${lost.local_id}; persisted=1; replay=200`);
    });
    await t.test('payload/photo conflict preserve original; real PNG and secure download headers/hash', async () => {
      assert.equal((await post(parts({ ...a, placa: 'XYZ1234' }))).status, 409);
      assert.equal((await post(parts(a, png, 'image/png'))).status, 409);
      assert.equal((await get(`?local_id=${a.local_id}`)).data.items[0].placa, a.placa);
      const b = weight(); assert.equal((await post(parts(b, png, 'image/png', true))).status, 201);
      for (const [id, bytes, mime] of [[a.local_id, jpeg, 'image/jpeg'], [b.local_id, png, 'image/png']] as const) {
        const r = await fetch(`${url}/v1/audit/weighings/${id}/romaneio`); assert.equal(r.status, 200); assert.equal(r.headers.get('cache-control'), 'private, no-store'); assert.equal(r.headers.get('x-content-type-options'), 'nosniff'); assert.equal(r.headers.get('content-type'), mime); assert.equal(Number(r.headers.get('content-length')), bytes.length); assert.equal(r.headers.get('etag'), `"${digest(bytes)}"`); assert.equal(digest(Buffer.from(await r.arrayBuffer())), digest(bytes));
      }
      assert.equal((await fetch(`${url}/v1/audit/weighings/bad/romaneio`)).status, 400);
      assert.equal((await fetch(`${url}/v1/audit/weighings/${randomUUID()}/romaneio`)).status, 404);
      assert.ok([400, 404].includes((await fetch(`${url}/v1/audit/weighings/..%2F..%2Fetc%2Fpasswd/romaneio`)).status));
    });
    await t.test('SQL failure rolls back row, preserves ready file orphan, retry succeeds; storage failure no SQL', async () => {
      const b = weight(), photo = await sharp({ create: { width: 32, height: 32, channels: 3, background: 'green' } }).png().toBuffer();
      sqlFailure = true; const failed = await post(parts(b, photo, 'image/png')); sqlFailure = false;
      assert.equal(failed.status, 500); assert.equal(await count(b.local_id), 0); assert.equal(await exists(finalPath(photo)), true);
      assert.equal((await post(parts(b, photo, 'image/png'))).status, 201); assert.equal(await count(b.local_id), 1);
      const blocked = resolve(root, 'not-a-directory'); await writeFile(blocked, 'fixture blocks directory creation');
      const badApp = build(new DiskPhotos(blocked)); const badUrl = await badApp.listen({ host: '127.0.0.1', port: 0 });
      const c = weight(); try { const failure = await post(parts(c), badUrl); assert.equal(failure.status, 500); assert.equal(failure.data.error.code, 'STORAGE_IO_ERROR'); assert.equal(await count(c.local_id), 0); } finally { await badApp.close(); }
    });
    await t.test('bad weights, unknown/duplicate fields, full multipart validation in either order', async () => {
      const before = await count();
      for (const patch of [{ peso_bruto_kg: 0 }, { tara_kg: -1 }, { tara_kg: 1.5 }, { peso_bruto_kg: Number.MAX_SAFE_INTEGER + 1 }, { peso_liquido_kg: 1 }, { tara_kg: 50000 }, { captured_at: '2026-02-30T00:00:00Z' }, { booking_id: 'bad' }, { unknown: true }]) assert.equal((await post(parts({ ...weight(), ...patch }, rejectedPng, 'image/png', true))).status, 400);
      for (const list of [
        [...parts(weight(), rejectedPng, 'image/png', true), { name: 'extra', value: 'bad' }],
        [...parts(weight(), rejectedPng, 'image/png'), { name: 'weighing', value: '{}' }],
        [...parts(weight(), rejectedPng, 'image/png'), { name: 'romaneio', bytes: rejectedPng, mime: 'image/png' }],
        [{ name: 'weighing', value: '{}' }], [{ name: 'romaneio', bytes: rejectedPng, mime: 'image/png' }],
        [{ name: 'weighing', value: '{"local_id":"a","local_id":"b"}' }, { name: 'romaneio', bytes: rejectedPng, mime: 'image/png' }],
      ]) assert.equal((await post(list)).status, 400);
      assert.equal(await count(), before); assert.equal(await exists(finalPath(rejectedPng)), false);
      const huge = [{ name: 'weighing', value: 'x'.repeat(65537) }, { name: 'romaneio', bytes: rejectedPng, mime: 'image/png' }];
      assert.equal((await post(huge)).data.error.code, 'JSON_PAYLOAD_TOO_LARGE');
      const max = { ...weight(), peso_bruto_kg: Number.MAX_SAFE_INTEGER, tara_kg: 0, peso_liquido_kg: Number.MAX_SAFE_INTEGER }; assert.equal((await post(parts(max))).status, 201);
      assert.equal((await get(`?local_id=${max.local_id}`)).data.items[0].peso_bruto_kg, Number.MAX_SAFE_INTEGER);
      assert.equal(await pool.query(`UPDATE ${s}.weighings SET tara_kg=-1 WHERE local_id=$1`, [max.local_id]).then(() => '', e => e.code), '23514');
      assert.equal(await pool.query(`UPDATE ${s}.weighings SET file_size_bytes=104857601 WHERE local_id=$1`, [max.local_id]).then(() => '', e => e.code), '23514');
      console.log('INVALID_MULTIPART=unknown/duplicate/missing parts and invalid weights rejected before promotion/SQL');
    });
    await t.test('full image decoder rejects JPEG/PNG truncation, fake headers, CRC corruption, MIME mismatch/pixel limits', async () => {
      const corrupted = Buffer.from(rejectedPng); corrupted[45] ^= 1;
      const before = await count();
      for (const [bytes, mime] of [[jpeg.subarray(0, jpeg.length - 10), 'image/jpeg'], [png.subarray(0, png.length - 10), 'image/png'], [Buffer.from([255, 216, 255, 0, 255, 217]), 'image/jpeg'], [Buffer.concat([jpeg, jpeg]), 'image/jpeg'], [corrupted, 'image/png'], [png, 'image/jpeg'], [Buffer.from('not an image'), 'image/jpeg']] as const) {
        const r = await post(parts(weight(), bytes, mime)); assert.equal(r.status, 415); assert.equal(r.data.error.code, 'INVALID_FILE_SIGNATURE');
      }
      const tinyApp = build(new DiskPhotos(resolve(root, 'pixel-limited'), 10485760, 1)); const tinyUrl = await tinyApp.listen({ host: '127.0.0.1', port: 0 });
      try { assert.equal((await post(parts(weight()), tinyUrl)).status, 415); } finally { await tinyApp.close(); }
      assert.equal(await count(), before);
      console.log(`REAL_IMAGES=JPEG ${jpeg.length} bytes,PNG ${png.length} bytes; 8 integrity/MIME/pixel failures rejected`);
    });
    await t.test('file size boundary and interrupted/malformed multipart cannot confirm or promote', async () => {
      const limitRoot = resolve(root, 'byte-limited'), limitApp = build(new DiskPhotos(limitRoot, jpeg.length - 1)); const limitUrl = await limitApp.listen({ host: '127.0.0.1', port: 0 });
      const b = weight(); try { assert.equal((await post(parts(b), limitUrl)).status, 413); assert.equal(await count(b.local_id), 0); } finally { await limitApp.close(); }
      const exactApp = build(new DiskPhotos(resolve(root, 'exact-byte-limit'), jpeg.length)); const exactUrl = await exactApp.listen({ host: '127.0.0.1', port: 0 });
      try { assert.equal((await post(parts(weight()), exactUrl)).status, 201); } finally { await exactApp.close(); }
      const partial = weight(), form = body(parts(partial, rejectedPng, 'image/png', true));
      const malformed = await fetch(`${url}/v1/audit/weighings/sync`, { method: 'POST', headers: { 'content-type': form.type }, body: form.bytes.subarray(0, form.bytes.length - 15), signal: AbortSignal.timeout(5000) }); assert.equal(malformed.status, 400, JSON.stringify(await malformed.json()));
      await new Promise<void>(resolve => {
        const request = nodeRequest(`${url}/v1/audit/weighings/sync`, { method: 'POST', headers: { 'content-type': form.type, 'content-length': form.bytes.length } });
        request.on('error', () => resolve()); request.write(form.bytes.subarray(0, form.bytes.length - 20)); setTimeout(() => request.destroy(), 100);
      });
      await new Promise(r => setTimeout(r, 100)); assert.equal(await count(partial.local_id), 0); assert.equal(await exists(finalPath(rejectedPng)), false);
    });
    await t.test('existing corrupt hash destination is never rewritten; junction is rejected', async () => {
      const photo = await sharp({ create: { width: 7, height: 7, channels: 3, background: '#ff7722' } }).png().toBuffer(), corruptRoot = resolve(root, 'corruption');
      const target = finalPath(photo, corruptRoot); await mkdir(resolve(target, '..'), { recursive: true }); await writeFile(target, 'preserved corruption fixture');
      const corruptApp = build(new DiskPhotos(corruptRoot)), corruptUrl = await corruptApp.listen({ host: '127.0.0.1', port: 0 });
      const b = weight(); try { const r = await post(parts(b, photo, 'image/png'), corruptUrl); assert.equal(r.status, 500); assert.equal(r.data.error.code, 'STORAGE_INTEGRITY_ERROR'); assert.equal(await count(b.local_id), 0); assert.equal((await readFile(target)).toString(), 'preserved corruption fixture'); } finally { await corruptApp.close(); }
      const outside = resolve(root, 'outside-controlled-fixture'), linked = resolve(root, 'junction-root'); await mkdir(outside); await symlink(outside, linked, process.platform === 'win32' ? 'junction' : 'dir');
      const linkApp = build(new DiskPhotos(linked)), linkUrl = await linkApp.listen({ host: '127.0.0.1', port: 0 });
      const c = weight(); try { assert.equal((await post(parts(c), linkUrl)).status, 500); assert.equal(await count(c.local_id), 0); assert.equal((await readdir(outside)).length, 0); } finally { await linkApp.close(); }
    });
    await t.test('keyset cursor cannot skip a later commit; filters/empty/invalid requests', async () => {
      const booking = randomUUID(), b = { ...weight(), booking_id: booking }, c = { ...weight(), booking_id: booking };
      blockedId = b.local_id; const started = new Promise<void>(r => { entered = r; });
      const first = post(parts(b)); await bounded(started, 5000);
      let secondDone = false; const second = post(parts(c)).then(r => { secondDone = true; return r; });
      await new Promise(r => setTimeout(r, 100)); assert.equal(secondDone, false); assert.equal(await count(c.local_id), 0);
      release!(); blockedId = undefined; assert.equal((await first).status, 201); assert.equal((await second).status, 201);
      const page = await get(`?booking_id=${booking}&placa=ABC1D23&limit=1`); assert.equal(page.data.items[0].local_id, b.local_id); assert.ok(page.data.next_cursor);
      const next = await get(`?booking_id=${booking}&cursor=${page.data.next_cursor}`); assert.equal(next.data.items[0].local_id, c.local_id); assert.equal(next.data.next_cursor, null);
      assert.equal((await get(`?local_id=${randomUUID()}`)).data.items.length, 0);
      for (const q of ['?limit=101', '?cursor=-1', '?placa=bad', '?booking_id=bad', '?unknown=1']) assert.equal((await get(q)).status, 400);
      console.log('COMMIT_ORDER=second blocked until first commit; cursor pages returned both IDs in sequence');
    });
    console.log(`WEIGHINGS_FINAL_ROWS=${await count()}; FINAL_FILES_MAIN=${(await finals(root)).length}; STAGING_MAIN=${(await readdir(join(root, '.staging'))).length}; directoryFsyncSupported=${storage.directoryFsyncSupported}`);
  } finally { sqlFailure = false; release?.(); await app.close(); await pool.end(); }
});
