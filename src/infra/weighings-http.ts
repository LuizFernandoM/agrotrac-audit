import multipart from '@fastify/multipart';
import type { FastifyInstance } from 'fastify';
import { fail, type Result } from '../domain/event.js';
import type { Weighings } from '../application/weighings.js';
import type { DiskPhotos } from './storage.js';
import type { RecordWeighing } from '../domain/weighing.js';
export type UploadOptions = { service: Weighings; storage: DiskPhotos; maxJsonBytes: number };
function strictJson(text: string) {
  const value: unknown = JSON.parse(text), tokens = text.match(/"(?:\\.|[^"\\])*"|[{}\[\]:,]|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?|true|false|null/g)!;
  let at = 0;
  function walk(depth: number) {
    if (depth > 32) throw new Error('Depth');
    const token = tokens[at++];
    if (token === '{') {
      const keys = new Set<string>();
      while (tokens[at] !== '}') { const key = JSON.parse(tokens[at++]); if (keys.has(key)) throw new Error('Duplicate'); keys.add(key); at++; walk(depth + 1); if (tokens[at] === ',') at++; }
      at++;
    } else if (token === '[') { while (tokens[at] !== ']') { walk(depth + 1); if (tokens[at] === ',') at++; } at++; }
  }
  walk(0); return value;
}
function view(record: RecordWeighing) { return { ...record, romaneio: { ...record.romaneio, url: `/v1/audit/weighings/${record.local_id}/romaneio` } }; }
export function weighingsRoutes(app: FastifyInstance, options: UploadOptions) {
  const { service, storage, maxJsonBytes } = options;
  app.register(multipart, { limits: { fileSize: storage.maxBytes + 1, fieldSize: maxJsonBytes + 1, files: 2, fields: 2, parts: 4 } });
  const send = (reply: any, result: Result<unknown>, status = 200) => result.ok ? reply.code(status).send(result.value) : reply.code(result.error.status).send({ error: { code: result.error.code, message: result.error.code, details: null, correlation_id: reply.getHeader('X-Correlation-Id'), timestamp: new Date().toISOString() } });
  app.post('/v1/audit/weighings/sync', async (req, reply) => {
    reply.header('Cache-Control', 'private, no-store').header('X-Content-Type-Options', 'nosniff');
    if (!req.isMultipart()) return send(reply, fail('INVALID_MULTIPART_REQUEST'));
    let json: string | undefined, staged, invalid: Result<never> | undefined, files = 0, fields = 0;
    try {
      for await (const part of req.parts()) {
        if (part.type === 'file') {
          files++;
          if (part.fieldname !== 'romaneio' || files !== 1) { part.file.resume(); invalid = fail('UNEXPECTED_FILE_COUNT'); continue; }
          const result = await storage.stage(part.file, part.mimetype);
          if (!result.ok) { invalid ??= result; part.file.resume(); continue; }
          staged = result.value;
        } else {
          fields++;
          if (part.fieldname !== 'weighing' || fields !== 1) { invalid = fail('UNEXPECTED_FIELD'); continue; }
          if (part.valueTruncated || typeof part.value !== 'string' || Buffer.byteLength(part.value) > maxJsonBytes) invalid = fail('JSON_PAYLOAD_TOO_LARGE');
          else json = part.value;
        }
      }
    } catch (e) {
      if (invalid) return send(reply, invalid);
      const code = (e as { code?: string }).code;
      return send(reply, fail(code === 'FST_REQ_FILE_TOO_LARGE' ? 'FILE_TOO_LARGE' : code === 'FST_FILES_LIMIT' ? 'UNEXPECTED_FILE_COUNT' : ['FST_FIELDS_LIMIT', 'FST_PARTS_LIMIT'].includes(code ?? '') ? 'UNEXPECTED_FIELD' : 'INVALID_MULTIPART_REQUEST', code === 'FST_REQ_FILE_TOO_LARGE' ? 413 : 400));
    }
    if (req.raw.aborted || !req.raw.complete) return send(reply, fail('INVALID_MULTIPART_REQUEST'));
    if (invalid) return send(reply, invalid);
    if (!staged || files !== 1) return send(reply, fail('UNEXPECTED_FILE_COUNT'));
    if (json === undefined || fields !== 1) return send(reply, fail('UNEXPECTED_FIELD'));
    let input;
    try { input = strictJson(json); } catch { return send(reply, fail('INVALID_JSON_PAYLOAD')); }
    const result = await service.sync(input, staged);
    return result.ok ? send(reply, { ok: true, value: view(result.value.record) }, result.value.created ? 201 : 200) : send(reply, result);
  });
  app.get('/v1/audit/weighings', async (req, reply) => {
    reply.header('Cache-Control', 'private, no-store').header('X-Content-Type-Options', 'nosniff');
    const result = await service.list(req.query as Record<string, unknown>);
    return result.ok ? send(reply, { ok: true, value: { ...result.value, items: result.value.items.map(view) } }) : send(reply, result);
  });
  app.get<{ Params: { local_id: string } }>('/v1/audit/weighings/:local_id/romaneio', async (req, reply) => {
    reply.header('Cache-Control', 'private, no-store').header('X-Content-Type-Options', 'nosniff');
    const result = await service.download(req.params.local_id); if (!result.ok) return send(reply, result);
    const { record, bytes } = result.value;
    return reply.type(record.romaneio.mime_type).header('Content-Length', bytes.length).header('Content-Disposition', `inline; filename="romaneio-${record.local_id}.${record.romaneio.mime_type === 'image/png' ? 'png' : 'jpg'}"`).header('ETag', `"${record.romaneio.file_sha256}"`).send(Buffer.from(bytes));
  });
}
