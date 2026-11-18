import Fastify from 'fastify';
import { randomUUID } from 'node:crypto';
import type { Audit } from '../application/ledger.js';
import { fail, uuid, type Result } from '../domain/event.js';
import { weighingsRoutes, type UploadOptions } from './weighings-http.js';
export function http(audit: Audit, uploads?: UploadOptions) {
  const app = Fastify({ logger: false, bodyLimit: 65536 }), correlations = new WeakMap<object, string>();
  function send(reply: any, result: Result<unknown>, cid: string) {
    return result.ok ? reply.send(result.value) : reply.code(result.error.status).send({ error: { code: result.error.code, message: result.error.code, details: null, correlation_id: cid, timestamp: new Date().toISOString() } });
  }
  app.addHook('onRequest', async (req, reply) => {
    const header = req.headers['x-correlation-id'], cid = uuid(header) ? header : randomUUID(); correlations.set(req, cid); reply.header('X-Correlation-Id', cid);
    if (header !== undefined && !uuid(header)) return send(reply, fail('INVALID_CORRELATION_ID'), cid);
  });
  app.setErrorHandler((error, req, reply) => { const status = (error as { statusCode?: number }).statusCode; send(reply, fail(status && status >= 400 && status < 500 ? 'INVALID_REQUEST' : 'INTERNAL_SERVER_ERROR', status && status >= 400 && status < 500 ? status : 500), correlations.get(req)!); });
  app.setNotFoundHandler((req, reply) => send(reply, fail('NOT_FOUND', 404), correlations.get(req)!));
  app.get('/v1/audit/events', async (req, reply) => send(reply, await audit.list(req.query as Record<string, unknown>), correlations.get(req)!));
  if (uploads) weighingsRoutes(app, uploads);
  return app;
}
