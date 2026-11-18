import { randomUUID } from 'node:crypto';
import type { Channel, ChannelModel, ConsumeMessage } from 'amqplib';
import type { Audit } from '../application/ledger.js';
import { fail, type Result } from '../domain/event.js';
import { bounded, closeConnection, ConfirmSender, connect, pause } from './amqp.js';
export type Topology = { exchange: string; queue: string; quarantineExchange: string; quarantineQueue: string };
export const canonical: Topology = { exchange: 'agrotrac.events', queue: 'agrotrac.audit.events', quarantineExchange: 'agrotrac.audit.quarantine', quarantineQueue: 'agrotrac.audit.quarantine.events' };
type Disposition = 'inserted' | 'duplicate' | 'quarantined';
export type ConsumerOptions = { prefetch?: number; attempts?: number; retryDelay?: number; reconnectDelay?: number; maxReconnectDelay?: number; timeout?: number;
  hooks?: { beforeAck?(message: ConsumeMessage, disposition: Disposition): Promise<void>; afterAck?(message: ConsumeMessage, disposition: Disposition): void; quarantineConfirmed?(message: ConsumeMessage): Promise<void> } };
export class AuditConsumer {
  ready = false;
  generation = 0;
  readonly stats = { acked: 0, inserted: 0, duplicate: 0, quarantined: 0, sqlRetries: 0, reconnects: 0, lastError: null as string | null };
  private stopController = new AbortController();
  private running?: Promise<void>;
  private timeout: number;
  constructor(private url: string, private audit: Audit, readonly topology = canonical, private options: ConsumerOptions = {}) {
    this.timeout = options.timeout ?? 5000;
    if (![options.prefetch ?? 8, options.attempts ?? 3].every(n => Number.isInteger(n) && n >= 1 && n <= 100)) throw new Error('Invalid consumer limits');
  }
  start() { if (this.running) throw new Error('Consumer already started'); this.running = this.run(); return this.running; }
  async stop() { this.stopController.abort(); await this.running; }
  private async run() {
    let failures = 0;
    while (!this.stopController.signal.aborted) {
      try { await this.session(() => { failures = 0; this.stats.lastError = null; }); } catch { this.stats.lastError = 'CONSUMER_SESSION_FAILED'; }
      if (this.stopController.signal.aborted) break;
      this.stats.reconnects++; failures++;
      await pause(Math.min(this.options.maxReconnectDelay ?? 30000, (this.options.reconnectDelay ?? 1000) * 2 ** Math.min(failures - 1, 8)), this.stopController.signal);
    }
  }
  private async session(onSuccess: () => void) {
    const local = new AbortController(), tasks = new Set<Promise<void>>();
    let finish!: () => void;
    const ended = new Promise<void>(resolve => { finish = resolve; });
    const failed = () => { local.abort(); finish(); };
    const global = this.stopController.signal;
    global.addEventListener('abort', failed, { once: true });
    let connection: ChannelModel | undefined, channel: Channel | undefined, tag: string | undefined;
    try {
      if (global.aborted) return;
      connection = await connect(this.url, this.timeout); connection.on('close', failed); connection.on('error', failed);
      channel = await bounded(connection.createChannel(), this.timeout); channel.on('error', failed); channel.on('close', failed);
      const confirm = await bounded(connection.createConfirmChannel(), this.timeout); confirm.on('error', failed); confirm.on('close', failed);
      const sender = new ConfirmSender(confirm, this.timeout);
      await bounded((async () => {
        await channel!.assertExchange(this.topology.exchange, 'topic', { durable: true, autoDelete: false });
        // No x-arguments on the existing canonical queue: never alter its policy.
        await channel!.assertQueue(this.topology.queue, { durable: true, autoDelete: false });
        await channel!.bindQueue(this.topology.queue, this.topology.exchange, 'agrotrac.core.#');
        await channel!.bindQueue(this.topology.queue, this.topology.exchange, 'agrotrac.live.#');
        await confirm.assertExchange(this.topology.quarantineExchange, 'direct', { durable: true, autoDelete: false });
        await confirm.assertQueue(this.topology.quarantineQueue, { durable: true, autoDelete: false });
        await confirm.bindQueue(this.topology.quarantineQueue, this.topology.quarantineExchange, 'rejected');
        await channel!.prefetch(this.options.prefetch ?? 8);
      })(), this.timeout);
      if (local.signal.aborted) return;
      const active = channel;
      const subscription = await bounded(active.consume(this.topology.queue, message => {
        if (!message) { failed(); return; }
        const task = this.handle(message, active, sender, local.signal).then(() => onSuccess()).catch(() => {
          this.stats.lastError = 'DELIVERY_DEFERRED'; failed();
        });
        tasks.add(task); void task.finally(() => tasks.delete(task));
      }, { noAck: false }), this.timeout);
      tag = subscription.consumerTag; this.ready = true; this.generation++;
      await ended;
    } finally {
      this.ready = false; local.abort(); global.removeEventListener('abort', failed);
      if (channel && tag) await bounded(channel.cancel(tag), this.timeout).catch(() => {});
      // SQL attempts are bounded by driver timeouts; finish them before releasing the session.
      await Promise.allSettled([...tasks]);
      if (connection) await closeConnection(connection, this.timeout);
    }
  }
  private async handle(message: ConsumeMessage, channel: Channel, sender: ConfirmSender, signal: AbortSignal) {
    let input: unknown, outcome: Result<'inserted' | 'duplicate'> | undefined;
    if (message.content.length > 1024 * 1024) outcome = fail('MESSAGE_TOO_LARGE');
    else { try { input = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(message.content)); } catch { outcome = fail('INVALID_JSON'); } }
    if (!outcome && input && typeof input === 'object' && 'type' in input && input.type !== message.fields.routingKey) outcome = fail('ROUTING_KEY_MISMATCH');
    for (let attempt = 1; !outcome && attempt <= (this.options.attempts ?? 3); attempt++) {
      if (signal.aborted) throw new Error('SESSION_ENDED');
      try { outcome = await this.audit.ingest(input); }
      catch {
        if (attempt === (this.options.attempts ?? 3)) throw new Error('SQL_RETRY_EXHAUSTED');
        this.stats.sqlRetries++; await pause(Math.min(5000, (this.options.retryDelay ?? 250) * 2 ** (attempt - 1)), signal);
      }
    }
    if (!outcome || signal.aborted) throw new Error('SESSION_ENDED');
    let disposition: Disposition;
    if (!outcome.ok) {
      const quarantineId = randomUUID();
      const rejection = { quarantine_id: quarantineId, error: { code: outcome.error.code }, quarantined_at: new Date().toISOString(),
        source: { exchange: message.fields.exchange, routing_key: message.fields.routingKey, message_id: typeof message.properties.messageId === 'string' ? message.properties.messageId.slice(0, 255) : null }, raw_base64: message.content.toString('base64') };
      await sender.send(this.topology.quarantineExchange, 'rejected', Buffer.from(JSON.stringify(rejection)), quarantineId);
      await this.options.hooks?.quarantineConfirmed?.(message);
      disposition = 'quarantined';
    } else disposition = outcome.value;
    if (signal.aborted) throw new Error('SESSION_ENDED');
    await this.options.hooks?.beforeAck?.(message, disposition);
    if (signal.aborted) throw new Error('SESSION_ENDED');
    channel.ack(message); this.stats.acked++; this.stats[disposition]++;
    this.options.hooks?.afterAck?.(message, disposition);
  }
}
