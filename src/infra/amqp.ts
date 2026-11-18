import amqp, { type ChannelModel, type ConfirmChannel } from 'amqplib';
export function bounded<T>(promise: Promise<T>, timeout: number): Promise<T> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('AMQP_TIMEOUT')), timeout);
    promise.then(v => { clearTimeout(timer); resolve(v); }, () => { clearTimeout(timer); reject(new Error('AMQP_UNAVAILABLE')); });
  });
}
export async function closeConnection(connection: ChannelModel, timeout: number) {
  await bounded(connection.close(), timeout).catch(() => {
    const internal = connection as unknown as { connection: { stream: { destroy(error: Error): void } } };
    internal.connection.stream.destroy(new Error('AUDIT_CONNECTION_CLOSED'));
  });
}
export async function connect(url: string, timeout: number) {
  let abandoned = false;
  const pending = amqp.connect(url, { timeout }).then(c => {
    c.on('error', () => {});
    if (abandoned) void closeConnection(c, timeout).catch(() => {});
    return c;
  });
  try { return await bounded(pending, timeout); } catch { abandoned = true; throw new Error('AMQP_CONNECT_FAILED'); }
}
export function pause(ms: number, signal: AbortSignal) {
  return new Promise<void>(resolve => {
    if (signal.aborted) { resolve(); return; }
    const done = () => { clearTimeout(timer); signal.removeEventListener('abort', done); resolve(); };
    const timer = setTimeout(done, ms); signal.addEventListener('abort', done, { once: true });
  });
}
export class ConfirmSender {
  private tail = Promise.resolve();
  constructor(private channel: ConfirmChannel, private timeout: number) {}
  send(exchange: string, key: string, content: Buffer, messageId: string) {
    const task = this.tail.then(async () => {
      let returned = false;
      const onReturn = () => { returned = true; }; this.channel.on('return', onReturn);
      try {
        await bounded(new Promise<void>((resolve, reject) => {
          this.channel.publish(exchange, key, content, { persistent: true, mandatory: true, contentType: 'application/json', messageId }, error => {
            if (error || returned) reject(new Error('QUARANTINE_NOT_CONFIRMED')); else resolve();
          });
        }), this.timeout);
      } finally { this.channel.removeListener('return', onReturn); }
    });
    this.tail = task.catch(() => {}); return task;
  }
}
