import { fail, object, ok, utc, uuid, type Result } from './event.js';
export const FILE_CEILING = 100 * 1024 * 1024;
export const plate = (v: unknown): v is string => typeof v === 'string' && /^[A-Z]{3}[0-9][A-Z0-9][0-9]{2}$/.test(v);
export type Weighing = { local_id: string; placa: string; peso_bruto_kg: number; tara_kg: number; peso_liquido_kg: number; booking_id: string | null; captured_at: string | null; motorista_nome: string | null; motorista_documento: string | null; observacoes: string | null };
export type Photo = { file_sha256: string; file_size_bytes: number; mime_type: 'image/jpeg' | 'image/png' };
export type RecordWeighing = Weighing & { sequence: string; received_at: string; romaneio: Photo };
export function normalize(input: unknown): Result<Weighing> {
  if (!object(input) || Object.keys(input).some(k => !['local_id', 'placa', 'peso_bruto_kg', 'tara_kg', 'peso_liquido_kg', 'booking_id', 'captured_at', 'motorista_nome', 'motorista_documento', 'observacoes'].includes(k))) return fail('INVALID_PAYLOAD');
  if (!uuid(input.local_id) || !plate(input.placa) || ![input.peso_bruto_kg, input.tara_kg, input.peso_liquido_kg].every(n => Number.isSafeInteger(n) && Number(n) >= 0) || Number(input.peso_bruto_kg) === 0) return fail('INVALID_PAYLOAD');
  if (Number(input.peso_bruto_kg) < Number(input.tara_kg) || input.peso_liquido_kg !== Number(input.peso_bruto_kg) - Number(input.tara_kg)) return fail('INVALID_NET_WEIGHT');
  if (input.booking_id != null && !uuid(input.booking_id) || input.captured_at != null && !utc(input.captured_at)) return fail('INVALID_PAYLOAD');
  for (const k of ['motorista_nome', 'motorista_documento', 'observacoes']) {
    const v = input[k];
    if (v != null && (typeof v !== 'string' || v.length > (k === 'observacoes' ? 500 : 100) || /\u0000|[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/.test(v))) return fail('INVALID_PAYLOAD');
  }
  return ok({ local_id: input.local_id, placa: input.placa, peso_bruto_kg: Number(input.peso_bruto_kg), tara_kg: Number(input.tara_kg), peso_liquido_kg: Number(input.peso_bruto_kg) - Number(input.tara_kg), booking_id: (input.booking_id ?? null) as string | null, captured_at: input.captured_at == null ? null : new Date(input.captured_at as string).toISOString(), motorista_nome: (input.motorista_nome ?? null) as string | null, motorista_documento: (input.motorista_documento ?? null) as string | null, observacoes: (input.observacoes ?? null) as string | null });
}
export function validPhoto(p: Photo) { return /^[a-f0-9]{64}$/.test(p.file_sha256) && Number.isSafeInteger(p.file_size_bytes) && p.file_size_bytes > 0 && p.file_size_bytes <= FILE_CEILING && ['image/jpeg', 'image/png'].includes(p.mime_type); }
