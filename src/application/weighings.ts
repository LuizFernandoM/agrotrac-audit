import { fail, ok, uuid, type Result } from '../domain/event.js';
import { normalize, plate, validPhoto, type Photo, type RecordWeighing, type Weighing } from '../domain/weighing.js';
export type StagedPhoto = Photo & { token: string };
export type WeighingFilters = { local_id?: string; booking_id?: string; placa?: string; cursor: string; limit: number };
export type WeighingPage = { items: RecordWeighing[]; next_cursor: string | null };
export interface WeighingRepository {
  append(input: Weighing, photo: Photo): Promise<Result<{ created: boolean; record: RecordWeighing }>>;
  get(id: string): Promise<RecordWeighing | undefined>;
  list(filters: WeighingFilters): Promise<WeighingPage>;
}
export interface PhotoStorage {
  promote(stage: StagedPhoto): Promise<Result<void>>;
  read(photo: Photo): Promise<Result<Uint8Array>>;
}
export class Weighings {
  constructor(private repository: WeighingRepository, private storage: PhotoStorage) {}
  async sync(input: unknown, staged: StagedPhoto) {
    const value = normalize(input); if (!value.ok) return value;
    if (!validPhoto(staged)) return fail('INVALID_FILE_SIGNATURE', 415);
    const stored = await this.storage.promote(staged); if (!stored.ok) return stored;
    return this.repository.append(value.value, { file_sha256: staged.file_sha256, file_size_bytes: staged.file_size_bytes, mime_type: staged.mime_type });
  }
  async list(query: Record<string, unknown>): Promise<Result<WeighingPage>> {
    if (Object.keys(query).some(k => !['local_id', 'booking_id', 'placa', 'cursor', 'limit'].includes(k))) return fail('INVALID_QUERY');
    for (const k of ['local_id', 'booking_id']) if (query[k] !== undefined && !uuid(query[k])) return fail('INVALID_QUERY');
    if (query.placa !== undefined && !plate(query.placa)) return fail('INVALID_QUERY');
    const cursor = query.cursor ?? '0', limit = query.limit ?? '50';
    if (typeof cursor !== 'string' || !/^(0|[1-9]\d{0,18})$/.test(cursor) || BigInt(cursor) > 9223372036854775807n || typeof limit !== 'string' || !/^[1-9]\d{0,2}$/.test(limit) || Number(limit) > 100) return fail('INVALID_QUERY');
    return ok(await this.repository.list({ local_id: query.local_id as string | undefined, booking_id: query.booking_id as string | undefined, placa: query.placa as string | undefined, cursor, limit: Number(limit) }));
  }
  async download(id: string) {
    if (!uuid(id)) return fail('INVALID_PAYLOAD');
    const record = await this.repository.get(id); if (!record) return fail('WEIGHING_NOT_FOUND', 404);
    const photo = await this.storage.read(record.romaneio); if (!photo.ok) return photo;
    return ok({ record, bytes: photo.value });
  }
}
