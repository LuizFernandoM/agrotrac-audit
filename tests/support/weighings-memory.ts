import type { PhotoStorage, StagedPhoto, WeighingFilters, WeighingRepository } from '../../src/application/weighings.js';
import { canonical, fail, ok } from '../../src/domain/event.js';
import type { Photo, RecordWeighing, Weighing } from '../../src/domain/weighing.js';
export class MemoryWeighings implements WeighingRepository {
  rows = new Map<string, RecordWeighing>();
  async append(input: Weighing, photo: Photo) {
    const existing = this.rows.get(input.local_id);
    if (existing) {
      const { sequence, received_at, romaneio, ...data } = existing;
      return canonical(data) === canonical(input) && canonical(romaneio) === canonical(photo) ? ok({ created: false, record: structuredClone(existing) }) : fail('WEIGHING_ID_CONFLICT', 409);
    }
    const record = { ...input, sequence: String(this.rows.size + 1), received_at: new Date().toISOString(), romaneio: photo };
    this.rows.set(input.local_id, structuredClone(record)); return ok({ created: true, record });
  }
  async get(id: string) { return structuredClone(this.rows.get(id)); }
  async list(filters: WeighingFilters) {
    const all = [...this.rows.values()].filter(r => BigInt(r.sequence) > BigInt(filters.cursor) && (!filters.placa || r.placa === filters.placa) && (!filters.booking_id || r.booking_id === filters.booking_id) && (!filters.local_id || r.local_id === filters.local_id));
    const items = all.slice(0, filters.limit); return { items, next_cursor: all.length > filters.limit ? items.at(-1)!.sequence : null };
  }
}
export class MemoryPhotos implements PhotoStorage {
  final = new Set<string>();
  unavailable = false;
  async promote(photo: StagedPhoto) { if (this.unavailable) return fail('STORAGE_IO_ERROR', 500); this.final.add(photo.file_sha256); return ok(undefined); }
  async read(photo: Photo) { return this.final.has(photo.file_sha256) ? ok(new Uint8Array([1])) : fail('STORAGE_IO_ERROR', 500); }
}
