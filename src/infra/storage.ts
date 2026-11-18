import { constants } from 'node:fs';
import { open, mkdir, lstat, realpath, link } from 'node:fs/promises';
import { isAbsolute, parse, join, relative, sep } from 'node:path';
import { createHash, randomUUID } from 'node:crypto';
import type { Readable } from 'node:stream';
import { fail, ok, type Result } from '../domain/event.js';
import { FILE_CEILING, validPhoto, type Photo } from '../domain/weighing.js';
import type { PhotoStorage, StagedPhoto } from '../application/weighings.js';
import { validateImage } from './image.js';
export class DiskPhotos implements PhotoStorage {
  directoryFsyncSupported = true;
  constructor(readonly root: string, readonly maxBytes = 10485760, readonly maxPixels = 16777216) { if (!isAbsolute(root) || !Number.isSafeInteger(maxBytes) || maxBytes < 1 || maxBytes > FILE_CEILING || !Number.isSafeInteger(maxPixels) || maxPixels < 1 || maxPixels > 16777216) throw new Error('Invalid storage path/limits'); }
  private async directory(path: string) {
    const base = parse(path).root;
    let current = base;
    for (const segment of path.slice(base.length).split(/[\\/]/).filter(Boolean)) {
      if (segment === '..' || segment === '.') throw new Error('Unsafe directory');
      current = join(current, segment);
      await mkdir(current).catch(e => { if (e.code !== 'EEXIST') throw e; });
      const s = await lstat(current); if (s.isSymbolicLink() || !s.isDirectory()) throw new Error('Unsafe directory');
      const resolved = await realpath(current);
      if ((process.platform === 'win32' ? resolved.toLowerCase() : resolved) !== (process.platform === 'win32' ? current.toLowerCase() : current)) throw new Error('Directory indirection rejected');
    }
  }
  async init() { await this.directory(this.root); await this.directory(join(this.root, '.staging')); }
  private async directorySync(path: string) {
    let handle;
    try { handle = await open(path, 'r'); await handle.sync(); }
    catch (e) { if (!['EPERM', 'EISDIR', 'EINVAL', 'ENOTSUP', 'EBADF'].includes((e as NodeJS.ErrnoException).code ?? '')) throw e; this.directoryFsyncSupported = false; }
    finally { await handle?.close(); }
  }
  private path(photo: Photo) { return join(this.root, photo.file_sha256.slice(0, 2), `${photo.file_sha256}.${photo.mime_type === 'image/png' ? 'png' : 'jpg'}`); }
  async stage(stream: Readable & { truncated?: boolean }, mime: string): Promise<Result<StagedPhoto>> {
    let handle, sourceFailed = false, writeFailed = false;
    const onError = () => { sourceFailed = true; }; stream.on('error', onError);
    try {
      await this.init(); const token = randomUUID(), path = join(this.root, '.staging', token);
      handle = await open(path, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | (constants.O_NOFOLLOW ?? 0), 0o600);
      const hash = createHash('sha256'); let size = 0;
      for await (const part of stream) {
        const bytes = Buffer.from(part); size += bytes.length; if (size > this.maxBytes) return fail('FILE_TOO_LARGE', 413); hash.update(bytes);
        try { await handle.writeFile(bytes); } catch (e) { writeFailed = true; throw e; }
      }
      await handle.sync(); await handle.close(); handle = undefined;
      if (stream.truncated) return fail('FILE_TOO_LARGE', 413);
      await this.directorySync(join(this.root, '.staging'));
      const file = await open(path, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0)); let bytes;
      try { bytes = await file.readFile(); } finally { await file.close(); }
      const verified = await validateImage(bytes, mime, this.maxPixels); if (!verified.ok) return verified;
      return ok({ token, file_sha256: hash.digest('hex'), file_size_bytes: size, mime_type: verified.value });
    } catch {
      // Closing iteration after a disk-write error can itself abort the source; retain the I/O cause.
      const inputFailure = !writeFailed && (sourceFailed || stream.readableAborted);
      return fail(inputFailure ? 'INVALID_MULTIPART_REQUEST' : 'STORAGE_IO_ERROR', inputFailure ? 400 : 500);
    }
    finally { stream.removeListener('error', onError); await handle?.close(); }
  }
  async promote(stage: StagedPhoto): Promise<Result<void>> {
    if (!validPhoto(stage) || !/^[a-f0-9-]{36}$/.test(stage.token)) return fail('STORAGE_IO_ERROR', 500);
    try {
      await this.init(); const target = this.path(stage); await this.directory(join(this.root, stage.file_sha256.slice(0, 2)));
      const source = join(this.root, '.staging', stage.token);
      const s = await lstat(source); if (!s.isFile() || s.isSymbolicLink()) throw new Error('Unsafe stage');
      // Verify stage again before exclusive promotion; stage files are never written after validation.
      const sourceBytes = await this.checked(source, stage); if (!sourceBytes.ok) return sourceBytes;
      await link(source, target).catch(e => { if (e.code !== 'EEXIST') throw e; });
      const result = await this.checked(target, stage); if (!result.ok) return result;
      await this.directorySync(join(this.root, stage.file_sha256.slice(0, 2))); await this.directorySync(this.root);
      return ok(undefined);
    } catch { return fail('STORAGE_IO_ERROR', 500); }
  }
  private async checked(path: string, photo: Photo): Promise<Result<Buffer>> {
    const rel = relative(this.root, path); if (rel.startsWith(`..${sep}`) || rel === '..' || isAbsolute(rel)) return fail('STORAGE_IO_ERROR', 500);
    const info = await lstat(path, { bigint: true }); if (!info.isFile() || info.isSymbolicLink() || info.size !== BigInt(photo.file_size_bytes)) return fail('STORAGE_INTEGRITY_ERROR', 500);
    const file = await open(path, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0));
    try {
      // Windows lstat may report dev=0 while fstat reports its volume ID; inode remains exact.
      const opened = await file.stat({ bigint: true }); if (opened.ino !== info.ino || info.dev !== 0n && opened.dev !== info.dev) return fail('STORAGE_INTEGRITY_ERROR', 500);
      const bytes = await file.readFile(); if (bytes.length !== photo.file_size_bytes || createHash('sha256').update(bytes).digest('hex') !== photo.file_sha256) return fail('STORAGE_INTEGRITY_ERROR', 500);
      return ok(bytes);
    } finally { await file.close(); }
  }
  async read(photo: Photo): Promise<Result<Uint8Array>> {
    if (!validPhoto(photo)) return fail('STORAGE_INTEGRITY_ERROR', 500);
    try { await this.directory(this.root); await this.directory(join(this.root, photo.file_sha256.slice(0, 2))); return await this.checked(this.path(photo), photo); }
    catch { return fail('STORAGE_IO_ERROR', 500); }
  }
}
