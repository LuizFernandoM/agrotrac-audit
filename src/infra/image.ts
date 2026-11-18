import sharp from 'sharp';
import { fail, ok, type Result } from '../domain/event.js';
function crc32(bytes: Buffer) {
  let n = 0xffffffff;
  for (const byte of bytes) { n ^= byte; for (let i = 0; i < 8; i++) n = (n >>> 1) ^ (n & 1 ? 0xedb88320 : 0); }
  return (n ^ 0xffffffff) >>> 0;
}
function pngComplete(bytes: Buffer) {
  let at = 8, chunks = 0, data = false;
  while (at + 12 <= bytes.length) {
    const length = bytes.readUInt32BE(at), end = at + 12 + length;
    if (end > bytes.length) return false;
    const kind = bytes.toString('ascii', at + 4, at + 8);
    if (['acTL', 'fcTL', 'fdAT'].includes(kind)) return false; // Single-frame photographs only.
    if (!chunks && (kind !== 'IHDR' || length !== 13)) return false;
    if (crc32(bytes.subarray(at + 4, end - 4)) !== bytes.readUInt32BE(end - 4)) return false;
    if (kind === 'IDAT') data = true;
    if (kind === 'IEND') return length === 0 && data && end === bytes.length;
    chunks++; at = end;
  }
  return false;
}
function jpegComplete(bytes: Buffer) {
  let at = 2;
  while (at < bytes.length) {
    if (bytes[at++] !== 255) return false;
    while (bytes[at] === 255) at++;
    const marker = bytes[at++];
    if (marker === 217) return at === bytes.length;
    if (marker === 0 || marker === 216 || marker === undefined || marker >= 208 && marker <= 215) return false;
    if (marker === 1) continue;
    if (at + 2 > bytes.length) return false;
    const length = bytes.readUInt16BE(at); if (length < 2 || at + length > bytes.length) return false;
    at += length;
    if (marker === 218) {
      while (at < bytes.length) {
        if (bytes[at] !== 255) { at++; continue; }
        let next = at + 1; while (bytes[next] === 255) next++;
        if (bytes[next] === 0 || bytes[next] >= 208 && bytes[next] <= 215) { at = next + 1; continue; }
        break;
      }
    }
  }
  return false;
}
export async function validateImage(bytes: Buffer, declared: string, maxPixels: number): Promise<Result<'image/jpeg' | 'image/png'>> {
  const png = bytes.subarray(0, 8).equals(Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]));
  const jpeg = bytes.length > 4 && bytes[0] === 255 && bytes[1] === 216 && bytes[2] === 255 && bytes.at(-2) === 255 && bytes.at(-1) === 217;
  const mime = png ? 'image/png' : jpeg ? 'image/jpeg' : null;
  if (!mime || mime !== declared || png && !pngComplete(bytes) || jpeg && !jpegComplete(bytes)) return fail('INVALID_FILE_SIGNATURE', 415);
  try {
    const image = sharp(bytes, { failOn: 'warning', limitInputPixels: maxPixels, sequentialRead: true });
    const metadata = await image.metadata();
    if (!metadata.width || !metadata.height || metadata.width > 8192 || metadata.height > 8192 || metadata.width * metadata.height > maxPixels || (metadata.pages ?? 1) > 1 || metadata.format !== (png ? 'png' : 'jpeg')) return fail('INVALID_FILE_SIGNATURE', 415);
    await image.raw().toBuffer(); // Force bounded full decoding, not metadata/signature-only validation.
    return ok(mime);
  } catch { return fail('INVALID_FILE_SIGNATURE', 415); }
}
