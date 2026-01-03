// src/services/uploadToObjectStore.js (R2, com tenant e PDF inline)
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { v4 as uuidv4 } from 'uuid';
import path from 'path';
import dotenv from 'dotenv';
dotenv.config();

/* ----------------------------- helpers ----------------------------- */

function requiredEnv(vars) {
  const missing = vars.filter((k) => !process.env[k] || String(process.env[k]).trim() === '');
  if (missing.length) {
    const msg = `[uploadToObjectStore] Variáveis ausentes: ${missing.join(', ')}.`;
    throw new Error(msg);
  }
}

function sanitizeName(name = '') {
  // conserva letras, números, pontos e hífens/underscores
  return String(name).replace(/[^\w.\-]+/g, '_');
}

function isPdfByName(name = '') {
  return /\.pdf(\?|$)/i.test(String(name));
}

function isPdfMime(mt = '') {
  return String(mt || '').toLowerCase().startsWith('application/pdf');
}

function inferExtFromMime(mt = '') {
  const base = String(mt || '').split(';')[0].trim(); // ex: "audio/ogg; codecs=opus" -> "audio/ogg"
  const [, extRaw] = base.split('/');
  return extRaw ? `.${extRaw.toLowerCase()}` : '';
}

function guessMimeByName(name = '') {
  const lower = String(name).toLowerCase();
  if (lower.endsWith('.pdf')) return 'application/pdf';
  if (/\.(jpe?g|png|gif|webp|bmp|svg)$/.test(lower)) return 'image/' + lower.split('.').pop().replace('jpg', 'jpeg');
  if (/\.(mp3|m4a|wav|ogg|opus)$/.test(lower)) {
    const ext = lower.split('.').pop();
    if (ext === 'mp3') return 'audio/mpeg';
    if (ext === 'm4a') return 'audio/mp4';
    if (ext === 'wav') return 'audio/wav';
    if (ext === 'ogg' || ext === 'opus') return 'audio/ogg';
    return 'audio/' + ext;
  }
  if (/\.(mp4|webm|mov)$/.test(lower)) {
    const ext = lower.split('.').pop();
    if (ext === 'mp4') return 'video/mp4';
    if (ext === 'webm') return 'video/webm';
    if (ext === 'mov') return 'video/quicktime';
    return 'video/' + ext;
  }
  return null;
}

/* ----------------------------- cliente R2 ----------------------------- */

const R2_ACCOUNT_ID = process.env.R2_ACCOUNT_ID;
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID;
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY;
const R2_BUCKET = process.env.R2_BUCKET;
const R2_PUBLIC_BASE = (process.env.R2_PUBLIC_BASE || '').replace(/\/$/, '');

requiredEnv(['R2_ACCOUNT_ID', 'R2_ACCESS_KEY_ID', 'R2_SECRET_ACCESS_KEY', 'R2_BUCKET']);

const s3 = new S3Client({
  region: 'auto',
  endpoint: `https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: R2_ACCESS_KEY_ID,
    secretAccessKey: R2_SECRET_ACCESS_KEY,
  },
});

export async function uploadToObjectStore(buffer, originalname, mimetype, opts = {}) {
  // 1) tenant/prefixo (ordem: opts.prefix > opts.tenant > env > 'uploads')
  const envTenant = process.env.TENANT_ID || process.env.TENANT || '';
  const safeTenant = sanitizeName(opts.tenant || envTenant || '');
  const basePrefix = opts.prefix
    ? String(opts.prefix).replace(/^\/+|\/+$/g, '')
    : (safeTenant ? `${safeTenant}` : 'uploads');

  // 2) filename
  const rawExt = path.extname(originalname || '');
  // se o nome não tem extensão, tenta inferir pelo mimetype, senão mantém
  const guessedExt = !rawExt && mimetype ? inferExtFromMime(mimetype) : '';
  const finalExt = (rawExt || guessedExt || '').slice(0, 10); // defensivo

  const baseClean = sanitizeName(path.basename(originalname || '').replace(rawExt, '')) || 'file';
  const finalFilename = sanitizeName(`${baseClean}${finalExt || ''}`);

  // 3) chave única
  const key = `${basePrefix}/${Date.now()}-${uuidv4()}-${finalFilename}`;

  // 4) MIME
  let mime = String(mimetype || '').trim();
  if (!mime) {
    mime = guessMimeByName(finalFilename) || 'application/octet-stream';
  }
  if (!mime && isPdfByName(finalFilename)) mime = 'application/pdf';

  // 5) inline vs attachment
  const inline =
    /^(image|audio|video)\//.test(mime) ||
    isPdfMime(mime) ||
    (!mimetype && isPdfByName(finalFilename)); // fallback por nome

  const contentDisposition = inline
    ? 'inline'
    : `attachment; filename="${sanitizeName(path.basename(finalFilename))}"`;

  // 6) upload
  await s3.send(
    new PutObjectCommand({
      Bucket: R2_BUCKET,              // ⚠️ garante que existe (validação no topo)
      Key: key,
      Body: buffer,
      ContentType: mime,
      CacheControl: 'public, max-age=300',
      ContentDisposition: contentDisposition,
    })
  );

  // 7) URL pública
  if (!R2_PUBLIC_BASE) {
    // fallback indicativo
    return `r2://${R2_BUCKET}/${key}`;
  }
  return `${R2_PUBLIC_BASE}/${key}`;
}

export default uploadToObjectStore;
