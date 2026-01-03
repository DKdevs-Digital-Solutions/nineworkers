// services/high/processFileHeavy.js
import axios from 'axios';
import { uploadToObjectStore } from '../uploadToObjectStore.js';

const API_VERSION = process.env.API_VERSION || 'v22.0';
const WABA_TOKEN  = process.env.WHATSAPP_TOKEN;
const TG_TOKEN    = process.env.TELEGRAM_TOKEN;

const ax = axios.create({
  timeout: 30000,
  maxBodyLength: Infinity,
  maxContentLength: Infinity,
});

/* ----------------------- Helpers comuns ----------------------- */
function ensureBuffer(data) {
  return Buffer.isBuffer(data) ? data : Buffer.from(data);
}

function inferExtFromMime(mime = '') {
  if (!mime) return '';
  const base = mime.split(';')[0].trim();
  const [, extRaw] = base.split('/');
  return extRaw ? `.${extRaw.toLowerCase()}` : '';
}

function safeFilename(prefix, idOrName, mime) {
  const ext = inferExtFromMime(mime);
  const clean = String(idOrName || 'file').replace(/[^\w.\-]+/g, '_');
  if (clean.toLowerCase().includes('.') || !ext) return clean;
  return `${clean}${ext}`;
}

function must(url, ctxMsg) {
  if (!url) throw new Error(`[processFileHeavy] URL ausente após upload (${ctxMsg})`);
  return url;
}

/* ----------------------- WhatsApp ----------------------- */
async function waDownloadMedia(mediaId) {
  if (!WABA_TOKEN) throw new Error('[WABA] WHATSAPP_TOKEN ausente');

  // 1) resolve a URL da mídia
  const meta = await ax.get(
    `https://graph.facebook.com/${API_VERSION}/${mediaId}`,
    { headers: { Authorization: `Bearer ${WABA_TOKEN}` } }
  );
  const mediaUrl = meta?.data?.url;
  if (!mediaUrl) throw new Error('[WABA] media.url ausente');

  // 2) download do arquivo com Bearer
  const res = await ax.get(mediaUrl, {
    responseType: 'arraybuffer',
    headers: { Authorization: `Bearer ${WABA_TOKEN}` },
  });

  const mime = res.headers['content-type'] || 'application/octet-stream';
  const buffer = ensureBuffer(res.data);
  return { buffer, mime };
}

/* ----------------------- Telegram ----------------------- */
async function tgGetFilePath(fileId) {
  if (!TG_TOKEN) throw new Error('[TG] TELEGRAM_TOKEN ausente');
  const { data } = await ax.get(
    `https://api.telegram.org/bot${TG_TOKEN}/getFile?file_id=${encodeURIComponent(fileId)}`
  );
  const filePath = data?.result?.file_path;
  if (!filePath) throw new Error('[TG] file_path ausente');
  return filePath;
}

async function tgDownloadAsBuffer(fileId) {
  const filePath = await tgGetFilePath(fileId);
  const fileUrl  = `https://api.telegram.org/file/bot${TG_TOKEN}/${filePath}`;

  const res = await ax.get(fileUrl, { responseType: 'arraybuffer' });
  const mime = res.headers['content-type'] || '';
  const buffer = ensureBuffer(res.data);
  const filename = filePath.split('/').pop() || `tg-${fileId}${inferExtFromMime(mime) || ''}`;

  return { buffer, mime, filename };
}

/* ----------------------- Função principal ----------------------- */
export async function processMediaIfNeeded(channel, ctx) {
  /* ============== WHATSAPP ============== */
  if (channel === 'whatsapp') {
    const { msg } = ctx;
    const type = msg?.type;

    if (type === 'text') {
      const text = msg.text?.body || '';
      return { content: text, userMessage: text, msgType: 'text' };
    }

    if (type === 'interactive') {
      const choice = msg.interactive?.button_reply?.id
        || msg.interactive?.list_reply?.id
        || '';
      return { content: choice, userMessage: choice, msgType: 'interactive' };
    }

    if (['image', 'video', 'audio', 'document', 'sticker'].includes(type)) {
      try {
        const mediaObj = msg[type] || {};
        const mediaId  = mediaObj.id;
        if (!mediaId) throw new Error(`[WABA] ${type}.id ausente`);

        const { buffer, mime } = await waDownloadMedia(mediaId);

        const captionWA =
          (type === 'image' || type === 'video' || type === 'sticker')
            ? (mediaObj.caption || '')
            : '';

        const fnFromWA   = type === 'document' ? (mediaObj.filename || '') : '';
        const fallbackId = `${type}-${mediaId}`;
        const filename   = safeFilename(type, fnFromWA || fallbackId, mime);

        const url = must(await uploadToObjectStore(buffer, filename, mime), `wa/${type}`);

        if (type === 'audio') {
          return {
            content: JSON.stringify({ url, filename }),
            userMessage: '[áudio recebido]',
            msgType: 'audio',
          };
        }

        if (type === 'document') {
          return {
            content: JSON.stringify({ url, filename }),
            userMessage: '[documento recebido]',
            msgType: 'document',
          };
        }

        if (type === 'image' || type === 'video' || type === 'sticker') {
          return {
            content: JSON.stringify({ url, filename, caption: captionWA }),
            userMessage: `[${type} recebido]`,
            msgType: type,
          };
        }

        return {
          content: JSON.stringify({ url, filename }),
          userMessage: '[mídia recebida]',
          msgType: type,
        };
      } catch (e) {
        console.error('❌ WA mídia erro:', e?.message || e);
        return { content: '[mídia erro]', userMessage: '[mídia erro]', msgType: type || 'media' };
      }
    }

    if (type === 'location') {
      const { latitude, longitude } = msg.location || {};
      const text = `📍 ${latitude}, ${longitude}`;
      return { content: text, userMessage: text, msgType: 'location' };
    }

    return {
      content: `[tipo não tratado: ${type}]`,
      userMessage: `[tipo não tratado: ${type}]`,
      msgType: type || 'unknown',
    };
  }

  /* ============== TELEGRAM ============== */
  if (channel === 'telegram') {
    const { update, message } = ctx;

    if (update?.callback_query?.data) {
      const data = update.callback_query.data;
      return { content: data, userMessage: data, msgType: 'interactive' };
    }

    if (message?.text) {
      return { content: message.text, userMessage: message.text, msgType: 'text' };
    }

    if (message?.photo && message.photo.length) {
      try {
        const f = message.photo[message.photo.length - 1];
        const { buffer, mime, filename } = await tgDownloadAsBuffer(f.file_id);
        const url = must(await uploadToObjectStore(buffer, filename, mime), 'tg/photo');
        return {
          content: JSON.stringify({ url, filename, caption: message.caption || '' }),
          userMessage: '[imagem recebida]',
          msgType: 'image',
        };
      } catch (e) {
        console.error('❌ TG photo erro:', e?.message || e);
        return { content: '[mídia erro]', userMessage: '[mídia erro]', msgType: 'image' };
      }
    }

    if (message?.video?.file_id) {
      try {
        const { buffer, mime, filename } = await tgDownloadAsBuffer(message.video.file_id);
        const url = must(await uploadToObjectStore(buffer, filename, mime), 'tg/video');
        return {
          content: JSON.stringify({ url, filename, caption: message.caption || '' }),
          userMessage: '[vídeo recebido]',
          msgType: 'video',
        };
      } catch (e) {
        console.error('❌ TG video erro:', e?.message || e);
        return { content: '[mídia erro]', userMessage: '[mídia erro]', msgType: 'video' };
      }
    }

    if (message?.document?.file_id) {
      try {
        const { buffer, mime, filename } = await tgDownloadAsBuffer(message.document.file_id);
        const url = must(await uploadToObjectStore(buffer, filename, mime), 'tg/document');
        const fname = message.document.file_name || filename;
        return {
          content: JSON.stringify({ url, filename: fname }),
          userMessage: '[documento recebido]',
          msgType: 'document',
        };
      } catch (e) {
        console.error('❌ TG document erro:', e?.message || e);
        return { content: '[mídia erro]', userMessage: '[mídia erro]', msgType: 'document' };
      }
    }

    if (message?.voice?.file_id) {
      try {
        const { buffer, mime, filename } = await tgDownloadAsBuffer(message.voice.file_id);
        const url = must(await uploadToObjectStore(buffer, filename, mime), 'tg/voice');
        return {
          content: JSON.stringify({ url, filename, voice: true }),
          userMessage: '[voz recebida]',
          msgType: 'audio',
        };
      } catch (e) {
        console.error('❌ TG voice erro:', e?.message || e);
        return { content: '[mídia erro]', userMessage: '[mídia erro]', msgType: 'audio' };
      }
    }

    if (message?.audio?.file_id) {
      try {
        const { buffer, mime, filename } = await tgDownloadAsBuffer(message.audio.file_id);
        const url = must(await uploadToObjectStore(buffer, filename, mime), 'tg/audio');
        return {
          content: JSON.stringify({ url, filename: message.audio.file_name || filename, voice: false }),
          userMessage: '[áudio recebido]',
          msgType: 'audio',
        };
      } catch (e) {
        console.error('❌ TG audio erro:', e?.message || e);
        return { content: '[mídia erro]', userMessage: '[mídia erro]', msgType: 'audio' };
      }
    }

    if (message?.location) {
      const text = `📍 ${message.location.latitude}, ${message.location.longitude}`;
      return { content: text, userMessage: text, msgType: 'location' };
    }

    return { content: '[tipo não tratado]', userMessage: '[tipo não tratado]', msgType: 'unknown' };
  }

  /* ============== DEFAULT ============== */
  return { content: '', userMessage: '', msgType: 'text' };
}
