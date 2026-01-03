// services/outgoing/senders/telegramSender.js
import FormData from 'form-data';
import { spawn } from 'child_process';
import { ax } from '../../http/ax.js';
import { initDB, dbPool } from '../../../engine/services/db.js';
import { emitUpdateMessage } from '../../realtime/emitToRoom.js';

const TG_TRANSMUX_WEBM =
  String(process.env.TG_TRANSMUX_WEBM ?? 'true').toLowerCase() === 'true';

// ---------- helpers básicos ----------
const isWebm = (u = '') => /\.webm(\?|#|$)/i.test(String(u));
const isOgg  = (u = '') => /\.ogg(\?|#|$)/i.test(String(u));

function tgFatal(desc = '') {
  const d = String(desc).toLowerCase();
  return (
    d.includes('bot was blocked by the user') ||
    d.includes('user is deactivated') ||
    d.includes('chat not found') ||
    d.includes('bad request: chat not found') ||
    d.includes('message text is empty') ||
    d.includes('wrong http url specified') ||
    d.includes('message is too long') ||
    d.includes('replied message not found')
  );
}

// ======================= Resolver de token via DB =======================

async function resolveTelegramToken({ tenant_id, flow_channel_id }) {
  const q = `
    SELECT settings->>'bot_token' AS bot_token
      FROM flow_channels
     WHERE channel_type = 'telegram'
       AND is_active    = true
       AND ($1::uuid IS NULL OR tenant_id = $1)
       AND ($2::uuid IS NULL OR id = $2)
     ORDER BY updated_at DESC
     LIMIT 1;
  `;
  const params = [tenant_id || null, flow_channel_id || null];
  const { rows } = await dbPool.query(q, params);

  const token =
    rows?.[0]?.bot_token?.trim()
    || process.env.TELEGRAM_TOKEN?.trim()
    || null;

  if (!token) throw new Error('[TG] bot_token não encontrado em flow_channels/ENV');
  return token;
}

async function tgCall(method, payload, token) {
  const base = `https://api.telegram.org/bot${token}`;
  const { data } = await ax.post(`${base}/${method}`, payload, { timeout: 15000 });
  if (data?.ok) return data;
  const err = new Error(data?.description || `${method} falhou`);
  err._tg = data;
  throw err;
}

// ffmpeg transmux: WEBM (opus) -> OGG (opus), sem re-encode
function spawnFfmpegWebmToOgg() {
  return spawn('ffmpeg', ['-i', 'pipe:0', '-vn', '-c:a', 'copy', '-f', 'ogg', 'pipe:1'], {
    stdio: ['pipe', 'pipe', 'inherit'],
  });
}

// Envia VOICE via multipart stream (rápido, sem escrever em disco)
async function sendVoiceMultipartFromUrl({ chat_id, url, caption, reply_to_message_id, token }) {
  const resp = await ax.get(url, { responseType: 'stream', timeout: 20000 });

  let stream = resp.data;
  let filename = 'voice.ogg';

  const contentType = String(resp.headers['content-type'] || '');
  const isWebmStream = isWebm(url) || /webm/i.test(contentType);

  if (isWebmStream) {
    if (!TG_TRANSMUX_WEBM) throw new Error('[TG] WEBM detectado e TG_TRANSMUX_WEBM=false');
    const ff = spawnFfmpegWebmToOgg();
    stream.pipe(ff.stdin);
    stream = ff.stdout;
  } else if (!isOgg(url) && !/ogg|opus/i.test(contentType)) {
    // Telegram voice exige ogg/opus
    throw new Error('[TG] Formato não suportado para voice (precisa ser ogg/opus)');
  }

  const form = new FormData();
  form.append('chat_id', String(chat_id));
  form.append('voice', stream, filename);
  if (caption) form.append('caption', caption);
  if (reply_to_message_id) form.append('reply_to_message_id', String(reply_to_message_id));

  const base = `https://api.telegram.org/bot${token}`;
  const { data } = await ax.post(`${base}/sendVoice`, form, {
    headers: form.getHeaders(),
    maxBodyLength: Infinity,
    maxContentLength: Infinity,
    timeout: 30000,
  });

  if (data?.ok) return data;
  const err = new Error(data?.description || 'sendVoice falhou');
  err._tg = data;
  throw err;
}

// --------- componentes / interativos ----------
function coerceInteractiveText(c = {}) {
  return (
    c.text ||
    c?.body?.text ||
    c.caption ||
    'Escolha uma opção'
  );
}

function buildInlineKeyboardFromButtons(buttons = []) {
  const rows = buttons
    .filter(b => b && (b.title || b.id || b.payload))
    .map(b => [{
      text: String(b.title || b.id || 'Opção').slice(0, 64),
      callback_data: String(b.id || b.payload || b.title || 'opt').slice(0, 64),
    }]);
  return rows.length ? { inline_keyboard: rows } : undefined;
}

function buildReplyKeyboardFromList(list = {}) {
  const sections = Array.isArray(list.sections) ? list.sections : [];
  const keyboard = sections
    .map(sec => (sec?.rows || []).map(row => ({ text: String(row?.title || '').slice(0, 64) })))
    .filter(row => row.length > 0);

  if (!keyboard.length) return undefined;
  return { keyboard, one_time_keyboard: true, resize_keyboard: true };
}

function normalizeInteractive(content = {}) {
  if (content?.reply_markup) {
    return {
      text: coerceInteractiveText(content),
      reply_markup: content.reply_markup
    };
  }

  const c = content?.interactive && typeof content.interactive === 'object'
    ? content.interactive
    : content;

  if (Array.isArray(c?.buttons)) {
    return {
      text: coerceInteractiveText(c),
      reply_markup: buildInlineKeyboardFromButtons(c.buttons)
    };
  }

  if (c?.type === 'button' && Array.isArray(c?.action?.buttons)) {
    const buttons = c.action.buttons.map(b => ({
      id: b?.reply?.id || b?.reply?.title,
      title: b?.reply?.title || 'Opção'
    }));
    return {
      text: coerceInteractiveText(c),
      reply_markup: buildInlineKeyboardFromButtons(buttons)
    };
  }

  if (c?.list?.sections) {
    return {
      text: coerceInteractiveText(c),
      reply_markup: buildReplyKeyboardFromList(c.list)
    };
  }

  if (c?.type === 'list' && Array.isArray(c?.action?.sections)) {
    const list = { sections: c.action.sections };
    return {
      text: coerceInteractiveText(c),
      reply_markup: buildReplyKeyboardFromList(list)
    };
  }

  return { text: coerceInteractiveText(c), reply_markup: undefined };
}

// ============= SENDER PRINCIPAL =============

export async function sendViaTelegram(job) {
  await initDB();

  const {
    tempId,
    to,
    type,
    content,
    context,
  } = job || {};

  const tenant_id       = job?.tenant_id || null;
  const flow_channel_id = job?.flow_channel_id || null;

  const token = await resolveTelegramToken({ tenant_id, flow_channel_id });

  try {
    let data;

    switch (type) {
      case 'text': {
        const text = content?.body || content?.text;
        if (!text) throw new Error('Telegram: text.body/text obrigatório');
        data = await tgCall('sendMessage', {
          chat_id: to,
          text,
          ...(content?.reply_markup ? { reply_markup: content.reply_markup } : {}),
          ...(context?.message_id ? { reply_to_message_id: context.message_id } : {})
        }, token);
        break;
      }

      case 'interactive': {
        const norm = normalizeInteractive(content || {});
        data = await tgCall('sendMessage', {
          chat_id: to,
          text: norm.text || 'Escolha uma opção',
          ...(norm.reply_markup ? { reply_markup: norm.reply_markup } : {}),
          ...(context?.message_id ? { reply_to_message_id: context.message_id } : {})
        }, token);
        break;
      }

      case 'image': {
        const photo = content?.url || content?.link;
        if (!photo) throw new Error('Telegram: image.url obrigatório');
        data = await tgCall('sendPhoto', {
          chat_id: to,
          photo,
          ...(content?.caption ? { caption: content.caption } : {}),
          ...(context?.message_id ? { reply_to_message_id: context.message_id } : {})
        }, token);
        break;
      }

      case 'audio': {
        const link = content?.url || content?.link;
        if (!link) throw new Error('Telegram: audio.url obrigatório');

        if (content?.voice === true || isWebm(link)) {
          try {
            data = await sendVoiceMultipartFromUrl({
              chat_id: to,
              url: link,
              caption: content?.caption,
              reply_to_message_id: context?.message_id,
              token,
            });
          } catch (_) {
            data = await tgCall('sendDocument', {
              chat_id: to,
              document: link,
              ...(content?.caption ? { caption: content.caption } : {}),
              ...(context?.message_id ? { reply_to_message_id: context.message_id } : {})
            }, token);
          }
        } else {
          data = await tgCall('sendAudio', {
            chat_id: to,
            audio: link,
            ...(content?.caption ? { caption: content.caption } : {}),
            ...(context?.message_id ? { reply_to_message_id: context.message_id } : {})
          }, token);
        }
        break;
      }

      case 'video': {
        const link = content?.url || content?.link;
        if (!link) throw new Error('Telegram: video.url obrigatório');
        data = await tgCall('sendVideo', {
          chat_id: to,
          video: link,
          ...(content?.caption ? { caption: content.caption } : {}),
          ...(context?.message_id ? { reply_to_message_id: context.message_id } : {})
        }, token);
        break;
      }

      case 'document': {
        const link = content?.url || content?.link;
        if (!link) throw new Error('Telegram: document.url obrigatório');
        data = await tgCall('sendDocument', {
          chat_id: to,
          document: link,
          ...(content?.caption ? { caption: content.caption } : {}),
          ...(context?.message_id ? { reply_to_message_id: context.message_id } : {})
        }, token);
        break;
      }

      case 'location': {
        const lat = Number(content?.latitude), lng = Number(content?.longitude);
        if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
          throw new Error('Telegram: latitude/longitude obrigatórios');
        }
        data = await tgCall('sendLocation', {
          chat_id: to,
          latitude: lat,
          longitude: lng,
          ...(context?.message_id ? { reply_to_message_id: context.message_id } : {})
        }, token);
        break;
      }

      default:
        return { ok: false, retry: false, reason: `Tipo não suportado no Telegram: ${type}` };
    }

    const platformId =
      data?.result?.message_id ||
      (Array.isArray(data?.result) ? data.result[0]?.message_id : null) ||
      null;

    await dbPool.query(
      `UPDATE messages
         SET status='sent',
             message_id=COALESCE($1, message_id),
             updated_at=NOW()
       WHERE message_id=$2`,
      [platformId, tempId]
    );

    await emitUpdateMessage({
      user_id: to,
      channel: 'telegram',
      message_id: tempId || platformId || null,
      provider_id: platformId || undefined,
      status: 'sent'
    });

    return { ok: true, platformId };
  } catch (e) {
    const tg = e?._tg;
    const desc = tg?.description || e?.message || '';

    try {
      await dbPool.query(
        `UPDATE messages
           SET status='error',
               metadata = jsonb_set(coalesce(metadata,'{}'::jsonb), '{error}', to_jsonb($1)),
               updated_at=NOW()
         WHERE message_id=$2`,
        [tg || e?.message, tempId]
      );
    } catch {}

    await emitUpdateMessage({
      user_id: to,
      channel: 'telegram',
      message_id: tempId || null,
      status: 'failed',
      reason: String(desc || 'send_failed')
    });

    if (tgFatal(desc)) {
      return { ok: false, retry: false, reason: desc };
    }
    return { ok: false, retry: true, reason: desc };
  }
}
