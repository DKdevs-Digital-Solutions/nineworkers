// services/outgoing/senders/whatsappSender.js
import FormData from 'form-data';
import { ax } from '../../http/ax.js';
import { emitUpdateMessage } from '../../realtime/emitToRoom.js';
import { spawn } from 'child_process';
import { initDB, dbPool } from '../../../engine/services/db.js';

// ======================= ENVs de comportamento =======================
// (token / phone_number_id / api_version agora PODEM vir do banco via flow_channels)
const {
  WABA_UPLOAD_MEDIA = 'true',
  WABA_TRANSMUX_WEBM = 'true',
  API_VERSION: API_VERSION_ENV = 'v22.0',
  WHATSAPP_TOKEN: WHATSAPP_TOKEN_ENV,
  PHONE_NUMBER_ID: PHONE_NUMBER_ID_ENV,
} = process.env;

const wantUpload = () => String(WABA_UPLOAD_MEDIA).toLowerCase() === 'true';
const wantTransmux = () => String(WABA_TRANSMUX_WEBM).toLowerCase() === 'true';

// ===================== Helpers gerais =====================

const normTo = (raw) =>
  String(raw || '')
    .replace(/@w\.msgcli\.net$/i, '')
    .replace(/\D/g, '')
    .replace(/^0+/, '');

const isE164 = (s) => /^\d{7,15}$/.test(s);
const isWebmUrl = (u = '') => /\.webm(\?|#|$)/i.test(String(u || ''));

function coerceContent(type, content) {
  if (typeof content === 'string' && type === 'text') return { body: content };
  const c = { ...(content || {}) };
  if (type === 'text' && !c.body && c.text) c.body = c.text;
  if (['image', 'audio', 'video', 'document'].includes(type) && !c.url && c.link) c.url = c.link;
  return c;
}

// Desaninha payload interativo: aceita { interactive:{...} } e { type, body, action }
function normalizeInteractiveShape(obj = {}) {
  if (obj && typeof obj === 'object' && obj.interactive && typeof obj.interactive === 'object') {
    return obj.interactive;
  }
  return obj;
}

// Timeout helper para não travar o worker
function withTimeout(promise, ms, label) {
  return Promise.race([
    promise,
    new Promise((_, rej) =>
      setTimeout(() => rej(new Error(`[WABA] Timeout em ${label} (${ms}ms)`)), ms)
    ),
  ]);
}

// ======================= Resolver de config via DB =======================
//
// Estrutura que você mostrou em flow_channels:
//  - channel_type = 'whatsapp'
//  - provider = 'meta'
//  - account_id  = WHATSAPP_BUSINESS_ACCOUNT_ID
//  - external_id = PHONE_NUMBER_ID
//  - settings    = {} (pode passar a ter { "token", "phone_number_id", "api_version" } se quiser)
//
// Prioridades:
//  - api_version: settings->>'api_version'  || env.API_VERSION
//  - token:      settings->>'token' ou 'access_token' || env.WHATSAPP_TOKEN
//  - phone_id:   settings->>'phone_number_id' || external_id || env.PHONE_NUMBER_ID
//
async function resolveWhatsAppConfig({ tenant_id, flow_channel_id, phone_number_id, waba_id }) {
  const defaultApiVersion = API_VERSION_ENV || 'v22.0';

  const q = `
    SELECT
      COALESCE(settings->>'api_version', $5)                AS api_version,
      COALESCE(settings->>'token', settings->>'access_token') AS token,
      COALESCE(settings->>'phone_number_id', external_id)   AS phone_number_id
    FROM flow_channels
    WHERE channel_type = 'whatsapp'
      AND provider     = 'meta'
      AND is_active    = true
      AND ($1::uuid IS NULL OR tenant_id = $1)
      AND ($2::uuid IS NULL OR id       = $2)
      AND ($3::text IS NULL OR account_id  = $3)
      AND ($4::text IS NULL OR external_id = $4)
    ORDER BY updated_at DESC
    LIMIT 1;
  `;

  const params = [
    tenant_id || null,
    flow_channel_id || null,
    waba_id || null,
    phone_number_id || null,
    defaultApiVersion,
  ];

  const { rows } = await dbPool.query(q, params);
  const row = rows[0];

  // Se não achou nenhum canal, cai pro ENV legado
  if (!row) {
    if (WHATSAPP_TOKEN_ENV && PHONE_NUMBER_ID_ENV) {
      return {
        apiVersion: defaultApiVersion,
        accessToken: WHATSAPP_TOKEN_ENV,
        phoneNumberId: PHONE_NUMBER_ID_ENV,
      };
    }
    throw new Error('[WABA] Nenhum canal WhatsApp ativo encontrado em flow_channels e ENV sem fallback');
  }

  const accessToken =
    row.token ||
    WHATSAPP_TOKEN_ENV ||
    null;

  const phoneId =
    row.phone_number_id ||
    PHONE_NUMBER_ID_ENV ||
    null;

  if (!accessToken) {
    throw new Error('[WABA] Token não encontrado (nem em settings.token/access_token nem em WHATSAPP_TOKEN)');
  }

  if (!phoneId) {
    throw new Error('[WABA] phone_number_id não encontrado (nem em settings.phone_number_id / external_id nem em PHONE_NUMBER_ID)');
  }

  return {
    apiVersion: row.api_version || defaultApiVersion,
    accessToken,
    phoneNumberId: phoneId,
  };
}

// ================= Upload helpers (usando accessToken por chamada) =================

async function uploadStreamToWaba(stream, filename = 'media', accessToken) {
  const form = new FormData();
  form.append('messaging_product', 'whatsapp');
  form.append('file', stream, filename);

  const uploadUrl = `https://graph.facebook.com/v22.0/media`; // /media aceita sem nos preocuparmos tanto com a versão
  const res = await withTimeout(
    ax.post(uploadUrl, form, {
      headers: { Authorization: `Bearer ${accessToken}`, ...form.getHeaders() },
      timeout: 30000,
      maxContentLength: Infinity,
      maxBodyLength: Infinity,
    }),
    45000,
    'upload /media'
  );
  const mediaId = res?.data?.id;
  if (!mediaId) throw new Error('[WABA] Upload retornou sem id');
  return mediaId;
}

function spawnFfmpegWebmToOgg() {
  const args = ['-i', 'pipe:0', '-vn', '-c:a', 'copy', '-f', 'ogg', 'pipe:1'];
  const ff = spawn('ffmpeg', args, { stdio: ['pipe', 'pipe', 'inherit'] });
  return ff;
}

async function uploadMediaSmart(url, filename, opts = {}) {
  const {
    accessToken,
    transmuxWebm = false,
  } = opts;

  const resp = await withTimeout(
    ax.get(url, { responseType: 'stream', timeout: 20000 }),
    25000,
    'download mídia'
  );

  const contentType = String(resp.headers['content-type'] || '');
  const isWebmStream = isWebmUrl(url) || /webm/i.test(contentType);

  if (transmuxWebm && isWebmStream) {
    try {
      const ff = spawnFfmpegWebmToOgg();
      resp.data.pipe(ff.stdin);
      const oggName = (filename || 'voice').replace(/\.webm$/i, '') + `-${Date.now()}.ogg`;
      console.log('[WABA] 🎚️ Transmux WEBM→OGG e upload por id...');
      return await uploadStreamToWaba(ff.stdout, oggName, accessToken);
    } catch (e) {
      console.warn('[WABA] Falha na transmux WEBM→OGG, fallback para document:', e?.message);
      throw e;
    }
  }

  // Upload “puro” (sem transformação)
  return uploadStreamToWaba(resp.data, filename || 'media', accessToken);
}

// =================== Payload builder ====================

async function buildPayload({ to, type, content, context, template, accessToken }) {
  // ✅ Template
  if (type === 'template') {
    if (!template?.name || !template?.language?.code) {
      throw new Error('[WABA] template.name e template.language.code são obrigatórios');
    }
    const payload = {
      messaging_product: 'whatsapp',
      to,
      type: 'template',
      template: {
        name: template.name,
        language: { code: template.language.code },
        ...(template.components ? { components: template.components } : {})
      }
    };
    if (context?.message_id) payload.context = { message_id: context.message_id };
    return payload;
  }

  // 🎧 Áudio (com suporte a WEBM → OGG)
  if (type === 'audio') {
    const link = content.url;
    if (!link) throw new Error('[WABA] audio.url/link é obrigatório');

    if (isWebmUrl(link)) {
      if (wantTransmux()) {
        try {
          const id = await uploadMediaSmart(link, content.filename, {
            transmuxWebm: true,
            accessToken,
          });
          const audio = { id };
          if (content.voice === true) audio.voice = true;
          if (content.caption) audio.caption = content.caption;
          return {
            messaging_product: 'whatsapp',
            to,
            type: 'audio',
            ...(context?.message_id ? { context: { message_id: context.message_id } } : {}),
            audio,
          };
        } catch {
          console.log('[WABA] ⚠️ Enviando .webm como document (fallback).');
          return {
            messaging_product: 'whatsapp',
            to,
            type: 'document',
            ...(context?.message_id ? { context: { message_id: context.message_id } } : {}),
            document: {
              link,
              filename: content.filename || `audio-${Date.now()}.webm`,
              caption: content.caption,
            },
          };
        }
      } else {
        console.log('[WABA] ⚠️ WEBM detectado e transmux desabilitado → enviando como document.');
        return {
          messaging_product: 'whatsapp',
          to,
          type: 'document',
          ...(context?.message_id ? { context: { message_id: context.message_id } } : {}),
          document: {
            link,
            filename: content.filename || `audio-${Date.now()}.webm`,
            caption: content.caption,
          },
        };
      }
    }

    if (wantUpload()) {
      const id = await uploadMediaSmart(link, content.filename, { accessToken });
      const audio = { id };
      if (content.voice === true) audio.voice = true;
      if (content.caption) audio.caption = content.caption;
      return {
        messaging_product: 'whatsapp',
        to,
        type: 'audio',
        ...(context?.message_id ? { context: { message_id: context.message_id } } : {}),
        audio,
      };
    }

    const audio = { link };
    if (content.voice === true) audio.voice = true;
    if (content.caption) audio.caption = content.caption;
    return {
      messaging_product: 'whatsapp',
      to,
      type: 'audio',
      ...(context?.message_id ? { context: { message_id: context.message_id } } : {}),
      audio,
    };
  }

  const payload = { messaging_product: 'whatsapp', to, type };
  if (context?.message_id) payload.context = { message_id: context.message_id };

  if (['image', 'video', 'document'].includes(type)) {
    if (content.id) {
      payload[type] = { id: content.id };
      if (content.caption) payload[type].caption = content.caption;
      if (type === 'document' && content.filename) payload[type].filename = content.filename;
      return payload;
    }
    const mediaUrl = content.url;
    if (!mediaUrl) throw new Error(`[WABA] ${type}.url/link é obrigatório`);

    if (wantUpload()) {
      const id = await uploadMediaSmart(mediaUrl, content.filename, { accessToken });
      payload[type] = { id };
    } else {
      payload[type] = { link: mediaUrl };
    }
    if (content.caption) payload[type].caption = content.caption;
    if (type === 'document' && content.filename) payload[type].filename = content.filename;
    return payload;
  }

  if (type === 'location') {
    payload.location = {
      latitude: content.latitude,
      longitude: content.longitude,
      ...(content.name ? { name: content.name } : {}),
      ...(content.address ? { address: content.address } : {}),
    };
    return payload;
  }

  if (type === 'text') {
    payload.text = { body: content.body ?? content.text ?? '' };
    return payload;
  }

  if (type === 'interactive') {
    const it = normalizeInteractiveShape(content);
    if (!it || typeof it !== 'object' || !it.type) {
      throw new Error('[WABA] interactive inválido: esperado { type, body, action }');
    }
    payload.interactive = it;
    return payload;
  }

  payload[type] = content;
  return payload;
}

// ===================== Sender principal ======================

export async function sendViaWhatsApp(job) {
  await initDB();

  const toRaw = job.to || job.userId || job.user_id;
  const to = normTo(toRaw);
  const type = String(job.type || 'text').toLowerCase();

  const content = type === 'template'
    ? undefined
    : coerceContent(type, job.content || {});

  const context = job.context || undefined;

  const template =
    job.template
      ?? job.content?.template
      ?? (
        job.content?.templateName && job.content?.languageCode
          ? {
              name: job.content.templateName,
              language: { code: job.content.languageCode },
              components: job.content.components
            }
          : undefined
      );

  if (!isE164(to)) {
    throw new Error(`[WABA] Destinatário inválido: "${toRaw}". Use DDI/DDD apenas dígitos, ex: 5521999998888`);
  }

  // 🔑 NOVO: resolve config a partir de flow_channels
  const tenant_id       = job.tenant_id || null;
  const flow_channel_id = job.flow_channel_id || null;
  const phone_number_id = job.phone_number_id || null; // se vier no job
  const waba_id         = job.waba_id || job.business_account_id || job.whatsapp_business_account_id || null;

  const cfg = await resolveWhatsAppConfig({ tenant_id, flow_channel_id, phone_number_id, waba_id });

  const API_VERSION     = cfg.apiVersion;
  const PHONE_NUMBER_ID = cfg.phoneNumberId;
  const ACCESS_TOKEN    = cfg.accessToken;

  console.log('[WABA] ➜ sending', {
    to,
    type,
    apiVersion: API_VERSION,
    phoneNumberId: PHONE_NUMBER_ID,
    hasToken: !!ACCESS_TOKEN,
    template: type === 'template'
      ? {
          name: template?.name,
          lang: template?.language?.code,
          compCount: Array.isArray(template?.components) ? template.components.length : 0
        }
      : undefined,
    webm: content && isWebmUrl(content.url) || undefined
  });

  // --- OUTGOING: marca PENDING e emite PENDING (idempotente) ---
  try {
    await dbPool.query(
      `UPDATE messages
          SET status='pending',
              updated_at=NOW()
        WHERE message_id=$1
          AND status IS DISTINCT FROM 'failed'`,
      [job.tempId]
    );
  } catch (e) {
    console.warn('[WABA] aviso: falha ao setar pending:', e?.message);
  }
  try {
    await emitUpdateMessage({
      user_id: to,
      channel: 'whatsapp',
      message_id: job.tempId || null,
      status: 'pending',
    });
  } catch {}

  const payload = await withTimeout(
    buildPayload({ to, type, content, context, template, accessToken: ACCESS_TOKEN }),
    45000,
    'buildPayload'
  );

  const url = `https://graph.facebook.com/${API_VERSION}/${PHONE_NUMBER_ID}/messages`;
  try {
    const res = await withTimeout(
      ax.post(url, payload, {
        headers: { Authorization: `Bearer ${ACCESS_TOKEN}`, 'Content-Type': 'application/json' },
        timeout: 20000,
      }),
      30000,
      'POST /messages'
    );

    const providerId = res?.data?.messages?.[0]?.id || null;

    try {
      await dbPool.query(
        `UPDATE messages
            SET message_id = COALESCE($1, message_id),
                updated_at = NOW()
          WHERE message_id = $2`,
        [providerId, job.tempId]
      );
    } catch (e) {
      console.warn('[WABA] aviso: falha ao atualizar providerId:', e?.message);
    }

    return { ok: true, providerId, response: res.data };
  } catch (err) {
    const status = err?.response?.status;
    const data = err?.response?.data;
    const reasonText = String(data?.error?.message || err?.message || 'send error');
    const code   = data?.error?.code;
    const sub    = data?.error?.error_subcode;

    console.error(`❌ [WABA] Falha ao enviar (status ${status ?? 'N/A'}):`, data?.error || err?.message);

    try {
      await dbPool.query(
        `
        UPDATE messages
           SET status = CASE
                          WHEN status IN ('pending','queued','sending') THEN 'failed'
                          ELSE status
                        END,
               metadata = COALESCE(metadata,'{}'::jsonb)
                          || jsonb_build_object(
                               'error',
                               jsonb_build_object(
                                 'message', to_jsonb($1::text),
                                 'code', to_jsonb($2::text),
                                 'subcode', to_jsonb($3::text)
                               )
                             ),
               updated_at=NOW()
         WHERE message_id=$4
        `,
        [reasonText, code != null ? String(code) : null, sub != null ? String(sub) : null, job.tempId]
      );
    } catch (e) {
      console.warn('[WABA] aviso: falha ao atualizar DB para failed:', e?.message);
    }

    try {
      await emitUpdateMessage({
        user_id: to,
        channel: 'whatsapp',
        message_id: job.tempId || null,
        status: 'failed',
        reason: reasonText,
      });
    } catch {}

    const retry =
      (status >= 500) || status === 429 || code === 4 || code === 613;

    return { ok: false, retry, reason: reasonText };
  }
}
