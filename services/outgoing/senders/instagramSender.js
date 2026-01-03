// services/outgoing/senders/instagramSender.js
import { ax } from '../../http/ax.js';
import { emitUpdateMessage } from '../../realtime/emitToRoom.js';
import { initDB, dbPool } from '../../../engine/services/db.js';

const FALLBACK_API_VERSION = process.env.FB_API_VERSION || 'v21.0';

// ============== helpers ==============
function normRecipient(raw) {
  const s = String(raw || '');
  const at = s.indexOf('@');
  if (at > -1 && /@(w|t|f|i)\.msgcli\.net$/i.test(s)) return s.slice(0, at);
  return s;
}

function mapAttachmentType(t) {
  const v = String(t || 'file').toLowerCase();
  if (['image', 'audio', 'video', 'file'].includes(v)) return v;
  return 'file';
}

function coerceContent(type, content) {
  const c = { ...(content || {}) };
  if (type === 'text') {
    if (typeof content === 'string') return { text: content };
    if (!c.text && c.body) c.text = c.body;
  } else {
    if (!c.url && c.link) c.url = c.link;
  }
  return c;
}

/**
 * Normaliza payloads "WhatsApp-like" de interativos.
 * Aceita:
 *  - { interactive: { type, body, action, buttons? } }
 *  - { type, body, action, buttons? }
 *  - { message: { text, quick_replies } } (pass-through)
 */
function normalizeInteractiveShape(content = {}) {
  if (content?.message) {
    // já está pronto para o Graph (text + quick_replies)
    return { passthrough: true, message: content.message };
  }

  const c = content?.interactive && typeof content.interactive === 'object'
    ? content.interactive
    : content;

  // estrutura esperada para WhatsApp: { type: 'list'|'button', body:{text}, action:{...} }
  // ou formato "genérico": { buttons:[], list:{sections:[]}, text }
  return { passthrough: false, raw: c || {} };
}

/**
 * Cria quick replies a partir de várias formas suportadas
 * - BUTTON (WA): action.buttons[].reply.{id,title}
 * - LIST (WA):   action.sections[].rows[]
 * - Genérico:    buttons[] (com {id,title} ou {payload,title})
 */
function extractQuickRepliesFromInteractive(raw = {}) {
  let titleText =
    raw?.body?.text ||
    raw?.text ||
    raw?.caption ||
    raw?.list?.title ||
    'Selecione uma opção:';

  let rows = [];

  // WhatsApp BUTTON
  if (raw?.type === 'button' && Array.isArray(raw?.action?.buttons)) {
    rows = raw.action.buttons.map(b => {
      const r = b?.reply || {};
      return { id: r.id || r.title, title: r.title || 'Opção' };
    });
  }

  // WhatsApp LIST
  if (rows.length === 0 && raw?.type === 'list' && Array.isArray(raw?.action?.sections)) {
    rows = raw.action.sections.flatMap(s => s?.rows || []);
  }

  // Genérico: buttons[]
  if (rows.length === 0 && Array.isArray(raw?.buttons)) {
    rows = raw.buttons.map(b => ({
      id: b.id || b.payload || b.title,
      title: b.title || 'Opção',
    }));
  }

  // Genérico: list.sections[]
  if (rows.length === 0 && Array.isArray(raw?.list?.sections)) {
    rows = raw.list.sections.flatMap(s => s?.rows || []);
  }

  // Normalização final (limites do IG: máx 13, títulos 20, payload 100)
  const quick_replies = rows.slice(0, 13).map(r => ({
    content_type: 'text',
    title: String(r.title || r.id || 'Opção').slice(0, 20),
    payload: String(r.id || r.title || 'opt').slice(0, 100),
  }));

  return { text: titleText, quick_replies };
}

/**
 * Lê token e api_version do flow_channels.settings
 * Fallback:
 *  - token: FB_PAGE_ACCESS_TOKEN
 *  - api_version: FALLBACK_API_VERSION
 */
async function resolveInstagramConfig({ tenant_id, env, page_id, connection_id }) {
  const q = `
    SELECT
      COALESCE(
        settings->>'page_access_token',
        settings->>'access_token',
        settings->>'token'
      ) AS access_token,
      COALESCE(
        settings->>'api_version',
        settings->>'fb_api_version'
      ) AS api_version
      FROM flow_channels
     WHERE channel_type = 'instagram'
       AND provider     = 'meta'
       AND is_active    = true
       AND ($1::uuid IS NULL OR tenant_id = $1)
       -- se quiser guardar env no settings, pode complementar:
       AND ($2::text IS NULL OR settings->>'env' = $2)
       AND ($3::uuid IS NULL OR id = $3)
       AND ($4::text IS NULL OR account_id = $4 OR external_id = $4)
     ORDER BY updated_at DESC
     LIMIT 1;
  `;
  const params = [tenant_id || null, env || null, connection_id || null, page_id || null];

  try {
    const { rows } = await dbPool.query(q, params);
    const row = rows?.[0] || {};

    let token =
      row.access_token?.trim() ||
      process.env.FB_PAGE_ACCESS_TOKEN?.trim() ||
      null;

    let apiVersion =
      row.api_version?.trim() ||
      FALLBACK_API_VERSION;

    return { token, apiVersion };
  } catch (e) {
    console.error('[instagramSender] config lookup fail:', e?.message || e);
    return {
      token: process.env.FB_PAGE_ACCESS_TOKEN?.trim() || null,
      apiVersion: FALLBACK_API_VERSION,
    };
  }
}

// monta payload(s)
function buildPayloads({ to, type, content, attachments }) {
  const payloads = [];
  const t = String(type || '').toLowerCase();

  if (t === 'error') {
    const c = coerceContent('text', content) || {};
    const text = c.text || 'Desculpe, não entendi. Vamos tentar de novo.';
    payloads.push({ messaging_type: 'RESPONSE', recipient: { id: to }, message: { text } });
    return payloads;
  }

  if (t === 'text') {
    // pass-through se já vier adaptado pelo adapter
    if (content?.message) {
      payloads.push({ messaging_type: 'RESPONSE', recipient: { id: to }, message: content.message });
      return payloads;
    }
    if (content?.text) {
      payloads.push({ messaging_type: 'RESPONSE', recipient: { id: to }, message: { text: content.text } });
      return payloads;
    }
    const c = coerceContent('text', content);
    if (!c?.text) throw new Error('[IG] text.body/text obrigatório');
    payloads.push({ messaging_type: 'RESPONSE', recipient: { id: to }, message: { text: c.text } });
    return payloads;
  }

  if (t === 'interactive') {
    // 1) já pronto (adapter montou message)
    if (content?.message) {
      payloads.push({ messaging_type: 'RESPONSE', recipient: { id: to }, message: content.message });
      return payloads;
    }

    // 2) normaliza formatos WA/genérico
    const norm = normalizeInteractiveShape(content);
    if (norm.passthrough) {
      payloads.push({ messaging_type: 'RESPONSE', recipient: { id: to }, message: norm.message });
      return payloads;
    }

    const { text, quick_replies } = extractQuickRepliesFromInteractive(norm.raw);
    payloads.push({
      messaging_type: 'RESPONSE',
      recipient: { id: to },
      message: quick_replies?.length ? { text, quick_replies } : { text }
    });
    return payloads;
  }

  const items = Array.isArray(attachments) && attachments.length
    ? attachments
    : content ? [content] : [];

  if (!items.length) throw new Error('[IG] Nenhum attachment/text para enviar');

  for (const a of items) {
    const c = coerceContent('attachment', a);
    if (!c?.url) throw new Error('[IG] attachment.url/link obrigatório');
    const tt = mapAttachmentType(c.type);
    payloads.push({
      messaging_type: 'RESPONSE',
      recipient: { id: to },
      message: {
        attachment: {
          type: tt, // image | audio | video | file
          payload: { url: c.url, is_reusable: false },
        },
      },
    });
  }
  return payloads;
}

async function postWithRetry(url, body, token, attempt = 1) {
  try {
    return await ax.post(url, body, {
      headers: { Authorization: `Bearer ${token}` },
      timeout: 20000,
    });
  } catch (err) {
    const status = err?.response?.status;
    const code = err?.response?.data?.error?.code;
    const transient = (status >= 500) || status === 429 || code === 4 || code === 613;
    if (attempt >= 3 || !transient) throw err;
    await new Promise(r => setTimeout(r, 300 * attempt));
    return postWithRetry(url, body, token, attempt + 1);
  }
}

// ===================== SENDER ======================
export async function sendViaInstagram(job) {
  await initDB();

  const {
    tenant_id, env, to: toRaw, page_id, connection_id, type = 'text', content, attachments, tempId,
  } = job || {};

  const to = normRecipient(toRaw);

  if (!tenant_id) return { ok: false, retry: false, reason: '[IG] tenant_id ausente' };
  if (!to)        return { ok: false, retry: false, reason: '[IG] destinatário ausente (to)' };

  // 1) pending
  try {
    await dbPool.query(
      `UPDATE messages
          SET status='pending', updated_at=NOW()
        WHERE message_id=$1
          AND status IS DISTINCT FROM 'failed'`,
      [tempId]
    );
  } catch {}
  try {
    await emitUpdateMessage({
      user_id: to,
      channel: 'instagram',
      message_id: tempId || null,
      status: 'pending',
    });
  } catch {}

  // 2) token + api_version via banco (com fallback env)
  const { token: cfgToken, apiVersion } = await resolveInstagramConfig({ tenant_id, env, page_id, connection_id });

  let token = cfgToken;
  if (!token && process.env.FB_PAGE_ACCESS_TOKEN) {
    token = process.env.FB_PAGE_ACCESS_TOKEN.trim();
  }
  if (!token) {
    const reason = '[IG] access_token não encontrado (db/env)';
    await markFailed({ to, tempId, reason, channel: 'instagram' });
    return { ok: false, retry: false, reason };
  }

  // 3) payload(s)
  let payloads;
  try {
    payloads = buildPayloads({
      to,
      type: String(type).toLowerCase(),
      content,
      attachments,
    });
  } catch (e) {
    const reason = e?.message || 'payload inválido';
    await markFailed({ to, tempId, reason, channel: 'instagram' });
    return { ok: false, retry: false, reason };
  }

  // 4) endpoint (usando apiVersion do banco com fallback)
  const url = page_id
    ? `https://graph.facebook.com/${apiVersion}/${encodeURIComponent(page_id)}/messages`
    : `https://graph.facebook.com/${apiVersion}/me/messages`;

  try {
    let last;
    for (const body of payloads) {
      const res = await postWithRetry(url, body, token);
      last = res?.data;
    }
    const providerId = last?.message_id || last?.id || null;

    try {
      await dbPool.query(
        `UPDATE messages
            SET message_id = COALESCE($1, message_id),
                updated_at = NOW()
          WHERE message_id = $2`,
        [providerId, tempId]
      );
    } catch {}

    return { ok: true, providerId };
  } catch (err) {
    const reason =
      err?.response?.data?.error?.message ||
      err?.message ||
      'erro ao enviar no Instagram';
    const status = err?.response?.status;
    const code   = err?.response?.data?.error?.code;
    const retry = (status >= 500) || status === 429 || code === 4 || code === 613;

    await markFailed({ to, tempId, reason, channel: 'instagram' });
    return { ok: false, retry, reason };
  }
}

async function markFailed({ to, tempId, reason, channel }) {
  try {
    await dbPool.query(
      `UPDATE messages
         SET status = CASE
                        WHEN status IN ('pending','queued','sending') THEN 'failed'
                        ELSE status
                      END,
             metadata = COALESCE(metadata,'{}'::jsonb)
                        || jsonb_build_object('error', to_jsonb($1::text)),
             updated_at=NOW()
       WHERE message_id=$2`,
      [String(reason), tempId]
    );
  } catch {}
  try {
    await emitUpdateMessage({
      user_id: to,
      channel: channel || 'instagram',
      message_id: tempId || null,
      status: 'failed',
      reason: String(reason),
    });
  } catch {}
}
