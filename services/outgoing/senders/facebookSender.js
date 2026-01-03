// services/outgoing/senders/facebookSender.js
import { ax } from '../../http/ax.js';
import { initDB, dbPool } from '../../../engine/services/db.js';

const FALLBACK_API_VERSION = process.env.FB_API_VERSION || 'v21.0';

/* ==================== Helpers ==================== */

function mapAttachmentType(t) {
  const v = String(t || 'file').toLowerCase();
  if (['image', 'audio', 'video', 'file'].includes(v)) return v;
  return 'file';
}

function normRecipient(raw) {
  const s = String(raw || '');
  const at = s.indexOf('@');
  if (at > -1 && /@(w|t|f|i)\.msgcli\.net$/i.test(s)) return s.slice(0, at);
  return s;
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

function buildQuickRepliesFromInteractive(c) {
  const text = c?.body?.text || 'Selecione uma opção:';
  const rows = c?.action?.sections?.[0]?.rows || [];
  const quick_replies = rows.slice(0, 13).map(r => ({
    content_type: 'text',
    title: (r.title || r.id || '').slice(0, 20),
    payload: String(r.id || r.title || '').slice(0, 100),
  }));
  return { text, quick_replies };
}

/**
 * Lê token e api_version do flow_channels.settings
 * Fallback:
 *  - token: FB_PAGE_ACCESS_TOKEN
 *  - api_version: FALLBACK_API_VERSION (FB_API_VERSION ou 'v21.0')
 */
async function resolveFacebookConfig({ tenant_id, page_id, connection_id }) {
  const q = `
    SELECT
      COALESCE(
        settings->>'page_access_token',
        settings->>'access_token',
        settings->>'token'
      ) AS page_access_token,
      COALESCE(
        settings->>'api_version',
        settings->>'fb_api_version'
      ) AS api_version
      FROM flow_channels
     WHERE channel_type = 'facebook'
       AND provider     = 'meta'
       AND is_active    = true
       AND ($1::uuid IS NULL OR tenant_id = $1)
       AND ($2::uuid IS NULL OR id = $2)              -- se você mandar flow_channel_id como connection_id
       AND ($3::text IS NULL OR account_id = $3 OR external_id = $3)
     ORDER BY updated_at DESC
     LIMIT 1;
  `;
  const params = [tenant_id || null, connection_id || null, page_id || null];

  try {
    const { rows } = await dbPool.query(q, params);
    const row = rows?.[0] || {};

    let token =
      row.page_access_token?.trim() ||
      process.env.FB_PAGE_ACCESS_TOKEN?.trim() ||
      null;

    let apiVersion =
      row.api_version?.trim() ||
      FALLBACK_API_VERSION;

    return { token, apiVersion };
  } catch (e) {
    console.error('[facebookSender] config lookup fail:', e?.message || e);
    return {
      token: process.env.FB_PAGE_ACCESS_TOKEN?.trim() || null,
      apiVersion: FALLBACK_API_VERSION,
    };
  }
}

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
    // pass-through se o adapter já montou message/text
    if (content?.message) {
      payloads.push({ messaging_type: 'RESPONSE', recipient: { id: to }, message: content.message });
      return payloads;
    }
    if (content?.text) {
      payloads.push({ messaging_type: 'RESPONSE', recipient: { id: to }, message: { text: content.text } });
      return payloads;
    }
    const c = coerceContent('text', content);
    if (!c?.text) throw new Error('[FB] text.body/text obrigatório');
    payloads.push({ messaging_type: 'RESPONSE', recipient: { id: to }, message: { text: c.text } });
    return payloads;
  }

  if (t === 'interactive') {
    // pass-through se já vier no formato final do adapter (quick_replies ou button template)
    if (content?.message) {
      payloads.push({ messaging_type: 'RESPONSE', recipient: { id: to }, message: content.message });
      return payloads;
    }
    if (content?.text && content?.quick_replies) {
      payloads.push({ messaging_type: 'RESPONSE', recipient: { id: to }, message: { text: content.text, quick_replies: content.quick_replies } });
      return payloads;
    }
    // fallback: construir quick_replies a partir do esquema unificado
    const qr = buildQuickRepliesFromInteractive(content);
    payloads.push({ messaging_type: 'RESPONSE', recipient: { id: to }, message: qr });
    return payloads;
  }

  const items = Array.isArray(attachments) && attachments.length
    ? attachments
    : content ? [content] : [];

  if (!items.length) throw new Error('[FB] Nenhum attachment/text para enviar');

  for (const a of items) {
    const c = coerceContent('attachment', a);
    if (!c?.url) throw new Error('[FB] attachment.url/link obrigatório');
    const at = mapAttachmentType(c.type);
    payloads.push({
      messaging_type: 'RESPONSE',
      recipient: { id: to },
      message: { attachment: { type: at, payload: { url: c.url, is_reusable: false } } },
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

/* ===================== SENDER ===================== */
export async function sendViaFacebook(job) {
  await initDB();

  const {
    tenant_id, to: toRaw, page_id, connection_id, type = 'text', content, attachments, tempId,
  } = job || {};

  const to = normRecipient(toRaw).replace(/\D/g, ''); // PSID só dígitos
  if (!to) return { ok: false, retry: false, reason: '[FB] destinatário ausente (to)' };

  // 1) token + api_version do banco (com fallback env)
  const { token: cfgToken, apiVersion } = await resolveFacebookConfig({ tenant_id, page_id, connection_id });

  let token = cfgToken;
  if (!tenant_id) {
    console.warn('[facebookSender] tenant_id ausente no job — usando fallback de token (env ou conexão por page_id).');
  }

  if (!token && process.env.FB_PAGE_ACCESS_TOKEN) {
    token = process.env.FB_PAGE_ACCESS_TOKEN.trim();
  }

  if (!token) {
    const reason = '[FB] access_token não encontrado (tenant/page/env)';
    console.error('[facebookSender] TOKEN MISSING ->', { tenant_id, page_id, connection_id });
    await markFailedDB(tempId, reason);
    return { ok: false, retry: false, reason };
  }

  // 2) payload(s)
  let payloads;
  try {
    payloads = buildPayloads({ to, type: String(type).toLowerCase(), content, attachments });
  } catch (e) {
    const reason = e?.message || 'payload inválido';
    console.error('[facebookSender] BUILD PAYLOAD FAIL ->', reason);
    await markFailedDB(tempId, reason);
    return { ok: false, retry: false, reason };
  }

  // 3) endpoint (agora usando apiVersion vinda do banco, com fallback)
  const url = page_id
    ? `https://graph.facebook.com/${apiVersion}/${encodeURIComponent(page_id)}/messages`
    : `https://graph.facebook.com/${apiVersion}/me/messages`;

  // 4) envio
  try {
    let last;
    for (const body of payloads) {
      const kind = body?.message?.attachment?.type || (type === 'interactive' ? 'quick_replies' : 'text');
      console.log('[facebookSender] POST ->', { url, to, kind });
      const res = await postWithRetry(url, body, token);
      last = res?.data;
      console.log('[facebookSender] OK <-', last);
    }

    const providerId = last?.message_id || last?.id || null;

    try {
      if (tempId) {
        await dbPool.query(
          `UPDATE messages
             SET message_id = COALESCE($1, message_id),
                 updated_at = NOW()
           WHERE message_id = $2`,
          [providerId, tempId]
        );
      }
    } catch {}

    return { ok: true, providerId };
  } catch (err) {
    const status = err?.response?.status;
    const resp   = err?.response?.data;
    const reason =
      resp?.error?.message ||
      err?.message ||
      'erro ao enviar no Facebook';
    const code   = resp?.error?.code;
    const sub    = resp?.error?.error_subcode;

    console.error('[facebookSender] FAIL <-', { status, code, subcode: sub, reason, url, page_id, to });

    await markFailedDB(tempId, reason);
    const retry = (status >= 500) || status === 429 || code === 4 || code === 613;
    return { ok: false, retry, reason };
  }
}

async function markFailedDB(tempId, reason) {
  if (!tempId) return;
  try {
    await dbPool.query(
      `UPDATE messages
         SET status = CASE
                        WHEN status IN ('pending','queued','sending') THEN 'failed'
                        ELSE status
                      END,
             metadata = COALESCE(metadata,'{}'::jsonb) || jsonb_build_object('error', to_jsonb($1::text)),
             updated_at=NOW()
       WHERE message_id=$2`,
      [String(reason), tempId]
    );
  } catch {}
}
