// services/high/processEvent.js
import crypto from 'crypto';
import { dbPool } from '../../engine/services/db.js';
import { runFlow } from '../../engine/flowExecutor.js';
import { processMediaIfNeeded } from './processFileHeavy.js';
import { emitToRoom } from '../realtime/emitToRoom.js';
import { SYSTEM_EVENT, SYSTEM_EVT_TICKET_STATUS } from '../../engine/messageTypes.js';
import { handleTicketStatusEvent } from './handleTicketStatusEvent.js';
import { loadSession, saveSession } from '../../engine/sessionManager.js';
import { determineNextBlock } from '../../engine/utils.js';

const publishDefault = (room, event, payload) => emitToRoom({ room, event, payload });

function ensureMessageId(channel, rawIdParts) {
  const joined = rawIdParts.filter(Boolean).map(String).join(':');
  return joined || `gen:${channel}:${crypto.randomUUID()}`;
}

/* ===================== messages: INSERT incoming ===================== */
// DEPOIS
async function upsertIncomingMessage({
  channel,
  userId,
  messageId,
  msgType,
  content,
  flowId = null,
  replyTo = null,
  metadata = null,
}) {
  const res = await dbPool.query(
    `
    INSERT INTO messages (
      user_id, message_id, direction, "type", "content",
      "timestamp", flow_id, reply_to, status, metadata,
      created_at, updated_at, channel
    )
    VALUES ($1, $2, 'incoming', $3, $4,
            NOW(), $5, $6, 'received', $7,
            NOW(), NOW(), $8)
    ON CONFLICT (channel, message_id, user_id) DO NOTHING
    RETURNING *
  `,
    [
      userId,
      messageId,
      msgType,
      typeof content === 'string' ? content : JSON.stringify(content),
      flowId,
      replyTo,
      metadata ? JSON.stringify(metadata) : null,
      channel,
    ]
  );
  return res.rows?.[0];
}


function parseForEmit(content) {
  if (content == null) return '';
  if (typeof content === 'string') {
    const s = content.trim();
    if (s.startsWith('{') || s.startsWith('[')) {
      try { return JSON.parse(s); } catch { return content; }
    }
    return content;
  }
  return content;
}

/* ===================== RESOLUÇÃO DO FLOW (via resource_id → TCC.id → flow_channels.channel_key) ===================== */
/**
 * Regra:
 * 1) Usar evt.resource_id (ex.: phone_number_id do WhatsApp) — obrigatório.
 * 2) SELECT id FROM public.tenant_channel_connections WHERE external_id = $1 LIMIT 1
 * 3) SELECT flow_id FROM flow_channels WHERE channel_key = $1 AND is_active = true ORDER BY updated_at DESC LIMIT 1
 * 4) SELECT data FROM flows WHERE id = flow_id
 *
 * - Não filtra por tenant
 * - Não usa channel_type
 * - Sem fallback para flows.active
 */
function extractResourceId(evt) {
  return (
    evt?.resource_id ||
    evt?.meta?.channel_key ||            // compat
    evt?.channel_lookup_external_id ||   // compat (se o webhook não populou resource_id)
    null
  );
}

async function getActiveFlow(evt) {
  const resourceId = extractResourceId(evt);
  if (!resourceId) {
    console.warn('[getActiveFlow] Sem resource_id no evento — não executar bot.');
    return null;
  }

  // 1) pegar o id do registro em tenant_channel_connections a partir do external_id
  const { rows: tccRows } = await dbPool.query(
    `SELECT id FROM public.tenant_channel_connections WHERE external_id = $1 LIMIT 1`,
    [String(resourceId)]
  );
  const tccId = tccRows?.[0]?.id || null;
  if (!tccId) {
    console.warn('[getActiveFlow] external_id=%s não encontrado em tenant_channel_connections.', String(resourceId));
    return null;
  }

  // 2) usar o tcc.id como channel_key em flow_channels
  const { rows: fcRows } = await dbPool.query(
    `
    SELECT flow_id
      FROM flow_channels
     WHERE channel_key = $1
       AND is_active   = true
     ORDER BY updated_at DESC
     LIMIT 1
    `,
    [String(tccId)]
  );
  const flowId = fcRows?.[0]?.flow_id || null;

  if (!flowId) {
    console.warn('[getActiveFlow] Nenhum flow mapeado em flow_channels.channel_key=%s', String(tccId));
    return null;
  }

  // 3) carregar spec do flow
  const { rows: flowRows } = await dbPool.query(
    `SELECT id, data AS spec_json FROM flows WHERE id = $1 LIMIT 1`,
    [flowId]
  );
  const row = flowRows?.[0];
  if (!row) {
    console.warn('[getActiveFlow] flow_id=%s não encontrado em flows.', flowId);
    return null;
  }

  const spec = typeof row.spec_json === 'string' ? JSON.parse(row.spec_json) : row.spec_json;
  console.log('[getActiveFlow] resource_id=%s → tcc.id=%s → flow_id=%s', String(resourceId), String(tccId), String(flowId));
  return { id: row.id, ...spec };
}

/* ---------------- pending_active helpers ---------------- */
function isExpired(ts) {
  if (!ts) return false;
  try { return new Date(ts).getTime() < Date.now(); } catch { return false; }
}

async function clearPendingActive(userId, session) {
  const nextVars = { ...(session?.vars || {}) };
  delete nextVars.pending_active;
  await saveSession(userId, session?.current_block || null, session?.last_flow_id || null, nextVars);
}

async function createTicketSafely(userId, fila, assignedTo) {
  try {
    const r = await dbPool.query(`SELECT create_ticket($1, $2, $3) AS ticket_number`, [
      userId, fila, assignedTo || null
    ]);
    return r.rows?.[0]?.ticket_number || null;
  } catch {
    const r2 = await dbPool.query(
      `
      INSERT INTO tickets (user_id, fila, assigned_to, status, created_at, updated_at)
      VALUES ($1, $2, $3, 'open', NOW(), NOW())
      RETURNING ticket_number
    `,
      [userId, fila, assignedTo || null]
    );
    return r2.rows?.[0]?.ticket_number || null;
  }
}

async function consumePendingActiveAndAct({ evt, userId, rawUserId, lastUserMessage, publish }) {
  const flow = await getActiveFlow(evt);
  if (!flow) return null;

  const session = await loadSession(userId);
  const vars = { ...(session?.vars || {}) };
  const pa = vars?.pending_active;

  if (!pa) return null;
  if (isExpired(pa.expires_at)) { await clearPendingActive(userId, session); return null; }

  const action = String(pa.action || '').toLowerCase();
  const payload = (typeof pa.payload === 'string'
    ? (() => { try { return JSON.parse(pa.payload); } catch { return {}; } })()
    : pa.payload) || {};

  await clearPendingActive(userId, session);

  if (action === 'flow') {
    const targetBlock = payload.block_id || flow?.start;
    const newVars = { ...vars, previousBlock: session?.current_block || null };
    await saveSession(userId, targetBlock, session?.last_flow_id || null, newVars);

    const outgoing = await runFlow({
      message: (lastUserMessage || '').toLowerCase(), flow,
      vars: undefined, rawUserId, publish
    });
    return outgoing || { ok: true };
  }

  if (action === 'queue' || action === 'agent') {
    const fila = payload.fila || payload.queue || 'geral';
    const assigned = payload.assigned_to || null;
    await createTicketSafely(userId, fila, assigned);

    const newVars = { ...vars, handover: { ...(vars.handover || {}), status: 'open', by: 'active' } };
    await saveSession(userId, session?.current_block || null, session?.last_flow_id || null, newVars);

    const flow2 = await getActiveFlow(evt);
    if (flow2?.blocks?.onhumanstart) {
      await saveSession(userId, 'onhumanstart', session?.last_flow_id || null, newVars);
      const outgoing = await runFlow({ message: null, flow: flow2, vars: undefined, rawUserId, publish });
      return outgoing || { ok: true };
    }
    return { ok: true };
  }

  return null;
}

/* ===================== WhatsApp ===================== */
async function processWhatsApp(evt, { publish = publishDefault } = {}) {
  const value = evt?.payload?.entry?.[0]?.changes?.[0]?.value;
  if (!value) return 'duplicate';
  const msg = value?.messages?.[0];
  if (!msg) return 'duplicate';

  const from = value?.contacts?.[0]?.wa_id || msg?.from;
  const profileName = value?.contacts?.[0]?.profile?.name || 'usuário';
  const userId = `${from}@w.msgcli.net`;
  const msgType = msg.type;

  const { content, userMessage } = await processMediaIfNeeded('whatsapp', { msg });

  // 🔹 resolve flow para este evento / conexão
  const flow = await getActiveFlow(evt);
  const flowId = flow?.id || null;

  const messageId = ensureMessageId('whatsapp', [msg.id]);
  const inserted = await upsertIncomingMessage({
    channel: 'whatsapp',
    userId,
    messageId,
    msgType,
    content,
    flowId,
  });
  if (!inserted) return 'duplicate';

  await publish(userId, 'new_message', {
    ...inserted,
    content: parseForEmit(inserted.content),
    flow_id: flowId,
  });

  await publish(userId, 'bot_processing', {
    user_id: userId,
    status: 'processing',
    flow_id: flowId,
  });

  const handled = await consumePendingActiveAndAct({
    evt,
    userId,
    rawUserId: from,
    lastUserMessage: userMessage,
    publish,
  });
  if (handled) {
    if (handled.user_id) await publish(userId, 'new_message', handled);
    return 'ok';
  }

  if (!flow) return 'no_flow';

  const outgoing = await runFlow({
    message: (userMessage || '').toLowerCase(),
    flow,
    vars: {
      userPhone: from,
      userName: profileName,
      lastUserMessage: userMessage,
      channel: 'whatsapp',
      now: new Date().toISOString(),
      lastMessageId: msg.id,
    },
    rawUserId: from,
    publish,
  });

  if (outgoing?.user_id) await publish(userId, 'new_message', outgoing);
  return 'ok';
}

/* ===================== Telegram ===================== */
async function processTelegram(evt, { publish = publishDefault } = {}) {
  const update = evt?.payload;
  if (!update) return 'duplicate';

  const message = update.message || update.callback_query?.message;
  const from = update.message?.from || update.callback_query?.from;
  const chatId = message?.chat?.id;
  const userId = `${chatId}@t.msgcli.net`;

  const { content, userMessage, msgType } = await processMediaIfNeeded('telegram', { update, message });

  // 🔹 resolve flow
  const flow = await getActiveFlow(evt);
  const flowId = flow?.id || null;

  const messageId = ensureMessageId('telegram', [chatId, (message?.message_id ?? update.update_id)]);
  const inserted = await upsertIncomingMessage({
    channel: 'telegram',
    userId,
    messageId,
    msgType,
    content,
    flowId,
  });
  if (!inserted) return 'duplicate';

  await publish(userId, 'new_message', {
    ...inserted,
    content: parseForEmit(inserted.content),
    flow_id: flowId,
  });

  await publish(userId, 'bot_processing', {
    user_id: userId,
    status: 'processing',
    flow_id: flowId,
  });

  const handled = await consumePendingActiveAndAct({
    evt,
    userId,
    rawUserId: String(chatId),
    lastUserMessage: userMessage,
    publish,
  });
  if (handled) {
    if (handled.user_id) await publish(userId, 'new_message', handled);
    return 'ok';
  }

  if (!flow) return 'no_flow';

  const outgoing = await runFlow({
    message: (userMessage || '').toLowerCase(),
    flow,
    vars: {
      userPhone: String(chatId),
      userName: `${from?.first_name || ''} ${from?.last_name || ''}`.trim(),
      lastUserMessage: userMessage,
      channel: 'telegram',
      now: new Date().toISOString(),
    },
    rawUserId: String(chatId),
    publish,
  });

  if (outgoing?.user_id) await publish(userId, 'new_message', outgoing);
  return 'ok';
}

/* ===================== Facebook (Messenger) ===================== */
async function processFacebook(evt, { publish = publishDefault } = {}) {
  const e = evt?.payload?.entry?.[0];
  const m = e?.messaging?.[0];
  if (!m?.sender?.id) return 'duplicate';

  const text = (m?.message?.text || '').trim();
  const atts = Array.isArray(m?.message?.attachments) ? m.message.attachments : [];
  if (!text && atts.length === 0) return 'duplicate';

  const pageId = m.recipient?.id;
  const from   = m.sender.id;          // PSID
  const userId = `${from}@f.msgcli.net`;
  const mid    = m.message?.mid;
  const ts     = m.timestamp;

  const profileName = 'Facebook User';

  let msgType = 'text';
  let content = text;
  if (!content && atts.length) {
    const att = atts[0];
    if (att?.type === 'location' && att?.payload?.coordinates) {
      msgType = 'location';
      const { lat, long } = att.payload.coordinates;
      content = `📍 ${lat}, ${long}`;
    } else {
      msgType = att?.type === 'image' ? 'image'
            : att?.type === 'audio' ? 'audio'
            : att?.type === 'video' ? 'video'
            : 'document';
      content = { url: att?.payload?.url, type: att?.type };
    }
  }

  // 🔹 resolve flow
  const flow = await getActiveFlow(evt);
  const flowId = flow?.id || null;

  const messageId = ensureMessageId('facebook', [mid, ts, from, pageId]);
  const inserted = await upsertIncomingMessage({
    channel: 'facebook',
    userId,
    messageId,
    msgType,
    content,
    flowId,
  });
  if (!inserted) return 'duplicate';

  await publish(userId, 'new_message', {
    ...inserted,
    content: parseForEmit(inserted.content),
    flow_id: flowId,
  });

  await publish(userId, 'bot_processing', {
    user_id: userId,
    status: 'processing',
    flow_id: flowId,
  });

  const handled = await consumePendingActiveAndAct({
    evt,
    userId,
    rawUserId: from,
    lastUserMessage: typeof content === 'string' ? content : null,
    publish,
  });
  if (handled) {
    if (handled.user_id) await publish(userId, 'new_message', handled);
    return 'ok';
  }

  if (!flow) return 'no_flow';

  const outgoing = await runFlow({
    message: (typeof content === 'string' ? content : '').toLowerCase(),
    flow,
    vars: {
      userPhone: from,
      userName: profileName,
      lastUserMessage: typeof content === 'string' ? content : null,
      channel: 'facebook',
      now: new Date().toISOString(),
      lastMessageId: mid || messageId,
      metaPageId: pageId,
    },
    rawUserId: from,
    publish,
  });

  if (outgoing?.user_id) await publish(userId, 'new_message', outgoing);
  return 'ok';
}

/* ===================== Instagram (Messenger API for IG) ===================== */
async function processInstagram(evt, { publish = publishDefault } = {}) {
  const entry = evt?.payload?.entry?.[0];
  const m1 = entry?.messaging?.[0];
  const v2 = entry?.changes?.[0]?.value;
  const m2 = v2?.messages?.[0];

  let from, pageId, timestamp, mid, text, attachments;

  if (m1?.sender?.id) {
    from = m1.sender.id;
    pageId = m1.recipient?.id;
    timestamp = m1.timestamp;
    mid = m1.message?.mid;
    text = (m1.message?.text || '').trim();
    attachments = Array.isArray(m1.message?.attachments) ? m1.message.attachments : [];
  } else if (m2?.from) {
    from = m2.from;
    pageId = v2?.id || v2?.page?.id;
    timestamp = m2.timestamp || entry?.time;
    mid = m2.id;
    text = (m2.text || '').trim();
    attachments = Array.isArray(m2.attachments) ? m2.attachments : [];
  } else {
    return 'duplicate';
  }

  if (!from) return 'duplicate';
  if (!text && attachments.length === 0) return 'duplicate';

  const userId = `${from}@i.msgcli.net`;
  const profileName = 'Instagram User';

  let msgType = 'text';
  let content = text;
  if (!content && attachments.length) {
    const att = attachments[0];
    if (att?.type === 'location' && att?.payload?.coordinates) {
      msgType = 'location';
      const { lat, long } = att.payload.coordinates;
      content = `📍 ${lat}, ${long}`;
    } else {
      msgType = att?.type === 'image' ? 'image'
            : att?.type === 'audio' ? 'audio'
            : att?.type === 'video' ? 'video'
            : 'document';
      content = { url: att?.payload?.url, type: att?.type };
    }
  }

  // 🔹 resolve flow
  const flow = await getActiveFlow(evt);
  const flowId = flow?.id || null;

  const messageId = ensureMessageId('instagram', [mid, timestamp, from, pageId]);
  const inserted = await upsertIncomingMessage({
    channel: 'instagram',
    userId,
    messageId,
    msgType,
    content,
    flowId,
  });
  if (!inserted) return 'duplicate';

  await publish(userId, 'new_message', {
    ...inserted,
    content: parseForEmit(inserted.content),
    flow_id: flowId,
  });

  await publish(userId, 'bot_processing', {
    user_id: userId,
    status: 'processing',
    flow_id: flowId,
  });

  const handled = await consumePendingActiveAndAct({
    evt,
    userId,
    rawUserId: from,
    lastUserMessage: typeof content === 'string' ? content : null,
    publish,
  });
  if (handled) {
    if (handled.user_id) await publish(userId, 'new_message', handled);
    return 'ok';
  }

  if (!flow) return 'no_flow';

  const outgoing = await runFlow({
    message: (typeof content === 'string' ? content : '').toLowerCase(),
    flow,
    vars: {
      userPhone: from,
      userName: profileName,
      lastUserMessage: typeof content === 'string' ? content : null,
      channel: 'instagram',
      now: new Date().toISOString(),
      lastMessageId: mid || messageId,
      metaPageId: pageId,
    },
    rawUserId: from,
    publish,
  });

  if (outgoing?.user_id) await publish(userId, 'new_message', outgoing);
  return 'ok';
}

/* ===================== Router ===================== */
export async function processEvent(evt, { publish = publishDefault } = {}) {
  if (evt?.kind === SYSTEM_EVENT && evt?.event?.type === SYSTEM_EVT_TICKET_STATUS) {
    const result = await handleTicketStatusEvent(evt.event, { publish });
    if (result?.resume) {
      const storageUserId = result.storageUserId || evt.event.userId;
      const rawUserId = result.rawUserId || String(storageUserId || '').split('@')[0];

      const flow = await getActiveFlow(evt);
      if (!flow) return 'no_flow';

      const session = await loadSession(storageUserId);
      const vars = { ...(session?.vars || {}) };

      const originId = vars?.handover?.originBlock;
      const originBlock = originId ? flow?.blocks?.[originId] : null;

      let nextFromHuman = null;
      if (originBlock) nextFromHuman = determineNextBlock(originBlock, vars, flow, originId);
      if (!nextFromHuman || !flow?.blocks?.[nextFromHuman]) {
        if (flow?.blocks?.onhumanreturn) nextFromHuman = 'onhumanreturn';
        else if (flow?.blocks?.onerror) nextFromHuman = 'onerror';
        else nextFromHuman = flow?.start;
      }

      const updatedVars = {
        ...vars,
        handover: { ...(vars.handover || {}), status: 'idle', result: 'closed' },
        previousBlock: originId || 'human'
      };

      await saveSession(storageUserId, nextFromHuman, session?.flow_id, updatedVars);

      const outgoing = await runFlow({ message: null, flow, vars: undefined, rawUserId, publish });
      if (outgoing?.user_id) await publish(storageUserId, 'new_message', outgoing);
    }
    return 'ok';
  }

  const ch = evt?.channel;
  if (!ch) return 'duplicate';
  if (ch === 'whatsapp')  return processWhatsApp(evt,  { publish });
  if (ch === 'telegram')  return processTelegram(evt,  { publish });
  if (ch === 'facebook')  return processFacebook(evt,  { publish });
  if (ch === 'instagram') return processInstagram(evt, { publish });
  return 'ok';
}
