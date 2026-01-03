// engine/messenger.js
import { v4 as uuidv4 } from 'uuid';
import { dbPool } from './services/db.js';
import { MessageAdapter } from './messageAdapters.js';
import { CHANNELS } from './messageTypes.js';
import { enqueueOutgoing } from './queue.js';

function stripMsgcliSuffix(id) {
  const s = String(id ?? '');
  const at = s.indexOf('@');
  if (at > -1 && /@(w|t|f|i)\.msgcli\.net$/i.test(s)) return s.slice(0, at);
  return s;
}

function normalizeRecipientForChannel(channel, rawTo) {
  const ch = String(channel || '').toLowerCase();
  const base = stripMsgcliSuffix(rawTo);

  if (ch === CHANNELS.WHATSAPP)  return String(base).replace(/\D/g, '');
  if (ch === CHANNELS.FACEBOOK)  return String(base).replace(/\D/g, '');
  if (ch === CHANNELS.INSTAGRAM) return String(base).replace(/\D/g, '');
  if (ch === CHANNELS.TELEGRAM)  return base;
  return base;
}

function buildDbContent(type, adapted, original) {
  if (String(type).toLowerCase() === 'text' || String(type).toLowerCase() === 'error') {
    const t =
      (typeof adapted === 'string' && adapted) ||
      adapted?.body ||
      adapted?.text ||
      adapted?.message?.text ||
      (typeof original === 'string' && original) ||
      original?.body ||
      original?.text ||
      original?.message?.text;
    return t != null ? String(t) : '';
  }
  const obj = adapted ?? original ?? {};
  try { return JSON.stringify(obj); } catch { return JSON.stringify({}); }
}

function adaptForChannel(channel, type, content) {
  const t = String(type || '').toLowerCase();
  const normalizedType = t === 'error' ? 'text' : t;
  const normalizedContent = (t === 'error')
    ? { text: (typeof content === 'string' && content) || content?.text || content?.body || 'Desculpe, não entendi.' }
    : content;

  if (channel === CHANNELS.WHATSAPP)  return MessageAdapter.toWhatsapp({ type: normalizedType, content: normalizedContent });
  if (channel === CHANNELS.TELEGRAM)  return MessageAdapter.toTelegram({ type: normalizedType, content: normalizedContent });
  if (channel === CHANNELS.FACEBOOK)  return MessageAdapter.toFacebook({ type: normalizedType, content: normalizedContent });
  if (channel === CHANNELS.INSTAGRAM) return MessageAdapter.toInstagram({ type: normalizedType, content: normalizedContent });
  return normalizedContent;
}

export async function sendMessageByChannel(channel, to, type, content, context, metadata = null) {
  const ch = String(channel || '').toLowerCase();
  const toNormalized = normalizeRecipientForChannel(channel, to);

  const userId =
    ch === CHANNELS.WHATSAPP ? `${toNormalized}@w.msgcli.net`
  : ch === CHANNELS.TELEGRAM ? `${toNormalized}@t.msgcli.net`
  : ch === CHANNELS.FACEBOOK ? `${toNormalized}@f.msgcli.net`
  : ch === CHANNELS.INSTAGRAM ? `${toNormalized}@i.msgcli.net`
  : toNormalized;

  const t = String(type || '').toLowerCase();
  const runtimeType = (t === 'error') ? 'text' : t;

  const adapted = adaptForChannel(channel, runtimeType, content);
  const saveType = runtimeType;

  const tempId = uuidv4();
  const dbContent = buildDbContent(saveType, adapted, content);

  // 🔹 extrai flow_id do metadata (runFlow já manda)
  const flowId =
    metadata?.flow_id ??
    metadata?.flowId ??
    null;

  const metadataJson = metadata ? JSON.stringify(metadata) : null;

  const { rows } = await dbPool.query(
    `
    INSERT INTO messages (
      user_id, message_id, direction, type, content, "timestamp",
      flow_id, status, metadata, created_at, updated_at, channel
    )
    VALUES ($1,$2,'outgoing',$3,$4,NOW(),
            $5,'pending',$6,NOW(),NOW(),$7)
    RETURNING *
  `,
    [userId, tempId, saveType, dbContent, flowId, metadataJson, channel]
  );
  const pending = rows[0];

  const tenant_id =
    context?.tenant_id || context?.tenantId || process.env.DEFAULT_TENANT_ID || null;

  await enqueueOutgoing({
    tempId,
    channel,
    to: toNormalized,
    userId,
    type: runtimeType,
    content: adapted,
    tenant_id,
    page_id: context?.page_id || context?.pageId || null,
    connection_id: context?.connection_id || context?.connectionId || null,
    flow_id: flowId, // 🔹 agora vai para o worker-outgoing
  });

  return pending;
}

export default { sendMessageByChannel };
