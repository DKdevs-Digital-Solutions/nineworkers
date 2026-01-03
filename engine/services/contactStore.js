// engine/services/contactStore.js
import axios from 'axios';
import { query } from './db.js';

function onlyDigits(s) {
  return String(s || '').replace(/\D+/g, '');
}

// 🔹 agora também controlamos flow_id
const MUTABLE = ['name', 'phone', 'channel', 'document', 'email', 'atendido', 'flow_id'];
const META_API_VERSION = 'v18.0';

/* --------------------------------
 * Mapeamento de linhas do banco → objeto contato
 * -------------------------------- */
function rowToContact(row) {
  if (!row) return null;
  return {
    id: row.user_id,     // usar user_id como contact.id (canônico)
    uuid: row.id,        // PK uuid
    phone: row.phone,
    name: row.name,
    channel: row.channel,
    document: row.document,
    email: row.email,
    atendido: row.atendido ?? false,
    flow_id: row.flow_id || null, // 🔹 espelha flow_id
    created_at: row.created_at,
    updated_at: row.updated_at,
  };
}

/* --------------------------------
 * CRUD básico
 * -------------------------------- */
export async function loadContactByUserId(userId) {
  const sql = `
    SELECT id,
           user_id,
           phone,
           "name",
           channel,
           "document",
           email,
           atendido,
           flow_id,
           created_at,
           updated_at
      FROM clientes
     WHERE user_id = $1
     LIMIT 1
  `;
  const { rows } = await query(sql, [userId]);
  return rowToContact(rows[0]);
}

// 🔹 aceitamos flowId opcional aqui também
export async function insertContact({
  userId,
  phone,
  channel,
  name = null,
  document = null,
  email = null,
  flowId = null,
}) {
  const sql = `
    INSERT INTO clientes (user_id, phone, channel, "name", "document", email, flow_id)
    VALUES ($1,$2,$3,$4,$5,$6,$7)
    RETURNING id,
              user_id,
              phone,
              "name",
              channel,
              "document",
              email,
              atendido,
              flow_id,
              created_at,
              updated_at
  `;
  const { rows } = await query(sql, [
    userId,
    phone,
    channel,
    name,
    document,
    email,
    flowId,
  ]);
  return rowToContact(rows[0]);
}

export async function updateContactFields(userId, patch = {}) {
  const keys = MUTABLE.filter(k => patch[k] !== undefined);
  if (!keys.length) return null;

  const sets = keys.map((k, i) => `"${k}" = $${i + 2}`);
  const params = [userId, ...keys.map(k => patch[k])];

  const sql = `
    UPDATE clientes
       SET ${sets.join(', ')},
           updated_at = NOW()
     WHERE user_id = $1
     RETURNING id,
               user_id,
               phone,
               "name",
               channel,
               "document",
               email,
               atendido,
               flow_id,
               created_at,
               updated_at
  `;
  const { rows } = await query(sql, params);
  return rowToContact(rows[0]);
}

export function diffContact(orig = {}, cur = {}) {
  const patch = {};
  for (const k of MUTABLE) {
    if (cur[k] !== undefined && cur[k] !== orig[k]) patch[k] = cur[k];
  }
  return patch;
}

/* =======================================================================
   Helpers específicos p/ upsert: phone por canal e lookup de nome (META)
   ======================================================================= */

function normalizeChannel(ch) {
  return String(ch || '').toLowerCase().trim();
}

function derivePhone(rawUserId, channel) {
  const ch = normalizeChannel(channel);
  if (ch === 'whatsapp') {
    // WA chega com dígitos; garantimos só números
    return onlyDigits(rawUserId);
  }
  if (ch === 'telegram') {
    // Telegram chat_id pode ser negativo/inteiro; gravamos string do id
    return String(rawUserId || '').trim();
  }
  if (ch === 'facebook' || ch === 'instagram') {
    // Guardamos o próprio ID (PSID/IGID) como "phone" por compatibilidade
    return String(rawUserId || '').trim();
  }
  // fallback
  return String(rawUserId || '').trim();
}

function isPlaceholderName(name) {
  const s = String(name || '').trim().toLowerCase();
  return (
    !s ||
    s === 'facebook user' ||
    s === 'instagram user' ||
    s === 'usuário' ||
    s === 'usuario'
  );
}

/**
 * Busca token META preferencialmente vinculado ao canal e/ou pageId.
 * Usa apenas settings->>'page_access_token' (sem coluna access_token).
 * Fallback para qualquer conexão META ativa ou env FB_PAGE_TOKEN.
 */
async function resolveAnyMetaToken(preferredChannel = null, pageId = null) {
  const ch = normalizeChannel(preferredChannel);

  // 1) Tentar pelo pageId (prioritário)
  if (pageId) {
    const sqlByPage = `
      SELECT settings->>'page_access_token' AS token
        FROM public.tenant_channel_connections
       WHERE provider='meta' AND is_active=true
         AND (
               settings->>'page_id' = $1
            OR settings->'page'->>'id' = $1
            OR COALESCE(external_id,'') = $1
         )
       ORDER BY updated_at DESC
       LIMIT 1
    `;
    const r = await query(sqlByPage, [String(pageId)]);
    const tok = r?.rows?.[0]?.token?.trim();
    if (process.env.DEBUG_META) {
      console.log(
        '[meta] token by pageId=%s -> %s',
        pageId,
        tok ? 'OK' : 'NULO'
      );
    }
    if (tok) return tok;
  }

  // 2) Tentar pelo canal (facebook/instagram)
  if (ch === 'facebook' || ch === 'instagram') {
    const sqlPref = `
      SELECT settings->>'page_access_token' AS token
        FROM public.tenant_channel_connections
       WHERE provider='meta' AND is_active=true
         AND channel = $1
       ORDER BY updated_at DESC
       LIMIT 1
    `;
    const pref = await query(sqlPref, [ch]);
    const tokPref = pref?.rows?.[0]?.token?.trim();
    if (process.env.DEBUG_META) {
      console.log(
        '[meta] token by channel=%s -> %s',
        ch,
        tokPref ? 'OK' : 'NULO'
      );
    }
    if (tokPref) return tokPref;
  }

  // 3) Fallback “qualquer meta” (facebook/instagram)
  const sqlAny = `
    SELECT settings->>'page_access_token' AS token
      FROM public.tenant_channel_connections
     WHERE provider='meta' AND is_active=true
       AND channel IN ('facebook','instagram')
     ORDER BY updated_at DESC
     LIMIT 1
  `;
  const any = await query(sqlAny, []);
  const tokAny =
    any?.rows?.[0]?.token?.trim() ||
    process.env.FB_PAGE_TOKEN?.trim() ||
    null;

  if (process.env.DEBUG_META) {
    console.log('[meta] token ANY -> %s', tokAny ? 'OK' : 'NULO');
  }
  return tokAny;
}

/**
 * Facebook: mesmo endpoint do seu cURL funcional.
 * fields = first_name,last_name,name,profile_pic
 */
async function fetchFacebookNameByPsid(psid, token) {
  const url = `https://graph.facebook.com/${META_API_VERSION}/${encodeURIComponent(
    psid
  )}?fields=first_name,last_name,name,profile_pic&access_token=${encodeURIComponent(
    token
  )}`;
  try {
    const { data } = await axios.get(url, { timeout: 15000 });
    const first = (data?.first_name || '').trim();
    const last = (data?.last_name || '').trim();
    const full = (data?.name || `${first} ${last}`.trim()).trim();
    if (process.env.DEBUG_META)
      console.log('[meta][fb] %s -> "%s"', psid, full || null);
    return full || null;
  } catch (e) {
    if (process.env.DEBUG_META)
      console.log('[meta][fb] erro %s: %s', psid, e?.message);
    return null;
  }
}

async function fetchInstagramNameById(igid, token) {
  const url = `https://graph.facebook.com/${META_API_VERSION}/${encodeURIComponent(
    igid
  )}?fields=name,username&access_token=${encodeURIComponent(token)}`;
  try {
    const { data } = await axios.get(url, { timeout: 15000 });
    const full = (data?.name || data?.username || '').trim();
    if (process.env.DEBUG_META)
      console.log('[meta][ig] %s -> "%s"', igid, full || null);
    return full || null;
  } catch (e) {
    if (process.env.DEBUG_META)
      console.log('[meta][ig] erro %s: %s', igid, e?.message);
    return null;
  }
}

/**
 * Se canal for facebook/instagram e o nome for placeholder/ausente,
 * tenta resolver via Graph com o token do canal correspondente.
 */
async function resolveMetaDisplayNameIfNeeded({
  rawUserId,
  channel,
  pageId,
  candidateName,
}) {
  const ch = normalizeChannel(channel);
  if (ch !== 'facebook' && ch !== 'instagram') return candidateName || null;

  const need = isPlaceholderName(candidateName);
  if (!need) return candidateName || null;

  // token prioriza pageId
  const token = await resolveAnyMetaToken(ch, pageId);
  if (!token) return candidateName || null;

  if (ch === 'facebook') {
    const name = await fetchFacebookNameByPsid(rawUserId, token);
    return name || candidateName || null;
  }
  if (ch === 'instagram') {
    const name = await fetchInstagramNameById(rawUserId, token);
    return name || candidateName || null;
  }
  return candidateName || null;
}

/* =======================================================================
   Upsert canônico (único ponto que grava/atualiza o cliente)
   ======================================================================= */
export async function upsertContactFromSession({
  userId,
  rawUserId,
  channel,
  meta = {},
  contactPatch = {},
  options = {},
}) {
  const { lean = false } = options || {}; // <- flag pra pular Graph

  // 1) tenta carregar
  let contact = await loadContactByUserId(userId);

  // 2) prepara dados básicos
  const phone =
    contact?.phone || contactPatch.phone || derivePhone(rawUserId, channel);
  let name = contact?.name || contactPatch.name || null;

  // 3) resolve / prioriza flow_id
  const flowId =
    contactPatch.flow_id ??
    meta.flow_id ??
    contact?.flow_id ??
    null;

  // 4) (opcional) resolve display name no Graph só quando lean=false
  if (!lean) {
    try {
      name = await resolveMetaDisplayNameIfNeeded({
        rawUserId,
        channel,
        pageId: meta?.pageId || null,
        candidateName: name,
      });
    } catch (e) {
      if (process.env.DEBUG_META)
        console.log('[meta] resolve display name falhou:', e?.message);
    }
  }

  // 5) insert se não existe
  if (!contact) {
    contact = await insertContact({
      userId,
      phone,
      channel,
      name: name ?? null,
      document: contactPatch.document ?? null,
      email: contactPatch.email ?? null,
      flowId, // 🔹 grava flow_id na criação
    });
    return contact;
  }

  // 6) update seletivo (incluindo flow_id)
  const desired = {
    name: name ?? undefined,
    phone: phone ?? undefined,
    channel: channel ?? undefined,
    document: contactPatch.document ?? undefined,
    email: contactPatch.email ?? undefined,
    flow_id: flowId ?? undefined,
  };
  Object.keys(desired).forEach(k => {
    if (desired[k] === undefined) delete desired[k];
  });

  const current = {
    name: contact.name,
    phone: contact.phone,
    channel: contact.channel,
    document: contact.document,
    email: contact.email,
    flow_id: contact.flow_id,
  };

  const patch = {};
  for (const k of Object.keys(desired)) {
    if (desired[k] != null && desired[k] !== current[k]) {
      patch[k] = desired[k];
    }
  }

  if (Object.keys(patch).length) {
    const updated = await updateContactFields(userId, patch);
    if (updated) contact = updated;
  }

  return contact;
}
