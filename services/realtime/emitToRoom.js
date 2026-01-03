// services/realtime/emitToRoom.js
import axios from "axios";

const BASE = (process.env.CENTRIFUGO_HTTP_URL || "").replace(/\/$/, "");
const KEY  = process.env.CENTRIFUGO_API_KEY || "";
const TENANT = process.env.TENANT_ID || "hmg"; // ajuste conforme seu ambiente

function notConfigured() {
  if (!BASE || !KEY) {
    console.warn("[emitToRoom] configure CENTRIFUGO_HTTP_URL e CENTRIFUGO_API_KEY");
    return true;
  }
  return false;
}

// Garante que publicaremos no mesmo prefixo que o front assina:
// - se vier "queue:Comercial" vira "conv:t:<TENANT>:queue:Comercial"
// - se vier já fully-qualified (começa com "conv:") mantemos
function normalizeChannel(room) {
  if (!room) return room;
  if (room.startsWith("conv:")) return room;
  return `conv:t:${TENANT}:${room}`;
}

export async function emitToRoom({ room, event, payload } = {}) {
  if (notConfigured()) return;
  if (!room || !event) {
    console.warn("[emitToRoom] faltando room/event", { room, event });
    return;
  }

  const channel = normalizeChannel(room);

  try {
    await axios.post(
      `${BASE}/api/publish`,
      { channel, data: { event, payload } },
      {
        timeout: 7000,
        headers: {
          "Content-Type": "application/json",
          // ✅ no v5/v6 o correto é X-API-Key (não Authorization)
          "X-API-Key": KEY,
        },
      }
    );
  } catch (e) {
    console.warn("[emitToRoom] falhou:", e?.response?.status ?? "ERR", e?.message);
  }
}

// ===== Helpers multi-tenant (alinhados com o front) =====
// O front entra nas filas como `queue:<FILA>` e o socket converte para
// `conv:t:<TENANT>:queue:<FILA>`. Vamos publicar no mesmo canal.

export const emitQueuePush = (tenantId, fila, extra = {}) =>
  emitToRoom({
    room: `conv:t:${tenantId}:queue:${fila}`,
    event: "queue_push",
    payload: { tenantId, fila, ...extra },
  });

export const emitQueuePop = (tenantId, fila, extra = {}) =>
  emitToRoom({
    room: `conv:t:${tenantId}:queue:${fila}`,
    event: "queue_pop",
    payload: { tenantId, fila, ...extra },
  });

export const emitQueueCount = (tenantId, fila, count) =>
  emitToRoom({
    room: `conv:t:${tenantId}:queue:${fila}`,
    event: "queue_count",
    payload: { tenantId, fila, count },
  });

export const emitUpdateMessage = (tenantId, agentId, payload = {}) =>
  emitToRoom({
    room: `user:t:${tenantId}:agent:${agentId}`,
    event: "update_message",
    payload: { tenantId, agentId, ...payload },
  });

// Mensagens de conversa (mantém prefixo conv:)
export const emitConversationEvent = (tenantId, waNumber, event, payload = {}) =>
  emitToRoom({
    room: `conv:t:${tenantId}:${waNumber}`,
    event,
    payload: { tenantId, waNumber, ...payload },
  });
