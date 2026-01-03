// services/high/handleTicketStatusEvent.js
import { loadSession, saveSession } from '../../engine/sessionManager.js';
import { SYSTEM_EVT_TICKET_STATUS, TICKET_STATUS } from '../../engine/messageTypes.js';

const toRaw = (storageUserId) => {
  const s = String(storageUserId || '');
  const i = s.indexOf('@');
  return i > -1 ? s.slice(0, i) : s;
};

/**
 * Atualiza a sessão ao encerrar o atendimento:
 * - vars.ticket.number = <número final do ticket>
 * - vars.handover.status = 'closed'
 * Mantém current_block = 'human' para o executor retomar pelas actions do bloco human.
 * NÃO carrega fluxo aqui — deixa o processEvent fazer pelo getActiveFlow() (DB).
 */
export async function handleTicketStatusEvent(evt /*, { io } */) {
  if (evt.type !== SYSTEM_EVT_TICKET_STATUS) return { ok: true, resume: false };

  const status = String(evt.status || '').toLowerCase();
  if (status !== TICKET_STATUS.CLOSED) return { ok: true, resume: false };

  const storageUserId = evt.userId;            // ex.: 5511...@w.msgcli.net
  const rawUserId     = toRaw(storageUserId);
  const number        = evt.ticketNumber || null;
  const fila          = evt.fila || null;

  const session = await loadSession(storageUserId);
  if (!session) return { ok: false, reason: 'no-session', resume: false };

  const vars = { ...(session.vars || {}) };

  // ✅ grava o NÚMERO FINAL do ticket
  vars.ticket = {
    ...(vars.ticket || {}),
    number: number || vars.ticket?.number || null,
    fila: fila || vars.ticket?.fila || vars.fila || null
  };

  // ✅ marca fechamento do handover
  vars.handover = { ...(vars.handover || {}), status: 'closed', result: 'closed' };

  // mantém current_block = 'human' — o executor retoma pelas actions do bloco human
  await saveSession(storageUserId, 'human', session.flow_id, vars);

  // sinaliza para o processEvent retomar o fluxo via getActiveFlow()
  return { ok: true, resume: true, storageUserId, rawUserId };
}
