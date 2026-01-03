// engine/ticketManager.js
import { dbPool } from './services/db.js';
import { v4 as uuidv4 } from 'uuid';
import { emitQueuePush, emitQueueCount } from '../services/realtime/emitToRoom.js';

// 🔧 Helper: monta o user_id persistido por canal
function buildStorageUserId(rawUserId, channel) {
  // já vem com sufixo? mantém
  if (/@[a-z]\./i.test(String(rawUserId))) return String(rawUserId);

  switch ((channel || '').toLowerCase()) {
    case 'whatsapp':
    case 'wa':
      return `${rawUserId}@w.msgcli.net`;
    case 'telegram':
    case 'tg':
      return `${rawUserId}@t.msgcli.net`;
    case 'webchat':
    case 'web':
      return `${rawUserId}@web`;
    default:
      return String(rawUserId);
  }
}

/**
 * Distribui ticket e insere mensagem de sistema "Ticket #123".
 * RECEBE: rawUserId + channel para compor o user_id correto.
 * EMITE: quando modo 'manual' (ou auto sem agente) cria ticket SEM assigned_to,
 *        emite 'queue_push' para o room 'queue:<fila>' e um 'queue_count' de sincronização.
 */
export async function distribuirTicket(rawUserId, queueName, channel) {
  const client = await dbPool.connect();
  const storageUserId = buildStorageUserId(rawUserId, channel);

  try {
    await client.query('BEGIN');

    async function inserirMensagemSistema(ticketNumber) {
      const systemMessage = `Ticket #${ticketNumber}`;
      const systemMessageId = uuidv4();

      await client.query(
        `
        INSERT INTO messages (
          user_id,
          type,
          direction,
          content,
          timestamp,
          message_id
        ) VALUES (
          $1, 'system', 'system', $2, NOW(), $3
        )
        `,
        [storageUserId, systemMessage, systemMessageId]
      );
    }

    // 0) Se já existe aberto, não cria outro
    const ticketAbertoQuery = await client.query(
      'SELECT * FROM tickets WHERE user_id = $1 AND status = $2 LIMIT 1',
      [storageUserId, 'open']
    );
    const ticketAberto = ticketAbertoQuery.rows[0];
    if (ticketAberto) {
      await client.query('COMMIT');
      return { ticketExists: true, ticketId: ticketAberto.id, userId: storageUserId };
    }

    // 1) Configuração de distribuição
    const configQuery = await client.query(
      'SELECT value FROM settings WHERE key = $1 LIMIT 1',
      ['distribuicao_tickets']
    );
    const modoDistribuicao = configQuery.rows[0]?.value || 'manual';

    // 2) Determinar fila
    let filaCliente = queueName;
    if (!filaCliente) {
      const filaResult = await client.query(
        'SELECT fila FROM clientes WHERE user_id = $1 LIMIT 1',
        [storageUserId]
      );
      filaCliente = filaResult.rows[0]?.fila || 'Default';
    }

    // ——— MODO MANUAL: cria SEM assigned_to (entra na FILA) ———
    if (modoDistribuicao === 'manual') {
      const createTicketQuery = await client.query(
        `SELECT create_ticket($1, $2, $3) as ticket_number`,
        [storageUserId, filaCliente, null] // <= sem atendente
      );

      const ticketNumber = createTicketQuery.rows[0].ticket_number;
      await inserirMensagemSistema(ticketNumber);

      // Aplica as mudanças no banco ANTES de emitir
      await client.query('COMMIT');

      // 🔔 Emite queue_push para atualizar contador imediatamente
      await emitQueuePush(filaCliente, {
        user_id: storageUserId,
        ticket_number: ticketNumber,
        assigned_to: null,
        mode: 'manual',
      });

      // 🔁 (opcional) emite um queue_count para sincronizar estado
      try {
        const { rows } = await dbPool.query(
          `SELECT COUNT(*)::int AS c
             FROM tickets
            WHERE status = 'open' AND assigned_to IS NULL AND fila = $1`,
          [filaCliente]
        );
        const count = rows?.[0]?.c ?? 0;
        await emitQueueCount(filaCliente, count);
      } catch (e) {
        console.warn('[ticketManager] falha ao emitir queue_count:', e?.message);
      }

      return {
        mode: 'manual',
        ticketNumber,
        assignedTo: null,
        userId: storageUserId,
      };
    }

    // ——— MODO AUTOMÁTICO: tenta achar atendente ———
    const atendentesQuery = await client.query(
      'SELECT email, filas FROM atendentes WHERE status = $1',
      ['online']
    );
    const candidatos = atendentesQuery.rows.filter(
      a => Array.isArray(a.filas) && a.filas.includes(filaCliente)
    );

    // Nenhum atendente: cria SEM assigned_to (entra na FILA) e emite push
    if (!candidatos.length) {
      const createTicketQuery = await client.query(
        `SELECT create_ticket($1, $2, $3) as ticket_number`,
        [storageUserId, filaCliente, null]
      );
      const ticketNumber = createTicketQuery.rows[0].ticket_number;

      await client.query('COMMIT');

      await emitQueuePush(filaCliente, {
        user_id: storageUserId,
        ticket_number: ticketNumber,
        assigned_to: null,
        mode: 'auto-no-agent',
      });

      // sincroniza contagem
      try {
        const { rows } = await dbPool.query(
          `SELECT COUNT(*)::int AS c
             FROM tickets
            WHERE status = 'open' AND assigned_to IS NULL AND fila = $1`,
          [filaCliente]
        );
        const count = rows?.[0]?.c ?? 0;
        await emitQueueCount(filaCliente, count);
      } catch (e) {
        console.warn('[ticketManager] falha ao emitir queue_count:', e?.message);
      }

      return {
        success: true,
        ticketNumber,
        assignedTo: null,
        mode: 'auto-no-agent',
        userId: storageUserId,
      };
    }

    // Há atendentes: escolhe o de menor carga
    const cargasQuery = await client.query(`
      SELECT assigned_to, COUNT(*) as total_tickets 
      FROM tickets 
      WHERE status = 'open' AND assigned_to IS NOT NULL
      GROUP BY assigned_to
    `);
    const mapaCargas = {};
    cargasQuery.rows.forEach(l => {
      mapaCargas[l.assigned_to] = parseInt(l.total_tickets);
    });

    candidatos.sort((a, b) => {
      const cargaA = mapaCargas[a.email] || 0;
      const cargaB = mapaCargas[b.email] || 0;
      return cargaA - cargaB;
    });

    const escolhido = candidatos[0]?.email;
    if (!escolhido) {
      await client.query('COMMIT');
      return { success: false, error: 'No agent available', userId: storageUserId };
    }

    // Cria já atribuído (não entra na fila => não emite push)
    const createTicketQuery = await client.query(
      `SELECT create_ticket($1, $2, $3) as ticket_number`,
      [storageUserId, filaCliente, escolhido]
    );
    const ticketNumber = createTicketQuery.rows[0].ticket_number;

    await client.query('COMMIT');

    return {
      success: true,
      ticketNumber,
      assignedTo: escolhido,
      mode: 'auto-created',
      userId: storageUserId,
    };

  } catch (error) {
    try { await client.query('ROLLBACK'); } catch {}
    console.error('❌ Erro na distribuição de ticket:', error);
    return { success: false, error: error.message };
  } finally {
    client.release();
  }
}
