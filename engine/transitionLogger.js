// File: engine/transitionLogger.js
// Exemplo com dbPool (exporta função logBlockTransition)
import { dbPool } from './services/db.js'; // ajuste o path conforme seu projeto

/**
 * params = {
 *   userId, channel, flowId, blockId, blockLabel, blockType, vars, ticketNumber, client (optional db client)
 * }
 */
export async function logBlockTransition(params = {}) {
  const {
    userId,
    channel = null,
    flowId = null,
    blockId,
    blockLabel = null,
    blockType = null,
    vars = null,
    ticketNumber = null,
    visible = true,
    client // opcional: passar client para transação externa
  } = params;

  if (!userId || !blockId) {
    throw new Error('logBlockTransition: falta userId ou blockId');
  }

  const db = client || dbPool;
  const inserted = null;

  // Prepare insert values
  const insertValues = [
    userId,
    channel,
    flowId,
    blockId,
    blockLabel,
    blockType,
    new Date().toISOString(),
    JSON.stringify(vars || {}),
    ticketNumber,
    visible
  ];

  // Transação: 1) fecha último registro aberto (sem left_at) para esse user 2) insere novo registro
  try {
    await db.query('BEGIN');

    // Fecha último registro aberto (se existir)
    await db.query(
      `UPDATE bot_transitions
       SET left_at = now()
       WHERE id = (
         SELECT id FROM bot_transitions
         WHERE user_id = $1
         AND left_at IS NULL
         ORDER BY entered_at DESC
         LIMIT 1
       ) AND left_at IS NULL`,
      [userId]
    );

    // Insere nova transição
    const insertSql = `
      INSERT INTO bot_transitions
        (user_id, channel, flow_id, block_id, block_label, block_type, entered_at, vars, ticket_number, visible)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
      RETURNING *;
    `;
    const { rows } = await db.query(insertSql, insertValues);

    await db.query('COMMIT');
    return rows[0];
  } catch (err) {
    try { await db.query('ROLLBACK'); } catch (e) { /* ignore */ }
    throw err;
  }
}
