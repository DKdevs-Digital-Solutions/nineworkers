import { dbPool } from './services/db.js'
import { randomUUID } from 'crypto'

export async function logOutgoingMessage(userId, type, content, flowId) {
  const query = `
    INSERT INTO messages (
      user_id, message_id, direction, type, content,
      timestamp, flow_id, status,
      metadata, created_at, updated_at, channel
    ) VALUES (
      $1, $2, $3, $4, $5,
      $6, $7, $8, $9, $10,
      $11, $12
    ) RETURNING *
  `;

  const values = [
    userId,
    randomUUID(),
    'outgoing',
    type,
    content,
    new Date().toISOString(),
    flowId || null,
    'sent',
    null, // metadata
    new Date().toISOString(),
    new Date().toISOString(),
    'whatsapp'
  ];

  try {
    const { rows } = await dbPool.query(query, values);
    const loggedMessage = rows[0];
    console.log('✅ Mensagem outgoing gravada:', loggedMessage);
    return loggedMessage;
  } catch (error) {
    console.error('❌ Erro ao gravar outgoing:', error);
    return null;
  }
}

/**
 * Grava um "fallback" quando falha no envio de mídia.
 */
export async function logOutgoingFallback(userId, fallbackText, flowId) {
  const query = `
    INSERT INTO messages (
      id, user_id, message_id, direction, type,
      content, timestamp, flow_id, status, metadata, created_at, updated_at, channel
    ) VALUES (
      $1, $2, $3, $4, $5,
      $6, $7, $8, $9, $10,
      $11, $12, $13
    )
  `;

  const values = [
    randomUUID(),
    userId,
    randomUUID(),
    'outgoing',
    'text',
    fallbackText,
    new Date().toISOString(),
    flowId || null,
    'sent',
    JSON.stringify({ fallback: true }),
    new Date().toISOString(),
    new Date().toISOString(),
    'whatsapp'
  ];

  try {
    await dbPool.query(query, values);
  } catch (error) {
    console.error('❌ Erro ao gravar fallback:', error);
  }
}
