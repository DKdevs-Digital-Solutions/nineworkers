import { dbPool } from './services/db.js';

export async function loadSession(userId) {
  try {
    const { rows } = await dbPool.query(
      'SELECT * FROM sessions WHERE user_id = $1 LIMIT 1',
      [userId]
    );
    
    return rows[0] || { current_block: null, vars: {} };
  } catch (error) {
    console.error('❌ Erro ao carregar sessão:', error);
    return { current_block: null, vars: {} };
  }
}

export async function saveSession(userId, currentBlock, flowId, vars) {
  const query = `
    INSERT INTO sessions (
      user_id, current_block, last_flow_id, vars, updated_at
    ) VALUES (
      $1, $2, $3, $4, $5
    )
    ON CONFLICT (user_id) 
    DO UPDATE SET
      current_block = EXCLUDED.current_block,
      last_flow_id = EXCLUDED.last_flow_id,
      vars = EXCLUDED.vars,
      updated_at = EXCLUDED.updated_at
  `;

  const values = [
    userId,
    currentBlock,
    flowId,
    vars,
    new Date().toISOString()
  ];

  try {
    await dbPool.query(query, values);
  } catch (error) {
    console.error('❌ Erro ao salvar sessão:', error);
    throw error; // Você pode optar por tratar o erro de forma diferente se necessário
  }
}
