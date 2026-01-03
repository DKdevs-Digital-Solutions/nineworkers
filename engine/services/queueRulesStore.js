// engine/services/queueRulesStore.js
import { query } from './db.js';
import { evalConditionsSmart } from '../helpers.js';

let _cache = { at: 0, rules: [] };
const TTL_MS = 15_000; // 15s

async function fetchAll() {
  const sql = `
    SELECT queue_name, enabled, conditions
      FROM queue_rules
     WHERE enabled = true
     ORDER BY queue_name ASC
  `;
  const { rows } = await query(sql);
  return rows.map(r => ({
    queue: r.queue_name,
    conditions: Array.isArray(r.conditions) ? r.conditions : (r.conditions || []),
  }));
}

export async function loadQueueRulesCached(force = false) {
  const now = Date.now();
  if (!force && _cache.rules.length && (now - _cache.at) < TTL_MS) return _cache.rules;
  const rules = await fetchAll();
  _cache = { at: now, rules };
  return rules;
}

/**
 * Decide a fila pela primeira regra que bater.
 * Sem prioridade: ordem por nome da fila (ASC).
 * Se vars.fila já estiver setada e a regra correspondente bater, mantém essa.
 * Caso contrário, pega a primeira que bater.
 */
export async function pickQueueByDBRules(vars = {}, fallbackQueue = null) {
  const rules = await loadQueueRulesCached();

  // 1) se já tem fila e a regra daquela fila bate, respeita
  if (vars?.fila) {
    const r = rules.find(rr => rr.queue === vars.fila);
    if (r && evalConditionsSmart(r.conditions, vars)) {
      return r.queue;
    }
  }

  // 2) caso contrário, escolhe a primeira que bater (ordem por nome ASC — determinístico)
  for (const r of rules) {
    if (evalConditionsSmart(r.conditions, vars)) return r.queue;
  }

  // 3) fallback se nada bater
  return fallbackQueue;
}

/* ------------------------- CRUD para endpoints ------------------------- */

export async function getAllQueueRules() {
  const { rows } = await query(
    `SELECT queue_name, enabled, conditions, created_at, updated_at
       FROM queue_rules
      ORDER BY queue_name ASC`
  );
  return rows;
}

export async function getQueueRule(queueName) {
  const { rows } = await query(
    `SELECT queue_name, enabled, conditions, created_at, updated_at
       FROM queue_rules
      WHERE queue_name = $1
      LIMIT 1`,
    [queueName]
  );
  return rows[0] || null;
}

export async function upsertQueueRule(queueName, { enabled = true, conditions = [] } = {}) {
  const sql = `
    INSERT INTO queue_rules (queue_name, enabled, conditions)
    VALUES ($1, $2, $3::jsonb)
    ON CONFLICT (queue_name)
    DO UPDATE SET enabled = EXCLUDED.enabled,
                  conditions = EXCLUDED.conditions,
                  updated_at = now()
    RETURNING queue_name, enabled, conditions, created_at, updated_at
  `;
  const { rows } = await query(sql, [queueName, !!enabled, JSON.stringify(conditions || [])]);
  // invalida cache
  _cache = { at: 0, rules: [] };
  return rows[0];
}

export async function deleteQueueRule(queueName) {
  const { rows } = await query(
    `DELETE FROM queue_rules
      WHERE queue_name = $1
      RETURNING queue_name`,
    [queueName]
  );
  // invalida cache
  _cache = { at: 0, rules: [] };
  return rows[0]?.queue_name || null;
}
