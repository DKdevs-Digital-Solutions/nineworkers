// engine/queueHoursService.js
import * as db from './services/db.js';

const CACHE_TTL_MS = 60_000; // 1 min
const cache = new Map();     // key -> cfg
const stamps = new Map();    // key -> ts

function getRunner() {
  // Prioriza função única query(text, params)
  if (typeof db.query === 'function') return db.query;
  // Depois pool.query
  if (db.pool && typeof db.pool.query === 'function') return db.pool.query.bind(db.pool);
  // Depois dbPool.query (caso exporte como dbPool)
  if (db.dbPool && typeof db.dbPool.query === 'function') return db.dbPool.query.bind(db.dbPool);
  throw new Error('[queueHoursService] Nenhum runner de query disponível');
}

function coerce(obj, fallback) {
  if (obj == null) return fallback;
  if (typeof obj === 'string') {
    try { return JSON.parse(obj); } catch { return fallback; }
  }
  return obj;
}

/**
 * Carrega configuração de horário comercial da fila
 * a partir da VIEW: hmg.queue_business_hours
 *
 * Retorna objeto:
 * {
 *   queue_name, timezone,
 *   hours: { sun: [{start,end}], mon: [...], ... },
 *   holidays: ["YYYY-MM-DD", ...],
 *   exceptions: { "YYYY-MM-DD": [{start,end}], ... } | {},
 *   pre_human: { enabled, type, message } | null,
 *   off_hours: { holiday: {...}, closed: {...} } | null,
 *   // aliases de compat:
 *   tz, schedule
 * }
 */
export async function loadQueueBH(queueName) {
  if (!queueName) return null;

  const key = String(queueName).toLowerCase();
  const now = Date.now();

  // cache
  if (cache.has(key) && now - (stamps.get(key) || 0) < CACHE_TTL_MS) {
    return cache.get(key);
  }

  // garante pool/conn viva (se seu db.js expõe)
  if (typeof db.initDB === 'function') {
    await db.initDB();
  }

  const runQuery = getRunner();

  const sql = `
    SELECT queue_name, timezone, hours, holidays, exceptions, pre_human, off_hours
      FROM queue_business_hours
     WHERE lower(queue_name) = lower($1)
     LIMIT 1
  `;
  const { rows } = await runQuery(sql, [queueName]);
  const row = rows?.[0] || null;

  const cfg = row
    ? {
        queue_name: row.queue_name,
        timezone: row.timezone || row.tz || 'America/Sao_Paulo',

        hours:      coerce(row.hours,      {}), // jsonb -> objeto
        holidays:   coerce(row.holidays,   []),
        exceptions: coerce(row.exceptions, {}),

        pre_human:  coerce(row.pre_human,  null),
        off_hours:  coerce(row.off_hours,  null),

        // aliases p/ compat
        tz:        row.timezone || row.tz || 'America/Sao_Paulo',
        schedule:  coerce(row.hours, {}),
      }
    : null;

  cache.set(key, cfg);
  stamps.set(key, now);
  return cfg;
}

/** Invalida o cache (uma fila ou tudo) */
export function clearQueueBHCache(queueName) {
  if (!queueName) {
    cache.clear(); stamps.clear(); return;
  }
  const key = String(queueName).toLowerCase();
  cache.delete(key); stamps.delete(key);
}
